from __future__ import annotations

import os
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.execution.entry_protection import (
    AcquireStatus,
    EntryProtection,
    EntryState,
    SubmitState,
)
from app.execution.entry_recovery import EntryRecoveryService
from app.execution.executor import BinanceExecutor
from shared_lib.persistence.db import DB


def _make_temp_db() -> tuple[str, DB]:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path, DB(path)


@pytest.fixture
def temp_db():
    path, db = _make_temp_db()
    try:
        yield path, db
    finally:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(path + suffix)
            except FileNotFoundError:
                pass


def _make_live_executor(db: DB, client: MagicMock, bot_id: str = "bot-never-again") -> BinanceExecutor:
    executor = BinanceExecutor(
        client=client,
        execution_mode="live",
        live_symbols=["BTCUSDT"],
        bot_instance_id=bot_id,
        db=db,
    )
    executor.run_id = "run-1"
    executor._allocation_type = "fixed_amount"
    executor._allocation_value = 100.0
    executor._max_notional_per_symbol = 100.0
    executor._allow_scale_in = False
    executor._allow_hedge_mode = False
    return executor


def _make_entry_order(order_id: str = "ENTRY-1", avg_fill_price: float = 50_000.0):
    return SimpleNamespace(
        broker_order_id=order_id,
        avg_fill_price=avg_fill_price,
        qty_filled=0.002,
        model_dump=lambda: {
            "orderId": order_id,
            "status": "FILLED",
            "executedQty": "0.002",
            "avgPrice": str(avg_fill_price),
            "updateTime": 1700000000000,
        },
    )


def test_retry_idempotency_reuses_same_active_intent(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)

    first = ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-fixed",
        intent_key="intent-fixed",
        cycle_id="cycle-1",
    )
    second = ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-fixed",
        intent_key="intent-fixed",
        cycle_id="cycle-1-retry",
    )
    third = ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-different",
        intent_key="intent-different",
        cycle_id="cycle-2",
    )

    assert first.status == AcquireStatus.ACQUIRED
    assert second.status == AcquireStatus.REUSED
    assert second.entry["client_order_id"] == "cid-fixed"
    assert third.status == AcquireStatus.BLOCKED
    assert ep.get_entry("bot-1", "BTCUSDT", "LONG")["state"] == EntryState.PENDING_OPEN.value
    event_types = [e["event_type"] for e in ep.list_events(bot_id="bot-1", symbol="BTCUSDT", limit=10)]
    assert "INTENT_REUSED" in event_types
    assert "INTENT_BLOCKED" in event_types


def test_timeout_and_sync_lag_hold_lock_until_exchange_truth_confirms(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-timeout",
        intent_key="intent-timeout",
    )
    ep.mark_submit_unknown("bot-1", "BTCUSDT", "LONG", "timeout_after_submit")

    first_reconcile = ep.reconcile_entry(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        exchange_pos_amt=0.0,
        order_evidence="FILLED",
    )
    blocked_retry = ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-second",
        intent_key="intent-second",
    )
    second_reconcile = ep.reconcile_entry(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        exchange_pos_amt=0.25,
        order_evidence=None,
    )
    final_entry = ep.get_entry("bot-1", "BTCUSDT", "LONG")

    assert first_reconcile == "ORDER_EVIDENCE_HELD"
    assert blocked_retry.status == AcquireStatus.BLOCKED
    assert second_reconcile == "CONFIRMED"
    assert final_entry["state"] == EntryState.OPEN_CONFIRMED.value
    assert final_entry["submit_state"] == SubmitState.SUBMIT_CONFIRMED.value


def test_symbol_global_lock_blocks_opposite_side_intent(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    first = ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-long",
        intent_key="intent-long",
    )
    blocked = ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="SHORT",
        intended_notional=100.0,
        client_order_id="cid-short",
        intent_key="intent-short",
    )

    assert first.status == AcquireStatus.ACQUIRED
    assert blocked.status == AcquireStatus.BLOCKED
    assert blocked.reason == "symbol_already_has_active_intent"


def test_restart_blocks_reopen_until_stronger_flat_proof(temp_db):
    path, db = temp_db
    ep1 = EntryProtection(db)
    ep1.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-restart",
        intent_key="intent-restart",
    )
    ep1.mark_submit_unknown("bot-1", "BTCUSDT", "LONG", "restart_during_pending")

    ep2 = EntryProtection(DB(path))
    blocked_after_restart = ep2.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-restart-2",
        intent_key="intent-restart-2",
    )

    with ep2.db.connect() as conn:
        conn.execute(
            """
            UPDATE pending_entries
            SET submitted_at_ms = submitted_at_ms - 120000
            WHERE bot_id=? AND symbol=? AND side=?
            """,
            ("bot-1", "BTCUSDT", "LONG"),
        )

    results = [
        ep2.reconcile_entry("bot-1", "BTCUSDT", "LONG", exchange_pos_amt=0.0, order_evidence=None),
        ep2.reconcile_entry("bot-1", "BTCUSDT", "LONG", exchange_pos_amt=0.0, order_evidence=None),
        ep2.reconcile_entry("bot-1", "BTCUSDT", "LONG", exchange_pos_amt=0.0, order_evidence=None),
    ]
    assert ep2.get_entry("bot-1", "BTCUSDT", "LONG") is None
    reopened = ep2.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-restart-3",
        intent_key="intent-restart-3",
    )

    assert blocked_after_restart.status == AcquireStatus.BLOCKED
    assert results[:2] == ["WAITING_STRONGER_FLAT_PROOF", "WAITING_STRONGER_FLAT_PROOF"]
    assert results[2] == "CLEARED"
    assert reopened.status == AcquireStatus.ACQUIRED


def test_post_submit_exception_keeps_lock_and_blocks_duplicate_retry(temp_db):
    _, db = temp_db
    frozen_now = 1_700_000_000.0

    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "0.0"}
    client.account.return_value = {
        "availableBalance": "1000.0",
        "totalWalletBalance": "1000.0",
        "totalMaintMargin": "0.0",
        "totalInitialMargin": "0.0",
    }
    client.get_prices.return_value = {"BTCUSDT": 50_000.0}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, int(frozen_now * 1000)]]
    client.place_order.return_value = _make_entry_order()

    executor = _make_live_executor(db, client)
    executor._size_qty = MagicMock(return_value=(0.002, {"price": 50_000.0, "leverage": 1}))
    executor._entry_prot.mark_submit_confirmed = MagicMock(side_effect=RuntimeError("db write failed"))

    with patch("app.execution.executor.time.time", return_value=frozen_now):
        first = executor.execute_signal(
            "BTCUSDT",
            "BUY",
            60.0,
            sl_price=49_000.0,
            tp_price=53_000.0,
            current_equity=1_000.0,
        )
        second = executor.execute_signal(
            "BTCUSDT",
            "BUY",
            60.0,
            sl_price=49_000.0,
            tp_price=53_000.0,
            current_equity=1_000.0,
        )

    entry = executor._entry_prot.get_entry("bot-never-again", "BTCUSDT", "LONG")

    assert first.status == "SUBMIT_UNCERTAIN"
    assert second.status == "ENTRY_INTENT_REUSED"
    assert second.details["client_order_id"] == entry["client_order_id"]
    assert entry["state"] == EntryState.PENDING_OPEN.value
    assert entry["submit_state"] == SubmitState.SUBMIT_UNKNOWN.value
    assert client.place_order.call_count == 1


def test_exposure_guard_counts_pending_and_blocks_over_allocation(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    ep.acquire_intent(
        bot_id="bot-never-again",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=80.0,
        client_order_id="cid-existing",
        intent_key="intent-existing",
    )

    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "0.0"}

    executor = _make_live_executor(db, client)

    result = executor.execute_signal(
        "BTCUSDT",
        "BUY",
        30.0,
        sl_price=49_000.0,
        tp_price=53_000.0,
        current_equity=1_000.0,
    )

    assert result.status == "ENTRY_LOCK_HELD"
    client.place_order.assert_not_called()


def test_exact_exposure_uses_final_sized_notional_not_requested_budget(temp_db):
    _, db = temp_db
    frozen_now = 1_700_000_000.0

    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "0.0"}
    client.account.return_value = {
        "availableBalance": "1000.0",
        "totalWalletBalance": "1000.0",
        "totalMaintMargin": "0.0",
        "totalInitialMargin": "0.0",
    }
    client.get_prices.return_value = {"BTCUSDT": 50_000.0}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, int(frozen_now * 1000)]]
    client.place_order.return_value = _make_entry_order()
    client.place_protection.return_value = SimpleNamespace(
        status="success",
        sl_order_id="SL-1",
        tp_order_id="TP-1",
        model_dump=lambda: {"status": "success", "sl_order_id": "SL-1", "tp_order_id": "TP-1"},
    )

    executor = _make_live_executor(db, client)
    executor._size_qty = MagicMock(return_value=(0.0018, {"price": 50_000.0, "leverage": 1}))

    with patch("app.execution.executor.time.time", return_value=frozen_now):
        result = executor.execute_signal(
            "BTCUSDT",
            "BUY",
            120.0,
            sl_price=49_000.0,
            tp_price=53_000.0,
            current_equity=1_000.0,
        )

    entry = executor._entry_prot.get_entry("bot-never-again", "BTCUSDT", "LONG")
    assert result.status == "ORDER_PLACED"
    assert entry["sized_notional"] == pytest.approx(90.0)
    assert entry["filled_notional"] == pytest.approx(100.0)


def test_exact_exposure_blocks_after_sizing_when_final_notional_exceeds_max(temp_db):
    _, db = temp_db
    frozen_now = 1_700_000_000.0

    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "0.0"}
    client.account.return_value = {
        "availableBalance": "1000.0",
        "totalWalletBalance": "1000.0",
        "totalMaintMargin": "0.0",
        "totalInitialMargin": "0.0",
    }
    client.get_prices.return_value = {"BTCUSDT": 50_000.0}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, int(frozen_now * 1000)]]

    executor = _make_live_executor(db, client)
    executor._size_qty = MagicMock(return_value=(0.0022, {"price": 50_000.0, "leverage": 1}))

    with patch("app.execution.executor.time.time", return_value=frozen_now):
        result = executor.execute_signal(
            "BTCUSDT",
            "BUY",
            90.0,
            sl_price=49_000.0,
            tp_price=53_000.0,
            current_equity=1_000.0,
        )

    assert result.status == "EXPOSURE_LIMIT_EXCEEDED"
    client.place_order.assert_not_called()
    events = executor._entry_prot.list_events(bot_id="bot-never-again", symbol="BTCUSDT", limit=20)
    assert any(e["event_type"] == "EXPOSURE_BLOCKED" for e in events)


def test_confirmed_exposure_uses_filled_notional_after_entry_confirm(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-1",
        intent_key="intent-1",
    )
    ep.mark_sized("bot-1", "BTCUSDT", "LONG", sized_notional=95.0, sized_qty=0.0019, reference_price=50_000.0)
    ep.mark_submit_prepared("bot-1", "BTCUSDT", "LONG", submitted_notional=95.0, submitted_qty=0.0019, reference_price=50_000.0)
    ep.mark_confirmed("bot-1", "BTCUSDT", "LONG", filled_notional=83.0, filled_qty=0.00166)

    assert ep.get_effective_exposure("bot-1", "BTCUSDT") == pytest.approx(83.0)


def test_flip_closes_old_side_before_new_side_proceeds(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    ep.acquire_intent(
        bot_id="bot-never-again",
        symbol="BTCUSDT",
        side="SHORT",
        intended_notional=50.0,
        client_order_id="cid-short",
        intent_key="intent-short",
    )
    ep.mark_confirmed("bot-never-again", "BTCUSDT", "SHORT", filled_notional=50.0, filled_qty=0.001)

    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "-0.001"}
    client.account.return_value = {
        "availableBalance": "1000.0",
        "totalWalletBalance": "1000.0",
        "totalMaintMargin": "0.0",
        "totalInitialMargin": "0.0",
    }
    client.get_prices.return_value = {"BTCUSDT": 50_000.0}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, int(1_700_000_000_000)]]
    client.close_position_market.return_value = {"orderId": "CLOSE-1", "status": "FILLED", "executedQty": "0.001", "avgPrice": "50000", "updateTime": 1700000000000}
    client.place_order.return_value = _make_entry_order(order_id="ENTRY-LONG")
    client.place_protection.return_value = SimpleNamespace(
        status="success",
        sl_order_id="SL-1",
        tp_order_id="TP-1",
        model_dump=lambda: {"status": "success", "sl_order_id": "SL-1", "tp_order_id": "TP-1"},
    )

    executor = _make_live_executor(db, client)
    executor._size_qty = MagicMock(return_value=(0.001, {"price": 50_000.0, "leverage": 1}))

    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        result = executor.execute_signal(
            "BTCUSDT",
            "BUY",
            50.0,
            sl_price=49_000.0,
            tp_price=53_000.0,
            current_equity=1_000.0,
        )

    assert result.status == "ORDER_PLACED"
    assert ep.get_entry("bot-never-again", "BTCUSDT", "SHORT") is None
    assert ep.get_entry("bot-never-again", "BTCUSDT", "LONG") is not None


def test_operator_recovery_releases_stale_entry_with_audit_event(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-stale",
        intent_key="intent-stale",
    )
    ep.mark_submit_unknown("bot-1", "BTCUSDT", "LONG", "timeout")
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE pending_entries
            SET submitted_at_ms = submitted_at_ms - 180000,
                flat_confirmations = 2
            WHERE bot_id=? AND symbol=? AND side=?
            """,
            ("bot-1", "BTCUSDT", "LONG"),
        )

    service = EntryRecoveryService(db)
    rows = service.list_entries("bot-1", symbol="BTCUSDT")
    result = service.release_entry(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        actor_user_id="admin-1",
        exchange_flat_confirmed=True,
        note="verified flat on exchange",
    )

    with db.connect() as conn:
        event = conn.execute(
            "SELECT event_type, action, details_json FROM events WHERE event_type=? ORDER BY rowid DESC LIMIT 1",
            ("ENTRY_PROTECTION_MANUAL_RELEASE",),
        ).fetchone()

    assert rows[0]["recoverable"] is True
    assert result["released"] is True
    assert ep.get_entry("bot-1", "BTCUSDT", "LONG") is None
    assert event["event_type"] == "ENTRY_PROTECTION_MANUAL_RELEASE"
    ep_events = ep.list_events(bot_id="bot-1", symbol="BTCUSDT", limit=20)
    assert any(e["event_type"] == "RELEASED" for e in ep_events)


def test_event_trail_emits_required_entry_lifecycle_events(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=120.0,
        client_order_id="cid-events",
        intent_key="intent-events",
        cycle_id="cycle-events",
    )
    ep.mark_sized("bot-1", "BTCUSDT", "LONG", sized_notional=95.0, sized_qty=0.0019, reference_price=50_000.0)
    ep.mark_submit_prepared("bot-1", "BTCUSDT", "LONG", submitted_notional=95.0, submitted_qty=0.0019, reference_price=50_000.0)
    ep.mark_submit_confirmed("bot-1", "BTCUSDT", "LONG", broker_order_id="BROKER-1")
    ep.mark_submit_unknown("bot-1", "BTCUSDT", "LONG", "timeout")
    ep.reconcile_entry(
        "bot-1",
        "BTCUSDT",
        "LONG",
        exchange_pos_amt=0.0,
        order_evidence="FILLED",
        order_snapshot={"executedQty": "0.0018", "avgPrice": "51000"},
    )
    ep.mark_confirmed("bot-1", "BTCUSDT", "LONG", filled_notional=91.8, filled_qty=0.0018)
    ep.mark_closed("bot-1", "BTCUSDT", "LONG")

    events = ep.list_events(bot_id="bot-1", symbol="BTCUSDT", limit=50)
    event_types = {e["event_type"] for e in events}

    assert {
        "INTENT_ACQUIRED",
        "SIZED",
        "SUBMIT_PREPARED",
        "SUBMIT_CONFIRMED",
        "SUBMIT_UNKNOWN",
        "RECONCILE_HELD",
        "OPEN_CONFIRMED",
        "MARK_CLOSED",
    }.issubset(event_types)


def test_forensic_report_proves_invariant_and_retry_reuse(temp_db):
    _, db = temp_db
    ep = EntryProtection(db)
    ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-proof",
        intent_key="intent-proof",
    )
    ep.acquire_intent(
        bot_id="bot-1",
        symbol="BTCUSDT",
        side="LONG",
        intended_notional=100.0,
        client_order_id="cid-proof",
        intent_key="intent-proof",
    )
    ep.mark_submit_unknown("bot-1", "BTCUSDT", "LONG", "timeout")
    ep.mark_confirmed("bot-1", "BTCUSDT", "LONG", filled_notional=99.0, filled_qty=0.00198)
    ep.mark_closed("bot-1", "BTCUSDT", "LONG")

    service = EntryRecoveryService(db)
    report = service.get_forensic_report(bot_id="bot-1", symbol="BTCUSDT")

    assert report["duplicate_unresolved_violations"] == []
    assert report["exposure_violations"] == []
    assert report["reused_retries"][0]["client_order_id"] == "cid-proof"
    assert report["submit_unknown_outcomes"][0]["outcome"] in {"OPEN_CONFIRMED", "MARK_CLOSED"}


def test_client_order_id_is_deterministic_for_same_intent_bucket(temp_db):
    _, db = temp_db
    client = MagicMock()
    executor = _make_live_executor(db, client)

    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        first_intent, first_cid = executor._build_entry_idempotency(
            symbol="BTCUSDT",
            side="LONG",
            usdt=60.0,
            sl_price=49_000.0,
            tp_price=53_000.0,
        )
        second_intent, second_cid = executor._build_entry_idempotency(
            symbol="BTCUSDT",
            side="LONG",
            usdt=60.0,
            sl_price=49_000.0,
            tp_price=53_000.0,
        )

    assert first_intent == second_intent
    assert first_cid == second_cid
