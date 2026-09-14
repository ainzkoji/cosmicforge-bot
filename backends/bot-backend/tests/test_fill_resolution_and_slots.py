"""Broker fill resolution, lifecycle registration, and hard position slots.

Found live on 2026-09-13 (sessions rts_b7423a77 and rts_f44f80f8): Binance demo
answered every MARKET entry with an ACK -- status NEW, executedQty 0 -- although
the broker shows each order FILLED in full. The runner raised
BROKER_FILL_QUANTITY_UNAVAILABLE and skipped lifecycle registration, and the
policy slot gate deferred to a budget engine whose own position count never saw
those positions. Ten entries opened against two slots. Reconciliation adopted
them afterwards; it must stay the backstop, not the routine path.
"""
from __future__ import annotations

import threading
import time
import uuid
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.decision.reasons import RiskReason
from app.evidence.fill_bridge import execution_attempt, project_fill
from app.evidence.writers import open_bot_run, open_runtime_session
from app.execution.entry_protection import get_entry_protection
from app.execution.executor import BinanceExecutor
from app.execution.fill_resolution import (
    BROKER_STATE_UNKNOWN,
    CANCELED,
    FILLED,
    INITIAL_ORDER_RESPONSE,
    ORDER_PENDING,
    ORDER_STATUS_QUERY,
    TRADE_FILL_QUERY,
    resolve_order_fill,
)
from app.execution.position_reconciliation import BrokerPosition, reconcile_position_rows
from app.execution.position_slots import (
    evaluate_slot,
    occupied_slots,
    reserve_entry_slot,
    slot_diagnostics,
)
from app.risk.capital_ledger import ACCOUNT_RESERVATIONS, margin_for
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import PAPER_FORWARD
from shared_lib.persistence.migrations import migrate

BOT = "bot_fill_res"
RUN_ID = "run_fill_res"
ACCOUNT = "brk_fill_res"
SYMBOL = "BTCUSDT"
PRICE = 100.0
LEV = 7


def _nosleep(_seconds):
    return None


def _prepare(database):
    migrate(database)
    session = open_runtime_session(
        database, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="fill-res-tests",
    )
    open_bot_run(
        database, run_id=RUN_ID, bot_instance_id=BOT, runtime_session_id=session,
        user_id="user_fill", policy_hash="policy_fill", provenance=PAPER_FORWARD,
        execution_mode="paper", broker_environment="demo",
    )
    return database


@pytest.fixture
def db():
    return _prepare(DB(":memory:"))


class _Ctx:
    bot_instance_id = BOT
    user_id = "user_fill"
    broker_account_id = ACCOUNT
    broker_environment = "demo"
    market_type = "CRYPTO"
    effective_policy_hash = "policy_fill"


class _Runner:
    def __init__(self, database):
        self.db = database
        self.run_id = RUN_ID
        self.cycle_id = "cyc_fill"
        self.context = _Ctx()
        self.position_manager = None
        self._active_decision_ids = {}
        self._symbol_evidence = {}

    def _effective_execution_mode(self):
        return "paper"


def _open(database, symbol, side="LONG", qty=10.0, price=PRICE, pid=None, requested=None):
    fill = {
        "symbol": symbol, "side": side, "action": "OPEN", "qty": qty, "price": price,
        "position_id": pid or f"pos_{symbol}_{side}", "leverage": LEV,
    }
    if requested is not None:
        fill["requested_qty"] = requested
    project_fill(_Runner(database), database, fill)


def _intent(ep, symbol, side="LONG", hedge=False):
    return ep.acquire_intent(
        bot_id=BOT, symbol=symbol, side=side, intended_notional=840.0,
        client_order_id=f"cid_{symbol}_{side}", intent_key=f"ik_{symbol}_{side}",
        cycle_id="c1", allow_hedge=hedge,
    )


# ══════════════════════════════════════════════════════════════════════════
# Fill resolution (1-11, 15)
# ══════════════════════════════════════════════════════════════════════════


class Broker:
    """get_order answers in sequence (the last answer repeats); user_trades returns fills."""

    def __init__(self, orders=(), trades=(), order_error=None, trades_error=None):
        self.orders = list(orders)
        self.trades = list(trades)
        self.order_error = order_error
        self.trades_error = trades_error
        self.order_calls = 0

    def get_order(self, symbol, order_id):
        self.order_calls += 1
        if self.order_error:
            raise self.order_error
        if not self.orders:
            raise RuntimeError("order not found")
        return self.orders[min(self.order_calls, len(self.orders)) - 1]

    def user_trades(self, symbol, limit=200, **_):
        if self.trades_error:
            raise self.trades_error
        return list(self.trades)


def _order(status, qty="0", avg="0.00", oid=555):
    return {"orderId": oid, "clientOrderId": "cid-1", "status": status,
            "executedQty": qty, "avgPrice": avg, "origQty": "100"}


def _fill(qty, price, fee="0", oid=555, asset="USDT"):
    return {"orderId": oid, "qty": qty, "price": price, "commission": fee, "commissionAsset": asset}


def resolve(broker, initial):
    return resolve_order_fill(broker, symbol="XUSDT", order_response=initial, sleep=_nosleep)


def test_filled_response_with_quantity_is_used_directly():
    broker = Broker(trades=[_fill("100", "2.0", "0.08")])
    r = resolve(broker, _order("FILLED", "100", "2.0"))
    assert (r.status, r.executed_qty, r.avg_price) == (FILLED, 100.0, 2.0)
    assert r.source == INITIAL_ORDER_RESPONSE
    assert broker.order_calls == 0
    assert r.fees == pytest.approx(0.08)


def test_filled_with_zero_executed_qty_is_resolved_by_the_order_query():
    r = resolve(Broker(orders=[_order("FILLED", "100", "2.0")]), _order("FILLED", "0"))
    assert (r.status, r.executed_qty, r.source) == (FILLED, 100.0, ORDER_STATUS_QUERY)


def test_order_query_without_quantity_is_resolved_from_the_fills():
    broker = Broker(
        orders=[_order("FILLED", "0")],
        trades=[_fill("60", "2.0", "0.05"), _fill("40", "2.5", "0.04"), _fill("999", "9", "9", oid=777)],
    )
    r = resolve(broker, _order("NEW"))
    assert r.source == TRADE_FILL_QUERY
    assert r.executed_qty == pytest.approx(100.0)
    assert r.fill_count == 2  # the other order's fill is not ours


def test_weighted_quantity_price_and_fees_across_fills():
    broker = Broker(trades=[_fill("60", "2.0", "0.05"), _fill("40", "2.5", "0.04")])
    r = resolve(broker, _order("NEW"))
    assert r.executed_qty == pytest.approx(100.0)
    assert r.avg_price == pytest.approx((60 * 2.0 + 40 * 2.5) / 100)
    assert r.fees == pytest.approx(0.09)
    assert r.fee_asset == "USDT"


def test_new_then_filled():
    broker = Broker(orders=[_order("NEW"), _order("FILLED", "100", "2.0")])
    r = resolve(broker, _order("NEW"))
    assert (r.status, r.executed_qty) == (FILLED, 100.0)
    assert broker.order_calls == 2


def test_partially_filled_then_filled():
    broker = Broker(orders=[_order("PARTIALLY_FILLED", "40", "2.0"), _order("FILLED", "100", "2.1")])
    r = resolve(broker, _order("NEW"))
    assert (r.status, r.executed_qty, r.avg_price) == (FILLED, 100.0, 2.1)


def test_partially_filled_then_canceled_keeps_the_actual_fill():
    broker = Broker(orders=[_order("PARTIALLY_FILLED", "40", "2.0"), _order("CANCELED", "40", "2.0")])
    r = resolve(broker, _order("NEW"))
    assert r.status == CANCELED
    assert r.executed_qty == 40.0
    assert r.has_fill and not r.zero_fill_terminal


def test_zero_fill_cancel_is_terminal():
    r = resolve(Broker(orders=[_order("CANCELED", "0")]), _order("NEW"))
    assert r.zero_fill_terminal and r.executed_qty == 0.0


def test_broker_state_unavailable_is_unknown_not_failure():
    broker = Broker(order_error=RuntimeError("timeout"), trades_error=RuntimeError("timeout"))
    r = resolve(broker, _order("NEW"))
    assert r.status == BROKER_STATE_UNKNOWN
    assert r.unresolved and not r.zero_fill_terminal


def test_an_order_still_working_is_pending_not_failed():
    r = resolve(Broker(orders=[_order("NEW")]), _order("NEW"))
    assert r.status == ORDER_PENDING and r.unresolved


def test_requested_quantity_is_never_used_as_executed_quantity():
    broker = Broker(order_error=RuntimeError("down"), trades_error=RuntimeError("down"))
    r = resolve(broker, {"orderId": 555, "origQty": "100", "status": "NEW", "executedQty": "0"})
    assert r.executed_qty == 0.0


def test_binance_demo_ack_regression_chillguy():
    """The exact 2026-09-13 shape: ACK now, FILLED in full at the broker."""
    ack = {"orderId": 297768987, "clientOrderId": "CFBOTALCHILLG03df5770c524ae3b",
           "status": "NEW", "executedQty": "0", "avgPrice": "0.00", "origQty": "66778"}
    broker = Broker(
        orders=[_order("FILLED", "66778", "0.0125700", oid=297768987)],
        trades=[_fill("66778", "0.01257", "0.41970", oid=297768987)],
    )
    r = resolve(broker, ack)
    assert (r.status, r.executed_qty) == (FILLED, 66778.0)
    assert r.avg_price == pytest.approx(0.01257)
    assert r.fees == pytest.approx(0.4197)
    assert r.initial_executed_qty == 0.0
    assert r.source == ORDER_STATUS_QUERY


def test_binance_orders_request_a_result_response():
    from app.exchange.binance.client import BinanceFuturesClient
    from app.models.unified_trading import OrderRequest, OrderType, Side

    client = object.__new__(BinanceFuturesClient)
    sent = []

    def post(path, params=None):
        sent.append(dict(params))
        return {"orderId": 1, "clientOrderId": "x", "status": "FILLED",
                "executedQty": "5", "avgPrice": "2"}

    client._signed_post = post
    order = client.place_order(OrderRequest(
        symbol="XUSDT", side=Side.BUY, type=OrderType.MARKET, qty=Decimal("5"),
        leverage=None, reduce_only=False, client_order_id="x",
    ))
    client.place_market_order("XUSDT", "BUY", 5)
    assert all(p["newOrderRespType"] == "RESULT" for p in sent)
    assert order.qty_filled == Decimal("5")
    assert str(getattr(order.status, "value", order.status)).upper() == "FILLED"


# ══════════════════════════════════════════════════════════════════════════
# Slots (12, 18-26)
# ══════════════════════════════════════════════════════════════════════════


def test_max_two_slots_block_a_third_position(db):
    _open(db, "AAAUSDT")
    _open(db, "BBBUSDT")
    verdict = evaluate_slot(db, BOT, "CCCUSDT", "LONG", 2)
    assert not verdict.allowed
    assert verdict.reason == RiskReason.MAX_OPEN_POSITIONS


def test_reconciled_positions_occupy_slots(db):
    reconcile_position_rows(
        db, bot_instance_id=BOT, broker_account_id=ACCOUNT,
        broker_positions=[BrokerPosition("AAAUSDT", "SHORT", Decimal("10"), Decimal("1"),
                                         Decimal("7"), "cross", "ONE_WAY")],
        position_mode="ONE_WAY", spec_resolver=lambda _s: None, run_id=RUN_ID,
        cycle_id="c", execution_mode="broker", broker_environment="demo",
    )
    assert occupied_slots(db, BOT)["positions"] == {"AAAUSDT"}


def test_in_flight_entries_occupy_slots(db):
    ep = get_entry_protection(db)
    assert _intent(ep, "AAAUSDT").status.value == "ACQUIRED"
    assert occupied_slots(db, BOT)["pending"] == {"AAAUSDT"}
    assert not evaluate_slot(db, BOT, "BBBUSDT", "LONG", 1).allowed


def test_failed_order_releases_its_slot(db):
    ep = get_entry_protection(db)
    _intent(ep, "AAAUSDT")
    ep.mark_failed(BOT, "AAAUSDT", "LONG", reason="rejected")
    assert evaluate_slot(db, BOT, "BBBUSDT", "LONG", 1).allowed


def test_one_way_one_symbol_is_one_slot(db):
    _open(db, "AAAUSDT", "LONG")
    verdict = evaluate_slot(db, BOT, "AAAUSDT", "SHORT", 1)
    assert verdict.allowed and verdict.reason == "SLOT_ALREADY_HELD"
    assert verdict.occupied == ("AAAUSDT",)


def test_hedge_mode_long_and_short_are_separate_slots(db):
    _open(db, "AAAUSDT", "LONG")
    assert not evaluate_slot(db, BOT, "AAAUSDT", "SHORT", 1, hedge_mode=True).allowed
    assert evaluate_slot(db, BOT, "AAAUSDT", "SHORT", 2, hedge_mode=True).allowed


def test_concurrent_last_slot_is_taken_exactly_once(tmp_path):
    database = _prepare(DB(str(tmp_path / "slots.db")))
    ep = get_entry_protection(database)
    _intent(ep, "AAAUSDT")  # slot 1 of 2
    barrier = threading.Barrier(2)
    verdicts = {}

    def contender(symbol):
        barrier.wait()

        def acquire():
            time.sleep(0.05)  # widen the window between check and reservation
            return _intent(ep, symbol)

        verdicts[symbol] = reserve_entry_slot(database, BOT, symbol, "LONG", 2, acquire=acquire)[0]

    threads = [threading.Thread(target=contender, args=(s,)) for s in ("BBBUSDT", "CCCUSDT")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(v.allowed for v in verdicts.values()) == [False, True]
    loser = next(v for v in verdicts.values() if not v.allowed)
    assert loser.reason == RiskReason.MAX_OPEN_POSITIONS
    assert len(occupied_slots(database, BOT)["pending"]) == 2


def test_slot_diagnostics(db):
    ep = get_entry_protection(db)
    _open(db, "AAAUSDT")
    _intent(ep, "BBBUSDT")
    ep.mark_submit_unknown(BOT, "BBBUSDT", "LONG", reason="timeout")
    diag = slot_diagnostics(db, BOT, 2)
    assert diag["economic_open_position_count"] == 1
    assert diag["pending_entry_count"] == 1
    assert diag["available_position_slots"] == 0
    assert diag["unresolved_broker_orders"] == 1


def test_budget_engine_can_no_longer_lift_the_position_limit():
    from app.policy.policy_engine import PolicyContext, PolicyEngine, ReasonCode

    class LooseBudget:
        def get_budget_state(self):
            return SimpleNamespace(allowed_slots=15, position_count=0)

    decision = PolicyEngine(budget_engine=LooseBudget()).evaluate(PolicyContext(
        symbol="ZZFILLUSDT", signal="BUY", position="NONE", open_positions_count=2,
        max_open_positions=2, entry_price=1.0,
    ))
    assert decision.reason_code == ReasonCode.MAX_POSITIONS_REACHED


# ══════════════════════════════════════════════════════════════════════════
# The live executor path (12-15, 22-24, 27-29)
# ══════════════════════════════════════════════════════════════════════════


def _entry(qty="0", avg="0.00", status="NEW", oid="555"):
    def build(req):
        dump = {"orderId": oid, "clientOrderId": req.client_order_id, "status": status,
                "executedQty": qty, "avgPrice": avg, "updateTime": 1_700_000_000_000}
        return SimpleNamespace(
            broker_order_id=oid, client_order_id=req.client_order_id,
            qty_filled=Decimal(qty), avg_fill_price=Decimal(avg), status=status,
            model_dump=lambda: dict(dump),
        )
    return build


def _live(database, *, entry, orders=(), trades=(), order_error=None, trades_error=None,
          max_slots=2):
    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "0.0"}
    client.account.return_value = {
        "availableBalance": "5000.0", "totalWalletBalance": "5000.0",
        "totalMaintMargin": "0.0", "totalInitialMargin": "0.0",
    }
    client.get_prices.return_value = {SYMBOL: PRICE}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, 1_700_000_000_000]]
    seen = {}

    def place_order(req):
        seen["order"] = req
        return entry(req)

    def protect(req):
        seen["protection"] = req
        return SimpleNamespace(status="success", sl_order_id="SL-1", tp_order_id="TP-1",
                               model_dump=lambda: {"status": "success"})

    broker = Broker(orders, trades, order_error, trades_error)
    client.place_order.side_effect = place_order
    client.get_order.side_effect = broker.get_order
    client.user_trades.side_effect = broker.user_trades
    client.place_protection.side_effect = protect

    ex = BinanceExecutor(client=client, execution_mode="live", live_symbols=[SYMBOL],
                         bot_instance_id=BOT, db=database)
    ex.run_id = "run-fill"
    ex._capital_budget = 120.0
    ex._allocation_type = "fixed_amount"
    ex._allocation_value = 120.0
    ex._max_notional_per_symbol = 120.0 * LEV * 1.2
    ex._broker_account_id = f"acct_{uuid.uuid4().hex}"
    ex._max_open_positions = max_slots
    ex._fill_resolution_sleep = _nosleep
    ex._size_qty = lambda symbol, budget, lev_mult, sl_price=0.0, leverage_override=None: (
        budget / PRICE, {"price": PRICE, "leverage": LEV}
    )
    return ex, client, seen


def _run(ex):
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        return ex.execute_signal(SYMBOL, "BUY", 120.0 * LEV, sl_price=95.0, tp_price=110.0,
                                 current_equity=5_000.0, leverage_override=LEV)


def test_an_ack_entry_is_resolved_and_confirmed_with_the_executed_quantity(db):
    ex, _, seen = _live(db, entry=_entry(), orders=[_order("FILLED", "8.38", "100.10")],
                        trades=[_fill("8.38", "100.10", "0.33552")])
    res = _run(ex)
    assert res.status == "ORDER_PLACED", res.error
    assert res.details["filled_qty"] == pytest.approx(8.38)
    assert res.avg_price == pytest.approx(100.10)
    assert res.details["fee"] == pytest.approx(0.33552)
    assert res.details["fill_resolution"]["source"] == ORDER_STATUS_QUERY
    assert res.details["fill_resolution"]["initial_executed_qty"] == 0.0
    # SL/TP is placed for what the broker executed.
    assert float(seen["protection"].qty) == pytest.approx(8.38)
    entry = ex._entry_prot.get_entry(BOT, SYMBOL, "LONG")
    assert entry["state"] == "OPEN_CONFIRMED"
    assert entry["filled_qty"] == pytest.approx(8.38)
    # Capital semantics are unchanged: its own 120 per trade.
    assert res.details["capital"]["approved_margin"] == pytest.approx(120.0)


def test_a_partial_fill_reports_protects_and_keeps_the_actual_quantity(db):
    ex, _, seen = _live(db, entry=_entry(),
                        orders=[_order("PARTIALLY_FILLED", "4.19", "100.0"),
                                _order("CANCELED", "4.19", "100.0")])
    res = _run(ex)
    assert res.status == "ORDER_PLACED", res.error
    assert res.details["filled_qty"] == pytest.approx(4.19)
    assert res.details["requested_qty"] > res.details["filled_qty"]  # intent stays intent
    assert float(seen["protection"].qty) == pytest.approx(4.19)
    # The slot stays taken, and the account reservation becomes the real exposure.
    assert "BTCUSDT" in occupied_slots(db, BOT)["pending"]
    assert ACCOUNT_RESERVATIONS.pending_margin(ex._broker_account_id) == pytest.approx(
        margin_for(4.19, 100.0, LEV)
    )


def test_a_zero_fill_cancel_releases_the_slot_and_the_reservation(db):
    ex, client, _ = _live(db, entry=_entry(), orders=[_order("CANCELED", "0")])
    res = _run(ex)
    assert res.status == "ORDER_NOT_FILLED"
    assert occupied_slots(db, BOT)["pending"] == set()
    assert ACCOUNT_RESERVATIONS.pending_margin(ex._broker_account_id) == 0.0
    client.place_protection.assert_not_called()


def test_an_unresolved_order_holds_its_slot(db):
    ex, client, _ = _live(db, entry=_entry(), order_error=RuntimeError("timeout"),
                          trades_error=RuntimeError("timeout"))
    res = _run(ex)
    assert res.status == "SUBMIT_UNCERTAIN"
    assert res.details["fill_resolution"]["status"] == BROKER_STATE_UNKNOWN
    entry = ex._entry_prot.get_entry(BOT, SYMBOL, "LONG")
    assert entry["submit_state"] == "SUBMIT_UNKNOWN"
    assert not evaluate_slot(db, BOT, "ETHUSDT", "LONG", 1).allowed
    assert slot_diagnostics(db, BOT, 2)["unresolved_broker_orders"] == 1
    client.place_protection.assert_not_called()


def test_the_executor_blocks_an_entry_when_every_slot_is_taken(db):
    _open(db, "AAAUSDT")
    _open(db, "BBBUSDT")
    ex, client, _ = _live(db, entry=_entry("8.4", "100.0", "FILLED"))
    res = _run(ex)
    assert res.status == "MAX_OPEN_POSITIONS"
    assert res.details["reason_code"] == RiskReason.MAX_OPEN_POSITIONS
    client.place_order.assert_not_called()
    client.account.assert_not_called()  # no capital reserved for a blocked slot


def test_a_filled_response_needs_no_extra_broker_query(db):
    ex, client, _ = _live(db, entry=_entry("8.38", "100.0", "FILLED"))
    res = _run(ex)
    assert res.status == "ORDER_PLACED", res.error
    assert res.details["fill_resolution"]["source"] == INITIAL_ORDER_RESPONSE
    client.get_order.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════
# Registration, evidence, reconciliation (13, 14, 16, 17)
# ══════════════════════════════════════════════════════════════════════════


def test_the_execution_attempt_persists_fill_resolution(db):
    runner = _Runner(db)
    with execution_attempt(runner, SYMBOL, "BUY") as attempt:
        attempt.completed(
            "ORDER_PLACED", broker_order_id="555", client_order_id="cid-1",
            requested_qty=8.4, executed_qty=8.38, avg_fill_price=100.1,
            fill_resolution={
                "initial_response_executed_qty": 0.0, "resolved_executed_qty": 8.38,
                "fill_resolution_source": ORDER_STATUS_QUERY,
                "fill_resolution_status": FILLED, "fees": 0.33, "fee_asset": "USDT",
            },
        )
    with db.connect() as conn:
        row = conn.execute(
            "SELECT requested_qty, initial_response_executed_qty, resolved_executed_qty, "
            "fill_resolution_source, fill_resolution_status, fees, broker_order_id "
            "FROM execution_attempts WHERE execution_attempt_id=?", (attempt.attempt_id,),
        ).fetchone()
    assert tuple(row) == (8.4, 0.0, 8.38, ORDER_STATUS_QUERY, FILLED, 0.33, "555")


def test_a_resolved_fill_creates_the_position_at_the_executed_quantity(db):
    _open(db, SYMBOL, qty=8.38, price=100.1, pid="pos_norm", requested=8.4)
    with db.connect() as conn:
        row = conn.execute(
            "SELECT original_qty, broker_executed_qty, requested_qty, status "
            "FROM positions WHERE position_id='pos_norm'"
        ).fetchone()
    assert tuple(row) == (8.38, 8.38, 8.4, "OPEN")


def _reconcile(database, positions):
    return reconcile_position_rows(
        database, bot_instance_id=BOT, broker_account_id=ACCOUNT, broker_positions=positions,
        position_mode="ONE_WAY", spec_resolver=lambda _s: None, run_id=RUN_ID, cycle_id="c",
        execution_mode="broker", broker_environment="demo",
    )


def test_reconciliation_after_normal_registration_is_idempotent(db):
    _open(db, SYMBOL, qty=8.38, price=100.1, pid="pos_norm", requested=8.4)
    broker = [BrokerPosition(SYMBOL, "LONG", Decimal("8.38"), Decimal("100.1"),
                             Decimal(str(LEV)), "cross", "ONE_WAY")]
    first, second = _reconcile(db, broker), _reconcile(db, broker)
    assert first["changed"] == 0 and second["changed"] == 0
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT position_id FROM positions WHERE symbol=? AND status='OPEN'", (SYMBOL,)
        ).fetchall()
    assert [r[0] for r in rows] == ["pos_norm"]  # no duplicate position


def test_reconciliation_repairs_a_crash_between_fill_and_registration(db):
    broker = [BrokerPosition(SYMBOL, "SHORT", Decimal("5"), Decimal("100"),
                             Decimal(str(LEV)), "cross", "ONE_WAY")]
    first = _reconcile(db, broker)
    assert [c["reason"] for c in first["changes"]] == ["BROKER_POSITION_DISCOVERED"]
    assert _reconcile(db, broker)["changed"] == 0


def test_the_runner_registers_the_lifecycle_with_the_broker_executed_quantity():
    from app.runner import runner as runner_module

    source = Path(runner_module.__file__).read_text(encoding="utf-8")
    start = source.index("_broker_filled_qty = (")
    assert '_execution_details.get("filled_qty")' in source[start:start + 200]
    assert "qty=_executed_qty," in source  # PositionManager.open_position


def test_fill_resolution_tests_do_not_write_the_canonical_database(db):
    assert getattr(db, "path", ":memory:") == ":memory:"
