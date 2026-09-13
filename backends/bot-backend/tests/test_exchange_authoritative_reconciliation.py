from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.evidence.fill_bridge import project_fill
from app.execution.executor import BinanceExecutor
from app.execution.position_reconciliation import (
    BrokerPosition,
    parse_broker_positions,
    quantity_close_reason,
    reconcile_position_rows,
)
from app.risk.capital_ledger import CapitalLedger
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import ensure_evidence_schema


BOT = "bot_reconcile"
ACCOUNT = "brk_reconcile"


@pytest.fixture()
def db(tmp_path):
    value = DB(str(tmp_path / "exchange-reconcile.db"))
    ensure_evidence_schema(value)
    return value


@pytest.fixture()
def spec():
    return SimpleNamespace(
        step_size=Decimal("0.0001"), min_qty=Decimal("0.0001"),
        min_notional=Decimal("50"), qty_precision=4,
    )


def _open(db, pid, side, qty, *, margin=120.0, price=77_000.0):
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO positions
               (position_id,bot_instance_id,broker_account_id,symbol,side,
                execution_mode,broker_environment,provenance,original_qty,
                remaining_qty,realized_qty,entry_price,status,opened_at,updated_at,
                leverage,committed_margin,requested_qty,broker_executed_qty)
               VALUES (?,?,?,?,?,'broker','demo','TESTNET',?,?,?,?, 'OPEN',
                       '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',10,?,?,?)""",
            (pid, BOT, ACCOUNT, "BTCUSDT", side, qty, qty, 0.0, price,
             margin, qty, qty),
        )


def _broker(side="SHORT", qty="0.0155", price="77178", mode="ONE_WAY"):
    return BrokerPosition("BTCUSDT", side, Decimal(qty), Decimal(price), Decimal("10"), "cross", mode)


def _reconcile(db, spec, positions, mode="ONE_WAY"):
    return reconcile_position_rows(
        db, bot_instance_id=BOT, broker_account_id=ACCOUNT,
        broker_positions=positions, position_mode=mode,
        spec_resolver=lambda _symbol: spec, run_id="run_1", cycle_id="cycle_1",
        execution_mode="broker", broker_environment="demo",
    )


def _row(db, pid):
    with db.connect() as conn:
        return dict(conn.execute("SELECT * FROM positions WHERE position_id=?", (pid,)).fetchone())


@pytest.mark.parametrize(
    ("qty", "broker_qty", "expected"),
    [
        ("0", None, "FILLED_TO_ZERO"),
        ("0.000013978094262930355", None, "UNTRADEABLE_DUST"),
        ("0.0001", None, None),
        ("0.0002", None, None),
        ("0.0155", "0", "BROKER_FLAT"),
    ],
)
def test_quantity_close_semantics(spec, qty, broker_qty, expected):
    assert quantity_close_reason(qty, spec, broker_quantity=broker_qty) == expected


def test_exact_full_close(db, spec):
    _open(db, "p", "LONG", 0.0155)
    runner = SimpleNamespace(
        context=SimpleNamespace(bot_instance_id=BOT, user_id="u", broker_account_id=ACCOUNT, broker_environment="demo"),
        run_id="run", cycle_id="cycle", _effective_execution_mode=lambda: "broker",
    )
    project_fill(runner, db, {"position_id": "p", "symbol": "BTCUSDT", "side": "LONG", "action": "CLOSE", "qty": 0.0155, "price": 77_100})
    assert _row(db, "p")["status"] == "CLOSED"
    assert _row(db, "p")["committed_margin"] == 0


def test_partial_close_preserves_executable_remainder(db):
    _open(db, "p", "LONG", 0.0155)
    runner = SimpleNamespace(
        context=SimpleNamespace(bot_instance_id=BOT, user_id="u", broker_account_id=ACCOUNT, broker_environment="demo"),
        run_id="run", cycle_id="cycle", _effective_execution_mode=lambda: "broker",
    )
    project_fill(runner, db, {"position_id": "p", "symbol": "BTCUSDT", "side": "LONG", "action": "PARTIAL_CLOSE", "qty": 0.005, "price": 77_100})
    assert _row(db, "p")["remaining_qty"] == pytest.approx(0.0105)
    assert _row(db, "p")["status"] == "OPEN"


def test_untradeable_dust_after_partial_close(db, spec, monkeypatch):
    from app.exchange.registry import get_instrument_registry
    monkeypatch.setitem(get_instrument_registry()._cache, "binance", {"BTCUSDT": spec})
    _open(db, "p", "LONG", 0.01551397809426293, margin=120.0)
    runner = SimpleNamespace(
        context=SimpleNamespace(bot_instance_id=BOT, user_id="u", broker_account_id=ACCOUNT, broker_environment="demo"),
        run_id="run", cycle_id="cycle", _effective_execution_mode=lambda: "broker",
    )
    project_fill(runner, db, {"position_id": "p", "symbol": "BTCUSDT", "side": "LONG", "action": "CLOSE", "qty": 0.0155, "price": 77_284})
    row = _row(db, "p")
    assert (row["status"], row["remaining_qty"], row["committed_margin"], row["close_reason"]) == (
        "CLOSED", 0.0, 0.0, "UNTRADEABLE_DUST"
    )


def test_broker_short_closes_stale_long_and_corrects_short(db, spec):
    _open(db, "long", "LONG", 0.000013978094262930355, margin=0.10812)
    _open(db, "short", "SHORT", 0.015547626266159815)
    result = _reconcile(db, spec, [_broker()])
    assert result["changed"] == 2
    assert _row(db, "long")["status"] == "CLOSED"
    short = _row(db, "short")
    assert short["remaining_qty"] == 0.0155
    assert short["original_qty"] == 0.0155
    assert short["requested_qty"] == pytest.approx(0.015547626266159815)
    assert short["committed_margin"] == pytest.approx(119.6259)


def test_broker_long_closes_stale_short(db, spec):
    _open(db, "short", "SHORT", 0.0155)
    _open(db, "long", "LONG", 0.0155)
    _reconcile(db, spec, [_broker("LONG")])
    assert _row(db, "short")["status"] == "CLOSED"
    assert _row(db, "long")["status"] == "OPEN"


@pytest.mark.parametrize(("old", "new"), [("LONG", "SHORT"), ("SHORT", "LONG")])
def test_one_way_flip_reconciles_to_net_side(db, spec, old, new):
    _open(db, "old", old, 0.01)
    _reconcile(db, spec, [_broker(new, "0.02")])
    assert _row(db, "old")["status"] == "CLOSED"
    with db.connect() as conn:
        rows = conn.execute("SELECT side,remaining_qty FROM positions WHERE status='OPEN'").fetchall()
    assert [(r["side"], r["remaining_qty"]) for r in rows] == [(new, 0.02)]


def test_hedge_mode_preserves_legitimate_opposite_sides(db, spec):
    _open(db, "long", "LONG", 0.01, margin=77)
    _open(db, "short", "SHORT", 0.02, margin=154)
    _reconcile(db, spec, [_broker("LONG", "0.01", mode="HEDGE"), _broker("SHORT", "0.02", mode="HEDGE")], "HEDGE")
    assert _row(db, "long")["status"] == "OPEN"
    assert _row(db, "short")["status"] == "OPEN"


def test_broker_flat_closes_local_dust(db, spec):
    _open(db, "dust", "LONG", 0.000013978094262930355, margin=0.10812)
    _reconcile(db, spec, [])
    row = _row(db, "dust")
    assert row["status"] == "CLOSED"
    assert row["close_reason"] == "ROUNDING_RESIDUAL_RECONCILED"


def test_local_quantity_entry_and_margin_corrected_from_broker(db, spec):
    _open(db, "short", "SHORT", 0.015547626266159815, price=77182.2)
    _reconcile(db, spec, [_broker()])
    row = _row(db, "short")
    assert row["broker_executed_qty"] == 0.0155
    assert row["entry_price"] == 77178
    assert row["leverage"] == 10
    assert row["committed_margin"] == pytest.approx(0.0155 * 77178 / 10)


def test_capital_ledger_ignores_reconciled_closed_position(db, spec):
    _open(db, "dust", "LONG", 0.000013978094262930355, margin=0.10812)
    _open(db, "short", "SHORT", 0.0155, margin=120)
    _reconcile(db, spec, [_broker()])
    ledger = CapitalLedger(db, bot_instance_id=BOT, capital_budget=120)
    assert ledger.open_positions() == 1
    assert ledger.committed_margin() <= 120


def test_reconciliation_is_idempotent_and_does_not_create_fills(db, spec):
    _open(db, "short", "SHORT", 0.0155, margin=119.6259, price=77178)
    first = _reconcile(db, spec, [_broker()])
    with db.connect() as conn:
        event_count = conn.execute("SELECT COUNT(*) FROM reconciliation_events").fetchone()[0]
        fill_count = conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()[0]
    second = _reconcile(db, spec, [_broker()])
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM reconciliation_events").fetchone()[0] == event_count
        assert conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()[0] == fill_count
    assert first["changed"] == second["changed"] == 0


def test_restart_discovers_open_broker_position_once(db, spec):
    first = _reconcile(db, spec, [_broker()])
    second = _reconcile(db, spec, [_broker()])
    assert first["changed"] == 1
    assert second["changed"] == 0
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM positions WHERE status='OPEN'").fetchone()[0] == 1


def test_multiple_partial_broker_fills_sum_to_executed_quantity():
    rows = [
        {"positionAmt": "-0.0155", "symbol": "BTCUSDT", "entryPrice": "77178", "leverage": "10", "positionSide": "BOTH"}
    ]
    parsed = parse_broker_positions(rows, hedge_mode=False)
    trade_fills = [Decimal("0.0010"), Decimal("0.0145")]
    assert sum(trade_fills) == parsed[0].quantity == Decimal("0.0155")


def test_unified_order_normalization_uses_executed_not_requested_quantity():
    executor = object.__new__(BinanceExecutor)
    executor.__dict__["_client_raw"] = SimpleNamespace()
    normalized = executor._normalize_order({
        "broker_order_id": "42", "client_order_id": "client-42",
        "qty_filled": Decimal("0.0155"), "avg_fill_price": Decimal("77178"),
        "status": SimpleNamespace(value="filled"), "timestamp": 123,
    }, "BTCUSDT", "SELL", "MARKET", 0.015547626266159815)
    assert normalized["quantity"] == pytest.approx(0.015547626266159815)
    assert normalized["executed_qty"] == 0.0155
    assert normalized["avg_price"] == 77178
    assert normalized["client_order_id"] == "client-42"


def test_reconciliation_preserves_historical_fill_rows(db, spec):
    _open(db, "dust", "LONG", 0.000013978094262930355, margin=0.10812)
    with db.connect() as conn:
        conn.execute("INSERT INTO trade_fills (symbol,side,action,qty,price,timestamp_utc) VALUES ('BTCUSDT','LONG','OPEN',0.0155,77349.6,'2026-01-01T00:00:00Z')")
    _reconcile(db, spec, [])
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM position_events WHERE position_id='dust'").fetchone()[0] == 1


def test_broker_position_parser_proves_one_way_net_side():
    parsed = parse_broker_positions([{
        "symbol": "BTCUSDT", "positionAmt": "-0.0155", "positionSide": "BOTH",
        "entryPrice": "77178", "leverage": "10", "marginType": "cross",
    }], hedge_mode=False)
    assert [(p.side, p.quantity, p.position_mode) for p in parsed] == [
        ("SHORT", Decimal("0.0155"), "ONE_WAY")
    ]
