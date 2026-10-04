"""Synthetic lifecycle/risk/restart tests; no production orders or holdout."""
import json
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.cati_schema import ensure_cati_schema
from app.trading_intelligence.integration import residual_prospective as obs
from app.trading_intelligence.integration import residual_simulation as sim

INSTRUMENT = {"status": "TRADING", "stepSize": ".001", "minQty": ".001", "maxQty": "10000", "minNotional": "5"}


@pytest.fixture
def setup(tmp_path):
    db = DB(str(tmp_path / "sim.db"))
    ensure_cati_schema(db)
    tracker = obs.Tracker(db, now_ms=1000 * obs.H + 20)
    book = sim.Book(db)
    book.activate("owner", 1000 * obs.H + 20)
    return tracker, book


def decision(tracker, offset=0, side="LONG", risk=10, atr=4):
    sign = 1 if side == "LONG" else -1
    t = tracker.state()["first_decision_time"] + offset * obs.H
    snapshot = {"reason": "SELECTED_TOP1", "eligible_universe": ["STRKUSDT"],
                "candidate": {"symbol": "STRKUSDT", "side": side, "score": sign * 3,
                              "entry_reference": 100., "risk": risk, "stop": 100 - sign * risk,
                              "target": 100 + sign * 2.5 * risk, "atr14": atr}}
    identity, _ = tracker.commit_decision(t, snapshot, t + 100)
    with tracker.db.connect() as c:
        row = dict(c.execute("SELECT * FROM cati_residual_decisions WHERE decision_id=?", (identity,)).fetchone())
    return row


def open_position(tracker, book, offset=0, side="LONG"):
    row = decision(tracker, offset, side)
    book.consider(row, [row["decision_time"] + 1, 100], row["recorded_at"] + 1, INSTRUMENT)
    return book.status()["positions"][0]


def test_atomic_model_entry_orders_fills_and_restart(setup):
    tracker, book = setup
    p = open_position(tracker, book)
    state = book.status()
    assert state["execution"] == "SIMULATED/TEST"
    assert state["real_money"] == state["real_broker_live_orders"] == "DISABLED"
    assert len(state["orders"]) == 4 and len(state["fills"]) == 1
    assert state["risk"]["portfolio_position_limit"] == 1
    assert state["equity"] < 10000
    assert p["quantity"] * (p["risk"] + .2175) <= 25
    restored = sim.Book(book.db)
    restored.activate("different-owner", 1005 * obs.H, 999999)
    assert restored.status()["owner_user_id"] == "owner"
    row = decision(tracker)
    restored.consider(row, [row["decision_time"] + 1, 100], row["recorded_at"] + 2, INSTRUMENT)
    assert len(restored.status()["fills"]) == 1
    overlap = decision(tracker, 1)
    restored.consider(overlap, [overlap["decision_time"] + 1, 100], overlap["recorded_at"] + 1, INSTRUMENT)
    assert restored.status()["decision_reasons"]["PORTFOLIO_ONE_POSITION_OVERLAP"] == 1
    assert restored.status()["positions"][0]["id"] == p["id"]


@pytest.mark.parametrize("side,outcome,bar", [
    ("LONG", "TARGET", [100, 126, 99, 124]),
    ("LONG", "STOP", [85, 126, 80, 110]),
    ("SHORT", "TARGET", [100, 101, 74, 80]),
    ("SHORT", "STOP", [115, 120, 74, 90]),
])
def test_frozen_exits_costs_oco_and_cash_reconcile(setup, side, outcome, bar):
    tracker, book = setup
    p = open_position(tracker, book, side=side)
    book.observe_bar(p["id"], [p["entry_time"], *bar, 1], p["entry_time"] + obs.Q + 1)
    state = book.status()
    closed = state["positions"][0]
    assert closed["outcome"] == outcome
    assert closed["exit_price"] == (75 if side == "SHORT" else 125) if outcome == "TARGET" else closed["exit_price"] == (115 if side == "SHORT" else 85)
    assert len(state["fills"]) == 2
    assert sum(o["status"] == "FILLED" for o in state["orders"]) == 2
    assert sum(o["status"] == "CANCELED" for o in state["orders"]) == 2
    parts = obs.cost_parts(100, closed["exit_price"], 10, book.rates)
    assert closed["cost_R"] == pytest.approx(sum(parts.values()))
    assert closed["costs"]["funding_buffer"] == pytest.approx(p["quantity"] * 100 * .0006)
    assert state["equity"] == pytest.approx(10000 + state["pnl"]["total_net"])
    assert state["pnl"]["total_net"] == pytest.approx(closed["realized_net_pnl"])
    book.observe_bar(p["id"], [p["entry_time"], *bar, 1], p["entry_time"] + obs.Q + 2)
    assert len(book.status()["fills"]) == 2


def test_full_timeout_incremental_restore_and_gap_guard(setup):
    tracker, book = setup
    p = open_position(tracker, book)
    with pytest.raises(ValueError, match="GAP"):
        book.observe_bar(p["id"], [p["entry_time"] + obs.Q, 100, 101, 99, 100, 1], p["entry_time"] + 2 * obs.Q + 1)
    for i in range(192):
        if i == 100:
            book = sim.Book(book.db)
        t = p["entry_time"] + i * obs.Q
        book.observe_bar(p["id"], [t, 100, 101, 99, 100, 1], t + obs.Q + 1)
        if i < 191:
            assert book.status()["open_positions"] == 1
    assert book.status()["positions"][0]["outcome"] == "TIMEOUT"


def test_no_pre_activation_or_expired_entries(setup):
    tracker, book = setup
    row = decision(tracker)
    with book.db.connect() as c:
        a = book.account(c)
        a["first_decision_time"] += obs.H
        book.save_account(c, a)
    book.consider(row, [row["decision_time"] + 1, 100], row["recorded_at"] + 1, INSTRUMENT)
    assert not book.status()["fills"]
    later = decision(tracker, 1)
    book.consider(later, [later["decision_time"] + 1, 100], later["decision_time"] + 1 + obs.Q, INSTRUMENT)
    assert book.status()["decision_reasons"] == {"MISSED_SIMULATION_ENTRY_WINDOW": 1}


@pytest.mark.parametrize("risk,atr,price,reason", [
    (20, 10, 100, "HARD_STOP_DISTANCE_LIMIT"),
    (10, 1, 100, "HARD_STOP_ATR_LIMIT"),
    (10, 4, 85, "NON_EXECUTABLE_GAP"),
])
def test_hard_risk_rejects_without_geometry_changes(setup, risk, atr, price, reason):
    tracker, book = setup
    row = decision(tracker, risk=risk, atr=atr)
    frozen_snapshot = row["snapshot_json"]
    book.consider(row, [row["decision_time"] + 1, price], row["recorded_at"] + 1, INSTRUMENT)
    assert not book.status()["positions"]
    assert book.status()["decision_reasons"] == {reason: 1}
    with book.db.connect() as c:
        assert c.execute("SELECT snapshot_json FROM cati_residual_decisions").fetchone()[0] == frozen_snapshot


def test_daily_cap_marks_unrealized_exit_costs_and_survives_restart(setup):
    tracker, book = setup
    p = open_position(tracker, book, side="SHORT")
    now = p["created_at"] + 10
    book.limits.max_daily_loss_pct = .99
    book.mark({p["symbol"]: (220, now)}, now)
    state = sim.Book(book.db).status()
    assert state["daily_halted"] and state["open_positions"] == 0
    assert state["positions"][0]["outcome"] == "RISK_HALT"
    assert state["risk"]["daily_cap_fraction"] == .025
    row = decision(tracker, 1)
    book.consider(row, [row["decision_time"] + 1, 100], row["recorded_at"] + 1, INSTRUMENT)
    assert len(book.status()["fills"]) == 2
    assert "DAILY_2_5_PERCENT_CAP" in book.status()["decision_reasons"]


def test_stale_quote_blocks_entries_and_daily_roll_keeps_weekly_halt(setup):
    tracker, book = setup
    p = open_position(tracker, book)
    now = p["created_at"] + 50000
    book.mark({p["symbol"]: (110, now - 40000)}, now)
    assert book.status()["status"] == "MARKET_DATA_STALE"
    assert book.status()["positions"][0]["mark"] == 100
    with book.db.connect() as c:
        a = book.account(c)
        a["daily_halted"] = True
        a["persistent_halt"] = "CONSECUTIVE_LOSSES"
        book.roll(a, now + 24 * obs.H)
        assert not a["daily_halted"] and a["persistent_halt"] == "CONSECUTIVE_LOSSES"


def test_owner_gate_and_get_only_public_client(setup, monkeypatch):
    _, book = setup
    market = Mock()
    monkeypatch.setattr(sim, "owner_current", lambda db: False)
    sim.tick(book, market)
    market.assert_not_called()
    assert not market.quotes.called and not market.candles.called
    client = sim.PublicMarket()
    client.candles.session = Mock()
    with pytest.raises(ValueError, match="FORBIDDEN"):
        client.get("/fapi/v1/order")
    assert not client.candles.session.get.called
    assert not client.candles.session.post.called
    assert not hasattr(book, "broker") and not hasattr(book, "executor")


def test_live_tick_uses_only_new_decision_and_open_not_partial_outcome(setup, monkeypatch):
    tracker, book = setup
    row = decision(tracker)
    now = row["recorded_at"] + 1
    monkeypatch.setattr(sim, "owner_current", lambda db: True)
    monkeypatch.setattr(sim.time, "time", lambda: now / 1000)
    market = SimpleNamespace(quotes=lambda: {"STRKUSDT": (101., now)},
                             instrument=lambda symbol: INSTRUMENT,
                             candles=lambda symbol, start, end, **kw: [[start, 100., 150., 50., 120., 1., start + obs.Q - 1]])
    sim.tick(book, market)
    assert book.status()["open_positions"] == 1
    assert len(book.status()["fills"]) == 1
    assert book.status()["positions"][0]["entry_price"] == 100
    sim.tick(book, market)
    assert book.status()["pnl"]["unrealized_gross"] > 0
    assert len(book.status()["fills"]) == 1
    now = row["decision_time"] + 1 + obs.Q + 1
    sim.tick(book, market)
    assert book.status()["positions"][0]["outcome"] == "STOP"
    assert len(book.status()["fills"]) == 2


def test_global_kill_blocks_entries_without_altering_existing_observation(setup):
    from app.trading_intelligence.governance.promotion import PromotionGovernance
    tracker, book = setup
    row = decision(tracker)
    PromotionGovernance(book.db).set_kill_switch(True, reason="synthetic risk test", actor_ref="test")
    book.consider(row, [row["decision_time"] + 1, 100], row["recorded_at"] + 1, INSTRUMENT)
    assert not book.status()["fills"]
    assert book.status()["decision_reasons"] == {"CATI_NEW_ENTRY_KILL_SWITCH": 1}
    assert tracker.pending()[0]["decision_id"] == row["decision_id"]


def test_api_auth_owner_scope_and_no_mutations(setup):
    from app.api import cati_simulation as api
    from app.core.auth import get_current_active_user
    _, book = setup
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[api.get_db] = lambda: book.db
    client = TestClient(app)
    assert client.get("/api/v1/cati/simulation/status").status_code == 401
    # Permission remains enforced independently of the active-user dependency.
    route = next(r for r in api.router.routes if r.path.endswith("/status"))
    permission = route.dependant.dependencies[-1].call
    app.dependency_overrides[permission] = lambda: "owner"
    app.dependency_overrides[get_current_active_user] = lambda: {"id": "outsider"}
    assert client.get("/api/v1/cati/simulation/status").status_code == 403
    app.dependency_overrides[get_current_active_user] = lambda: {"id": "owner"}
    assert client.get("/api/v1/cati/simulation/status").json()["real_money"] == "DISABLED"
    assert client.post("/api/v1/cati/simulation/status", json={"execution": "LIVE"}).status_code == 405
