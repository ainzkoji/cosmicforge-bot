"""The customer dashboard read model: truthful environment, exact blockers, NO_SIGNAL, stop state, labels."""
import json
from types import SimpleNamespace

import pytest

from app.trading_intelligence.integration import customer_dashboard as CD
from shared_lib.persistence.db import DB

USER = "u1"


def engine_state(**over):
    state = {"account_id": "brk_demo", "broker": "BINANCE", "environment": "DEMO", "status": "SYNCED",
             "age_seconds": 12.0, "execution_permission": "WAITING_SIGNAL",
             "execution": {"execution_permission": "WAITING_SIGNAL", "reason": "PROSPECTIVE_ENTRY_WINDOW_EXPIRED",
                           "auto_trading": {"enabled": True}},
             "positions": [{"symbol": "ADAUSDT", "positionAmt": "0"},
                           {"symbol": "ETHUSDT", "positionAmt": "-0.5", "entryPrice": "3000", "markPrice": "2990",
                            "unRealizedProfit": "5", "leverage": "3", "liquidationPrice": "4000"},
                           {"symbol": "BTCUSDT", "positionAmt": "0.01", "entryPrice": "60000"}],
             "protection_uncertain": [{"symbol": "BTCUSDT"}]}
    state.update(over)
    return state


@pytest.fixture
def db(tmp_path, monkeypatch):
    database = DB(str(tmp_path / "dash.db"))
    with database.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS cati_production_protection (account_id TEXT, client_id TEXT, "
                  "document TEXT, response TEXT)")
        c.execute("INSERT INTO cati_production_protection VALUES (?,?,?,?)", ("brk_demo", "c1", json.dumps(
            {"type": "STOP_MARKET", "symbol": "ETHUSDT", "triggerPrice": "3100", "_requested_at": 5}), "{}"))
        c.execute("CREATE TABLE IF NOT EXISTS cati_production_fills (account_id TEXT, trade_id TEXT, order_id TEXT, "
                  "symbol TEXT, document TEXT)")
        c.executemany("INSERT INTO cati_production_fills VALUES (?,?,?,?,?)", [
            ("brk_demo", "t1", "o1", "ETHUSDT", json.dumps({"symbol": "ETHUSDT", "time": 10, "side": "SELL",
                                                            "_execution": {"trade_plan_id": "tp_x", "leg": "ENTRY"}})),
            ("brk_demo", "t2", "o2", "ADAUSDT", json.dumps({"symbol": "ADAUSDT", "time": 5, "side": "BUY",
                                                            "_execution": {"purpose": "DEMO_CERTIFICATION"}})),
        ])
    return database


def install(monkeypatch, state, *, signal=None, authority=None, experimental=None, plans=None):
    from app.trading_intelligence.integration import production_runtime as PR
    monkeypatch.setattr(PR, "status", lambda db, user_id=None: {
        "accounts": [state], "strategy": "RESIDUAL", "status": "EXECUTION_ACCOUNTS_PRESENT",
        "demo_order_submission_enabled": True, "live_order_submission_enabled": False})
    monkeypatch.setattr(CD, "_latest_signal", lambda db: signal or {"state": "NO_SIGNAL", "reason": None})
    monkeypatch.setattr(CD, "_strategy_authority", lambda db, s, e, sym: authority or
                        {"authority": "CERTIFIED", "blockers": [], "orders_possible": True})
    from app.trading_intelligence.governance import experimental_demo as X
    monkeypatch.setattr(X, "account_status", lambda db, a, now_ms=None: experimental)
    monkeypatch.setattr(X, "experimental_plans", lambda db, a: plans or {})


def test_a_demo_account_is_never_presented_as_real_money(db, monkeypatch):
    install(monkeypatch, engine_state())
    out = CD.customer_dashboard(db, user_id=USER)
    account = out["accounts"][0]
    assert account["environment"] == "DEMO" and account["real_money"] is False
    assert out["real_money_trading_possible"] is False
    assert "configuration" not in json.dumps(out)


def test_waiting_with_blockers_is_not_reported_as_ready(db, monkeypatch):
    blockers = [{"code": "FAMILY_FAILED_RESEARCH", "severity": "blocked", "message": "m", "action": "a"}]
    install(monkeypatch, engine_state(), authority={"authority": "NONE", "blockers": blockers,
                                                    "orders_possible": False})
    entry = CD.customer_dashboard(db, user_id=USER)["accounts"][0]["entry_permission"]
    assert entry["entries_allowed"] is False and entry["state"] == "BLOCKED_STRATEGY_AUTHORITY"
    assert entry["blockers"] == blockers and "blocked" in entry["summary"]


def test_ready_only_when_waiting_and_nothing_blocks(db, monkeypatch):
    install(monkeypatch, engine_state())
    entry = CD.customer_dashboard(db, user_id=USER)["accounts"][0]["entry_permission"]
    assert entry["entries_allowed"] is True and entry["state"] == "WAITING_SIGNAL"


def test_read_failure_shows_the_exact_reason_and_action(db, monkeypatch):
    install(monkeypatch, engine_state(status="READ_FAILED", reason="BROKER_CREDENTIALS_REJECTED",
                                      execution_permission="BLOCKED_ACCOUNT",
                                      execution={"execution_permission": "BLOCKED_ACCOUNT",
                                                 "reason": "BROKER_CREDENTIALS_REJECTED"}))
    account = CD.customer_dashboard(db, user_id=USER)["accounts"][0]
    assert account["connection"]["health"] == "ERROR"
    assert account["connection"]["problem"]["code"] == "BROKER_CREDENTIALS_REJECTED"
    assert "Reconnect the exchange" in account["connection"]["problem"]["action"]
    assert account["entry_permission"]["entries_allowed"] is False


def test_open_positions_carry_their_protective_stop_state(db, monkeypatch):
    install(monkeypatch, engine_state())
    positions = {p["symbol"]: p for p in CD.customer_dashboard(db, user_id=USER)["accounts"][0]["open_positions"]}
    assert set(positions) == {"ETHUSDT", "BTCUSDT"}                       # flat rows are not positions
    assert positions["ETHUSDT"]["side"] == "SHORT"
    assert positions["ETHUSDT"]["protective_stop"] == {"state": "PLACED", "trigger_price": "3100",
                                                        "requested_at_ms": 5}
    assert positions["BTCUSDT"]["protective_stop"]["state"] == "UNVERIFIED"


def test_trades_are_labelled_experimental_certification_or_strategy(db, monkeypatch):
    install(monkeypatch, engine_state(), plans={"tp_x": "xdemo_1"},
            experimental={"state": "ACTIVE", "authorization_id": "xdemo_1", "label": "EXPERIMENTAL_DEMO"})
    account = CD.customer_dashboard(db, user_id=USER)["accounts"][0]
    assert [t["label"] for t in account["recent_trades"]] == ["EXPERIMENTAL_DEMO", "VENUE_CERTIFICATION_TEST"]
    assert account["experimental_demo"]["state"] == "ACTIVE"
    assert account["entry_permission"].get("experimental_demo") is True


def test_no_signal_is_reported_as_no_signal(db, monkeypatch):
    install(monkeypatch, engine_state())
    monkeypatch.undo()
    from app.trading_intelligence.integration import production_execution as PE
    monkeypatch.setattr(PE, "latest_decision", lambda db: {"risk_state_json": json.dumps({"reason": "NO_SCORE_AT_LEAST_2"}),
                                                           "decision_time": 1})
    monkeypatch.setattr(PE, "eligibility", lambda row, now: "NO_SCORE_AT_LEAST_2")
    signal = CD._latest_signal(db)
    assert signal["state"] == "NO_SIGNAL" and signal["reason"]["code"] == "NO_SCORE_AT_LEAST_2"
    monkeypatch.setattr(PE, "latest_decision", lambda db: None)
    assert CD._latest_signal(db)["state"] == "NO_SIGNAL"
