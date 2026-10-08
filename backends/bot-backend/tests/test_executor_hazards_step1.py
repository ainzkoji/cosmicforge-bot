"""Step 1.0e -- the three audited executor / runtime hazards.

1. The flip branch (reverse the open side) closed and re-entered in one tick
   without confirming the close: it is now refused before any broker call.
2. "Protection failed and the rollback close failed" raised
   FatalIntegrationError ("Halting system"; the legacy runner answered with
   sys.exit): it is now a reported NAKED-position outcome with the order
   identity, alerted, and reconciled by the boundary like any unknown outcome.
3. The runtime rebuilt the broker client on ANY exception, including local
   gates (lease, shutdown, policy) that say nothing about the client: only
   doubt about the broker rebuilds now.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from test_entry_protection_never_again import _make_entry_order, _make_live_executor, temp_db  # noqa: F401
from app.core.broker_capability_gate import BrokerCapabilityGateError
from app.execution.entry_protection import EntryProtection
from app.execution.production_close import NAKED_POSITION_OPERATOR_REQUIRED
from app.trading_intelligence.contracts.execution import ExecutionAttemptStatus as X
from app.trading_intelligence.execution.binance_adapter import BinanceExecutionAdapter
from app.trading_intelligence.integration import production_runtime as runtime


def _client(position_amt):
    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": position_amt}
    client.account.return_value = {"availableBalance": "1000.0", "totalWalletBalance": "1000.0",
                                   "totalMaintMargin": "0.0", "totalInitialMargin": "0.0"}
    client.get_prices.return_value = {"BTCUSDT": 50_000.0}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, int(1_700_000_000_000)]]
    return client


# ── 1. flip ──────────────────────────────────────────────────────────────────

def test_a_reversal_is_refused_without_a_single_broker_call(temp_db):
    _, db = temp_db
    client = _client("-0.001")                              # SHORT open; a BUY would be a flip
    executor = _make_live_executor(db, client)
    executor._size_qty = MagicMock(return_value=(0.001, {"price": 50_000.0, "leverage": 1}))
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        result = executor.execute_signal("BTCUSDT", "BUY", 50.0, sl_price=49_000.0, tp_price=53_000.0, current_equity=1_000.0)
    assert result.status == "FLIP_REFUSED" and result.success is False
    assert result.details["reason_code"] == "POSITION_REVERSAL_REQUIRES_EXPLICIT_CLOSE"
    for name in ("cancel_all_orders", "close_position_market", "close_position", "place_order", "place_protection"):
        getattr(client, name).assert_not_called()
    # The boundary reads it as a rejection of THIS plan: nothing exists at the venue.
    assert BinanceExecutionAdapter.translate(result).status == X.REJECTED.value


def test_an_explicit_close_still_works(temp_db):
    _, db = temp_db
    client = _client("-0.001")
    client.close_position_market.return_value = {"orderId": "CLOSE-1", "status": "FILLED", "executedQty": "0.001",
                                                 "avgPrice": "50000", "updateTime": 1700000000000}
    executor = _make_live_executor(db, client)
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        result = executor.execute_signal("BTCUSDT", "CLOSE", 0.0, current_equity=1_000.0)
    assert result.status == "CLOSED_POSITION" and result.success
    client.close_position_market.assert_called_once()


# ── 2. protection failed, rollback failed ───────────────────────────────────

def _alerts(db):
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, alert_type TEXT,"
                  " severity TEXT, trace_id TEXT, symbol TEXT, message TEXT, details_json TEXT, acknowledged INTEGER DEFAULT 0)")
        return [tuple(r) for r in c.execute("SELECT alert_type, severity, symbol FROM alerts")]


def test_a_naked_entry_is_reported_and_alerted_not_raised(temp_db):
    _, db = temp_db
    assert _alerts(db) == []
    client = _client("0.0")
    client.place_order.return_value = _make_entry_order(order_id="ENTRY-NAKED")
    client.place_protection.side_effect = TimeoutError("protection read timed out")
    client.close_position_market.side_effect = RuntimeError("Binance HTTP 503: service unavailable")
    executor = _make_live_executor(db, client)
    executor._broker_account_id = "acct-naked"
    executor._size_qty = MagicMock(return_value=(0.001, {"price": 50_000.0, "leverage": 1}))
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        result = executor.execute_signal("BTCUSDT", "BUY", 50.0, sl_price=49_000.0, tp_price=53_000.0, current_equity=1_000.0)
    assert result.status == "PROTECTION_FAILED_CLOSE_FAILED" and result.success is False
    assert result.order_id == "ENTRY-NAKED"
    assert result.details["reason_code"] == "ENTRY_FILLED_PROTECTION_AND_ROLLBACK_FAILED"
    assert result.details["protect_error"].startswith("protection read") and "503" in result.details["close_error"]
    client.close_position_market.assert_called_once()        # one rollback attempt, no retry storm
    assert _alerts(db) == [(NAKED_POSITION_OPERATOR_REQUIRED, "CRITICAL", "BTCUSDT")]
    # The entry record is kept: the position exists and is reconciled, not forgotten.
    assert EntryProtection(db).get_entry(executor.bot_instance_id, "BTCUSDT", "LONG") is not None
    # The boundary treats it as an unknown outcome with a known order identity:
    # recover_pending reads broker truth and verifies protection (Step 1.0a).
    translated = BinanceExecutionAdapter.translate(result)
    assert translated.status == X.SUBMIT_UNKNOWN.value and translated.broker_order_id == "ENTRY-NAKED"


def test_a_successful_rollback_keeps_its_existing_outcome(temp_db):
    _, db = temp_db
    client = _client("0.0")
    client.place_order.return_value = _make_entry_order(order_id="ENTRY-ROLLED")
    client.place_protection.side_effect = RuntimeError('Binance HTTP 400: {"code":-2021,"msg":"Order would immediately trigger."}')
    client.close_position_market.return_value = {"orderId": "CLOSE-2", "status": "FILLED"}
    executor = _make_live_executor(db, client)
    executor._size_qty = MagicMock(return_value=(0.001, {"price": 50_000.0, "leverage": 1}))
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        result = executor.execute_signal("BTCUSDT", "BUY", 50.0, sl_price=49_000.0, tp_price=53_000.0, current_equity=1_000.0)
    assert result.status == "PROTECTION_FAILED_ENTRY_CLOSED"
    assert BinanceExecutionAdapter.translate(result).status == X.PROTECTION_FAILED_ROLLED_BACK.value


# ── 3. client rebuild only on broker doubt ──────────────────────────────────

@pytest.mark.parametrize("exc,doubt", [
    (ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED"), False),
    (ValueError("RUNTIME_SHUTDOWN_IN_PROGRESS"), False),
    (ValueError("PRODUCTION_REJECTS_PAPER_BOT"), False),
    (ValueError("PROTECTION_STATE_UNKNOWN"), False),
    (BrokerCapabilityGateError("BROKER_EXECUTION_CAPABILITY_INCOMPLETE", "missing permission"), False),
    (ValueError("BROKER_READ_SHAPE_INVALID"), True),
    (ValueError("BROKER_INCOME_HISTORY_UNAVAILABLE"), True),
    (TimeoutError("read timed out"), True),
    (RuntimeError("Binance HTTP 503: {}"), True),
    (ValueError("some other text"), True),
])
def test_only_doubt_about_the_broker_rebuilds_the_client(exc, doubt):
    assert runtime.client_doubt(exc) is doubt


def _auth():
    return SimpleNamespace(account_id="acct", user_id="u", broker_type="binance", environment="demo",
                           base_url="https://demo-fapi.binance.com", credential_version=1, key_fingerprint="fp")


@pytest.fixture
def cycle_db(tmp_path):
    from shared_lib.persistence.db import DB
    from app.execution import production_schema
    production_schema.forget()
    db = DB(str(tmp_path / "cycle.db"))
    runtime.initialize(db)
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS bot_instances (id TEXT PRIMARY KEY, broker_account_id TEXT, user_id TEXT, status TEXT)")
    runtime._clients.clear()
    yield db
    runtime._clients.clear()
    production_schema.forget()


def test_a_local_gate_keeps_the_cached_client_and_a_broker_failure_rebuilds_it(cycle_db, monkeypatch):
    from app.activation import account_status
    from app.trading_intelligence.integration import production_execution
    built = []

    def factory(auth):
        client = Mock()
        client.account.return_value = {"totalWalletBalance": "1000", "totalMarginBalance": "1000", "availableBalance": "1000",
                                       "totalInitialMargin": "0", "totalUnrealizedProfit": "0"}
        client.position_risk.return_value = []
        client.open_orders.return_value = []
        built.append(client)
        return client
    account = {"id": "acct", "user_id": "u", "broker_id": "binance", "environment": "DEMO"}
    # Only the production factory's clients are cached: stand in for it.
    monkeypatch.setattr(runtime, "_DEFAULT_CLIENT_FACTORY", factory)
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: _auth())
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "SYNCED"})
    gate = Mock(side_effect=ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED"))
    monkeypatch.setattr(production_execution, "process_account", gate)
    runtime.sync_account(cycle_db, account, factory=factory, execute=True)
    runtime.sync_account(cycle_db, account, factory=factory, execute=True)
    assert len(built) == 1 and runtime._clients["acct"][1] is built[0]      # the gate did not cost a rebuild
    with cycle_db.connect() as c:
        import json
        state = json.loads(c.execute("SELECT document FROM cati_production_state").fetchone()[0])
    assert state["execution"]["reason"] == "CANONICAL_RUNTIME_LEASE_REQUIRED"
    built[0].position_risk.side_effect = TimeoutError("read timed out")
    with pytest.raises(TimeoutError):
        runtime.sync_account(cycle_db, account, factory=factory, execute=True)
    assert "acct" not in runtime._clients                                     # broker doubt: rebuilt next cycle
    runtime.sync_account(cycle_db, account, factory=factory, execute=True)
    assert len(built) == 2
