"""Production configuration and broker read/write separation, isolated DBs only."""
from types import SimpleNamespace
from unittest.mock import Mock
import asyncio
import json
import sqlite3

import pytest
from pydantic import ValidationError
from app.core import config
from app.core.config import Settings
from shared_lib.core.production import LiveOrderSubmissionDisabled, require_live_account


@pytest.fixture(autouse=True)
def no_constructor_network(monkeypatch):
    response = Mock(status_code=200, content=b"{}", headers={}, text="{}")
    response.json.return_value = {"symbols": []}
    monkeypatch.setattr("requests.sessions.Session.get", Mock(return_value=response))


def production(**changes):
    values = dict(APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production", EXECUTION_MODE="live")
    values.update(changes)
    return Settings(_env_file=None, **values)


@pytest.mark.parametrize("key,value", [
    ("EXECUTION_MODE", "paper"), ("CATI_MODE", "OBSERVE"), ("DATABASE_ROLE", "paper"),
    ("BINANCE_ENV", "testnet"), ("BINANCE_FAPI_BASE_URL", "https://demo-fapi.binance.com"),
    ("BYBIT_BASE_URL", "https://api-testnet.bybit.com"), ("BINGX_ENV", "demo"),
    ("ML_ENABLED", True), ("LEGACY_V2_ENABLED", True), ("FALLBACK_ENGINE_ENABLED", True),
    ("PAPER_TRADING_MODE", True), ("TRADINGVIEW_TESTNET_ONLY", True),
    ("STRATEGY_NAME", "master_ensemble"), ("ADAPTIVE_DAILY_RISK_MAX_DAILY_LOSS_PCT", .026),
    ("ADAPTIVE_DAILY_RISK_ENABLED", False), ("RISK_ENGINE_ENABLED", False),
])
def test_contradictory_production_startup_fails(key, value):
    with pytest.raises(ValidationError, match="PRODUCTION_PROFILE_CONFLICT"):
        production(**{key: value})


def test_submission_permission_independent_of_live_mode():
    s = production()
    assert s.production and s.EXECUTION_MODE == "live"
    assert s.LIVE_ORDER_SUBMISSION_ENABLED is False
    assert s.validate_safety()[0]
    assert production(LIVE_ORDER_SUBMISSION_ENABLED=True).CATI_MODE == "LIVE"


@pytest.mark.parametrize("broker,path", [
    ("binance", "/fapi/v1/order"), ("bybit", "/v5/order/create"), ("bingx", "/openApi/swap/v2/trade/order"),
])
@pytest.mark.parametrize("method", ["POST", "DELETE", "PUT"])
def test_mutations_blocked_before_network(monkeypatch, broker, path, method):
    monkeypatch.setattr(config, "settings", production())
    if broker == "binance":
        from app.exchange.binance.client import BinanceFuturesClient
        c = BinanceFuturesClient("key", "secret", "https://fapi.binance.com")
        transport = c._signed_request
    elif broker == "bybit":
        from app.exchange.bybit.client import BybitClient
        c = BybitClient("key", "secret")
        transport = c._request_v5
    else:
        from app.exchange.bingx.client import BingXClient
        c = BingXClient("key", "secret")
        transport = c._request
    with pytest.raises(LiveOrderSubmissionDisabled):
        transport(method, path, {})


def test_signed_live_reads_remain_functional(monkeypatch):
    monkeypatch.setattr(config, "settings", production())
    from app.exchange.binance.client import BinanceFuturesClient
    c = BinanceFuturesClient("key", "secret", "https://fapi.binance.com")
    response = Mock(status_code=200, content=b"[]", headers={}, text="[]")
    response.json.return_value = [{"symbol": "BTCUSDT", "positionAmt": "0"}]
    c.session.get = Mock(return_value=response)
    rows = c._signed_get("/fapi/v2/positionRisk")
    assert rows[0]["symbol"] == "BTCUSDT"
    c.session.get.assert_called_once()
    assert c.session.get.call_args.args[0].startswith("https://fapi.binance.com/")


@pytest.mark.parametrize("broker", ["bybit", "bingx"])
def test_other_live_read_transports_remain_functional(monkeypatch, broker):
    monkeypatch.setattr(config, "settings", production())
    response = Mock(status_code=200, content=b"[]", headers={}, text="[]")
    response.json.return_value = {"retCode": 0, "result": {"list": []}} if broker == "bybit" else {"code": 0, "data": []}
    get = Mock(return_value=response)
    monkeypatch.setattr("requests.get", get)
    if broker == "bybit":
        from app.exchange.bybit.client import BybitClient
        BybitClient("key", "secret")._request_v5("GET", "/v5/position/list")
    else:
        from app.exchange.bingx.client import BingXClient
        BingXClient("key", "secret")._request("GET", "/openApi/swap/v2/user/positions")
    get.assert_called_once()


def test_demo_identity_and_endpoint_cannot_enter_production(monkeypatch):
    monkeypatch.setattr(config, "settings", production())
    with pytest.raises(ValueError, match="LIVE_BROKER_ACCOUNT"):
        require_live_account("demo")
    with pytest.raises(ValueError, match="CANONICAL_LIVE_ENDPOINT"):
        require_live_account("live", "binance", "https://testnet.binancefuture.com")
    from app.exchange.binance.client import BinanceFuturesClient
    with pytest.raises(ValueError, match="CANONICAL_LIVE_ENDPOINT"):
        BinanceFuturesClient("key", "secret", "https://demo-fapi.binance.com")


def test_production_does_not_construct_legacy_runner(monkeypatch):
    monkeypatch.setattr(config, "settings", production())
    from app.runner.multi_runner import MultiBotRunner
    runner = object.__new__(MultiBotRunner)
    runner.iteration = 0
    runner.service = Mock()
    asyncio.run(runner.run_once())
    runner.service.get_all_bot_instances.assert_not_called()


def test_production_health_and_status_report_loaded_profile(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "settings", production())
    state = main.runner_status()
    assert state["engine"] == "CATI" and state["legacy_instances_scheduled"] is False
    assert state["live_order_submission_enabled"] is False
    report = asyncio.run(main.health())
    assert report["production_configuration"]["APP/RUNTIME"] == "PRODUCTION"
    assert "cati_simulation" not in report and "cati_simulation_running" not in report["components"]


def test_production_cannot_start_simulation(monkeypatch):
    monkeypatch.setattr(config, "settings", production())
    from app.trading_intelligence.integration.residual_simulation import run
    with pytest.raises(ValueError, match="DISABLED_IN_PRODUCTION"):
        asyncio.run(run(Mock()))


@pytest.fixture
def db(tmp_path):
    class Store:
        path = str(tmp_path / "production.db")
        def connect(self):
            c = sqlite3.connect(self.path)
            c.row_factory = sqlite3.Row
            return c
    store = Store()
    with store.connect() as c:
        c.executescript("CREATE TABLE broker_accounts(id,user_id,broker_id,environment,status); CREATE TABLE bot_instances(id,broker_account_id,status);")
        c.executemany("INSERT INTO broker_accounts VALUES(?,?,?,?,?)", [
            ("live", "alice", "binance", "live", "connected"),
            ("demo", "alice", "binance", "demo", "connected"),
            ("other", "bob", "binance", "live", "connected")])
    return store


def test_production_status_is_owner_scoped_and_not_virtual(monkeypatch, db):
    from app.trading_intelligence.integration import production_runtime as runtime
    monkeypatch.setattr(runtime, "settings", production())
    runtime.initialize(db)
    runtime.save(db, "live", "alice", 1, {"status": "SYNCED", "balance": {"equity": 10}})
    state = runtime.status(db, user_id="alice")
    assert [r["account_id"] for r in state["accounts"]] == ["live"]
    assert state["accounts"][0]["status"] == "STALE"
    assert state["execution_permission"] == "BLOCKED"
    assert "cash" not in state and "fills" not in state
    with db.connect() as c:
        assert c.execute("SELECT environment FROM broker_accounts WHERE id='demo'").fetchone()[0] == "demo"


def test_first_sync_is_pending_without_creating_tables(monkeypatch, db):
    from app.trading_intelligence.integration import production_runtime as runtime
    monkeypatch.setattr(runtime, "settings", production())
    result = runtime.status(db, user_id="alice")
    assert result["accounts"] == [{"account_id": "live", "status": "AWAITING_FIRST_LIVE_SYNC"}]
    with db.connect() as c:
        assert not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_state'").fetchone()


def test_read_sync_persists_broker_snapshot_without_orders(monkeypatch, db):
    from app.trading_intelligence.integration import production_runtime as runtime
    from app.activation import account_status
    monkeypatch.setattr(runtime, "settings", production())
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: SimpleNamespace(environment="live"))
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "CURRENT"})
    c = Mock()
    c.get_balance.return_value = {"equity": 2000, "wallet": 2000}
    c.position_risk.return_value = []
    c.open_orders.return_value = []
    runtime.initialize(db)
    runtime.sync_account(db, {"id": "live", "user_id": "alice"}, factory=lambda auth: c)
    assert [call[0] for call in c.method_calls] == ["get_balance", "position_risk", "open_orders"]
    with db.connect() as conn:
        document = json.loads(conn.execute("SELECT document FROM cati_production_state").fetchone()[0])
    assert document["environment"] == "LIVE" and document["balance"]["equity"] == 2000
    assert document["risk"]["daily_hard_loss_fraction"] == .025
