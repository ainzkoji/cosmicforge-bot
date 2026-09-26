"""Section 7.11 refresh triggers: instrument-related venue API errors, account-mode changes, the
background refresh pass, throttling (no refresh storm), the capability-matrix generator's pagination
guard, and a stale catalog that stays fail-closed when its refresh fails. No network."""
from __future__ import annotations

import ast
import importlib.util
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
from shared_lib.persistence.db import DB

from app.activation import account_status as st
from app.exchange import catalog_refresh as cr
from app.exchange.instruments import InstrumentCatalog, parse_bybit_instrument

BACKEND = Path(__file__).resolve().parents[1]
REPO = BACKEND.parents[1]
H = 3_600_000


def _bybit(symbol, base, symbol_type=""):
    return {"symbol": symbol, "baseCoin": base, "quoteCoin": "USDT", "settleCoin": "USDT", "symbolType": symbol_type,
            "contractType": "LinearPerpetual", "status": "Trading", "launchTime": "1788858596000",
            "lotSizeFilter": {"maxOrderQty": "100", "minOrderQty": "0.1", "qtyStep": "0.1", "minNotionalValue": "5"},
            "priceFilter": {"tickSize": "0.01"}, "leverageFilter": {"maxLeverage": "50"}}


BTC = parse_bybit_instrument(_bybit("BTCUSDT", "BTC"))
EUR = parse_bybit_instrument(_bybit("EURUSDUSDT", "EURUSD", "forex"))
DEMO_AUTH = SimpleNamespace(broker_type="bybit", environment=BrokerEnvironment.DEMO)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    cr._reset_for_tests()
    monkeypatch.setattr(st, "_refresh_attempts", {})
    monkeypatch.setattr("app.ops.multi_asset_metrics.instrument_sync", lambda *a, **k: None)
    yield
    cr._reset_for_tests()


@pytest.fixture
def cat_db(tmp_path):
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    d = DB(path=str(tmp_path / "cat.db"))
    ensure_market_data_schema(d)
    return d


class _Client:
    def __init__(self, result=None, fail=False):
        self.calls, self.result, self.fail = 0, result or [BTC, EUR], fail

    def discover_instruments(self):
        self.calls += 1
        if self.fail:
            raise TimeoutError("venue down")
        return self.result


# ── classification ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("broker,err,cls", [
    ("binance", 'Binance HTTP 400: {"code":-1121,"msg":"Invalid symbol."}', "SYMBOL_UNKNOWN"),
    ("binance", 'Binance HTTP 400: {"code":-4140,"msg":"Invalid symbol status for opening position."}',
     "SYMBOL_NOT_TRADING"),
    ("binance", 'Binance HTTP 400: {"code":-4164,"msg":"Order\'s notional must be no smaller than 5"}',
     "MIN_NOTIONAL_FILTER"),
    ("bybit", "Bybit Order Failed: params error: symbol invalid", "SYMBOL_UNKNOWN"),
    ("bybit", "Bybit Order Failed: Order does not meet minimum order value 5USDT", "MIN_NOTIONAL_FILTER"),
    ("bingx", "BingX API Error: this symbol is not open for trading (Code 101204)", "SYMBOL_NOT_TRADING"),
    ("bingx", "BingX API Error: symbol does not exist (Code 109400)", "SYMBOL_UNKNOWN"),
])
def test_instrument_errors_are_classified(broker, err, cls):
    assert cr.classify_instrument_error(broker, err) == cls


@pytest.mark.parametrize("broker,err", [
    ("binance", 'Binance HTTP 400: {"code":-2019,"msg":"Margin is insufficient."}'),
    ("binance", 'Binance HTTP 400: {"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}'),
    ("binance", "Binance request failed after retries: POST /fapi/v1/order (Read timed out)"),
    ("bybit", "Bybit HTTP 401: API key is invalid."),
    ("bybit", "Bybit Order Failed: ab not enough for new order"),
    ("bingx", "BingX HTTP 429: Too many requests"),
    ("bingx", "BingX Network Error: Connection reset"),
])
def test_account_transport_and_rate_limit_errors_are_not_instrument_errors(broker, err):
    assert cr.classify_instrument_error(broker, err) is None
    assert cr.observe_instrument_error(broker, "DEMO", err) is None and cr.pending_all() == {}


# ── API-error-triggered refresh ────────────────────────────────────────────

def test_instrument_api_error_triggers_a_refresh_of_a_current_catalog(cat_db):
    from app.exchange.bybit.client import BybitClient

    now = int(time.time() * 1000)
    InstrumentCatalog(cat_db).upsert("bybit_linear", "DEMO", [BTC, EUR], now - H)  # current (< 6 h)
    assert st.refresh_if_stale(cat_db, DEMO_AUTH, client_factory=lambda a: _Client(), now_ms=now) is None
    client = BybitClient("k", "s", base_url=resolve_base_url("bybit", BrokerEnvironment.DEMO))
    assert cr.observe_client_error(client, RuntimeError("Bybit Order Failed: symbol invalid")) == "SYMBOL_UNKNOWN"
    assert cr.pending("bybit_linear", "DEMO")["trigger"] == cr.TRIGGER_INSTRUMENT_ERROR
    assert cr.pending("bybit_linear", "LIVE") is None  # the other environment is untouched
    fake = _Client()
    res = st.refresh_if_stale(cat_db, DEMO_AUTH, client_factory=lambda a: fake, now_ms=now + 10_000)
    assert res["status"] == "SYNCED" and fake.calls == 1 and cr.pending("bybit_linear", "DEMO") is None
    assert st.refresh_if_stale(cat_db, DEMO_AUTH, client_factory=lambda a: fake, now_ms=now + 20_000) is None


def test_an_unregistered_host_never_guesses_the_environment():
    from app.exchange.bybit.client import BybitClient

    assert cr.client_venue_environment(BybitClient("k", "s", base_url="https://proxy.example")) == ("bybit", None)
    assert cr.observe_client_error(BybitClient("k", "s", base_url="https://proxy.example"), "symbol invalid") is None
    assert cr.observe_client_error(SimpleNamespace(base_url="https://api.bybit.com"), "symbol invalid") is None
    assert cr.pending_all() == {}


def test_the_executor_order_rejection_path_reports_to_the_catalog():
    src = (BACKEND / "app/execution/executor.py").read_text(encoding="utf-8")
    handlers = [n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ExceptHandler) and n.name == "order_err"]
    assert handlers, "order_err handler moved"
    calls = {getattr(c.func, "id", getattr(c.func, "attr", "")) for h in handlers for c in ast.walk(h)
             if isinstance(c, ast.Call)}
    assert "observe_client_error" in calls


# ── account-mode-change refresh ────────────────────────────────────────────

def _bybit_adapter(status):
    from shared_lib.broker.resolver import BrokerAuth

    from app.transfers.adapters import BybitTransferAdapter

    auth = BrokerAuth("acc_m", "u", "bybit", BrokerEnvironment.DEMO,
                      resolve_base_url("bybit", BrokerEnvironment.DEMO), "k", "s", 1, "...")
    trading = SimpleNamespace(account_info=lambda: status)
    return BybitTransferAdapter(auth, build=lambda a: trading)


def test_account_mode_change_requests_a_refresh_and_nothing_else():
    classic = {"retCode": 0, "result": {"unifiedMarginStatus": 1}}
    uta = {"retCode": 0, "result": {"unifiedMarginStatus": 4}}
    assert _bybit_adapter(classic).account_mode() == "CLASSIC" and cr.pending_all() == {}  # first observation
    assert _bybit_adapter(classic).account_mode() == "CLASSIC" and cr.pending_all() == {}  # unchanged
    assert _bybit_adapter({"retCode": 10002}).account_mode() is None and cr.pending_all() == {}  # unreadable
    assert _bybit_adapter(uta).account_mode() == "UNIFIED"  # the returned mode is unaffected
    req = cr.pending("bybit_linear", "DEMO")
    assert req["trigger"] == cr.TRIGGER_ACCOUNT_MODE_CHANGED and req["detail"] == "CLASSIC->UNIFIED"
    assert cr.observe_account_mode("other_acc", "bybit", "DEMO", "UNIFIED") is False  # per-account history


# ── throttling / no refresh storm ──────────────────────────────────────────

def test_error_bursts_coalesce_and_refresh_attempts_are_throttled(cat_db):
    now = int(time.time() * 1000)
    InstrumentCatalog(cat_db).upsert("bybit_linear", "DEMO", [BTC, EUR], now - H)
    new = [cr.request_refresh("bybit", "DEMO", cr.TRIGGER_INSTRUMENT_ERROR, now_ms=now + i) for i in range(50)]
    assert new.count(True) == 1 and len(cr.pending_all()) == 1  # 50 errors -> one request
    down = _Client(fail=True)
    assert st.refresh_if_stale(cat_db, DEMO_AUTH, client_factory=lambda a: down, now_ms=now + 100)["status"] == "FAILED"
    for dt in (1_000, 60_000, st.REFRESH_MIN_INTERVAL_MS - 1):
        assert st.refresh_if_stale(cat_db, DEMO_AUTH, client_factory=lambda a: down,
                                   now_ms=now + 100 + dt)["status"] == "THROTTLED"
    assert down.calls == 1 and cr.pending("bybit_linear", "DEMO") is not None  # a failed sync keeps the request
    up = _Client()
    later = now + 100 + st.REFRESH_MIN_INTERVAL_MS
    assert st.refresh_if_stale(cat_db, DEMO_AUTH, client_factory=lambda a: up, now_ms=later)["status"] == "SYNCED"
    assert up.calls == 1 and cr.pending("bybit_linear", "DEMO") is None


def test_background_pass_refreshes_only_what_is_due(tmp_path, monkeypatch):
    from shared_lib.broker import BrokerResolverError
    from shared_lib.persistence.migrations import migrate

    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "bg.db").as_posix())
    d = DB()
    migrate(d)
    with d.connect() as c:
        for acc, user, broker, env, ts in (("a_bad", "u1", "bybit", "demo", "2026-09-26T02"),
                                           ("a_ok", "u2", "bybit", "demo", "2026-09-26T01"),
                                           ("a_oanda", "u3", "oanda", "demo", "2026-09-26T01")):
            c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,"
                      "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                      (acc, user, broker, "crypto", "t", "connected", env, ts, ts))
    resolved = []

    def resolver(acc, user, db):
        resolved.append(acc)
        if acc == "a_bad":
            raise BrokerResolverError(BrokerResolverError.REASON_REVOKED, "revoked")
        return SimpleNamespace(broker_type="bybit", environment=BrokerEnvironment.DEMO)

    fake = _Client()
    now = int(time.time() * 1000)
    first = st.refresh_due_catalogs(d, resolver=resolver, client_factory=lambda a: fake, now_ms=now)
    assert first == {"bybit_linear:DEMO": first["bybit_linear:DEMO"]} and first["bybit_linear:DEMO"]["status"] == "SYNCED"
    assert resolved == ["a_bad", "a_ok"] and fake.calls == 1  # unusable account skipped; oanda not a catalog venue
    resolved.clear()
    again = st.refresh_due_catalogs(d, resolver=resolver, client_factory=lambda a: fake, now_ms=now + 60_000)
    assert again["bybit_linear:DEMO"]["status"] == "CURRENT" and resolved == [] and fake.calls == 1  # no call
    cr.request_refresh("bybit", "DEMO", cr.TRIGGER_ACCOUNT_MODE_CHANGED, now_ms=now + 120_000)
    assert st.refresh_due_catalogs(d, resolver=resolver, client_factory=lambda a: fake,
                                   now_ms=now + 180_000)["bybit_linear:DEMO"]["status"] == "THROTTLED"
    assert st.refresh_due_catalogs(d, resolver=resolver, client_factory=lambda a: fake,
                                   now_ms=now + st.REFRESH_MIN_INTERVAL_MS + 1)["bybit_linear:DEMO"]["status"] == "SYNCED"
    assert fake.calls == 2


# ── stale catalog stays fail-closed; a pending request changes no decision ──

class _Svc:
    def __init__(self):
        self.store = SimpleNamespace(in_flight=lambda account_id: [])

    def _auth(self, user_id, account_id):
        return SimpleNamespace(broker_type="bybit", environment=BrokerEnvironment.DEMO, credential_version=1)

    def _evidence(self, *a):
        return None

    def _adapter_factory(self, auth):
        return None


def test_stale_catalog_remains_fail_closed_when_its_refresh_fails(cat_db):
    now = int(time.time() * 1000)
    InstrumentCatalog(cat_db).upsert("bybit_linear", "DEMO", [BTC, EUR], now - 7 * H)
    out = st.account_status(cat_db, user_id="u", account_id="a", service=_Svc(), refresh_stale=True,
                            client_factory=lambda a: _Client(fail=True))
    assert out["discovery"]["status"] == "STALE" and out["discovery"]["refresh"]["status"] == "FAILED"
    assert out["markets"]["CRYPTO"]["execution"]["reason"] == "DISCOVERY_STALE"
    assert out["capabilities"]["CRYPTO_EXECUTION"]["status"] == "DATA_NOT_READY"
    # throttled retry: still stale, still blocked
    again = st.account_status(cat_db, user_id="u", account_id="a", service=_Svc(), refresh_stale=True,
                              client_factory=lambda a: _Client())
    assert again["discovery"]["refresh"]["status"] == "THROTTLED"
    assert again["markets"]["FX"]["execution"]["reason"] == "DISCOVERY_STALE"


def test_a_pending_request_on_a_current_catalog_changes_no_capability_decision(cat_db):
    now = int(time.time() * 1000)
    InstrumentCatalog(cat_db).upsert("bybit_linear", "DEMO", [BTC, EUR], now - H)
    before = st.account_status(cat_db, user_id="u", account_id="a", service=_Svc())
    cr.request_refresh("bybit", "DEMO", cr.TRIGGER_INSTRUMENT_ERROR)
    after = st.account_status(cat_db, user_id="u", account_id="a", service=_Svc(), refresh_stale=True,
                              client_factory=lambda a: _Client(fail=True))
    assert after["discovery"]["refresh"]["status"] == "FAILED"
    assert after["markets"] == before["markets"] and after["capabilities"] == before["capabilities"]


# ── capability-matrix generator pagination guard ───────────────────────────

def _generator(monkeypatch, bybit_pages):
    spec = importlib.util.spec_from_file_location("generate_capability_matrix_t",
                                                  REPO / "scripts" / "generate_capability_matrix.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    import requests

    def get(url, params=None, timeout=None):
        if "binance" in url:
            body = {"symbols": []}
        elif "bingx" in url:
            body = {"data": []}
        else:
            body = bybit_pages((params or {}).get("cursor"))
        return SimpleNamespace(json=lambda: body)

    monkeypatch.setattr(requests, "get", get)
    return mod


def test_matrix_generator_reads_every_bybit_page(monkeypatch):
    pages = {None: {"retCode": 0, "result": {"list": [_bybit("BTCUSDT", "BTC")], "nextPageCursor": "p2"}},
             "p2": {"retCode": 0, "result": {"list": [_bybit("EURUSDUSDT", "EURUSD", "forex")],
                                             "nextPageCursor": ""}}}
    out = _generator(monkeypatch, lambda c: pages[c]).discover()
    assert {i.venue_symbol for i in out["bybit"]} == {"BTCUSDT", "EURUSDUSDT"}


def test_matrix_generator_refuses_a_non_terminating_pagination(monkeypatch):
    n = iter(range(10_000))
    mod = _generator(monkeypatch, lambda c: {"retCode": 0, "result": {"list": [], "nextPageCursor": f"c{next(n)}"}})
    with pytest.raises(RuntimeError, match="did not terminate"):
        mod.discover()


def test_matrix_generator_refuses_a_repeated_cursor_and_venue_errors(monkeypatch):
    mod = _generator(monkeypatch, lambda c: {"retCode": 0, "result": {"list": [_bybit("BTCUSDT", "BTC")],
                                                                      "nextPageCursor": "same"}})
    with pytest.raises(RuntimeError, match="repeated pagination cursor"):
        mod.discover()
    mod = _generator(monkeypatch, lambda c: {"retCode": 10006, "result": {}})
    with pytest.raises(RuntimeError, match="retCode=10006"):
        mod.discover()
