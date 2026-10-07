"""Access-control regression tests for the bot-backend API surface.

Covers the audit findings fixed together:

* every registered route outside a short, explicit public allow-list carries
  an authentication dependency (the route walk below is the tripwire for any
  route added later without one);
* the broker draft store is owner-scoped and caller-supplied gateway / bridge
  addresses go through the outbound (SSRF) guard before the server connects;
* shadow-trade routes enforce bot ownership; system-wide views are admin-only;
* the SSE stream only delivers a subscriber their own events;
* ``/bot-instances/inventory`` and the TradingView operator status routes are
  admin-only;
* ``/debug/config`` masks secrets by rule;
* the TradingView webhook requires HMAC where a secret is configured and never
  persists the raw token.

No test here touches the network: destinations are IP literals, and every
broker client is replaced by a fake.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate


# ── helpers ─────────────────────────────────────────────────────────────────

def _token(user: str, role: str = "user") -> str:
    from jose import jwt

    from app.core.config import settings
    from app.core.security import AUDIENCE, ISSUER

    now = int(time.time())
    return jwt.encode(
        {"sub": user, "type": "access", "role": role, "iss": ISSUER, "aud": AUDIENCE, "iat": now, "exp": now + 600},
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


def _auth(user: str, role: str = "user") -> dict:
    return {"Authorization": f"Bearer {_token(user, role)}"}


ALICE = "hardening-alice"
BOB = "hardening-bob"
ADMIN = "hardening-admin"


@pytest.fixture(scope="module")
def api_client():
    from fastapi.testclient import TestClient

    from app.core.config import settings

    # app.main's startup guard compares settings.DATABASE_URL with DB().path;
    # settings may have been built while another test's temp DB was active.
    with patch.object(settings, "DATABASE_URL", "sqlite:///" + DB().path):
        from app.main import app
    return TestClient(app)  # no context manager: startup tasks do not run


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "hardening.db").as_posix())
    d = DB()
    migrate(d)
    return d


def _insert_row(db: DB, table: str, **values) -> None:
    """Insert a row, filling every other NOT NULL column with a neutral value."""
    with db.connect() as conn:
        for col in conn.execute(f"PRAGMA table_info({table})").fetchall():
            name, col_type, notnull, default = col[1], str(col[2] or "").upper(), col[3], col[4]
            if name in values or not notnull or default is not None:
                continue
            values[name] = 0 if ("INT" in col_type or "REAL" in col_type) else "x"
        columns = ", ".join(values)
        marks = ", ".join("?" for _ in values)
        conn.execute(f"INSERT INTO {table} ({columns}) VALUES ({marks})", tuple(values.values()))


# ── 1. every route is authenticated unless explicitly public ────────────────

# Intentionally unauthenticated application routes. Keep this list SHORT: an
# entry here is a decision, not a convenience.
PUBLIC_ROUTES = {
    "/": "service banner: no account data, no credentials",
    "/health": "liveness / readiness probe for uptime monitors",
    "/cati": "static monitor shell; every API call it makes is authenticated",
    "/api/v1/tradingview/webhook/{token_or_id}": "authenticated by the per-webhook token, not a user session",
}

# Served by FastAPI / Starlette itself (not application endpoints). The docs
# routes exist outside production only -- see test_docs_disabled_in_production.
FRAMEWORK_ROUTES = {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc", "/assets"}

# Known unauthenticated routes in modules outside this change's ownership.
# Tolerated here so the tripwire can land; each one is reported for a fix and
# must be removed from this set when it gains an auth dependency.
#
# Empty: /api/v1/forex/instruments (the last entry) now requires a user.
KNOWN_UNAUTHENTICATED_GAPS: dict[str, str] = {}

# Modules whose dependency callables authenticate the caller (they validate
# the JWT and raise 401/403). A new auth dependency belongs in one of these.
AUTH_MODULES = {"app.core.auth", "app.api.deps"}


def _dependency_calls(dependant) -> list:
    """Every dependency callable reachable from a route, recursively."""
    calls = []
    for dep in getattr(dependant, "dependencies", []) or []:
        calls.append(dep.call)
        calls.extend(_dependency_calls(dep))
    return calls


def _is_auth_callable(call) -> bool:
    return inspect.isfunction(call) and getattr(call, "__module__", None) in AUTH_MODULES


def _route_is_authenticated(route) -> bool:
    return any(_is_auth_callable(call) for call in _dependency_calls(route.dependant))


def test_every_route_requires_auth_unless_explicitly_public(api_client):
    """Tripwire: a route added without an auth dependency fails this test."""
    unauthenticated = []
    for route in api_client.app.routes:
        path = getattr(route, "path", "")
        if path in PUBLIC_ROUTES or path in FRAMEWORK_ROUTES or path in KNOWN_UNAUTHENTICATED_GAPS:
            continue
        if not hasattr(route, "dependant"):
            # A non-API route (mount, static file, raw Starlette route) that
            # is not in the framework allow-list is unexpected: fail closed.
            unauthenticated.append(f"{type(route).__name__} {path} (no dependency graph)")
            continue
        if not _route_is_authenticated(route):
            methods = ",".join(sorted(getattr(route, "methods", None) or []))
            unauthenticated.append(f"{methods} {path} -> {route.endpoint.__module__}.{route.endpoint.__name__}")
    assert not unauthenticated, (
        "Routes without an authentication dependency (add Depends(get_current_user_id / "
        "require_admin / ...) or, if truly public, list them in PUBLIC_ROUTES with a reason):\n  "
        + "\n  ".join(sorted(unauthenticated))
    )


def test_public_allow_list_is_short_and_not_stale(api_client):
    paths = {getattr(route, "path", "") for route in api_client.app.routes}
    assert len(PUBLIC_ROUTES) <= 5, "the public allow-list must stay short"
    for path in PUBLIC_ROUTES:
        assert path in paths, f"{path} is allow-listed as public but no longer exists"


def test_route_walk_detects_an_unauthenticated_route():
    """The walker itself must fail open routes, including ones that merely read the token."""
    from fastapi import Depends, FastAPI

    from app.core.auth import get_current_user_id, oauth2_scheme, require_admin

    def reads_token_but_never_rejects(token: str = Depends(oauth2_scheme)) -> bool:
        return bool(token)

    probe = FastAPI()

    @probe.get("/open")
    def open_route():
        return {}

    @probe.get("/token-only")
    def token_only(flag: bool = Depends(reads_token_but_never_rejects)):
        return {}

    @probe.get("/user")
    def user_route(user_id: str = Depends(get_current_user_id)):
        return {}

    @probe.get("/admin", dependencies=[Depends(require_admin)])
    def admin_route():
        return {}

    def nested(user_id: str = Depends(get_current_user_id)) -> str:
        return user_id

    @probe.get("/nested")
    def nested_route(value: str = Depends(nested)):
        return {}

    by_path = {r.path: r for r in probe.routes if hasattr(r, "dependant")}
    assert not _route_is_authenticated(by_path["/open"])
    assert not _route_is_authenticated(by_path["/token-only"])
    assert _route_is_authenticated(by_path["/user"])
    assert _route_is_authenticated(by_path["/admin"])
    assert _route_is_authenticated(by_path["/nested"])


# Routes that were reachable without any credentials before this change.
PREVIOUSLY_OPEN = [
    ("get", "/api/v1/brokers/catalog"),
    ("get", "/api/v1/brokers/accounts"),
    ("post", "/api/v1/brokers/connect"),
    ("post", "/api/v1/brokers/some-account/credentials"),
    ("post", "/api/v1/brokers/some-account/validate"),
    ("post", "/api/v1/brokers/some-account/disconnect"),
    ("delete", "/api/v1/brokers/some-account"),
    ("post", "/api/v1/brokers/test-connection"),
    ("post", "/api/v1/brokers/ibkr/connect/start"),
    ("post", "/api/v1/ibkr/connect/start"),
    ("post", "/api/v1/ibkr/connect/callback"),
    ("get", "/api/v1/shadow/status"),
    ("get", "/api/v1/shadow/summary"),
    ("get", "/api/v1/shadow/trades"),
    ("get", "/api/v1/shadow/trades/some-trade"),
    ("get", "/api/v1/shadow/outcomes"),
    ("get", "/api/v1/shadow/analytics/ml-gate"),
    ("get", "/api/v1/shadow/analytics/threshold"),
    ("get", "/api/v1/shadow/analytics/compare"),
    ("get", "/api/v1/shadow/analytics/regime"),
    ("get", "/api/v1/shadow/analytics/symbol"),
    ("get", "/api/v1/events/stream/health"),
    ("get", "/api/v1/forex/instruments"),
    ("get", "/api/admin/tradingview/runtime-fingerprint"),
    ("get", "/api/admin/tradingview/limited-status"),
    ("get", "/api/admin/tradingview/processor-status"),
]


@pytest.mark.parametrize("method,path", PREVIOUSLY_OPEN)
def test_previously_open_routes_reject_anonymous_callers(api_client, method, path):
    response = getattr(api_client, method)(path)
    assert response.status_code == 401, (path, response.status_code)
    forged = getattr(api_client, method)(path, headers={"Authorization": "Bearer not-a-jwt"})
    assert forged.status_code == 401, (path, forged.status_code)


def test_forex_instruments_is_served_to_an_authenticated_user(api_client):
    """The route accepts broker credentials, so it needs a user -- and still works for one."""
    from app.api import forex_instruments
    from app.core.auth import get_current_user_id

    route = next(r for r in api_client.app.routes if getattr(r, "path", "") == "/api/v1/forex/instruments")
    assert get_current_user_id in _dependency_calls(route.dependant)

    forex_instruments._instruments_cache.clear()
    try:
        response = api_client.get("/api/v1/forex/instruments", headers=_auth(ALICE))
        assert response.status_code == 200, response.text
        assert response.json()["source"] == "fallback"
        # A listing cached for one user's broker account is not handed to another user.
        api_client.get("/api/v1/forex/instruments?broker_account_id=acc-1", headers=_auth(ALICE))
        assert any(ALICE in key for key in forex_instruments._instruments_cache)
        api_client.get("/api/v1/forex/instruments?broker_account_id=acc-1", headers=_auth(BOB))
        assert any(BOB in key for key in forex_instruments._instruments_cache)
    finally:
        forex_instruments._instruments_cache.clear()


ADMIN_ONLY = [
    "/api/admin/tradingview/runtime-fingerprint",
    "/api/admin/tradingview/limited-status",
    "/api/admin/tradingview/processor-status",
    "/api/v1/shadow/status",
    "/api/v1/shadow/outcomes",
    "/api/v1/shadow/analytics/regime",
    "/api/v1/shadow/analytics/symbol",
]


@pytest.mark.parametrize("path", ADMIN_ONLY)
def test_operator_routes_reject_normal_users(api_client, path):
    assert api_client.get(path, headers=_auth(ALICE)).status_code == 403, path


def test_tradingview_runtime_fingerprint_is_served_to_admins(api_client):
    response = api_client.get("/api/admin/tradingview/runtime-fingerprint", headers=_auth(ADMIN, "admin"))
    assert response.status_code == 200
    assert "phase6_gate_code_version" in response.json()


# ── 2. brokers: owner-scoped store + SSRF guard ─────────────────────────────

@pytest.fixture
def clean_broker_store():
    from app.api import brokers

    brokers._ACCOUNTS_DB.clear()
    brokers._CREDENTIALS_DB.clear()
    yield brokers
    brokers._ACCOUNTS_DB.clear()
    brokers._CREDENTIALS_DB.clear()


def test_broker_draft_accounts_are_isolated_per_user(api_client, clean_broker_store):
    alice, bob = _auth(ALICE), _auth(BOB)

    created = api_client.post("/api/v1/brokers/connect", headers=alice, json={"broker_id": "oanda", "market_type": "forex"})
    assert created.status_code == 200
    account_id = created.json()["account_id"]
    submitted = api_client.post(
        f"/api/v1/brokers/{account_id}/credentials", headers=alice,
        json={"credentials": {"account_id": "001-ALICE", "api_token": "alice-private-token"}},
    )
    assert submitted.status_code == 200

    # Alice sees her account; Bob sees nothing of it.
    assert [a["id"] for a in api_client.get("/api/v1/brokers/accounts", headers=alice).json()["accounts"]] == [account_id]
    bobs_view = api_client.get("/api/v1/brokers/accounts", headers=bob)
    assert bobs_view.status_code == 200 and bobs_view.json()["accounts"] == []
    assert "001-ALICE" not in bobs_view.text

    # Bob cannot use, overwrite, validate, disconnect or delete it.
    assert api_client.post(f"/api/v1/brokers/{account_id}/credentials", headers=bob,
                           json={"credentials": {"api_key": "bob-key"}}).status_code == 404
    assert api_client.post(f"/api/v1/brokers/{account_id}/validate", headers=bob).status_code == 404
    api_client.post(f"/api/v1/brokers/{account_id}/disconnect", headers=bob)
    api_client.delete(f"/api/v1/brokers/{account_id}", headers=bob)

    still_there = api_client.get("/api/v1/brokers/accounts", headers=alice).json()["accounts"]
    assert [a["id"] for a in still_there] == [account_id]
    assert still_there[0]["status"] == "validating"          # not "disconnected"
    assert still_there[0]["masked_key"] == "001-ALICE"        # not overwritten by Bob
    store = clean_broker_store
    assert store._CREDENTIALS_DB[ALICE][account_id]["api_token"] == "alice-private-token"
    assert BOB not in store._CREDENTIALS_DB or account_id not in store._CREDENTIALS_DB[BOB]

    # The owner can still delete her own account.
    assert api_client.delete(f"/api/v1/brokers/{account_id}", headers=alice).status_code == 200
    assert api_client.get("/api/v1/brokers/accounts", headers=alice).json()["accounts"] == []


class _FakeBridgeClient:
    """Stands in for MTBridgeClient: records construction, never opens a socket."""

    created: list = []

    def __init__(self, base_url, api_token, timeout=10, verify_ssl=True):
        self.base_url, self.api_token, self.verify_ssl = base_url, api_token, verify_ssl
        self._session = SimpleNamespace(max_redirects=30)
        type(self).created.append(self)

    def get_health(self):
        return {"platform": "MT5", "account": "1", "server": "demo", "time": "now"}

    def get_balance(self):
        return {"balance": 1.0, "equity": 1.0, "currency": "USD", "free_margin": 1.0}


@pytest.fixture
def fake_bridge(monkeypatch):
    import app.exchange.mt_bridge.client as bridge_module

    _FakeBridgeClient.created = []
    monkeypatch.setattr(bridge_module, "MTBridgeClient", _FakeBridgeClient)
    return _FakeBridgeClient


def _test_bridge(api_client, url, **extra_credentials):
    return api_client.post(
        "/api/v1/brokers/test-connection", headers=_auth(ALICE),
        json={"broker_id": "mt5", "environment": "paper",
              "credentials": {"bridge_url": url, "bridge_token": "bridge-token", **extra_credentials}},
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
        "http://[fe80::1]:8080/",
        "http://[::ffff:169.254.169.254]/",
        "http://0.0.0.0:8000/",
        "file:///etc/passwd",
        "gopher://93.184.216.34:70/_x",
        "https://user:secret@93.184.216.34/",
        "https://93.184.216.34:99999/",
    ],
)
def test_bridge_test_connection_refuses_unsafe_destinations(api_client, fake_bridge, url):
    response = _test_bridge(api_client, url)
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False and "Destination not allowed" in body["error"], body
    assert "secret" not in response.text
    assert fake_bridge.created == [], "the server must not connect to a refused destination"


def _production_settings(**overrides):
    values = {"production": True, "BROKER_GATEWAY_ALLOWED_HOSTS": "", "BROKER_GATEWAY_VERIFY_TLS": "auto"}
    values.update(overrides)
    return SimpleNamespace(**values)


def test_bridge_private_addresses_allowed_in_dev_refused_in_production(api_client, fake_bridge, monkeypatch):
    from app.api import brokers

    # Non-production (tests): a local / private bridge keeps working.
    dev = _test_bridge(api_client, "https://10.0.0.5:8443", tls_mode="insecure")
    assert dev.json()["ok"] is True, dev.json()
    assert len(fake_bridge.created) == 1
    assert fake_bridge.created[0].verify_ssl is False            # legacy behaviour outside production
    assert fake_bridge.created[0]._session.max_redirects == 0    # never follow a redirect off the validated host

    # Production: refused unless allow-listed.
    fake_bridge.created.clear()
    monkeypatch.setattr(brokers, "settings", _production_settings())
    refused = _test_bridge(api_client, "https://10.0.0.5:8443")
    assert refused.json()["ok"] is False and "DESTINATION_NOT_ALLOWED" in refused.json()["error"]
    assert _test_bridge(api_client, "http://127.0.0.1:8000").json()["ok"] is False
    assert fake_bridge.created == []

    # Production + allow-list: accepted, and TLS verification cannot be switched off by the caller.
    monkeypatch.setattr(brokers, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="10.0.0.5:8443"))
    allowed = _test_bridge(api_client, "https://10.0.0.5:8443", tls_mode="insecure")
    assert allowed.json()["ok"] is True, allowed.json()
    assert fake_bridge.created[0].verify_ssl is True

    # Production: a public bridge needs no allow-list.
    fake_bridge.created.clear()
    monkeypatch.setattr(brokers, "settings", _production_settings())
    assert _test_bridge(api_client, "https://93.184.216.34:8443").json()["ok"] is True
    assert fake_bridge.created[0].verify_ssl is True


def test_gateway_verify_tls_setting(monkeypatch):
    from app.api import brokers

    cases = [
        # (production, setting, legacy_default) -> expected
        ((True, "auto", False), True),
        ((True, "auto", True), True),
        ((False, "auto", False), False),
        ((False, "auto", True), True),
        ((False, "true", False), True),
        ((True, "false", False), False),
        ((True, "false", True), True),
        ((True, "", False), True),
        ((True, "nonsense", False), True),
    ]
    for (production, setting, legacy_default), expected in cases:
        monkeypatch.setattr(
            brokers, "settings", _production_settings(production=production, BROKER_GATEWAY_VERIFY_TLS=setting)
        )
        assert brokers.gateway_verify_tls(legacy_default) is expected, (production, setting, legacy_default)


class _FakeIBKRSession:
    def is_connected(self):
        return True


class _FakeIBKRSessionManager:
    calls: list = []

    async def get_session(self, connection_id, host="127.0.0.1", port=7496, client_id=1):
        type(self).calls.append((host, port))
        return _FakeIBKRSession()


class _FakeIBKRClient:
    def __init__(self, session):
        self.session = session

    def get_portfolio_accounts(self):
        return ["U1234567"]

    def get_account_summary(self, account):
        return {"wallet": 1, "equity": 1, "available": 1}


@pytest.fixture
def fake_ibkr(monkeypatch):
    import app.api.ibkr as ibkr_api
    import app.exchange.ibkr.client as client_module
    import app.exchange.ibkr.session as session_module

    _FakeIBKRSessionManager.calls = []
    for module in (session_module, ibkr_api):
        monkeypatch.setattr(module, "IBKRSessionManager", _FakeIBKRSessionManager)
    for module in (client_module, ibkr_api):
        monkeypatch.setattr(module, "IBKRClient", _FakeIBKRClient)
    return _FakeIBKRSessionManager


def _test_ibkr(api_client, **credentials):
    return api_client.post(
        "/api/v1/brokers/test-connection", headers=_auth(ALICE),
        json={"broker_id": "ibkr", "environment": "paper", "credentials": credentials},
    )


def test_ibkr_test_connection_guards_host_and_port(api_client, fake_ibkr, monkeypatch):
    from app.api import brokers

    # Always refused: metadata / link-local, and out-of-range ports.
    for credentials in ({"host": "169.254.169.254", "port": 80}, {"host": "127.0.0.1", "port": 70000},
                        {"host": "127.0.0.1", "port": 0}, {"host": "evil.example/x", "port": 4001}):
        body = _test_ibkr(api_client, **credentials).json()
        assert body["ok"] is False and "Destination not allowed" in body["error"], (credentials, body)
    assert fake_ibkr.calls == []

    # Non-production: the local gateway keeps working, and the connection goes to the validated IP.
    assert _test_ibkr(api_client, host="127.0.0.1", port=4002).json()["ok"] is True
    assert fake_ibkr.calls == [("127.0.0.1", 4002)]

    # Production: loopback only when allow-listed (host AND port).
    fake_ibkr.calls.clear()
    monkeypatch.setattr(brokers, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="127.0.0.1:4001"))
    assert _test_ibkr(api_client, host="127.0.0.1", port=7496).json()["ok"] is False
    assert _test_ibkr(api_client, host="10.1.2.3", port=4001).json()["ok"] is False
    assert fake_ibkr.calls == []
    assert _test_ibkr(api_client, host="127.0.0.1", port=4001).json()["ok"] is True
    assert fake_ibkr.calls == [("127.0.0.1", 4001)]


def test_brokers_ibkr_link_flow_guards_destination(api_client, fake_ibkr):
    refused = api_client.post("/api/v1/brokers/ibkr/connect/start", headers=_auth(ALICE),
                              json={"host": "169.254.169.254", "port": 80})
    assert refused.status_code == 200
    assert refused.json()["status"] == "error" and "Destination not allowed" in refused.json()["message"]
    assert fake_ibkr.calls == []


def test_ibkr_connect_start_guards_destination_and_scopes_callback(api_client, fake_ibkr, monkeypatch):
    import app.api.ibkr as ibkr_api

    alice, bob = _auth(ALICE), _auth(BOB)

    for body in ({"host": "169.254.169.254", "port": 7496}, {"host": "127.0.0.1", "port": 70000},
                 {"host": "127.0.0.1", "port": 7496, "gateway_url": "http://169.254.169.254/"}):
        refused = api_client.post("/api/v1/ibkr/connect/start", headers=alice, json=body)
        assert refused.status_code == 400, (body, refused.status_code)
        assert "Destination not allowed" in refused.json()["detail"]
    assert fake_ibkr.calls == []

    started = api_client.post("/api/v1/ibkr/connect/start", headers=alice, json={"host": "127.0.0.1", "port": 7496})
    assert started.status_code == 200 and started.json()["status"] == "connected", started.json()
    assert fake_ibkr.calls == [("127.0.0.1", 7496)]
    connection_id = started.json()["connection_id"]

    # The connection belongs to Alice: Bob's lookup is a 404, exactly like an unknown id.
    callback = {"connection_id": connection_id}
    assert api_client.post("/api/v1/ibkr/connect/callback", headers=alice, json=callback).json()["connected"] is True
    assert api_client.post("/api/v1/ibkr/connect/callback", headers=bob, json=callback).status_code == 404

    # Production: loopback is refused unless allow-listed.
    fake_ibkr.calls.clear()
    monkeypatch.setattr(ibkr_api, "settings", _production_settings())
    prod = api_client.post("/api/v1/ibkr/connect/start", headers=alice, json={"host": "127.0.0.1", "port": 7496})
    assert prod.status_code == 400 and "DESTINATION_NOT_ALLOWED" in prod.json()["detail"]
    assert fake_ibkr.calls == []
    # ... and accepted once the operator lists the gateway.
    monkeypatch.setattr(ibkr_api, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="127.0.0.1:7496"))
    listed = api_client.post("/api/v1/ibkr/connect/start", headers=alice, json={"host": "127.0.0.1", "port": 7496})
    assert listed.status_code == 200 and listed.json()["status"] == "connected", listed.json()
    assert fake_ibkr.calls == [("127.0.0.1", 7496)]


# ── 3. shadow routes: ownership ─────────────────────────────────────────────

class _FakeShadowStore:
    def __init__(self):
        self.list_calls = []

    def list_trades(self, **kwargs):
        self.list_calls.append(kwargs)
        return [{"id": "st-1", "bot_instance_id": kwargs.get("bot_instance_id")}]

    def get_shadow_trade(self, shadow_trade_id):
        owners = {"st-alice": "bot-alice", "st-bob": "bot-bob"}
        if shadow_trade_id not in owners:
            return None
        return {"id": shadow_trade_id, "bot_instance_id": owners[shadow_trade_id]}

    def get_outcome(self, shadow_trade_id):
        return None

    def list_outcomes(self, **kwargs):
        return []

    def count_by_status(self):
        return {}

    def count_by_stage(self):
        return {}


class _FakeShadowAnalytics:
    def __init__(self):
        self.calls = []

    def _record(self, name, kwargs, result):
        self.calls.append((name, kwargs.get("bot_instance_id")))
        return result

    def get_category_summary(self, **kwargs):
        return self._record("summary", kwargs, [])

    def get_ml_gate_analysis(self, **kwargs):
        return self._record("ml-gate", kwargs, {})

    def get_threshold_gap_analysis(self, **kwargs):
        return self._record("threshold", kwargs, [])

    def compare_vs_real_trades(self, **kwargs):
        return self._record("compare", kwargs, {})

    def get_regime_breakdown(self, **kwargs):
        return []

    def get_symbol_breakdown(self, **kwargs):
        return []


@pytest.fixture
def shadow(monkeypatch):
    from app.api import shadow_routes

    store, analytics = _FakeShadowStore(), _FakeShadowAnalytics()
    owners = {"bot-alice": ALICE, "bot-bob": BOB}
    monkeypatch.setattr(shadow_routes, "_get_store", lambda: store)
    monkeypatch.setattr(shadow_routes, "_get_analytics", lambda: analytics)
    monkeypatch.setattr(shadow_routes, "_bot_owner_id", lambda bot_id: owners.get(bot_id))
    return SimpleNamespace(store=store, analytics=analytics)


SCOPED_SHADOW_ROUTES = [
    "/api/v1/shadow/trades",
    "/api/v1/shadow/summary",
    "/api/v1/shadow/analytics/ml-gate",
    "/api/v1/shadow/analytics/threshold",
    "/api/v1/shadow/analytics/compare",
]


@pytest.mark.parametrize("path", SCOPED_SHADOW_ROUTES)
def test_shadow_routes_are_scoped_to_the_callers_own_bot(api_client, shadow, path):
    alice = _auth(ALICE)
    # No bot named: a regular user may not read across bots.
    assert api_client.get(path, headers=alice).status_code == 403
    # Someone else's bot and an unknown bot are indistinguishable.
    assert api_client.get(f"{path}?bot_instance_id=bot-bob", headers=alice).status_code == 404
    assert api_client.get(f"{path}?bot_instance_id=does-not-exist", headers=alice).status_code == 404
    assert shadow.store.list_calls == [] and shadow.analytics.calls == []
    # Her own bot works, and the store is queried with exactly that bot.
    assert api_client.get(f"{path}?bot_instance_id=bot-alice", headers=alice).status_code == 200
    queried = [c.get("bot_instance_id") for c in shadow.store.list_calls] + [b for _, b in shadow.analytics.calls]
    assert queried == ["bot-alice"]


@pytest.mark.parametrize("path", SCOPED_SHADOW_ROUTES)
def test_shadow_routes_admin_may_query_any_or_all_bots(api_client, shadow, path):
    admin = _auth(ADMIN, "admin")
    assert api_client.get(path, headers=admin).status_code == 200
    assert api_client.get(f"{path}?bot_instance_id=bot-bob", headers=admin).status_code == 200
    queried = [c.get("bot_instance_id") for c in shadow.store.list_calls] + [b for _, b in shadow.analytics.calls]
    assert queried == [None, "bot-bob"]


def test_shadow_trade_detail_enforces_ownership(api_client, shadow):
    alice = _auth(ALICE)
    own = api_client.get("/api/v1/shadow/trades/st-alice", headers=alice)
    assert own.status_code == 200 and own.json()["trade"]["id"] == "st-alice"
    assert api_client.get("/api/v1/shadow/trades/st-bob", headers=alice).status_code == 404
    assert api_client.get("/api/v1/shadow/trades/unknown", headers=alice).status_code == 404
    assert api_client.get("/api/v1/shadow/trades/st-bob", headers=_auth(ADMIN, "admin")).status_code == 200


def test_shadow_system_wide_views_are_served_to_admins(api_client, shadow):
    admin = _auth(ADMIN, "admin")
    for path in ("/api/v1/shadow/status", "/api/v1/shadow/outcomes",
                 "/api/v1/shadow/analytics/regime", "/api/v1/shadow/analytics/symbol"):
        assert api_client.get(path, headers=admin).status_code == 200, path


def test_shadow_bot_owner_lookup_reads_bot_instances(db):
    from app.api import shadow_routes

    _insert_row(db, "bot_instances", id="bot-owned", user_id=ALICE)
    assert shadow_routes._bot_owner_id("bot-owned") == ALICE
    assert shadow_routes._bot_owner_id("no-such-bot") is None
    assert shadow_routes._authorize_bot_scope("bot-owned", ALICE, False) == "bot-owned"
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        shadow_routes._authorize_bot_scope("bot-owned", BOB, False)
    assert exc_info.value.status_code == 404


# ── 4. SSE: per-user event filtering ────────────────────────────────────────

def _event(event_type=None, **payload):
    from shared_lib.persistence.events import Event, EventLevel, EventType

    return Event(event_type=event_type or EventType.POSITION_CLOSED, level=EventLevel.INFO,
                 payload=payload, symbol="BTCUSDT", trade_id="trade-1")


@pytest.fixture
def events_api(monkeypatch):
    pytest.importorskip("sse_starlette")
    from app.api import events

    owners = {("bot", "bot-alice"): ALICE, ("bot", "bot-bob"): BOB, ("account", "acc-bob"): BOB}
    monkeypatch.setattr(events, "_lookup_owner", lambda kind, entity_id: owners.get((kind, str(entity_id))))
    return events


def test_event_owner_resolution(events_api):
    resolve = events_api.resolve_event_owner
    assert resolve(_event(user_id=ALICE)) == ALICE
    assert resolve(_event(bot_instance_id="bot-alice")) == ALICE
    assert resolve(_event(bot_id="bot-bob")) == BOB
    assert resolve(_event(broker_account_id="acc-bob")) == BOB
    assert resolve(_event(bot_instance_id="unknown-bot")) is None
    assert resolve(_event(realized_pnl=12.5)) is None            # no owner information at all
    assert resolve(SimpleNamespace(payload=None)) is None


def test_event_visibility_is_owner_or_admin_only(events_api):
    visible = events_api.event_visible_to
    alices, bobs, ownerless = _event(user_id=ALICE), _event(bot_instance_id="bot-bob"), _event(realized_pnl=1.0)

    assert visible(alices, ALICE, False) is True
    assert visible(alices, BOB, False) is False
    assert visible(bobs, ALICE, False) is False
    assert visible(bobs, BOB, False) is True
    # No resolvable owner: nobody but an admin.
    assert visible(ownerless, ALICE, False) is False
    assert visible(ownerless, BOB, False) is False
    for event in (alices, bobs, ownerless):
        assert visible(event, ADMIN, True) is True


def test_sse_stream_only_delivers_the_subscribers_own_events(events_api):
    from shared_lib.persistence.event_broadcaster import get_event_broadcaster
    from shared_lib.persistence.events import EventType

    async def scenario(user_id, is_admin, expected_count):
        stream = events_api._event_stream(user_id, is_admin)
        received = [await stream.__anext__()]                     # subscribes; "connected"
        broadcaster = get_event_broadcaster()
        broadcaster.broadcast(_event(EventType.POSITION_CLOSED, user_id=BOB, realized_pnl=111.0))
        broadcaster.broadcast(_event(EventType.POSITION_OPENED, entry_price=222.0))      # ownerless
        broadcaster.broadcast(_event(EventType.TP1_HIT, bot_instance_id="bot-alice", fill_price=333.0))
        try:
            for _ in range(expected_count):
                received.append(await asyncio.wait_for(stream.__anext__(), timeout=5))
        finally:
            await stream.aclose()
        return received

    before = get_event_broadcaster().get_listener_count()

    alice_stream = asyncio.run(scenario(ALICE, False, 1))
    assert alice_stream[0]["event"] == "connected"
    assert [m["event"] for m in alice_stream[1:]] == ["TP1_HIT"]
    # Payload format is unchanged.
    data = json.loads(alice_stream[1]["data"])
    assert set(data) == {"event_id", "trade_id", "symbol", "ts", "payload"}
    assert data["payload"] == {"bot_instance_id": "bot-alice", "fill_price": 333.0}
    assert "111.0" not in json.dumps(alice_stream) and "222.0" not in json.dumps(alice_stream)

    admin_stream = asyncio.run(scenario(ADMIN, True, 3))
    assert [m["event"] for m in admin_stream[1:]] == ["POSITION_CLOSED", "POSITION_OPENED", "TP1_HIT"]

    assert get_event_broadcaster().get_listener_count() == before   # listeners cleaned up


def test_sse_owner_lookup_resolves_bot_owner_from_database(db):
    pytest.importorskip("sse_starlette")
    from app.api import events

    events._OWNER_CACHE.clear()
    try:
        _insert_row(db, "bot_instances", id="bot-sse", user_id=ALICE)
        event = _event(bot_instance_id="bot-sse")
        assert events.resolve_event_owner(event) == ALICE
        assert events.event_visible_to(event, ALICE, False) is True
        assert events.event_visible_to(event, BOB, False) is False
        assert events.resolve_event_owner(_event(bot_instance_id="bot-missing")) is None
    finally:
        events._OWNER_CACHE.clear()


# ── 5. inventory is admin-only ──────────────────────────────────────────────

def test_bot_inventory_is_admin_only(api_client):
    from app.core.auth import require_admin

    assert api_client.get("/api/v1/bot-instances/inventory").status_code == 401
    route = next(r for r in api_client.app.routes if getattr(r, "path", "") == "/api/v1/bot-instances/inventory")
    assert require_admin in _dependency_calls(route.dependant)


def test_bot_inventory_rejects_an_active_non_admin_user(api_client, db):
    _insert_row(db, "users", id=ALICE, email="alice@hardening.test", status="active", role="user")
    response = api_client.get("/api/v1/bot-instances/inventory", headers=_auth(ALICE))
    assert response.status_code == 403


# ── 6. /debug/config masks by rule ──────────────────────────────────────────

def test_debug_config_masks_every_secret_by_rule(api_client):
    import app.main as main
    from app.core.config import settings

    public = main._settings_public_dict()
    masks = {main.CONFIG_MASK, main.CONFIG_MASK_UNSET}

    for name in ("SECRET_KEY", "CREDENTIAL_KEY", "BINANCE_API_KEY", "BINANCE_API_SECRET",
                 "BYBIT_API_KEY", "BYBIT_API_SECRET", "BINGX_API_KEY", "BINGX_API_SECRET",
                 "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "SMTP_PASSWORD"):
        assert public[name] in masks, name

    # By rule, not by list: any current or future setting whose name carries a marker is masked.
    for name, value in public.items():
        if any(marker in name.upper() for marker in main.SENSITIVE_NAME_MARKERS):
            assert value in masks, name

    serialized = json.dumps(public, default=str)
    for secret in (settings.SECRET_KEY, settings.CREDENTIAL_KEY):
        assert secret and secret not in serialized
    # Non-secret settings stay readable.
    assert public["EXECUTION_MODE"] == settings.EXECUTION_MODE
    assert public["TRADE_SYMBOLS"] == settings.TRADE_SYMBOLS


def test_debug_config_mask_rules():
    import app.main as main

    mask = main._mask_config_value
    assert mask("SOME_NEW_PRIVATE_KEY", "abcd1234") == main.CONFIG_MASK
    assert mask("some_webhook_url", "https://hooks.example/abc") == main.CONFIG_MASK
    assert mask("SENTRY_DSN", "https://abc@sentry.example/1") == main.CONFIG_MASK
    assert mask("SMTP_PASS", "pw") == main.CONFIG_MASK
    assert mask("REFRESH_TOKEN_EXPIRE_DAYS", 14) == main.CONFIG_MASK
    assert mask("STRIPE_SECRET_KEY", "") == main.CONFIG_MASK_UNSET
    assert mask("STRIPE_SECRET_KEY", None) == main.CONFIG_MASK_UNSET
    # Fixed mask, never a partial reveal.
    assert "abcd" not in str(mask("BYBIT_API_SECRET", "abcd-very-secret-wxyz"))
    assert "wxyz" not in str(mask("BYBIT_API_SECRET", "abcd-very-secret-wxyz"))
    # A value that is a URL with embedded credentials is masked whatever the name.
    assert mask("DATABASE_URL", "postgresql://bot:hunter2@db.internal:5432/cf") == main.CONFIG_MASK
    assert mask("REDIS_URL", "redis://:hunter2@cache:6379/0") == main.CONFIG_MASK
    assert mask("DATABASE_URL", "sqlite:///data/bot.db") == "sqlite:///data/bot.db"
    assert mask("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com") == "https://fapi.binance.com"
    # Nested values are masked by the same rule.
    nested = mask("EXTRA", {"api_key": "k", "proxies": ["http://u:p@proxy:8080", "http://proxy:8080"], "n": 1})
    assert nested == {"api_key": main.CONFIG_MASK, "proxies": [main.CONFIG_MASK, "http://proxy:8080"], "n": 1}


def test_debug_config_route_stays_admin_only(api_client):
    assert api_client.get("/debug/config").status_code == 401
    assert api_client.get("/debug/config", headers=_auth(ALICE)).status_code == 403


# ── 7. API docs are not served in production ────────────────────────────────

def test_docs_disabled_in_production_unless_enabled(api_client):
    import app.main as main

    assert main._api_docs_enabled(SimpleNamespace(production=True, API_DOCS_ENABLED=False)) is False
    assert main._api_docs_enabled(SimpleNamespace(production=True, API_DOCS_ENABLED=True)) is True
    assert main._api_docs_enabled(SimpleNamespace(production=False, API_DOCS_ENABLED=False)) is True
    assert main._api_docs_enabled(SimpleNamespace()) is False        # unknown environment: closed

    # The test process is not production, so the docs stay available here.
    app = api_client.app
    assert main.API_DOCS_ENABLED is True
    assert (app.docs_url, app.redoc_url, app.openapi_url) == ("/docs", "/redoc", "/openapi.json")

    # What a production process builds: no docs, no schema.
    from fastapi import FastAPI

    enabled = main._api_docs_enabled(SimpleNamespace(production=True, API_DOCS_ENABLED=False))
    prod_app = FastAPI(docs_url="/docs" if enabled else None, redoc_url="/redoc" if enabled else None,
                       openapi_url="/openapi.json" if enabled else None)
    prod_paths = {getattr(r, "path", "") for r in prod_app.routes}
    assert not prod_paths & {"/docs", "/redoc", "/openapi.json"}


# ── 8. TradingView webhook: HMAC where configured, no raw token at rest ─────

def _tv_client(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api import tradingview
    from shared_lib.persistence.tradingview import create_webhook

    db_path = tmp_path / "tradingview.db"
    migrate(str(db_path))
    tv_db = DB(path=str(db_path))
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    monkeypatch.setenv("LIVE_SYMBOLS", "BTCUSDT,ETHUSDT")
    monkeypatch.setattr(tradingview, "_get_db", lambda: tv_db)
    app = FastAPI()
    app.include_router(tradingview.router, prefix="/api/v1/tradingview")
    seeded = create_webhook(tv_db, bot_id="bot-tv-test", allowed_symbols=["BTCUSDT", "ETHUSDT"])
    return TestClient(app), tv_db, seeded


def _tv_payload(token, **overrides):
    from shared_lib.persistence.tradingview import utc_now_iso

    data = {
        "token": token, "bot_id": "bot-tv-test", "alert_id": "alert-1", "symbol": "BINANCE:BTCUSDT.P",
        "exchange": "BINANCE", "timeframe": "1m", "strategy_name": "TV Test", "action": "BUY", "side": "LONG",
        "price": "100.5", "timestamp": utc_now_iso(), "confidence": 0.75,
    }
    data.update(overrides)
    return data


def _require_hmac(tv_db, webhook_id):
    from shared_lib.persistence.tradingview import hash_secret

    with tv_db.connect() as conn:
        conn.execute("UPDATE tradingview_webhooks SET secret_hash=? WHERE id=?",
                     (hash_secret("relay-shared-secret"), webhook_id))


def _signed_headers(token, body: bytes):
    timestamp, nonce = "1700000000", "nonce-1"
    signature = hmac.new(token.encode(), timestamp.encode() + b"." + nonce.encode() + b"." + body,
                         hashlib.sha256).hexdigest()
    return {"Content-Type": "application/json", "X-CF-TV-Signature": f"sha256={signature}",
            "X-CF-TV-Timestamp": timestamp, "X-CF-TV-Nonce": nonce}


def test_webhook_without_hmac_secret_still_accepts_plain_tradingview_alerts(tmp_path, monkeypatch):
    client, _tv_db, seeded = _tv_client(tmp_path, monkeypatch)
    response = client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=_tv_payload(seeded["token"]))
    assert response.json()["status"] == "accepted"


def test_webhook_with_hmac_secret_requires_the_signature_header(tmp_path, monkeypatch):
    from shared_lib.persistence.tradingview import list_alerts

    client, tv_db, seeded = _tv_client(tmp_path, monkeypatch)
    _require_hmac(tv_db, seeded["id"])
    url = f"/api/v1/tradingview/webhook/{seeded['token']}"

    # Valid token, header simply omitted: no longer a way around the check.
    missing = client.post(url, json=_tv_payload(seeded["token"]))
    assert missing.json()["status"] == "rejected"
    assert missing.json()["reason"] == "SIGNATURE_REQUIRED"
    assert list_alerts(tv_db)[0]["status"] == "INVALID_SIGNATURE"

    bad = client.post(url, json=_tv_payload(seeded["token"], alert_id="alert-bad"),
                      headers={"X-CF-TV-Signature": "sha256=bad", "X-CF-TV-Timestamp": "1", "X-CF-TV-Nonce": "n"})
    assert bad.json()["reason"] == "INVALID_SIGNATURE"

    body = json.dumps(_tv_payload(seeded["token"], alert_id="alert-signed"), separators=(",", ":")).encode()
    signed = client.post(url, content=body, headers=_signed_headers(seeded["token"], body))
    assert signed.json()["status"] == "accepted", signed.json()


def test_webhook_payload_is_persisted_without_token_or_secrets(tmp_path, monkeypatch):
    from shared_lib.persistence.tradingview import PAYLOAD_REDACTED, list_alerts

    client, tv_db, seeded = _tv_client(tmp_path, monkeypatch)
    token = seeded["token"]
    url = f"/api/v1/tradingview/webhook/{token}"

    accepted = client.post(url, json=_tv_payload(
        token, secret="relay-secret-value", passphrase="open-sesame", api_key="AKIA-not-for-disk",
        comment=f"forwarded with {token}", nested={"webhook_token": token, "note": "keep me"},
    ))
    assert accepted.json()["status"] == "accepted", accepted.json()
    # A rejected alert is stored too -- and must be just as clean.
    rejected = client.post(url, json=_tv_payload(token, alert_id="alert-close", action="CLOSE", secret="another-secret"))
    assert rejected.json()["status"] == "rejected"

    rows = list_alerts(tv_db)
    assert len(rows) == 2
    for row in rows:
        stored = row["payload_json"]
        for leaked in (token, "relay-secret-value", "open-sesame", "AKIA-not-for-disk", "another-secret"):
            assert leaked not in stored, leaked
    stored = json.loads(next(r for r in rows if r["alert_id"] == "alert-1")["payload_json"])
    assert stored["token"] == PAYLOAD_REDACTED
    assert stored["secret"] == PAYLOAD_REDACTED and stored["passphrase"] == PAYLOAD_REDACTED
    assert stored["nested"] == {"webhook_token": PAYLOAD_REDACTED, "note": "keep me"}
    # Everything that is not a credential is kept for audit.
    assert stored["symbol"] == "BINANCE:BTCUSDT.P" and stored["alert_id"] == "alert-1" and stored["action"] == "BUY"


def test_redact_webhook_payload_unit():
    from shared_lib.persistence.tradingview import PAYLOAD_REDACTED, generate_webhook_token, redact_webhook_payload

    token = generate_webhook_token()
    original = {"token": token, "Secret": "s", "PASSPHRASE": "p", "password": "pw", "apiKey": "k",
                "qty": 3, "empty_token": "", "list": [{"signature": "sig"}, f"x {token} y"], "symbol": "BTCUSDT"}
    redacted = redact_webhook_payload(original)
    assert redacted == {"token": PAYLOAD_REDACTED, "Secret": PAYLOAD_REDACTED, "PASSPHRASE": PAYLOAD_REDACTED,
                        "password": PAYLOAD_REDACTED, "apiKey": PAYLOAD_REDACTED, "qty": 3, "empty_token": "",
                        "list": [{"signature": PAYLOAD_REDACTED}, f"x {PAYLOAD_REDACTED} y"], "symbol": "BTCUSDT"}
    assert original["token"] == token                      # the caller's dict is not mutated
    assert redact_webhook_payload("not-a-dict") == "not-a-dict"
