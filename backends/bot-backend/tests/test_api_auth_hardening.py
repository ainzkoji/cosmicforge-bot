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


def test_forex_instruments_cache_is_bounded():
    """The cache key carries caller-chosen values, so the cache has a size cap and a TTL sweep."""
    from datetime import datetime, timedelta

    from app.api import forex_instruments as fx

    # Synchronous broker fetches: the handler is a plain function (threadpool), not a coroutine.
    assert not inspect.iscoroutinefunction(fx.get_forex_instruments)

    response = fx.ForexInstrumentsResponse(broker_id="x", source="fallback", instruments=[])
    fx._instruments_cache.clear()
    try:
        for index in range(fx.CACHE_MAX_ENTRIES + 50):
            fx.set_cached_instruments(f"broker-{index}:default:practice", response)
        assert len(fx._instruments_cache) == fx.CACHE_MAX_ENTRIES
        assert "broker-0:default:practice" not in fx._instruments_cache          # oldest evicted first
        assert f"broker-{fx.CACHE_MAX_ENTRIES + 49}:default:practice" in fx._instruments_cache

        # An absurdly long caller-chosen key is served but never remembered.
        fx.set_cached_instruments("b" * (fx.CACHE_MAX_KEY_LENGTH + 1), response)
        assert len(fx._instruments_cache) == fx.CACHE_MAX_ENTRIES

        # Expired entries are swept on the next write, not only when the same key is read again.
        stale = datetime.utcnow() - timedelta(seconds=fx.CACHE_TTL_SECONDS + 1)
        for key in list(fx._instruments_cache):
            fx._instruments_cache[key] = (stale, response)
        fx.set_cached_instruments("fresh:default:practice", response)
        assert list(fx._instruments_cache) == ["fresh:default:practice"]
        assert fx.get_cached_instruments("fresh:default:practice") is response
    finally:
        fx._instruments_cache.clear()


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
    brokers._DRAFT_EXPIRES_AT.clear()
    yield brokers
    brokers._ACCOUNTS_DB.clear()
    brokers._CREDENTIALS_DB.clear()
    brokers._DRAFT_EXPIRES_AT.clear()


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


def test_broker_draft_store_expires_and_caps_credentials(api_client, clean_broker_store):
    """Drafts hold plaintext credentials in memory: they expire, and one set cannot be huge."""
    store = clean_broker_store
    alice = _auth(ALICE)
    assert store._DRAFT_TTL_SECONDS <= 30 * 60

    def new_draft():
        created = api_client.post("/api/v1/brokers/connect", headers=alice,
                                  json={"broker_id": "oanda", "market_type": "forex"})
        assert created.status_code == 200
        return created.json()["account_id"]

    # An oversized credential set is refused and nothing of it is kept.
    account_id = new_draft()
    huge = api_client.post(f"/api/v1/brokers/{account_id}/credentials", headers=alice,
                           json={"credentials": {"api_key": "k" * (store._MAX_CREDENTIALS_BYTES + 1)}})
    assert huge.status_code == 413
    assert account_id not in store._CREDENTIALS_DB.get(ALICE, {})
    nested = api_client.post(f"/api/v1/brokers/{account_id}/credentials", headers=alice,
                             json={"credentials": {"blob": ["x" * 1024] * 32}})
    assert nested.status_code == 413
    ok = api_client.post(f"/api/v1/brokers/{account_id}/credentials", headers=alice,
                         json={"credentials": {"account_id": "001-ALICE", "api_token": "alice-private-token"}})
    assert ok.status_code == 200
    assert store._CREDENTIALS_DB[ALICE][account_id]["api_token"] == "alice-private-token"

    # Before the TTL nothing is dropped; after it the draft AND its credentials are gone.
    other = new_draft()
    deadline = store._DRAFT_EXPIRES_AT[account_id]
    assert store._purge_expired_drafts(now=deadline - 1) == 0
    assert account_id in store._ACCOUNTS_DB[ALICE]
    store._DRAFT_EXPIRES_AT[account_id] = time.monotonic() - 1          # this one has expired
    listed = api_client.get("/api/v1/brokers/accounts", headers=alice).json()["accounts"]
    assert [a["id"] for a in listed] == [other]
    assert account_id not in store._CREDENTIALS_DB.get(ALICE, {})
    assert account_id not in store._DRAFT_EXPIRES_AT
    assert api_client.post(f"/api/v1/brokers/{account_id}/validate", headers=alice).status_code == 404

    # When a user's last draft expires, their (empty) containers are released too.
    store._DRAFT_EXPIRES_AT[other] = time.monotonic() - 1
    assert store._purge_expired_drafts() == 1
    assert ALICE not in store._ACCOUNTS_DB and ALICE not in store._CREDENTIALS_DB
    assert store._DRAFT_EXPIRES_AT == {}


class _FakeBridgeClient:
    """Stands in for MTBridgeClient: records construction, never opens a socket."""

    created: list = []
    #: Set to an exception instance to make the "bridge" fail.
    health_error = None

    def __init__(self, base_url, api_token, timeout=10, verify_ssl=True):
        self.base_url, self.api_token, self.verify_ssl = base_url, api_token, verify_ssl
        self._session = SimpleNamespace(max_redirects=30)
        self.called_on_event_loop = None
        type(self).created.append(self)

    def get_health(self):
        try:
            asyncio.get_running_loop()
            self.called_on_event_loop = True
        except RuntimeError:
            self.called_on_event_loop = False
        if type(self).health_error is not None:
            raise type(self).health_error
        return {"platform": "MT5", "account": "1", "server": "demo", "time": "now"}

    def get_balance(self):
        return {"balance": 1.0, "equity": 1.0, "currency": "USD", "free_margin": 1.0}


@pytest.fixture
def fake_bridge(monkeypatch):
    import app.exchange.mt_bridge.client as bridge_module

    _FakeBridgeClient.created = []
    _FakeBridgeClient.health_error = None
    monkeypatch.setattr(bridge_module, "MTBridgeClient", _FakeBridgeClient)
    yield _FakeBridgeClient
    _FakeBridgeClient.health_error = None


def _test_bridge(api_client, url, as_admin=False, **extra_credentials):
    return api_client.post(
        "/api/v1/brokers/test-connection",
        headers=_auth(ADMIN, "admin") if as_admin else _auth(ALICE),
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

    # Production + allow-list: the listed address is the platform's own
    # infrastructure, so an ordinary user is still refused -- with the same
    # answer as for an unlisted address (the allow-list is not disclosed).
    monkeypatch.setattr(brokers, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="10.0.0.5:8443"))
    as_user = _test_bridge(api_client, "https://10.0.0.5:8443", tls_mode="insecure")
    assert as_user.json() == refused.json()
    assert fake_bridge.created == []
    # ... an admin is accepted, and TLS verification cannot be switched off by the caller.
    allowed = _test_bridge(api_client, "https://10.0.0.5:8443", as_admin=True, tls_mode="insecure")
    assert allowed.json()["ok"] is True, allowed.json()
    assert fake_bridge.created[0].verify_ssl is True
    # Being an admin does not open an address that is not listed.
    assert _test_bridge(api_client, "https://10.0.0.6:8443", as_admin=True).json()["ok"] is False

    # Production: a public bridge needs no allow-list.
    fake_bridge.created.clear()
    monkeypatch.setattr(brokers, "settings", _production_settings())
    assert _test_bridge(api_client, "https://93.184.216.34:8443").json()["ok"] is True
    assert fake_bridge.created[0].verify_ssl is True


def test_bridge_url_must_be_https_in_production_and_never_carry_query_or_fragment(api_client, fake_bridge, monkeypatch):
    from app.api import brokers

    # A trailing "#" / "?x=" would turn the path the server appends ("/v1/health")
    # into a fragment / query value and leave the request path to the caller.
    for url in ("https://93.184.216.34/#", "https://93.184.216.34/internal/admin?x=", "https://93.184.216.34?"):
        body = _test_bridge(api_client, url).json()                       # every environment
        assert body["ok"] is False and "URL_QUERY_NOT_ALLOWED" in body["error"], (url, body)
    assert fake_bridge.created == []

    monkeypatch.setattr(brokers, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="10.0.0.5:8000"))
    # Production: plain http sends the bearer token in clear text -- refused for a public bridge ...
    plain = _test_bridge(api_client, "http://93.184.216.34:8443").json()
    assert plain["ok"] is False and "HTTPS_REQUIRED" in plain["error"], plain
    assert _test_bridge(api_client, "http://93.184.216.34:8443", as_admin=True).json()["ok"] is False
    assert fake_bridge.created == []
    # ... and accepted only for an allow-listed host, which only an admin can target.
    assert _test_bridge(api_client, "http://10.0.0.5:8000").json()["ok"] is False
    assert fake_bridge.created == []
    assert _test_bridge(api_client, "http://10.0.0.5:8000", as_admin=True).json()["ok"] is True
    assert fake_bridge.created[0].base_url == "http://10.0.0.5:8000"
    assert fake_bridge.created[0]._session.max_redirects == 0


def test_bridge_test_runs_off_the_event_loop_and_revalidates_before_each_request(api_client, fake_bridge):
    from app.api import brokers
    from shared_lib.core.security.url_guard import UnsafeDestinationError

    # The handler's blocking part (requests + DNS) is a plain function run in the threadpool.
    assert not inspect.iscoroutinefunction(brokers._test_mt_bridge_connection)
    assert not inspect.iscoroutinefunction(brokers.validate_connection)
    assert _test_bridge(api_client, "https://93.184.216.34:8443").json()["ok"] is True
    client = fake_bridge.created[0]
    assert client.called_on_event_loop is False

    # The client was given a guard that re-applies the outbound policy to the
    # validated destination; MTBridgeClient runs it before every request.
    assert callable(client.destination_guard)
    client.destination_guard()                                   # still a public address: passes
    with patch.object(brokers, "revalidate_destination", side_effect=UnsafeDestinationError("DESTINATION_NOT_ALLOWED")):
        with pytest.raises(UnsafeDestinationError):
            client.destination_guard()


def test_bridge_failure_never_echoes_the_upstream_response(api_client, fake_bridge, monkeypatch):
    from unittest.mock import Mock

    from app.api import brokers
    from app.exchange.mt_bridge.errors import MTBridgeError

    server_log = Mock()
    monkeypatch.setattr(brokers, "logger", server_log)

    upstream_text = "root:x:0:0:root:/root:/bin/bash INTERNAL-ONLY-BODY"
    cases = [
        (MTBridgeError(f"Bridge API error: {upstream_text}", status_code=500, failure_kind="HTTP_ERROR"),
         "Bridge connection failed: the bridge rejected the request (HTTP 500)"),
        (MTBridgeError("Invalid JSON response from bridge (HTTP 200)", status_code=200, failure_kind="INVALID_RESPONSE"),
         "Bridge connection failed: the bridge sent an unexpected response (HTTP 200)"),
        (MTBridgeError(f"Cannot connect to bridge at https://x: {upstream_text}", failure_kind="UNREACHABLE"),
         "Bridge connection failed: the bridge could not be reached"),
        (MTBridgeError("Bridge request timeout after 10s", failure_kind="TIMEOUT"),
         "Bridge connection failed: the bridge did not answer in time"),
        (RuntimeError(f"boom {upstream_text} bridge-token"), "Bridge connection failed: unexpected error"),
    ]
    for error, expected in cases:
        fake_bridge.health_error = error
        response = _test_bridge(api_client, "https://93.184.216.34:8443")
        assert response.json() == {"ok": False, "error": expected, "details": None}
        assert "INTERNAL-ONLY-BODY" not in response.text and "root:x" not in response.text
    # The detail is kept for the operator -- in the server log, without the bearer token.
    logged = " ".join(str(call) for call in server_log.error.call_args_list)
    assert len(server_log.error.call_args_list) == len(cases)
    assert "INTERNAL-ONLY-BODY" in logged
    assert "bridge-token" not in logged

    # A destination that stops passing the policy between validation and the request.
    fake_bridge.health_error = MTBridgeError("refused", failure_kind="DESTINATION_NOT_ALLOWED")
    changed = _test_bridge(api_client, "https://93.184.216.34:8443").json()
    assert changed["ok"] is False and "Destination not allowed" in changed["error"]


def test_mt_bridge_client_hardening(monkeypatch):
    """The real client: guard before every request, no redirects, no response body in errors."""
    from unittest.mock import Mock

    import app.exchange.mt_bridge.client as bridge_module
    from app.exchange.mt_bridge.client import MTBridgeClient, harden_bridge_client

    server_log = Mock()
    monkeypatch.setattr(bridge_module, "logger", server_log)
    from app.exchange.mt_bridge.errors import MTBridgeConnectionError, MTBridgeError
    from shared_lib.core.security.url_guard import UnsafeDestinationError

    client = MTBridgeClient("https://93.184.216.34:8443", "bridge-secret-token")
    guard_calls = []
    harden_bridge_client(client, lambda: guard_calls.append(1))
    assert client._session.max_redirects == 0

    with patch.object(client._session, "request") as request:
        request.return_value = Mock(status_code=200, json=Mock(return_value={"ok": True}))
        client.get_health()
        client.get_health()
        assert len(guard_calls) == 2 and request.call_count == 2          # once per request, before it

        # The guard refuses: nothing is sent, and the caller gets a bridge error.
        def refuse():
            raise UnsafeDestinationError("DESTINATION_NOT_ALLOWED", "rebound to 127.0.0.1")

        client.destination_guard = refuse
        with pytest.raises(MTBridgeConnectionError) as refused:
            client.get_health()
        assert refused.value.failure_kind == "DESTINATION_NOT_ALLOWED" and request.call_count == 2
        client.destination_guard = None

        # A non-JSON answer: status code only in the error; the body goes to the log, token removed.
        request.return_value = Mock(status_code=502, text="<html>internal bridge-secret-token page</html>",
                                    json=Mock(side_effect=ValueError("no json")))
        with pytest.raises(MTBridgeError) as invalid:
            client.get_health()
        assert str(invalid.value) == "Invalid JSON response from bridge (HTTP 502)"
        assert invalid.value.status_code == 502 and invalid.value.failure_kind == "INVALID_RESPONSE"
        logged = str(server_log.warning.call_args)
        assert "internal" in logged and "bridge-secret-token" not in logged

        request.return_value = Mock(status_code=401, json=Mock(return_value={"error": "Invalid token",
                                                                             "error_code": "UNAUTHORIZED"}))
        with pytest.raises(MTBridgeError) as rejected:
            client.get_health()
        assert rejected.value.status_code == 401 and rejected.value.error_code == "UNAUTHORIZED"
        assert "Invalid token" in str(rejected.value)                    # engine-side message is unchanged


def test_exchange_factory_guards_a_stored_bridge_url(monkeypatch):
    """A stored bridge URL gets the same policy when the engine builds its client."""
    from app.core import config
    from app.exchange import factory

    # Outside production (this process): relaxed -- nothing is resolved or refused.
    assert factory._guard_stored_bridge_url("http://localhost:8000/?x=#", "mt5") is None

    monkeypatch.setattr(config, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="10.0.0.5:8443"))
    for url, reason in [
        ("http://93.184.216.34:8443", "HTTPS_REQUIRED"),
        ("https://93.184.216.34/#", "URL_QUERY_NOT_ALLOWED"),
        ("https://93.184.216.34/admin?x=", "URL_QUERY_NOT_ALLOWED"),
        ("https://169.254.169.254/", "DESTINATION_BLOCKED"),
        ("https://127.0.0.1:8443", "DESTINATION_NOT_ALLOWED"),
        ("https://10.0.0.5:8443", "DESTINATION_NOT_ALLOWED"),          # the allow-list is not for stored user URLs
        ("https://user:hunter2@93.184.216.34/", "URL_CREDENTIALS_NOT_ALLOWED"),
    ]:
        with pytest.raises(ValueError) as refused:
            factory._guard_stored_bridge_url(url, "mt5")
        message = str(refused.value)
        assert message.startswith("MT_BRIDGE_URL_NOT_ALLOWED") and reason in message, (url, message)
        assert "hunter2" not in message and "93.184.216.34" not in message   # names the reason, never the URL

    guard = factory._guard_stored_bridge_url("https://93.184.216.34:8443", "mt4")
    assert callable(guard) and guard() == ("93.184.216.34",)


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
    opened: list = []
    closed: list = []

    async def get_session(self, connection_id, host="127.0.0.1", port=7496, client_id=1):
        type(self).calls.append((host, port))
        type(self).opened.append(connection_id)
        return _FakeIBKRSession()

    def close_session(self, connection_id):
        type(self).closed.append(connection_id)
        return True


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
    _FakeIBKRSessionManager.opened = []
    _FakeIBKRSessionManager.closed = []
    for module in (session_module, ibkr_api):
        monkeypatch.setattr(module, "IBKRSessionManager", _FakeIBKRSessionManager)
    for module in (client_module, ibkr_api):
        monkeypatch.setattr(module, "IBKRClient", _FakeIBKRClient)
    return _FakeIBKRSessionManager


def _test_ibkr(api_client, as_admin=False, **credentials):
    return api_client.post(
        "/api/v1/brokers/test-connection",
        headers=_auth(ADMIN, "admin") if as_admin else _auth(ALICE),
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
    # A test connection is one-off: its session is closed again, not left in the cache.
    assert len(fake_ibkr.opened) == 1 and fake_ibkr.closed == fake_ibkr.opened

    # Production: loopback only when allow-listed (host AND port) ...
    fake_ibkr.calls.clear()
    monkeypatch.setattr(brokers, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="127.0.0.1:4001"))
    assert _test_ibkr(api_client, as_admin=True, host="127.0.0.1", port=7496).json()["ok"] is False
    assert _test_ibkr(api_client, as_admin=True, host="10.1.2.3", port=4001).json()["ok"] is False
    assert fake_ibkr.calls == []
    # ... and only for an admin: the listed gateway is the platform's own, and
    # an ordinary user must not be able to read its accounts and equity.
    as_user = _test_ibkr(api_client, host="127.0.0.1", port=4001).json()
    assert as_user["ok"] is False and "DESTINATION_NOT_ALLOWED" in as_user["error"], as_user
    assert as_user["details"] is None
    assert fake_ibkr.calls == []
    as_admin = _test_ibkr(api_client, as_admin=True, host="127.0.0.1", port=4001).json()
    assert as_admin["ok"] is True and as_admin["details"]["accounts"] == ["U1234567"]
    assert fake_ibkr.calls == [("127.0.0.1", 4001)]
    assert fake_ibkr.closed == fake_ibkr.opened and len(fake_ibkr.opened) == 2
    # An ordinary user can still test their own PUBLIC gateway in production.
    assert _test_ibkr(api_client, host="93.184.216.34", port=4001).json()["ok"] is True


def test_brokers_ibkr_link_flow_guards_destination(api_client, fake_ibkr, monkeypatch):
    from app.api import brokers

    refused = api_client.post("/api/v1/brokers/ibkr/connect/start", headers=_auth(ALICE),
                              json={"host": "169.254.169.254", "port": 80})
    assert refused.status_code == 200
    assert refused.json()["status"] == "error" and "Destination not allowed" in refused.json()["message"]
    assert fake_ibkr.calls == []

    # Production, default local gateway allow-listed: admins only, and the
    # discovery session is closed once the accounts have been read.
    monkeypatch.setattr(brokers, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="127.0.0.1:4001"))
    as_user = api_client.post("/api/v1/brokers/ibkr/connect/start", headers=_auth(ALICE), json={})
    assert as_user.json()["status"] == "error" and "DESTINATION_NOT_ALLOWED" in as_user.json()["message"]
    assert fake_ibkr.calls == []
    as_admin = api_client.post("/api/v1/brokers/ibkr/connect/start", headers=_auth(ADMIN, "admin"), json={})
    assert as_admin.json() == {"status": "connected", "accounts": ["U1234567"], "connect_url": None}
    assert fake_ibkr.calls == [("127.0.0.1", 4001)]
    assert len(fake_ibkr.opened) == 1 and fake_ibkr.closed == fake_ibkr.opened


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
    # Listed by the operator: still refused for an ordinary user (same answer
    # as for an unlisted address), accepted for an admin.
    monkeypatch.setattr(ibkr_api, "settings", _production_settings(BROKER_GATEWAY_ALLOWED_HOSTS="127.0.0.1:7496"))
    as_user = api_client.post("/api/v1/ibkr/connect/start", headers=alice, json={"host": "127.0.0.1", "port": 7496})
    assert as_user.status_code == 400 and as_user.json() == prod.json()
    assert fake_ibkr.calls == []
    listed = api_client.post("/api/v1/ibkr/connect/start", headers=_auth(ADMIN, "admin"),
                             json={"host": "127.0.0.1", "port": 7496})
    assert listed.status_code == 200 and listed.json()["status"] == "connected", listed.json()
    assert fake_ibkr.calls == [("127.0.0.1", 7496)]


def test_ibkr_connection_and_session_caches_are_bounded():
    """Neither per-request cache can grow without limit."""
    import app.api.ibkr as ibkr_api
    from app.exchange.ibkr.session import IBKRSessionManager

    # Connection-flow records: capped, and expired after the TTL.
    records = ibkr_api.ConnectionManager()
    for index in range(records.MAX_CONNECTIONS + 25):
        records.record_connection(f"conn-{index}", {"user_id": ALICE, "status": "connected"})
    assert len(records._connections) == records.MAX_CONNECTIONS == len(records._recorded_at)
    assert records.get_connection("conn-0") is None                         # oldest evicted first
    newest = f"conn-{records.MAX_CONNECTIONS + 24}"
    assert records.get_connection(newest, user_id=ALICE) is not None
    records._recorded_at[newest] = time.monotonic() - records.TTL_SECONDS - 1
    assert records.get_connection(newest, user_id=ALICE) is None            # expired
    assert newest not in records._recorded_at

    # TWS sessions: closed when idle too long, least recently used closed beyond the cap.
    class _Session:
        def __init__(self):
            self.disconnected = False

        def disconnect(self):
            self.disconnected = True

    manager = object.__new__(IBKRSessionManager)                             # not the process-wide singleton
    manager._sessions, manager._last_used = {}, {}
    now = time.monotonic()
    sessions = {}
    for index in range(manager.MAX_SESSIONS):
        sessions[index] = manager._sessions[f"s-{index}"] = _Session()
        manager._last_used[f"s-{index}"] = now - (manager.MAX_SESSIONS - index)   # s-0 is the least recent
    manager._last_used["s-5"] = now - manager.SESSION_IDLE_TTL_SECONDS - 1         # idle for too long
    manager._evict_sessions(now)
    assert sessions[5].disconnected and "s-5" not in manager._sessions
    assert len(manager._sessions) == manager.MAX_SESSIONS - 1                      # room for exactly one more
    manager._sessions["s-new"], manager._last_used["s-new"] = _Session(), now
    manager._evict_sessions(now)
    assert sessions[0].disconnected and "s-0" not in manager._sessions             # least recently used goes
    assert len(manager._sessions) == manager.MAX_SESSIONS - 1 and "s-new" in manager._sessions

    assert manager.close_session("s-new") is True and "s-new" not in manager._last_used
    assert manager.close_session("s-new") is False


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


def test_shadow_compare_scopes_real_trades_to_the_named_bot(db):
    """The 'real' side of /analytics/compare is filtered by the same bot as the shadow side.

    It used to aggregate trade_fills with no bot filter, so a user naming their
    own bot received the platform-wide trade count, win rate and realised PnL.
    """
    from datetime import datetime

    from app.shadow.analytics import ShadowAnalytics

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def fill(bot_id, action, pnl):
        _insert_row(db, "trade_fills", bot_instance_id=bot_id, symbol="BTCUSDT", side="LONG", action=action,
                    qty=1.0, price=100.0, realized_pnl=pnl, timestamp_utc=now)

    fill("bot-alice", "OPEN", None)
    fill("bot-alice", "CLOSE", -5.0)                       # Alice: one losing round trip
    for _ in range(3):                                     # Bob: three large winners
        fill("bot-bob", "OPEN", None)
        fill("bot-bob", "CLOSE", 1000.0)

    analytics = ShadowAnalytics(db)
    alice = analytics.compare_vs_real_trades(days=30, bot_instance_id="bot-alice")["real"]
    assert alice["source"] == "real"
    assert (alice["trade_count"], alice["win_rate_pct"], alice["total_pnl_net"]) == (1, 0.0, -5.0)
    bob = analytics.compare_vs_real_trades(days=30, bot_instance_id="bot-bob")["real"]
    assert (bob["trade_count"], bob["win_rate_pct"], bob["total_pnl_net"]) == (3, 100.0, 3000.0)
    # A bot with no fills sees nothing of anyone else's.
    nobody = analytics.compare_vs_real_trades(days=30, bot_instance_id="bot-without-fills")["real"]
    assert nobody["trade_count"] == 0 and not nobody["total_pnl_net"] and nobody["win_rate_pct"] is None
    # No bot named = all bots; the route only lets an admin ask for that
    # (test_shadow_routes_are_scoped_to_the_callers_own_bot: 403 for a user).
    everyone = analytics.compare_vs_real_trades(days=30, bot_instance_id=None)["real"]
    assert (everyone["trade_count"], everyone["total_pnl_net"]) == (4, 2995.0)


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


def test_public_health_hides_process_details_from_remote_callers(api_client):
    """/health is unauthenticated: pid / install path / interpreter path are for loopback callers only."""
    import app.main as main

    # The test client's peer is not loopback: this is what the outside world gets.
    response = api_client.get("/health")
    assert response.status_code == 200
    body = response.json()
    fingerprint = body["tradingview_runtime_fingerprint"]
    for field in ("pid", "working_directory", "python_executable"):
        assert field in main._HEALTH_LOCAL_ONLY_FINGERPRINT_FIELDS and field not in fingerprint, field
    # Everything a readiness / ops check reads is still there.
    assert body["status"] == "ok" and "components" in body and "time_utc" in body
    assert "phase6_gate_code_version" in fingerprint and "active_safety_lockout" in fingerprint

    def request(host, **headers):
        return SimpleNamespace(client=SimpleNamespace(host=host) if host else None, headers=headers)

    assert main._health_request_is_local(request("127.0.0.1")) is True
    assert main._health_request_is_local(request("::1")) is True
    assert main._health_request_is_local(request("203.0.113.9")) is False
    assert main._health_request_is_local(request(None)) is False
    assert main._health_request_is_local(None) is False               # called without a request: closed
    # A request relayed by the reverse proxy arrives FROM loopback but is not a local caller.
    assert main._health_request_is_local(request("127.0.0.1", **{"x-forwarded-for": "203.0.113.9"})) is False
    assert main._health_request_is_local(request("127.0.0.1", **{"x-real-ip": "203.0.113.9"})) is False

    full = {
        "status": "ok",
        "runtime": {"state": "HEALTHY", "pid": 4242, "lease": {"held_by_this_process": True}},
        "tradingview_runtime_fingerprint": {"pid": 4242, "working_directory": "/opt/cosmicforge",
                                            "python_executable": "/opt/venv/bin/python", "code_version": "abc1234"},
    }
    assert main._redact_health_for_remote(full) == {
        "status": "ok",
        "runtime": {"state": "HEALTHY", "lease": {"held_by_this_process": True}},
        "tradingview_runtime_fingerprint": {"code_version": "abc1234"},
    }
    assert full["runtime"]["pid"] == 4242 and "pid" in full["tradingview_runtime_fingerprint"]   # not mutated
    # The authenticated operator route still serves the full fingerprint.
    admin_view = api_client.get("/api/admin/tradingview/runtime-fingerprint", headers=_auth(ADMIN, "admin")).json()
    assert {"pid", "working_directory", "python_executable"} <= set(admin_view)


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


def test_webhook_with_invalid_token_persists_nothing(tmp_path, monkeypatch):
    """An unauthenticated request must not be able to write rows -- least of all under a bot it names."""
    from app.api import tradingview
    from shared_lib.persistence.tradingview import list_alerts, list_decisions

    client, tv_db, seeded = _tv_client(tmp_path, monkeypatch)
    before = tradingview.unauthenticated_rejection_count()

    no_token = _tv_payload("unused", bot_id="victim-bot")
    no_token.pop("token")
    attempts = [
        ("not-a-token", _tv_payload("wrong-token", bot_id="victim-bot", alert_id="forged-1")),
        (seeded["id"], _tv_payload("wrong-token", bot_id="victim-bot", alert_id="forged-2")),   # known id, bad token
        ("not-a-token", no_token),
    ]
    for path_token, payload in attempts:
        response = client.post(f"/api/v1/tradingview/webhook/{path_token}", json=payload)
        body = response.json()
        # The caller sees exactly what it saw before.
        assert response.status_code == 200
        assert (body["status"], body["reason"], body["execution_enabled"]) == ("rejected", "INVALID_TOKEN", False)

    assert list_alerts(tv_db) == [] and list_decisions(tv_db) == []
    with tv_db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0] == 0
    # Counted (and logged) instead.
    assert tradingview.unauthenticated_rejection_count() == before + len(attempts)

    # A valid token behaves exactly as before: accepted alerts and authenticated rejections are persisted.
    url = f"/api/v1/tradingview/webhook/{seeded['token']}"
    assert client.post(url, json=_tv_payload(seeded["token"])).json()["status"] == "accepted"
    rejected = client.post(url, json=_tv_payload(seeded["token"], alert_id="alert-close", action="CLOSE"))
    assert rejected.json()["status"] == "rejected"
    rows = list_alerts(tv_db)
    assert len(rows) == 2 and {row["bot_id"] for row in rows} == {"bot-tv-test"}
    assert tradingview.unauthenticated_rejection_count() == before + len(attempts)


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
