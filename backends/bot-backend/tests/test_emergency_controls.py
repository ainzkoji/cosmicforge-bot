"""Operator emergency controls: the kill switch and flatten are real in production.

* the kill switch API writes the persisted governance control the execution
  boundary consults before every entry;
* flatten sets the kill switch first, takes the broker-cycle lock, goes through
  the runtime's own close path and never reports success for nothing;
* the per-account broker client is reused across cycles and rebuilt on doubt.
"""
from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import ANY, Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import emergency
from app.core import config
from app.core.auth import (
    EMERGENCY_ACTOR_CLAIM,
    EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS,
    require_admin,
    require_admin_emergency,
)
from app.core.config import Settings
from app.ops import runtime_shutdown
from app.trading_intelligence.governance.promotion import GovernanceAuthority, PromotionGovernance
from app.trading_intelligence.integration import production_execution, production_runtime as runtime
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

ADMIN = "admin-1"
BASE = "/api/v1/admin/emergency"


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "emergency.db"))
    migrate(database)
    return database


@pytest.fixture
def api(db):
    app = FastAPI()
    app.include_router(emergency.router)
    app.dependency_overrides[require_admin_emergency] = lambda: ADMIN
    app.dependency_overrides[emergency.get_db] = lambda: db
    return TestClient(app)


def _emergency_token(*, role="admin", act=EMERGENCY_ACTOR_CLAIM, lifetime=60, sub=ADMIN, issued_ago=0, **extra):
    """A token signed like the user-backend's; defaults = what its admin emergency proxy mints."""
    from jose import jwt

    from app.core import security
    from app.core.security import AUDIENCE, ISSUER

    issued_at = int(time.time()) - issued_ago
    claims = {"sub": sub, "type": "access", "role": role, "iss": ISSUER, "aud": AUDIENCE,
              "iat": issued_at, "exp": issued_at + lifetime, **extra}
    if act is not None:
        claims["act"] = act
    # The very settings object decode_token verifies with.
    return jwt.encode(claims, security.settings.SECRET_KEY, algorithm=security.settings.ALGORITHM)


def production_settings(demo=True, live=False):
    return Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production",
                    EXECUTION_MODE="live", DEMO_ORDER_SUBMISSION_ENABLED=demo, LIVE_ORDER_SUBMISSION_ENABLED=live)


FLATTEN = {"scope": "all", "account_id": None, "confirm": "FLATTEN", "reason": "incident drill"}


# ── Kill switch ──────────────────────────────────────────────────────────────

def test_every_emergency_route_is_admin_only(db):
    app = FastAPI()
    app.include_router(emergency.router)
    app.dependency_overrides[emergency.get_db] = lambda: db
    client = TestClient(app)
    assert client.get(f"{BASE}/status").status_code == 401
    assert client.post(f"{BASE}/kill-switch", json={"enabled": True, "reason": "x y z"}).status_code == 401
    assert client.post(f"{BASE}/flatten", json=FLATTEN).status_code == 401
    assert not PromotionGovernance(db).kill_switch_on()
    for route in emergency.router.routes:
        calls = [d.call for d in route.dependant.dependencies]
        # The dedicated emergency dependency, not the generic role check.
        assert require_admin_emergency in calls and require_admin not in calls, route.path


def test_emergency_routes_require_the_admin_emergency_service_token(db):
    """role=admin alone is not enough: an end-user account with users.role='admin' holds such a token.

    Only the short-lived token the user-backend admin emergency proxy mints for
    a verified ``admins``-table operator (``act`` claim + lifetime) is accepted.
    """
    app = FastAPI()
    app.include_router(emergency.router)
    app.dependency_overrides[emergency.get_db] = lambda: db
    client = TestClient(app)

    def call(token, method="get", path="/status", **kwargs):
        return getattr(client, method)(f"{BASE}{path}", headers={"Authorization": f"Bearer {token}"}, **kwargs)

    refused = {
        "ordinary admin-role access token (no act claim)": _emergency_token(act=None, lifetime=600),
        "admin-role token with a short lifetime but no act claim": _emergency_token(act=None),
        "wrong act claim": _emergency_token(act="admin-portal"),
        "right act claim on a long-lived token": _emergency_token(lifetime=EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS + 1),
        "right act claim without the admin role": _emergency_token(role="user"),
    }
    for label, token in refused.items():
        assert call(token).status_code == 403, label
        assert call(token, "post", "/kill-switch", json={"enabled": True, "reason": "incident drill"}).status_code == 403, label
        assert call(token, "post", "/flatten", json=FLATTEN).status_code == 403, label
    assert not PromotionGovernance(db).kill_switch_on()                    # none of them changed anything
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM events WHERE event_type='EMERGENCY'").fetchone()[0] == 0

    # Not a valid token at all: 401, as before.
    assert call("not-a-jwt").status_code == 401
    assert call(_emergency_token(issued_ago=3600)).status_code == 401      # expired
    refresh = _emergency_token(type="refresh")
    assert call(refresh).status_code == 401

    # The proxy's token: accepted, and the audit trail names the operator in ``sub``.
    token = _emergency_token()
    assert call(token).status_code == 200
    on = call(token, "post", "/kill-switch", json={"enabled": True, "reason": "incident drill"})
    assert on.status_code == 200 and on.json()["kill_switch"]["set_by"] == f"admin:{ADMIN}"
    assert PromotionGovernance(db).kill_switch_on()
    # At the limit of the allowed lifetime it is still that token.
    assert call(_emergency_token(lifetime=EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS)).status_code == 200

    # require_admin itself is unchanged: every other admin route keeps accepting a plain admin token.
    assert require_admin(_emergency_token(act=None, lifetime=600)) == ADMIN
    assert require_admin_emergency(token) == ADMIN


def test_status_reports_the_contract_shape(api):
    body = api.get(f"{BASE}/status").json()
    assert set(body) == {"kill_switch", "live_order_submission_enabled", "demo_order_submission_enabled",
                         "open_positions", "generated_at"}
    assert body["kill_switch"] == {"enabled": False, "reason": None, "set_at": None, "set_by": None}
    assert body["live_order_submission_enabled"] is False
    assert isinstance(body["demo_order_submission_enabled"], bool)
    assert body["open_positions"] is None                   # no local broker snapshot: unknown, not "flat"
    assert body["generated_at"]


def test_kill_switch_is_the_control_the_entry_authority_consults(api, db):
    plan = SimpleNamespace(environment="DEMO", broker_account_id="acct", venue="BINANCE_USDM")
    assert api.post(f"{BASE}/kill-switch", json={"enabled": True, "reason": ""}).status_code == 400
    assert not PromotionGovernance(db).kill_switch_on()

    on = api.post(f"{BASE}/kill-switch", json={"enabled": True, "reason": "incident drill"})
    assert on.status_code == 200
    switch = on.json()["kill_switch"]
    assert switch["enabled"] is True and "incident drill" in switch["reason"]
    assert switch["set_by"] == f"admin:{ADMIN}" and switch["set_at"]
    assert PromotionGovernance(db).kill_switch_on(scope="any-account")
    assert GovernanceAuthority(db).authorize_entry(plan) == (False, "CATI_NEW_ENTRY_KILL_SWITCH")
    assert api.get(f"{BASE}/status").json()["kill_switch"]["enabled"] is True

    off = api.post(f"{BASE}/kill-switch", json={"enabled": False, "reason": "drill over"})
    assert off.status_code == 200 and off.json()["kill_switch"]["enabled"] is False
    assert not PromotionGovernance(db).kill_switch_on()
    with db.connect() as conn:
        actions = [r[0] for r in conn.execute("SELECT action FROM events WHERE event_type='EMERGENCY' ORDER BY id")]
    assert actions.count("KILL_SWITCH_SET") == 2 and "KILL_SWITCH_REQUESTED" in actions


def test_kill_switch_status_reads_the_latest_record_per_scope(db):
    gov = PromotionGovernance(db)
    assert gov.kill_switch_status() == {"enabled": False, "reason": None, "set_at_ms": None, "set_by": None,
                                        "scope": "GLOBAL"}
    gov.set_kill_switch(True, reason="first", actor_ref="op", now_ms=1_000)
    gov.set_kill_switch(False, reason="cleared", actor_ref="op2", now_ms=2_000)
    gov.set_kill_switch(True, reason="only this account", actor_ref="op3", scope="acct", now_ms=3_000)
    assert gov.kill_switch_status() == {"enabled": False, "reason": "cleared", "set_at_ms": 2_000, "set_by": "op2",
                                        "scope": "GLOBAL"}
    assert gov.kill_switch_status("acct")["enabled"] is True


def test_open_positions_come_from_the_local_snapshot_only_when_it_is_current(api, db):
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, status, environment, created_at,"
                     " updated_at) VALUES ('acct','u1','binance','crypto','connected','demo','2026-01-01','2026-01-01')")
    runtime.initialize(db)
    now = int(time.time() * 1000)
    runtime.save(db, "acct", "u1", now, {"positions": [{"symbol": "ADAUSDT", "positionAmt": "-3"},
                                                        {"symbol": "ETHUSDT", "positionAmt": "0"}]})
    assert api.get(f"{BASE}/status").json()["open_positions"] == [
        {"account_id": "acct", "user_id": "u1", "symbol": "ADAUSDT", "side": "SHORT", "qty": 3.0}]
    runtime.save(db, "acct", "u1", now - emergency.SNAPSHOT_MAX_AGE_MS - 1_000, {"positions": []})
    assert api.get(f"{BASE}/status").json()["open_positions"] is None      # stale: never shown as flat
    runtime.save(db, "acct", "u1", now, {"positions": None})
    assert api.get(f"{BASE}/status").json()["open_positions"] is None      # the last read failed


# ── Flatten API ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("body", [
    {**FLATTEN, "confirm": "flatten"}, {**FLATTEN, "confirm": ""}, {"scope": "all", "reason": "incident drill"},
    {**FLATTEN, "scope": "account"}, {**FLATTEN, "scope": "everything"}, {**FLATTEN, "reason": " "},
])
def test_flatten_refuses_an_unconfirmed_or_malformed_request_before_doing_anything(api, db, monkeypatch, body):
    engine = Mock()
    monkeypatch.setattr(runtime, "flatten", engine)
    assert api.post(f"{BASE}/flatten", json=body).status_code == 400
    engine.assert_not_called()
    assert not PromotionGovernance(db).kill_switch_on()


def test_flatten_without_a_production_engine_is_a_409_never_a_success(api, db):
    response = api.post(f"{BASE}/flatten", json=FLATTEN)
    assert response.status_code == 409
    body = response.json()
    assert body["ok"] is False and body["kill_switch_enabled"] is True and body["results"] == []
    assert body["reason"] == "CATI_PRODUCTION_PROFILE_REQUIRED" and "NO position was closed" in body["detail"]
    # The kill switch was still set first: nothing can re-enter.
    assert PromotionGovernance(db).kill_switch_on()


def test_flatten_sets_the_kill_switch_before_the_engine_acts(api, db, monkeypatch):
    seen = {}

    def engine(database, *, account_id, request_id):
        seen.update(kill_switch=PromotionGovernance(database).kill_switch_on(), account_id=account_id,
                    request_id=request_id)
        return [{"account_id": "acct", "symbol": "ADAUSDT", "status": "closed", "detail": "ok", "extra": "dropped"},
                {"account_id": "acct2", "symbol": None, "status": "no_position", "detail": None}]

    monkeypatch.setattr(runtime, "flatten", engine)
    response = api.post(f"{BASE}/flatten", json={**FLATTEN, "scope": "account", "account_id": "acct"})
    assert response.status_code == 200
    assert response.json() == {"ok": True, "kill_switch_enabled": True, "results": [
        {"account_id": "acct", "symbol": "ADAUSDT", "status": "closed", "detail": "ok"},
        {"account_id": "acct2", "symbol": "*", "status": "no_position", "detail": ""}]}      # always strings
    assert seen["kill_switch"] is True and seen["account_id"] == "acct" and seen["request_id"]
    with db.connect() as conn:
        actions = [r[0] for r in conn.execute("SELECT action FROM events WHERE event_type='EMERGENCY' ORDER BY id")]
    assert actions[0] == "FLATTEN_REQUESTED" and actions[-1] == "FLATTEN_COMPLETED"


@pytest.mark.parametrize("results", [
    [{"account_id": "a", "symbol": "ADAUSDT", "status": "closed", "detail": "ok"},
     {"account_id": "b", "symbol": "ETHUSDT", "status": "failed", "detail": "CLOSE_SUBMIT_OUTCOME_UNKNOWN"}],
    [{"account_id": "a", "symbol": "ADAUSDT", "status": "something-new", "detail": ""}],
    # "submitted" without the evidence that THIS request sent an acknowledged order
    # (e.g. an earlier close that is merely still unconfirmed) is not a success.
    [{"account_id": "a", "symbol": "ADAUSDT", "status": "submitted", "detail": "CLOSE_FILL_OR_FLAT_UNCONFIRMED"}],
    [{"account_id": "a", "symbol": "ADAUSDT", "status": "closed", "detail": "ok"},
     {"account_id": "a", "symbol": "ETHUSDT", "status": "submitted", "detail": ""}],
    [],
])
def test_flatten_is_a_502_whenever_any_close_failed_or_nothing_was_acted_on(api, db, monkeypatch, results):
    monkeypatch.setattr(runtime, "flatten", lambda database, **kw: results)
    response = api.post(f"{BASE}/flatten", json=FLATTEN)
    assert response.status_code == 502
    body = response.json()
    assert body["ok"] is False and body["kill_switch_enabled"] is True
    # Whatever the engine called it, nothing unproven is left looking like a success.
    assert all(r["status"] != "submitted" for r in body["results"])
    assert not results or any(r["status"] == "failed" or r["status"] == "something-new" for r in body["results"])
    assert PromotionGovernance(db).kill_switch_on()


def test_flatten_accepts_submitted_only_with_the_venue_acknowledgement_evidence(api, db, monkeypatch):
    assert emergency.SUBMITTED_EVIDENCE == production_execution.SUBMITTED_DETAIL_PREFIX
    acknowledged = f"{emergency.SUBMITTED_EVIDENCE}:CLOSE_FILL_OR_FLAT_UNCONFIRMED:CLOSE_ORDER_WORKING"
    monkeypatch.setattr(runtime, "flatten", lambda database, **kw: [
        {"account_id": "a", "symbol": "ADAUSDT", "status": "submitted", "detail": acknowledged},
        {"account_id": "a", "symbol": "ETHUSDT", "status": "closed", "detail": "ok"}])
    response = api.post(f"{BASE}/flatten", json=FLATTEN)
    assert response.status_code == 200 and response.json()["ok"] is True
    assert response.json()["results"][0] == {"account_id": "a", "symbol": "ADAUSDT", "status": "submitted",
                                             "detail": acknowledged}
    # The same row without the evidence: reported failed, with the reason, as a 502.
    monkeypatch.setattr(runtime, "flatten", lambda database, **kw: [
        {"account_id": "a", "symbol": "ADAUSDT", "status": "submitted", "detail": "CLOSE_FILL_OR_FLAT_UNCONFIRMED"}])
    response = api.post(f"{BASE}/flatten", json=FLATTEN)
    assert response.status_code == 502 and response.json()["ok"] is False
    assert response.json()["results"] == [{
        "account_id": "a", "symbol": "ADAUSDT", "status": "failed",
        "detail": "SUBMITTED_WITHOUT_VENUE_ACKNOWLEDGEMENT:CLOSE_FILL_OR_FLAT_UNCONFIRMED"}]


def test_flatten_engine_crash_is_a_502_with_the_kill_switch_on(api, db, monkeypatch):
    monkeypatch.setattr(runtime, "flatten", Mock(side_effect=RuntimeError("boom")))
    response = api.post(f"{BASE}/flatten", json=FLATTEN)
    assert response.status_code == 502 and response.json()["ok"] is False
    assert response.json()["results"][0]["status"] == "failed"
    assert PromotionGovernance(db).kill_switch_on()


# ── Flatten in the runtime: gates, lock, per-account isolation ───────────────

ACCOUNTS = [{"id": "a", "user_id": "u", "broker_id": "binance", "environment": "DEMO", "status": "connected"},
            {"id": "b", "user_id": "u", "broker_id": "binance", "environment": "DEMO", "status": "connected"},
            {"id": "c", "user_id": "u", "broker_id": "oanda", "environment": "DEMO", "status": "connected"}]


@pytest.fixture
def engine(monkeypatch):
    settings = production_settings()
    monkeypatch.setattr(config, "settings", settings)
    monkeypatch.setattr(runtime, "settings", settings)
    monkeypatch.setattr(runtime, "owner_current", lambda db: True)
    monkeypatch.setattr(runtime, "execution_accounts", lambda db: [dict(a) for a in ACCOUNTS])
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda account, user, db: SimpleNamespace(
        account_id=account, user_id=user, broker_type="binance", environment="demo", base_url="https://x",
        credential_version=1, key_fingerprint="fp"))
    calls = []

    def flatten_account(db, account, client, *, request_id):
        calls.append((account["id"], account["environment"], client.account_id, request_id))
        if account["id"] == "a":
            raise ValueError("BROKER_READ_SHAPE_INVALID")
        return [{"account_id": account["id"], "symbol": "ADAUSDT", "status": "closed", "detail": "ok"}]

    monkeypatch.setattr(production_execution, "flatten_account", flatten_account)
    runtime._clients.clear()
    yield SimpleNamespace(calls=calls, factory=lambda auth: SimpleNamespace(account_id=auth.account_id))
    runtime._clients.clear()


def test_runtime_flatten_isolates_accounts_and_reports_each_one(engine):
    results = runtime.flatten("db", request_id="req", factory=engine.factory)
    assert engine.calls == [("a", "DEMO", "a", "req"), ("b", "DEMO", "b", "req")]
    assert results == [
        {"account_id": "a", "symbol": "*", "status": "failed", "detail": "BROKER_READ_SHAPE_INVALID"},
        {"account_id": "b", "symbol": "ADAUSDT", "status": "closed", "detail": "ok"},
        {"account_id": "c", "symbol": "*", "status": "failed", "detail": "EMERGENCY_FLATTEN_UNSUPPORTED_BROKER"}]
    assert runtime.flatten("db", account_id="b", request_id="req2", factory=engine.factory) == [results[1]]
    assert runtime._cycle_lock.acquire(blocking=False)      # released again
    runtime._cycle_lock.release()


def test_a_rate_limited_account_is_reported_failed_at_once_and_others_still_flatten(engine, monkeypatch):
    from app.exchange.binance.client import BinanceRateLimited

    def flatten_account(db, account, client, *, request_id):
        if account["id"] == "a":                            # its pre-close read met the venue's backoff
            raise BinanceRateLimited("Binance rate limit backoff: read deferred for 30s (HTTP 418)")
        return [{"account_id": account["id"], "symbol": "ADAUSDT", "status": "closed", "detail": "ok"}]

    monkeypatch.setattr(production_execution, "flatten_account", flatten_account)
    results = runtime.flatten("db", request_id="req", factory=engine.factory)
    assert results[:2] == [
        {"account_id": "a", "symbol": "*", "status": "failed", "detail": "BinanceRateLimited"},
        {"account_id": "b", "symbol": "ADAUSDT", "status": "closed", "detail": "ok"}]


@pytest.mark.parametrize("case,reason", [
    ("profile", "CATI_PRODUCTION_PROFILE_REQUIRED"), ("lease", "CANONICAL_RUNTIME_LEASE_REQUIRED"),
    ("gates", "ORDER_SUBMISSION_DISABLED"), ("account", "EXECUTION_ACCOUNT_NOT_FOUND"),
    ("none", "BROKER_ACCOUNT_REQUIRED"), ("busy", "BROKER_CYCLE_BUSY")])
def test_runtime_flatten_refuses_when_the_engine_cannot_act(engine, monkeypatch, case, reason):
    kwargs, holder, release = {}, None, threading.Event()
    if case == "profile":
        monkeypatch.setattr(runtime, "settings", SimpleNamespace(production=False))
    elif case == "lease":
        monkeypatch.setattr(runtime, "owner_current", lambda db: False)
    elif case == "gates":
        monkeypatch.setattr(config, "settings", production_settings(demo=False))
    elif case == "account":
        kwargs["account_id"] = "missing"
    elif case == "none":
        monkeypatch.setattr(runtime, "execution_accounts", lambda db: [])
    else:                                                   # a broker cycle holds the lock
        held = threading.Event()

        def cycle():
            with runtime._cycle_lock:
                held.set()
                release.wait(10)

        holder = threading.Thread(target=cycle)
        holder.start()
        assert held.wait(5)
        kwargs["lock_wait"] = 0.05
    try:
        with pytest.raises(runtime.EngineUnavailable, match=reason):
            runtime.flatten("db", request_id="req", factory=engine.factory, **kwargs)
    finally:
        release.set()
        if holder:
            holder.join(5)
    assert engine.calls == []


def test_the_broker_cycle_holds_the_lock_flatten_takes(monkeypatch):
    from app.execution import demo_boundary_certification, demo_transport_smoke
    runtime_shutdown.reset_for_tests()
    observed = []

    def sync(db):
        probe = threading.Thread(target=lambda: observed.append(runtime._cycle_lock.acquire(blocking=False)))
        probe.start()
        probe.join(5)

    monkeypatch.setattr(demo_transport_smoke, "process_local_request", lambda db: None)
    monkeypatch.setattr(demo_boundary_certification, "process_local_request", lambda db: None)
    monkeypatch.setattr(runtime, "sync", sync)
    runtime.broker_cycle("db")
    assert observed == [False] and runtime.wait_idle(0)
    assert runtime._cycle_lock.acquire(blocking=False)
    runtime._cycle_lock.release()


# ── One broker client per account across cycles ──────────────────────────────

def auth(version=1, fingerprint="fp", account="a"):
    return SimpleNamespace(account_id=account, user_id="u", broker_type="binance", environment="demo",
                           base_url="https://x", credential_version=version, key_fingerprint=fingerprint)


@pytest.fixture
def factory(monkeypatch):
    built = []

    def build(credentials):
        built.append(credentials)
        return SimpleNamespace(_production_intent_identity="left-over")

    monkeypatch.setattr(runtime, "_DEFAULT_CLIENT_FACTORY", build)
    runtime._clients.clear()
    yield SimpleNamespace(build=build, built=built)
    runtime._clients.clear()


def test_the_account_client_is_reused_across_cycles_with_a_clean_identity(factory):
    db = SimpleNamespace(path="/tmp/x.db")
    first = runtime.account_client(db, "a", auth(), factory.build)
    first._production_intent_identity = "plan|hash"
    assert runtime.account_client(db, "a", auth(), factory.build) is first
    assert first._production_intent_identity is None        # never inherited from the previous cycle
    assert runtime.account_client(db, "b", auth(account="b"), factory.build) is not first
    assert len(factory.built) == 2


def test_the_account_client_is_rebuilt_on_any_doubt(factory, monkeypatch):
    db = SimpleNamespace(path="/tmp/x.db")
    first = runtime.account_client(db, "a", auth(), factory.build)
    assert runtime.account_client(db, "a", auth(version=2), factory.build) is not first      # credentials rotated
    second = runtime.account_client(db, "a", auth(version=2), factory.build)
    assert runtime.account_client(db, "a", auth(version=2, fingerprint="other"), factory.build) is not second
    third = runtime.account_client(db, "a", auth(version=2, fingerprint="other"), factory.build)
    runtime.invalidate_client("a")                          # an error in the cycle
    fourth = runtime.account_client(db, "a", auth(version=2, fingerprint="other"), factory.build)
    assert fourth is not third
    monkeypatch.setattr(runtime, "CLIENT_CACHE_MAX_AGE_SECONDS", 0.)                         # too old
    assert runtime.account_client(db, "a", auth(version=2, fingerprint="other"), factory.build) is not fourth
    # Credentials whose change could not be detected are never cached.
    unversioned = auth(version=None, fingerprint=None)
    assert runtime.account_client(db, "z", unversioned, factory.build) is not \
        runtime.account_client(db, "z", unversioned, factory.build)


def test_an_injected_factory_is_never_cached(factory):
    injected = Mock(side_effect=lambda credentials: object())
    db = SimpleNamespace(path="/tmp/x.db")
    assert runtime.account_client(db, "a", auth(), injected) is not runtime.account_client(db, "a", auth(), injected)
    assert injected.call_count == 2 and not runtime._clients


# ── The cycle: one account's failure never costs another account ─────────────

def test_one_failing_account_does_not_skip_the_next_account(db, monkeypatch):
    runtime_shutdown.reset_for_tests()
    touched = []

    def sync_account(database, account, **kw):
        touched.append(account["id"])
        if account["id"] == "a":
            raise ValueError("CLOSE_SUBMIT_OUTCOME_UNKNOWN")

    monkeypatch.setattr(runtime, "owner_current", lambda database: True)
    monkeypatch.setattr(runtime, "execution_accounts", lambda database: [dict(a) for a in ACCOUNTS[:2]])
    monkeypatch.setattr(runtime, "sync_account", sync_account)
    runtime.sync(db)
    assert touched == ["a", "b"]


def test_position_maintenance_still_runs_when_an_auxiliary_sync_step_fails(db, monkeypatch):
    from app.activation import account_status
    client = Mock()
    client.get_balance.return_value = {"equity": 1000}
    client.position_risk.return_value = []
    client.open_orders.return_value = []
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: SimpleNamespace(environment="demo"))
    monkeypatch.setattr(account_status, "refresh_if_stale", Mock(side_effect=RuntimeError("discovery unavailable")))
    maintained = Mock()
    monkeypatch.setattr(production_execution, "maintain_only", maintained)
    account = {"id": "acct", "user_id": "u", "broker_id": "binance", "environment": "DEMO"}
    with pytest.raises(RuntimeError, match="discovery unavailable"):
        runtime.sync_account(db, account, factory=lambda credentials: client, execute=True)
    maintained.assert_called_once_with(db, ANY, client)
    assert maintained.call_args.args[1]["id"] == "acct"
    # A read-only sync never maintains (and never mutates) anything.
    with pytest.raises(RuntimeError, match="discovery unavailable"):
        runtime.sync_account(db, account, factory=lambda credentials: client, execute=False)
    maintained.assert_called_once()
