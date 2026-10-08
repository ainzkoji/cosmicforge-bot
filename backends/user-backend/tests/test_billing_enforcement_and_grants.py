"""Step 1.5 -- BILLING_ENFORCED and operator plan grants.

Billing off: entitlements block nothing on the permitted path. Billing on: the
existing rules apply. Grants: auditable, time-limited, distinguishable from
Stripe, never overwritten by webhooks while active, revocable only by an
administrator.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_billing  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from shared_lib.billing import enforcement, entitlements, operator_grants, webhooks  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
ADMIN = {"id": "admin-ops", "email": "ops@example.test", "role": "admin", "is_active": 1}


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "grants.db"
    migrate(str(path))
    database = DB(path=str(path))
    with database.connect() as c:
        for uid in ("alice", "bob"):
            c.execute("INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", NOW.isoformat(), NOW.isoformat()))
    return database


@pytest.fixture
def enforced(monkeypatch):
    monkeypatch.setenv("BILLING_ENFORCED", "true")


@pytest.fixture
def not_enforced(monkeypatch):
    monkeypatch.setenv("BILLING_ENFORCED", "false")


def _load_plan_gate():
    """The bot-backend gate module, loaded by path (it shares shared_lib with this backend)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("bot_plan_gate", ROOT / "backends" / "bot-backend" / "app" / "core" / "plan_gate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def expired_pro(db, user_id="alice"):
    with db.connect() as c:
        c.execute("INSERT INTO subscriptions (user_id, plan_id, status, provider, provider_sub_id, current_period_end, "
                  "cancel_at_period_end, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  (user_id, "plan_pro", "active", "stripe", "sub_old", (NOW - timedelta(days=30)).isoformat(), 1,
                   NOW.isoformat(), NOW.isoformat()))


def many_bots(db, user_id="alice", n=3):
    with db.connect() as c:
        for i in range(n):
            c.execute("INSERT INTO bot_instances (id,user_id,broker_account_id,market_type,strategy_id,mode,status,created_at,updated_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?)", (f"bot{i}", user_id, "acct", "crypto", "cati", "live", "active",
                                                     NOW.isoformat(), NOW.isoformat()))


# ── the switch ──────────────────────────────────────────────────────────────

def test_the_switch_defaults_to_off_and_reads_the_environment(monkeypatch):
    monkeypatch.delenv("BILLING_ENFORCED", raising=False)
    assert enforcement.billing_enforced(settings=object()) is False
    for value in ("1", "true", "YES", "on"):
        monkeypatch.setenv("BILLING_ENFORCED", value)
        assert enforcement.billing_enforced() is True
    monkeypatch.setenv("BILLING_ENFORCED", "false")
    assert enforcement.billing_enforced() is False


def test_billing_off_no_subscription_blocks_nothing(db, not_enforced):
    gate = _load_plan_gate()
    many_bots(db, n=3)                                      # the free plan allows one
    gate.require_bot_slots(db, "alice", requested=2)         # no 403
    gate.require_live_trading(db, "alice")                   # no 403
    assert entitlements.get_effective_plan(db, "alice")["plan_id"] == "plan_free"   # reporting is unchanged


def test_billing_off_expired_subscription_blocks_nothing(db, not_enforced):
    gate = _load_plan_gate()
    expired_pro(db)
    many_bots(db, n=2)
    gate.require_bot_slots(db, "alice")
    gate.require_live_trading(db, "alice")
    assert entitlements.get_effective_plan(db, "alice", now=NOW)["plan_id"] == "plan_free"


def test_billing_on_valid_subscription_is_allowed_within_limits(db, enforced):
    gate = _load_plan_gate()
    with db.connect() as c:
        c.execute("INSERT INTO subscriptions (user_id, plan_id, status, provider, provider_sub_id, current_period_end, "
                  "cancel_at_period_end, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  ("alice", "plan_pro", "active", "stripe", "sub_live", (datetime.now(timezone.utc) + timedelta(days=20)).isoformat(),
                   0, NOW.isoformat(), NOW.isoformat()))
    many_bots(db, n=2)                                      # pro allows five
    gate.require_bot_slots(db, "alice")
    gate.require_live_trading(db, "alice")
    with pytest.raises(HTTPException) as denied:
        gate.require_bot_slots(db, "alice", requested=4)
    assert denied.value.status_code == 403 and denied.value.detail["error_code"] == "BOT_LIMIT_REACHED"


def test_billing_on_invalid_or_expired_subscription_is_refused(db, enforced):
    gate = _load_plan_gate()
    expired_pro(db)
    many_bots(db, n=1)
    with pytest.raises(HTTPException) as denied:
        gate.require_bot_slots(db, "alice")
    assert denied.value.detail["error_code"] == "BOT_LIMIT_REACHED" and denied.value.detail["plan_id"] == "plan_free"
    with pytest.raises(HTTPException) as live:
        gate.require_live_trading(db, "alice")
    assert live.value.detail["error_code"] == "LIVE_TRADING_NOT_IN_PLAN"


# ── operator grants ─────────────────────────────────────────────────────────

def test_an_operator_grant_is_identified_timed_and_distinguishable_from_stripe(db):
    out = operator_grants.grant(db, user_id="alice", plan_id="plan_pro", issued_by="admin-ops", reason="pilot cohort",
                                duration_days=14, now=NOW)
    assert out["issued_by"] == "admin-ops" and out["expires_at"] == (NOW + timedelta(days=14)).isoformat()
    state = entitlements.get_effective_plan(db, "alice", now=NOW + timedelta(days=13))
    assert state["plan_id"] == "plan_pro" and state["is_paid"] and state["provider"] == "operator"
    assert state["provider_sub_id"] == f"operator:{out['grant_id']}"
    lapsed = entitlements.get_effective_plan(db, "alice", now=NOW + timedelta(days=14, seconds=1))
    assert lapsed["plan_id"] == "plan_free"                 # the expiry rule: the period end, no grace
    [row] = operator_grants.list_grants(db, user_id="alice")
    assert (row["plan_id"], row["reason"], row["revoked_at"]) == ("plan_pro", "pilot cohort", None)
    assert entitlements.get_effective_plan(db, "bob", now=NOW)["plan_id"] == "plan_free"   # tenant isolation


@pytest.mark.parametrize("kwargs,code", [
    (dict(plan_id="plan_free"), "UNKNOWN_PLAN"),
    (dict(plan_id="plan_gold"), "UNKNOWN_PLAN"),
    (dict(issued_by=""), "ISSUER_REQUIRED"),
    (dict(reason="x"), "REASON_REQUIRED"),
    (dict(duration_days=0), "INVALID_DURATION"),
    (dict(duration_days=400), "INVALID_DURATION"),
    (dict(user_id="nobody"), "USER_NOT_FOUND"),
])
def test_invalid_grants_are_refused(db, kwargs, code):
    base = dict(user_id="alice", plan_id="plan_pro", issued_by="admin-ops", reason="pilot cohort", duration_days=30, now=NOW)
    with pytest.raises(operator_grants.GrantError) as err:
        operator_grants.grant(db, **{**base, **kwargs})
    assert err.value.code == code
    assert entitlements.get_effective_plan(db, "alice", now=NOW)["plan_id"] == "plan_free"


def test_a_repeated_grant_supersedes_and_a_revocation_restores_the_original_row(db):
    expired_pro(db)                                          # the row that existed before any grant
    first = operator_grants.grant(db, user_id="alice", plan_id="plan_pro", issued_by="admin-ops", reason="pilot", now=NOW)
    second = operator_grants.grant(db, user_id="alice", plan_id="plan_whale", issued_by="admin-two", reason="upgrade", now=NOW)
    assert second["superseded_grant_id"] == first["grant_id"]
    rows = {r["grant_id"]: r for r in operator_grants.list_grants(db, user_id="alice")}
    assert rows[first["grant_id"]]["revoke_reason"] == "SUPERSEDED" and rows[second["grant_id"]]["revoked_at"] is None
    assert entitlements.get_effective_plan(db, "alice", now=NOW)["plan_id"] == "plan_whale"
    out = operator_grants.revoke(db, grant_id=second["grant_id"], revoked_by="admin-ops", reason="cohort closed", now=NOW)
    assert out["restored"] == "PREVIOUS_ROW" and out["stripe_resync_required"] is False
    with db.connect() as c:
        row = dict(c.execute("SELECT plan_id, provider, provider_sub_id FROM subscriptions WHERE user_id='alice'").fetchone())
    assert row == {"plan_id": "plan_pro", "provider": "stripe", "provider_sub_id": "sub_old"}   # the original Stripe row
    assert entitlements.get_effective_plan(db, "alice", now=NOW)["plan_id"] == "plan_free"     # and it is still expired
    with pytest.raises(operator_grants.GrantError) as twice:
        operator_grants.revoke(db, grant_id=second["grant_id"], revoked_by="admin-ops", reason="again", now=NOW)
    assert twice.value.code == "GRANT_ALREADY_REVOKED"


def test_revoking_a_grant_without_a_previous_row_downgrades_to_free(db):
    out = operator_grants.grant(db, user_id="alice", plan_id="plan_pro", issued_by="admin-ops", reason="pilot", now=NOW)
    revoked = operator_grants.revoke(db, grant_id=out["grant_id"], revoked_by="admin-ops", reason="done", now=NOW)
    assert revoked["restored"] == "FREE_PLAN"
    state = entitlements.get_effective_plan(db, "alice", now=NOW)
    assert state["plan_id"] == "plan_free" and state["provider"] is None


def test_a_stripe_webhook_never_overwrites_an_active_grant(db):
    out = operator_grants.grant(db, user_id="alice", plan_id="plan_whale", issued_by="admin-ops", reason="pilot", now=NOW)
    event = {"id": "evt_1", "type": "customer.subscription.updated", "created": int(NOW.timestamp()), "livemode": True,
             "data": {"object": {"id": "sub_new", "object": "subscription", "status": "active", "customer": "cus_alice",
                                 "created": int(NOW.timestamp()), "metadata": {"user_id": "alice"},
                                 "items": {"data": [{"price": {"id": "price_pro_m"}}]},
                                 "current_period_end": int((NOW + timedelta(days=30)).timestamp())}}}
    result = webhooks.process_stripe_event(db, event, price_to_plan={"price_pro_m": "plan_pro"}, now=NOW)
    assert result["status"] == "processed"
    state = entitlements.get_effective_plan(db, "alice", now=NOW)
    assert state["plan_id"] == "plan_whale" and state["provider"] == "operator"          # untouched
    [row] = operator_grants.list_grants(db, user_id="alice")
    assert len(row["deferred_events"]) >= 1 and row["deferred_events"][0]["fields"]["plan_id"] == "plan_pro"
    revoked = operator_grants.revoke(db, grant_id=out["grant_id"], revoked_by="admin-ops", reason="done", now=NOW)
    assert revoked["stripe_resync_required"] is True and len(revoked["deferred_stripe_events"]) >= 1
    # Without a grant the same event applies as before.
    again = {**event, "id": "evt_2"}
    webhooks.process_stripe_event(db, again, price_to_plan={"price_pro_m": "plan_pro"}, now=NOW)
    assert entitlements.get_effective_plan(db, "alice", now=NOW)["provider"] == "stripe"


# ── the admin API ───────────────────────────────────────────────────────────

def _client(monkeypatch, db, *, admin=True):
    monkeypatch.setattr(admin_billing, "_db", lambda: db)
    app = FastAPI()
    app.include_router(admin_billing.router, prefix="/api")
    if admin:
        app.dependency_overrides[require_admin] = lambda: dict(ADMIN)
    return TestClient(app)


def audit(db):
    with db.connect() as c:
        return [(r[0], r[1], json.loads(r[2])) for r in c.execute(
            "SELECT event_type, user_id, details FROM auth_audit_log ORDER BY created_at, rowid")]


def test_admin_grant_and_revoke_are_audited(db, monkeypatch):
    client = _client(monkeypatch, db)
    created = client.post("/api/admin/billing/grants", json={"user_id": "alice", "plan_id": "plan_pro", "reason": "pilot cohort",
                                                             "duration_days": 7})
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["issued_by"] == "admin-ops" and body["billing_enforced"] in (True, False)
    listed = client.get("/api/admin/billing/grants", params={"user_id": "alice"}).json()
    assert [g["grant_id"] for g in listed["grants"]] == [body["grant_id"]]
    revoked = client.post(f"/api/admin/billing/grants/{body['grant_id']}/revoke", json={"reason": "cohort closed"})
    assert revoked.status_code == 200 and revoked.json()["restored"] == "FREE_PLAN"
    events = audit(db)
    assert [e[0] for e in events] == ["operator_plan_granted", "operator_plan_revoked"]
    assert all(e[1] == "alice" and e[2]["admin_id"] == "admin-ops" for e in events)
    refused = client.post("/api/admin/billing/grants", json={"user_id": "nobody", "plan_id": "plan_pro", "reason": "pilot cohort"})
    assert refused.status_code == 404 and refused.json()["detail"]["error_code"] == "USER_NOT_FOUND"
    assert audit(db)[-1][0] == "operator_plan_grant_refused"


def test_customers_cannot_call_operator_plan_endpoints(db, monkeypatch):
    client = _client(monkeypatch, db, admin=False)        # no admin identity: the real dependency runs
    for method, path, body in (("post", "/api/admin/billing/grants", {"user_id": "alice", "plan_id": "plan_pro", "reason": "pilot"}),
                               ("get", "/api/admin/billing/grants", None),
                               ("post", "/api/admin/billing/grants/x/revoke", {"reason": "pilot"})):
        response = getattr(client, method)(path, json=body) if body is not None else client.get(path)
        assert response.status_code == 401, (path, response.status_code)
    # A customer access token is not an admin token either.
    from app.core.security import create_access_token
    token = create_access_token({"sub": "alice", "role": "user"})
    response = client.post("/api/admin/billing/grants", json={"user_id": "alice", "plan_id": "plan_pro", "reason": "pilot"},
                           headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401
    assert operator_grants.list_grants(db) == []


def test_the_subscription_view_reports_the_source_and_the_switch(db, monkeypatch, not_enforced):
    from app.core import billing_service
    monkeypatch.setattr(billing_service, "_db", lambda: db)
    view = billing_service.get_user_subscription("alice")
    assert view["billing_enforced"] is False and view["source"] == "free"
    operator_grants.grant(db, user_id="alice", plan_id="plan_pro", issued_by="admin-ops", reason="pilot cohort")
    view = billing_service.get_user_subscription("alice")
    assert view["source"] == "operator_grant" and view["plan"]["id"] == "plan_pro"
