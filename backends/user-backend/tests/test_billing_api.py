"""HTTP-level tests of the billing router: webhook authentication, checkout,
checkout status, and the removed self-service "simulate success" route.

No network: Stripe is a local fake and webhook signatures are computed here
with Stripe's documented scheme (``t=<ts>,v1=hmac_sha256(secret, "<ts>.<body>")``).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import billing  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402
from app.core import billing_service  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

SECRET = "whsec_test_only_not_a_real_secret"
WEBHOOK = "/api/billing/webhook"
PRICE_CFG = {
    "STRIPE_PRICE_PRO_MONTHLY": "price_pro_m",
    "STRIPE_PRICE_PRO_YEARLY": "price_pro_y",
    "STRIPE_PRICE_WHALE_MONTHLY": "price_whale_m",
    "STRIPE_PRICE_WHALE_YEARLY": "price_whale_y",
}


class FakeProvider(billing_service.PaymentProvider):
    name = "stripe"
    mode = "test"

    def __init__(self):
        self.sessions = []
        self.cancelled = []
        self.plan_changes = []

    def create_customer(self, user_id, email):
        return f"cus_fake_{user_id}"

    def create_checkout_session(self, *, plan_id, user_id, price_id, success_url, cancel_url, customer_id=None):
        session_id = f"cs_test_fake_{len(self.sessions) + 1}"
        self.sessions.append(dict(id=session_id, plan_id=plan_id, user_id=user_id, price_id=price_id,
                                  success_url=success_url, cancel_url=cancel_url, customer_id=customer_id))
        return {"id": session_id, "url": f"https://checkout.stripe.test/{session_id}"}

    def cancel_subscription(self, provider_sub_id):
        self.cancelled.append(provider_sub_id)
        return True

    def resume_subscription(self, provider_sub_id):
        return True

    def change_plan(self, provider_sub_id, price_id, plan_id):
        self.plan_changes.append((provider_sub_id, price_id, plan_id))
        return True


class Ctx:
    def __init__(self, client: TestClient, db: DB, app: FastAPI, provider: FakeProvider):
        self.client = client
        self.db = db
        self.app = app
        self.provider = provider

    def login_as(self, user_id: str) -> None:
        self.app.dependency_overrides[get_current_active_user] = lambda: {
            "id": user_id, "email": f"{user_id}@example.test", "status": "active",
        }

    def logout(self) -> None:
        self.app.dependency_overrides.pop(get_current_active_user, None)

    def sub_row(self, user_id: str = "user-1") -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)).fetchone()
            return dict(row) if row else None

    def event_count(self) -> int:
        with self.db.connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM billing_events").fetchone()[0]


def _make(tmp_path: Path, monkeypatch, *, production: bool = False, with_provider: bool = True, **cfg) -> Ctx:
    db_path = tmp_path / "billing_api.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        for user_id in ("user-1", "user-2"):
            conn.execute(
                "INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) "
                "VALUES (?, ?, 'x', 'active', ?, ?)",
                (user_id, f"{user_id}@example.test", now, now),
            )

    config = {"FRONTEND_URL": "https://app.example.test", "STRIPE_WEBHOOK_SECRET": SECRET, **PRICE_CFG, **cfg}
    provider = FakeProvider()
    monkeypatch.setattr(billing_service, "_db", lambda: db)
    monkeypatch.setattr(billing_service, "_cfg", lambda name: str(config.get(name, "") or "").strip())
    monkeypatch.setattr(billing_service, "is_production", lambda: production)
    if with_provider:
        monkeypatch.setattr(billing_service, "get_provider", lambda: provider)

    app = FastAPI()
    app.include_router(billing.router, prefix="/api/billing")
    ctx = Ctx(TestClient(app), db, app, provider)
    ctx.login_as("user-1")
    return ctx


def _sign(payload: bytes, secret: str = SECRET, ts: int | None = None) -> str:
    ts = int(time.time()) if ts is None else ts
    digest = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def _checkout_event(session_id: str = "cs_test_1", *, user_id: str = "user-1", plan_id: str = "plan_pro",
                    event_id: str = "evt_1", livemode: bool = False) -> dict:
    return {
        "id": event_id, "object": "event", "type": "checkout.session.completed",
        "created": int(time.time()), "livemode": livemode,
        "data": {"object": {
            "id": session_id, "object": "checkout.session", "mode": "subscription", "payment_status": "paid",
            "client_reference_id": user_id, "customer": f"cus_fake_{user_id}", "subscription": "sub_1",
            "metadata": {"user_id": user_id, "plan_id": plan_id, "interval": "month"},
        }},
    }


def _post_webhook(ctx: Ctx, payload: bytes, signature: str | None):
    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["Stripe-Signature"] = signature
    return ctx.client.post(WEBHOOK, content=payload, headers=headers)


# --------------------------------------------------------------------------- webhook authentication

def test_webhook_with_valid_signature_activates_plan(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    ctx.logout()  # Stripe calls without a session; the signature is the authentication
    payload = json.dumps(_checkout_event()).encode()

    res = _post_webhook(ctx, payload, _sign(payload))
    assert res.status_code == 200, res.text
    assert res.json() == {"status": "processed"}
    row = ctx.sub_row()
    assert (row["plan_id"], row["status"], row["provider_sub_id"]) == ("plan_pro", "active", "sub_1")

    # Stripe redelivers: acknowledged, applied once.
    res = _post_webhook(ctx, payload, _sign(payload))
    assert res.status_code == 200
    assert res.json() == {"status": "duplicate"}
    assert ctx.event_count() == 1


def test_webhook_without_signature_is_rejected(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    payload = json.dumps(_checkout_event(plan_id="plan_whale")).encode()
    res = _post_webhook(ctx, payload, None)
    assert res.status_code == 400
    assert ctx.sub_row() is None
    assert ctx.event_count() == 0


def test_webhook_with_wrong_secret_or_stale_timestamp_is_rejected(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    payload = json.dumps(_checkout_event(plan_id="plan_whale")).encode()
    assert _post_webhook(ctx, payload, _sign(payload, secret="whsec_guess")).status_code == 400
    assert _post_webhook(ctx, payload, _sign(payload, ts=int(time.time()) - 301)).status_code == 400
    assert _post_webhook(ctx, payload, _sign(payload, ts=int(time.time()) + 301)).status_code == 400
    assert _post_webhook(ctx, payload, "not-a-signature").status_code == 400
    assert ctx.sub_row() is None
    assert ctx.event_count() == 0


def test_webhook_signature_covers_the_raw_body(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    event = _checkout_event()
    signed_body = json.dumps(event, separators=(",", ":")).encode()
    signature = _sign(signed_body)

    # Same JSON value, different bytes (re-serialised): must not verify.
    reserialised = json.dumps(event, indent=2).encode()
    assert json.loads(reserialised) == json.loads(signed_body)
    assert _post_webhook(ctx, reserialised, signature).status_code == 400

    # A body edited after signing (plan swapped) must not verify either.
    tampered = signed_body.replace(b"plan_pro", b"plan_whale")
    assert _post_webhook(ctx, tampered, signature).status_code == 400
    assert ctx.sub_row() is None

    assert _post_webhook(ctx, signed_body, signature).status_code == 200
    assert ctx.sub_row()["plan_id"] == "plan_pro"


def test_webhook_fails_closed_when_secret_is_not_configured(tmp_path, monkeypatch):
    for production in (True, False):
        ctx = _make(tmp_path / str(production), monkeypatch, production=production, STRIPE_WEBHOOK_SECRET="")
        payload = json.dumps(_checkout_event()).encode()
        for signature in (None, _sign(payload, secret=""), _sign(payload)):
            res = _post_webhook(ctx, payload, signature)
            assert res.status_code == 503, res.text
        assert ctx.sub_row() is None
        assert ctx.event_count() == 0


def test_old_unsigned_payload_shape_grants_nothing(tmp_path, monkeypatch):
    # The pre-fix handler trusted exactly this: an unauthenticated JSON body
    # naming a user and a plan.
    ctx = _make(tmp_path, monkeypatch)
    forged = {
        "type": "checkout.session.completed",
        "data": {"client_reference_id": "user-1", "metadata": {"plan_id": "plan_whale"}, "id": "cs_forged"},
    }
    res = ctx.client.post(WEBHOOK, json=forged)
    assert res.status_code == 400
    assert ctx.sub_row() is None


def test_webhook_unknown_event_type_is_acknowledged_and_ignored(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    event = {"id": "evt_other", "object": "event", "type": "charge.refunded", "created": int(time.time()),
             "livemode": False, "data": {"object": {"id": "ch_1"}}}
    payload = json.dumps(event).encode()
    res = _post_webhook(ctx, payload, _sign(payload))
    assert res.status_code == 200
    assert res.json() == {"status": "ignored"}
    assert ctx.sub_row() is None


def test_webhook_signed_garbage_is_a_400(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    for payload in (b"not json at all", b'{"id": "evt_1"}', b"[]"):
        res = _post_webhook(ctx, payload, _sign(payload))
        assert res.status_code == 400, payload
    assert ctx.event_count() == 0


def test_webhook_for_unknown_user_grants_nothing(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    payload = json.dumps(_checkout_event(user_id="ghost")).encode()
    res = _post_webhook(ctx, payload, _sign(payload))
    assert res.status_code == 200  # acknowledged so Stripe stops retrying
    with ctx.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0] == 0


# --------------------------------------------------------------------------- removed self-grant route

def test_simulate_success_route_no_longer_exists(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    res = ctx.client.post("/api/billing/test-simulate-success", json={"plan_id": "plan_whale"})
    assert res.status_code in (404, 405)
    assert ctx.sub_row() is None
    paths = {route.path for route in billing.router.routes}
    assert not any("simulate" in path for path in paths)
    assert not hasattr(billing_service, "handle_checkout_success")


# --------------------------------------------------------------------------- checkout

def test_checkout_returns_provider_url_and_grants_nothing(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    res = ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"})
    assert res.status_code == 200, res.text
    assert res.json() == {"checkout_url": "https://checkout.stripe.test/cs_test_fake_1",
                          "session_id": "cs_test_fake_1"}
    session = ctx.provider.sessions[0]
    assert session["price_id"] == "price_pro_m"
    assert session["user_id"] == "user-1"
    assert session["customer_id"] == "cus_fake_user-1"
    assert ctx.sub_row() is None

    sub = ctx.client.get("/api/billing/subscription").json()
    assert sub["plan"]["id"] == "plan_free"


def test_checkout_ignores_client_supplied_redirect_urls(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    res = ctx.client.post("/api/billing/checkout", json={
        "plan_id": "plan_pro",
        "success_url": "https://evil.example/phish",
        "cancel_url": "https://evil.example/phish",
    })
    assert res.status_code == 200, res.text
    session = ctx.provider.sessions[0]
    assert session["success_url"] == "https://app.example.test/payment/success?session_id={CHECKOUT_SESSION_ID}"
    assert session["cancel_url"] == "https://app.example.test/dashboard/subscription?checkout=cancelled"
    assert "evil.example" not in json.dumps(ctx.provider.sessions)


def test_checkout_in_production_without_stripe_is_503_not_a_free_plan(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch, production=True, with_provider=False, STRIPE_SECRET_KEY="")
    res = ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_whale"})
    assert res.status_code == 503
    assert res.json()["detail"] == "Billing is not configured."
    assert "mock" not in res.text
    assert ctx.sub_row() is None

    res = ctx.client.post("/api/billing/subscription/manage", json={"action": "upgrade", "plan_id": "plan_whale"})
    assert res.status_code == 503
    assert ctx.sub_row() is None


def test_checkout_validation_errors(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    assert ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_nope"}).status_code == 400
    assert ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_free"}).status_code == 400
    assert ctx.client.post("/api/billing/checkout", json={}).status_code == 422
    assert ctx.provider.sessions == []

    ctx.logout()
    assert ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"}).status_code == 401


def test_second_checkout_while_subscribed_is_a_conflict(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    payload = json.dumps(_checkout_event()).encode()
    assert _post_webhook(ctx, payload, _sign(payload)).status_code == 200

    res = ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_whale"})
    assert res.status_code == 409
    assert ctx.provider.sessions == []


# --------------------------------------------------------------------------- checkout status

def test_checkout_status_is_pending_until_the_webhook_arrives(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    session_id = ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"}).json()["session_id"]

    res = ctx.client.get("/api/billing/checkout-status", params={"session_id": session_id})
    assert res.status_code == 200, res.text
    assert res.json() == {"session_id": session_id, "status": "pending",
                          "plan_id": "plan_pro", "current_plan_id": "plan_free"}
    # Polling the status never activates anything.
    for _ in range(3):
        ctx.client.get("/api/billing/checkout-status", params={"session_id": session_id})
    assert ctx.sub_row() is None

    payload = json.dumps(_checkout_event(session_id)).encode()
    assert _post_webhook(ctx, payload, _sign(payload)).status_code == 200

    body = ctx.client.get("/api/billing/checkout-status", params={"session_id": session_id}).json()
    assert (body["status"], body["current_plan_id"]) == ("active", "plan_pro")


def test_checkout_status_is_scoped_to_the_logged_in_user(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    session_id = ctx.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"}).json()["session_id"]

    ctx.login_as("user-2")
    assert ctx.client.get("/api/billing/checkout-status", params={"session_id": session_id}).status_code == 404
    assert ctx.client.get("/api/billing/checkout-status", params={"session_id": "cs_unknown"}).status_code == 404
    assert ctx.client.get("/api/billing/checkout-status").status_code == 422

    ctx.logout()
    assert ctx.client.get("/api/billing/checkout-status", params={"session_id": session_id}).status_code == 401


# --------------------------------------------------------------------------- subscription status / manage

def test_subscription_status_includes_usage(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc).isoformat()
    with ctx.db.connect() as conn:
        conn.execute(
            """
            INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, strategy_version,
                                       config_id, risk_profile_id, symbols_json, timeframes_json, allocation_type,
                                       allocation_value, mode, status, created_at, updated_at)
            VALUES ('bot_1', 'user-1', 'brk_1', 'CRYPTO', 'cati', '1.0.0', 'cfg', 'risk', '[]', '["15m"]',
                    'fixed_amount', 100.0, 'paper', 'active', ?, ?)
            """,
            (now, now),
        )
        conn.execute(
            "INSERT INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at) "
            "VALUES ('brk_1', 'user-1', 'binance', 'crypto', 'connected', ?, ?)",
            (now, now),
        )
    res = ctx.client.get("/api/billing/subscription")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["plan"]["id"] == "plan_free"
    assert body["status"] == "active"
    assert body["usage"] == {"bots": 1, "brokers": 1}
    assert body["entitlements"] == {"max_bots": 1, "max_brokers": 1, "live_trading": False, "api_access": False}


def test_manage_cancel_and_upgrade(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    # Nothing to cancel on the free plan.
    assert ctx.client.post("/api/billing/subscription/manage", json={"action": "cancel"}).status_code == 400

    # Without a subscription, "upgrade" starts a checkout.
    res = ctx.client.post("/api/billing/subscription/manage", json={"action": "upgrade", "plan_id": "plan_pro"})
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "upgrade_initiated"
    session_id = res.json()["session_id"]

    payload = json.dumps(_checkout_event(session_id)).encode()
    assert _post_webhook(ctx, payload, _sign(payload)).status_code == 200
    with ctx.db.connect() as conn:  # the real period end normally comes from invoice.paid
        conn.execute("UPDATE subscriptions SET current_period_end = ?",
                     ((datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),))

    # With one, "upgrade" changes the existing subscription instead of adding another.
    res = ctx.client.post("/api/billing/subscription/manage", json={"action": "upgrade", "plan_id": "plan_whale"})
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "plan_change_requested"
    assert ctx.provider.plan_changes == [("sub_1", "price_whale_m", "plan_whale")]
    assert len(ctx.provider.sessions) == 1
    assert ctx.sub_row()["plan_id"] == "plan_pro"  # unchanged until Stripe's webhook

    res = ctx.client.post("/api/billing/subscription/manage", json={"action": "cancel"})
    assert res.status_code == 200, res.text
    assert ctx.provider.cancelled == ["sub_1"]
    assert ctx.sub_row()["cancel_at_period_end"] == 1
    assert ctx.client.get("/api/billing/subscription").json()["cancel_at_period_end"] is True

    assert ctx.client.post("/api/billing/subscription/manage", json={"action": "resume"}).status_code == 200
    assert ctx.sub_row()["cancel_at_period_end"] == 0
    assert ctx.client.post("/api/billing/subscription/manage", json={"action": "explode"}).status_code == 400


def test_plans_endpoint_is_public_and_consistent(tmp_path, monkeypatch):
    ctx = _make(tmp_path, monkeypatch)
    ctx.logout()
    res = ctx.client.get("/api/billing/plans")
    assert res.status_code == 200
    plans = {p["id"]: p for p in res.json()["plans"]}
    assert plans["plan_free"]["limits"] == {"max_bots": 1, "max_brokers": 1, "live_trading": False, "api_access": False}
    assert plans["plan_pro"]["limits"]["max_brokers"] == 3
