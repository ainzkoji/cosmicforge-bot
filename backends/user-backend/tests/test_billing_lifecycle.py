"""Section H Step 8.1: complete TEST-MODE billing lifecycle over HTTP.

purchase -> renewal -> payment failure -> recovery -> cancellation (at period
end and immediate) -> refund -> dispute, plus idempotency, out-of-order
delivery, the billing audit log and configuration validation.

No network and no real Stripe: the ``stripe`` SDK is replaced by a local fake
that records calls, and every webhook body is signed here with a test-only
secret using Stripe's documented ``v1`` scheme. Nothing a client reports is
used to grant anything: the plan changes only when a signed webhook arrives.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_billing, billing  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402
from app.core import billing_service  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from shared_lib.billing import webhooks as billing_webhooks  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

SECRET = "whsec_test_only_not_a_real_secret"
TEST_KEY = "sk_test_placeholder_not_a_key"
PRICE_CFG = {
    "STRIPE_PRICE_PRO_MONTHLY": "price_test_pro_m",
    "STRIPE_PRICE_PRO_YEARLY": "price_test_pro_y",
    "STRIPE_PRICE_WHALE_MONTHLY": "price_test_whale_m",
    "STRIPE_PRICE_WHALE_YEARLY": "price_test_whale_y",
}
DAY = 86400
ADMIN = {"id": "admin-1", "email": "ops@example.test"}


class FakeStripe:
    """Stands in for the ``stripe`` module. Records every call; no network."""

    def __init__(self):
        self.api_key = None
        self.calls = []
        self.Customer = SimpleNamespace(create=self._customer_create)
        self.checkout = SimpleNamespace(Session=SimpleNamespace(create=self._session_create))
        self.Subscription = SimpleNamespace(modify=self._subscription_modify)
        self.billing_portal = SimpleNamespace(Session=SimpleNamespace(create=self._portal_create))

    def _customer_create(self, **params):
        self.calls.append(("Customer.create", params))
        return SimpleNamespace(id="cus_test_1")

    def _session_create(self, **params):
        self.calls.append(("checkout.Session.create", params))
        n = sum(1 for c in self.calls if c[0] == "checkout.Session.create")
        return SimpleNamespace(id=f"cs_test_{n}", url=f"https://checkout.stripe.test/cs_test_{n}")

    def _subscription_modify(self, sub_id, **params):
        self.calls.append(("Subscription.modify", {"id": sub_id, **params}))
        return SimpleNamespace(id=sub_id)

    def _portal_create(self, **params):
        self.calls.append(("billing_portal.Session.create", params))
        return SimpleNamespace(id="bps_test_1", url="https://billing.stripe.test/p/session_1")


class World:
    def __init__(self, tmp_path: Path, monkeypatch, **cfg):
        db_path = tmp_path / "lifecycle.db"
        migrate(str(db_path))
        self.db = DB(path=str(db_path))
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
        with self.db.connect() as conn:
            for user_id in ("user-1", "user-2"):
                conn.execute(
                    "INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) "
                    "VALUES (?, ?, 'x', 'active', ?, ?)",
                    (user_id, f"{user_id}@example.test", now_iso, now_iso),
                )
        self.config = {"FRONTEND_URL": "https://app.example.test", "STRIPE_WEBHOOK_SECRET": SECRET,
                       "STRIPE_SECRET_KEY": TEST_KEY, **PRICE_CFG, **cfg}
        self.stripe = FakeStripe()
        monkeypatch.setattr(billing_service, "_db", lambda: self.db)
        monkeypatch.setattr(admin_billing, "_db", lambda: self.db)
        monkeypatch.setattr(billing_service, "_cfg", lambda name: str(self.config.get(name, "") or "").strip())
        monkeypatch.setattr(billing_service, "is_production", lambda: False)
        monkeypatch.setattr(billing_service, "stripe", self.stripe)
        monkeypatch.delenv("BILLING_ENFORCED", raising=False)

        app = FastAPI()
        app.include_router(billing.router, prefix="/api/billing")
        app.include_router(admin_billing.router, prefix="/api")
        app.dependency_overrides[get_current_active_user] = lambda: {
            "id": "user-1", "email": "user-1@example.test", "status": "active"}
        app.dependency_overrides[require_admin] = lambda: dict(ADMIN)
        self.client = TestClient(app)
        self.t0 = int(time.time())
        self._seq = 0

    # ---- webhooks ---------------------------------------------------------
    def event(self, event_type: str, obj: dict, *, created: int | None = None, event_id: str | None = None) -> dict:
        self._seq += 1
        return {"id": event_id or f"evt_test_{self._seq:04d}", "object": "event", "type": event_type,
                "created": created if created is not None else self.t0 + self._seq, "livemode": False,
                "api_version": "2024-06-20", "data": {"object": obj}}

    def deliver(self, event: dict, *, secret: str = SECRET):
        payload = json.dumps(event, separators=(",", ":")).encode()
        ts = int(time.time())
        signature = f"t={ts},v1={billing_webhooks.compute_signature(payload, secret, ts)}"
        return self.client.post("/api/billing/webhook", content=payload,
                                headers={"Content-Type": "application/json", "Stripe-Signature": signature})

    def sub(self) -> dict:
        res = self.client.get("/api/billing/subscription")
        assert res.status_code == 200, res.text
        return res.json()

    def row(self) -> dict:
        with self.db.connect() as conn:
            r = conn.execute("SELECT * FROM subscriptions WHERE user_id = 'user-1'").fetchone()
            return dict(r) if r else {}

    def audit_actions(self) -> list:
        with self.db.connect() as conn:
            return [r[0] for r in conn.execute("SELECT action FROM billing_audit_log ORDER BY id").fetchall()]


def _invoice(w: World, invoice_id: str, *, start: int, end: int, charge: str, reason="subscription_cycle",
             amount=2900) -> dict:
    return {
        "id": invoice_id, "object": "invoice", "customer": "cus_test_1", "subscription": "sub_test_1",
        "currency": "usd", "amount_paid": amount, "amount_due": amount, "total": amount,
        "billing_reason": reason, "created": start, "charge": charge, "payment_intent": f"pi_{charge}",
        "hosted_invoice_url": f"https://invoice.stripe.test/{invoice_id}",
        "status_transitions": {"paid_at": start + 5},
        "lines": {"object": "list", "data": [{
            "id": f"il_{invoice_id}", "period": {"start": start, "end": end},
            "price": {"id": "price_test_pro_m", "recurring": {"interval": "month"}}, "metadata": {}}]},
    }


def _subscription(status: str, *, end: int, cancel_at_period_end=False, created: int) -> dict:
    return {"id": "sub_test_1", "object": "subscription", "customer": "cus_test_1", "status": status,
            "created": created, "cancel_at_period_end": cancel_at_period_end, "current_period_end": end,
            "metadata": {"user_id": "user-1", "plan_id": "plan_pro"},
            "items": {"data": [{"id": "si_1", "price": {"id": "price_test_pro_m",
                                                         "recurring": {"interval": "month"}}}]}}


def test_complete_test_mode_lifecycle(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    t0 = w.t0

    # ---------------------------------------------------------------- purchase
    res = w.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"})
    assert res.status_code == 200, res.text
    session_id = res.json()["session_id"]
    assert res.json()["checkout_url"].startswith("https://checkout.stripe.test/")
    created = [c for c in w.stripe.calls if c[0] == "checkout.Session.create"][0][1]
    # Price from server configuration (env), user and plan as server-set references.
    assert created["line_items"] == [{"price": "price_test_pro_m", "quantity": 1}]
    assert created["client_reference_id"] == "user-1"
    assert created["customer"] == "cus_test_1"
    assert w.stripe.api_key == TEST_KEY

    # The browser coming back to the success page grants nothing.
    assert w.client.get(f"/api/billing/checkout-status?session_id={session_id}").json()["status"] == "pending"
    assert w.sub()["plan"]["id"] == "plan_free"

    # An unsigned or wrongly signed "payment succeeded" is rejected.
    forged = w.event("checkout.session.completed", {"id": session_id})
    assert w.deliver(forged, secret="whsec_attacker").status_code == 400
    assert w.sub()["plan"]["id"] == "plan_free"

    # customer.subscription.created (incomplete) -> still nothing granted.
    sub_created = t0 + 1
    res = w.deliver(w.event("customer.subscription.created",
                            _subscription("incomplete", end=t0 + 30 * DAY, created=sub_created)))
    assert res.json() == {"status": "processed"}
    assert w.sub()["plan"]["id"] == "plan_free"

    checkout = w.event("checkout.session.completed", {
        "id": session_id, "object": "checkout.session", "mode": "subscription", "payment_status": "paid",
        "client_reference_id": "user-1", "customer": "cus_test_1", "subscription": "sub_test_1",
        "metadata": {"user_id": "user-1", "plan_id": "plan_pro", "interval": "month"}})
    assert w.deliver(checkout).json() == {"status": "processed"}
    assert w.sub()["plan"]["id"] == "plan_pro"
    assert w.client.get(f"/api/billing/checkout-status?session_id={session_id}").json()["status"] == "active"

    first_invoice = w.event("invoice.paid", _invoice(w, "in_test_1", start=t0, end=t0 + 30 * DAY,
                                                     charge="ch_test_1", reason="subscription_create"))
    assert w.deliver(first_invoice).json() == {"status": "processed"}
    period_1 = w.sub()["current_period_end"]

    # Duplicate delivery of the same event id: acknowledged, applied once.
    assert w.deliver(first_invoice).json() == {"status": "duplicate"}
    with w.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM invoices WHERE user_id='user-1'").fetchone()[0] == 1

    # Out-of-order: an older "incomplete" snapshot delivered late does not regress state.
    late = w.event("customer.subscription.updated",
                   _subscription("incomplete", end=t0 + 30 * DAY, created=sub_created), created=t0)
    assert w.deliver(late).json() == {"status": "processed"}
    assert (w.row()["status"], w.sub()["plan"]["id"]) == ("active", "plan_pro")

    # ---------------------------------------------------------------- renewal
    renewal = w.event("invoice.paid", _invoice(w, "in_test_2", start=t0 + 30 * DAY, end=t0 + 60 * DAY,
                                               charge="ch_test_2"))
    assert w.deliver(renewal).json() == {"status": "processed"}
    state = w.sub()
    assert state["current_period_end"] > period_1
    assert state["status"] == "active" and state["notices"] == []

    # ---------------------------------------------------------------- payment failure
    failed_invoice = _invoice(w, "in_test_3", start=t0 + 60 * DAY, end=t0 + 90 * DAY, charge="ch_test_3")
    assert w.deliver(w.event("invoice.payment_failed", failed_invoice)).json() == {"status": "processed"}
    state = w.sub()
    assert state["status"] == "past_due"
    assert state["plan"]["id"] == "plan_pro"  # grace period
    assert state["grace_period_end"]
    assert [n["code"] for n in state["notices"]] == ["PAYMENT_FAILED"]

    # ---------------------------------------------------------------- recovery
    assert w.deliver(w.event("invoice.paid", failed_invoice)).json() == {"status": "processed"}
    state = w.sub()
    assert (state["status"], state["grace_period_end"], state["notices"]) == ("active", None, [])
    history = w.client.get("/api/billing/history").json()["invoices"]
    assert {i["id"] for i in history} == {"in_test_1", "in_test_2", "in_test_3"}
    assert all(i["status"] == "paid" and i["pdf_url"].startswith("https://invoice.stripe.test/") for i in history)

    # ---------------------------------------------------------------- cancellation at period end
    res = w.client.post("/api/billing/subscription/manage", json={"action": "cancel"})
    assert res.status_code == 200, res.text
    assert ("Subscription.modify", {"id": "sub_test_1", "cancel_at_period_end": True}) in w.stripe.calls
    sync = w.event("customer.subscription.updated",
                   _subscription("active", end=t0 + 90 * DAY, cancel_at_period_end=True, created=sub_created))
    assert w.deliver(sync).json() == {"status": "processed"}
    state = w.sub()
    assert state["cancel_at_period_end"] is True and state["plan"]["id"] == "plan_pro"
    assert [n["code"] for n in state["notices"]] == ["CANCELLATION_SCHEDULED"]

    # Provider portal (manage payment method / receipts) via the manage endpoint.
    assert state["portal_available"] is True
    res = w.client.post("/api/billing/subscription/manage", json={"action": "portal"})
    assert res.status_code == 200, res.text
    assert res.json() == {"status": "portal", "portal_url": "https://billing.stripe.test/p/session_1"}
    portal_call = [c for c in w.stripe.calls if c[0] == "billing_portal.Session.create"][0][1]
    assert portal_call == {"customer": "cus_test_1", "return_url": "https://app.example.test/dashboard/subscription"}

    # ---------------------------------------------------------------- refund
    refund = w.event("charge.refunded", {"id": "ch_test_3", "object": "charge", "amount": 2900,
                                         "amount_refunded": 2900, "refunded": True, "currency": "usd",
                                         "payment_intent": "pi_ch_test_3",
                                         "refunds": {"data": [{"reason": "requested_by_customer"}]}})
    assert w.deliver(refund).json() == {"status": "processed"}
    state = w.sub()
    # Paid access is not silently kept after a full refund: held for review.
    assert state["billing_hold"] is True and state["plan"]["id"] == "plan_free"
    assert [n["code"] for n in state["notices"]] == ["PAYMENT_UNDER_REVIEW"]
    reviews = w.client.get("/api/admin/billing/reviews").json()["reviews"]
    assert len(reviews) == 1
    review = reviews[0]
    assert (review["kind"], review["user_id"], review["invoice_id"], review["amount"], review["review_status"]) == (
        "REFUND", "user-1", "in_test_3", "29.00", "OPEN")
    assert history_status(w, "in_test_3") == "refunded"
    # A late invoice.paid for the refunded invoice does not turn it back into "paid".
    assert w.deliver(w.event("invoice.paid", failed_invoice)).json() == {"status": "processed"}
    assert history_status(w, "in_test_3") == "refunded"
    assert w.sub()["billing_hold"] is True

    res = w.client.post(f"/api/admin/billing/reviews/{review['review_id']}/resolve",
                        json={"decision": "RESOLVED_KEEP_ACCESS", "note": "goodwill refund, keep access"})
    assert res.status_code == 200, res.text
    state = w.sub()
    assert state["billing_hold"] is False and state["plan"]["id"] == "plan_pro"

    # ---------------------------------------------------------------- dispute
    dispute = w.event("charge.dispute.created", {"id": "dp_test_1", "object": "dispute", "charge": "ch_test_2",
                                                 "amount": 2900, "currency": "usd", "reason": "fraudulent",
                                                 "status": "needs_response"})
    assert w.deliver(dispute).json() == {"status": "processed"}
    assert w.deliver(dispute).json() == {"status": "duplicate"}
    state = w.sub()
    assert state["billing_hold"] is True and state["plan"]["id"] == "plan_free"
    open_reviews = w.client.get("/api/admin/billing/reviews?status=OPEN").json()["reviews"]
    assert [(r["kind"], r["user_id"], r["invoice_id"]) for r in open_reviews] == [("DISPUTE", "user-1", "in_test_2")]
    dispute_review = open_reviews[0]["review_id"]
    # Dispute closing (won) is recorded, but only an operator lifts the hold.
    closed = w.event("charge.dispute.closed", {"id": "dp_test_1", "object": "dispute", "charge": "ch_test_2",
                                               "amount": 2900, "currency": "usd", "reason": "fraudulent",
                                               "status": "won"})
    assert w.deliver(closed).json() == {"status": "processed"}
    assert w.sub()["billing_hold"] is True
    res = w.client.post(f"/api/admin/billing/reviews/{dispute_review}/resolve",
                        json={"decision": "RESOLVED_REVOKE_ACCESS", "note": "chargeback, access revoked"})
    assert res.status_code == 200, res.text
    assert w.sub()["billing_hold"] is True
    # Even a renewal does not lift an operator's revocation.
    assert w.deliver(w.event("invoice.paid", _invoice(w, "in_test_4", start=t0 + 90 * DAY, end=t0 + 120 * DAY,
                                                      charge="ch_test_4"))).json() == {"status": "processed"}
    assert w.sub()["plan"]["id"] == "plan_free"

    # ---------------------------------------------------------------- immediate cancellation
    deleted = w.event("customer.subscription.deleted", {**_subscription("canceled", end=t0 + 120 * DAY,
                                                                         created=sub_created),
                                                         "ended_at": int(time.time()) - 5})
    assert w.deliver(deleted).json() == {"status": "processed"}
    row = w.row()
    assert (row["plan_id"], row["status"]) == ("plan_free", "canceled")
    # Nothing delivered after the deletion reactivates it.
    assert w.deliver(w.event("invoice.paid", _invoice(w, "in_test_5", start=t0 + 120 * DAY, end=t0 + 150 * DAY,
                                                      charge="ch_test_5"))).json() == {"status": "processed"}
    assert w.row()["status"] == "canceled"

    # ---------------------------------------------------------------- audit trail
    actions = w.audit_actions()
    for expected in ("subscription_created", "subscription_updated", "provider_event_applied",
                     "cancel_at_period_end_requested", "payment_review_opened:refund", "entitlement_hold_set",
                     "payment_review_resolved", "entitlement_hold_cleared", "payment_review_opened:dispute"):
        assert expected in actions, expected
    entries = w.client.get("/api/admin/billing/audit?user_id=user-1").json()["entries"]
    assert entries and all(e["user_id"] == "user-1" for e in entries)
    webhook_rows = [e for e in entries if e["actor_type"] == "stripe_webhook"]
    assert webhook_rows and all(e["provider_event_id"].startswith("evt_test_") for e in webhook_rows)
    # No raw payloads or contact data in the audit trail.
    dump = json.dumps(entries)
    assert "user-1@example.test" not in dump and "hosted_invoice_url" not in dump

    # Append-only.
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        with w.db.connect() as conn:
            conn.execute("UPDATE billing_audit_log SET action = 'x'")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        with w.db.connect() as conn:
            conn.execute("DELETE FROM billing_audit_log")


def history_status(w: World, invoice_id: str) -> str:
    with w.db.connect() as conn:
        return conn.execute("SELECT status FROM invoices WHERE id = ?", (invoice_id,)).fetchone()[0]


def test_partial_refund_is_flagged_without_suspending_access(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    w.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"})
    w.deliver(w.event("checkout.session.completed", {
        "id": "cs_test_1", "object": "checkout.session", "mode": "subscription", "payment_status": "paid",
        "client_reference_id": "user-1", "customer": "cus_test_1", "subscription": "sub_test_1",
        "metadata": {"user_id": "user-1", "plan_id": "plan_pro"}}))
    w.deliver(w.event("invoice.paid", _invoice(w, "in_test_1", start=w.t0, end=w.t0 + 30 * DAY, charge="ch_1")))
    partial = {"id": "ch_1", "object": "charge", "amount": 2900, "amount_refunded": 900, "refunded": False,
               "currency": "usd", "customer": "cus_test_1"}
    assert w.deliver(w.event("charge.refunded", partial)).json() == {"status": "processed"}
    state = w.sub()
    assert state["plan"]["id"] == "plan_pro" and state["billing_hold"] is False
    review = w.client.get("/api/admin/billing/reviews").json()["reviews"][0]
    assert (review["full_refund"], review["holds_entitlement"], review["review_status"]) == (0, 0, "OPEN")
    assert history_status(w, "in_test_1") == "partially_refunded"

    # The rest refunded later (cumulative amount): now a full refund, which holds.
    assert w.deliver(w.event("charge.refunded", {**partial, "amount_refunded": 2900, "refunded": True})).json() == {
        "status": "processed"}
    assert w.sub()["billing_hold"] is True
    review = w.client.get("/api/admin/billing/reviews").json()["reviews"]
    assert len(review) == 1 and review[0]["full_refund"] == 1 and review[0]["amount_refunded_minor"] == 2900


def test_unmatched_dispute_is_recorded_for_the_operator(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    res = w.deliver(w.event("charge.dispute.created", {"id": "dp_x", "charge": "ch_unknown", "amount": 500,
                                                       "currency": "usd", "status": "needs_response"}))
    assert res.json() == {"status": "processed"}
    reviews = w.client.get("/api/admin/billing/reviews").json()["reviews"]
    assert [(r["kind"], r["user_id"], r["review_status"]) for r in reviews] == [("DISPUTE", None, "OPEN")]


def test_review_resolution_validation(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    assert w.client.post("/api/admin/billing/reviews/bpr_missing/resolve",
                         json={"decision": "RESOLVED_KEEP_ACCESS", "note": "n/a here"}).status_code == 404
    assert w.client.post("/api/admin/billing/reviews/bpr_missing/resolve",
                         json={"decision": "KEEP_EVERYTHING", "note": "n/a here"}).status_code == 422


# --------------------------------------------------------------------------- configuration validation

def test_config_status_reports_a_ready_test_configuration(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    status = w.client.get("/api/admin/billing/config").json()
    assert status["ready"] is True, status["errors"]
    assert status["secret_key_mode"] == "test" and status["live_mode_authorized"] is False
    assert {p["env"] for p in status["prices"] if p["valid"]} == set(PRICE_CFG)
    assert status["billing_enforced"] is False
    assert TEST_KEY not in json.dumps(status) and SECRET not in json.dumps(status)


def test_live_key_is_refused_without_explicit_authorization(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, STRIPE_SECRET_KEY="sk_live_placeholder_not_a_key")
    status = w.client.get("/api/admin/billing/config").json()
    assert status["ready"] is False and status["secret_key_mode"] == "live"
    assert any("BILLING_LIVE_MODE_AUTHORIZED" in e for e in status["errors"])
    res = w.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"})
    assert res.status_code == 503
    assert not [c for c in w.stripe.calls if c[0] == "checkout.Session.create"]  # nothing reached the SDK

    w.config["BILLING_LIVE_MODE_AUTHORIZED"] = "true"
    status = w.client.get("/api/admin/billing/config").json()
    assert status["ready"] is True and status["live_mode_authorized"] is True


def test_restricted_live_key_is_also_refused(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, STRIPE_SECRET_KEY="rk_live_placeholder_not_a_key")
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.get_provider()


def test_missing_price_id_is_refused_with_a_clear_error(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, STRIPE_PRICE_WHALE_YEARLY="")
    status = w.client.get("/api/admin/billing/config").json()
    assert status["ready"] is False
    assert "STRIPE_PRICE_WHALE_YEARLY is not set (plan plan_whale_yearly cannot be purchased)" in status["errors"]
    with pytest.raises(billing_service.BillingNotConfigured, match="set STRIPE_PRICE_WHALE_YEARLY"):
        billing_service.create_checkout_session("user-1", "plan_whale_yearly")
    assert w.client.post("/api/billing/checkout", json={"plan_id": "plan_whale_yearly"}).status_code == 503
    # Other plans are unaffected.
    assert w.client.post("/api/billing/checkout", json={"plan_id": "plan_pro"}).status_code == 200


def test_billing_enforcement_stays_off_by_default(tmp_path, monkeypatch):
    from shared_lib.billing.enforcement import billing_enforced
    monkeypatch.delenv("BILLING_ENFORCED", raising=False)
    assert billing_enforced(SimpleNamespace()) is False
