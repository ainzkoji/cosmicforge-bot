"""Stripe webhook verification and event handling (shared_lib.billing.webhooks).

No network and no Stripe SDK: events are plain dicts shaped like Stripe's JSON,
and signatures are computed locally with Stripe's documented ``v1`` scheme.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))

from shared_lib.billing import webhooks  # noqa: E402
from shared_lib.billing.entitlements import GRACE_PERIOD_DAYS, get_effective_plan  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

SECRET = "whsec_test_only_not_a_real_secret"
PRICES = {
    "price_pro_m": "plan_pro",
    "price_pro_y": "plan_pro_yearly",
    "price_whale_m": "plan_whale",
    "price_whale_y": "plan_whale_yearly",
}
NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
T0 = int(NOW.timestamp())
DAY = 86400


# --------------------------------------------------------------------------- helpers

def _make_db(tmp_path: Path, users=("user-1",)) -> DB:
    path = tmp_path / "billing_webhook.db"
    migrate(str(path))
    db = DB(path=str(path))
    with db.connect() as conn:
        for user_id in users:
            conn.execute(
                "INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) "
                "VALUES (?, ?, 'x', 'active', ?, ?)",
                (user_id, f"{user_id}@example.test", NOW.isoformat(), NOW.isoformat()),
            )
    return db


_SEQ = {"n": 0}


def _event(event_type: str, obj: dict, *, event_id: str | None = None, created: int = T0, livemode: bool = False) -> dict:
    _SEQ["n"] += 1
    return {
        "id": event_id or f"evt_{_SEQ['n']:06d}",
        "object": "event",
        "type": event_type,
        "created": created,
        "livemode": livemode,
        "data": {"object": obj},
    }


def _checkout(user_id="user-1", plan_id="plan_pro", *, session_id="cs_test_1", sub="sub_1", cus="cus_1",
              payment_status="paid", mode="subscription", metadata_user=None) -> dict:
    return {
        "id": session_id,
        "object": "checkout.session",
        "mode": mode,
        "payment_status": payment_status,
        "client_reference_id": user_id,
        "customer": cus,
        "subscription": sub,
        "metadata": {"user_id": metadata_user or user_id, "plan_id": plan_id, "interval": "month"},
    }


def _invoice(invoice_id="in_1", *, sub="sub_1", cus="cus_1", price="price_pro_m", period_start=T0,
             period_end=T0 + 30 * DAY, amount=2900, metadata=None, new_shape=False) -> dict:
    line = {"id": "il_1", "period": {"start": period_start, "end": period_end}, "metadata": metadata or {}}
    inv = {
        "id": invoice_id,
        "object": "invoice",
        "customer": cus,
        "currency": "usd",
        "amount_paid": amount,
        "amount_due": amount,
        "total": amount,
        "created": period_start,
        "period_start": period_start - 30 * DAY,
        "period_end": period_start,
        "hosted_invoice_url": f"https://invoice.stripe.test/{invoice_id}",
        "status_transitions": {"paid_at": period_start + 5},
        "lines": {"object": "list", "data": [line]},
    }
    if new_shape:
        # API 2025-03-31 ("basil") and later.
        line["pricing"] = {"price_details": {"price": price, "product": "prod_1"}}
        line["parent"] = {"subscription_item_details": {"subscription": sub, "proration": False}}
        inv["parent"] = {"type": "subscription_details",
                         "subscription_details": {"subscription": sub, "metadata": metadata or {}}}
    else:
        line["price"] = {"id": price, "recurring": {"interval": "month"}}
        inv["subscription"] = sub
        inv["subscription_details"] = {"metadata": metadata or {}}
    return inv


def _subscription(sub="sub_1", *, cus="cus_1", status="active", price="price_pro_m", period_end=T0 + 30 * DAY,
                  cancel_at_period_end=False, metadata=None, new_shape=False, interval="month",
                  created: int | None = None) -> dict:
    item = {"id": "si_1", "price": {"id": price, "recurring": {"interval": interval}}}
    obj = {
        "id": sub,
        "object": "subscription",
        "customer": cus,
        "status": status,
        "cancel_at_period_end": cancel_at_period_end,
        "cancel_at": None,
        "ended_at": None,
        "metadata": metadata or {},
        "items": {"object": "list", "data": [item]},
    }
    if created is not None:
        obj["created"] = created
    if period_end is None:
        pass  # a payload that carries no period end at all
    elif new_shape:
        item["current_period_end"] = period_end
    else:
        obj["current_period_end"] = period_end
    return obj


def _process(db: DB, event: dict, *, now: datetime = NOW, **kwargs) -> dict:
    return webhooks.process_stripe_event(db, event, price_to_plan=PRICES, now=now, **kwargs)


def _sub_row(db: DB, user_id="user-1") -> dict | None:
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)).fetchone()
        return dict(row) if row else None


def _rows(db: DB, sql: str, params: tuple = ()) -> list[dict]:
    with db.connect() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _sign(payload: bytes, secret: str = SECRET, ts: int | None = None) -> str:
    ts = int(time.time()) if ts is None else ts
    digest = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


# --------------------------------------------------------------------------- signature

def test_signature_valid_returns_timestamp():
    payload = b'{"id":"evt_1","type":"x"}'
    ts = int(time.time())
    assert webhooks.verify_stripe_signature(payload, _sign(payload, ts=ts), SECRET) == ts


def test_signature_matches_stripe_scheme():
    payload = b'{"a":1}'
    expected = hmac.new(b"whsec_x", b"1700000000." + payload, hashlib.sha256).hexdigest()
    assert webhooks.compute_signature(payload, "whsec_x", 1700000000) == expected


def test_signature_missing_header_rejected():
    for header in (None, ""):
        with pytest.raises(webhooks.WebhookSignatureError):
            webhooks.verify_stripe_signature(b"{}", header, SECRET)


def test_signature_malformed_header_rejected():
    for header in ("garbage", "t=abc,v1=00", "t=1700000000", "v1=deadbeef", "t=1700000000,v0=deadbeef"):
        with pytest.raises(webhooks.WebhookSignatureError):
            webhooks.verify_stripe_signature(b"{}", header, SECRET)


def test_signature_wrong_secret_rejected():
    payload = b'{"id":"evt_1"}'
    with pytest.raises(webhooks.WebhookSignatureError):
        webhooks.verify_stripe_signature(payload, _sign(payload, secret="whsec_other"), SECRET)


def test_signature_tampered_body_rejected():
    payload = b'{"plan":"plan_pro"}'
    header = _sign(payload)
    with pytest.raises(webhooks.WebhookSignatureError):
        webhooks.verify_stripe_signature(b'{"plan":"plan_whale"}', header, SECRET)


def test_signature_non_ascii_candidate_rejected_not_crashed():
    with pytest.raises(webhooks.WebhookSignatureError):
        webhooks.verify_stripe_signature(b"{}", f"t={int(time.time())},v1=éé", SECRET)


def test_signature_outside_tolerance_rejected():
    payload = b"{}"
    now = 1_800_000_000
    for ts in (now - 301, now + 301):
        with pytest.raises(webhooks.WebhookSignatureError):
            webhooks.verify_stripe_signature(payload, _sign(payload, ts=ts), SECRET, now=now)
    # Just inside the 5 minute window is accepted.
    assert webhooks.verify_stripe_signature(payload, _sign(payload, ts=now - 299), SECRET, now=now) == now - 299


def test_signature_tolerance_is_five_minutes():
    assert webhooks.SIGNATURE_TOLERANCE_SECONDS == 300


def test_signature_unconfigured_secret_fails_closed():
    payload = b"{}"
    for secret in ("", "   ", None):
        with pytest.raises(webhooks.WebhookNotConfiguredError):
            webhooks.verify_stripe_signature(payload, _sign(payload, secret=""), secret)
    # ...and it is a signature error, so callers that only catch that still reject.
    assert issubclass(webhooks.WebhookNotConfiguredError, webhooks.WebhookSignatureError)


def test_signature_accepts_any_matching_v1_during_secret_rotation():
    payload = b'{"id":"evt_1"}'
    ts = int(time.time())
    good = _sign(payload, ts=ts).split("v1=")[1]
    header = f"t={ts},v1={'0' * 64},v1={good}"
    assert webhooks.verify_stripe_signature(payload, header, SECRET) == ts


def test_parse_event_rejects_non_events():
    for body in (b"not json", b"[]", b'{"id":"evt_1"}', b'{"id":"evt_1","type":"x","data":{}}'):
        with pytest.raises(webhooks.WebhookPayloadError):
            webhooks.parse_event(body)
    event = webhooks.parse_event(json.dumps(_event("x.y", {"id": "obj"})).encode())
    assert event["type"] == "x.y"


# --------------------------------------------------------------------------- checkout.session.completed

def test_checkout_completed_activates_plan(tmp_path):
    db = _make_db(tmp_path)
    result = _process(db, _event("checkout.session.completed", _checkout()))
    assert result == {"status": "processed", "result": "ok:activated"}

    row = _sub_row(db)
    assert row["plan_id"] == "plan_pro"
    assert row["status"] == "active"
    assert row["provider"] == "stripe"
    assert row["provider_sub_id"] == "sub_1"
    assert row["provider_customer_id"] == "cus_1"
    assert row["checkout_session_id"] == "cs_test_1"
    assert row["cancel_at_period_end"] == 0
    provisional_end = NOW + timedelta(days=webhooks.PROVISIONAL_PERIOD_DAYS)
    assert row["current_period_end"] == provisional_end.isoformat()

    customers = _rows(db, "SELECT * FROM billing_customers")
    assert [(c["user_id"], c["mode"], c["provider_customer_id"]) for c in customers] == [("user-1", "test", "cus_1")]
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_pro"
    # Activation alone writes no invoice: invoices come only from invoice.paid.
    assert _rows(db, "SELECT * FROM invoices") == []


def test_checkout_completed_is_idempotent_by_event_id(tmp_path):
    db = _make_db(tmp_path)
    event = _event("checkout.session.completed", _checkout(), event_id="evt_dup")
    assert _process(db, event)["status"] == "processed"

    # Simulate later state, then redeliver the same event: nothing may change.
    with db.connect() as conn:
        conn.execute("UPDATE subscriptions SET plan_id = 'plan_free', status = 'canceled' WHERE user_id = 'user-1'")
    assert _process(db, event) == {"status": "duplicate", "result": "duplicate"}
    assert _process(db, json.loads(json.dumps(event))) == {"status": "duplicate", "result": "duplicate"}

    row = _sub_row(db)
    assert (row["plan_id"], row["status"]) == ("plan_free", "canceled")
    assert len(_rows(db, "SELECT * FROM billing_events WHERE event_id = 'evt_dup'")) == 1


def test_checkout_completed_unknown_user_grants_nothing(tmp_path):
    db = _make_db(tmp_path)
    result = _process(db, _event("checkout.session.completed", _checkout(user_id="ghost")))
    assert result["result"] == "unmatched:unknown_user"
    assert _rows(db, "SELECT * FROM subscriptions") == []
    assert _rows(db, "SELECT * FROM billing_customers") == []


def test_checkout_completed_requires_server_set_reference_and_paid_plan(tmp_path):
    db = _make_db(tmp_path)
    no_ref = _checkout()
    no_ref["client_reference_id"] = None
    no_ref["metadata"] = {}
    assert _process(db, _event("checkout.session.completed", no_ref))["result"] == "unmatched:missing_reference"
    for plan in ("plan_free", "plan_enterprise", None):
        result = _process(db, _event("checkout.session.completed", _checkout(plan_id=plan)))
        assert result["result"] == "unmatched:missing_reference"
    conflict = _checkout(metadata_user="user-2")
    assert _process(db, _event("checkout.session.completed", conflict))["result"] == "unmatched:reference_conflict"
    one_off = _checkout(mode="payment")
    assert _process(db, _event("checkout.session.completed", one_off))["result"] == "ignored:not_subscription_checkout"
    assert _rows(db, "SELECT * FROM subscriptions") == []


def test_checkout_completed_must_match_recorded_intent(tmp_path):
    db = _make_db(tmp_path, users=("user-1", "user-2"))
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO pricing_intents (id, user_id, plan_id, session_id, created_at) VALUES (?, ?, ?, ?, ?)",
            ("pi_1", "user-2", "plan_pro", "cs_test_1", NOW.isoformat()),
        )
    # The session was created for user-2; an event naming user-1 is not applied.
    result = _process(db, _event("checkout.session.completed", _checkout(user_id="user-1")))
    assert result["result"] == "unmatched:intent_conflict"
    # Same user but a pricier plan than the one recorded is not applied either.
    result = _process(db, _event("checkout.session.completed", _checkout(user_id="user-2", plan_id="plan_whale")))
    assert result["result"] == "unmatched:intent_conflict"
    assert _rows(db, "SELECT * FROM subscriptions") == []
    assert _process(db, _event("checkout.session.completed", _checkout(user_id="user-2")))["result"] == "ok:activated"


def test_checkout_completed_unpaid_does_not_grant_access(tmp_path):
    db = _make_db(tmp_path)
    result = _process(db, _event("checkout.session.completed", _checkout(payment_status="unpaid")))
    assert result["result"] == "pending_payment"
    assert _sub_row(db)["status"] == "incomplete"
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"

    # The payment clearing (invoice.paid) is what activates it.
    _process(db, _event("invoice.paid", _invoice()))
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_pro"


def test_customer_cannot_be_claimed_by_a_second_user(tmp_path):
    db = _make_db(tmp_path, users=("user-1", "user-2"))
    _process(db, _event("checkout.session.completed", _checkout(user_id="user-1")))
    result = _process(db, _event("checkout.session.completed",
                                 _checkout(user_id="user-2", session_id="cs_test_2", sub="sub_2", cus="cus_1")))
    assert result["result"] == "unmatched:customer_conflict"
    assert _sub_row(db, "user-2") is None


# --------------------------------------------------------------------------- invoice.paid

def test_invoice_paid_extends_period_and_records_invoice(tmp_path):
    db = _make_db(tmp_path)
    _process(db, _event("checkout.session.completed", _checkout()))
    period_end = T0 + 30 * DAY
    result = _process(db, _event("invoice.paid", _invoice(period_end=period_end)))
    assert result["result"] == "ok:period_extended"

    row = _sub_row(db)
    assert row["status"] == "active"
    assert row["plan_id"] == "plan_pro"
    assert row["provider_price_id"] == "price_pro_m"
    # The short provisional checkout period is replaced by the invoice's real one.
    assert row["current_period_end"] == _iso(period_end)

    # Renewal a month later moves the period end to the invoice's period end.
    renewal_end = T0 + 61 * DAY
    _process(db, _event("invoice.paid", _invoice("in_2", period_start=T0 + 30 * DAY, period_end=renewal_end),
                        created=T0 + 30 * DAY), now=NOW + timedelta(days=30))
    assert _sub_row(db)["current_period_end"] == _iso(renewal_end)

    invoices = _rows(db, "SELECT * FROM invoices ORDER BY id")
    assert [i["id"] for i in invoices] == ["in_1", "in_2"]
    first = invoices[0]
    assert first["user_id"] == "user-1"
    assert first["status"] == "paid"
    assert first["amount_cents"] == 2900 and isinstance(first["amount_cents"], int)
    assert first["amount"] == 29.0
    assert first["currency"] == "USD"
    assert first["provider_invoice_id"] == "in_1"
    assert first["provider_sub_id"] == "sub_1"
    assert first["plan_id"] == "plan_pro"
    assert first["period_end"] == _iso(period_end)
    assert first["hosted_invoice_url"].endswith("/in_1")


def test_invoice_paid_redelivery_does_not_duplicate(tmp_path):
    db = _make_db(tmp_path)
    _process(db, _event("checkout.session.completed", _checkout()))
    event = _event("invoice.paid", _invoice(), event_id="evt_inv")
    assert _process(db, event)["status"] == "processed"
    assert _process(db, event)["status"] == "duplicate"
    # A different event id for the same invoice (manual resend) still yields one row.
    _process(db, _event("invoice.paid", _invoice(), event_id="evt_inv_resend"))
    assert len(_rows(db, "SELECT * FROM invoices")) == 1


def test_invoice_paid_before_checkout_completed(tmp_path):
    db = _make_db(tmp_path)
    # Our server stores the customer before redirecting to Checkout.
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at) "
            "VALUES ('user-1', 'test', 'stripe', 'cus_1', ?, ?)", (NOW.isoformat(), NOW.isoformat()),
        )
    period_end = T0 + 30 * DAY
    assert _process(db, _event("invoice.paid", _invoice(period_end=period_end)))["result"] == "ok:period_extended"
    assert _sub_row(db)["current_period_end"] == _iso(period_end)

    assert _process(db, _event("checkout.session.completed", _checkout()))["result"] == "ok:already_active"
    row = _sub_row(db)
    assert row["current_period_end"] == _iso(period_end)  # real period kept, not the provisional one
    assert row["checkout_session_id"] == "cs_test_1"
    assert row["status"] == "active"


def test_invoice_paid_new_api_shape(tmp_path):
    db = _make_db(tmp_path)
    _process(db, _event("checkout.session.completed", _checkout(plan_id="plan_whale")))
    period_end = T0 + 40 * DAY
    inv = _invoice(price="price_whale_m", period_end=period_end, amount=9900, new_shape=True)
    assert "subscription" not in inv and "price" not in inv["lines"]["data"][0]
    assert _process(db, _event("invoice.paid", inv))["result"] == "ok:period_extended"
    row = _sub_row(db)
    assert row["plan_id"] == "plan_whale"
    assert row["current_period_end"] == _iso(period_end)
    assert _rows(db, "SELECT amount_cents FROM invoices")[0]["amount_cents"] == 9900


def test_plan_comes_from_price_not_from_metadata(tmp_path):
    db = _make_db(tmp_path)
    _process(db, _event("checkout.session.completed", _checkout()))
    # Metadata claims the top plan but the price actually paid is Pro.
    inv = _invoice(price="price_pro_m", metadata={"user_id": "user-1", "plan_id": "plan_whale"})
    _process(db, _event("invoice.paid", inv))
    assert _sub_row(db)["plan_id"] == "plan_pro"


def test_invoice_for_unknown_customer_changes_nothing(tmp_path):
    db = _make_db(tmp_path)
    inv = _invoice(cus="cus_stranger", sub="sub_stranger", metadata={"user_id": "ghost", "plan_id": "plan_whale"})
    assert _process(db, _event("invoice.paid", inv))["result"] == "unmatched:unknown_customer"
    assert _rows(db, "SELECT * FROM subscriptions") == []
    assert _rows(db, "SELECT * FROM invoices") == []


def test_invoice_without_subscription_is_ignored(tmp_path):
    db = _make_db(tmp_path)
    inv = _invoice()
    inv.pop("subscription")
    assert _process(db, _event("invoice.paid", inv))["result"] == "ignored:not_subscription_invoice"


def test_zero_decimal_currency_amount(tmp_path):
    db = _make_db(tmp_path)
    _process(db, _event("checkout.session.completed", _checkout()))
    inv = _invoice(amount=3000)
    inv["currency"] = "jpy"
    _process(db, _event("invoice.paid", inv))
    row = _rows(db, "SELECT amount, amount_cents, currency FROM invoices")[0]
    assert (row["amount"], row["amount_cents"], row["currency"]) == (3000.0, 3000, "JPY")


# --------------------------------------------------------------------------- invoice.payment_failed

def _active_user(db: DB, period_end: int = T0 + 30 * DAY) -> None:
    _process(db, _event("checkout.session.completed", _checkout()))
    _process(db, _event("invoice.paid", _invoice(period_end=period_end)))


def test_payment_failed_marks_past_due_with_grace(tmp_path):
    db = _make_db(tmp_path)
    period_end = T0 + 30 * DAY
    _active_user(db, period_end)

    failed_at = period_end + 3600
    failed_now = datetime.fromtimestamp(failed_at, timezone.utc)
    renewal = _invoice("in_2", period_start=period_end, period_end=period_end + 30 * DAY)
    result = _process(db, _event("invoice.payment_failed", renewal, created=failed_at), now=failed_now)
    assert result["result"] == "ok:past_due"

    row = _sub_row(db)
    assert row["status"] == "past_due"
    assert row["plan_id"] == "plan_pro"
    grace_end = failed_at + GRACE_PERIOD_DAYS * DAY
    assert row["grace_period_end"] == _iso(grace_end)
    assert GRACE_PERIOD_DAYS == 7

    # Access is kept during the grace period...
    during = get_effective_plan(db, "user-1", now=failed_now + timedelta(days=6))
    assert (during["plan_id"], during["status"]) == ("plan_pro", "past_due")

    # ...a later retry failing does not push the grace period out...
    retry_at = failed_at + 3 * DAY
    _process(db, _event("invoice.payment_failed", renewal, created=retry_at),
             now=datetime.fromtimestamp(retry_at, timezone.utc))
    assert _sub_row(db)["grace_period_end"] == _iso(grace_end)

    # ...and once it ends the user is on the free plan (persisted when asked).
    after = get_effective_plan(db, "user-1", now=failed_now + timedelta(days=7, seconds=1), persist=True)
    assert after["plan_id"] == "plan_free"
    row = _sub_row(db)
    assert (row["plan_id"], row["status"], row["previous_plan_id"]) == ("plan_free", "expired", "plan_pro")

    open_invoice = _rows(db, "SELECT * FROM invoices WHERE id = 'in_2'")[0]
    assert open_invoice["status"] == "open"


def test_payment_recovered_after_failure_restores_access(tmp_path):
    db = _make_db(tmp_path)
    period_end = T0 + 30 * DAY
    _active_user(db, period_end)
    failed_at = period_end + 3600
    renewal = _invoice("in_2", period_start=period_end, period_end=period_end + 30 * DAY)
    _process(db, _event("invoice.payment_failed", renewal, created=failed_at),
             now=datetime.fromtimestamp(failed_at, timezone.utc))

    # Even after the lazy downgrade was persisted, a successful retry reactivates.
    late = datetime.fromtimestamp(failed_at, timezone.utc) + timedelta(days=9)
    assert get_effective_plan(db, "user-1", now=late, persist=True)["plan_id"] == "plan_free"
    assert _sub_row(db)["status"] == "expired"
    result = _process(db, _event("invoice.paid", renewal, created=int(late.timestamp())), now=late)
    assert result["result"] == "ok:period_extended"

    row = _sub_row(db)
    assert (row["plan_id"], row["status"], row["grace_period_end"]) == ("plan_pro", "active", None)
    assert row["current_period_end"] == _iso(period_end + 30 * DAY)
    assert _rows(db, "SELECT status FROM invoices WHERE id = 'in_2'")[0]["status"] == "paid"
    assert get_effective_plan(db, "user-1", now=late)["plan_id"] == "plan_pro"


def test_stale_payment_failed_after_paid_is_ignored(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    result = _process(db, _event("invoice.payment_failed", _invoice("in_1")))
    assert result["result"] == "ignored:invoice_already_paid"
    assert _sub_row(db)["status"] == "active"
    assert _rows(db, "SELECT status FROM invoices WHERE id = 'in_1'")[0]["status"] == "paid"


def test_payment_failed_for_abandoned_checkout_is_ignored(tmp_path):
    db = _make_db(tmp_path)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at) "
            "VALUES ('user-1', 'test', 'stripe', 'cus_1', ?, ?)", (NOW.isoformat(), NOW.isoformat()),
        )
    result = _process(db, _event("invoice.payment_failed", _invoice()))
    assert result["result"] == "ignored:no_matching_subscription"
    assert _rows(db, "SELECT * FROM subscriptions") == []
    assert _rows(db, "SELECT * FROM invoices") == []


# --------------------------------------------------------------------------- customer.subscription.updated

def test_subscription_updated_syncs_state(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    period_end = T0 + 365 * DAY
    sub = _subscription(price="price_whale_y", period_end=period_end, cancel_at_period_end=True, interval="year")
    result = _process(db, _event("customer.subscription.updated", sub, created=T0 + 10))
    assert result["result"] == "ok:synced:active"

    row = _sub_row(db)
    assert row["plan_id"] == "plan_whale_yearly"
    assert row["provider_price_id"] == "price_whale_y"
    assert row["billing_interval"] == "year"
    assert row["cancel_at_period_end"] == 1
    assert row["current_period_end"] == _iso(period_end)
    assert row["status"] == "active"
    assert row["provider_updated_at"] == T0 + 10


def test_subscription_updated_new_api_shape_and_cancel_at(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    period_end = T0 + 45 * DAY
    sub = _subscription(period_end=period_end, new_shape=True)
    sub["cancel_at"] = period_end
    assert "current_period_end" not in sub
    _process(db, _event("customer.subscription.updated", sub, created=T0 + 10))
    row = _sub_row(db)
    assert row["current_period_end"] == _iso(period_end)
    assert row["cancel_at_period_end"] == 1


def test_subscription_updated_out_of_order_events_ignored(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    newer = _subscription(price="price_whale_m")
    _process(db, _event("customer.subscription.updated", newer, created=T0 + 100))
    older = _subscription(price="price_pro_m", cancel_at_period_end=True)
    assert _process(db, _event("customer.subscription.updated", older, created=T0 + 50))["result"] == "ignored:stale_event"
    row = _sub_row(db)
    assert (row["plan_id"], row["cancel_at_period_end"]) == ("plan_whale", 0)

    # A pre-payment "incomplete" snapshot can never follow an active one.
    incomplete = _subscription(status="incomplete")
    result = _process(db, _event("customer.subscription.updated", incomplete, created=T0 + 200))
    assert result["result"] == "ignored:stale_event"
    assert _sub_row(db)["status"] == "active"


def test_subscription_updated_past_due_and_unpaid(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    at = T0 + 31 * DAY
    _process(db, _event("customer.subscription.updated", _subscription(status="past_due"), created=at))
    row = _sub_row(db)
    assert row["status"] == "past_due"
    assert row["grace_period_end"] == _iso(at + GRACE_PERIOD_DAYS * DAY)

    _process(db, _event("customer.subscription.updated", _subscription(status="unpaid"), created=at + DAY))
    row = _sub_row(db)
    assert (row["status"], row["grace_period_end"]) == ("unpaid", None)
    # "unpaid" grants nothing, whatever the dates say.
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"

    _process(db, _event("customer.subscription.updated", _subscription(status="active"), created=at + 2 * DAY))
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_pro"


def test_subscription_updated_unknown_price_keeps_current_plan(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    sub = _subscription(price="price_not_configured")
    assert _process(db, _event("customer.subscription.updated", sub, created=T0 + 10))["status"] == "processed"
    assert _sub_row(db)["plan_id"] == "plan_pro"


# --------------------------------------------------------------------------- customer.subscription.deleted

def test_subscription_deleted_downgrades_to_free(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    ended = T0 + 30 * DAY
    sub = _subscription(status="canceled")
    sub["ended_at"] = ended
    assert _process(db, _event("customer.subscription.deleted", sub, created=ended))["result"] == "ok:downgraded"

    row = _sub_row(db)
    assert (row["plan_id"], row["status"], row["previous_plan_id"]) == ("plan_free", "canceled", "plan_pro")
    assert row["cancel_at_period_end"] == 0
    plan = get_effective_plan(db, "user-1", now=NOW)
    assert (plan["plan_id"], plan["status"], plan["is_paid"]) == ("plan_free", "active", False)

    # Nothing about the ended subscription can bring the plan back.
    late_update = _event("customer.subscription.updated", _subscription(status="active"), created=ended + 5)
    assert _process(db, late_update)["result"] == "ignored:subscription_already_ended"
    late_invoice = _event("invoice.paid", _invoice("in_9"), created=ended + 6)
    assert _process(db, late_invoice)["result"] == "invoice_recorded:subscription_ended"
    assert _sub_row(db)["plan_id"] == "plan_free"

    # A brand new subscription for the same customer does.
    _process(db, _event("checkout.session.completed", _checkout(session_id="cs_test_2", sub="sub_2")))
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_pro"


def test_subscription_deleted_for_other_subscription_ignored(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    other = _subscription("sub_old", status="canceled")
    assert _process(db, _event("customer.subscription.deleted", other))["result"] == "ignored:other_subscription"
    assert _sub_row(db)["plan_id"] == "plan_pro"


# --------------------------------------------------------------------------- events of another subscription

def _two_subscriptions(db: DB) -> None:
    """user-1 subscribed (sub_A, pro), then subscribed again (sub_B, whale): sub_B is the live one."""
    _process(db, _event("checkout.session.completed", _checkout(session_id="cs_a", sub="sub_A"), created=T0))
    _process(db, _event("invoice.paid", _invoice("in_a1", sub="sub_A"), created=T0 + 1))
    _process(db, _event("customer.subscription.updated", _subscription("sub_A", created=T0 - 2), created=T0 + 2))
    _process(db, _event("checkout.session.completed",
                        _checkout(plan_id="plan_whale", session_id="cs_b", sub="sub_B"), created=T0 + 1000))
    _process(db, _event("invoice.paid", _invoice("in_b1", sub="sub_B", price="price_whale_m", amount=9900),
                        created=T0 + 1001))
    row = _sub_row(db)
    assert (row["provider_sub_id"], row["plan_id"], row["status"]) == ("sub_B", "plan_whale", "active")


def test_subscription_creation_time_is_stored_when_first_seen(tmp_path):
    db = _make_db(tmp_path)
    # Checkout creates the subscription: its event time stands in for the creation time...
    _process(db, _event("checkout.session.completed", _checkout(), created=T0 + 7))
    assert _sub_row(db)["provider_sub_created_at"] == T0 + 7
    # ...until a subscription payload supplies the real one.
    _process(db, _event("customer.subscription.updated", _subscription(created=T0 + 3), created=T0 + 20))
    assert _sub_row(db)["provider_sub_created_at"] == T0 + 3
    # A payload without ``created`` keeps what is stored.
    _process(db, _event("customer.subscription.updated", _subscription(), created=T0 + 30))
    assert _sub_row(db)["provider_sub_created_at"] == T0 + 3


def test_stale_update_of_an_old_subscription_cannot_take_over_the_live_one(tmp_path):
    db = _make_db(tmp_path)
    _two_subscriptions(db)

    # A late "active" snapshot of the OLD subscription (sent before sub_B existed).
    late = _event("customer.subscription.updated", _subscription("sub_A", created=T0 - 2), created=T0 + 500)
    assert _process(db, late)["result"] == "ignored:other_subscription"
    # Even one whose event is newer than anything applied so far.
    late = _event("customer.subscription.updated", _subscription("sub_A", created=T0 - 2), created=T0 + 5000)
    assert _process(db, late)["result"] == "ignored:other_subscription"
    # ...or that does not say when the subscription was created.
    late = _event("customer.subscription.updated", _subscription("sub_A"), created=T0 + 6000)
    assert _process(db, late)["result"] == "ignored:other_subscription"

    row = _sub_row(db)
    assert (row["provider_sub_id"], row["plan_id"], row["status"]) == ("sub_B", "plan_whale", "active")

    # So the live subscription's renewal is still accepted: the paying user stays on whale.
    renewal_end = T0 + 61 * DAY
    renewal = _invoice("in_b2", sub="sub_B", price="price_whale_m", amount=9900,
                       period_start=T0 + 30 * DAY, period_end=renewal_end)
    at = NOW + timedelta(days=30)
    assert _process(db, _event("invoice.paid", renewal, created=T0 + 30 * DAY), now=at)["result"] == "ok:period_extended"
    row = _sub_row(db)
    assert (row["provider_sub_id"], row["plan_id"], row["current_period_end"]) == (
        "sub_B", "plan_whale", _iso(renewal_end))
    assert get_effective_plan(db, "user-1", now=at)["plan_id"] == "plan_whale"


def test_update_of_other_subscription_ignored_when_stored_creation_time_is_unknown(tmp_path):
    db = _make_db(tmp_path)
    _two_subscriptions(db)
    with db.connect() as conn:  # a row written before creation times were stored
        conn.execute("UPDATE subscriptions SET provider_sub_created_at = NULL")
    newer_looking = _event("customer.subscription.updated",
                           _subscription("sub_A", created=T0 + 99999), created=T0 + 5000)
    assert _process(db, newer_looking)["result"] == "ignored:other_subscription"
    assert _sub_row(db)["provider_sub_id"] == "sub_B"


def test_provably_newer_active_subscription_replaces_the_stored_one(tmp_path):
    db = _make_db(tmp_path)
    _two_subscriptions(db)
    stored_created = _sub_row(db)["provider_sub_created_at"]
    assert stored_created == T0 + 1000

    # Newer, but not active: changes nothing.
    pending = _subscription("sub_C", status="incomplete", created=stored_created + 50)
    assert _process(db, _event("customer.subscription.updated", pending, created=T0 + 2000))["result"] == (
        "ignored:other_subscription")
    # Created in the same second as the stored one: not provably newer.
    tie = _subscription("sub_C", created=stored_created)
    assert _process(db, _event("customer.subscription.updated", tie, created=T0 + 2001))["result"] == (
        "ignored:other_subscription")
    assert _sub_row(db)["provider_sub_id"] == "sub_B"

    newer = _subscription("sub_C", price="price_pro_y", interval="year", created=stored_created + 50,
                          period_end=T0 + 365 * DAY)
    assert _process(db, _event("customer.subscription.updated", newer, created=T0 + 2002))["result"] == (
        "ok:synced:active")
    row = _sub_row(db)
    assert (row["provider_sub_id"], row["plan_id"], row["provider_sub_created_at"]) == (
        "sub_C", "plan_pro_yearly", stored_created + 50)


def test_lapsed_stored_subscription_does_not_block_another_one(tmp_path):
    db = _make_db(tmp_path)
    _process(db, _event("checkout.session.completed", _checkout(session_id="cs_a", sub="sub_A"), created=T0))
    # sub_A is still "active" in the row but ran out long ago (period + grace).
    later = NOW + timedelta(days=60)
    other = _subscription("sub_B", price="price_whale_m", period_end=T0 + 90 * DAY)
    result = _process(db, _event("customer.subscription.updated", other, created=T0 + 60 * DAY), now=later)
    assert result["result"] == "ok:synced:active"
    row = _sub_row(db)
    assert (row["provider_sub_id"], row["plan_id"]) == ("sub_B", "plan_whale")
    # The previous subscription's ordering state does not leak into the new one.
    assert row["provider_sub_created_at"] is None


def test_deleting_an_old_subscription_does_not_downgrade_the_live_one(tmp_path):
    db = _make_db(tmp_path)
    _two_subscriptions(db)

    ended = _subscription("sub_A", status="canceled", created=T0 - 2)
    ended["ended_at"] = T0 + 3000
    assert _process(db, _event("customer.subscription.deleted", ended, created=T0 + 3000))["result"] == (
        "ignored:other_subscription")
    canceled_update = _event("customer.subscription.updated", ended, created=T0 + 3001)
    assert _process(db, canceled_update)["result"] == "ignored:other_subscription"
    row = _sub_row(db)
    assert (row["provider_sub_id"], row["plan_id"], row["status"]) == ("sub_B", "plan_whale", "active")
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_whale"

    # The live one ends too. A late "active" snapshot of the long-deleted
    # sub_A must not bring a plan back now that nothing live is stored.
    gone = _subscription("sub_B", status="canceled")
    assert _process(db, _event("customer.subscription.deleted", gone, created=T0 + 4000))["result"] == "ok:downgraded"
    zombie = _event("customer.subscription.updated", _subscription("sub_A", created=T0 - 2), created=T0 + 10)
    assert _process(db, zombie)["result"] == "ignored:subscription_already_ended"
    zombie_invoice = _event("invoice.paid", _invoice("in_a9", sub="sub_A"), created=T0 + 11)
    assert _process(db, zombie_invoice)["result"] == "invoice_recorded:subscription_ended"
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"


def test_deleted_before_checkout_completed_grants_nothing(tmp_path):
    db = _make_db(tmp_path)
    with db.connect() as conn:  # our server stores the customer before redirecting to Checkout
        conn.execute(
            "INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at) "
            "VALUES ('user-1', 'test', 'stripe', 'cus_1', ?, ?)", (NOW.isoformat(), NOW.isoformat()),
        )
    gone = _subscription(status="canceled")
    gone["ended_at"] = T0 + 5
    # The deletion is delivered FIRST (events are not ordered)...
    first = _event("customer.subscription.deleted", gone, created=T0 + 5)
    assert _process(db, first)["result"] == "ignored:other_subscription"
    assert _sub_row(db) is None
    # ...and remembered, once, in the processed-events table.
    markers = _rows(db, "SELECT event_id FROM billing_events WHERE event_type = 'internal.subscription_ended'")
    assert [m["event_id"] for m in markers] == ["subscription_ended:sub_1"]

    # Nothing that arrives afterwards for that subscription grants a plan.
    assert _process(db, _event("checkout.session.completed", _checkout(), created=T0))["result"] == (
        "ignored:subscription_already_ended")
    assert _process(db, _event("checkout.session.completed",
                               _checkout(session_id="cs_test_unpaid", payment_status="unpaid")))["result"] == (
        "ignored:subscription_already_ended")
    assert _process(db, _event("invoice.paid", _invoice(), created=T0 + 1))["result"] == (
        "invoice_recorded:subscription_ended")
    assert _process(db, _event("customer.subscription.updated", _subscription(), created=T0 + 2))["result"] == (
        "ignored:subscription_already_ended")
    assert _sub_row(db) is None
    for when in (NOW, NOW + timedelta(days=5)):
        assert get_effective_plan(db, "user-1", now=when)["plan_id"] == "plan_free"
    # The payment itself is still on record.
    assert [i["status"] for i in _rows(db, "SELECT status FROM invoices")] == ["paid"]

    # A second deletion event for it does not add a second marker...
    _process(db, _event("customer.subscription.deleted", gone, created=T0 + 6))
    assert len(_rows(db, "SELECT 1 FROM billing_events WHERE event_type = 'internal.subscription_ended'")) == 1
    # ...and a genuinely new subscription still activates.
    assert _process(db, _event("checkout.session.completed",
                               _checkout(session_id="cs_test_2", sub="sub_2")))["result"] == "ok:activated"
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_pro"


def test_deleted_for_unknown_customer_is_still_remembered(tmp_path):
    db = _make_db(tmp_path)
    gone = _subscription(cus="cus_new", status="canceled")
    assert _process(db, _event("customer.subscription.deleted", gone))["result"] == "unmatched:unknown_customer"
    late = _checkout(cus="cus_new")
    assert _process(db, _event("checkout.session.completed", late))["result"] == "ignored:subscription_already_ended"
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"


def test_canceled_update_is_applied_even_when_its_event_is_older(tmp_path):
    db = _make_db(tmp_path)
    _active_user(db)
    _process(db, _event("customer.subscription.updated", _subscription(), created=T0 + 100))
    # "canceled" is terminal: an older event time does not make it stale.
    canceled = _event("customer.subscription.updated", _subscription(status="canceled"), created=T0 + 50)
    assert _process(db, canceled)["result"] == "ok:downgraded"
    assert get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"


# --------------------------------------------------------------------------- missing period end

def test_new_subscription_without_period_end_is_provisionally_active_then_filled(tmp_path):
    db = _make_db(tmp_path)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at) "
            "VALUES ('user-1', 'test', 'stripe', 'cus_1', ?, ?)", (NOW.isoformat(), NOW.isoformat()),
        )
    bare = _subscription(period_end=None, created=T0)
    assert "current_period_end" not in bare and "current_period_end" not in bare["items"]["data"][0]
    assert _process(db, _event("customer.subscription.updated", bare, created=T0 + 1))["result"] == "ok:synced:active"

    row = _sub_row(db)
    assert (row["status"], row["plan_id"], row["current_period_end"]) == ("active", "plan_pro", None)
    # Not treated as expired, however much later it is read...
    for when in (NOW, NOW + timedelta(days=45)):
        plan = get_effective_plan(db, "user-1", now=when, persist=True)
        assert (plan["plan_id"], plan["status"], plan["is_paid"]) == ("plan_pro", "active", True)
    assert _sub_row(db)["status"] == "active"

    # ...an invoice without a usable line does not shorten it...
    no_period = _invoice("in_0")
    no_period["lines"]["data"][0]["period"] = {}
    assert _process(db, _event("invoice.paid", no_period, created=T0 + 2))["result"] == "ok:period_extended"
    assert _sub_row(db)["current_period_end"] is None

    # ...and the next event that carries the date fills it in.
    period_end = T0 + 30 * DAY
    assert _process(db, _event("invoice.paid", _invoice(period_end=period_end), created=T0 + 3))["result"] == (
        "ok:period_extended")
    assert _sub_row(db)["current_period_end"] == _iso(period_end)
    after = datetime.fromtimestamp(period_end, timezone.utc) + timedelta(days=GRACE_PERIOD_DAYS, seconds=1)
    assert get_effective_plan(db, "user-1", now=after)["plan_id"] == "plan_free"


def test_period_end_is_read_from_the_subscription_item_and_filled_by_update(tmp_path):
    db = _make_db(tmp_path)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at) "
            "VALUES ('user-1', 'test', 'stripe', 'cus_1', ?, ?)", (NOW.isoformat(), NOW.isoformat()),
        )
    _process(db, _event("customer.subscription.updated", _subscription(period_end=None), created=T0 + 1))
    assert _sub_row(db)["current_period_end"] is None

    period_end = T0 + 33 * DAY
    item_shape = _subscription(period_end=period_end, new_shape=True)  # items.data[0].current_period_end
    assert "current_period_end" not in item_shape
    _process(db, _event("customer.subscription.updated", item_shape, created=T0 + 2))
    assert _sub_row(db)["current_period_end"] == _iso(period_end)

    # A later payload without the date keeps the known one.
    _process(db, _event("customer.subscription.updated", _subscription(period_end=None), created=T0 + 3))
    assert _sub_row(db)["current_period_end"] == _iso(period_end)


# --------------------------------------------------------------------------- generic behaviour

def test_unknown_event_type_is_ignored(tmp_path):
    db = _make_db(tmp_path)
    result = _process(db, _event("charge.refunded", {"id": "ch_1"}, event_id="evt_unknown"))
    assert result == {"status": "ignored", "result": "ignored:unhandled_type"}
    stored = _rows(db, "SELECT * FROM billing_events WHERE event_id = 'evt_unknown'")
    assert len(stored) == 1 and stored[0]["payload_json"] is None
    assert _rows(db, "SELECT * FROM subscriptions") == []


def test_handled_event_types_are_exactly_the_documented_set():
    assert set(webhooks.HANDLED_EVENT_TYPES) == {
        "checkout.session.completed",
        "invoice.paid",
        "invoice.payment_failed",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    }


def test_failed_handler_rolls_back_so_retry_is_processed(tmp_path, monkeypatch):
    db = _make_db(tmp_path)
    event = _event("checkout.session.completed", _checkout(), event_id="evt_retry")

    def boom(conn, ctx):
        conn.execute("UPDATE users SET status = 'tampered' WHERE id = 'user-1'")
        raise RuntimeError("database hiccup")

    monkeypatch.setitem(webhooks._HANDLERS, "checkout.session.completed", boom)
    with pytest.raises(RuntimeError):
        _process(db, event)
    # Neither the partial write nor the event id survived.
    assert _rows(db, "SELECT * FROM billing_events") == []
    assert _rows(db, "SELECT status FROM users WHERE id = 'user-1'")[0]["status"] == "active"

    monkeypatch.setitem(webhooks._HANDLERS, "checkout.session.completed", webhooks._on_checkout_completed)
    assert _process(db, event)["result"] == "ok:activated"


def test_test_mode_events_ignored_when_livemode_required(tmp_path):
    db = _make_db(tmp_path)
    test_event = _event("checkout.session.completed", _checkout(), livemode=False)
    result = _process(db, test_event, require_livemode=True)
    assert result == {"status": "ignored", "result": "ignored:test_mode_event"}
    assert _rows(db, "SELECT * FROM subscriptions") == []

    live_event = _event("checkout.session.completed", _checkout(), livemode=True)
    assert _process(db, live_event, require_livemode=True)["result"] == "ok:activated"
    assert _rows(db, "SELECT mode FROM billing_customers")[0]["mode"] == "live"


def test_malformed_event_raises_payload_error(tmp_path):
    db = _make_db(tmp_path)
    for bad in ({}, {"id": "evt_1"}, {"id": "evt_1", "type": "invoice.paid", "data": {}}):
        with pytest.raises(webhooks.WebhookPayloadError):
            _process(db, bad)
