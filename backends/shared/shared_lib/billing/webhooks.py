"""Stripe webhook verification and event handling.

Subscription state is changed here and nowhere else. Every event is

* authenticated by its signature over the raw request body
  (:func:`verify_stripe_signature`) before anything is parsed, and
* applied at most once: the event id is inserted into ``billing_events``
  (unique on ``(provider, event_id)``) in the same transaction as the state
  change, so a redelivered event is a no-op and a failed one rolls back whole
  and is retried by Stripe.

The handlers read plain JSON (no Stripe SDK objects) and accept both the
classic object shapes and the 2025 ("basil" and later) API shapes, where
``invoice.subscription`` moved under ``invoice.parent.subscription_details`` and
``current_period_end`` moved from the subscription to its items.

Nothing a browser sends is trusted: the user comes from the stored
customer/subscription mapping or from ``client_reference_id`` / metadata that
our own server set when it created the Checkout Session, the plan comes from the
Stripe Price ID (server-side price configuration), and amounts come from the
signed invoice.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional

from shared_lib.billing.entitlements import GRACE_PERIOD_DAYS, parse_ts, resolve_subscription
from shared_lib.billing.plans import FREE_PLAN_ID, interval_for, is_paid_plan

log = logging.getLogger("cosmicforge.billing.webhooks")

PROVIDER = "stripe"

#: Maximum accepted age (or clock skew) of a signed webhook, in seconds.
SIGNATURE_TOLERANCE_SECONDS = 300

#: Access granted by ``checkout.session.completed`` until ``invoice.paid`` (or
#: ``customer.subscription.updated``) supplies the real period end from Stripe.
#: Deliberately short: the real period end always replaces it, and a deployment
#: that is not receiving those events is noticed within days, not a month later.
PROVISIONAL_PERIOD_DAYS = 3

#: Currencies whose smallest unit is the whole unit (Stripe "zero-decimal").
ZERO_DECIMAL_CURRENCIES = frozenset({
    "bif", "clp", "djf", "gnf", "jpy", "kmf", "krw", "mga", "pyg", "rwf",
    "ugx", "vnd", "vuv", "xaf", "xof", "xpf",
})

_SUBSCRIPTION_COLUMNS = frozenset({
    "plan_id", "status", "provider", "provider_sub_id", "provider_customer_id",
    "provider_price_id", "billing_interval", "current_period_end",
    "cancel_at_period_end", "grace_period_end", "checkout_session_id",
    "previous_plan_id", "provider_updated_at", "provider_sub_created_at",
})

#: ``billing_events`` rows that remember a provider subscription has ended for
#: good (Stripe never revives a canceled subscription). They share the table --
#: and its unique ``(provider, event_id)`` index -- with the processed events.
_ENDED_MARKER_PREFIX = "subscription_ended:"
_ENDED_MARKER_TYPE = "internal.subscription_ended"


class WebhookSignatureError(Exception):
    """The request is not a genuine, fresh Stripe webhook."""


class WebhookNotConfiguredError(WebhookSignatureError):
    """No signing secret is configured: every request is rejected."""


class WebhookPayloadError(Exception):
    """The body is signed correctly but is not a usable event."""


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------

def compute_signature(payload: bytes, secret: str, timestamp: int) -> str:
    """``hex(hmac_sha256(secret, "<timestamp>.<payload>"))`` -- Stripe's ``v1`` scheme."""
    signed = str(int(timestamp)).encode("ascii") + b"." + payload
    return hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()


def verify_stripe_signature(
    payload: bytes,
    signature_header: Optional[str],
    secret: Optional[str],
    *,
    tolerance: int = SIGNATURE_TOLERANCE_SECONDS,
    now: Optional[float] = None,
) -> int:
    """Verify ``Stripe-Signature`` over the raw body. Returns the signed timestamp.

    Raises :class:`WebhookNotConfiguredError` when no secret is configured (fail
    closed) and :class:`WebhookSignatureError` for a missing, malformed, wrong
    or stale signature.
    """
    secret = (secret or "").strip()
    if not secret:
        raise WebhookNotConfiguredError("webhook signing secret is not configured")
    if not isinstance(payload, (bytes, bytearray)):
        raise WebhookSignatureError("payload must be the raw request body")
    if not signature_header:
        raise WebhookSignatureError("missing Stripe-Signature header")

    timestamp: Optional[int] = None
    candidates: List[str] = []
    for part in str(signature_header).split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError:
                raise WebhookSignatureError("malformed signature timestamp") from None
        elif key == "v1":
            candidates.append(value.strip())
    if timestamp is None or not candidates:
        raise WebhookSignatureError("malformed Stripe-Signature header")

    expected = compute_signature(bytes(payload), secret, timestamp).encode("ascii")
    matched = False
    for candidate in candidates:
        # No early exit: compare every candidate in constant time.
        if hmac.compare_digest(expected, candidate.encode("utf-8", "replace")):
            matched = True
    if not matched:
        raise WebhookSignatureError("signature mismatch")

    current = time.time() if now is None else float(now)
    if abs(current - timestamp) > tolerance:
        raise WebhookSignatureError("signature timestamp outside tolerance")
    return timestamp


def parse_event(payload: bytes) -> Dict[str, Any]:
    """Decode a verified body into an event dict."""
    try:
        event = json.loads(bytes(payload).decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise WebhookPayloadError("body is not valid JSON") from None
    if not isinstance(event, dict) or not event.get("id") or not event.get("type"):
        raise WebhookPayloadError("body is not a Stripe event")
    if not isinstance((event.get("data") or {}).get("object"), dict):
        raise WebhookPayloadError("event has no data.object")
    return event


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _dig(obj: Any, *path: Any) -> Any:
    for key in path:
        if isinstance(key, int):
            if not isinstance(obj, list) or len(obj) <= key:
                return None
            obj = obj[key]
        else:
            if not isinstance(obj, dict):
                return None
            obj = obj.get(key)
    return obj


def _ref_id(value: Any) -> Optional[str]:
    """Id of a field that is either an id string or an expanded object."""
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        ident = value.get("id")
        return ident if isinstance(ident, str) and ident else None
    return None


def _as_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _iso(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), timezone.utc).isoformat()


def _later_iso(a: Optional[str], b: Optional[str]) -> Optional[str]:
    pa, pb = parse_ts(a), parse_ts(b)
    if pa is None:
        return b
    if pb is None:
        return a
    return a if pa >= pb else b


def _fetch_dict(conn, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    return {desc[0]: row[idx] for idx, desc in enumerate(cur.description)}


def _metadata(*sources: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for source in sources:
        if isinstance(source, dict):
            for key, value in source.items():
                merged.setdefault(key, value)
    return merged


class _Ctx:
    def __init__(self, event: Dict[str, Any], price_to_plan: Mapping[str, str], now: datetime):
        self.event = event
        self.obj: Dict[str, Any] = event["data"]["object"]
        self.price_to_plan = dict(price_to_plan or {})
        self.now = now
        self.now_iso = now.isoformat()
        self.created = _as_int(event.get("created")) or int(now.timestamp())
        self.mode = "live" if event.get("livemode") else "test"


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _ensure_schema(conn) -> None:
    """Add ``subscriptions.provider_sub_created_at`` if absent (idempotent).

    Stripe's ``created`` of the stored subscription: events for a *different*
    subscription may only replace a live one that is provably older.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(subscriptions)").fetchall()}
    if not cols or "provider_sub_created_at" in cols:
        return
    try:
        conn.execute("ALTER TABLE subscriptions ADD COLUMN provider_sub_created_at INTEGER")
    except sqlite3.OperationalError as exc:
        # Another process added it between the check and the ALTER.
        if "duplicate column name" not in str(exc).lower():
            raise


def _mark_subscription_ended(conn, ctx: "_Ctx", subscription_id: Optional[str]) -> None:
    if not subscription_id:
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO billing_events
            (event_id, event_type, provider, payload_json, processed_at, created_at, result)
        VALUES (?, ?, ?, NULL, ?, ?, ?)
        """,
        (
            _ENDED_MARKER_PREFIX + str(subscription_id), _ENDED_MARKER_TYPE, PROVIDER,
            ctx.now_iso, ctx.now_iso, f"ended_by:{ctx.event.get('id')}",
        ),
    )


def _subscription_known_ended(conn, subscription_id: Optional[str]) -> bool:
    """Stripe already told us this subscription was deleted / canceled."""
    if not subscription_id:
        return False
    row = conn.execute(
        "SELECT 1 FROM billing_events WHERE provider = ? AND event_id = ?",
        (PROVIDER, _ENDED_MARKER_PREFIX + str(subscription_id)),
    ).fetchone()
    return row is not None


def _get_subscription(conn, user_id: str) -> Optional[Dict[str, Any]]:
    return _fetch_dict(conn, "SELECT * FROM subscriptions WHERE user_id = ?", (user_id,))


def _save_subscription(conn, user_id: str, now_iso: str, **fields: Any) -> bool:
    """Write the user's subscription row. Returns False when the write was
    deferred because an operator grant is active (Step 1.5): the grant is
    recorded with the attempted fields and the row is left untouched."""
    unknown = set(fields) - _SUBSCRIPTION_COLUMNS
    if unknown:
        raise ValueError(f"unknown subscription columns: {sorted(unknown)}")
    from shared_lib.billing import operator_grants
    active = operator_grants.active_grant(conn, user_id, parse_ts(now_iso))
    if active is not None:
        operator_grants.defer_event(conn, active, {"at": now_iso, "fields": fields})
        log.warning("stripe write for user %s deferred: operator grant %s is active until %s",
                    user_id, active["grant_id"], active["expires_at"])
        return False
    exists = conn.execute("SELECT 1 FROM subscriptions WHERE user_id = ?", (user_id,)).fetchone()
    if exists:
        cols = sorted(fields)
        assignments = ", ".join(f"{col} = ?" for col in cols)
        conn.execute(
            f"UPDATE subscriptions SET {assignments}, updated_at = ? WHERE user_id = ?",
            (*[fields[col] for col in cols], now_iso, user_id),
        )
        return True
    fields.setdefault("plan_id", FREE_PLAN_ID)
    fields.setdefault("status", "incomplete")
    cols = sorted(fields)
    conn.execute(
        f"INSERT INTO subscriptions (user_id, {', '.join(cols)}, created_at, updated_at) "
        f"VALUES (?, {', '.join('?' for _ in cols)}, ?, ?)",
        (user_id, *[fields[col] for col in cols], now_iso, now_iso),
    )
    return True


def _user_exists(conn, user_id: str) -> bool:
    return conn.execute("SELECT 1 FROM users WHERE id = ?", (user_id,)).fetchone() is not None


def _customer_owner(conn, customer_id: Optional[str]) -> Optional[str]:
    if not customer_id:
        return None
    row = conn.execute(
        "SELECT user_id FROM billing_customers WHERE provider = ? AND provider_customer_id = ?",
        (PROVIDER, customer_id),
    ).fetchone()
    return str(row[0]) if row else None


def _remember_customer(conn, ctx: _Ctx, user_id: str, customer_id: Optional[str]) -> bool:
    """Record user <-> customer. False if the customer already belongs to someone else."""
    if not customer_id:
        return True
    owner = _customer_owner(conn, customer_id)
    if owner and owner != user_id:
        log.error(
            "stripe customer %s is mapped to user %s but event %s names user %s",
            customer_id, owner, ctx.event.get("id"), user_id,
        )
        return False
    conn.execute(
        """
        INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, mode) DO UPDATE SET
            provider_customer_id = excluded.provider_customer_id,
            updated_at = excluded.updated_at
        """,
        (user_id, ctx.mode, PROVIDER, customer_id, ctx.now_iso, ctx.now_iso),
    )
    return True


def _resolve_user(
    conn,
    *,
    customer_id: Optional[str],
    subscription_id: Optional[str],
    hinted_user_id: Optional[str],
) -> Optional[str]:
    """Which of our users a provider object belongs to.

    Stored mappings win; the server-set metadata hint is only a fallback and
    must name an existing user.
    """
    owner = _customer_owner(conn, customer_id)
    if owner:
        return owner
    if subscription_id:
        row = conn.execute(
            "SELECT user_id FROM subscriptions WHERE provider_sub_id = ?", (subscription_id,)
        ).fetchone()
        if row:
            return str(row[0])
    if customer_id:
        row = conn.execute(
            "SELECT user_id FROM subscriptions WHERE provider_customer_id = ?", (customer_id,)
        ).fetchone()
        if row:
            return str(row[0])
    if hinted_user_id and _user_exists(conn, str(hinted_user_id)):
        return str(hinted_user_id)
    return None


def _resolve_plan(
    ctx: _Ctx,
    price_id: Optional[str],
    metadata_plan_id: Optional[str],
    existing: Optional[Dict[str, Any]] = None,
    subscription_id: Optional[str] = None,
) -> Optional[str]:
    """Paid plan for an event: configured price first, then server-set metadata."""
    plan = ctx.price_to_plan.get(price_id or "")
    if is_paid_plan(plan):
        return plan
    if is_paid_plan(metadata_plan_id):
        if price_id:
            log.warning(
                "stripe price %s is not a configured plan price; using metadata plan %s",
                price_id, metadata_plan_id,
            )
        return metadata_plan_id
    if existing and subscription_id and existing.get("provider_sub_id") == subscription_id:
        for candidate in (existing.get("plan_id"), existing.get("previous_plan_id")):
            if is_paid_plan(candidate):
                return candidate
    return None


def _is_live_row(row: Optional[Dict[str, Any]]) -> bool:
    """Row currently tracks a provider subscription that has not ended."""
    if not row or not row.get("provider_sub_id"):
        return False
    return str(row.get("status") or "").lower() in {"active", "trialing", "past_due"}


def _is_entitled_row(row: Optional[Dict[str, Any]], now: datetime) -> bool:
    """Live per its status AND still inside its period / grace window."""
    return _is_live_row(row) and bool(resolve_subscription(row, now).get("is_paid"))


def _other_live_subscription(existing: Optional[Dict[str, Any]], same_sub: bool, now: datetime) -> bool:
    """The user's stored subscription is a different, still entitled Stripe one."""
    return (
        bool(existing) and not same_sub
        and existing.get("provider") == PROVIDER
        and _is_entitled_row(existing, now)
    )


def _invoice_subscription_id(inv: Dict[str, Any]) -> Optional[str]:
    return (
        _ref_id(inv.get("subscription"))
        or _ref_id(_dig(inv, "parent", "subscription_details", "subscription"))
        or _ref_id(_dig(inv, "lines", "data", 0, "subscription"))
        or _ref_id(_dig(inv, "lines", "data", 0, "parent", "subscription_item_details", "subscription"))
    )


def _invoice_metadata(inv: Dict[str, Any]) -> Dict[str, Any]:
    return _metadata(
        _dig(inv, "subscription_details", "metadata"),
        _dig(inv, "parent", "subscription_details", "metadata"),
        _dig(inv, "lines", "data", 0, "metadata"),
    )


def _invoice_main_line(inv: Dict[str, Any]) -> Dict[str, Any]:
    """The line covering the furthest period (the upcoming one on a renewal)."""
    lines = _dig(inv, "lines", "data")
    best: Dict[str, Any] = {}
    best_end = -1
    for line in lines if isinstance(lines, list) else []:
        if not isinstance(line, dict):
            continue
        end = _as_int(_dig(line, "period", "end")) or 0
        if end > best_end:
            best, best_end = line, end
    return best


def _line_price_id(line: Dict[str, Any]) -> Optional[str]:
    return (
        _ref_id(line.get("price"))
        or _ref_id(_dig(line, "pricing", "price_details", "price"))
        or _ref_id(_dig(line, "plan"))
    )


def _upsert_invoice(
    conn,
    ctx: _Ctx,
    inv: Dict[str, Any],
    *,
    user_id: str,
    status: str,
    amount_minor: int,
    subscription_id: Optional[str],
    plan_id: Optional[str],
    line: Dict[str, Any],
) -> None:
    currency = str(inv.get("currency") or "usd").lower()
    divisor = 1 if currency in ZERO_DECIMAL_CURRENCIES else 100
    created = _as_int(_dig(inv, "status_transitions", "paid_at")) or _as_int(inv.get("created")) or ctx.created
    conn.execute(
        """
        INSERT INTO invoices (
            id, user_id, amount, amount_cents, currency, status, period_start, period_end,
            hosted_invoice_url, created_at, provider_invoice_id, provider_sub_id, plan_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            amount = excluded.amount,
            amount_cents = excluded.amount_cents,
            currency = excluded.currency,
            status = excluded.status,
            period_start = excluded.period_start,
            period_end = excluded.period_end,
            hosted_invoice_url = excluded.hosted_invoice_url,
            created_at = excluded.created_at,
            provider_sub_id = excluded.provider_sub_id,
            plan_id = COALESCE(excluded.plan_id, invoices.plan_id)
        WHERE invoices.status != 'paid' AND invoices.user_id = excluded.user_id
        """,
        (
            inv["id"],
            user_id,
            amount_minor / divisor,
            int(amount_minor),
            currency.upper(),
            status,
            _iso(_as_int(_dig(line, "period", "start")) or _as_int(inv.get("period_start"))),
            _iso(_as_int(_dig(line, "period", "end")) or _as_int(inv.get("period_end"))),
            inv.get("hosted_invoice_url"),
            _iso(created),
            inv["id"],
            subscription_id,
            plan_id,
        ),
    )


# ---------------------------------------------------------------------------
# Event handlers. Each returns a short result string stored with the event.
# ---------------------------------------------------------------------------

def _on_checkout_completed(conn, ctx: _Ctx) -> str:
    session = ctx.obj
    if session.get("mode") != "subscription":
        return "ignored:not_subscription_checkout"

    metadata = _metadata(session.get("metadata"))
    user_id = session.get("client_reference_id") or metadata.get("user_id")
    plan_id = metadata.get("plan_id")
    session_id = session.get("id")
    if not user_id or not is_paid_plan(plan_id):
        # Not a session our server created for a plan (it always sets both).
        return "unmatched:missing_reference"
    user_id = str(user_id)
    if metadata.get("user_id") and str(metadata["user_id"]) != user_id:
        return "unmatched:reference_conflict"
    if not _user_exists(conn, user_id):
        return "unmatched:unknown_user"

    # Cross-check against the intent our server recorded when it created the session.
    intent = _fetch_dict(
        conn, "SELECT user_id, plan_id FROM pricing_intents WHERE session_id = ?", (session_id,)
    ) if session_id else None
    if intent and (str(intent.get("user_id")) != user_id or intent.get("plan_id") != plan_id):
        log.error("checkout session %s does not match its recorded intent", session_id)
        return "unmatched:intent_conflict"

    customer_id = _ref_id(session.get("customer"))
    subscription_id = _ref_id(session.get("subscription"))
    if not subscription_id:
        return "unmatched:no_subscription"
    if not _remember_customer(conn, ctx, user_id, customer_id):
        return "unmatched:customer_conflict"

    existing = _get_subscription(conn, user_id)
    same_sub = bool(existing) and existing.get("provider_sub_id") == subscription_id
    if same_sub and str(existing.get("status") or "").lower() == "canceled":
        return "ignored:subscription_already_ended"
    if _subscription_known_ended(conn, subscription_id):
        # customer.subscription.deleted arrived first: a checkout delivered
        # after it must not grant the provisional period.
        return "ignored:subscription_already_ended"

    # A subscription first seen here: the provider-event ordering state of the
    # previous subscription does not apply to it. Checkout creates the
    # subscription as it completes, so this event's time stands in for the
    # subscription's ``created`` until a subscription payload supplies it.
    first_seen: Dict[str, Any] = {}
    if not same_sub:
        first_seen = dict(provider_sub_created_at=ctx.created, provider_updated_at=None)
    elif existing.get("provider_sub_created_at") is None:
        first_seen = dict(provider_sub_created_at=ctx.created)

    interval = interval_for(plan_id) or "month"
    paid = session.get("payment_status") in ("paid", "no_payment_required")
    if not paid:
        # Delayed payment method: access starts when invoice.paid arrives.
        if not same_sub and _is_entitled_row(existing, ctx.now):
            return "pending_payment:kept_existing"
        if same_sub and _is_live_row(existing):
            _save_subscription(conn, user_id, ctx.now_iso, checkout_session_id=session_id)
            return "ok:already_active"
        _save_subscription(
            conn, user_id, ctx.now_iso,
            plan_id=plan_id, status="incomplete", provider=PROVIDER,
            provider_sub_id=subscription_id, provider_customer_id=customer_id,
            billing_interval=interval, checkout_session_id=session_id,
            cancel_at_period_end=0, grace_period_end=None,
            **first_seen,
        )
        return "pending_payment"

    if same_sub and _is_live_row(existing):
        # invoice.paid / subscription.updated got here first; keep their state.
        _save_subscription(
            conn, user_id, ctx.now_iso,
            checkout_session_id=session_id, provider_customer_id=customer_id, provider=PROVIDER,
            **first_seen,
        )
        return "ok:already_active"

    if not same_sub and _is_live_row(existing) and existing.get("provider") == PROVIDER:
        log.warning(
            "user %s completed checkout for subscription %s while subscription %s is still live; "
            "the older one must be cancelled in Stripe",
            user_id, subscription_id, existing.get("provider_sub_id"),
        )
    provisional = (ctx.now + timedelta(days=PROVISIONAL_PERIOD_DAYS)).isoformat()
    period_end = _later_iso(existing.get("current_period_end"), provisional) if same_sub else provisional
    _save_subscription(
        conn, user_id, ctx.now_iso,
        plan_id=plan_id, status="active", provider=PROVIDER,
        provider_sub_id=subscription_id, provider_customer_id=customer_id,
        billing_interval=interval, checkout_session_id=session_id,
        current_period_end=period_end, cancel_at_period_end=0, grace_period_end=None,
        previous_plan_id=None,
        **first_seen,
    )
    return "ok:activated"


def _on_invoice_paid(conn, ctx: _Ctx) -> str:
    inv = ctx.obj
    if not inv.get("id"):
        return "ignored:no_invoice_id"
    subscription_id = _invoice_subscription_id(inv)
    if not subscription_id:
        return "ignored:not_subscription_invoice"
    customer_id = _ref_id(inv.get("customer"))
    metadata = _invoice_metadata(inv)
    user_id = _resolve_user(
        conn, customer_id=customer_id, subscription_id=subscription_id,
        hinted_user_id=metadata.get("user_id"),
    )
    if not user_id:
        return "unmatched:unknown_customer"

    existing = _get_subscription(conn, user_id)
    line = _invoice_main_line(inv)
    price_id = _line_price_id(line)
    plan_id = _resolve_plan(ctx, price_id, metadata.get("plan_id"), existing, subscription_id)

    amount_minor = _as_int(inv.get("amount_paid"))
    if amount_minor is None:
        amount_minor = _as_int(inv.get("total")) or 0
    _upsert_invoice(
        conn, ctx, inv, user_id=user_id, status="paid", amount_minor=amount_minor,
        subscription_id=subscription_id, plan_id=plan_id, line=line,
    )

    if not plan_id:
        log.error("invoice %s paid but its plan could not be determined (price=%s)", inv["id"], price_id)
        return "invoice_recorded:unknown_plan"

    same_sub = bool(existing) and existing.get("provider_sub_id") == subscription_id
    if same_sub and str(existing.get("status") or "").lower() == "canceled":
        return "invoice_recorded:subscription_ended"
    if _subscription_known_ended(conn, subscription_id):
        # Deleted before this invoice was delivered: the payment is recorded
        # above but an ended subscription grants nothing.
        return "invoice_recorded:subscription_ended"
    if _other_live_subscription(existing, same_sub, ctx.now):
        # The user's current subscription is a different, live one: do not let a
        # late invoice of an older subscription replace it.
        log.warning(
            "invoice %s is for subscription %s but user %s is on %s",
            inv["id"], subscription_id, user_id, existing.get("provider_sub_id"),
        )
        return "invoice_recorded:other_subscription"

    if not _remember_customer(conn, ctx, user_id, customer_id):
        return "invoice_recorded:customer_conflict"

    period_end = _iso(_as_int(_dig(line, "period", "end")))
    if same_sub:
        period_end = _later_iso(existing.get("current_period_end"), period_end)
    if not period_end and not (same_sub and _is_live_row(existing)):
        # (A live row of this subscription keeps what it has: a missing period
        # end there means "not known yet", which is not a reason to shorten it.)
        period_end = (ctx.now + timedelta(days=PROVISIONAL_PERIOD_DAYS)).isoformat()

    fields: Dict[str, Any] = dict(
        plan_id=plan_id, status="active", provider=PROVIDER,
        provider_sub_id=subscription_id, provider_customer_id=customer_id,
        billing_interval=interval_for(plan_id), current_period_end=period_end,
        grace_period_end=None, previous_plan_id=None,
    )
    if price_id:
        fields["provider_price_id"] = price_id
    if not same_sub:
        fields["cancel_at_period_end"] = 0
        # First sight of this subscription: forget the previous one's ordering
        # state. Its first invoice is created together with the subscription.
        fields["provider_updated_at"] = None
        fields["provider_sub_created_at"] = (
            _as_int(inv.get("created")) if inv.get("billing_reason") == "subscription_create" else None
        )
    _save_subscription(conn, user_id, ctx.now_iso, **fields)
    return "ok:period_extended"


def _on_invoice_payment_failed(conn, ctx: _Ctx) -> str:
    inv = ctx.obj
    if not inv.get("id"):
        return "ignored:no_invoice_id"
    subscription_id = _invoice_subscription_id(inv)
    if not subscription_id:
        return "ignored:not_subscription_invoice"
    customer_id = _ref_id(inv.get("customer"))
    metadata = _invoice_metadata(inv)
    user_id = _resolve_user(
        conn, customer_id=customer_id, subscription_id=subscription_id,
        hinted_user_id=metadata.get("user_id"),
    )
    if not user_id:
        return "unmatched:unknown_customer"

    recorded = conn.execute("SELECT status FROM invoices WHERE id = ?", (inv["id"],)).fetchone()
    if recorded and str(recorded[0]).lower() == "paid":
        return "ignored:invoice_already_paid"

    existing = _get_subscription(conn, user_id)
    if not existing or existing.get("provider_sub_id") != subscription_id:
        # e.g. the first payment of a checkout that never completed.
        return "ignored:no_matching_subscription"
    status = str(existing.get("status") or "").lower()
    if status not in ("active", "trialing", "past_due"):
        return "ignored:subscription_not_active"

    line = _invoice_main_line(inv)
    amount_minor = _as_int(inv.get("amount_due"))
    if amount_minor is None:
        amount_minor = _as_int(inv.get("total")) or 0
    _upsert_invoice(
        conn, ctx, inv, user_id=user_id, status="open", amount_minor=amount_minor,
        subscription_id=subscription_id,
        plan_id=_resolve_plan(ctx, _line_price_id(line), metadata.get("plan_id"), existing, subscription_id),
        line=line,
    )

    # Grace runs from the first failure; later retries must not extend it.
    grace_end = existing.get("grace_period_end") if status == "past_due" else None
    if not grace_end:
        grace_end = _iso(ctx.created + GRACE_PERIOD_DAYS * 86400)
    _save_subscription(conn, user_id, ctx.now_iso, status="past_due", grace_period_end=grace_end)
    return "ok:past_due"


def _subscription_period_end(sub: Dict[str, Any]) -> Optional[int]:
    end = _as_int(sub.get("current_period_end"))
    if end is not None:
        return end
    items = _dig(sub, "items", "data")
    ends = [
        _as_int(item.get("current_period_end"))
        for item in (items if isinstance(items, list) else [])
        if isinstance(item, dict)
    ]
    ends = [value for value in ends if value is not None]
    return max(ends) if ends else None


def _downgrade_to_free(conn, ctx: _Ctx, user_id: str, existing: Dict[str, Any], ended_at: Optional[int]) -> None:
    previous = existing.get("plan_id") if is_paid_plan(existing.get("plan_id")) else existing.get("previous_plan_id")
    _save_subscription(
        conn, user_id, ctx.now_iso,
        plan_id=FREE_PLAN_ID, status="canceled", previous_plan_id=previous,
        cancel_at_period_end=0, grace_period_end=None,
        current_period_end=_iso(ended_at) or ctx.now_iso,
        provider_updated_at=ctx.created,
    )


def _on_subscription_updated(conn, ctx: _Ctx) -> str:
    sub = ctx.obj
    subscription_id = sub.get("id")
    if not subscription_id:
        return "ignored:no_subscription_id"
    status = str(sub.get("status") or "").lower()
    sub_created = _as_int(sub.get("created"))
    if status == "canceled":
        # Terminal in Stripe. Remembered before anything else (even when the
        # user cannot be resolved): nothing delivered later may activate it.
        _mark_subscription_ended(conn, ctx, subscription_id)
    customer_id = _ref_id(sub.get("customer"))
    metadata = _metadata(sub.get("metadata"))
    user_id = _resolve_user(
        conn, customer_id=customer_id, subscription_id=subscription_id,
        hinted_user_id=metadata.get("user_id"),
    )
    if not user_id:
        return "unmatched:unknown_customer"

    existing = _get_subscription(conn, user_id)
    same_sub = bool(existing) and existing.get("provider_sub_id") == subscription_id

    if same_sub:
        if str(existing.get("status") or "").lower() == "canceled":
            return "ignored:subscription_already_ended"
        if status != "canceled":
            # ("canceled" is terminal in Stripe, so it is never out of date.)
            applied = _as_int(existing.get("provider_updated_at"))
            if applied is not None and ctx.created < applied:
                return "ignored:stale_event"
            if status in ("incomplete", "incomplete_expired") and _is_live_row(existing):
                # Stripe never moves a subscription back to incomplete: this is an
                # older event delivered after the payment was already recorded.
                return "ignored:stale_event"
    elif status == "canceled":
        # A sibling ended; the stored subscription is untouched.
        return "ignored:other_subscription"
    elif _subscription_known_ended(conn, subscription_id):
        # A late snapshot of a subscription Stripe has since deleted.
        return "ignored:subscription_already_ended"
    elif _other_live_subscription(existing, same_sub, ctx.now):
        # The user is on a different, live subscription. The event-time guard
        # above only orders events of ONE subscription, so an old sibling's
        # late "active" snapshot must not take the row over (the live
        # subscription's next invoice would then be refused and a paying user
        # downgraded). Only a subscription that the signed payload shows to be
        # active AND created after the stored one may replace it.
        if status not in ("active", "trialing"):
            return "ignored:other_subscription"
        stored_created = _as_int(existing.get("provider_sub_created_at"))
        if sub_created is None or stored_created is None or sub_created <= stored_created:
            log.warning(
                "ignoring update for subscription %s (created=%s): user %s is on live subscription %s (created=%s)",
                subscription_id, sub_created, user_id, existing.get("provider_sub_id"), stored_created,
            )
            return "ignored:other_subscription"
        log.warning(
            "user %s moves from live subscription %s to newer subscription %s; "
            "the older one must be cancelled in Stripe",
            user_id, existing.get("provider_sub_id"), subscription_id,
        )

    if status == "incomplete_expired":
        # Also terminal (the first payment never succeeded).
        _mark_subscription_ended(conn, ctx, subscription_id)
    if status == "canceled":
        _downgrade_to_free(conn, ctx, user_id, existing, _as_int(sub.get("ended_at")) or _as_int(sub.get("canceled_at")))
        return "ok:downgraded"

    price_id = _ref_id(_dig(sub, "items", "data", 0, "price")) or _ref_id(_dig(sub, "items", "data", 0, "plan"))
    plan_id = _resolve_plan(ctx, price_id, metadata.get("plan_id"), existing, subscription_id)
    if not plan_id:
        log.error("subscription %s updated but its plan could not be determined (price=%s)", subscription_id, price_id)
        return "unmatched:unknown_plan"
    if not _remember_customer(conn, ctx, user_id, customer_id):
        return "unmatched:customer_conflict"

    interval = _dig(sub, "items", "data", 0, "price", "recurring", "interval") or interval_for(plan_id)
    period_end = _iso(_subscription_period_end(sub))
    cancel_pending = bool(sub.get("cancel_at_period_end")) or sub.get("cancel_at") is not None

    fields: Dict[str, Any] = dict(
        plan_id=plan_id, status=status or "incomplete", provider=PROVIDER,
        provider_sub_id=subscription_id, provider_customer_id=customer_id,
        billing_interval=interval, cancel_at_period_end=1 if cancel_pending else 0,
        provider_updated_at=ctx.created,
    )
    if price_id:
        fields["provider_price_id"] = price_id
    if period_end:
        fields["current_period_end"] = period_end
    elif not same_sub:
        # No period end anywhere in the payload. An active Stripe-backed row
        # without one is provisionally entitled (see entitlements) and the next
        # invoice.paid / subscription.updated fills it in.
        fields["current_period_end"] = None
    if sub_created is not None:
        fields["provider_sub_created_at"] = sub_created
    elif not same_sub:
        fields["provider_sub_created_at"] = None

    if status == "past_due":
        grace_end = None
        if same_sub and str(existing.get("status") or "").lower() == "past_due":
            grace_end = existing.get("grace_period_end")
        fields["grace_period_end"] = grace_end or _iso(ctx.created + GRACE_PERIOD_DAYS * 86400)
    else:
        fields["grace_period_end"] = None
    if status in ("active", "trialing"):
        fields["previous_plan_id"] = None

    _save_subscription(conn, user_id, ctx.now_iso, **fields)
    return f"ok:synced:{status or 'unknown'}"


def _on_subscription_deleted(conn, ctx: _Ctx) -> str:
    sub = ctx.obj
    subscription_id = sub.get("id")
    if not subscription_id:
        return "ignored:no_subscription_id"
    # Remembered whatever happens next (even when the user cannot be resolved
    # yet): a checkout / invoice / update for this subscription delivered after
    # its deletion must never activate it.
    _mark_subscription_ended(conn, ctx, subscription_id)
    user_id = _resolve_user(
        conn, customer_id=_ref_id(sub.get("customer")), subscription_id=subscription_id,
        hinted_user_id=_metadata(sub.get("metadata")).get("user_id"),
    )
    if not user_id:
        return "unmatched:unknown_customer"
    existing = _get_subscription(conn, user_id)
    if not existing or existing.get("provider_sub_id") != subscription_id:
        # The ended subscription is not the one the user is on now (e.g. an old
        # subscription ending while a newer one is live): nothing to downgrade.
        return "ignored:other_subscription"
    _downgrade_to_free(conn, ctx, user_id, existing, _as_int(sub.get("ended_at")) or _as_int(sub.get("canceled_at")))
    return "ok:downgraded"


_HANDLERS: Dict[str, Callable[[Any, _Ctx], str]] = {
    "checkout.session.completed": _on_checkout_completed,
    "invoice.paid": _on_invoice_paid,
    "invoice.payment_failed": _on_invoice_payment_failed,
    "customer.subscription.updated": _on_subscription_updated,
    "customer.subscription.deleted": _on_subscription_deleted,
}

HANDLED_EVENT_TYPES = tuple(sorted(_HANDLERS))


def process_stripe_event(
    db,
    event: Dict[str, Any],
    *,
    price_to_plan: Optional[Mapping[str, str]] = None,
    now: Optional[datetime] = None,
    require_livemode: bool = False,
) -> Dict[str, str]:
    """Apply one verified Stripe event exactly once.

    Returns ``{"status": "processed" | "ignored" | "duplicate", "result": ...}``.
    Any exception rolls the whole transaction back (including the event-id
    record), so Stripe's retry of the event is processed from scratch.
    """
    event_id = event.get("id")
    event_type = event.get("type")
    if not event_id or not event_type or not isinstance(_dig(event, "data", "object"), dict):
        raise WebhookPayloadError("not a Stripe event")

    now = now or datetime.now(timezone.utc)
    ctx = _Ctx(event, price_to_plan or {}, now)
    handler = _HANDLERS.get(str(event_type))
    if handler is not None and require_livemode and not event.get("livemode"):
        log.warning("ignoring test-mode stripe event %s (%s) in production", event_id, event_type)
        handler = None
        forced_result = "ignored:test_mode_event"
    else:
        forced_result = None

    with db.connect() as conn:
        # Take the write lock first so concurrent deliveries of one event serialise.
        conn.execute("BEGIN IMMEDIATE")
        _ensure_schema(conn)
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO billing_events
                (event_id, event_type, provider, payload_json, processed_at, created_at)
            VALUES (?, ?, ?, ?, NULL, ?)
            """,
            (
                str(event_id),
                str(event_type),
                PROVIDER,
                json.dumps(event, separators=(",", ":")) if handler is not None else None,
                ctx.now_iso,
            ),
        )
        if cur.rowcount == 0:
            return {"status": "duplicate", "result": "duplicate"}

        result = handler(conn, ctx) if handler is not None else (forced_result or "ignored:unhandled_type")
        conn.execute(
            "UPDATE billing_events SET processed_at = ?, result = ? WHERE provider = ? AND event_id = ?",
            (ctx.now_iso, result, PROVIDER, str(event_id)),
        )

    if handler is None:
        return {"status": "ignored", "result": result}
    log.info("stripe event %s (%s): %s", event_id, event_type, result)
    return {"status": "processed", "result": result}
