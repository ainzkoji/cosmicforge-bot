"""Refunds and disputes: recorded, flagged for an operator, never silently ignored.

Policy (Section H Step 8.1):

* A **dispute** (chargeback) on any charge, and a **full refund** of a charge,
  open a review that *holds* the user's paid entitlement: while the review is
  open the user is served the free plan (``subscriptions.billing_hold``, read
  by ``entitlements.resolve_subscription``). Paid access is not silently kept
  after the money went back.
* A **partial refund** opens a review that does not hold access (the customer
  still paid for the period) but is listed for an operator.
* Only an operator resolves a review: ``RESOLVED_KEEP_ACCESS`` lifts the hold,
  ``RESOLVED_REVOKE_ACCESS`` keeps it (the paid plan stays suspended until a
  later review resolution says otherwise; cancelling the subscription in
  Stripe is the operator's follow-up).
* Every review change is written to the billing audit log.

A refund or dispute whose user cannot be resolved is still recorded (user_id
NULL) so an operator sees it.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from shared_lib.billing import audit

TABLE = "billing_payment_reviews"
OPEN = "OPEN"
RESOLVED_KEEP = "RESOLVED_KEEP_ACCESS"
RESOLVED_REVOKE = "RESOLVED_REVOKE_ACCESS"
RESOLUTIONS = (RESOLVED_KEEP, RESOLVED_REVOKE)
#: Review states that keep the paid entitlement suspended (when the review holds).
HOLDING_STATES = (OPEN, RESOLVED_REVOKE)

ZERO_DECIMAL_CURRENCIES = frozenset({
    "bif", "clp", "djf", "gnf", "jpy", "kmf", "krw", "mga", "pyg", "rwf",
    "ugx", "vnd", "vuv", "xaf", "xof", "xpf",
})


class ReviewError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _now_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


def decimal_amount(amount_minor: Optional[int], currency: Optional[str]) -> Optional[str]:
    """Exact major-unit amount as TEXT (never a float)."""
    if amount_minor is None:
        return None
    exponent = 0 if str(currency or "").lower() in ZERO_DECIMAL_CURRENCIES else 2
    value = Decimal(int(amount_minor)).scaleb(-exponent)
    return format(value, "f")


def _fetch_dict(conn, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    return {desc[0]: row[idx] for idx, desc in enumerate(cur.description)}


def _fetch_all(conn, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_review(conn, review_id: str) -> Optional[Dict[str, Any]]:
    return _fetch_dict(conn, f"SELECT * FROM {TABLE} WHERE review_id = ?", (review_id,))


def find_review(conn, kind: str, provider_object_id: str) -> Optional[Dict[str, Any]]:
    return _fetch_dict(
        conn,
        f"SELECT * FROM {TABLE} WHERE provider = 'stripe' AND kind = ? AND provider_object_id = ?",
        (kind, provider_object_id),
    )


def upsert_review(
    conn,
    *,
    kind: str,
    provider_object_id: str,
    user_id: Optional[str],
    charge_id: Optional[str],
    invoice_id: Optional[str],
    amount_minor: Optional[int],
    amount_refunded_minor: Optional[int],
    currency: Optional[str],
    reason: Optional[str],
    provider_status: Optional[str],
    full_refund: bool,
    holds_entitlement: bool,
    event_id: Optional[str],
    event_created: Optional[int],
    now_iso: str,
) -> Dict[str, Any]:
    """Create or update the review for one refund (per charge) or dispute.

    A later event never lowers what an earlier one established: the refunded
    amount only grows, a hold once required stays required, and a review an
    operator resolved is reopened only when the new event is worse (more money
    refunded, or a newly holding condition).
    """
    existing = find_review(conn, kind, provider_object_id)
    if existing is None:
        review_id = f"bpr_{uuid.uuid4().hex[:16]}"
        conn.execute(
            f"""
            INSERT INTO {TABLE} (review_id, kind, provider, provider_object_id, charge_id, invoice_id, user_id,
                amount_minor, amount, amount_refunded_minor, currency, reason, provider_status, full_refund,
                holds_entitlement, review_status, first_event_id, last_event_id, last_event_created,
                created_at, updated_at)
            VALUES (?, ?, 'stripe', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?)
            """,
            (
                review_id, kind, provider_object_id, charge_id, invoice_id, user_id,
                amount_minor, decimal_amount(amount_minor, currency), amount_refunded_minor,
                (currency or "").upper() or None, reason, provider_status, 1 if full_refund else 0,
                1 if holds_entitlement else 0, event_id, event_id, event_created, now_iso, now_iso,
            ),
        )
        review = get_review(conn, review_id)
        audit.record(conn, action=f"payment_review_opened:{kind.lower()}", user_id=user_id, after=review,
                     details={"holds_entitlement": bool(holds_entitlement)}, now=now_iso)
        return review

    worse = (
        (amount_refunded_minor or 0) > (existing.get("amount_refunded_minor") or 0)
        or (holds_entitlement and not existing.get("holds_entitlement"))
    )
    stale = (
        event_created is not None and existing.get("last_event_created") is not None
        and int(event_created) < int(existing["last_event_created"])
    )
    fields: Dict[str, Any] = {
        "user_id": existing.get("user_id") or user_id,
        "charge_id": existing.get("charge_id") or charge_id,
        "invoice_id": existing.get("invoice_id") or invoice_id,
        "amount_refunded_minor": max(existing.get("amount_refunded_minor") or 0, amount_refunded_minor or 0) or None,
        "full_refund": 1 if (existing.get("full_refund") or full_refund) else 0,
        "holds_entitlement": 1 if (existing.get("holds_entitlement") or holds_entitlement) else 0,
        "last_event_id": event_id,
        "updated_at": now_iso,
    }
    if not stale:
        fields["provider_status"] = provider_status or existing.get("provider_status")
        fields["last_event_created"] = event_created
        if reason:
            fields["reason"] = reason
    if worse and existing.get("review_status") != OPEN:
        fields.update(review_status=OPEN, resolved_at=None, resolved_by=None, resolution_note=None)
    cols = sorted(fields)
    conn.execute(
        f"UPDATE {TABLE} SET {', '.join(f'{c} = ?' for c in cols)} WHERE review_id = ?",
        (*[fields[c] for c in cols], existing["review_id"]),
    )
    review = get_review(conn, existing["review_id"])
    audit.record(conn, action=f"payment_review_updated:{kind.lower()}", user_id=review.get("user_id"),
                 before=existing, after=review, details={"reopened": bool(worse and existing.get("review_status") != OPEN)},
                 now=now_iso)
    return review


def holding_review_id(conn, user_id: str) -> Optional[str]:
    row = conn.execute(
        f"""SELECT review_id FROM {TABLE}
            WHERE user_id = ? AND holds_entitlement = 1 AND review_status IN (?, ?)
            ORDER BY created_at, review_id LIMIT 1""",
        (str(user_id), *HOLDING_STATES),
    ).fetchone()
    return str(row[0]) if row else None


def recompute_hold(conn, user_id: Optional[str], now_iso: str) -> Optional[str]:
    """Set ``subscriptions.billing_hold`` from the user's reviews. Returns the hold."""
    if not user_id:
        return None
    hold = holding_review_id(conn, user_id)
    row = _fetch_dict(conn, "SELECT * FROM subscriptions WHERE user_id = ?", (str(user_id),))
    if row is None or row.get("billing_hold") == hold:
        return hold
    conn.execute("UPDATE subscriptions SET billing_hold = ?, updated_at = ? WHERE user_id = ?",
                 (hold, now_iso, str(user_id)))
    after = dict(row, billing_hold=hold)
    audit.record(conn, action="entitlement_hold_set" if hold else "entitlement_hold_cleared", user_id=str(user_id),
                 before=audit.snapshot(row), after=audit.snapshot(after), details={"review_id": hold}, now=now_iso)
    return hold


def resolve_review(
    conn_or_db,
    review_id: str,
    *,
    decision: str,
    operator_id: str,
    note: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Operator decision on a refund / dispute review."""
    if decision not in RESOLUTIONS:
        raise ReviewError("INVALID_DECISION", f"decision must be one of {', '.join(RESOLUTIONS)}")
    note = (note or "").strip()
    if len(note) < 3:
        raise ReviewError("NOTE_REQUIRED", "a resolution note is required")

    def _run(conn) -> Dict[str, Any]:
        review = get_review(conn, review_id)
        if review is None:
            raise ReviewError("REVIEW_NOT_FOUND", "payment review not found")
        now_iso = _now_iso(now)
        conn.execute(
            f"""UPDATE {TABLE} SET review_status = ?, resolved_at = ?, resolved_by = ?, resolution_note = ?,
                updated_at = ? WHERE review_id = ?""",
            (decision, now_iso, str(operator_id), note, now_iso, review_id),
        )
        after = get_review(conn, review_id)
        audit.record(conn, action="payment_review_resolved", user_id=review.get("user_id"),
                     actor_type="operator", actor_id=str(operator_id), before=review, after=after,
                     details={"decision": decision}, now=now_iso)
        hold = recompute_hold(conn, review.get("user_id"), now_iso)
        return {**after, "user_billing_hold": hold}

    if hasattr(conn_or_db, "connect"):
        with conn_or_db.connect() as conn:
            return _run(conn)
    return _run(conn_or_db)


def list_reviews(conn_or_db, *, status: Optional[str] = None, user_id: Optional[str] = None,
                 limit: int = 200) -> List[Dict[str, Any]]:
    def _run(conn) -> List[Dict[str, Any]]:
        clauses, params = [], []
        if status:
            clauses.append("review_status = ?")
            params.append(status)
        if user_id:
            clauses.append("user_id = ?")
            params.append(str(user_id))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return _fetch_all(conn, f"SELECT * FROM {TABLE}{where} ORDER BY created_at DESC LIMIT ?",
                          (*params, int(limit)))

    if hasattr(conn_or_db, "connect"):
        with conn_or_db.connect() as conn:
            return _run(conn)
    return _run(conn_or_db)


__all__ = [
    "OPEN", "RESOLVED_KEEP", "RESOLVED_REVOKE", "RESOLUTIONS", "ReviewError",
    "decimal_amount", "upsert_review", "recompute_hold", "holding_review_id",
    "resolve_review", "list_reviews", "get_review", "find_review",
]
