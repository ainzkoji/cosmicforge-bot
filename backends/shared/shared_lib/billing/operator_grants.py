"""Operator plan grants (Step 1.5): auditable, time-limited, distinguishable from Stripe.

A grant writes the user's single ``subscriptions`` row with ``provider =
'operator'``, ``status = 'active'`` and an explicit ``current_period_end``.
``resolve_subscription`` treats a non-Stripe row as entitled exactly until its
period end (no grace), so the expiration rule is the row itself. Every grant
and revocation is recorded in ``billing_operator_grants`` with the issuing
administrator, the user, the plan, the timestamps and the reason; the row the
grant replaced is kept so a revocation restores it.

Stripe webhook processing never overwrites an active grant: ``webhooks._save_subscription``
asks ``active_grant`` first and, while a grant is active, records the attempted
write on the grant (``deferred_events``) instead of applying it. Revoking the
grant restores the stored previous row; if Stripe events were deferred in the
meantime the revocation reports it so the operator can resynchronise.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from shared_lib.billing.entitlements import parse_ts
from shared_lib.billing.plans import FREE_PLAN_ID, is_paid_plan

PROVIDER = "operator"
TABLE = "billing_operator_grants"
MAX_DURATION_DAYS = 366
DEFAULT_DURATION_DAYS = 30


class GrantError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def ensure_schema(conn) -> None:
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        grant_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, plan_id TEXT NOT NULL,
        issued_by TEXT NOT NULL, reason TEXT NOT NULL, granted_at TEXT NOT NULL, expires_at TEXT NOT NULL,
        revoked_at TEXT, revoked_by TEXT, revoke_reason TEXT,
        previous_row_json TEXT, deferred_events_json TEXT NOT NULL DEFAULT '[]')""")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_user ON {TABLE}(user_id, granted_at)")


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


def active_grant(conn, user_id: str, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """The user's unexpired, unrevoked grant, or None. Safe before the table exists."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
        return None
    row = _fetch_dict(conn, f"SELECT * FROM {TABLE} WHERE user_id=? AND revoked_at IS NULL ORDER BY granted_at DESC LIMIT 1",
                      (str(user_id),))
    if row is None:
        return None
    expires = parse_ts(row["expires_at"])
    if expires is None or _now(now) > expires:
        return None
    return row


def grant(db, *, user_id: str, plan_id: str, issued_by: str, reason: str, duration_days: int = DEFAULT_DURATION_DAYS,
          now: Optional[datetime] = None) -> Dict[str, Any]:
    """Grant ``plan_id`` to ``user_id`` until ``now + duration_days``.

    An existing active grant is superseded (revoked with reason SUPERSEDED);
    the row the FIRST grant replaced stays the one a later revocation restores.
    """
    if not is_paid_plan(plan_id):
        raise GrantError("UNKNOWN_PLAN", f"{plan_id!r} is not a grantable paid plan")
    if not str(issued_by or "").strip():
        raise GrantError("ISSUER_REQUIRED", "the issuing administrator must be identified")
    if len(str(reason or "").strip()) < 3:
        raise GrantError("REASON_REQUIRED", "a reason is required")
    days = int(duration_days)
    if not 1 <= days <= MAX_DURATION_DAYS:
        raise GrantError("INVALID_DURATION", f"duration_days must be between 1 and {MAX_DURATION_DAYS}")
    at = _now(now)
    expires = at + timedelta(days=days)
    grant_id = f"grant_{uuid.uuid4().hex[:16]}"
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        ensure_schema(conn)
        if not conn.execute("SELECT 1 FROM users WHERE id=?", (str(user_id),)).fetchone():
            raise GrantError("USER_NOT_FOUND", "no such user")
        current = active_grant(conn, user_id, at)
        existing_row = _fetch_dict(conn, "SELECT * FROM subscriptions WHERE user_id=?", (str(user_id),))
        if current is not None:
            # Keep the ORIGINAL replaced row across a chain of grants.
            previous_json = current["previous_row_json"]
            conn.execute(f"UPDATE {TABLE} SET revoked_at=?, revoked_by=?, revoke_reason='SUPERSEDED' WHERE grant_id=?",
                         (_iso(at), str(issued_by), current["grant_id"]))
        else:
            previous_json = json.dumps(existing_row, sort_keys=True, default=str) if existing_row else None
        conn.execute(f"INSERT INTO {TABLE} (grant_id,user_id,plan_id,issued_by,reason,granted_at,expires_at,previous_row_json) "
                     "VALUES(?,?,?,?,?,?,?,?)",
                     (grant_id, str(user_id), plan_id, str(issued_by), str(reason).strip(), _iso(at), _iso(expires), previous_json))
        fields = {"plan_id": plan_id, "status": "active", "provider": PROVIDER, "provider_sub_id": f"operator:{grant_id}",
                  "current_period_end": _iso(expires), "cancel_at_period_end": 1, "grace_period_end": None,
                  "previous_plan_id": (existing_row or {}).get("plan_id")}
        if existing_row:
            cols = sorted(fields)
            conn.execute(f"UPDATE subscriptions SET {', '.join(f'{c}=?' for c in cols)}, updated_at=? WHERE user_id=?",
                         (*[fields[c] for c in cols], _iso(at), str(user_id)))
        else:
            cols = sorted(fields)
            conn.execute(f"INSERT INTO subscriptions (user_id, {', '.join(cols)}, created_at, updated_at) "
                         f"VALUES (?, {', '.join('?' for _ in cols)}, ?, ?)",
                         (str(user_id), *[fields[c] for c in cols], _iso(at), _iso(at)))
    return {"grant_id": grant_id, "user_id": str(user_id), "plan_id": plan_id, "issued_by": str(issued_by),
            "reason": str(reason).strip(), "granted_at": _iso(at), "expires_at": _iso(expires),
            "superseded_grant_id": current["grant_id"] if current else None}


def revoke(db, *, grant_id: str, revoked_by: str, reason: str, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Revoke a grant and restore the row it replaced (or the free plan)."""
    if not str(revoked_by or "").strip():
        raise GrantError("ISSUER_REQUIRED", "the revoking administrator must be identified")
    if len(str(reason or "").strip()) < 3:
        raise GrantError("REASON_REQUIRED", "a reason is required")
    at = _now(now)
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        ensure_schema(conn)
        row = _fetch_dict(conn, f"SELECT * FROM {TABLE} WHERE grant_id=?", (grant_id,))
        if row is None:
            raise GrantError("GRANT_NOT_FOUND", "no such grant")
        if row["revoked_at"]:
            raise GrantError("GRANT_ALREADY_REVOKED", "the grant was already revoked")
        conn.execute(f"UPDATE {TABLE} SET revoked_at=?, revoked_by=?, revoke_reason=? WHERE grant_id=?",
                     (_iso(at), str(revoked_by), str(reason).strip(), grant_id))
        deferred = json.loads(row.get("deferred_events_json") or "[]")
        previous = json.loads(row["previous_row_json"]) if row.get("previous_row_json") else None
        restorable = {k: v for k, v in (previous or {}).items() if k not in ("user_id", "created_at", "updated_at")}
        if restorable:
            cols = sorted(restorable)
            conn.execute(f"UPDATE subscriptions SET {', '.join(f'{c}=?' for c in cols)}, updated_at=? WHERE user_id=?",
                         (*[restorable[c] for c in cols], _iso(at), row["user_id"]))
            restored = "PREVIOUS_ROW"
        else:
            conn.execute("UPDATE subscriptions SET plan_id=?, status='canceled', provider=NULL, provider_sub_id=NULL, "
                         "current_period_end=NULL, cancel_at_period_end=0, grace_period_end=NULL, previous_plan_id=?, "
                         "updated_at=? WHERE user_id=?", (FREE_PLAN_ID, row["plan_id"], _iso(at), row["user_id"]))
            restored = "FREE_PLAN"
    return {"grant_id": grant_id, "user_id": row["user_id"], "revoked_at": _iso(at), "revoked_by": str(revoked_by),
            "reason": str(reason).strip(), "restored": restored, "deferred_stripe_events": deferred,
            "stripe_resync_required": bool(deferred)}


def defer_event(conn, grant_row: Dict[str, Any], event: Dict[str, Any]) -> None:
    """Record a Stripe write that was NOT applied because a grant is active."""
    deferred = json.loads(grant_row.get("deferred_events_json") or "[]")
    deferred.append(event)
    conn.execute(f"UPDATE {TABLE} SET deferred_events_json=? WHERE grant_id=?",
                 (json.dumps(deferred, sort_keys=True, default=str), grant_row["grant_id"]))


def list_grants(db, *, user_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
            return []
        if user_id is not None:
            rows = _fetch_all(conn, f"SELECT * FROM {TABLE} WHERE user_id=? ORDER BY granted_at DESC LIMIT ?",
                              (str(user_id), int(limit)))
        else:
            rows = _fetch_all(conn, f"SELECT * FROM {TABLE} ORDER BY granted_at DESC LIMIT ?", (int(limit),))
    for r in rows:
        r["deferred_events"] = json.loads(r.pop("deferred_events_json") or "[]")
        r.pop("previous_row_json", None)
    return rows


__all__ = ["PROVIDER", "TABLE", "MAX_DURATION_DAYS", "DEFAULT_DURATION_DAYS", "GrantError", "ensure_schema",
           "active_grant", "grant", "revoke", "defer_event", "list_grants"]
