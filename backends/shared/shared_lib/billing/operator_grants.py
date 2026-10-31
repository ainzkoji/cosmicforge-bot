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

At most one CURRENTLY ACTIVE grant per user (K2). An expired grant is never
revoked, so a user who was granted again legitimately has several unrevoked
rows; the rule is therefore "at most one unrevoked grant whose ``expires_at``
has not passed", which depends on the current time. No unique index can say
that (a partial index cannot reference ``now()``), and the overlap form as a
trigger would compare ISO-8601 TEXT, whose order on PostgreSQL follows the
database collation. The rule is enforced here instead, in the grant
transaction: ``BEGIN IMMEDIATE`` serialises every writer of this table on both
dialects (SQLite's write lock; the legacy PostgreSQL adapter's global write
advisory lock), ``grant`` supersedes EVERY grant still in force, and after its
insert it re-reads the user's grants and rolls back with
``ACTIVE_GRANT_CONFLICT`` unless the new grant is the only one in force.
Whether expiry should revoke (which would allow a plain partial unique index
on unrevoked rows) is an open owner decision; nothing here changes that policy.
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


def _in_force(conn, user_id: str, at: datetime) -> List[Dict[str, Any]]:
    """Every unrevoked grant of ``user_id`` whose expiry has not passed at ``at``, newest first. An expired
    unrevoked grant is history, not in force (the time comparison is on parsed datetimes, never on TEXT)."""
    rows = _fetch_all(conn, f"SELECT * FROM {TABLE} WHERE user_id=? AND revoked_at IS NULL "
                            "ORDER BY granted_at DESC, grant_id DESC", (str(user_id),))
    out = []
    for row in rows:
        expires = parse_ts(row["expires_at"])
        if expires is not None and not at > expires:
            out.append(row)
    return out


def active_grant(conn, user_id: str, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """The user's unexpired, unrevoked grant (the newest, should the invariant ever have been broken by a write
    outside ``grant``), or None. Safe before the table exists."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
        return None
    rows = _in_force(conn, user_id, _now(now))
    return rows[0] if rows else None


def grants_in_force_conflicts(conn, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Read-only integrity check of the one-active-grant rule: every user with more than one grant in force at
    ``now`` (``[{"user_id", "grant_ids"}]``). Expired-unrevoked history is not a conflict. Safe before the table
    exists. ``grant`` keeps this empty; a non-empty result means a write bypassed it."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
        return []
    at = _now(now)
    users = [r[0] for r in conn.execute(f"SELECT user_id FROM {TABLE} WHERE revoked_at IS NULL GROUP BY user_id "
                                        "HAVING COUNT(*) > 1").fetchall()]
    out = []
    for user_id in users:
        rows = _in_force(conn, user_id, at)
        if len(rows) > 1:
            out.append({"user_id": user_id, "grant_ids": [r["grant_id"] for r in rows]})
    return out


def grant(db, *, user_id: str, plan_id: str, issued_by: str, reason: str, duration_days: int = DEFAULT_DURATION_DAYS,
          now: Optional[datetime] = None) -> Dict[str, Any]:
    """Grant ``plan_id`` to ``user_id`` until ``now + duration_days``.

    An existing active grant is superseded (revoked with reason SUPERSEDED);
    the row the FIRST grant replaced stays the one a later revocation restores.
    Every grant still in force is superseded, and the transaction rolls back
    with ``ACTIVE_GRANT_CONFLICT`` unless the new grant is then the user's only
    grant in force (see the module docstring). Expired grants are left as they
    are: they are history, and whether expiry revokes is an owner decision.
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
        in_force = _in_force(conn, user_id, at)
        current = in_force[0] if in_force else None
        existing_row = _fetch_dict(conn, "SELECT * FROM subscriptions WHERE user_id=?", (str(user_id),))
        if current is not None:
            # Keep the ORIGINAL replaced row across a chain of grants (the oldest grant still in force carries it;
            # in a consistent table there is exactly one).
            previous_json = in_force[-1]["previous_row_json"]
            for row in in_force:
                conn.execute(f"UPDATE {TABLE} SET revoked_at=?, revoked_by=?, revoke_reason='SUPERSEDED' "
                             "WHERE grant_id=? AND revoked_at IS NULL", (_iso(at), str(issued_by), row["grant_id"]))
        else:
            previous_json = json.dumps(existing_row, sort_keys=True, default=str) if existing_row else None
        conn.execute(f"INSERT INTO {TABLE} (grant_id,user_id,plan_id,issued_by,reason,granted_at,expires_at,previous_row_json) "
                     "VALUES(?,?,?,?,?,?,?,?)",
                     (grant_id, str(user_id), plan_id, str(issued_by), str(reason).strip(), _iso(at), _iso(expires), previous_json))
        # The invariant, re-read inside the same serialised transaction: raising rolls back every write above.
        after = [r["grant_id"] for r in _in_force(conn, user_id, at)]
        if after != [grant_id]:
            raise GrantError("ACTIVE_GRANT_CONFLICT",
                             f"{len(after)} grants would be in force for this user; nothing was written")
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
            "superseded_grant_id": current["grant_id"] if current else None,
            "superseded_grant_ids": [r["grant_id"] for r in in_force]}


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
           "active_grant", "grant", "grants_in_force_conflicts", "revoke", "defer_event", "list_grants"]
