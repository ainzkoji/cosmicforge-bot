"""Append-only billing audit log (Section H Step 8.1).

Every billing state change -- a webhook writing a subscription row, a
refund or dispute being recorded, an operator resolving a payment review, a
user cancelling or resuming, a lapsed subscription being downgraded -- adds one
row to ``billing_audit_log``. The table rejects UPDATE and DELETE (triggers in
``shared_lib.billing.schema``).

Only billing state is recorded: plan, status, period and provider ids. No card
data, no email address and no raw webhook payload.
"""
from __future__ import annotations

import contextvars
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

TABLE = "billing_audit_log"

#: Subscription columns copied into before/after snapshots.
SNAPSHOT_COLUMNS = (
    "plan_id", "status", "provider", "provider_sub_id", "provider_customer_id",
    "provider_price_id", "billing_interval", "current_period_end",
    "cancel_at_period_end", "grace_period_end", "previous_plan_id", "billing_hold",
)

#: The provider event being applied on this thread (set by process_stripe_event).
_CURRENT_EVENT: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar(
    "billing_current_event", default=None
)


@contextmanager
def event_context(event_id: Optional[str], event_type: Optional[str]) -> Iterator[None]:
    token = _CURRENT_EVENT.set({"id": event_id, "type": event_type})
    try:
        yield
    finally:
        _CURRENT_EVENT.reset(token)


def current_event() -> Dict[str, Any]:
    return dict(_CURRENT_EVENT.get() or {})


def _table_exists(conn) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone() is not None


def snapshot(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {col: row.get(col) for col in SNAPSHOT_COLUMNS if col in row}


def _dump(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def record(
    conn,
    *,
    action: str,
    user_id: Optional[str] = None,
    actor_type: Optional[str] = None,
    actor_id: Optional[str] = None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
    details: Optional[Dict[str, Any]] = None,
    now: Optional[str] = None,
) -> None:
    """Append one audit row inside the caller's transaction.

    Without an explicit actor, a row written while a provider event is being
    applied is attributed to that event (``stripe_webhook``).
    """
    if not _table_exists(conn):
        # Only before migrate() has run (e.g. a hand-built test schema).
        return
    event = current_event()
    if actor_type is None:
        actor_type = "stripe_webhook" if event.get("id") else "system"
    conn.execute(
        f"""
        INSERT INTO {TABLE} (occurred_at, actor_type, actor_id, user_id, action,
                             provider_event_id, provider_event_type, before_json, after_json, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now or datetime.now(timezone.utc).isoformat(),
            actor_type,
            actor_id,
            user_id,
            action,
            event.get("id"),
            event.get("type"),
            _dump(before),
            _dump(after),
            _dump(details),
        ),
    )


def list_entries(conn_or_db, *, user_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    def _run(conn) -> List[Dict[str, Any]]:
        if not _table_exists(conn):
            return []
        sql = f"SELECT * FROM {TABLE}"
        params: tuple = ()
        if user_id is not None:
            sql += " WHERE user_id = ?"
            params = (str(user_id),)
        sql += " ORDER BY id DESC LIMIT ?"
        cur = conn.execute(sql, (*params, int(limit)))
        cols = [d[0] for d in cur.description]
        out = []
        for row in cur.fetchall():
            item = dict(zip(cols, row))
            for key in ("before_json", "after_json", "details_json"):
                raw = item.pop(key)
                item[key[:-5]] = json.loads(raw) if raw else None
            out.append(item)
        return out

    if hasattr(conn_or_db, "connect"):
        with conn_or_db.connect() as conn:
            return _run(conn)
    return _run(conn_or_db)


__all__ = ["SNAPSHOT_COLUMNS", "event_context", "current_event", "snapshot", "record", "list_entries"]
