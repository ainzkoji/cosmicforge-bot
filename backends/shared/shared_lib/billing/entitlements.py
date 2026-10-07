"""Database-backed entitlements.

What a user may do is decided from the ``subscriptions`` row at the moment of
the request, never from a claim baked into an access token: a token minted
before a downgrade would otherwise keep paid limits until it expired.

Both backends share one SQLite database through ``shared_lib.persistence.db``;
every function here accepts either a ``DB`` (anything with ``connect()``) or an
open ``sqlite3`` connection.

Expiry rules (the same ones the webhook handlers write towards):

* ``active`` / ``trialing``  -- entitled until ``current_period_end`` plus
  :data:`GRACE_PERIOD_DAYS` (the renewal webhook normally moves the period end
  forward well before that). No grace when the subscription is set to cancel at
  period end, or when the row is not backed by a payment provider.
  A Stripe-backed row whose period end is not known yet (the provider payload
  carried none) is provisionally entitled; the next provider event fills it in.
* ``past_due``               -- entitled until ``grace_period_end``.
* anything else              -- free plan.

Reading is side-effect free: a row whose time has run out is *reported* as the
free plan. It is only downgraded in place (``plan_free`` / ``expired``) when a
caller asks for that with ``persist=True`` -- and that write never waits long
for the database's single write lock (see :data:`PERSIST_BUSY_TIMEOUT_MS`).
"""
from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Optional

from shared_lib.billing.plans import (
    FREE_PLAN_ID,
    PLAN_LIMITS,
    is_paid_plan,
    limits_for,
)

log = logging.getLogger("cosmicforge.billing.entitlements")

#: Days of continued access after a failed renewal payment (and the tolerance
#: for a late renewal webhook) before the user is downgraded to the free plan.
GRACE_PERIOD_DAYS = 7

#: How long the optional downgrade write (``persist=True``) may wait for the
#: write lock. SQLite has one writer; the regular 10 s busy timeout would stall
#: the request whenever another connection (the trading engine, or the caller's
#: own open transaction) holds it. On contention the write is skipped: the
#: computed plan is correct either way and a later read persists it.
PERSIST_BUSY_TIMEOUT_MS = 250

ENTITLED_STATUSES = frozenset({"active", "trialing"})
GRACE_STATUSES = frozenset({"past_due"})

#: Bots in these states no longer occupy a plan slot (matches the bot-backend's
#: ``BotInstanceService.get_user_bot_instances`` default listing).
_BOT_FREE_STATUSES = ("deleted", "archived")

__all__ = [
    "GRACE_PERIOD_DAYS",
    "get_effective_plan",
    "limits_for",
    "count_user_bots",
    "count_user_brokers",
    "bot_quota",
    "can_create_bot",
    "can_trade_live",
    "can_add_broker",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: Any) -> Optional[datetime]:
    """Parse a stored ISO timestamp. Naive values are taken as UTC."""
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@contextmanager
def _connection(conn_or_db) -> Iterator[Any]:
    if hasattr(conn_or_db, "connect"):
        with conn_or_db.connect() as conn:
            yield conn
    else:
        yield conn_or_db


def _fetch_dict(conn, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    # Built from the cursor description so it works with any row factory.
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    return {desc[0]: row[idx] for idx, desc in enumerate(cur.description)}


def _free_state(row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    row = row or {}
    return {
        "plan_id": FREE_PLAN_ID,
        "limits": limits_for(FREE_PLAN_ID),
        "status": "active",  # the free plan is always active
        "is_paid": False,
        "current_period_end": None,
        "cancel_at_period_end": False,
        "grace_period_end": None,
        "provider": row.get("provider"),
        "provider_sub_id": row.get("provider_sub_id"),
        "provider_status": row.get("status"),
        "checkout_session_id": row.get("checkout_session_id"),
        "previous_plan_id": row.get("previous_plan_id"),
    }


def resolve_subscription(row: Optional[Dict[str, Any]], now: Optional[datetime] = None) -> Dict[str, Any]:
    """Pure decision: the effective plan for a ``subscriptions`` row at ``now``.

    The returned dict carries ``expired=True`` when a paid row has run out of
    time and should be downgraded in the database.
    """
    now = now or _utc_now()
    if not row:
        state = _free_state()
        state["expired"] = False
        return state

    plan_id = row.get("plan_id")
    status = str(row.get("status") or "").strip().lower()
    if not is_paid_plan(plan_id) or plan_id not in PLAN_LIMITS:
        state = _free_state(row)
        state["expired"] = False
        return state

    period_end = parse_ts(row.get("current_period_end"))
    grace_end = parse_ts(row.get("grace_period_end"))
    cancel_at_period_end = bool(row.get("cancel_at_period_end"))
    provider_backed = str(row.get("provider") or "").lower() == "stripe"
    grace = timedelta(days=GRACE_PERIOD_DAYS)

    deadline: Optional[datetime] = None
    timed = False
    provisional = False
    if status in ENTITLED_STATUSES:
        timed = True
        if period_end is not None:
            deadline = period_end if (cancel_at_period_end or not provider_backed) else period_end + grace
        elif provider_backed:
            # Stripe says the subscription is active but the payload that told
            # us carried no period end (newer API versions keep it on the
            # subscription items / the invoice lines). That is not an expired
            # subscription: stay entitled until the next event supplies the
            # date or ends the subscription. Rows without a provider keep
            # lapsing at their recorded period end (none recorded = lapsed).
            provisional = True
    elif status in GRACE_STATUSES:
        timed = True
        deadline = grace_end or (period_end + grace if period_end is not None else None)

    if timed and (provisional or (deadline is not None and now <= deadline)):
        return {
            "plan_id": plan_id,
            "limits": limits_for(plan_id),
            "status": status,
            "is_paid": True,
            "current_period_end": row.get("current_period_end"),
            "cancel_at_period_end": cancel_at_period_end,
            "grace_period_end": row.get("grace_period_end") if status in GRACE_STATUSES else None,
            "provider": row.get("provider"),
            "provider_sub_id": row.get("provider_sub_id"),
            "provider_status": status,
            "checkout_session_id": row.get("checkout_session_id"),
            "previous_plan_id": row.get("previous_plan_id"),
            "expired": False,
        }

    # Not entitled. ``timed`` rows ran out of time (persist the downgrade);
    # other statuses (canceled, unpaid, incomplete, ...) simply grant nothing.
    state = _free_state(row)
    state["expired"] = timed
    if timed:
        state["previous_plan_id"] = plan_id
    return state


def _persist_expiry(conn, row: Dict[str, Any], now: datetime) -> bool:
    """Downgrade an out-of-time row. Guarded so a concurrent renewal wins."""
    cur = conn.execute(
        """
        UPDATE subscriptions
           SET previous_plan_id = plan_id,
               plan_id = ?,
               status = 'expired',
               cancel_at_period_end = 0,
               grace_period_end = NULL,
               updated_at = ?
         WHERE user_id = ?
           AND plan_id = ?
           AND status = ?
           AND updated_at = ?
        """,
        (
            FREE_PLAN_ID,
            now.isoformat(),
            row["user_id"],
            row["plan_id"],
            row["status"],
            row["updated_at"],
        ),
    )
    return cur.rowcount == 1


def _is_lock_contention(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def _try_persist_expiry(conn, row: Dict[str, Any], now: datetime) -> Optional[bool]:
    """:func:`_persist_expiry` that gives up quickly when the write lock is taken.

    True = downgraded, False = the row changed underneath us, None = skipped
    because another connection holds the write lock.
    """
    previous_timeout: Optional[int] = None
    try:
        current = conn.execute("PRAGMA busy_timeout").fetchone()
        previous_timeout = int(current[0]) if current is not None else None
        conn.execute(f"PRAGMA busy_timeout = {int(PERSIST_BUSY_TIMEOUT_MS)}")
    except (sqlite3.Error, TypeError, ValueError):
        previous_timeout = None
    try:
        return _persist_expiry(conn, row, now)
    except sqlite3.OperationalError as exc:
        if not _is_lock_contention(exc):
            raise
        log.info("subscription expiry not persisted for user=%s (database busy); will retry on a later read",
                 row.get("user_id"))
        return None
    finally:
        if previous_timeout is not None:
            try:
                conn.execute(f"PRAGMA busy_timeout = {previous_timeout}")
            except sqlite3.Error:
                pass


def get_effective_plan(
    conn_or_db,
    user_id: str,
    *,
    now: Optional[datetime] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    """The plan ``user_id`` is entitled to right now, read from the database.

    Returns ``plan_id``, ``limits``, ``status``, ``is_paid`` and the period /
    provider fields of the row. An expired subscription is reported as the free
    plan.

    Side-effect free by default. With ``persist=True`` an expired row is also
    downgraded in place; only ask for that where no *other* connection of the
    same request holds uncommitted writes (SQLite has a single writer). Even
    then the write gives up after :data:`PERSIST_BUSY_TIMEOUT_MS` instead of
    stalling the request: the returned plan is correct whether or not the
    downgrade could be stored.
    """
    now = now or _utc_now()
    with _connection(conn_or_db) as conn:
        row = _fetch_dict(conn, "SELECT * FROM subscriptions WHERE user_id = ?", (str(user_id),))
        state = resolve_subscription(row, now)
        if state.pop("expired", False) and persist and row:
            outcome = _try_persist_expiry(conn, row, now)
            if outcome:
                state["provider_status"] = "expired"
                log.info(
                    "subscription expired: user=%s plan=%s status=%s period_end=%s -> %s",
                    user_id, row.get("plan_id"), row.get("status"),
                    row.get("current_period_end"), FREE_PLAN_ID,
                )
            elif outcome is False:
                # The row changed underneath us (a webhook landed): re-read it.
                row = _fetch_dict(conn, "SELECT * FROM subscriptions WHERE user_id = ?", (str(user_id),))
                state = resolve_subscription(row, now)
                state.pop("expired", None)
        return state


def count_user_bots(conn_or_db, user_id: str) -> int:
    """Bots that occupy a plan slot (everything not deleted / archived)."""
    with _connection(conn_or_db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM bot_instances WHERE user_id = ? AND status NOT IN (?, ?)",
            (str(user_id), *_BOT_FREE_STATUSES),
        ).fetchone()
    return int(row[0] or 0)


def count_user_brokers(conn_or_db, user_id: str) -> int:
    """Connected broker accounts, counted as ``broker_service`` enforces them."""
    with _connection(conn_or_db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM broker_accounts WHERE user_id = ? AND status != 'disconnected'",
            (str(user_id),),
        ).fetchone()
    return int(row[0] or 0)


def bot_quota(conn_or_db, user_id: str, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    """``{"plan_id", "limit", "used", "remaining"}`` for the user's bots."""
    with _connection(conn_or_db) as conn:
        plan = get_effective_plan(conn, user_id, now=now)
        used = count_user_bots(conn, user_id)
    limit = int(plan["limits"].get("max_bots", 0) or 0)
    return {
        "plan_id": plan["plan_id"],
        "limit": limit,
        "used": used,
        "remaining": max(limit - used, 0),
    }


def can_create_bot(conn_or_db, user_id: str, requested: int = 1, *, now: Optional[datetime] = None) -> bool:
    """True when ``requested`` more bots fit within the plan's ``max_bots``."""
    quota = bot_quota(conn_or_db, user_id, now=now)
    return quota["used"] + max(int(requested), 0) <= quota["limit"]


def can_trade_live(conn_or_db, user_id: str, *, now: Optional[datetime] = None) -> bool:
    """True when the user's effective plan includes live trading."""
    plan = get_effective_plan(conn_or_db, user_id, now=now)
    return bool(plan["limits"].get("live_trading", False))


def can_add_broker(conn_or_db, user_id: str, *, now: Optional[datetime] = None) -> bool:
    with _connection(conn_or_db) as conn:
        plan = get_effective_plan(conn, user_id, now=now)
        used = count_user_brokers(conn, user_id)
    return used < int(plan["limits"].get("max_brokers", 0) or 0)
