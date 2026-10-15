"""Account coordination and preemptive emergency control (Section H, Step 3.13).

Four things, each usable alone:

``AccountLeases``
    A durable lease per account, in the database, so it holds between threads, workers and processes. One
    transaction decides who holds an account; a lease ends by release or by time; every acquisition raises the
    account's **fencing token**, which never goes back. Work that held an earlier claim finds out at its next
    check (``verify`` / ``LeaseGuard.check``) and stops *before its next mutation* with ``LeaseLost``. A check
    made inside the mutation's own transaction (``verify_on``) cannot be overtaken at all: SQLite has one
    writer, so nobody can take the lease between that check and the commit.

``EmergencyControl``
    The durable record of an emergency request -- who, when, which scope (all accounts or one), why, how far it
    got -- and the append-only log of what happened to it. While a request is ACTIVE (requested or in progress,
    and still heartbeating) ordinary work for its scope does not start: an ordinary lease is refused, queued
    account tasks are cancelled. The emergency itself takes the account's lease *with priority*: it does not
    wait for an ordinary holder, it takes the lease over, and the ordinary holder yields at its next check.

    **The hold.** A request closes its scope to new entries by switching on the governance kill switch of that
    scope -- the platform switch for "all", the account's own switch for one account -- BEFORE its record is
    written. That switch is the control the execution boundary already consults before every entry, twice. It
    is not lowered by this module, by the flatten or by time: the scope stays closed until an operator lowers
    the switch through the authenticated kill-switch controls. That is what keeps an ordinary cycle from
    reopening a position right after a flatten.

``PreemptionGate``
    The same request inside ONE process, as a thread-safe flag: the production broker cycle asks it between
    accounts and before the entry-capable step of an account, and hands the cycle lock to the flatten.

``BoundedAccountRunner``
    The cycle driver's ``account_runner``: a fixed maximum concurrency, one account never in two workers (in
    this process by grouping, across processes by the lease), one account's exception isolated from the
    others, results in input order, no account started once a stop or a preemption is requested (a running
    account finishes its transaction; nothing is killed mid-transaction), and an optional rate-budget
    admission. See ``docs/implementation/STEP3_COORDINATION_AND_RETENTION.md`` for what parallelism does and
    does not buy on SQLite.

This module never calls an exchange, holds no client and imports nothing that can send an order.
"""
from __future__ import annotations

import contextvars
import json
import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from shared_lib.persistence.cati_operations_schema import ACCOUNT_LEASE_TABLE, EMERGENCY_EVENT_TABLE, EMERGENCY_TABLE

logger = logging.getLogger(__name__)

COORDINATION_VERSION = "1.0.0"

# ── leases ───────────────────────────────────────────────────────────────────────────────────────────────
ORDINARY, EMERGENCY = "ORDINARY", "EMERGENCY"
ORDINARY_PRIORITY, EMERGENCY_PRIORITY = 0, 100
STALE, PREEMPTED = "STALE", "PREEMPTED"
#: How long an ordinary claim on an account lasts unless renewed. One account of a shadow cycle is one
#: transaction of a few milliseconds (measured: see the document above); a minute is three orders of magnitude
#: above that and still recovers a dead worker's account within one production cycle or two.
DEFAULT_LEASE_TTL_MS = 60_000
#: An emergency holds an account for the length of its venue work (reads, a close, its read-back).
EMERGENCY_LEASE_TTL_MS = 180_000

# ── emergency requests ───────────────────────────────────────────────────────────────────────────────────
ALL, ACCOUNT = "ALL", "ACCOUNT"
REQUESTED, IN_PROGRESS, DONE = "REQUESTED", "IN_PROGRESS", "DONE"
ACTIVE_STATES: Tuple[str, ...] = (REQUESTED, IN_PROGRESS)
FLATTEN = "FLATTEN"
COMPLETED, INCOMPLETE = "COMPLETED", "INCOMPLETE"
#: A request that has not heartbeat for this long is no longer ACTIVE: its executor is gone. Its scope stays
#: closed to entries (the kill switch does that), but it stops cancelling that scope's ordinary evaluation and
#: maintenance, which would otherwise be suspended for ever by a process that died. It is reported as STALLED.
EMERGENCY_ACTIVE_TTL_MS = 180_000

# ── runner ───────────────────────────────────────────────────────────────────────────────────────────────
#: The runner's default: one account at a time. Measured on the shadow cycle (1,000 accounts, SQLite): one
#: worker 1.0 s, two 1.2-1.6 s, four 1.5-1.7 s, eight 2.3 s, and the slowest single account went from 16 ms to
#: 2.4 s -- every account is one write transaction and SQLite has one writer, so more workers only queue for
#: it. Raise the number for work that waits on something other than this database (a venue), not before.
SHADOW_CYCLE_CONCURRENCY = 1

# ── runner outcomes ──────────────────────────────────────────────────────────────────────────────────────
RUN_DONE, RUN_FAILED, RUN_NOT_STARTED, RUN_LEASE_LOST = "DONE", "FAILED", "NOT_STARTED", "LEASE_LOST"
STOP_REQUESTED, EMERGENCY_PREEMPTED = "STOP_REQUESTED", "EMERGENCY_PREEMPTED"
LEASE_HELD, EMERGENCY_ACTIVE = "LEASE_HELD", "EMERGENCY_ACTIVE"
RATE_BUDGET_EXHAUSTED, RATE_BUDGET_UNAVAILABLE = "RATE_BUDGET_EXHAUSTED", "RATE_BUDGET_UNAVAILABLE"
ACCOUNT_ID_UNKNOWN = "ACCOUNT_ID_UNKNOWN"
#: What the driver calls an account it has not processed; a task that was not started is reported with it.
DRIVER_PENDING, DRIVER_FAILED = "PENDING", "FAILED"

# ── supervisor faults of the platform driver (thresholds: ``app.ops.runtime_supervisor.Limits``) ─────────
PLATFORM_CYCLE_STALLED = "PLATFORM_CYCLE_STALLED"
PLATFORM_ACCOUNT_STALLED = "PLATFORM_ACCOUNT_STALLED"
ACCOUNT_LEASE_ABANDONED = "ACCOUNT_LEASE_ABANDONED"
EMERGENCY_REQUEST_STALLED = "EMERGENCY_REQUEST_STALLED"


def wall_ms() -> int:
    return int(time.time() * 1000)


def priority_of(purpose: str) -> int:
    if purpose == EMERGENCY:
        return EMERGENCY_PRIORITY
    if purpose == ORDINARY:
        return ORDINARY_PRIORITY
    raise ValueError(f"LEASE_PURPOSE_UNKNOWN:{purpose}")


class LeaseLost(RuntimeError):
    """The caller no longer holds the account. Raised before a mutation, never after one."""

    code = "ACCOUNT_LEASE_LOST"

    def __init__(self, lease: "Lease", reason: str, holder: Optional[str] = None) -> None:
        super().__init__(f"{reason}:{lease.account_id}")
        self.lease, self.reason, self.holder = lease, reason, holder


class EmergencyError(RuntimeError):
    code = "EMERGENCY_REQUEST_INVALID"


@dataclass(frozen=True)
class Lease:
    account_id: str
    owner_id: str
    purpose: str
    fencing_token: int
    acquired_at: int
    expires_at: int
    takeover: Optional[str] = None
    previous_owner: Optional[str] = None
    granted = True


@dataclass(frozen=True)
class LeaseDenied:
    """Why an account could not be claimed. Falsy, so ``if not leases.acquire(...)`` reads as it should."""
    account_id: str
    reason: str
    holder: Optional[str] = None
    holder_purpose: Optional[str] = None
    expires_at: Optional[int] = None
    retry_after_ms: Optional[int] = None
    granted = False

    def __bool__(self) -> bool:
        return False


def _active_emergency_on(conn: Any, account_id: Optional[str], now: int, ttl_ms: int) -> Optional[Dict[str, Any]]:
    """The oldest ACTIVE request whose scope covers ``account_id`` (any ACTIVE request when it is None)."""
    row = conn.execute(
        f"SELECT request_id, scope, account_id, action, state, requested_by, requested_at, heartbeat_at "
        f"FROM {EMERGENCY_TABLE} WHERE state IN (?, ?) AND heartbeat_at > ? AND (scope = ? OR account_id = ? OR ? IS NULL) "
        "ORDER BY requested_at, request_id LIMIT 1",
        (REQUESTED, IN_PROGRESS, now - ttl_ms, ALL, account_id, account_id)).fetchone()
    if row is None:
        return None
    return {"request_id": row[0], "scope": row[1], "account_id": row[2], "action": row[3], "state": row[4],
            "requested_by": row[5], "requested_at": int(row[6]), "heartbeat_at": int(row[7])}


class AccountLeases:
    """The durable per-account lease. Every operation is one ``BEGIN IMMEDIATE`` transaction: the read that
    decides and the write that claims are under the same write lock, in every thread and every process that
    opens this database."""

    def __init__(self, db: Any, *, clock: Optional[Callable[[], int]] = None,
                 emergency_active_ttl_ms: int = EMERGENCY_ACTIVE_TTL_MS) -> None:
        self._db = db
        self._clock = clock or wall_ms
        self._emergency_ttl = int(emergency_active_ttl_ms)

    def _now(self, now_ms: Optional[int]) -> int:
        return int(now_ms if now_ms is not None else self._clock())

    def acquire(self, account_id: str, owner_id: str, *, ttl_ms: int = DEFAULT_LEASE_TTL_MS, purpose: str = ORDINARY,
                now_ms: Optional[int] = None, detail: Optional[Mapping[str, Any]] = None) -> Union[Lease, LeaseDenied]:
        """Claim ``account_id`` for ``owner_id``.

        Granted when the account is free, when its holder's time ran out (takeover ``STALE``) or -- for an
        EMERGENCY claim only -- when it is held by ordinary work (takeover ``PREEMPTED``). Refused while
        somebody of the same or a higher priority holds it, and, for ordinary work, while an emergency request
        is ACTIVE for the account. Not re-entrant: the same owner asking twice is refused the second time, so
        two workers that share an owner id still cannot both hold one account."""
        if not account_id or not owner_id:
            raise ValueError("LEASE_ACCOUNT_AND_OWNER_REQUIRED")
        if int(ttl_ms) <= 0:
            raise ValueError("LEASE_TTL_INVALID")
        priority = priority_of(purpose)
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now(now_ms)                      # read under the write lock: it is the deciding instant
            if purpose != EMERGENCY:
                emergency = _active_emergency_on(conn, account_id, now, self._emergency_ttl)
                if emergency is not None:
                    return LeaseDenied(account_id, EMERGENCY_ACTIVE, holder=f"emergency:{emergency['request_id']}",
                                       holder_purpose=EMERGENCY)
            row = conn.execute(
                f"SELECT owner_id, purpose, priority, fencing_token, expires_at, released_at FROM {ACCOUNT_LEASE_TABLE} "
                "WHERE account_id=?", (account_id,)).fetchone()
            takeover = previous = None
            if row is not None and row[5] is None:
                if int(row[4]) > now:
                    if priority <= int(row[2]):
                        return LeaseDenied(account_id, LEASE_HELD, holder=row[0], holder_purpose=row[1],
                                           expires_at=int(row[4]), retry_after_ms=int(row[4]) - now)
                    takeover = PREEMPTED
                else:
                    takeover = STALE
                previous = row[0]
            token = (int(row[3]) if row is not None else 0) + 1
            expires = now + int(ttl_ms)
            conn.execute(
                f"INSERT INTO {ACCOUNT_LEASE_TABLE} (account_id, owner_id, purpose, priority, fencing_token, acquired_at, "
                "renewed_at, expires_at, released_at, release_reason, previous_owner, takeover, detail) "
                "VALUES (?,?,?,?,?,?,?,?,NULL,NULL,?,?,?) ON CONFLICT(account_id) DO UPDATE SET "
                "owner_id=excluded.owner_id, purpose=excluded.purpose, priority=excluded.priority, "
                "fencing_token=excluded.fencing_token, acquired_at=excluded.acquired_at, renewed_at=excluded.renewed_at, "
                "expires_at=excluded.expires_at, released_at=NULL, release_reason=NULL, "
                "previous_owner=excluded.previous_owner, takeover=excluded.takeover, detail=excluded.detail",
                (account_id, owner_id, purpose, priority, token, now, now, expires, previous, takeover,
                 json.dumps(dict(detail or {}), sort_keys=True, default=str)))
            return Lease(account_id=account_id, owner_id=owner_id, purpose=purpose, fencing_token=token,
                         acquired_at=now, expires_at=expires, takeover=takeover, previous_owner=previous)

    @staticmethod
    def verify_on(conn: Any, lease: Lease, now_ms: int) -> None:
        """Raise ``LeaseLost`` unless ``lease`` is the account's present, unexpired claim. Called on the
        connection of the caller's own write transaction it is a fencing check nothing can overtake."""
        row = conn.execute(
            f"SELECT owner_id, fencing_token, expires_at, released_at FROM {ACCOUNT_LEASE_TABLE} WHERE account_id=?",
            (lease.account_id,)).fetchone()
        if row is None:
            raise LeaseLost(lease, "LEASE_ROW_MISSING")
        if row[0] != lease.owner_id or int(row[1]) != lease.fencing_token:
            raise LeaseLost(lease, "LEASE_TAKEN_OVER", holder=row[0])
        if row[3] is not None:
            raise LeaseLost(lease, "LEASE_RELEASED")
        if int(row[2]) <= int(now_ms):
            raise LeaseLost(lease, "LEASE_EXPIRED")

    def verify(self, lease: Lease, *, now_ms: Optional[int] = None) -> None:
        with self._db.connect() as conn:
            self.verify_on(conn, lease, self._now(now_ms))

    def renew(self, lease: Lease, *, ttl_ms: int = DEFAULT_LEASE_TTL_MS, now_ms: Optional[int] = None) -> Lease:
        """Extend a claim that is still the caller's. A claim whose time has run out is NOT revived, even when
        nobody took it: from that instant others were entitled to, and the holder cannot know they did not."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now(now_ms)
            self.verify_on(conn, lease, now)
            expires = now + int(ttl_ms)
            conn.execute(f"UPDATE {ACCOUNT_LEASE_TABLE} SET renewed_at=?, expires_at=? WHERE account_id=?",
                         (now, expires, lease.account_id))
            return replace(lease, expires_at=expires)

    def release(self, lease: Lease, *, reason: str = "DONE", now_ms: Optional[int] = None) -> bool:
        """Give the account back. False when the claim was no longer the caller's (taken over, or released):
        nothing is changed then, so a late release can never free somebody else's lease."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now(now_ms)
            cursor = conn.execute(
                f"UPDATE {ACCOUNT_LEASE_TABLE} SET released_at=?, release_reason=? WHERE account_id=? AND owner_id=? "
                "AND fencing_token=? AND released_at IS NULL", (now, reason, lease.account_id, lease.owner_id, lease.fencing_token))
            return cursor.rowcount == 1

    def holder(self, account_id: str, *, now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Who holds the account now, or None."""
        now = self._now(now_ms)
        with self._db.connect() as conn:
            row = conn.execute(
                f"SELECT owner_id, purpose, fencing_token, acquired_at, expires_at, takeover, previous_owner "
                f"FROM {ACCOUNT_LEASE_TABLE} WHERE account_id=? AND released_at IS NULL AND expires_at > ?",
                (account_id, now)).fetchone()
        if row is None:
            return None
        return {"account_id": account_id, "owner_id": row[0], "purpose": row[1], "fencing_token": int(row[2]),
                "acquired_at": int(row[3]), "expires_at": int(row[4]), "takeover": row[5], "previous_owner": row[6]}

    def fencing_token(self, account_id: str) -> int:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT fencing_token FROM {ACCOUNT_LEASE_TABLE} WHERE account_id=?", (account_id,)).fetchone()
        return int(row[0]) if row else 0

    def guard(self, lease: Lease) -> "LeaseGuard":
        return LeaseGuard(self, lease)


class LeaseGuard:
    """What multi-step work carries: ``check()`` before every mutation, ``check_on(conn)`` inside one."""

    def __init__(self, leases: AccountLeases, lease: Lease) -> None:
        self._leases = leases
        self.lease = lease

    def check(self, *, now_ms: Optional[int] = None) -> None:
        self._leases.verify(self.lease, now_ms=now_ms)

    def check_on(self, conn: Any, *, now_ms: Optional[int] = None) -> None:
        AccountLeases.verify_on(conn, self.lease, self._leases._now(now_ms))

    def renew(self, *, ttl_ms: int = DEFAULT_LEASE_TTL_MS, now_ms: Optional[int] = None) -> Lease:
        self.lease = self._leases.renew(self.lease, ttl_ms=ttl_ms, now_ms=now_ms)
        return self.lease


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
class EmergencyControl:
    """The durable emergency request and its hold. See the module documentation."""

    def __init__(self, db: Any, *, clock: Optional[Callable[[], int]] = None,
                 active_ttl_ms: int = EMERGENCY_ACTIVE_TTL_MS) -> None:
        self._db = db
        self._clock = clock or wall_ms
        self._ttl = int(active_ttl_ms)

    def _now(self, now_ms: Optional[int]) -> int:
        return int(now_ms if now_ms is not None else self._clock())

    # ------------------------------------------------------------------------------------------ the hold
    def _governance(self) -> Any:
        from app.trading_intelligence.governance.promotion import PromotionGovernance

        return PromotionGovernance(self._db)

    @staticmethod
    def hold_scope_of(scope: str, account_id: Optional[str]) -> str:
        from app.trading_intelligence.governance.promotion import GLOBAL_SCOPE

        return GLOBAL_SCOPE if scope == ALL else str(account_id)

    def _raise_hold(self, hold_scope: str, request_id: str, reason: str, actor_ref: str) -> bool:
        """Switch the scope's new-entry kill switch on. True when this call did it, False when it was on."""
        governance = self._governance()
        if governance.kill_switch_status(hold_scope)["enabled"]:
            return False
        try:
            governance.set_kill_switch(True, reason=f"EMERGENCY_{FLATTEN} {request_id}: {reason}", actor_ref=actor_ref,
                                       scope=hold_scope)
        except Exception:  # noqa: BLE001 -- e.g. an identical record in the same millisecond: the state decides
            logger.exception("[EMERGENCY] the hold record of %s could not be written", hold_scope)
        if not governance.kill_switch_status(hold_scope)["enabled"]:
            raise EmergencyError("KILL_SWITCH_NOT_PERSISTED")
        return True

    def hold(self, scope: str, account_id: Optional[str] = None) -> Dict[str, Any]:
        """The kill switch that keeps ``scope`` closed to entries, as it is now."""
        return dict(self._governance().kill_switch_status(self.hold_scope_of(scope, account_id)))

    # -------------------------------------------------------------------------------------------- record
    def request(self, *, scope: str, account_id: Optional[str] = None, requested_by: str, reason: str,
                request_id: Optional[str] = None, action: str = FLATTEN, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Record an emergency request. The scope's kill switch is switched on FIRST; then the record is
        written. Idempotent on ``request_id``. From this instant, until the request is DONE (or its executor
        stops heartbeating), ordinary work for the scope is not started anywhere this database is open."""
        from app.trading_intelligence.governance.promotion import GLOBAL_SCOPE

        scope = str(scope or "").upper()
        if scope not in (ALL, ACCOUNT) or (scope == ACCOUNT) != bool(account_id) or account_id == GLOBAL_SCOPE:
            raise EmergencyError("EMERGENCY_SCOPE_INVALID")
        if not str(requested_by or "").strip() or len(str(reason or "").strip()) < 3:
            raise EmergencyError("EMERGENCY_ACTOR_AND_REASON_REQUIRED")
        request_id = request_id or uuid.uuid4().hex
        existing = self.get(request_id)
        if existing is not None:
            if (existing["scope"], existing["account_id"]) != (scope, account_id):
                raise EmergencyError("EMERGENCY_REQUEST_ID_REUSED_FOR_ANOTHER_SCOPE")
            return existing
        hold_scope = self.hold_scope_of(scope, account_id)
        raised, hold_error = False, None
        try:
            raised = self._raise_hold(hold_scope, request_id, str(reason).strip(), str(requested_by))
        except Exception as exc:  # noqa: BLE001 -- the record below still stops ordinary work for the scope
            hold_error = type(exc).__name__ if not isinstance(exc, EmergencyError) else str(exc)
            logger.error("[EMERGENCY] request %s: the kill switch of %s is NOT on (%s)", request_id, hold_scope, hold_error)
        detail = {"hold_error": hold_error} if hold_error else {}
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now(now_ms)
            inserted = conn.execute(
                f"INSERT OR IGNORE INTO {EMERGENCY_TABLE} (request_id, scope, account_id, action, requested_by, reason, "
                "state, requested_at, heartbeat_at, hold_scope, hold_raised, detail) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (request_id, scope, account_id, action, str(requested_by), str(reason).strip(), REQUESTED, now, now,
                 hold_scope, 1 if raised else 0, json.dumps(detail, sort_keys=True))).rowcount
            if inserted:
                self._event(conn, request_id, REQUESTED, str(requested_by), now,
                            {"scope": scope, "account_id": account_id, "reason": str(reason).strip(), "action": action,
                             "hold_scope": hold_scope, "hold_raised": raised, **detail})
        return self.get(request_id)  # type: ignore[return-value]

    def begin(self, request_id: str, *, account_id: Optional[str] = None, actor_ref: str = "engine",
              reason: str = "engine emergency flatten", now_ms: Optional[int] = None) -> Dict[str, Any]:
        """The executor has taken the request up. A request nobody recorded (a direct engine call) is recorded
        here, with the same hold, so no flatten runs without one."""
        if self.get(request_id) is None:
            self.request(scope=ACCOUNT if account_id else ALL, account_id=account_id, requested_by=actor_ref,
                         reason=reason, request_id=request_id, now_ms=now_ms)
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now(now_ms)
            conn.execute(f"UPDATE {EMERGENCY_TABLE} SET state=?, started_at=COALESCE(started_at, ?), heartbeat_at=?, "
                         "finished_at=NULL, outcome=NULL WHERE request_id=?", (IN_PROGRESS, now, now, request_id))
            self._event(conn, request_id, "STARTED", actor_ref, now, {})
        return self.get(request_id)  # type: ignore[return-value]

    def heartbeat(self, request_id: str, *, now_ms: Optional[int] = None) -> bool:
        with self._db.connect() as conn:
            return conn.execute(f"UPDATE {EMERGENCY_TABLE} SET heartbeat_at=? WHERE request_id=? AND state IN (?, ?)",
                                (self._now(now_ms), request_id, REQUESTED, IN_PROGRESS)).rowcount == 1

    def finish(self, request_id: str, *, outcome: str, detail: Optional[Mapping[str, Any]] = None,
               actor_ref: str = "engine", now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """The request has run its course (whatever the outcome): it stops preempting. Its hold stays."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now(now_ms)
            row = conn.execute(f"SELECT detail FROM {EMERGENCY_TABLE} WHERE request_id=?", (request_id,)).fetchone()
            if row is None:
                return None
            merged = {**(json.loads(row[0]) if row[0] else {}), **dict(detail or {})}
            conn.execute(f"UPDATE {EMERGENCY_TABLE} SET state=?, outcome=?, finished_at=?, heartbeat_at=?, detail=? "
                         "WHERE request_id=?", (DONE, str(outcome), now, now, json.dumps(merged, sort_keys=True, default=str),
                                                request_id))
            self._event(conn, request_id, "FINISHED", actor_ref, now, {"outcome": str(outcome), **dict(detail or {})})
        return self.get(request_id)

    @staticmethod
    def _event(conn: Any, request_id: str, event: str, actor_ref: str, now: int, detail: Mapping[str, Any]) -> None:
        conn.execute(f"INSERT INTO {EMERGENCY_EVENT_TABLE} (request_id, event, actor_ref, recorded_at, detail) "
                     "VALUES (?,?,?,?,?)", (request_id, event, actor_ref, now, json.dumps(dict(detail), sort_keys=True, default=str)))

    # --------------------------------------------------------------------------------------------- reads
    _COLUMNS = ("request_id", "scope", "account_id", "action", "requested_by", "reason", "state", "outcome",
                "requested_at", "started_at", "heartbeat_at", "finished_at", "hold_scope", "hold_raised", "detail")

    def _rows(self, where: str, args: Sequence[Any], now: int) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT {', '.join(self._COLUMNS)} FROM {EMERGENCY_TABLE} WHERE {where}", tuple(args)).fetchall()
        out = []
        for row in rows:
            item = dict(zip(self._COLUMNS, row))
            item["detail"] = json.loads(item["detail"]) if item["detail"] else {}
            item["hold_raised"] = bool(item["hold_raised"])
            pending = item["state"] in ACTIVE_STATES
            item["active"] = pending and int(item["heartbeat_at"]) > now - self._ttl
            item["stalled"] = pending and not item["active"]
            out.append(item)
        return out

    def get(self, request_id: str, *, now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
        rows = self._rows("request_id=?", (request_id,), self._now(now_ms))
        return rows[0] if rows else None

    def active(self, *, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        """Requests that preempt ordinary work right now, oldest first."""
        now = self._now(now_ms)
        return self._rows("state IN (?, ?) AND heartbeat_at > ? ORDER BY requested_at, request_id",
                          (REQUESTED, IN_PROGRESS, now - self._ttl), now)

    def stalled(self, *, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        now = self._now(now_ms)
        return self._rows("state IN (?, ?) AND heartbeat_at <= ? ORDER BY requested_at, request_id",
                          (REQUESTED, IN_PROGRESS, now - self._ttl), now)

    def active_for(self, account_id: Optional[str], *, now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """The ACTIVE request whose scope covers ``account_id``, or None. Ordinary work asks this between
        accounts and before every entry-capable step."""
        with self._db.connect() as conn:
            return _active_emergency_on(conn, account_id, self._now(now_ms), self._ttl)

    def entries_blocked(self, account_id: str, *, now_ms: Optional[int] = None) -> Optional[str]:
        """Why no entry may be submitted for the account, or None. An emergency in flight blocks; so does the
        hold it leaves behind (the kill switch); so does a kill switch that cannot be read."""
        if self.active_for(account_id, now_ms=now_ms) is not None:
            return "EMERGENCY_FLATTEN_ACTIVE"
        try:
            held = self._governance().kill_switch_on(scope=account_id)
        except Exception:  # noqa: BLE001 -- an unreadable control is never read as "off"
            return "KILL_SWITCH_STATE_UNAVAILABLE"
        return "CATI_NEW_ENTRY_KILL_SWITCH" if held else None

    def events(self, request_id: str) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT seq, event, actor_ref, recorded_at, detail FROM {EMERGENCY_EVENT_TABLE} "
                                "WHERE request_id=? ORDER BY seq", (request_id,)).fetchall()
        return [{"seq": int(r[0]), "event": r[1], "actor_ref": r[2], "recorded_at": int(r[3]),
                 "detail": json.loads(r[4]) if r[4] else {}} for r in rows]

    def status(self, *, limit: int = 20, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """For operators: the latest requests, which of them still preempt, and whether each one's scope is
        still closed to entries (read from the kill switch, the one authority for that)."""
        now = self._now(now_ms)
        rows = self._rows("1=1 ORDER BY requested_at DESC, request_id LIMIT ?", (int(limit),), now)
        governance = self._governance()
        for item in rows:
            try:
                switch = governance.kill_switch_status(item["hold_scope"])
                item["hold"] = {"scope": item["hold_scope"], "enabled": bool(switch["enabled"]), "set_by": switch["set_by"],
                                "set_at_ms": switch["set_at_ms"], "reason": switch["reason"]}
            except Exception as exc:  # noqa: BLE001
                item["hold"] = {"scope": item["hold_scope"], "enabled": None, "error": type(exc).__name__}
        return {"generated_at_ms": now, "active": sum(r["active"] for r in rows), "stalled": sum(r["stalled"] for r in rows),
                "requests": rows}


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
class PreemptionGate:
    """An emergency request as the threads of ONE process see it. No I/O, never blocks the asker."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._tickets: Dict[int, Tuple[str, Optional[str]]] = {}
        self._serial = 0

    def request(self, request_id: str, account_id: Optional[str] = None) -> int:
        """Ask ordinary work to yield: every account when ``account_id`` is None, else that account."""
        with self._condition:
            self._serial += 1
            self._tickets[self._serial] = (str(request_id), account_id)
            return self._serial

    def release(self, ticket: int) -> None:
        with self._condition:
            self._tickets.pop(ticket, None)
            self._condition.notify_all()

    def pending(self) -> bool:
        with self._condition:
            return bool(self._tickets)

    def covers(self, account_id: Optional[str]) -> bool:
        with self._condition:
            return any(scope is None or scope == account_id for _, scope in self._tickets.values())

    def requests(self) -> List[Tuple[str, Optional[str]]]:
        with self._condition:
            return list(self._tickets.values())

    def wait_clear(self, timeout: Optional[float] = None) -> bool:
        """Block until no request is pending. True when clear, False on timeout."""
        with self._condition:
            return self._condition.wait_for(lambda: not self._tickets, timeout)


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def account_id_of_call(call: Any) -> Optional[str]:
    """The account a per-account callable works on: its ``account_id`` attribute when it has one, else the
    ``account_id`` of the account object the callable closes over (the cycle driver's callables do)."""
    explicit = getattr(call, "account_id", None)
    if isinstance(explicit, str) and explicit:
        return explicit
    for cell in getattr(call, "__closure__", None) or ():
        try:
            value = cell.cell_contents
        except ValueError:  # an empty cell
            continue
        candidate = getattr(value, "account_id", None)
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


@dataclass(frozen=True)
class AccountTask:
    """One account's unit of work. ``wants_guard`` work is called with a ``LeaseGuard`` and is expected to
    call ``guard.check()`` before every mutation after its first."""
    account_id: Optional[str]
    call: Callable[..., Any]
    wants_guard: bool = False


@dataclass
class TaskOutcome:
    index: int
    account_id: Optional[str]
    status: str
    value: Any = None
    reason: Optional[str] = None
    error: Optional[str] = None
    message: Optional[str] = None
    retry_after: Optional[float] = None
    fencing_token: Optional[int] = None
    lease_lost: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class _Run:
    queue: "deque[Tuple[Any, List[int]]]"
    outcomes: List[Optional[TaskOutcome]]
    lock: threading.Lock = field(default_factory=threading.Lock)
    in_flight: int = 0
    max_in_flight: int = 0
    fatal: Optional[BaseException] = None


class BoundedAccountRunner:
    """Runs per-account work with a fixed maximum concurrency. Callable as the cycle driver's
    ``account_runner``; ``run`` is the same thing with explicit tasks and full outcomes.

    ``leases``      every account is worked under its durable lease (omit: no cross-process exclusion)
    ``emergency``   the durable emergency control: an ACTIVE request for an account cancels its queued task
    ``gate``        the same, in-process (no database read)
    ``stop``        a callable; once it returns True no further account is started
    ``admit``       the rate budget: ``admit(account_id)`` returns True to proceed, False to refuse, or a
                    number of seconds after which to retry (refused). A budget that raises refuses.
    ``db``          the database handle the work itself uses. Each worker then keeps ONE connection for its
                    whole share of the run (``DB.scope``): every ``connect()`` block of the work and of the
                    lease stays its own atomic unit, only the open / pragmas / close are paid once per worker
                    instead of once per block (measured: the larger part of an account's cost on Windows).
    """

    def __init__(self, *, max_concurrency: int = SHADOW_CYCLE_CONCURRENCY, db: Any = None,
                 leases: Optional[AccountLeases] = None,
                 emergency: Optional[EmergencyControl] = None, gate: Optional[PreemptionGate] = None,
                 stop: Optional[Callable[[], bool]] = None, admit: Optional[Callable[[str], Any]] = None,
                 owner: Optional[str] = None, lease_ttl_ms: int = DEFAULT_LEASE_TTL_MS,
                 clock: Optional[Callable[[], int]] = None,
                 account_id_of: Callable[[Any], Optional[str]] = account_id_of_call,
                 require_account_id: Optional[bool] = None) -> None:
        if int(max_concurrency) < 1:
            raise ValueError("MAX_CONCURRENCY_INVALID")
        self.max_concurrency = int(max_concurrency)
        self._db = db
        self._leases, self._emergency, self._gate = leases, emergency, gate
        self._stop, self._admit = stop, admit
        self._owner = owner or f"runner-{uuid.uuid4().hex[:12]}"
        self._lease_ttl = int(lease_ttl_ms)
        self._clock = clock or wall_ms
        self._account_id_of = account_id_of
        #: work whose account is unknown cannot be leased or checked against an emergency: by default it is
        #: refused whenever either of those is configured, never run unprotected
        self._require_id = (leases is not None or emergency is not None or gate is not None) \
            if require_account_id is None else bool(require_account_id)
        self._stop_reason: Optional[str] = None
        self._state = threading.Lock()
        self._serial = 0
        self._current: Optional[_Run] = None
        self.stats: Dict[str, Any] = {}

    # ---------------------------------------------------------------------------------------- controls
    def request_stop(self, reason: str = STOP_REQUESTED) -> None:
        """No account is started after this. Accounts in flight finish their transaction."""
        self._stop_reason = reason

    def resume(self) -> None:
        self._stop_reason = None

    def queue_depth(self) -> int:
        """Accounts of the run in flight that no worker has taken yet (0 when no run is in flight)."""
        run = self._current
        if run is None:
            return 0
        with run.lock:
            return sum(len(indices) for _, indices in run.queue)

    # ----------------------------------------------------------------------------------- driver's seam
    def __call__(self, calls: Sequence[Callable[[], Any]]) -> List[Any]:
        """The cycle driver's ``account_runner``. A completed call returns what it returned. A call that was
        not started returns ``(account_id, {"status": "PENDING", "not_started": True, "reason": ...})``: the
        driver leaves that account PENDING, ends the cycle PARTIAL and retries it."""
        outcomes = self.run([AccountTask(self._account_id_of(call), call) for call in calls])
        results: List[Any] = []
        for outcome in outcomes:
            if outcome.status == RUN_DONE:
                value = outcome.value
                if outcome.lease_lost and isinstance(value, tuple) and len(value) == 2 and isinstance(value[1], dict):
                    value[1]["lease_lost"] = outcome.lease_lost
                results.append(value)
            elif outcome.status == RUN_NOT_STARTED:
                results.append((outcome.account_id, {"status": DRIVER_PENDING, "not_started": True, "reason": outcome.reason,
                                                     "retry_after": outcome.retry_after}))
            else:
                results.append((outcome.account_id, {"status": DRIVER_FAILED, "error": outcome.error or outcome.status,
                                                     "reason": outcome.reason}))
        return results

    # --------------------------------------------------------------------------------------------- run
    def run(self, tasks: Sequence[AccountTask]) -> List[TaskOutcome]:
        """Run ``tasks``; outcomes are in input order. Tasks of the same account never overlap: they are one
        unit of work, run in order by one worker."""
        started = time.perf_counter()
        groups: Dict[Any, List[int]] = {}
        for index, task in enumerate(tasks):
            groups.setdefault(task.account_id if task.account_id is not None else ("anonymous", index), []).append(index)
        run = _Run(queue=deque(groups.items()), outcomes=[None] * len(tasks))
        workers = min(self.max_concurrency, len(groups))
        self._current = run
        try:
            if workers <= 1:
                self._drain(run, tasks)
            else:
                # each worker gets its own copy of the caller's context, so context-local permits and
                # request state follow the work onto the pool instead of silently staying behind
                threads = [threading.Thread(target=contextvars.copy_context().run, args=(self._drain, run, tasks),
                                            name=f"account-runner-{n}", daemon=True) for n in range(workers)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()                        # a running account is never abandoned or killed
        finally:
            self._current = None
        outcomes = [o for o in run.outcomes if o is not None]
        not_started: Dict[str, int] = {}
        for outcome in outcomes:
            if outcome.status == RUN_NOT_STARTED:
                not_started[outcome.reason or "UNKNOWN"] = not_started.get(outcome.reason or "UNKNOWN", 0) + 1
        self.stats = {"tasks": len(tasks), "accounts": len(groups), "workers": workers, "max_concurrency": self.max_concurrency,
                      "max_in_flight": run.max_in_flight, "max_queue_depth": len(tasks),
                      "done": sum(o.status == RUN_DONE for o in outcomes), "failed": sum(o.status == RUN_FAILED for o in outcomes),
                      "lease_lost": sum(o.status == RUN_LEASE_LOST or bool(o.lease_lost) for o in outcomes),
                      "not_started": not_started, "wall_ms": round((time.perf_counter() - started) * 1000.0, 3),
                      "work_ms": round(sum(o.duration_ms for o in outcomes), 3)}
        if run.fatal is not None:
            raise run.fatal
        return outcomes

    def _drain(self, run: _Run, tasks: Sequence[AccountTask]) -> None:
        scope = getattr(self._db, "scope", None)
        if callable(scope):
            with scope():
                self._drain_queue(run, tasks)
        else:
            self._drain_queue(run, tasks)

    def _drain_queue(self, run: _Run, tasks: Sequence[AccountTask]) -> None:
        while True:
            with run.lock:
                if not run.queue or run.fatal is not None:
                    return
                _, indices = run.queue.popleft()
                run.in_flight += 1
                run.max_in_flight = max(run.max_in_flight, run.in_flight)
            try:
                for index in indices:
                    run.outcomes[index] = self._one(index, tasks[index])
            except BaseException as exc:  # noqa: BLE001 -- KeyboardInterrupt / SystemExit: stop taking work, re-raise in run()
                with run.lock:
                    run.fatal = run.fatal or exc
                return
            finally:
                with run.lock:
                    run.in_flight -= 1

    def _halt_reason(self, account_id: Optional[str]) -> Optional[str]:
        """Why this account must not be started now. Asked immediately before every start."""
        if self._stop_reason is not None:
            return self._stop_reason
        if self._stop is not None and self._stop():
            return STOP_REQUESTED
        if self._gate is not None and (self._gate.covers(account_id) if account_id is not None else self._gate.pending()):
            return EMERGENCY_PREEMPTED
        # with a lease the claim itself refuses, in its own transaction, while an emergency is ACTIVE
        if self._emergency is not None and (self._leases is None or account_id is None)                 and self._emergency.active_for(account_id) is not None:
            return EMERGENCY_PREEMPTED
        return None

    def _one(self, index: int, task: AccountTask) -> TaskOutcome:
        account_id = task.account_id
        not_started = lambda reason, **kw: TaskOutcome(index, account_id, RUN_NOT_STARTED, reason=reason, **kw)  # noqa: E731
        try:
            reason = self._halt_reason(account_id)
        except Exception as exc:  # noqa: BLE001 -- an emergency state that cannot be read starts nothing
            return not_started("EMERGENCY_STATE_UNAVAILABLE", error=type(exc).__name__)
        if reason is not None:
            return not_started(reason)
        if account_id is None and self._require_id:
            return not_started(ACCOUNT_ID_UNKNOWN)
        if self._admit is not None:
            try:
                admitted = self._admit(account_id)
            except Exception as exc:  # noqa: BLE001 -- a budget that cannot answer admits nothing
                return not_started(RATE_BUDGET_UNAVAILABLE, error=type(exc).__name__)
            if admitted is not True:
                wait = float(admitted) if isinstance(admitted, (int, float)) and not isinstance(admitted, bool) else None
                return not_started(RATE_BUDGET_EXHAUSTED, retry_after=wait)
        lease: Optional[Lease] = None
        if self._leases is not None and account_id is not None:
            with self._state:
                self._serial += 1
                owner = f"{self._owner}#{self._serial}"
            try:
                claim = self._leases.acquire(account_id, owner, ttl_ms=self._lease_ttl)
            except Exception as exc:  # noqa: BLE001 -- no lease, no work
                return not_started("LEASE_UNAVAILABLE", error=type(exc).__name__)
            if not claim:
                return not_started(EMERGENCY_PREEMPTED if claim.reason == EMERGENCY_ACTIVE else claim.reason,
                                   retry_after=None if claim.retry_after_ms is None else claim.retry_after_ms / 1000.0)
            lease = claim  # type: ignore[assignment]
        outcome = TaskOutcome(index, account_id, RUN_DONE, fencing_token=lease.fencing_token if lease else None)
        started = time.perf_counter()
        try:
            outcome.value = task.call(self._leases.guard(lease)) if task.wants_guard and lease is not None else task.call()
        except LeaseLost as exc:
            outcome.status, outcome.reason, outcome.lease_lost = RUN_LEASE_LOST, exc.reason, exc.reason
        except Exception as exc:  # noqa: BLE001 -- isolation: this account failed, the others go on
            outcome.status, outcome.error, outcome.message = RUN_FAILED, type(exc).__name__, str(exc)[:300]
        finally:
            outcome.duration_ms = round((time.perf_counter() - started) * 1000.0, 3)
            if lease is not None:
                try:
                    expired = self._clock() >= lease.expires_at
                    if not self._leases.release(lease, reason=outcome.status):  # type: ignore[union-attr]
                        outcome.lease_lost = outcome.lease_lost or "LEASE_TAKEN_OVER"
                    elif expired:
                        outcome.lease_lost = outcome.lease_lost or "LEASE_EXPIRED"
                except Exception as exc:  # noqa: BLE001 -- the lease then ends by time
                    logger.warning("[COORDINATION] lease of %s could not be released (%s)", account_id, type(exc).__name__)
        return outcome


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def platform_observation(db: Any, *, now_ms: Optional[int] = None,
                         emergency_active_ttl_ms: int = EMERGENCY_ACTIVE_TTL_MS) -> Dict[str, Any]:
    """What the runtime supervisor needs to know about the platform driver, read from the database only: how
    long the oldest unfinished cycle has gone without a heartbeat, the slowest account of recent cycles, leases
    nobody released, and emergency requests that never finished. Ages are seconds."""
    from shared_lib.persistence.cati_platform_schema import CYCLE_ACCOUNT_TABLE, CYCLE_TABLE

    now = int(now_ms if now_ms is not None else wall_ms())
    age = lambda stamp: None if stamp is None else round((now - int(stamp)) / 1000.0, 1)  # noqa: E731
    with db.connect() as conn:
        running = conn.execute(f"SELECT COUNT(*), MIN(heartbeat_at) FROM {CYCLE_TABLE} WHERE status IN ('STARTED', 'MARKET_DECIDED')").fetchone()
        completed = conn.execute(f"SELECT MAX(completed_at) FROM {CYCLE_TABLE} WHERE status='COMPLETED'").fetchone()
        slowest = conn.execute(f"SELECT MAX(duration_ms) FROM {CYCLE_ACCOUNT_TABLE}").fetchone()
        leases = conn.execute(f"SELECT COUNT(*), MIN(expires_at) FROM {ACCOUNT_LEASE_TABLE} WHERE released_at IS NULL AND expires_at <= ?",
                              (now,)).fetchone()
        held = conn.execute(f"SELECT COUNT(*) FROM {ACCOUNT_LEASE_TABLE} WHERE released_at IS NULL AND expires_at > ?", (now,)).fetchone()
        pending = conn.execute(f"SELECT COUNT(*), MIN(requested_at), MIN(heartbeat_at) FROM {EMERGENCY_TABLE} WHERE state IN (?, ?)",
                               (REQUESTED, IN_PROGRESS)).fetchone()
    return {"cycles": {"running": int(running[0]), "oldest_running_heartbeat_age": age(running[1]),
                       "last_completed_age": age(completed[0])},
            "accounts": {"slowest_ms": None if slowest[0] is None else float(slowest[0])},
            "leases": {"held": int(held[0]), "abandoned": int(leases[0]), "oldest_abandoned_age": age(leases[1])},
            "emergency": {"pending": int(pending[0]), "oldest_pending_age": age(pending[1]),
                          "oldest_heartbeat_age": age(pending[2]),
                          "active_ttl_seconds": emergency_active_ttl_ms / 1000.0}}


def platform_faults(observation: Mapping[str, Any], limits: Any) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Classify ``platform_observation`` against the supervisor's platform thresholds. Pure. Returns
    ``(faults, warnings)``; what is unknown is never a fault."""
    faults: List[str] = []
    warnings: List[str] = []
    cycles, accounts = observation.get("cycles") or {}, observation.get("accounts") or {}
    leases, emergency = observation.get("leases") or {}, observation.get("emergency") or {}
    heartbeat = cycles.get("oldest_running_heartbeat_age")
    if cycles.get("running") and heartbeat is not None and heartbeat > limits.platform_cycle_stall_seconds:
        faults.append(PLATFORM_CYCLE_STALLED)
    slowest = accounts.get("slowest_ms")
    if slowest is not None and slowest / 1000.0 > limits.platform_account_stall_seconds:
        warnings.append(PLATFORM_ACCOUNT_STALLED)
    abandoned = leases.get("oldest_abandoned_age")
    if leases.get("abandoned") and abandoned is not None and abandoned > limits.account_lease_abandoned_seconds:
        warnings.append(ACCOUNT_LEASE_ABANDONED)
    oldest = emergency.get("oldest_pending_age")
    if emergency.get("pending") and oldest is not None and oldest > limits.emergency_stall_seconds:
        faults.append(EMERGENCY_REQUEST_STALLED)
    return tuple(faults), tuple(warnings)


__all__ = ["COORDINATION_VERSION", "ORDINARY", "EMERGENCY", "STALE", "PREEMPTED", "DEFAULT_LEASE_TTL_MS",
           "EMERGENCY_LEASE_TTL_MS", "ALL", "ACCOUNT", "REQUESTED", "IN_PROGRESS", "DONE", "ACTIVE_STATES", "FLATTEN",
           "COMPLETED", "INCOMPLETE", "EMERGENCY_ACTIVE_TTL_MS", "SHADOW_CYCLE_CONCURRENCY", "RUN_DONE", "RUN_FAILED", "RUN_NOT_STARTED",
           "RUN_LEASE_LOST", "STOP_REQUESTED", "EMERGENCY_PREEMPTED", "LEASE_HELD", "EMERGENCY_ACTIVE",
           "RATE_BUDGET_EXHAUSTED", "RATE_BUDGET_UNAVAILABLE", "ACCOUNT_ID_UNKNOWN", "DRIVER_PENDING", "DRIVER_FAILED",
           "PLATFORM_CYCLE_STALLED", "PLATFORM_ACCOUNT_STALLED", "ACCOUNT_LEASE_ABANDONED", "EMERGENCY_REQUEST_STALLED",
           "Lease", "LeaseDenied", "LeaseLost", "LeaseGuard", "AccountLeases", "EmergencyControl", "EmergencyError",
           "PreemptionGate", "AccountTask", "TaskOutcome", "BoundedAccountRunner", "account_id_of_call",
           "platform_observation", "platform_faults", "priority_of", "wall_ms"]
