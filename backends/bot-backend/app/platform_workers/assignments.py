"""Durable account-to-worker assignment with fencing (Section H, Step 5.2).

Each executable account is owned by at most one trading worker at a time. The owner holds a row in
``platform_account_assignments`` with a **fencing token** that rises on every change of owner, an expiry
measured on the **database server's clock** (so two hosts with skewed clocks cannot both believe they own an
account), a renewal time and the last successful processing time. Every change of owner is appended to
``platform_assignment_history``.

Relation to ``pipeline.coordination.AccountLeases``: that lease is per *task* (one shadow account evaluation, or
an emergency flatten taking priority) on the SQLite runtime database. This module is the sticky, per-*worker*
ownership a multi-worker deployment needs on a shared PostgreSQL server. A worker only runs account work for
accounts assigned to it, and still takes the task lease where the existing code does; the two are not
alternatives.

Rules:

* ``claim`` decides in one transaction under an application-wide lock: an account is free when it has no row,
  was released, or its expiry has passed on the database clock. Each worker claims up to its fair share
  (``ceil(active accounts / live workers)``), in a deterministic order, so a fleet converges without a
  coordinator.
* ``renew`` extends only claims that are still the caller's and still unexpired. An expired claim is never
  revived -- from that instant somebody else was entitled to it.
* ``verify_on`` is the fencing check to run inside the transaction of any durable mutation: it locks the
  assignment row (PostgreSQL ``FOR UPDATE``; SQLite holds the write lock), so no takeover can commit between the
  check and the mutation.
* A new owner must ``mark_recovered`` -- reconstruct exchange state and durable work -- before it may submit
  anything for an account (``needs_recovery``).
"""
from __future__ import annotations

import math
import socket
import os
import uuid
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

from shared_lib.persistence.sql import POSTGRESQL, Connection, Database

DEFAULT_ASSIGNMENT_TTL_MS = 30_000
#: A worker whose heartbeat is older than this is not counted when computing the fair share.
WORKER_LIVENESS_MS = 30_000
_ASSIGN_LOCK_KEY = 0x0C05_A551

TAKEOVER, NEW, RENEWED = "TAKEOVER", "NEW", "RENEWED"


class AssignmentLost(RuntimeError):
    """The worker no longer owns the account. Raised before a mutation, never after one."""

    code = "ACCOUNT_ASSIGNMENT_LOST"

    def __init__(self, account_id: str, reason: str) -> None:
        super().__init__(f"{account_id}: {reason}")
        self.account_id = account_id
        self.reason = reason


@dataclass(frozen=True)
class Assignment:
    account_id: str
    tenant_id: str
    worker_id: str
    fencing_token: int
    expires_at: int
    kind: str                # NEW / TAKEOVER / RENEWED
    needs_recovery: bool
    previous_worker: Optional[str] = None


def _lock(conn: Connection) -> None:
    if conn.dialect == POSTGRESQL:
        conn.execute("SELECT pg_advisory_xact_lock(?)", (_ASSIGN_LOCK_KEY,))


class WorkerAssignments:
    def __init__(self, db: Database, *, ttl_ms: int = DEFAULT_ASSIGNMENT_TTL_MS,
                 worker_liveness_ms: int = WORKER_LIVENESS_MS) -> None:
        if ttl_ms <= 0:
            raise ValueError("ASSIGNMENT_TTL_INVALID")
        self.db = db
        self.ttl_ms = int(ttl_ms)
        self.worker_liveness_ms = int(worker_liveness_ms)

    # ── workers ────────────────────────────────────────────────────────────────────────────────────────
    def register_worker(self, worker_id: Optional[str] = None, *, role: str = "trading",
                        build_version: str = "unknown") -> str:
        worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        with self.db.transaction() as conn:
            now = conn.now_ms()
            conn.execute(
                "INSERT INTO platform_workers (worker_id, role, host, pid, build_version, started_at, heartbeat_at, state) "
                "VALUES (?,?,?,?,?,?,?,'ACTIVE') ON CONFLICT (worker_id) DO UPDATE SET heartbeat_at=excluded.heartbeat_at, "
                "state='ACTIVE', stopped_at=NULL, build_version=excluded.build_version",
                (worker_id, role, socket.gethostname(), os.getpid(), build_version, now, now))
        return worker_id

    def heartbeat(self, worker_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("UPDATE platform_workers SET heartbeat_at=? WHERE worker_id=?", (conn.now_ms(), worker_id))

    def set_worker_state(self, worker_id: str, state: str) -> None:
        with self.db.transaction() as conn:
            now = conn.now_ms()
            conn.execute("UPDATE platform_workers SET state=?, heartbeat_at=?, stopped_at=? WHERE worker_id=?",
                         (state, now, now if state == "STOPPED" else None, worker_id))

    # ── accounts ───────────────────────────────────────────────────────────────────────────────────────
    def upsert_accounts(self, accounts: Iterable[Tuple[str, str, str, str]], *, state: str = "ACTIVE") -> int:
        """``accounts``: (account_id, tenant_id, venue, environment). An account's tenant never changes: an
        upsert that would move it to another tenant is refused (a re-used id must not inherit another
        customer's assignment history)."""
        rows = list(accounts)
        with self.db.transaction() as conn:
            now = conn.now_ms()
            for account_id, tenant_id, venue, environment in rows:
                existing = conn.one("SELECT tenant_id FROM platform_accounts WHERE account_id=?", (account_id,))
                if existing is not None and existing[0] != tenant_id:
                    raise ValueError(f"ACCOUNT_TENANT_IMMUTABLE:{account_id}")
                conn.execute(
                    "INSERT INTO platform_accounts (account_id, tenant_id, venue, environment, state, created_at) "
                    "VALUES (?,?,?,?,?,?) ON CONFLICT (account_id) DO UPDATE SET state=excluded.state, "
                    "venue=excluded.venue, environment=excluded.environment",
                    (account_id, tenant_id, venue, environment, state, now))
        return len(rows)

    # ── claiming ───────────────────────────────────────────────────────────────────────────────────────
    def claim(self, worker_id: str, *, max_new: Optional[int] = None) -> List[Assignment]:
        """Renew this worker's live claims and take free accounts up to its fair share. Returns every account
        the worker owns after the call."""
        with self.db.transaction() as conn:
            _lock(conn)
            now = conn.now_ms()
            conn.execute("UPDATE platform_workers SET heartbeat_at=? WHERE worker_id=?", (now, worker_id))
            state = conn.scalar("SELECT state FROM platform_workers WHERE worker_id=?", (worker_id,))
            if state is None:
                raise ValueError(f"WORKER_NOT_REGISTERED:{worker_id}")
            expires = now + self.ttl_ms
            conn.execute(
                "UPDATE platform_account_assignments SET expires_at=?, renewed_at=? WHERE worker_id=? "
                "AND released_at IS NULL AND expires_at > ?", (expires, now, worker_id, now))
            owned = self._owned_on(conn, worker_id, now)
            result = [Assignment(a, t, worker_id, tok, exp, RENEWED, rec != tok) for a, t, tok, exp, rec in owned]
            if state != "ACTIVE":
                return result
            live_workers = int(conn.scalar(
                "SELECT COUNT(*) FROM platform_workers WHERE state='ACTIVE' AND heartbeat_at > ?",
                (now - self.worker_liveness_ms,)) or 1)
            active_accounts = int(conn.scalar("SELECT COUNT(*) FROM platform_accounts WHERE state='ACTIVE'") or 0)
            share = math.ceil(active_accounts / max(live_workers, 1))
            room = share - len(owned)
            if max_new is not None:
                room = min(room, max_new)
            if room <= 0:
                return result
            free = conn.all(
                "SELECT a.account_id, a.tenant_id, s.fencing_token, s.worker_id, s.released_at, s.expires_at "
                "FROM platform_accounts a LEFT JOIN platform_account_assignments s ON s.account_id = a.account_id "
                "WHERE a.state='ACTIVE' AND (s.account_id IS NULL OR s.released_at IS NOT NULL OR s.expires_at <= ?) "
                "ORDER BY a.account_id LIMIT ?", (now, room))
            for account_id, tenant_id, token, previous, released_at, prev_expiry in free:
                new_token = int(token or 0) + 1
                kind = TAKEOVER if (previous is not None and released_at is None) else NEW
                conn.execute(
                    "INSERT INTO platform_account_assignments (account_id, worker_id, fencing_token, acquired_at, "
                    "renewed_at, expires_at, released_at, release_reason, recovered_token) VALUES (?,?,?,?,?,?,NULL,NULL,NULL) "
                    "ON CONFLICT (account_id) DO UPDATE SET worker_id=excluded.worker_id, fencing_token=excluded.fencing_token, "
                    "acquired_at=excluded.acquired_at, renewed_at=excluded.renewed_at, expires_at=excluded.expires_at, "
                    "released_at=NULL, release_reason=NULL",
                    (account_id, worker_id, new_token, now, now, expires))
                conn.execute(
                    "INSERT INTO platform_assignment_history (account_id, from_worker, to_worker, fencing_token, reason, at) "
                    "VALUES (?,?,?,?,?,?)",
                    (account_id, previous, worker_id, new_token,
                     "EXPIRED_TAKEOVER" if kind == TAKEOVER else ("REASSIGNED" if previous else "INITIAL"), now))
                result.append(Assignment(account_id, tenant_id, worker_id, new_token, expires, kind, True,
                                         previous if kind == TAKEOVER else None))
            return result

    @staticmethod
    def _owned_on(conn: Connection, worker_id: str, now: int):
        return conn.all(
            "SELECT s.account_id, a.tenant_id, s.fencing_token, s.expires_at, s.recovered_token "
            "FROM platform_account_assignments s JOIN platform_accounts a ON a.account_id = s.account_id "
            "WHERE s.worker_id=? AND s.released_at IS NULL AND s.expires_at > ? ORDER BY s.account_id",
            (worker_id, now))

    def surplus(self, worker_id: str) -> List[str]:
        """Accounts above this worker's fair share (after another worker joined): release them at a safe point."""
        with self.db.transaction(write=False) as conn:
            now = conn.now_ms()
            live = int(conn.scalar("SELECT COUNT(*) FROM platform_workers WHERE state='ACTIVE' AND heartbeat_at > ?",
                                   (now - self.worker_liveness_ms,)) or 1)
            active = int(conn.scalar("SELECT COUNT(*) FROM platform_accounts WHERE state='ACTIVE'") or 0)
            owned = [r[0] for r in self._owned_on(conn, worker_id, now)]
        share = math.ceil(active / max(live, 1))
        return owned[share:] if len(owned) > share else []

    # ── fencing ────────────────────────────────────────────────────────────────────────────────────────
    @staticmethod
    def verify_on(conn: Connection, account_id: str, worker_id: str, fencing_token: int) -> None:
        row = conn.one(
            "SELECT worker_id, fencing_token, expires_at, released_at FROM platform_account_assignments "
            "WHERE account_id=?" + conn.lock_rows_suffix(), (account_id,))
        if row is None:
            raise AssignmentLost(account_id, "ASSIGNMENT_MISSING")
        if row[0] != worker_id or int(row[1]) != int(fencing_token):
            raise AssignmentLost(account_id, "ASSIGNMENT_TAKEN_OVER")
        if row[3] is not None:
            raise AssignmentLost(account_id, "ASSIGNMENT_RELEASED")
        if int(row[2]) <= conn.now_ms():
            raise AssignmentLost(account_id, "ASSIGNMENT_EXPIRED")

    def verify(self, a: Assignment) -> None:
        with self.db.transaction() as conn:
            self.verify_on(conn, a.account_id, a.worker_id, a.fencing_token)

    def mark_recovered(self, a: Assignment) -> None:
        with self.db.transaction() as conn:
            self.verify_on(conn, a.account_id, a.worker_id, a.fencing_token)
            conn.execute("UPDATE platform_account_assignments SET recovered_token=? WHERE account_id=?",
                         (a.fencing_token, a.account_id))

    def mark_success(self, a: Assignment) -> None:
        with self.db.transaction() as conn:
            self.verify_on(conn, a.account_id, a.worker_id, a.fencing_token)
            conn.execute("UPDATE platform_account_assignments SET last_success_at=? WHERE account_id=?",
                         (conn.now_ms(), a.account_id))

    def release(self, worker_id: str, account_ids: Optional[Sequence[str]] = None, *, reason: str = "RELEASED") -> int:
        with self.db.transaction() as conn:
            now = conn.now_ms()
            rows = self._owned_on(conn, worker_id, now)
            targets = [r for r in rows if account_ids is None or r[0] in set(account_ids)]
            for account_id, _, token, _, _ in targets:
                conn.execute("UPDATE platform_account_assignments SET released_at=?, release_reason=? "
                             "WHERE account_id=? AND worker_id=? AND fencing_token=?",
                             (now, reason, account_id, worker_id, token))
                conn.execute("INSERT INTO platform_assignment_history (account_id, from_worker, to_worker, fencing_token, "
                             "reason, at) VALUES (?,?,NULL,?,?,?)", (account_id, worker_id, token, reason, now))
            return len(targets)

    def owner_of(self, account_id: str) -> Optional[Tuple[str, int, int]]:
        with self.db.transaction(write=False) as conn:
            row = conn.one("SELECT worker_id, fencing_token, expires_at FROM platform_account_assignments "
                           "WHERE account_id=? AND released_at IS NULL AND expires_at > ?", (account_id, conn.now_ms()))
            return (row[0], int(row[1]), int(row[2])) if row else None

    def history(self, account_id: str) -> List[tuple]:
        with self.db.transaction(write=False) as conn:
            return conn.all("SELECT from_worker, to_worker, fencing_token, reason, at FROM platform_assignment_history "
                            "WHERE account_id=? ORDER BY id", (account_id,))
