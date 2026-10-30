"""Runtime ownership lease — one trading scheduler per database.

Port numbers do not protect anything: two backends on different ports pointed
at the same runtime database will both start a MultiBotRunner and both drive
the same bots. The lease is held against the *database*, which is the thing
that actually must not have two writers.

Contract:

* Exactly one process may hold the trading lease for a given database path.
* The holder renews it on every scheduler heartbeat.
* A lease whose heartbeat has gone stale can be taken over — that is how a
  crashed process is recovered without manual intervention.
* A process that cannot acquire the lease **must not start a scheduler**. It
  may still serve read-only and admin APIs.

Fail closed: any error acquiring the lease means "not the owner".
"""
from __future__ import annotations

import logging
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

#: A lease is considered abandoned once its heartbeat is older than this. Set
#: well above the 10-second scheduler tick so a slow cycle never steals it from
#: a healthy process, but low enough that a crash recovers promptly.
LEASE_STALE_SECONDS = 90

#: How often the holder should renew. The scheduler calls this each cycle.
LEASE_RENEW_SECONDS = 10

TRADING_SCHEDULER = "trading_scheduler"

#: Slack between a process's creation time and the lease it later writes.
PROCESS_START_TOLERANCE_SECONDS = 2.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _is_shared_server(db: Any) -> bool:
    """A database server shared by several hosts (PostgreSQL). On SQLite the database is one local file, so the
    local clock and local process table are the database's own."""
    return getattr(db, "dialect", "sqlite") == "postgresql"


def _db_now(conn: Any) -> datetime:
    """The DATABASE SERVER's clock (UTC, milliseconds): every host compares leases against one clock."""
    text = conn.execute("SELECT datetime('now', 'subsec')").fetchone()[0]
    return datetime.fromisoformat(str(text)).replace(tzinfo=timezone.utc)


def lease_database_path(database_path: Any) -> str:
    """The lease key of a database: an absolute file path, or a database URL unchanged (``abspath`` of a URL would
    depend on the working directory, so two processes could compute two keys for one database)."""
    text = str(database_path)
    return text if "://" in text else os.path.abspath(text)


def ensure_ownership_schema(db: Any) -> None:
    """Create the lease table. Safe to run repeatedly."""
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_ownership (
                lease_name TEXT NOT NULL,
                database_path TEXT NOT NULL,
                runtime_owner_id TEXT NOT NULL,
                runtime_session_id TEXT,
                pid INTEGER NOT NULL,
                hostname TEXT NOT NULL,
                started_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL,
                database_role TEXT,
                released_at TEXT,
                release_reason TEXT,
                PRIMARY KEY (lease_name, database_path)
            )
            """
        )


@dataclass(frozen=True)
class OwnershipResult:
    acquired: bool
    runtime_owner_id: str | None
    reason: str
    holder_pid: int | None = None
    holder_hostname: str | None = None
    holder_heartbeat_at: str | None = None
    holder_session_id: str | None = None

    @property
    def may_start_scheduler(self) -> bool:
        return self.acquired


class RuntimeOwnership:
    """Holds (or fails to hold) the trading lease for one database."""

    def __init__(
        self,
        db: Any,
        *,
        database_path: str,
        database_role: str = "development",
        runtime_session_id: str | None = None,
        lease_name: str = TRADING_SCHEDULER,
        stale_seconds: int = LEASE_STALE_SECONDS,
    ) -> None:
        self.db = db
        self.database_path = lease_database_path(database_path)
        self.database_role = database_role
        self.runtime_session_id = runtime_session_id
        self.lease_name = lease_name
        self.stale_seconds = int(stale_seconds)
        self.runtime_owner_id = f"own_{uuid.uuid4().hex[:20]}"
        self.hostname = socket.gethostname()
        self._is_owner = False
        self.renew_failures = 0

    # ── Acquisition ─────────────────────────────────────────────────────────

    def acquire(self) -> OwnershipResult:
        """Try to take the trading lease. Fails closed on any error."""
        try:
            ensure_ownership_schema(self.db)
            now = _now()
            takeover_reason = None
            with self.db.connect() as conn:
                # Read-then-write under one write lock: two processes starting
                # together cannot both find the lease free and both take it.
                conn.execute("BEGIN IMMEDIATE")
                shared = _is_shared_server(self.db)
                if shared:
                    # A shared server: lease times come from the database clock, never a host's own.
                    now = _db_now(conn)
                row = conn.execute(
                    """SELECT * FROM runtime_ownership
                       WHERE lease_name=? AND database_path=?""",
                    (self.lease_name, self.database_path),
                ).fetchone()

                if row is not None and row["released_at"] is None:
                    holder_pid = int(row["pid"])
                    heartbeat = row["heartbeat_at"]
                    # Another host's (or container's) PID means nothing in this process table: such a holder
                    # is alive until its heartbeat goes stale (holder_is_alive with its hostname).
                    holder_alive = holder_is_alive(holder_pid, row["started_at"], hostname=row["hostname"])
                    if not self._is_stale(heartbeat, now) and holder_alive:
                        # A live holder exists. This process must not schedule.
                        return OwnershipResult(
                            acquired=False,
                            runtime_owner_id=None,
                            reason="RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS",
                            holder_pid=holder_pid,
                            holder_hostname=row["hostname"],
                            holder_heartbeat_at=heartbeat,
                            holder_session_id=row["runtime_session_id"],
                        )
                    takeover_reason = (
                        "STALE_HEARTBEAT" if self._is_stale(heartbeat, now) else "HOLDER_PID_GONE"
                    )
                    logger.warning(
                        "[RUNTIME_OWNERSHIP] taking over lease from pid=%s (%s)",
                        holder_pid, takeover_reason,
                    )

                conn.execute(
                    """INSERT INTO runtime_ownership
                       (lease_name,database_path,runtime_owner_id,runtime_session_id,pid,
                        hostname,started_at,heartbeat_at,database_role,released_at,release_reason)
                       VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)
                       ON CONFLICT(lease_name, database_path) DO UPDATE SET
                         runtime_owner_id=excluded.runtime_owner_id,
                         runtime_session_id=excluded.runtime_session_id,
                         pid=excluded.pid, hostname=excluded.hostname,
                         started_at=excluded.started_at, heartbeat_at=excluded.heartbeat_at,
                         database_role=excluded.database_role,
                         released_at=NULL, release_reason=NULL""",
                    (self.lease_name, self.database_path, self.runtime_owner_id,
                     self.runtime_session_id, os.getpid(), self.hostname,
                     now.isoformat(), now.isoformat(), self.database_role),
                )
            self._is_owner = True
            # Printed, not only logged: an ownership change must reach the
            # service journal whatever the logging configuration is.
            print(f"[RUNTIME_OWNERSHIP] acquired pid={os.getpid()} host={self.hostname} "
                  f"runtime_session_id={self.runtime_session_id} owner_id={self.runtime_owner_id} "
                  f"took_over={takeover_reason or 'NO'}", flush=True)
            return OwnershipResult(
                acquired=True, runtime_owner_id=self.runtime_owner_id, reason="ACQUIRED",
            )
        except Exception as exc:
            logger.error("[RUNTIME_OWNERSHIP] acquire failed: %s", exc)
            return OwnershipResult(
                acquired=False, runtime_owner_id=None,
                reason=f"RUNTIME_OWNERSHIP_ERROR:{type(exc).__name__}",
            )

    # ── Renewal / release ───────────────────────────────────────────────────

    def renew(self) -> bool:
        """Refresh the heartbeat. False means ownership was lost."""
        if not self._is_owner:
            return False
        try:
            with self.db.connect() as conn:
                stamp = _db_now(conn) if _is_shared_server(self.db) else _now()
                cursor = conn.execute(
                    """UPDATE runtime_ownership SET heartbeat_at=?
                       WHERE lease_name=? AND database_path=? AND runtime_owner_id=?
                         AND released_at IS NULL""",
                    (stamp.isoformat(), self.lease_name, self.database_path,
                     self.runtime_owner_id),
                )
                if (cursor.rowcount or 0) == 0:
                    self._is_owner = False
                    logger.error("[RUNTIME_OWNERSHIP] lost: lease taken by another process")
                    return False
            self.renew_failures = 0
            return True
        except Exception as exc:
            # A heartbeat that could not be WRITTEN is not a lease that was
            # LOST: nobody took it. Treating a transient "database is locked"
            # as loss stopped the scheduler for good while the process stayed
            # up. Ownership is kept and the heartbeat simply ages; a holder
            # that cannot renew for the whole stale window is taken over by the
            # ordinary staleness rule, and every trading gate reads the
            # persisted heartbeat, so nothing trades on a lease that went stale.
            self.renew_failures += 1
            logger.error("[RUNTIME_OWNERSHIP] renew failed (%d consecutive): %s", self.renew_failures, exc)
            return True

    def release(self, *, reason: str = "SHUTDOWN") -> None:
        """Release the lease so a replacement can start immediately."""
        if not self._is_owner:
            return
        try:
            with self.db.connect() as conn:
                stamp = _db_now(conn) if _is_shared_server(self.db) else _now()
                conn.execute(
                    """UPDATE runtime_ownership SET released_at=?, release_reason=?
                       WHERE lease_name=? AND database_path=? AND runtime_owner_id=?""",
                    (stamp.isoformat(), reason, self.lease_name,
                     self.database_path, self.runtime_owner_id),
                )
            print(f"[RUNTIME_OWNERSHIP] released pid={os.getpid()} reason={reason} "
                  f"owner_id={self.runtime_owner_id}", flush=True)
        except Exception as exc:
            logger.error("[RUNTIME_OWNERSHIP] release failed: %s", exc)
        finally:
            self._is_owner = False

    @property
    def is_owner(self) -> bool:
        return self._is_owner

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _is_stale(self, heartbeat_at: str | None, now: datetime) -> bool:
        if not heartbeat_at:
            return True
        try:
            last = datetime.fromisoformat(heartbeat_at)
        except Exception:
            return True
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return (now - last) > timedelta(seconds=self.stale_seconds)

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        """Best effort. Unknown counts as alive so we never steal a live lease."""
        if pid == os.getpid():
            return True
        try:
            if os.name == "nt":
                import subprocess

                out = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    capture_output=True, text=True, timeout=5,
                )
                return str(pid) in (out.stdout or "")
            os.kill(pid, 0)
            return True
        except PermissionError:
            return True
        except (OSError, ProcessLookupError):
            return False
        except Exception:
            return True


def pid_is_alive(pid: int | None) -> bool:
    """Public form of the liveness check the lease uses to decide a takeover.

    Exposed so runtime preflight asks the same question the same way. Unknown
    counts as alive, so a process we cannot inspect is never treated as dead.
    """
    if not pid:
        return False
    return RuntimeOwnership._pid_alive(int(pid))


_OWN_CREATED_AT: float | None = None


def _process_created_at(pid: int) -> float | None:
    """Creation time of ``pid`` in epoch seconds; None when it is gone."""
    global _OWN_CREATED_AT
    import psutil

    if pid == os.getpid():
        if _OWN_CREATED_AT is None:
            _OWN_CREATED_AT = psutil.Process(pid).create_time()
        return _OWN_CREATED_AT
    try:
        return psutil.Process(pid).create_time()
    except psutil.NoSuchProcess:
        return None


def is_local_host(hostname: str | None) -> bool:
    """True when ``hostname`` names THIS host -- the only one whose process table can be inspected. Containers
    have their own hostnames and their own PID namespaces, so another container counts as another host."""
    return hostname is None or str(hostname) == socket.gethostname()


def holder_is_alive(pid: int | None, started_at: str | None, *, hostname: str | None = None) -> bool:
    """Is the process that WROTE this lease still running?

    ``hostname`` (the lease's): a holder on ANOTHER host -- another machine or another container, whose PIDs live
    in a separate namespace -- cannot be inspected from here, so it is alive; only its heartbeat going stale
    (``lease_is_stale``) frees the lease. Two trading containers on one database therefore never judge each
    other's live lease dead by PID. On this host a dead PID is still taken over at once.

    A PID alone cannot answer that. After a reboot the service returns with a
    low and frequently identical PID, so an unreleased lease from before the
    reboot names a live process -- possibly this very one -- that never held
    it, and the runtime refused to start against its own ghost until the
    heartbeat aged out. A process created after the lease was written is not
    its holder.

    Unknown stays alive: a lease is never taken from a process that cannot be
    inspected.
    """
    if not pid:
        return False
    if not is_local_host(hostname):
        return True
    pid = int(pid)
    if not RuntimeOwnership._pid_alive(pid):
        return False
    try:
        created = _process_created_at(pid)
        if created is None:
            return False
        started = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        return created <= started.timestamp() + PROCESS_START_TOLERANCE_SECONDS
    except Exception:
        return True


def lease_is_stale(heartbeat_at: str | None, *, stale_seconds: int = LEASE_STALE_SECONDS) -> bool:
    """Public form of the staleness policy, so preflight cannot drift from it.

    A missing or unparseable heartbeat is stale: the alternative is treating an
    unreadable lease as healthy, which would block startup forever.
    """
    if not heartbeat_at:
        return True
    try:
        last = datetime.fromisoformat(heartbeat_at)
    except Exception:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (_now() - last) > timedelta(seconds=int(stale_seconds))


def current_owner(db: Any, database_path: str, lease_name: str = TRADING_SCHEDULER) -> dict | None:
    """Read the active lease row, if any."""
    try:
        ensure_ownership_schema(db)
        with db.connect() as conn:
            row = conn.execute(
                """SELECT * FROM runtime_ownership
                   WHERE lease_name=? AND database_path=? AND released_at IS NULL""",
                (lease_name, lease_database_path(database_path)),
            ).fetchone()
        return dict(row) if row else None
    except Exception:
        return None
