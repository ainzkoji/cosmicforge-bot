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


def _now() -> datetime:
    return datetime.now(timezone.utc)


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
        self.database_path = os.path.abspath(str(database_path))
        self.database_role = database_role
        self.runtime_session_id = runtime_session_id
        self.lease_name = lease_name
        self.stale_seconds = int(stale_seconds)
        self.runtime_owner_id = f"own_{uuid.uuid4().hex[:20]}"
        self.hostname = socket.gethostname()
        self._is_owner = False

    # ── Acquisition ─────────────────────────────────────────────────────────

    def acquire(self) -> OwnershipResult:
        """Try to take the trading lease. Fails closed on any error."""
        try:
            ensure_ownership_schema(self.db)
            now = _now()
            with self.db.connect() as conn:
                row = conn.execute(
                    """SELECT * FROM runtime_ownership
                       WHERE lease_name=? AND database_path=?""",
                    (self.lease_name, self.database_path),
                ).fetchone()

                if row is not None and row["released_at"] is None:
                    holder_pid = int(row["pid"])
                    heartbeat = row["heartbeat_at"]
                    if not self._is_stale(heartbeat, now) and self._pid_alive(holder_pid):
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
                cursor = conn.execute(
                    """UPDATE runtime_ownership SET heartbeat_at=?
                       WHERE lease_name=? AND database_path=? AND runtime_owner_id=?
                         AND released_at IS NULL""",
                    (_now().isoformat(), self.lease_name, self.database_path,
                     self.runtime_owner_id),
                )
                if (cursor.rowcount or 0) == 0:
                    self._is_owner = False
                    logger.error("[RUNTIME_OWNERSHIP] lost: lease taken by another process")
                    return False
            return True
        except Exception as exc:
            logger.error("[RUNTIME_OWNERSHIP] renew failed: %s", exc)
            return False

    def release(self, *, reason: str = "SHUTDOWN") -> None:
        """Release the lease so a replacement can start immediately."""
        if not self._is_owner:
            return
        try:
            with self.db.connect() as conn:
                conn.execute(
                    """UPDATE runtime_ownership SET released_at=?, release_reason=?
                       WHERE lease_name=? AND database_path=? AND runtime_owner_id=?""",
                    (_now().isoformat(), reason, self.lease_name,
                     self.database_path, self.runtime_owner_id),
                )
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


def current_owner(db: Any, database_path: str, lease_name: str = TRADING_SCHEDULER) -> dict | None:
    """Read the active lease row, if any."""
    try:
        ensure_ownership_schema(db)
        with db.connect() as conn:
            row = conn.execute(
                """SELECT * FROM runtime_ownership
                   WHERE lease_name=? AND database_path=? AND released_at IS NULL""",
                (lease_name, os.path.abspath(str(database_path))),
            ).fetchone()
        return dict(row) if row else None
    except Exception:
        return None
