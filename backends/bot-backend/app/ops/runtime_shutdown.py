"""Bring the trading runtime down cleanly, and prove that it went.

Stopping the process was never the hard part. The problem was what a stop left
behind: the supervised launcher used ``Stop-Process -Force``, so the
application's shutdown path never ran, and the ownership lease stayed with
``released_at`` NULL against a PID that no longer existed. PID-liveness
takeover repairs that on the next start, but "eventually self-heals" is a poor
description of a normal operator shutdown, and in the meantime the lease says
something untrue.

The runtime database currently holds 48 ``runtime_sessions`` rows marked
RUNNING against one live process. Most of those are duplicate starts (fixed by
runtime preflight); the rest are stops that never closed their session.

Quiescing is idempotent and ordered so that the dangerous work stops first:

    1. stop accepting new entry work
    2. stop the scheduler loop
    3. let the current cycle finish rather than interrupting it
    4. release the ownership lease
    5. close the runtime session

**Positions are never flattened here.** A process stopping is not a reason to
exit a trade. Position state is persisted and restart-restorable, which is the
property Phase 12 proved; closing on shutdown would destroy it.
"""
from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

#: Longest we wait for an in-flight cycle to finish before proceeding anyway.
#: A cycle is ~10s; beyond this it is not going to finish on its own.
CYCLE_DRAIN_TIMEOUT_S = 15.0

#: Where the operator scripts ask for a graceful stop. Windows has no usable
#: "please stop cleanly" signal for a process started by another process, so
#: the request is a file the runtime polls.
_RUNTIME_LOG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "logs", "runtime",
)
STOP_FILE = os.path.join(_RUNTIME_LOG_DIR, "STOP")

#: Why the process exited, written by the runtime and read by the supervisor
#: after the child is gone.
#:
#: The stop file alone is not enough. The runtime polls for it every second and
#: the supervisor every two, so whoever looked first won and the other never saw
#: the request at all -- in practice the runtime consumed it, the supervisor saw
#: an unexplained exit, and restarted the runtime the operator had just stopped.
#: An exit reason that outlives the process removes the race entirely: the
#: supervisor asks "why did it exit", not "did I happen to catch the request".
STOPPED_MARKER = os.path.join(_RUNTIME_LOG_DIR, "STOPPED_BY_OPERATOR")

_LOCK = threading.Lock()
_STATE: dict[str, Any] = {"done": False, "report": None}


@dataclass
class QuiesceReport:
    """What the shutdown actually managed to do, step by step."""

    reason: str
    started_at: str
    entries_stopped: bool = False
    scheduler_stopped: bool = False
    cycle_drained: bool = False
    lease_released: bool = False
    session_closed: bool = False
    runtime_session_id: str | None = None
    lease_owner_pid: int | None = None
    errors: list[str] = field(default_factory=list)
    completed_at: str | None = None

    @property
    def clean(self) -> bool:
        """A clean stop released the lease and closed the session."""
        return self.lease_released and self.session_closed and not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "clean": self.clean,
            "entries_stopped": self.entries_stopped,
            "scheduler_stopped": self.scheduler_stopped,
            "cycle_drained": self.cycle_drained,
            "lease_released": self.lease_released,
            "session_closed": self.session_closed,
            "runtime_session_id": self.runtime_session_id,
            "lease_owner_pid": self.lease_owner_pid,
            "errors": list(self.errors),
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def quiesce(reason: str = "OPERATOR_SHUTDOWN", *, timeout_s: float | None = None) -> QuiesceReport:
    """Stop trading and release ownership. Safe to call more than once.

    Called from the application shutdown hook and from the stop-file watcher,
    which can both fire for the same stop, so the first call does the work and
    the rest return its report.
    """
    with _LOCK:
        if _STATE["done"]:
            return _STATE["report"]

        report = QuiesceReport(reason=reason, started_at=_now())
        try:
            _quiesce_inner(report, timeout_s or CYCLE_DRAIN_TIMEOUT_S)
        except Exception as exc:
            report.errors.append(f"{type(exc).__name__}: {exc}")
            logger.error("[RUNTIME_SHUTDOWN] quiesce failed: %s", exc)
        report.completed_at = _now()

        _STATE["done"] = True
        _STATE["report"] = report
        logger.info("[RUNTIME_SHUTDOWN] %s", report.to_dict())
        print(f"[RUNTIME_SHUTDOWN] clean={report.clean} reason={reason} "
              f"lease_released={report.lease_released} "
              f"session_closed={report.session_closed}")
        return report


def _quiesce_inner(report: QuiesceReport, timeout_s: float) -> None:
    import app.main as main_module

    runner_service = getattr(main_module, "runner_service", None)
    multi = getattr(runner_service, "multi_runner", None) if runner_service else None

    # ── 1/2. Stop new entry work and the scheduler loop ─────────────────────
    if runner_service is not None:
        try:
            runner_service.running = False
            report.entries_stopped = True
        except Exception as exc:
            report.errors.append(f"stop_entries: {exc}")

    if multi is not None:
        try:
            multi.running = False
            multi._stop_requested = True
            report.scheduler_stopped = True
        except Exception as exc:
            report.errors.append(f"stop_scheduler: {exc}")

    # ── 3. Let the current cycle finish ─────────────────────────────────────
    # Interrupting mid-cycle is how half-written evidence happens. Each cached
    # runner is asked to stop at its next check rather than being cancelled.
    try:
        for runner in list((getattr(multi, "_runners", None) or {}).values()):
            try:
                runner._stop_requested = True
            except Exception:
                pass
        report.cycle_drained = _wait_for_idle(multi, timeout_s)
    except Exception as exc:
        report.errors.append(f"drain: {exc}")

    # ── 4. Release the lease ────────────────────────────────────────────────
    # This is the step force-kill skipped, and the reason a dead PID kept
    # holding ownership until the next start noticed.
    if multi is not None:
        try:
            ownership = getattr(multi, "_ownership", None)
            report.lease_owner_pid = getattr(ownership, "pid", None) or _own_pid()
            multi.release_runtime_ownership(reason=report.reason)
            report.lease_released = _lease_is_released()
        except Exception as exc:
            report.errors.append(f"release_lease: {exc}")
    else:
        # No runner was ever built, so this process never owned anything.
        report.lease_released = True

    # ── 5. Close the runtime session ────────────────────────────────────────
    session_id = getattr(main_module, "RUNTIME_SESSION_ID", None)
    report.runtime_session_id = session_id
    if session_id:
        try:
            from app.evidence.writers import close_runtime_session
            from shared_lib.persistence.db import DB

            close_runtime_session(DB(), session_id, reason=report.reason)
            report.session_closed = True
        except Exception as exc:
            report.errors.append(f"close_session: {exc}")
    else:
        report.session_closed = True


def _own_pid() -> int:
    import os

    return os.getpid()


def _wait_for_idle(multi: Any, timeout_s: float) -> bool:
    """Wait for the scheduler loop to report it is no longer iterating."""
    import time

    if multi is None:
        return True
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if not getattr(multi, "running", False):
            return True
        time.sleep(0.25)
    return False


def _lease_is_released() -> bool:
    try:
        from app.ops.runtime_ownership import TRADING_SCHEDULER, current_owner
        from shared_lib.persistence.db import DB

        database = DB()
        return current_owner(database, database.path, TRADING_SCHEDULER) is None
    except Exception:
        return False


def record_operator_stop(reason: str = "OPERATOR_STOP_FILE") -> None:
    """Leave the exit reason on disk so the supervisor does not restart us."""
    try:
        os.makedirs(_RUNTIME_LOG_DIR, exist_ok=True)
        with open(STOPPED_MARKER, "w", encoding="utf-8") as handle:
            handle.write(f"{reason} pid={os.getpid()} at={_now()}\n")
    except Exception as exc:
        logger.warning("[RUNTIME_SHUTDOWN] could not record stop reason: %s", exc)


def clear_stop_signals() -> None:
    """Drop leftover stop requests at startup.

    A stop file surviving from a previous run would otherwise shut down a
    freshly started runtime a second after it came up.
    """
    for path in (STOP_FILE, STOPPED_MARKER):
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as exc:
            logger.warning("[RUNTIME_SHUTDOWN] could not clear %s: %s", path, exc)


def reset_for_tests() -> None:
    """Clear the once-only latch. Test support; never called in production."""
    with _LOCK:
        _STATE["done"] = False
        _STATE["report"] = None


def last_report() -> QuiesceReport | None:
    return _STATE["report"]
