"""The runtime-owner lease loop (Section I Batch 7: moved out of the legacy ``MultiBotRunner``).

Exactly one process may schedule trading against a runtime database. That process holds the
``trading_scheduler`` lease (``app.ops.runtime_ownership.RuntimeOwnership``) and proves it is alive by renewing
the lease and beating the watchdog heartbeat every iteration. Everything that trades or reconciles in production
-- the CATI production runtime, the shadow scheduler, the venue runtime, the background jobs -- asks whether
this process owns the runtime (``residual_prospective.owner_current`` reads the persisted lease; ``app.main``
reads ``owns_runtime`` / ``ownership_decided`` / ``ownership_reason``).

In production the legacy ``MultiBotRunner.run_loop`` did only this: its ``run_once`` returns at once, because the
V2 ``PaperRunner`` is never built there. This class is that loop, with no runner in it. In production it is
hosted by ``runtime_owner.RuntimeOwnerHost`` (``MultiBotRunner`` is not built there); outside production
``MultiBotRunner`` delegates to it. The readers -- ``app.main``, ``app.api.bot_instances``,
``app.ops.runtime_shutdown`` (quiesce and lease release) and ``app.ops.runtime_supervisor`` (health) -- ask
through ``runtime_owner`` (E-05), so they do not depend on which host it is.

Behaviour (pinned by ``tests/test_runtime_owner_loop.py``):

* acquisition fails closed: a fresh lease of another holder -- including one on another host, whose PID cannot be
  inspected here -- is never taken; a collision is audited; any error is ``RUNTIME_OWNERSHIP_ERROR:<type>``;
* without the lease the loop does not schedule a single tick;
* every iteration renews the lease first; a lost lease (another owner id) stops the loop at once, marks the
  watchdog ``RUNTIME_OWNERSHIP_LOST`` and audits it. A heartbeat that could not be WRITTEN is not a loss
  (``RuntimeOwnership.renew`` keeps ownership and lets the heartbeat age);
* a failing tick is recorded on the watchdog and the loop continues;
* stopping the loop does not release the lease: shutdown releases it explicitly (``release``).
"""
from __future__ import annotations

import asyncio
import logging
import time
import traceback
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

NOT_ACQUIRED = "NOT_ACQUIRED"
RUNTIME_OWNERSHIP_COLLISION = "RUNTIME_OWNERSHIP_COLLISION"
RUNTIME_OWNERSHIP_LOST = "RUNTIME_OWNERSHIP_LOST"

#: why ``run`` returned
REFUSED, LOST, STOPPED = "REFUSED", "LOST", "STOPPED"

_SLEEP_CHUNK_S = 0.5
_MIN_SLEEP_S = 0.5


def _watchdog():
    from app.ops import runtime_watchdog

    return runtime_watchdog.get_watchdog()


class RuntimeOwnerLoop:
    """Holds (or fails to hold) the runtime-ownership lease of one database and drives one tick per interval
    while it holds it."""

    def __init__(self, db: Any, *, runtime_session_id: Callable[[], Optional[str]] = lambda: None,
                 alert: Optional[Callable[[str, str], None]] = None,
                 ownership_factory: Optional[Callable[..., Any]] = None) -> None:
        self.db = db
        self._runtime_session_id = runtime_session_id
        self._alert = alert or self._audit_alert
        self._ownership_factory = ownership_factory
        self.ownership: Any = None
        self.ownership_reason: str = NOT_ACQUIRED
        self.runtime_owner_id: Optional[str] = None

    # ── the lease ────────────────────────────────────────────────────────────

    def acquire(self) -> bool:
        """Take the trading lease for this database. Fails closed."""
        if self.ownership is not None and self.ownership.is_owner:
            return True
        try:
            factory = self._ownership_factory
            if factory is None:
                from app.core.config import settings
                from app.ops.runtime_ownership import RuntimeOwnership

                def factory(db, *, runtime_session_id):
                    return RuntimeOwnership(db, database_path=db.path,
                                            database_role=str(getattr(settings, "DATABASE_ROLE", "production")),
                                            runtime_session_id=runtime_session_id)

            self.ownership = factory(self.db, runtime_session_id=self._runtime_session_id())
            result = self.ownership.acquire()
            self.ownership_reason = result.reason
            self.runtime_owner_id = result.runtime_owner_id
            if not result.acquired:
                self._alert(RUNTIME_OWNERSHIP_COLLISION,
                            f"another process holds the trading lease "
                            f"(pid={result.holder_pid} host={result.holder_hostname})")
            return result.acquired
        except Exception as exc:
            self.ownership_reason = f"RUNTIME_OWNERSHIP_ERROR:{type(exc).__name__}"
            logger.error("[RUNTIME_OWNERSHIP] acquire failed: %s", exc)
            return False

    @property
    def owns_runtime(self) -> bool:
        """True only while this process actually holds the lease (passing preflight is not owning)."""
        return bool(self.ownership is not None and self.ownership.is_owner)

    @property
    def ownership_decided(self) -> bool:
        """True once acquisition has been attempted, win or lose."""
        return self.ownership_reason != NOT_ACQUIRED

    def release(self, *, reason: str = "SHUTDOWN") -> None:
        if self.ownership is not None:
            self.ownership.release(reason=reason)

    def renew(self) -> bool:
        """Renew the lease. False -- with the loss reported -- when another process now holds it."""
        if self.ownership is None or self.ownership.renew():
            return True
        try:
            _watchdog().ownership_lost()
        except Exception as exc:
            logger.error("[RUNTIME_OWNERSHIP] watchdog update failed: %s", exc)
        self._alert(RUNTIME_OWNERSHIP_LOST, "scheduler lease lost to another process; stopping")
        logger.critical("[RUNTIME_OWNERSHIP] lease lost - stopping scheduler")
        return False

    # ── the loop ─────────────────────────────────────────────────────────────

    async def run(self, tick: Callable[[], Awaitable[Any]], *, interval_seconds: float,
                  keep_running: Callable[[], bool], after_tick: Optional[Callable[[], None]] = None) -> str:
        """Acquire, then renew + heartbeat + ``tick`` every ``interval_seconds`` while ``keep_running()``.

        Returns ``REFUSED`` (no lease), ``LOST`` (lease taken by another process) or ``STOPPED``."""
        if not self.acquire():
            logger.critical("[RUNTIME_OWNERSHIP] refusing to start scheduler: %s", self.ownership_reason)
            return REFUSED
        while keep_running():
            started = time.monotonic()
            if not self.renew():
                return LOST
            try:
                _watchdog().scheduler_heartbeat()
            except Exception:
                pass
            try:
                await tick()
                try:
                    _watchdog().iteration_succeeded()
                except Exception:
                    pass
            except Exception as exc:
                logger.critical("Runtime owner loop iteration error: %s", exc)
                try:
                    _watchdog().record_error(f"{type(exc).__name__}: {exc}")
                except Exception:
                    pass
                traceback.print_exc()
            if after_tick is not None:
                try:
                    after_tick()
                except Exception as exc:
                    logger.error("[RUNTIME_OWNER_LOOP] after-tick hook failed: %s", exc)
            remaining = max(_MIN_SLEEP_S, float(interval_seconds) - (time.monotonic() - started))
            slept = 0.0
            while slept < remaining and keep_running():   # chunked, so a stop is noticed within one chunk
                await asyncio.sleep(_SLEEP_CHUNK_S)
                slept += _SLEEP_CHUNK_S
        return STOPPED

    # ── evidence ─────────────────────────────────────────────────────────────

    def _audit_alert(self, event_type: str, message: str) -> None:
        """No silent recovery: every runtime-ownership fault leaves an event."""
        try:
            from shared_lib.persistence.audit import Audit

            Audit(self.db).event(event_type=event_type, run_id=self._runtime_session_id(), action="RUNTIME_ALERT",
                                 details={"message": message})
        except Exception as exc:
            logger.error("[RUNTIME_ALERT] %s: %s (audit failed: %s)", event_type, message, exc)


__all__ = ["RuntimeOwnerLoop", "NOT_ACQUIRED", "RUNTIME_OWNERSHIP_COLLISION", "RUNTIME_OWNERSHIP_LOST",
           "REFUSED", "LOST", "STOPPED"]
