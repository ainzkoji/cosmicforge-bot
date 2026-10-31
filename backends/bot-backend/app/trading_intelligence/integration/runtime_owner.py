"""The runtime owner of this process, and the one place its readers ask about it (Section I Batch 7, E-05).

``RuntimeOwnerLoop`` (``runtime_owner_loop.py``) holds the ``trading_scheduler`` lease. Something has to run
that loop for the life of the process -- the *host* -- and several readers ask the host about it:

* ``app.main``: the background-job gate (``_may_run_background_jobs``), ``/runner/status`` and ``/health``;
* ``app.api.bot_instances``: ``runner_policy_hash`` on the effective-policy route;
* ``app.ops.runtime_supervisor``: the liveness observation;
* ``app.ops.runtime_shutdown``: stop the scheduler, drain, release the lease.

The host is chosen by ``app.main.runner_loop``:

* **production** -- ``RuntimeOwnerHost`` below: the lease loop and nothing else. The legacy ``MultiBotRunner``
  is not built (its ``run_once`` returned at once there; only the CATI production runtime trades, as its own
  task gated on the persisted lease);
* **outside production** -- ``app.runner.multi_runner.MultiBotRunner``, which runs the V2 ``PaperRunner`` per
  bot (the non-production simulation) and delegates the same lease to its own ``RuntimeOwnerLoop``.

The readers use only the functions of this module, so they do not depend on which host it is. A host is
read through its ``owner_loop`` when it has one; a host without one is read through the ownership surface
``MultiBotRunner`` has always exposed (``owns_runtime``, ``ownership_decided``, ``ownership_reason``,
``_ownership``, ``release_runtime_ownership``, ``running``, ``_stop_requested``, ``_runners``, ``iteration``).

Nothing here changes a lease rule: acquisition, renewal, loss and release are ``RuntimeOwnerLoop`` over
``app.ops.runtime_ownership.RuntimeOwnership``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from app.trading_intelligence.integration.runtime_owner_loop import LOST, REFUSED, RuntimeOwnerLoop

logger = logging.getLogger(__name__)


class RuntimeOwnerHost:
    """The production runtime owner: hold the lease, renew it and beat the heartbeat every interval.

    Its tick only counts iterations: in production nothing is scheduled by this loop (the CATI production
    runtime, the shadow scheduler and the venue runtime are separate tasks that each check the lease)."""

    def __init__(self, db: Any, *, runtime_session_id: Callable[[], Optional[str]] = lambda: None,
                 alert: Optional[Callable[[str, str], None]] = None,
                 ownership_factory: Optional[Callable[..., Any]] = None) -> None:
        self.owner_loop = RuntimeOwnerLoop(db, runtime_session_id=runtime_session_id, alert=alert,
                                           ownership_factory=ownership_factory)
        self.running = False
        self.stop_requested = False
        self.iteration = 0

    def stop(self) -> None:
        """Stop at the loop's next check (within one sleep chunk). The lease is kept until ``release``."""
        self.stop_requested = True
        self.running = False

    def release(self, *, reason: str = "SHUTDOWN") -> None:
        self.owner_loop.release(reason=reason)

    async def _tick(self) -> None:
        self.iteration += 1

    async def run_loop(self, interval_seconds: float = 10) -> str:
        """Acquire, then renew + heartbeat every ``interval_seconds`` until stopped or the lease is gone."""
        self.running = True
        logger.info("CATI runtime ownership loop started (production host, no legacy runner)")
        outcome = await self.owner_loop.run(self._tick, interval_seconds=interval_seconds,
                                            keep_running=lambda: self.running and not self.stop_requested)
        if outcome in (REFUSED, LOST):
            self.running = False
        logger.info("CATI runtime ownership loop stopped: %s", outcome)
        return outcome


# ── the readers' interface ──────────────────────────────────────────────────


@dataclass(frozen=True)
class OwnershipView:
    """What a reader may know about this process's runtime ownership."""

    owns_runtime: bool = False
    decided: bool = False
    reason: Optional[str] = None
    lease_pid: Optional[int] = None
    renew_failures: int = 0


def host_of(service: Any) -> Any:
    """The runtime-owner host registered on ``app.main.runner_service`` (``runtime_owner``), or None.

    A service object without that slot (one made before it existed) is read through ``multi_runner``."""
    if service is None:
        return None
    host = getattr(service, "runtime_owner", None)
    return host if host is not None else getattr(service, "multi_runner", None)


def owner_loop_of(host: Any) -> Optional[RuntimeOwnerLoop]:
    owner = getattr(host, "owner_loop", None) if host is not None else None
    return owner if isinstance(owner, RuntimeOwnerLoop) else None


def ownership(host: Any) -> OwnershipView:
    if host is None:
        return OwnershipView()
    loop = owner_loop_of(host)
    if loop is not None:
        owns, decided, reason, held = loop.owns_runtime, loop.ownership_decided, loop.ownership_reason, loop.ownership
    else:
        owns = getattr(host, "owns_runtime", False)
        decided = getattr(host, "ownership_decided", False)
        reason = getattr(host, "ownership_reason", None)
        held = getattr(host, "_ownership", None)
    return OwnershipView(owns_runtime=bool(owns), decided=bool(decided), reason=reason,
                         lease_pid=getattr(held, "pid", None), renew_failures=getattr(held, "renew_failures", 0))


def scheduler_running(host: Any) -> bool:
    return bool(getattr(host, "running", False))


def iteration(host: Any) -> int:
    return getattr(host, "iteration", 0) if host is not None else 0


def stop_scheduler(host: Any) -> None:
    """Ask the lease loop to stop at its next check. Raises if the host cannot be told."""
    stop = getattr(host, "stop", None)
    if callable(stop):
        stop()
        return
    host.running = False
    host._stop_requested = True


def release(host: Any, *, reason: str) -> None:
    """Release the lease this host holds (a no-op if it holds none). Raises on failure."""
    loop = owner_loop_of(host)
    if loop is not None:
        loop.release(reason=reason)
    else:
        host.release_runtime_ownership(reason=reason)


def simulation_runners(host: Any) -> Mapping[str, Any]:
    """The cached per-bot V2 runners of the non-production simulation (none in production)."""
    return dict(getattr(host, "_runners", None) or {}) if host is not None else {}


def simulation_runner(host: Any, bot_id: str) -> Any:
    return simulation_runners(host).get(bot_id)


__all__ = ["RuntimeOwnerHost", "OwnershipView", "host_of", "owner_loop_of", "ownership", "scheduler_running",
           "iteration", "stop_scheduler", "release", "simulation_runners", "simulation_runner"]
