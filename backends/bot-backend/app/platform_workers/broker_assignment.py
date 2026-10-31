"""Durable worker assignments for the production broker cycle (Section H, Step 5.2, gate EXT-WRK-01).

``production_runtime``'s broker cycle used to mutate every execution account under one guard: the canonical
runtime-owner lease. This module puts each account step of that cycle under the account's durable assignment
(``assignments.WorkerAssignments``) whenever a platform database is configured, so the ASSIGNMENT TABLE decides
which process may mutate an account -- in both rollout modes, therefore never two at once:

``BROKER_CYCLE_ASSIGNMENT_MODE=owner`` (default, today's compatibility path)
    The canonical runtime owner (the process holding the runtime-owner lease) registers as a worker and claims
    EVERY active account no other live worker holds (``claim(take_all_free=True)``); it never steals a live
    assignment. Without a platform database it behaves exactly as before this module existed (no layer).

``BROKER_CYCLE_ASSIGNMENT_MODE=assigned``
    Every trading-role process claims its fair share (``claim``) and processes only those accounts; the
    runtime-owner lease keeps only the singletons (collection schedule, forward observation, FX watcher, shadow
    scheduler, venue runtime). Without a platform database the mode refuses: no account work at all (fail closed,
    logged).

One account step (``run_account``): the claim is re-verified and renewed, the fence (account, worker, fencing
token) is set for the thread (``account_fence.active``), and -- when the assignment is NEW or a TAKEOVER
(``needs_recovery``) -- the step is RECOVERY ONLY (exchange truth reconciled, unknown submissions resolved,
protection verified and repaired; no entry). Only a complete recovery marks the assignment recovered.

Every round (``begin_round``) first makes ``platform_accounts`` match the execution accounts (a disconnected or
deleted account becomes REMOVED and is released, whoever held it), then claims. ``drain`` releases every
assignment with reason DRAIN, so a successor takes over at once instead of after one TTL. ``preempt`` is the
emergency flatten's: the account is taken with a higher fencing token whoever holds it.

An ``api``-role process never builds this layer, never registers and never claims (``app.ops.process_role``).
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid
from dataclasses import replace
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from .account_fence import AccountFence, active
from .assignments import DRAIN, EMERGENCY_DONE, REBALANCE, RENEWED, Assignment, AssignmentLost, WorkerAssignments

logger = logging.getLogger(__name__)

OWNER, ASSIGNED = "owner", "assigned"
MODES = (OWNER, ASSIGNED)
DEFAULT_TTL_MS = 60_000
DEFAULT_MARGIN_MS = 15_000
#: How long an emergency flatten holds a preempted account (its venue work: reads, a close, the read-back).
EMERGENCY_TTL_MS = 180_000

#: Refusal reasons of a layer that does no account work at all.
PLATFORM_DATABASE_REQUIRED = "ASSIGNED_MODE_REQUIRES_PLATFORM_DATABASE"
PLATFORM_DATABASE_UNAVAILABLE = "PLATFORM_DATABASE_UNAVAILABLE"
MODE_INVALID = "BROKER_CYCLE_ASSIGNMENT_MODE_INVALID"
FENCE_MARGIN_INVALID = "BROKER_ASSIGNMENT_FENCE_MARGIN_MUST_BE_BELOW_TTL"
POOL_SIZE_INVALID = "PLATFORM_DB_POOL_SIZE_INVALID"
RECOVERY_COMPLETE = "COMPLETE"


def recovered(result: Any) -> bool:
    """True when an account step's result reports a COMPLETE takeover recovery (``recover_account``)."""
    execution = result.get("execution") if isinstance(result, Mapping) else None
    return isinstance(execution, Mapping) and execution.get("assignment_recovery") == RECOVERY_COMPLETE


class BrokerCycleAssignments:
    """The assignment layer of ONE process's broker cycle. Thread-safe for the cycle thread plus an API thread
    (emergency flatten) and the shutdown path."""

    def __init__(self, platform_db: Any, *, mode: str = OWNER, ttl_ms: int = DEFAULT_TTL_MS,
                 margin_ms: int = DEFAULT_MARGIN_MS, worker_id: Optional[str] = None, build_version: str = "unknown",
                 worker_liveness_ms: Optional[int] = None, refused: Optional[str] = None) -> None:
        if mode not in MODES:
            raise ValueError(MODE_INVALID)
        self.mode = mode
        self.refused = refused
        self.margin_ms = int(margin_ms)
        self.build_version = build_version
        self.assignments: Optional[WorkerAssignments] = None
        if refused is None:
            if not 0 <= self.margin_ms < int(ttl_ms):
                raise ValueError("FENCE_MARGIN_MUST_BE_BELOW_TTL")
            self.assignments = WorkerAssignments(platform_db, ttl_ms=int(ttl_ms),
                                                 worker_liveness_ms=int(worker_liveness_ms or 2 * int(ttl_ms)))
        self._requested_worker_id = worker_id
        self.worker_id: Optional[str] = None
        self.owned: Dict[str, Assignment] = {}
        self._lock = threading.RLock()
        self._draining = False
        self._drained = False
        self._renewed_at = 0.0
        self._job_runner: Optional[Tuple[Any, Any]] = None
        self.last_round: Dict[str, Any] = {}
        #: Between two accounts of a round, at most this often: is an account free (an expired claim of a dead
        #: worker, a released one)? If so, claim it now and add it to the round in progress, instead of waiting
        #: for the next round. A sixth of the TTL (production: 10 s of a 60 s TTL).
        self.claim_poll_ms = max(1, int(ttl_ms) // 6)
        self._claim_polled_at = 0.0
        self._round: Optional[List[Mapping[str, Any]]] = None
        self._round_accounts: Dict[str, Mapping[str, Any]] = {}

    @classmethod
    def refusing(cls, mode: str, reason: str) -> "BrokerCycleAssignments":
        """A layer that does no account work at all (``sync`` checks ``refused``)."""
        return cls(None, mode=mode if mode in MODES else ASSIGNED, refused=reason)

    # ── state ──────────────────────────────────────────────────────────────────────────────────────────
    @property
    def enforced(self) -> bool:
        """The assignment table decides (a platform database is configured and usable)."""
        return self.refused is None and self.assignments is not None

    @property
    def draining(self) -> bool:
        return self._draining

    @property
    def ttl_ms(self) -> int:
        return self.assignments.ttl_ms if self.assignments is not None else 0

    def register(self) -> str:
        if self.worker_id is None:
            from app.ops import process_role
            if not process_role.runs_trading():
                raise ValueError("PROCESS_ROLE_API_CLAIMS_NO_ACCOUNT")
            wid = self._requested_worker_id or f"broker:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
            self.worker_id = self.assignments.register_worker(wid, role=f"broker-cycle-{self.mode}",
                                                              build_version=self.build_version)
        return self.worker_id

    def _fence(self, a: Assignment, *, requires_runtime_lease: Optional[bool] = None) -> AccountFence:
        return AccountFence(a.account_id, a.worker_id, int(a.fencing_token), self.assignments, self.margin_ms,
                            self.mode == OWNER if requires_runtime_lease is None else requires_runtime_lease)

    # ── a round ────────────────────────────────────────────────────────────────────────────────────────
    def begin_round(self, accounts: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        """Registry sync, then claim. Returns the accounts of ``accounts`` this worker owns, in their order.
        Raises when the platform database cannot be used (the caller does no account work)."""
        if not self.enforced:
            return []
        from app.ops.runtime_shutdown import stop_requested
        with self._lock:
            if self._draining:
                return []
            if stop_requested():
                self.drain()
                return []
            self.register()
            report = self.assignments.sync_registry(
                [(str(a["id"]), str(a["user_id"]), str(a["broker_id"]).lower(), str(a["environment"]).upper())
                 for a in accounts])
            claimed = self.assignments.claim(self.worker_id, take_all_free=self.mode == OWNER)
            self.owned = {a.account_id: a for a in claimed}
            self._renewed_at = self._claim_polled_at = time.monotonic()
            self.last_round = {"owned": len(self.owned), "recovering": sum(a.needs_recovery for a in claimed),
                               "removed": list(report["removed"]), "refused": list(report["refused"]),
                               "takeovers": [a.account_id for a in claimed if a.kind == "TAKEOVER"],
                               "claimed_mid_round": []}
            if report["refused"]:
                logger.error("[BROKER_ASSIGNMENT] accounts refused by the registry (tenant changed): %s",
                             report["refused"])
            # The round's list GROWS: ``keepalive`` (called before each account step) appends accounts claimed
            # between two accounts, so the loop iterating this list processes them -- recovery first -- in this round.
            self._round_accounts = {str(a["id"]): a for a in accounts}
            self._round = [a for a in accounts if str(a["id"]) in self.owned]
            return self._round

    def _claim_between_accounts(self) -> None:
        """Claim free accounts (an expired claim of a dead worker, a released one) at a safe point between two
        account steps, and append them to the round in progress. Fencing is unchanged: ``claim`` takes only rows
        that are released or expired on the database clock, under the row's own condition, with a new fencing
        token; the new assignment needs recovery before any entry. Opportunistic: a failure here is logged and
        the next round claims as before -- it never fails the account step that follows."""
        from app.ops.runtime_shutdown import stop_requested
        with self._lock:
            if self._draining or self._round is None or stop_requested():
                return
            try:
                if not self.assignments.has_free():
                    return
                claimed = self.assignments.claim(self.worker_id, take_all_free=self.mode == OWNER)
            except Exception as exc:  # noqa: BLE001 -- the round goes on with what it holds
                logger.warning("[BROKER_ASSIGNMENT] claim between accounts failed (%s)", type(exc).__name__)
                return
            self._renewed_at = time.monotonic()           # claim renewed every live claim too
            held = {a.account_id for a in claimed}
            for lost in set(self.owned) - held:
                self.owned.pop(lost, None)
            queued = {str(a["id"]) for a in self._round}
            for a in claimed:
                current = self.owned.get(a.account_id)
                if current is not None and int(current.fencing_token) == int(a.fencing_token):
                    continue                                # already held: keep the round's own record of it
                self.owned[a.account_id] = a
                account = self._round_accounts.get(a.account_id)
                if account is not None and a.account_id not in queued:
                    self._round.append(account)
                    queued.add(a.account_id)
                self.last_round.setdefault("claimed_mid_round", []).append(a.account_id)
                if a.kind == "TAKEOVER":
                    self.last_round.setdefault("takeovers", []).append(a.account_id)
                logger.info("[BROKER_ASSIGNMENT] %s claimed between accounts (%s, token %d)", a.account_id, a.kind,
                            int(a.fencing_token))

    def keepalive(self) -> None:
        """Renew every owned claim once a third of the TTL has passed since the last renewal (a long round must
        not let the claims of its last accounts expire before their turn), and -- at most every ``claim_poll_ms``
        -- claim accounts that became free since the round began (``_claim_between_accounts``): a dead worker's
        accounts are taken over once its claims expire, not at the start of the next round."""
        if not self.enforced or self.worker_id is None:
            return
        if self._round is not None and time.monotonic() - self._claim_polled_at >= self.claim_poll_ms / 1000.0:
            self._claim_polled_at = time.monotonic()
            self._claim_between_accounts()
        if not self.owned:
            return
        if time.monotonic() - self._renewed_at < self.ttl_ms / 3000.0:
            return
        with self._lock:
            still = set(self.assignments.renew(self.worker_id))
            self._renewed_at = time.monotonic()
            for lost in set(self.owned) - still:
                self.owned.pop(lost, None)

    def run_account(self, account: Mapping[str, Any], step: Callable[[bool], Any]) -> Any:
        """Run ``step(recovery_only)`` for one owned account under its fence. Raises ``AssignmentLost`` (before
        any work) when the account is no longer this worker's."""
        account_id = str(account["id"])
        a = self.owned.get(account_id)
        if a is None or self._draining:
            raise AssignmentLost(account_id, "NOT_ASSIGNED_TO_THIS_WORKER")
        fence = self._fence(a)
        fence.verify()
        with active(fence):
            result = step(bool(a.needs_recovery))
        if a.needs_recovery:
            if recovered(result):
                try:
                    self.assignments.mark_recovered(a)
                    self.owned[account_id] = replace(a, needs_recovery=False, kind=RENEWED)
                except AssignmentLost:
                    raise
                except Exception as exc:  # noqa: BLE001 -- recovered but not recorded: recovery runs again
                    logger.warning("[BROKER_ASSIGNMENT] %s recovered, not recorded (%s)", account_id, type(exc).__name__)
        else:
            try:
                self.assignments.mark_success(a)
            except AssignmentLost:
                raise
            except Exception as exc:  # noqa: BLE001 -- bookkeeping only; the step's own state is already saved
                logger.warning("[BROKER_ASSIGNMENT] %s success not recorded (%s)", account_id, type(exc).__name__)
        return result

    def end_round(self) -> List[str]:
        """``assigned`` mode, at the end of a round (a safe point): release the accounts above the fair share
        (another worker joined). The successor claims them as NEW and recovers first."""
        if not self.enforced or self.mode != ASSIGNED or self._draining or self.worker_id is None:
            return []
        with self._lock:
            surplus = self.assignments.surplus(self.worker_id)
            if surplus:
                self.assignments.release(self.worker_id, surplus, reason=REBALANCE)
                for account_id in surplus:
                    self.owned.pop(account_id, None)
            return surplus

    def drain(self) -> int:
        """Graceful stop: no more claims, every assignment released with reason DRAIN (a successor takes over at
        once), the worker STOPPED. Idempotent. An account step still in flight on another thread finds its fence
        released at its next check and stops before its next mutation."""
        with self._lock:
            self._draining = True
            self.stop_jobs()
            if not self.enforced or self.worker_id is None or self._drained:
                return 0
            self.assignments.set_worker_state(self.worker_id, "DRAINING")
            released = self.assignments.release(self.worker_id, reason=DRAIN)
            self.assignments.set_worker_state(self.worker_id, "STOPPED")
            self.owned.clear()
            self._drained = True
            logger.info("[BROKER_ASSIGNMENT] worker %s drained: %d assignment(s) released", self.worker_id, released)
            return released

    # ── emergency ──────────────────────────────────────────────────────────────────────────────────────
    def preempt(self, account: Mapping[str, Any], request_id: str) -> Assignment:
        """Take ``account`` for an emergency flatten, whoever holds it. Raises when the platform database cannot
        record it (the flatten then reports that account failed: exclusivity could not be established)."""
        a = self.assignments.preempt(str(account["id"]), f"emergency:{request_id}", "EMERGENCY_FLATTEN",
                                     ttl_ms=max(EMERGENCY_TTL_MS, self.ttl_ms), tenant_id=str(account["user_id"]),
                                     venue=str(account.get("broker_id", "unknown")).lower(),
                                     environment=str(account.get("environment", "unknown")).upper())
        with self._lock:
            self.owned.pop(str(account["id"]), None)
        return a

    def emergency_fence(self, a: Assignment) -> AccountFence:
        """The flatten runs under the canonical runtime lease AND its preemptive assignment."""
        return self._fence(a, requires_runtime_lease=True)

    def release_emergency(self, a: Assignment) -> bool:
        return self.assignments.release_assignment(a, reason=EMERGENCY_DONE)

    # ── for durable job handlers ───────────────────────────────────────────────────────────────────────
    def fence_for(self, account_id: Any) -> Optional[Tuple[str, str, int]]:
        """The fence token of an account this worker holds and has recovered, else None (a job never acts on an
        account before its takeover recovery)."""
        if not self.enforced or self._draining:
            return None
        a = self.owned.get(str(account_id))
        if a is None or a.needs_recovery:
            return None
        return (a.account_id, a.worker_id, int(a.fencing_token))

    def fence_from_token(self, token: Sequence[Any]) -> AccountFence:
        a = self.owned.get(str(token[0]))
        if a is None or (a.worker_id, int(a.fencing_token)) != (str(token[1]), int(token[2])):
            raise AssignmentLost(str(token[0]), "FENCE_TOKEN_NOT_HELD")
        return self._fence(a)

    def job_fence(self, live: Callable[[str], Optional[tuple]]) -> Callable[[str], Optional[tuple]]:
        """``fence_for_account`` of the durable jobs: ``live`` (``runtime_jobs.assignment_fence`` -- the claim read
        from the platform database now) AND this worker has recovered the account (``fence_for``): a job never
        acts on an account before its takeover recovery."""
        def fence(account_id: str) -> Optional[tuple]:
            mine = self.fence_for(account_id)
            current = live(account_id) if mine is not None else None
            return current if current is not None and tuple(current) == tuple(mine) else None
        return fence

    def job_runner(self, legacy_db: Any) -> Any:
        """``runtime_jobs.RuntimeJobRunner`` for this worker, or None when that module is not installed."""
        if not self.enforced or self.worker_id is None:
            return None
        try:
            from .runtime_jobs import RuntimeJobRunner, assignment_fence
        except ImportError:
            return None
        key = (id(legacy_db), self.worker_id)
        if self._job_runner is None or self._job_runner[0] != key:
            fence = self.job_fence(assignment_fence(self.assignments, self.worker_id))
            self._job_runner = (key, RuntimeJobRunner(legacy_db, self.assignments.db, worker_id=self.worker_id,
                                                      fence_for_account=fence))
        return self._job_runner[1]

    def owned_accounts(self) -> List[Tuple[str, str]]:
        """(account_id, tenant_id) of every account this worker holds, for ``RuntimeJobRunner.schedule``."""
        return [(a.account_id, a.tenant_id) for a in list(self.owned.values())]

    def stop_jobs(self) -> None:
        if self._job_runner is not None:
            try:
                self._job_runner[1].request_stop()
            except Exception:  # noqa: BLE001
                logger.warning("[BROKER_ASSIGNMENT] job runner stop failed", exc_info=True)

    def describe(self) -> Dict[str, Any]:
        return {"mode": self.mode, "enforced": self.enforced, "refused": self.refused, "worker_id": self.worker_id,
                "owned": len(self.owned), "draining": self._draining, "ttl_ms": self.ttl_ms,
                "margin_ms": self.margin_ms, "last_round": dict(self.last_round)}


# ── the process's layer ────────────────────────────────────────────────────────────────────────────────────
_installed: Optional[BrokerCycleAssignments] = None
_install_lock = threading.Lock()


def installed() -> Optional[BrokerCycleAssignments]:
    """This process's layer, or None (no platform database in ``owner`` mode, or the runtime was not started)."""
    return _installed


def install(layer: Optional[BrokerCycleAssignments]) -> Optional[BrokerCycleAssignments]:
    global _installed
    with _install_lock:
        _installed = layer
    return layer


def mode_setting(settings_obj: Any = None) -> str:
    if settings_obj is None:
        from app.core.config import settings as settings_obj
    value = getattr(settings_obj, "BROKER_CYCLE_ASSIGNMENT_MODE", None) or os.environ.get(
        "BROKER_CYCLE_ASSIGNMENT_MODE") or OWNER
    return str(value).strip().lower()


def configure(*, env: Optional[Mapping[str, str]] = None, settings_obj: Any = None,
              build_version: str = "unknown") -> Optional[BrokerCycleAssignments]:
    """Build and install this process's layer at runtime start (``production_runtime.run``).

    * ``api`` role: nothing (it runs no broker cycle and claims nothing).
    * no ``PLATFORM_DATABASE_URL``: ``owner`` -> None (today's behaviour); ``assigned`` -> a refusing layer.
    * an invalid mode, or a platform database that cannot be opened: a refusing layer (fail closed).
    """
    from app.ops import process_role
    if not process_role.runs_trading():
        return install(None)
    if settings_obj is None:
        from app.core.config import settings as settings_obj
    env = os.environ if env is None else env
    mode = mode_setting(settings_obj)
    if mode not in MODES:
        logger.error("[BROKER_ASSIGNMENT] BROKER_CYCLE_ASSIGNMENT_MODE=%r is not one of %s: no account work", mode, MODES)
        return install(BrokerCycleAssignments.refusing(ASSIGNED, MODE_INVALID))
    url = env.get("PLATFORM_DATABASE_URL")
    if not url:
        if mode == OWNER:
            return install(None)
        logger.error("[BROKER_ASSIGNMENT] assigned mode without PLATFORM_DATABASE_URL: this process does no account work")
        return install(BrokerCycleAssignments.refusing(ASSIGNED, PLATFORM_DATABASE_REQUIRED))
    ttl = int(getattr(settings_obj, "BROKER_ASSIGNMENT_TTL_MS", DEFAULT_TTL_MS) or DEFAULT_TTL_MS)
    margin = int(getattr(settings_obj, "BROKER_ASSIGNMENT_FENCE_MARGIN_MS", DEFAULT_MARGIN_MS) or DEFAULT_MARGIN_MS)
    if not 0 <= margin < ttl:
        logger.error("[BROKER_ASSIGNMENT] fence margin %d ms is not below the %d ms assignment TTL: no account work",
                     margin, ttl)
        return install(BrokerCycleAssignments.refusing(mode, FENCE_MARGIN_INVALID))
    try:
        pool_size = int(env.get("PLATFORM_DB_POOL_SIZE", 4))
        if not 1 <= pool_size <= 64:
            raise ValueError(pool_size)
    except (TypeError, ValueError):
        logger.error("[BROKER_ASSIGNMENT] PLATFORM_DB_POOL_SIZE must be an integer from 1 to 64: no account work")
        return install(BrokerCycleAssignments.refusing(mode, POOL_SIZE_INVALID))
    try:
        from shared_lib.persistence.sql import Database
        layer = BrokerCycleAssignments(Database(url, pool_size=pool_size), mode=mode,
                                       ttl_ms=ttl, margin_ms=margin, build_version=build_version)
    except Exception as exc:  # noqa: BLE001 -- configured but unusable: the assignment table cannot decide
        logger.error("[BROKER_ASSIGNMENT] platform database unusable (%s): no account work", type(exc).__name__)
        return install(BrokerCycleAssignments.refusing(mode, PLATFORM_DATABASE_UNAVAILABLE))
    logger.info("[BROKER_ASSIGNMENT] broker cycle runs under durable account assignments (mode=%s ttl_ms=%d margin_ms=%d)",
                mode, ttl, margin)
    return install(layer)


__all__ = ["ASSIGNED", "BrokerCycleAssignments", "EMERGENCY_TTL_MS", "MODES", "OWNER", "configure", "install",
           "installed", "mode_setting", "recovered"]
