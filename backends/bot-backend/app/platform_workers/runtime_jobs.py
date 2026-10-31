"""The durable job queue wired into the trading runtime (Section H, Step 5.3 integration).

``jobs.py`` is the queue (at least once, idempotency keys, lease tokens, dead letters). This module is what the
trading runtime runs through it: one handler per job category, the producers that enqueue work with stable
idempotency keys, and ``RuntimeJobRunner`` -- the single registration path used by the production trading loop
and by the standalone ``TradingWorker`` process alike.

Delivery stays at least once. Every business effect is made exactly once by **durable state that the handler
reads before acting**, never by trusting the queue:

=================  ==========================================  ===========================  ===========================================
category           what it does                                idempotency key              what makes the effect exactly once
=================  ==========================================  ===========================  ===========================================
ORDER              drives a CATI execution attempt left        ``order-resolution:          the attempt history (``cati_execution_
                   SUBMIT_UNKNOWN / PENDING_SUBMIT to a        {account}:{attempt_id}``     attempts``, append-only, deterministic record
                   terminal state from BROKER truth: the                                    ids) and the reservation's compare-and-set;
                   boundary's own ``reconcile_submit_unknown``                              a terminal attempt is never sent to the venue
                   on a boundary built exactly as the                                       again. The handler has no path to the venue's
                   production cycle builds it. **Never                                      order-create call: a payload asking for one
                   submits an order**                                                       is a poison message (QUEUE_ORDER_SUBMISSION_
                                                                                            NOT_PERMITTED)
RECONCILIATION     the production account maintenance          ``reconciliation:{account}:  ``platform_job_watermarks``: a request older
                   (resolve unknown submits, protection        {reason}:{requested_at}``    than the start of the last completed
                   truth, fail-safe / horizon close of                                      reconciliation is SUPERSEDED (no venue call);
                   existing lineage) for one account                                        the watermark only rises
NOTIFICATION       ops alert -> ``alerting.notification_job_   ``ops-alert-delivery:        ops alert: the queue's job state (one job per
                   handler``; customer event -> ONE row of     {delivery_id}`` /            engine delivery). Customer event: the
                   the existing ``notification_outbox`` via    ``notification:outbox:       outbox row's status, claimed before the send
                   ``user_events.deliver_one`` (same claim,    {outbox_id}``                (``user_events.DELIVERY_CLAIM_MS``) so the
                   senders, attempts and backoff as the                                     notification worker and a job never both send
                   notification worker; no second outbox)                                   it
SHADOW_COMPARISON  the daily model-vs-frozen-evaluator         ``shadow-comparison:         ``cati_shadow_comparisons``: rows are keyed by
                   comparison of one UTC day, by the           {family}:{kind}:{day}``      content hash (``shadow_compare.record``) and
                   pipeline's own ``compare_models``                                        a day already recorded for every model book is
                                                                                            not recomputed
OPERATIONAL_RETRY  EXPIRE_STALE_RESERVATIONS, RECOVER_PENDING  ``operational-retry:{task}:  each task is itself idempotent on durable
                   (the boundary's ``recover_pending`` with    {account}:{bucket}``         state (compare-and-set expiry; the
                   its escalation), VERIFY_PROTECTION                                       boundary's reconciliation; the watermark)
=================  ==========================================  ===========================  ===========================================

Ownership and gates:

* A handler that acts for an account first asks ``fence_for_account``; an account this worker does not own is
  ``RetryLater`` **without spending an attempt**. The fence is verified again on the platform database
  (``WorkerAssignments.verify_on``) and the job's own lease is checked (``JobQueue.heartbeat``) immediately
  before any venue call or account mutation.
* Broker-touching handlers pass ``RuntimeServices.broker_gate`` (production profile, the canonical runtime lease,
  no shutdown, no pending emergency flatten for the account) and run inside ``broker_section`` -- the same
  ``production_runtime`` cycle lock the broker cycle and the operator emergency flatten take, so a job never
  interleaves with either.
* A runner is refused in an ``api``-role process (``process_role.runs_trading()`` false): no handler in this
  module ever runs in a process that serves only HTTP.
* No emergency category was added. An emergency flatten stays on its existing path (kill switch +
  ``EmergencyControl`` + ``PreemptionGate`` under the cycle lock); queued work yields to it.

Drain: ``request_stop`` makes the runner start no further job. A job claimed in the current batch but not yet
started is handed back at once (due now, no attempt spent); a job whose handler is running finishes. A job that
was running when the process died stays CLAIMED until its lease expires and is then claimed by any worker.
"""
from __future__ import annotations

import logging
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from shared_lib.persistence.sql import Database

from .assignments import AssignmentLost, WorkerAssignments
from .jobs import (DEFAULT_JOB_LEASE_MS, NOTIFICATION, OPERATIONAL_RETRY, ORDER, RECONCILIATION, SHADOW_COMPARISON,
                   Handler, Job, JobProcessor, JobQueue, PermanentJobError, RetryLater)

logger = logging.getLogger(__name__)

CATEGORIES = (ORDER, RECONCILIATION, NOTIFICATION, SHADOW_COMPARISON, OPERATIONAL_RETRY)

#: ORDER: the one operation an ORDER job may ask for.
OP_RESOLVE_ATTEMPT = "RESOLVE_EXECUTION_ATTEMPT"
QUEUE_ORDER_SUBMISSION_NOT_PERMITTED = "QUEUE_ORDER_SUBMISSION_NOT_PERMITTED"
#: Payload fields (or ``op`` values) that would describe a NEW order. New entries are submitted only inline, by
#: ``CATIExecutionBoundary`` under the account fence and all its gates; the queue never carries one.
SUBMISSION_FIELDS = frozenset({"intent_key", "side", "quantity", "qty", "notional", "limit_price", "price",
                               "order_type", "client_order_id", "trade_plan", "entry", "submit"})
SUBMISSION_OPS = frozenset({"SUBMIT", "SUBMIT_ORDER", "SUBMIT_ENTRY", "CREATE", "CREATE_ORDER", "PLACE_ORDER",
                            "NEW_ORDER", "ENTRY"})
UNRESOLVED_ATTEMPT_STATES = ("SUBMIT_UNKNOWN", "PENDING_SUBMIT")

#: NOTIFICATION kinds.
KIND_OPS_ALERT = "ops_alert"            # produced by ``app.ops.alerting.JobQueueSink``
KIND_USER_EVENT = "user_event_outbox"   # one ``notification_outbox`` row

#: SHADOW_COMPARISON: the comparison kinds and the families that have one.
SHADOW_KIND_DAILY = "DAILY_FROZEN_EVALUATOR"
SHADOW_FAMILY = "DAILY_TREND"
#: A day whose shadow cycle has not completed this long after its bar closed is dead-lettered (operator review)
#: instead of waiting for ever.
SHADOW_CYCLE_WAIT_BOUND_MS = 3 * 86_400_000

#: OPERATIONAL_RETRY tasks.
TASK_EXPIRE_RESERVATIONS = "EXPIRE_STALE_RESERVATIONS"
TASK_RECOVER_PENDING = "RECOVER_PENDING"
TASK_VERIFY_PROTECTION = "VERIFY_PROTECTION"
OPERATIONAL_TASKS = (TASK_EXPIRE_RESERVATIONS, TASK_RECOVER_PENDING, TASK_VERIFY_PROTECTION)
OPERATIONAL_PERIOD_MS = 300_000

RECONCILIATION_SCOPE = "RECONCILIATION"

PRIORITY = {ORDER: 50, RECONCILIATION: 40, OPERATIONAL_RETRY: 30, NOTIFICATION: 5, SHADOW_COMPARISON: 0}
NOT_OWNED_RETRY_MS = 5_000
STILL_UNKNOWN_RETRY_MS = 30_000
DEFAULT_MAX_PROCESSING_MS = 5 * 60_000

_KEY_SAFE = re.compile(r"[^A-Za-z0-9_.@-]")


class RuntimeJobsRefused(RuntimeError):
    code = "RUNTIME_JOBS_REFUSED_IN_API_ROLE"


def _safe(part: Any) -> str:
    return _KEY_SAFE.sub("_", str(part))[:128]


def _require_trading_process() -> None:
    from app.ops import process_role
    if not process_role.runs_trading():
        raise RuntimeJobsRefused(f"{RuntimeJobsRefused.code}: PROCESS_ROLE={process_role.role()} serves HTTP only; "
                                 "trading jobs run in a trading (or all) process")


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# How a handler reaches the trading runtime
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class AccountRuntime:
    """One account as the production cycle sees it: its ``broker_accounts`` row and its broker client."""
    account: Dict[str, Any]
    client: Any


@dataclass
class ShadowSource:
    """Where the shadow comparison gets its family registry, market data and model accounts."""
    registry: Any
    data: Mapping[str, Any]
    accounts: Any


class RuntimeServices:
    """The production constructions every handler uses. Each method is the same construction the production
    cycle uses (``production_runtime`` / ``production_execution``); a test overrides methods to put a fake
    venue behind the real boundary."""

    broker_lock_timeout_s = 5.0

    def __init__(self, legacy_db: Any) -> None:
        self.legacy_db = legacy_db
        self._shadow: Optional[ShadowSource] = None

    def clock(self) -> int:
        return int(time.time() * 1000)

    # -- gates ---------------------------------------------------------------------------------------
    def broker_gate(self, account_id: Optional[str]) -> None:
        """Raise ``RetryLater`` when this process may not act at a venue for ``account_id`` now."""
        from app.core.config import settings
        if not settings.production:
            raise RetryLater("CATI_PRODUCTION_PROFILE_REQUIRED", 300_000)
        from app.trading_intelligence.integration.residual_prospective import owner_current
        if not owner_current(self.legacy_db):
            raise RetryLater("CANONICAL_RUNTIME_LEASE_REQUIRED", 30_000)
        from app.ops.runtime_shutdown import stop_requested
        if stop_requested():
            raise RetryLater("RUNTIME_SHUTDOWN_IN_PROGRESS", 30_000)
        from app.trading_intelligence.integration import production_runtime
        if production_runtime._preemption.covers(account_id):
            raise RetryLater("EMERGENCY_FLATTEN_PENDING", 10_000)

    @contextmanager
    def broker_section(self, account_id: Optional[str]):
        """The broker-cycle lock (shared with the broker cycle and the operator emergency flatten)."""
        from app.trading_intelligence.integration import production_runtime
        lock = production_runtime._cycle_lock
        if not lock.acquire(timeout=self.broker_lock_timeout_s):
            raise RetryLater("BROKER_CYCLE_BUSY", 5_000)
        try:
            if production_runtime._preemption.covers(account_id):
                raise RetryLater("EMERGENCY_FLATTEN_PENDING", 10_000)
            yield
        finally:
            lock.release()

    def shadow_gate(self) -> None:
        """The shadow comparison writes the runtime's research tables: only the runtime-lease owner does."""
        from app.trading_intelligence.integration.residual_prospective import owner_current
        if not owner_current(self.legacy_db):
            raise RetryLater("CANONICAL_RUNTIME_LEASE_REQUIRED", 60_000)

    # -- the account, its client and its boundary ----------------------------------------------------
    def account_runtime(self, account_id: str) -> AccountRuntime:
        from shared_lib.broker.environment import normalize_environment
        from shared_lib.broker.resolver import resolve_broker_auth
        from app.trading_intelligence.integration import production_runtime
        found = [a for a in production_runtime.execution_accounts(self.legacy_db) if a["id"] == account_id]
        if not found:
            raise RetryLater("EXECUTION_ACCOUNT_NOT_CONNECTED", 300_000)
        account = found[0]
        if str(account["broker_id"]).lower() not in {"binance", "bybit", "bingx"}:
            raise RetryLater("EXECUTION_ADAPTER_UNVALIDATED", 3_600_000)
        auth = resolve_broker_auth(account["id"], account["user_id"], self.legacy_db)
        env = normalize_environment(auth.environment)
        if normalize_environment(account["environment"]) != env:
            raise RetryLater("BROKER_ENVIRONMENT_MISMATCH", 300_000)
        account = {**account, "environment": env.value.upper()}
        client = production_runtime.account_client(self.legacy_db, account["id"], auth)
        client._production_credential_version = getattr(auth, "credential_version", None)
        return AccountRuntime(account, client)

    def boundary(self, runtime: AccountRuntime, bot_id: str) -> Any:
        from app.trading_intelligence.integration.production_execution import boundary_for
        return boundary_for(self.legacy_db, runtime.account, {"id": bot_id}, runtime.client)

    def reconcile_account(self, runtime: AccountRuntime, now_ms: int) -> Any:
        """Production account maintenance of existing lineage (``production_execution.maintain_only``)."""
        from app.trading_intelligence.integration.production_execution import maintain_only
        return maintain_only(self.legacy_db, runtime.account, runtime.client, now_ms=now_ms)

    # -- notifications -------------------------------------------------------------------------------
    def ops_channels(self) -> Any:
        from app.ops.alerting import NotifyChannelsSink
        return NotifyChannelsSink()

    def notification_sender(self) -> Callable[..., bool]:
        from app.observability import user_events
        return user_events._send

    # -- shadow comparison ---------------------------------------------------------------------------
    def shadow_source(self) -> ShadowSource:
        if self._shadow is None:
            from app.market_data.daily_feed import default_daily_bars
            from app.trading_intelligence.families.daily_trend import spec
            from app.trading_intelligence.integration.platform_runtime import budgeted_archive_get
            from app.trading_intelligence.pipeline.accounts import ModelAccounts
            from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
            bars = default_daily_bars(self.legacy_db, http_get=budgeted_archive_get(self.legacy_db))
            self._shadow = ShadowSource(registry=FamilyRegistry(self.legacy_db), data={"1d": bars},
                                        accounts=ModelAccounts(initial_equity=spec.INTERPRETATION["initial_equity_usdt"]))
        return self._shadow


def assignment_fence(assignments: WorkerAssignments, worker_id: str) -> Callable[[str], Optional[tuple]]:
    """``fence_for_account`` from the durable assignments: ``(account, worker, fencing token)`` while this worker
    holds a live claim on the account, else None."""
    def fence(account_id: str) -> Optional[tuple]:
        owner = assignments.owner_of(account_id)
        if owner is None or owner[0] != worker_id:
            return None
        return (account_id, worker_id, int(owner[1]))
    return fence


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# Handler context
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
class RuntimeJobContext:
    def __init__(self, legacy_db: Any, platform_db: Database, *, worker_id: str,
                 fence_for_account: Callable[[str], Optional[tuple]], services: Optional[RuntimeServices] = None,
                 queue: Optional[JobQueue] = None) -> None:
        self.legacy_db = legacy_db
        self.platform_db = platform_db
        self.worker_id = worker_id
        self.fence_for_account = fence_for_account
        self.services = services or RuntimeServices(legacy_db)
        self.queue = queue or JobQueue(platform_db)

    def now(self) -> int:
        return int(self.services.clock())

    def fence(self, job: Job) -> tuple:
        """This worker's fence on the job's account; ``RetryLater`` (no attempt spent) when it does not own it."""
        if not job.account_id:
            raise PermanentJobError(f"{job.category}_JOB_WITHOUT_ACCOUNT")
        fence = self.fence_for_account(job.account_id)
        if not fence:
            raise RetryLater(f"ACCOUNT_NOT_OWNED_BY_WORKER:{job.account_id}", NOT_OWNED_RETRY_MS)
        fence = tuple(fence)
        if len(fence) != 3 or fence[0] != job.account_id:
            raise RetryLater(f"ACCOUNT_FENCE_INVALID:{job.account_id}", NOT_OWNED_RETRY_MS)
        return fence

    def verify_fence(self, fence: tuple) -> None:
        try:
            with self.platform_db.transaction() as conn:
                WorkerAssignments.verify_on(conn, *fence)
        except AssignmentLost as lost:
            raise RetryLater(f"ACCOUNT_ASSIGNMENT_LOST:{lost.reason}", NOT_OWNED_RETRY_MS) from lost

    def still_leased(self, job: Job) -> None:
        """Raises ``StaleJobLease`` when another worker took the job over: checked right before a side effect."""
        self.queue.heartbeat(job)

    @contextmanager
    def at_venue(self, job: Job, fence: tuple):
        """Everything a venue-touching step needs, in order: gate, cycle lock, fence, own lease."""
        self.services.broker_gate(fence[0])
        with self.services.broker_section(fence[0]):
            self.verify_fence(fence)
            self.still_leased(job)
            yield

    # -- watermarks ------------------------------------------------------------------------------------
    def watermark(self, scope: str, account_id: str) -> Optional[int]:
        with self.platform_db.transaction(write=False) as conn:
            value = conn.scalar("SELECT watermark FROM platform_job_watermarks WHERE scope=? AND account_id=?",
                                (scope, account_id))
        return None if value is None else int(value)

    def platform_now(self) -> int:
        with self.platform_db.transaction(write=False) as conn:
            return conn.now_ms()

    def advance_watermark(self, scope: str, job: Job, fence: tuple, value: int) -> int:
        """Raise the account's watermark to ``value`` (never lower it), under the fence. A fence lost after the
        work itself completed only means the watermark is not raised: the new owner reconciles again."""
        try:
            with self.platform_db.transaction() as conn:
                WorkerAssignments.verify_on(conn, *fence)
                conn.execute(
                    "INSERT INTO platform_job_watermarks (scope, account_id, tenant_id, watermark, job_id, updated_at) "
                    "VALUES (?,?,?,?,?,?) ON CONFLICT (scope, account_id) DO UPDATE SET watermark=excluded.watermark, "
                    "job_id=excluded.job_id, updated_at=excluded.updated_at "
                    "WHERE platform_job_watermarks.watermark < excluded.watermark",
                    (scope, job.account_id, job.tenant_id, int(value), job.job_id, conn.now_ms()))
                return int(conn.scalar("SELECT watermark FROM platform_job_watermarks WHERE scope=? AND account_id=?",
                                       (scope, job.account_id)))
        except AssignmentLost as lost:
            raise RetryLater(f"ACCOUNT_ASSIGNMENT_LOST:{lost.reason}", NOT_OWNED_RETRY_MS) from lost


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# ORDER -- resolve an unknown entry from broker truth; never submit
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def _refuse_submission(payload: Mapping[str, Any]) -> None:
    op = str(payload.get("op") or "").upper()
    asked = sorted(SUBMISSION_FIELDS & set(payload))
    if op in SUBMISSION_OPS or asked:
        raise PermanentJobError(f"{QUEUE_ORDER_SUBMISSION_NOT_PERMITTED}:op={op or '-'}:fields={','.join(asked) or '-'}")


def _text(payload: Mapping[str, Any], field: str, code: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PermanentJobError(f"{code}:{field}")
    return value


def _attempt_and_plan(ctx: RuntimeJobContext, account_id: str, trade_plan_id: str, attempt_id: str):
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    latest = ExecutionAttemptStore(ctx.legacy_db).latest(account_id, attempt_id)
    if latest is None:
        raise PermanentJobError(f"EXECUTION_ATTEMPT_NOT_FOUND:{attempt_id}")
    if latest["trade_plan_id"] != trade_plan_id:
        raise PermanentJobError("ORDER_JOB_ATTEMPT_PLAN_MISMATCH")
    try:
        plan = TradePlanEvidenceStore(ctx.legacy_db).load_plan(account_id, trade_plan_id)
    except Exception as exc:  # an integrity failure is not retryable
        raise PermanentJobError(f"TRADE_PLAN_EVIDENCE_INVALID:{type(exc).__name__}") from exc
    if plan is None:
        raise PermanentJobError(f"TRADE_PLAN_EVIDENCE_UNAVAILABLE:{trade_plan_id}")
    reservation = CATIReservationStore(ctx.legacy_db).get(plan.portfolio_reservation_id)
    return latest, plan, (reservation.status if reservation is not None else None)


def order_resolution_handler(ctx: RuntimeJobContext) -> Handler:
    def handle(job: Job) -> Dict[str, Any]:
        payload = job.payload
        _refuse_submission(payload)
        if str(payload.get("op") or "") != OP_RESOLVE_ATTEMPT:
            raise PermanentJobError(f"ORDER_JOB_UNSUPPORTED_OP:{payload.get('op')}")
        trade_plan_id = _text(payload, "trade_plan_id", "ORDER_JOB_MALFORMED")
        attempt_id = _text(payload, "execution_attempt_id", "ORDER_JOB_MALFORMED")
        fence = ctx.fence(job)
        account_id = fence[0]
        latest, plan, reservation = _attempt_and_plan(ctx, account_id, trade_plan_id, attempt_id)
        if latest["status"] not in UNRESOLVED_ATTEMPT_STATES:
            if reservation == "RESOLUTION_PENDING":
                # The attempt is terminal but its reservation was never resolved (a crash between the two
                # writes). The boundary's reconciliation does not repair this; an operator must.
                raise PermanentJobError(f"RESERVATION_PENDING_WITH_TERMINAL_ATTEMPT:{latest['status']}")
            return {"outcome": "ALREADY_RESOLVED", "attempt_status": latest["status"],
                    "reservation_status": reservation, "execution_attempt_id": attempt_id, "venue_calls": 0}
        with ctx.at_venue(job, fence):
            runtime = ctx.services.account_runtime(account_id)
            boundary = ctx.services.boundary(runtime, plan.bot_instance_id)
            result = boundary.reconcile_submit_unknown(plan, now_ms=ctx.now())
        latest, _, reservation = _attempt_and_plan(ctx, account_id, trade_plan_id, attempt_id)
        if latest["status"] in UNRESOLVED_ATTEMPT_STATES:
            raise RetryLater(f"BROKER_STATE_STILL_UNKNOWN:{','.join(result.reason_codes)}", STILL_UNKNOWN_RETRY_MS)
        return {"outcome": "RESOLVED", "boundary_status": result.status, "reason_codes": list(result.reason_codes),
                "attempt_status": latest["status"], "reservation_status": reservation,
                "execution_attempt_id": attempt_id}
    return handle


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# RECONCILIATION -- the production account maintenance, out-of-order safe
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def _summarise_maintenance(done: Any) -> Dict[str, Any]:
    if not isinstance(done, Mapping):
        return {"maintained": done is not None}
    recovery = done.get("recovery") or []
    history = done.get("execution_history") or []
    return {"maintained": True, "bot_id": done.get("bot_id"),
            "recovery": [r.get("status") if isinstance(r, Mapping) else str(r) for r in recovery],
            "positions_reconciled": len(history)}


def _reconcile(ctx: RuntimeJobContext, job: Job, fence: tuple, source: str) -> Dict[str, Any]:
    with ctx.at_venue(job, fence):
        started = ctx.platform_now()            # broker truth read from here on is newer than this instant
        runtime = ctx.services.account_runtime(fence[0])
        done = ctx.services.reconcile_account(runtime, ctx.now())
    through = ctx.advance_watermark(RECONCILIATION_SCOPE, job, fence, started)
    return {"outcome": "RECONCILED", "source": source, "reconciled_from": started, "watermark": through,
            **_summarise_maintenance(done)}


def reconciliation_handler(ctx: RuntimeJobContext) -> Handler:
    def handle(job: Job) -> Dict[str, Any]:
        requested_at = job.payload.get("requested_at")
        if isinstance(requested_at, bool) or not isinstance(requested_at, int) or requested_at <= 0:
            raise PermanentJobError("RECONCILIATION_JOB_MALFORMED:requested_at")
        fence = ctx.fence(job)
        mark = ctx.watermark(RECONCILIATION_SCOPE, fence[0])
        if mark is not None and requested_at <= mark:
            # A reconciliation that began reading the venue after this request was made has completed: the
            # request is satisfied. Nothing is read or changed (an older job delivered after a newer one).
            return {"outcome": "SUPERSEDED", "requested_at": requested_at, "watermark": mark, "venue_calls": 0}
        return _reconcile(ctx, job, fence, str(job.payload.get("reason") or "REQUESTED"))
    return handle


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# NOTIFICATION -- a bridge to the existing delivery semantics
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def notification_handler(ctx: RuntimeJobContext) -> Handler:
    ops: Dict[str, Handler] = {}

    def handle(job: Job) -> Dict[str, Any]:
        kind = job.payload.get("kind")
        if kind == KIND_OPS_ALERT:
            for field in ("subject", "text"):
                if not isinstance(job.payload.get(field), str):
                    raise PermanentJobError(f"NOTIFICATION_JOB_MALFORMED:{field}")
            if "handler" not in ops:
                from app.ops.alerting import notification_job_handler
                ops["handler"] = notification_job_handler(ctx.services.ops_channels())
            ctx.still_leased(job)
            return ops["handler"](job)
        if kind == KIND_USER_EVENT:
            outbox_id = _text(job.payload, "outbox_id", "NOTIFICATION_JOB_MALFORMED")
            from app.observability import user_events
            ctx.still_leased(job)
            now = ctx.now()
            result = user_events.deliver_one(ctx.legacy_db, outbox_id, now_ms=now,
                                             sender=ctx.services.notification_sender())
            outcome = result["outcome"]
            if outcome in ("SENT", "ALREADY_SENT", "SUPPRESSED", "NOT_EXTERNAL"):
                return result
            if outcome in ("RETRY", "NOT_DUE"):
                # The outbox owns attempts and backoff: the job only follows it (no queue attempt spent).
                wait = int(result.get("next_attempt_at") or now) - now
                raise RetryLater(f"OUTBOX_{outcome}:{result.get('last_error') or ''}", max(wait, 1_000))
            if outcome == "FAILED":
                raise PermanentJobError(f"OUTBOX_DELIVERY_FAILED:{result.get('last_error')}")
            raise PermanentJobError(f"OUTBOX_ROW_{outcome}:{outbox_id}")
        raise PermanentJobError(f"UNSUPPORTED_NOTIFICATION_KIND:{kind}")
    return handle


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# SHADOW_COMPARISON -- one comparison per (kind, UTC day), the pipeline's own function
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def _day_int(text: Any) -> int:
    from app.trading_intelligence.pipeline.daily_trend import day_of
    if not isinstance(text, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        raise PermanentJobError("SHADOW_COMPARISON_JOB_MALFORMED:day")
    try:
        return day_of(text)
    except ValueError as exc:
        raise PermanentJobError("SHADOW_COMPARISON_JOB_MALFORMED:day") from exc


def compare_shadow_day(legacy_db: Any, source: ShadowSource, *, family_id: str, kind: str, day: str,
                       now_ms: int) -> Dict[str, Any]:
    """The daily comparison of ``day`` for every model book of ``family_id`` -- ``pipeline.compare_models``,
    exactly as the cycle driver calls it, after the day's shadow cycle completed. Idempotent: a day whose rows
    exist for every model book is not recomputed, and ``shadow_compare.record`` stores a row once per content."""
    from shared_lib.persistence.cati_platform_schema import CYCLE_TABLE, SHADOW_COMPARISON_TABLE
    from app.trading_intelligence.pipeline import contracts as C
    from app.trading_intelligence.pipeline import shadow_book
    from app.trading_intelligence.pipeline.daily_trend import close_ms
    from app.trading_intelligence.pipeline.driver import COMPLETED, PlatformCycleDriver, cycle_id_of

    if kind != SHADOW_KIND_DAILY:
        raise PermanentJobError(f"SHADOW_COMPARISON_KIND_UNKNOWN:{kind}")
    day_number = _day_int(day)
    overdue = now_ms - close_ms(day_number) > SHADOW_CYCLE_WAIT_BOUND_MS
    entry = source.registry.entry(family_id)
    if entry is None:
        if overdue:
            raise PermanentJobError(f"SHADOW_FAMILY_NOT_REGISTERED:{family_id}")
        raise RetryLater(f"SHADOW_FAMILY_NOT_REGISTERED:{family_id}", 300_000)
    pipeline = PlatformCycleDriver.pipeline_of(entry)
    if not callable(getattr(pipeline, "compare_models", None)):
        raise PermanentJobError(f"SHADOW_COMPARISON_NOT_SUPPORTED:{family_id}")
    with legacy_db.connect() as conn:
        row = conn.execute(f"SELECT status FROM {CYCLE_TABLE} WHERE cycle_id=?",
                           (cycle_id_of(family_id, day, C.SHADOW),)).fetchone()
    status = row[0] if row else None
    if status != COMPLETED:
        if overdue:
            raise PermanentJobError(f"SHADOW_CYCLE_NOT_COMPLETED_WITHIN_BOUND:{status}")
        raise RetryLater(f"SHADOW_CYCLE_NOT_COMPLETED:{status}", 300_000)
    books = []
    with legacy_db.connect() as conn:
        for account in source.accounts.accounts(entry):
            if not account.model:
                continue
            book = shadow_book.load(conn, account.account_id, family_id)
            if book is not None and book.last_day is not None and book.first_day <= day_number <= book.last_day:
                books.append(account)
    expected = sorted(a.account_id for a in books)

    def recorded() -> Dict[str, int]:
        with legacy_db.connect() as conn:
            rows = conn.execute(f"SELECT account_id, COUNT(*) FROM {SHADOW_COMPARISON_TABLE} WHERE family_id=? AND day=? "
                                "AND reference=? GROUP BY account_id", (family_id, day_number, kind)).fetchall()
        return {r[0]: int(r[1]) for r in rows}

    before = recorded()
    if not expected:
        return {"outcome": "NOTHING_TO_COMPARE", "day": day, "kind": kind, "accounts": 0}
    if all(before.get(a) for a in expected):
        return {"outcome": "ALREADY_RECORDED", "day": day, "kind": kind, "accounts": len(expected),
                "rows": sum(before.get(a, 0) for a in expected)}
    try:
        market = pipeline.market(source.data.get(entry.definition.timeframe), day)
    except Exception as exc:  # noqa: BLE001 -- data for a completed cycle is missing: wait for the feed
        if getattr(exc, "code", None) and not overdue:
            raise RetryLater(f"SHADOW_MARKET_UNAVAILABLE:{exc.code}", 300_000) from exc
        raise
    pipeline.compare_models(legacy_db, market, books, now_ms=now_ms)
    after = recorded()
    return {"outcome": "RECORDED", "day": day, "kind": kind, "accounts": len(expected),
            "rows": sum(after.get(a, 0) for a in expected),
            "new_rows": sum(after.get(a, 0) for a in expected) - sum(before.get(a, 0) for a in expected)}


def shadow_comparison_handler(ctx: RuntimeJobContext) -> Handler:
    def handle(job: Job) -> Dict[str, Any]:
        family_id = _text(job.payload, "family_id", "SHADOW_COMPARISON_JOB_MALFORMED")
        kind = _text(job.payload, "kind", "SHADOW_COMPARISON_JOB_MALFORMED")
        day = job.payload.get("day")
        _day_int(day)
        ctx.services.shadow_gate()
        ctx.still_leased(job)
        return compare_shadow_day(ctx.legacy_db, ctx.services.shadow_source(), family_id=family_id, kind=kind,
                                  day=day, now_ms=ctx.now())
    return handle


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# OPERATIONAL_RETRY -- recovery work the runtime already has
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def _lineage_bot(ctx: RuntimeJobContext, account_id: str, pending: Sequence[Mapping[str, Any]]) -> Optional[str]:
    for row in pending:
        if row.get("bot_instance_id"):
            return str(row["bot_instance_id"])
    from app.trading_intelligence.integration.production_execution import open_lineage
    bots = open_lineage(ctx.legacy_db, account_id)["bots"]
    return bots[0] if bots else None


def operational_retry_handler(ctx: RuntimeJobContext) -> Handler:
    def handle(job: Job) -> Dict[str, Any]:
        task = job.payload.get("task")
        if task not in OPERATIONAL_TASKS:
            raise PermanentJobError(f"OPERATIONAL_RETRY_TASK_UNKNOWN:{task}")
        fence = ctx.fence(job)
        account_id = fence[0]
        from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
        if task == TASK_EXPIRE_RESERVATIONS:
            store = CATIReservationStore(ctx.legacy_db)
            ctx.verify_fence(fence)
            ctx.still_leased(job)
            # compare-and-set (RESERVED and past expiry only): a repeat expires nothing more
            return {"task": task, "expired": store.expire_stale(account_id, ctx.now())}
        if task == TASK_RECOVER_PENDING:
            pending = CATIReservationStore(ctx.legacy_db).pending_resolutions(account_id)
            unresolved = unresolved_attempts(ctx.legacy_db, account_id)
            if not pending and not unresolved:
                return {"task": task, "outcome": "NOTHING_PENDING", "venue_calls": 0}
            bot_id = _lineage_bot(ctx, account_id, pending)
            if bot_id is None:
                raise RetryLater("RECOVERY_BOUNDARY_UNAVAILABLE", 60_000)
            with ctx.at_venue(job, fence):
                runtime = ctx.services.account_runtime(account_id)
                results = ctx.services.boundary(runtime, bot_id).recover_pending(now_ms=ctx.now())
            return {"task": task, "outcome": "RECOVERY_RAN",
                    "results": [{"trade_plan_id": r.trade_plan_id, "status": r.status,
                                 "reason_codes": list(r.reason_codes)} for r in results]}
        from app.execution import protection_state
        uncertain = protection_state.uncertain_positions(ctx.legacy_db, account_id)
        if not uncertain:
            return {"task": task, "outcome": "NOTHING_TO_VERIFY", "venue_calls": 0}
        return {"task": task, **_reconcile(ctx, job, fence, f"{task}:{len(uncertain)}")}
    return handle


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# Registration
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def production_handlers(legacy_db: Any, platform_db: Database, *, worker_id: str,
                        fence_for_account: Callable[[str], Optional[tuple]], services: Optional[RuntimeServices] = None,
                        queue: Optional[JobQueue] = None, context: Optional[RuntimeJobContext] = None
                        ) -> Dict[str, Handler]:
    """The production handler registry: category -> handler. Refused in an ``api``-role process."""
    _require_trading_process()
    ctx = context or RuntimeJobContext(legacy_db, platform_db, worker_id=worker_id,
                                       fence_for_account=fence_for_account, services=services, queue=queue)
    return {ORDER: order_resolution_handler(ctx), RECONCILIATION: reconciliation_handler(ctx),
            NOTIFICATION: notification_handler(ctx), SHADOW_COMPARISON: shadow_comparison_handler(ctx),
            OPERATIONAL_RETRY: operational_retry_handler(ctx)}


class RuntimeJobRunner:
    """Runs the production handlers for one worker. ``run_once`` claims due jobs (highest priority first) and
    runs them with no transaction open; ``schedule`` enqueues the work that is due (idempotent, safe to call
    every loop iteration from every worker); ``request_stop`` drains."""

    def __init__(self, legacy_db: Any, platform_db: Database, *, worker_id: str,
                 fence_for_account: Callable[[str], Optional[tuple]], services: Optional[RuntimeServices] = None,
                 lease_ms: int = DEFAULT_JOB_LEASE_MS, max_processing_ms: Optional[int] = DEFAULT_MAX_PROCESSING_MS,
                 categories: Optional[Sequence[str]] = None) -> None:
        _require_trading_process()
        if not worker_id:
            raise ValueError("RUNTIME_JOBS_WORKER_ID_REQUIRED")
        self.legacy_db = legacy_db
        self.platform_db = platform_db
        self.worker_id = worker_id
        self.queue = JobQueue(platform_db, lease_ms=lease_ms)
        self.context = RuntimeJobContext(legacy_db, platform_db, worker_id=worker_id,
                                         fence_for_account=fence_for_account, services=services, queue=self.queue)
        handlers = production_handlers(legacy_db, platform_db, worker_id=worker_id,
                                       fence_for_account=fence_for_account, context=self.context)
        if categories is not None:
            unknown = set(categories) - set(handlers)
            if unknown:
                raise ValueError(f"RUNTIME_JOBS_UNKNOWN_CATEGORY:{sorted(unknown)}")
            handlers = {c: h for c, h in handlers.items() if c in set(categories)}
        self.handlers = handlers
        self._stop = threading.Event()
        self.processor = JobProcessor(self.queue, worker_id, handlers, should_stop=self._stop.is_set,
                                      max_processing_ms=max_processing_ms)

    @property
    def services(self) -> RuntimeServices:
        return self.context.services

    def run_once(self, limit: int = 10) -> list:
        if self._stop.is_set():
            return []
        return self.processor.run_once(limit=limit)

    def request_stop(self) -> None:
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def schedule(self, accounts: Iterable[Tuple[str, Optional[str]]] = (), *, now_ms: Optional[int] = None,
                 outbox_older_than_ms: Optional[int] = 60_000) -> Dict[str, Any]:
        """Enqueue the work that is due. ``accounts``: (account_id, tenant_id) this worker owns; an account it does
        not own (``fence_for_account`` None) is skipped. Every step is isolated and idempotent."""
        out: Dict[str, Any] = {"enqueued": 0, "errors": []}
        if self._stop.is_set():
            return out

        def step(name: str, call: Callable[[], int]) -> None:
            try:
                out["enqueued"] += int(call() or 0)
            except Exception as exc:  # noqa: BLE001 -- one producer must not stop the others
                out["errors"].append(f"{name}: {type(exc).__name__}: {exc}"[:300])
                logger.warning("runtime job producer %s failed: %s", name, exc)

        step("shadow_comparison", lambda: int(schedule_shadow_comparison(self.queue, now_ms=now_ms)[1]))
        if outbox_older_than_ms is not None:
            step("notification_outbox", lambda: enqueue_outbox_notifications(
                self.queue, self.legacy_db, now_ms=self.context.now(), older_than_ms=outbox_older_than_ms))
        for account_id, tenant_id in accounts:
            if not self.context.fence_for_account(account_id):
                continue
            step(f"orders:{account_id}", lambda a=account_id, t=tenant_id: enqueue_unresolved_orders(
                self.queue, self.legacy_db, a, tenant_id=t))
            step(f"operational:{account_id}", lambda a=account_id, t=tenant_id: schedule_operational_retries(
                self.queue, self.legacy_db, a, tenant_id=t, now_ms=self.context.now()))
        return out


def for_worker(legacy_db: Any, platform_db: Database, **kw: Any) -> Callable[[str, WorkerAssignments], RuntimeJobRunner]:
    """A ``TradingWorker(job_runner=...)`` factory: the runner is built once the worker id is registered, fenced
    by that worker's durable assignments."""
    def build(worker_id: str, assignments: WorkerAssignments) -> RuntimeJobRunner:
        return RuntimeJobRunner(legacy_db, platform_db, worker_id=worker_id,
                                fence_for_account=assignment_fence(assignments, worker_id), **kw)
    return build


# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
# Producers (stable idempotency keys)
# ══════════════════════════════════════════════════════════════════════════════════════════════════════════
def _tenant_of(queue: JobQueue, account_id: str, tenant_id: Optional[str]) -> str:
    if tenant_id:
        return tenant_id
    with queue.db.transaction(write=False) as conn:
        found = conn.scalar("SELECT tenant_id FROM platform_accounts WHERE account_id=?", (account_id,))
    if not found:
        raise ValueError(f"PLATFORM_ACCOUNT_UNKNOWN:{account_id}")
    return str(found)


def order_resolution_key(account_id: str, execution_attempt_id: str) -> str:
    return f"order-resolution:{_safe(account_id)}:{_safe(execution_attempt_id)}"


def enqueue_order_resolution(queue: JobQueue, account_id: str, *, trade_plan_id: str, execution_attempt_id: str,
                             tenant_id: Optional[str] = None) -> tuple:
    """One ORDER job per execution attempt whose broker outcome is unknown."""
    return queue.enqueue(ORDER, order_resolution_key(account_id, execution_attempt_id),
                         {"op": OP_RESOLVE_ATTEMPT, "trade_plan_id": trade_plan_id,
                          "execution_attempt_id": execution_attempt_id},
                         account_id=account_id, tenant_id=_tenant_of(queue, account_id, tenant_id),
                         priority=PRIORITY[ORDER])


def unresolved_attempts(legacy_db: Any, account_id: str) -> List[Dict[str, str]]:
    """Execution attempts of the account whose latest state is SUBMIT_UNKNOWN / PENDING_SUBMIT, and attempts
    behind a RESOLUTION_PENDING reservation. Local reads only."""
    out: Dict[str, Dict[str, str]] = {}
    try:
        with legacy_db.connect() as c:
            latest: Dict[str, tuple] = {}
            for r in c.execute("SELECT execution_attempt_id, status, trade_plan_id FROM cati_execution_attempts "
                               "WHERE broker_account_id=? ORDER BY recorded_at, sequence", (account_id,)):
                latest[r[0]] = (r[1], r[2])
            for attempt_id, (status, plan_id) in latest.items():
                if status in UNRESOLVED_ATTEMPT_STATES:
                    out[attempt_id] = {"execution_attempt_id": attempt_id, "trade_plan_id": plan_id, "status": status}
            for r in c.execute("SELECT execution_attempt_id, trade_plan_id FROM cati_portfolio_reservations "
                               "WHERE broker_account_id=? AND status='RESOLUTION_PENDING' "
                               "AND execution_attempt_id IS NOT NULL AND trade_plan_id IS NOT NULL", (account_id,)):
                out.setdefault(r[0], {"execution_attempt_id": r[0], "trade_plan_id": r[1], "status": "RESOLUTION_PENDING"})
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
    return list(out.values())


def enqueue_unresolved_orders(queue: JobQueue, legacy_db: Any, account_id: str, *,
                              tenant_id: Optional[str] = None) -> int:
    """Discovery (restart / takeover / every loop): an ORDER job for every unresolved attempt of the account."""
    created = 0
    for item in unresolved_attempts(legacy_db, account_id):
        created += int(enqueue_order_resolution(queue, account_id, trade_plan_id=item["trade_plan_id"],
                                                execution_attempt_id=item["execution_attempt_id"],
                                                tenant_id=tenant_id)[1])
    return created


def reconciliation_key(account_id: str, reason: str, requested_at: int) -> str:
    return f"reconciliation:{_safe(account_id)}:{_safe(reason)}:{int(requested_at)}"


def enqueue_reconciliation(queue: JobQueue, account_id: str, *, reason: str, tenant_id: Optional[str] = None,
                           requested_at: Optional[int] = None) -> tuple:
    """Ask for a reconciliation of the account as of ``requested_at`` (platform database clock by default)."""
    if requested_at is None:
        with queue.db.transaction(write=False) as conn:
            requested_at = conn.now_ms()
    return queue.enqueue(RECONCILIATION, reconciliation_key(account_id, reason, requested_at),
                         {"reason": str(reason), "requested_at": int(requested_at)}, account_id=account_id,
                         tenant_id=_tenant_of(queue, account_id, tenant_id), priority=PRIORITY[RECONCILIATION])


def outbox_notification_key(outbox_id: str) -> str:
    return f"notification:outbox:{_safe(outbox_id)}"


def enqueue_outbox_notification(queue: JobQueue, outbox_id: str) -> tuple:
    return queue.enqueue(NOTIFICATION, outbox_notification_key(outbox_id),
                         {"kind": KIND_USER_EVENT, "outbox_id": outbox_id}, priority=PRIORITY[NOTIFICATION])


def enqueue_outbox_notifications(queue: JobQueue, legacy_db: Any, *, now_ms: int, older_than_ms: int = 0,
                                 limit: int = 100) -> int:
    """A NOTIFICATION job for every external-channel outbox row still pending ``older_than_ms`` after it was due
    (the notification worker delivers rows first; the queue is the durable backstop with dead-lettering)."""
    try:
        with legacy_db.connect() as c:
            rows = c.execute("SELECT outbox_id FROM notification_outbox WHERE status='pending' AND channel IN "
                             "('email','telegram','push') AND next_attempt_at<=? ORDER BY next_attempt_at LIMIT ?",
                             (int(now_ms) - int(older_than_ms), int(limit))).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        return 0
    return sum(int(enqueue_outbox_notification(queue, r[0])[1]) for r in rows)


def shadow_comparison_key(family_id: str, kind: str, day: str) -> str:
    return f"shadow-comparison:{_safe(family_id)}:{_safe(kind)}:{day}"


def enqueue_shadow_comparison(queue: JobQueue, *, day: str, family_id: str = SHADOW_FAMILY,
                              kind: str = SHADOW_KIND_DAILY) -> tuple:
    _day_int(day)
    return queue.enqueue(SHADOW_COMPARISON, shadow_comparison_key(family_id, kind, day),
                         {"family_id": family_id, "kind": kind, "day": day}, priority=PRIORITY[SHADOW_COMPARISON])


def schedule_shadow_comparison(queue: JobQueue, *, now_ms: Optional[int] = None, family_id: str = SHADOW_FAMILY,
                               kind: str = SHADOW_KIND_DAILY) -> tuple:
    """Enqueue the due day's comparison (the last UTC day whose daily bar has closed). Idempotent per
    (family, kind, day): safe to call every loop iteration from every worker."""
    from app.trading_intelligence.pipeline.daily_trend import cycle_key_due
    if now_ms is None:
        with queue.db.transaction(write=False) as conn:
            now_ms = conn.now_ms()
    return enqueue_shadow_comparison(queue, day=cycle_key_due(int(now_ms)), family_id=family_id, kind=kind)


def operational_retry_key(task: str, account_id: str, bucket: int) -> str:
    return f"operational-retry:{_safe(task)}:{_safe(account_id)}:{int(bucket)}"


def enqueue_operational_retry(queue: JobQueue, task: str, account_id: str, *, bucket: int,
                              tenant_id: Optional[str] = None) -> tuple:
    if task not in OPERATIONAL_TASKS:
        raise ValueError(f"OPERATIONAL_RETRY_TASK_UNKNOWN:{task}")
    return queue.enqueue(OPERATIONAL_RETRY, operational_retry_key(task, account_id, bucket),
                         {"task": task, "bucket": int(bucket)}, account_id=account_id,
                         tenant_id=_tenant_of(queue, account_id, tenant_id), priority=PRIORITY[OPERATIONAL_RETRY])


def operational_work(legacy_db: Any, account_id: str, *, now_ms: int) -> List[str]:
    """Which recovery tasks the account needs now (local reads only): a job is enqueued only when there is work,
    so the audit table does not grow with empty periodic jobs."""
    tasks: List[str] = []
    try:
        with legacy_db.connect() as c:
            if c.execute("SELECT 1 FROM cati_portfolio_reservations WHERE broker_account_id=? AND status='RESERVED' "
                         "AND expires_at<=? LIMIT 1", (account_id, int(now_ms))).fetchone():
                tasks.append(TASK_EXPIRE_RESERVATIONS)
            # Resolution itself is the ORDER job's (one per attempt). The recovery sweep is scheduled for what
            # only it does: escalate an unknown outcome still unresolved past its deadline.
            if c.execute("SELECT 1 FROM cati_portfolio_reservations WHERE broker_account_id=? "
                         "AND status='RESOLUTION_PENDING' AND resolution_deadline IS NOT NULL "
                         "AND resolution_deadline<=? LIMIT 1", (account_id, int(now_ms))).fetchone():
                tasks.append(TASK_RECOVER_PENDING)
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
    from app.execution import protection_state
    if protection_state.uncertain_positions(legacy_db, account_id):
        tasks.append(TASK_VERIFY_PROTECTION)
    return tasks


def schedule_operational_retries(queue: JobQueue, legacy_db: Any, account_id: str, *, now_ms: int,
                                 tenant_id: Optional[str] = None, period_ms: int = OPERATIONAL_PERIOD_MS) -> int:
    bucket = int(now_ms) // int(period_ms)
    return sum(int(enqueue_operational_retry(queue, task, account_id, bucket=bucket, tenant_id=tenant_id)[1])
               for task in operational_work(legacy_db, account_id, now_ms=now_ms))


__all__ = [
    "CATEGORIES", "OP_RESOLVE_ATTEMPT", "QUEUE_ORDER_SUBMISSION_NOT_PERMITTED", "KIND_OPS_ALERT", "KIND_USER_EVENT",
    "SHADOW_KIND_DAILY", "SHADOW_FAMILY", "TASK_EXPIRE_RESERVATIONS", "TASK_RECOVER_PENDING", "TASK_VERIFY_PROTECTION",
    "RECONCILIATION_SCOPE", "RuntimeJobsRefused", "AccountRuntime", "ShadowSource", "RuntimeServices",
    "RuntimeJobContext", "RuntimeJobRunner", "assignment_fence", "for_worker", "production_handlers",
    "order_resolution_handler", "reconciliation_handler", "notification_handler", "shadow_comparison_handler",
    "operational_retry_handler", "compare_shadow_day", "unresolved_attempts", "operational_work",
    "order_resolution_key", "enqueue_order_resolution", "enqueue_unresolved_orders", "reconciliation_key",
    "enqueue_reconciliation", "outbox_notification_key", "enqueue_outbox_notification",
    "enqueue_outbox_notifications", "shadow_comparison_key", "enqueue_shadow_comparison",
    "schedule_shadow_comparison", "operational_retry_key", "enqueue_operational_retry",
    "schedule_operational_retries",
]
