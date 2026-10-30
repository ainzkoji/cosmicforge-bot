"""The trading worker: account maintenance in its own process, separate from the API (Section H, Step 5.2).

One loop iteration (``run_once``):

1. heartbeat and ``claim`` -- renew this worker's accounts, take free or expired ones up to its fair share;
2. for every account it now owns, **recover first** when the assignment is new (``needs_recovery``): the
   ``recover`` callback reconstructs exchange state and durable work (open intents, protective orders) before
   anything new may be submitted for the account; then ``maintain`` (position maintenance, protection,
   reconciliation, risk brakes) with a ``fence`` it must check before each durable mutation;
3. release accounts above the fair share (another worker joined) at this safe point;
4. run due durable jobs (with a ``runtime_jobs`` runner: first enqueue the work due for the owned accounts).

Accounts are processed with bounded concurrency; one account's exception is isolated from the others.
``drain()`` (SIGTERM / deploy) stops claiming, lets running account work finish, then releases every account
with reason ``DRAIN`` so a successor takes it immediately instead of waiting for expiry. A worker that is
killed without draining loses its accounts when their assignment expires (one TTL) -- measured in
``tests/platform_workers/test_worker_takeover.py`` and the process-kill drill.
"""
from __future__ import annotations

import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

from shared_lib.persistence.sql import Database

from .assignments import Assignment, AssignmentLost, WorkerAssignments
from .jobs import JobProcessor, JobQueue

logger = logging.getLogger(__name__)


@dataclass
class AccountContext:
    assignment: Assignment
    assignments: WorkerAssignments

    @property
    def account_id(self) -> str:
        return self.assignment.account_id

    @property
    def tenant_id(self) -> str:
        return self.assignment.tenant_id

    @property
    def fence(self) -> tuple:
        return (self.assignment.account_id, self.assignment.worker_id, self.assignment.fencing_token)

    def check(self) -> None:
        """Raise ``AssignmentLost`` if this worker no longer owns the account. Call before each mutation."""
        self.assignments.verify(self.assignment)


Callback = Callable[[AccountContext], Any]


@dataclass
class IterationReport:
    owned: int = 0
    recovered: List[str] = field(default_factory=list)
    maintained: List[str] = field(default_factory=list)
    lost: List[str] = field(default_factory=list)
    failed: Dict[str, str] = field(default_factory=dict)
    released: List[str] = field(default_factory=list)
    jobs: List[tuple] = field(default_factory=list)
    duration_ms: float = 0.0


class TradingWorker:
    """``job_runner``: the production job registration (``runtime_jobs.RuntimeJobRunner``), or a factory
    ``(worker_id, assignments) -> runner`` such as ``runtime_jobs.for_worker(legacy_db, platform_db)`` -- the same
    registration path the production trading loop uses. Each iteration then enqueues the due work for the
    accounts this worker owns (``runner.schedule``) and runs due jobs. ``job_handlers`` (a plain category ->
    handler map) is the older, lower-level form; the two are exclusive.

    Drain and jobs: ``drain()`` stops claiming new jobs. A job claimed but not yet started is handed back at once
    (no attempt spent); a job whose handler is running finishes before the accounts are released. A job that was
    running when the process died stays CLAIMED until its lease expires and is then taken by any worker --
    nothing is lost or released early."""

    def __init__(self, db: Database, *, maintain: Callback, recover: Callback, worker_id: Optional[str] = None,
                 assignments: Optional[WorkerAssignments] = None, job_handlers: Optional[Mapping[str, Any]] = None,
                 max_concurrency: int = 8, build_version: str = "unknown", job_runner: Any = None,
                 job_batch: int = 10) -> None:
        if job_runner is not None and job_handlers:
            raise ValueError("WORKER_JOB_HANDLERS_AND_RUNNER_ARE_EXCLUSIVE")
        self.db = db
        self.assignments = assignments or WorkerAssignments(db)
        self.worker_id = self.assignments.register_worker(worker_id, build_version=build_version)
        self.maintain = maintain
        self.recover = recover
        self.max_concurrency = max(1, int(max_concurrency))
        self.job_batch = max(1, int(job_batch))
        self._draining = threading.Event()
        self._jobs_running = threading.Lock()
        if job_runner is not None and not hasattr(job_runner, "run_once"):
            job_runner = job_runner(self.worker_id, self.assignments)
        if job_runner is not None and getattr(job_runner, "worker_id", self.worker_id) != self.worker_id:
            raise ValueError("JOB_RUNNER_WORKER_MISMATCH")
        self.jobs = job_runner if job_runner is not None else (
            JobProcessor(JobQueue(db), self.worker_id, job_handlers, should_stop=self._draining.is_set)
            if job_handlers else None)
        self._pool = ThreadPoolExecutor(max_workers=self.max_concurrency, thread_name_prefix=f"acct-{self.worker_id[-8:]}")

    def _one(self, a: Assignment, report: IterationReport) -> None:
        ctx = AccountContext(a, self.assignments)
        try:
            if a.needs_recovery:
                self.recover(ctx)
                self.assignments.mark_recovered(a)
                report.recovered.append(a.account_id)
            self.maintain(ctx)
            self.assignments.mark_success(a)
            report.maintained.append(a.account_id)
        except AssignmentLost:
            report.lost.append(a.account_id)
        except Exception as exc:
            logger.exception("account %s maintenance failed", a.account_id)
            report.failed[a.account_id] = f"{type(exc).__name__}: {exc}"[:300]

    def run_once(self) -> IterationReport:
        started = time.perf_counter()
        report = IterationReport()
        owned = self.assignments.claim(self.worker_id) if not self._draining.is_set() else []
        report.owned = len(owned)
        futures = [self._pool.submit(self._one, a, report) for a in owned]
        for f in futures:
            f.result()
        surplus = self.assignments.surplus(self.worker_id)
        if surplus:
            self.assignments.release(self.worker_id, surplus, reason="REBALANCE")
            report.released = surplus
        if self.jobs is not None and not self._draining.is_set():
            with self._jobs_running:
                schedule = getattr(self.jobs, "schedule", None)
                if callable(schedule):
                    try:
                        schedule([(a.account_id, a.tenant_id) for a in owned if a.account_id not in surplus])
                    except Exception:
                        logger.exception("job scheduling failed")
                report.jobs = self.jobs.run_once(limit=self.job_batch)
        report.duration_ms = (time.perf_counter() - started) * 1000
        return report

    def drain(self, *, job_wait_s: float = 60.0) -> int:
        self._draining.set()
        stop_jobs = getattr(self.jobs, "request_stop", None)
        if callable(stop_jobs):
            stop_jobs()
        self.assignments.set_worker_state(self.worker_id, "DRAINING")
        self._pool.shutdown(wait=True)
        # The accounts are released only after the job in flight (if any) returned, so no job of this worker
        # acts for an account a successor already owns. A handler hung past ``job_wait_s`` is abandoned: its
        # job returns on lease expiry and every later step it takes is re-checked against the fence.
        if self._jobs_running.acquire(timeout=job_wait_s):
            self._jobs_running.release()
        else:
            logger.warning("worker %s drained with a job still running after %.0fs", self.worker_id, job_wait_s)
        released = self.assignments.release(self.worker_id, reason="DRAIN")
        self.assignments.set_worker_state(self.worker_id, "STOPPED")
        return released

    def run_forever(self, *, interval_s: float = 5.0, stop: Optional[threading.Event] = None) -> None:
        stop = stop or threading.Event()
        # SIGTERM from systemd/containers; SIGBREAK is how Windows delivers CTRL_BREAK to a process group.
        for sig in (signal.SIGTERM, signal.SIGINT, getattr(signal, "SIGBREAK", None)):
            if sig is None:
                continue
            try:
                signal.signal(sig, lambda *_: stop.set())
            except ValueError:  # not the main thread
                pass
        while not stop.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("worker iteration failed")
            stop.wait(interval_s)
        self.drain()
