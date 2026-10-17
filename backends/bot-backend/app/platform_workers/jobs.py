"""Durable job queue and order-intent ledger (Section H, Step 5.3).

Delivery is **at least once**: a job can be claimed again after its worker died, its lease expired, or its
acknowledgement was lost. Effects are made exactly-once by the business layer, not by the queue:

* every job has a stable ``idempotency_key`` (unique) -- enqueuing the same work twice returns the first job;
* a job's lease carries a token that rises on every claim; ``complete``/``fail`` with a stale token is refused
  (``StaleJobLease``), so a slow worker that lost its lease cannot overwrite its successor's result;
* order jobs go through ``OrderIntents``: the intent is written ``SUBMITTING`` and **committed before** the venue
  call, the venue call happens with **no transaction open**, and a retry of an intent left ``SUBMITTING`` first
  reads the venue by client order id. It resubmits only after the venue has said the order does not exist and
  a settle window has passed -- and even then with the same deterministic client order id, so a venue that
  enforces client-id uniqueness rejects a duplicate on its side too.

A job that exhausts ``max_attempts`` -- including attempts lost to crashes (the claim counts) -- goes to
``DEAD`` (the dead-letter state) with its last failure reason; ``requeue_dead`` is the manual recovery.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence

from shared_lib.persistence.sql import Connection, Database, decimal_in, decimal_out

from .assignments import WorkerAssignments

logger = logging.getLogger(__name__)

ORDER, RECONCILIATION, NOTIFICATION, SHADOW_COMPARISON, OPERATIONAL_RETRY = (
    "ORDER", "RECONCILIATION", "NOTIFICATION", "SHADOW_COMPARISON", "OPERATIONAL_RETRY")
DEFAULT_JOB_LEASE_MS = 60_000
DEFAULT_MAX_ATTEMPTS = 8
BACKOFF_MS = (1_000, 5_000, 30_000, 120_000, 600_000, 1_800_000)


class StaleJobLease(RuntimeError):
    code = "JOB_LEASE_STALE"


class PermanentJobError(RuntimeError):
    """Raised by a handler for an error that retrying cannot fix (a poison message). Goes straight to DEAD."""

    code = "JOB_PERMANENT_FAILURE"


class RetryLater(RuntimeError):
    """Raised by a handler that cannot finish yet (e.g. an order outcome still settling). Not a failure."""

    def __init__(self, reason: str, delay_ms: int = 5_000) -> None:
        super().__init__(reason)
        self.delay_ms = delay_ms


@dataclass(frozen=True)
class Job:
    job_id: str
    idempotency_key: str
    category: str
    payload_version: int
    payload: Dict[str, Any]
    account_id: Optional[str]
    tenant_id: Optional[str]
    attempts: int
    max_attempts: int
    lease_owner: Optional[str]
    lease_token: int
    status: str


def backoff_ms(attempt: int) -> int:
    return BACKOFF_MS[min(max(attempt - 1, 0), len(BACKOFF_MS) - 1)]


_COLS = ("job_id, idempotency_key, category, payload_version, payload, account_id, tenant_id, attempts, max_attempts, "
         "lease_owner, lease_token, status")


def _job(row) -> Job:
    return Job(row[0], row[1], row[2], int(row[3]), json.loads(row[4]), row[5], row[6], int(row[7]), int(row[8]),
               row[9], int(row[10]), row[11])


class JobQueue:
    def __init__(self, db: Database, *, lease_ms: int = DEFAULT_JOB_LEASE_MS) -> None:
        self.db = db
        self.lease_ms = int(lease_ms)

    def enqueue(self, category: str, idempotency_key: str, payload: Mapping[str, Any], *, payload_version: int = 1,
                account_id: Optional[str] = None, tenant_id: Optional[str] = None, delay_ms: int = 0,
                priority: int = 0, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> tuple:
        """Returns ``(job_id, created)``. A second enqueue with the same key is a no-op returning the first job --
        and it is refused if its category or payload differ, because that is two different pieces of work
        sharing one identity."""
        if not idempotency_key:
            raise ValueError("JOB_IDEMPOTENCY_KEY_REQUIRED")
        body = json.dumps(dict(payload), sort_keys=True, default=str)
        with self.db.transaction() as conn:
            now = conn.now_ms()
            job_id = uuid.uuid4().hex
            cur = conn.execute(
                "INSERT INTO platform_jobs (job_id, idempotency_key, category, payload_version, payload, account_id, "
                "tenant_id, created_at, eligible_at, priority, status, attempts, max_attempts, next_attempt_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,'PENDING',0,?,?) ON CONFLICT (idempotency_key) DO NOTHING",
                (job_id, idempotency_key, category, payload_version, body, account_id, tenant_id, now, now + delay_ms,
                 priority, max_attempts, now + delay_ms))
            if cur.rowcount == 1:
                return job_id, True
            row = conn.one("SELECT job_id, category, payload FROM platform_jobs WHERE idempotency_key=?",
                           (idempotency_key,))
            if row[1] != category or row[2] != body:
                raise ValueError(f"JOB_IDEMPOTENCY_CONFLICT:{idempotency_key}")
            return row[0], False

    def claim(self, owner: str, *, categories: Optional[Sequence[str]] = None, limit: int = 10) -> List[Job]:
        """Claim due jobs, including jobs whose previous lease expired. A job whose attempts are already used up
        when it is found again (its worker died on the last attempt) goes to DEAD instead of being run."""
        cats = tuple(categories or ())
        cat_sql = (" AND category IN (" + ",".join("?" * len(cats)) + ")") if cats else ""
        claimed: List[Job] = []
        with self.db.transaction() as conn:
            now = conn.now_ms()
            rows = conn.all(
                f"SELECT {_COLS} FROM platform_jobs WHERE ((status IN ('PENDING','RETRY') AND next_attempt_at <= ?) "
                f"OR (status='CLAIMED' AND lease_expires_at <= ?)){cat_sql} "
                "ORDER BY priority DESC, next_attempt_at, job_id LIMIT ?" + conn.skip_locked_suffix(),
                (now, now, *cats, limit))
            for row in rows:
                job = _job(row)
                if job.attempts >= job.max_attempts:
                    conn.execute("UPDATE platform_jobs SET status='DEAD', dead_lettered_at=?, lease_owner=NULL, "
                                 "failure_reason=COALESCE(failure_reason, 'ATTEMPTS_EXHAUSTED_BY_LOST_LEASES') WHERE job_id=?",
                                 (now, job.job_id))
                    continue
                token = job.lease_token + 1
                conn.execute("UPDATE platform_jobs SET status='CLAIMED', lease_owner=?, lease_token=?, lease_expires_at=?, "
                             "attempts=attempts+1 WHERE job_id=?", (owner, token, now + self.lease_ms, job.job_id))
                conn.execute("INSERT INTO platform_job_attempts (job_id, attempt, lease_owner, lease_token, started_at) "
                             "VALUES (?,?,?,?,?)", (job.job_id, job.attempts + 1, owner, token, now))
                claimed.append(Job(job.job_id, job.idempotency_key, job.category, job.payload_version, job.payload,
                                   job.account_id, job.tenant_id, job.attempts + 1, job.max_attempts, owner, token,
                                   "CLAIMED"))
        return claimed

    def _finish(self, conn: Connection, job: Job, outcome: str, detail: Optional[str]) -> None:
        conn.execute("UPDATE platform_job_attempts SET finished_at=?, outcome=?, detail=? WHERE job_id=? AND lease_token=?",
                     (conn.now_ms(), outcome, (detail or "")[:2000], job.job_id, job.lease_token))

    def _guard(self, conn: Connection, job: Job) -> None:
        row = conn.one("SELECT status, lease_owner, lease_token FROM platform_jobs WHERE job_id=?" + conn.lock_rows_suffix(),
                       (job.job_id,))
        if row is None or row[0] != "CLAIMED" or row[1] != job.lease_owner or int(row[2]) != job.lease_token:
            raise StaleJobLease(f"{job.job_id}: lease superseded")

    def heartbeat(self, job: Job) -> None:
        with self.db.transaction() as conn:
            self._guard(conn, job)
            conn.execute("UPDATE platform_jobs SET lease_expires_at=? WHERE job_id=?", (conn.now_ms() + self.lease_ms,
                                                                                         job.job_id))

    def complete(self, job: Job, evidence: Optional[Mapping[str, Any]] = None) -> None:
        with self.db.transaction() as conn:
            self._guard(conn, job)
            now = conn.now_ms()
            conn.execute("UPDATE platform_jobs SET status='SUCCEEDED', completed_at=?, completion_evidence=?, "
                         "lease_owner=NULL, lease_expires_at=NULL, failure_reason=NULL WHERE job_id=?",
                         (now, json.dumps(dict(evidence or {}), sort_keys=True, default=str), job.job_id))
            self._finish(conn, job, "SUCCEEDED", None)

    def fail(self, job: Job, reason: str, *, permanent: bool = False, delay_ms: Optional[int] = None,
             counts_as_attempt: bool = True) -> str:
        with self.db.transaction() as conn:
            self._guard(conn, job)
            now = conn.now_ms()
            if not counts_as_attempt:
                conn.execute("UPDATE platform_jobs SET attempts=attempts-1 WHERE job_id=?", (job.job_id,))
            attempts = job.attempts - (0 if counts_as_attempt else 1)
            if permanent or attempts >= job.max_attempts:
                conn.execute("UPDATE platform_jobs SET status='DEAD', dead_lettered_at=?, failure_reason=?, lease_owner=NULL, "
                             "lease_expires_at=NULL WHERE job_id=?", (now, reason[:1000], job.job_id))
                self._finish(conn, job, "DEAD", reason)
                return "DEAD"
            wait = delay_ms if delay_ms is not None else backoff_ms(attempts)
            conn.execute("UPDATE platform_jobs SET status='RETRY', next_attempt_at=?, failure_reason=?, lease_owner=NULL, "
                         "lease_expires_at=NULL WHERE job_id=?", (now + wait, reason[:1000], job.job_id))
            self._finish(conn, job, "RETRY", reason)
            return "RETRY"

    def requeue_dead(self, job_id: str, *, operator: str, reason: str) -> None:
        if not operator or not reason:
            raise ValueError("REQUEUE_OPERATOR_AND_REASON_REQUIRED")
        with self.db.transaction() as conn:
            now = conn.now_ms()
            cur = conn.execute("UPDATE platform_jobs SET status='RETRY', attempts=0, next_attempt_at=?, dead_lettered_at=NULL, "
                               "failure_reason=? WHERE job_id=? AND status='DEAD'",
                               (now, f"REQUEUED by {operator}: {reason}"[:1000], job_id))
            if cur.rowcount != 1:
                raise ValueError(f"JOB_NOT_DEAD:{job_id}")

    def get(self, job_id: str) -> Dict[str, Any]:
        with self.db.transaction(write=False) as conn:
            row = conn.one(f"SELECT {_COLS}, completion_evidence, failure_reason FROM platform_jobs WHERE job_id=?", (job_id,))
        job = _job(row)
        return {**job.__dict__, "completion_evidence": json.loads(row[12]) if row[12] else None,
                "failure_reason": row[13]}

    def backlog(self) -> Dict[str, Any]:
        with self.db.transaction(write=False) as conn:
            now = conn.now_ms()
            counts = conn.all("SELECT category, status, COUNT(*) FROM platform_jobs GROUP BY category, status")
            oldest = conn.scalar("SELECT MIN(next_attempt_at) FROM platform_jobs WHERE status IN ('PENDING','RETRY') "
                                 "AND next_attempt_at <= ?", (now,))
        out: Dict[str, Dict[str, int]] = {}
        for cat, status, n in counts:
            out.setdefault(cat, {})[status] = int(n)
        return {"by_category": out, "oldest_due_age_ms": (now - int(oldest)) if oldest is not None else 0,
                "dead": sum(v.get("DEAD", 0) for v in out.values())}


Handler = Callable[[Job], Optional[Mapping[str, Any]]]


class JobProcessor:
    """Runs handlers. Claims in one transaction, runs the handler with **no transaction open**, records the
    outcome in another; a crash anywhere leaves the job claimable again after its lease."""

    def __init__(self, queue: JobQueue, owner: str, handlers: Mapping[str, Handler]) -> None:
        self.queue = queue
        self.owner = owner
        self.handlers = dict(handlers)

    def run_once(self, *, limit: int = 10) -> List[tuple]:
        results = []
        for job in self.queue.claim(self.owner, categories=list(self.handlers), limit=limit):
            handler = self.handlers[job.category]
            try:
                evidence = handler(job)
            except RetryLater as wait:
                results.append((job.job_id, self.queue.fail(job, f"RETRY_LATER:{wait}", delay_ms=wait.delay_ms,
                                                             counts_as_attempt=False)))
                continue
            except PermanentJobError as exc:
                results.append((job.job_id, self.queue.fail(job, f"{type(exc).__name__}: {exc}", permanent=True)))
                continue
            except StaleJobLease:
                results.append((job.job_id, "STALE"))
                continue
            except Exception as exc:  # retryable by default
                logger.warning("job %s (%s) failed: %s", job.job_id, job.category, exc)
                results.append((job.job_id, self.queue.fail(job, f"{type(exc).__name__}: {exc}")))
                continue
            try:
                self.queue.complete(job, evidence)
                results.append((job.job_id, "SUCCEEDED"))
            except StaleJobLease:
                results.append((job.job_id, "STALE"))
        return results


# ── order intents ─────────────────────────────────────────────────────────────────────────────────────────
class OrderExecutor(Protocol):
    """The venue side. In production this is the existing CATI execution boundary; this module never talks
    to a venue itself."""

    def submit(self, *, account_id: str, symbol: str, side: str, quantity: Decimal, limit_price: Optional[Decimal],
               client_order_id: str) -> str: ...

    def lookup(self, *, account_id: str, symbol: str, client_order_id: str) -> Optional[str]: ...


class UnknownSubmitOutcome(RuntimeError):
    """The venue call failed in a way that does not tell whether the order exists."""


class SubmitNotReached(RuntimeError):
    """The venue call provably did not reach the venue (refused before sending, or rejected before
    acceptance). The intent may be submitted again."""


def client_order_id_for(idempotency_key: str) -> str:
    return "cf" + hashlib.sha256(idempotency_key.encode()).hexdigest()[:30]


@dataclass(frozen=True)
class Intent:
    intent_id: str
    idempotency_key: str
    account_id: str
    tenant_id: str
    venue: str
    symbol: str
    side: str
    quantity: Decimal
    limit_price: Optional[Decimal]
    client_order_id: str
    status: str
    venue_order_id: Optional[str]
    submit_attempts: int
    updated_at: int


class OrderIntents:
    #: After a lost acknowledgement, an order the venue does not (yet) show is not declared absent until this
    #: long after the submission: a request still in flight could land after the first lookup.
    SETTLE_MS = 30_000

    def __init__(self, db: Database, assignments: Optional[WorkerAssignments] = None, *, settle_ms: int = SETTLE_MS) -> None:
        self.db = db
        self.assignments = assignments
        self.settle_ms = settle_ms

    def prepare(self, *, idempotency_key: str, account_id: str, tenant_id: str, venue: str, symbol: str, side: str,
                quantity: Any, limit_price: Any = None, source_intent_id: Optional[str] = None,
                fencing_token: int = 0) -> Intent:
        with self.db.transaction() as conn:
            now = conn.now_ms()
            conn.execute(
                "INSERT INTO platform_order_intents (intent_id, idempotency_key, account_id, tenant_id, source_intent_id, "
                "venue, symbol, side, quantity, limit_price, client_order_id, status, fencing_token, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?,?) ON CONFLICT (idempotency_key) DO NOTHING",
                (uuid.uuid4().hex, idempotency_key, account_id, tenant_id, source_intent_id, venue, symbol, side,
                 decimal_in(quantity), decimal_in(limit_price), client_order_id_for(idempotency_key), fencing_token,
                 now, now))
            return self._load_on(conn, idempotency_key)

    @staticmethod
    def _load_on(conn: Connection, key: str) -> Intent:
        r = conn.one("SELECT intent_id, idempotency_key, account_id, tenant_id, venue, symbol, side, quantity, limit_price, "
                     "client_order_id, status, venue_order_id, submit_attempts, updated_at FROM platform_order_intents "
                     "WHERE idempotency_key=?", (key,))
        return Intent(r[0], r[1], r[2], r[3], r[4], r[5], r[6], decimal_out(r[7]), decimal_out(r[8]), r[9], r[10], r[11],
                      int(r[12]), int(r[13]))

    def load(self, key: str) -> Intent:
        with self.db.transaction(write=False) as conn:
            return self._load_on(conn, key)

    def _set(self, intent: Intent, status: str, *, expect: Sequence[str], venue_order_id: Optional[str] = None,
             bump_attempt: bool = False, fence: Optional[tuple] = None, detail: Optional[str] = None) -> Intent:
        with self.db.transaction() as conn:
            if fence is not None and self.assignments is not None:
                self.assignments.verify_on(conn, *fence)
            cur = conn.execute(
                "UPDATE platform_order_intents SET status=?, venue_order_id=COALESCE(?, venue_order_id), "
                "submit_attempts=submit_attempts+?, updated_at=?, detail=COALESCE(?, detail) WHERE intent_id=? AND status IN ("
                + ",".join("?" * len(expect)) + ")",
                (status, venue_order_id, 1 if bump_attempt else 0, conn.now_ms(), detail, intent.intent_id, *expect))
            if cur.rowcount != 1:
                current = self._load_on(conn, intent.idempotency_key)
                raise StaleJobLease(f"intent {intent.intent_id} is {current.status}, expected {list(expect)}")
            return self._load_on(conn, intent.idempotency_key)

    def execute(self, key: str, executor: OrderExecutor, *, fence: Optional[tuple] = None) -> Intent:
        """Drive one intent to a terminal state, safely re-entrant from any worker at any point.

        ``fence`` = (account_id, worker_id, fencing_token): the submission is refused if the worker no longer owns
        the account -- checked in the same transaction that marks the intent SUBMITTING."""
        intent = self.load(key)
        if intent.status in ("ACKNOWLEDGED", "REJECTED"):
            return intent
        if intent.status == "SUBMITTING":
            intent = self._recover(intent, executor)
            if intent.status != "NOT_SUBMITTED":
                return intent
        intent = self._set(intent, "SUBMITTING", expect=("PENDING", "NOT_SUBMITTED"), bump_attempt=True, fence=fence)
        # The SUBMITTING row is committed. No transaction is open across the venue call.
        try:
            venue_id = executor.submit(account_id=intent.account_id, symbol=intent.symbol, side=intent.side,
                                       quantity=intent.quantity, limit_price=intent.limit_price,
                                       client_order_id=intent.client_order_id)
        except UnknownSubmitOutcome:
            raise RetryLater("SUBMIT_OUTCOME_UNKNOWN", delay_ms=self.settle_ms)
        except SubmitNotReached as exc:
            self._set(intent, "NOT_SUBMITTED", expect=("SUBMITTING",), detail=f"NOT_REACHED:{exc}"[:500])
            raise
        except PermanentJobError as exc:
            self._set(intent, "REJECTED", expect=("SUBMITTING",), detail=str(exc)[:500])
            raise
        return self._set(intent, "ACKNOWLEDGED", expect=("SUBMITTING",), venue_order_id=venue_id)

    def _recover(self, intent: Intent, executor: OrderExecutor) -> Intent:
        venue_id = executor.lookup(account_id=intent.account_id, symbol=intent.symbol,
                                   client_order_id=intent.client_order_id)
        if venue_id is not None:
            return self._set(intent, "ACKNOWLEDGED", expect=("SUBMITTING",), venue_order_id=venue_id,
                             detail="RECOVERED_BY_READBACK")
        with self.db.transaction(write=False) as conn:
            age = conn.now_ms() - intent.updated_at
        if age < self.settle_ms:
            raise RetryLater("SUBMIT_OUTCOME_SETTLING", delay_ms=self.settle_ms - age)
        return self._set(intent, "NOT_SUBMITTED", expect=("SUBMITTING",), detail="ABSENT_AFTER_SETTLE_WINDOW")


def order_job_handler(intents: OrderIntents, executor: OrderExecutor,
                      fence_for: Optional[Callable[[Job], Optional[tuple]]] = None) -> Handler:
    """An ORDER job's payload names an intent (``idempotency_key``) prepared earlier by the decision layer."""

    def handle(job: Job):
        key = job.payload.get("intent_key")
        if not key:
            raise PermanentJobError("ORDER_JOB_WITHOUT_INTENT")
        intent = intents.execute(key, executor, fence=fence_for(job) if fence_for else None)
        return {"intent_id": intent.intent_id, "status": intent.status, "venue_order_id": intent.venue_order_id,
                "client_order_id": intent.client_order_id, "submit_attempts": intent.submit_attempts}

    return handle
