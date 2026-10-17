"""Step 5.3: durable job queue -- at-least-once delivery, exactly-once business effects."""
from __future__ import annotations

import time

import pytest

from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.fake import FakeVenue
from app.platform_workers.jobs import (NOTIFICATION, ORDER, SHADOW_COMPARISON, JobProcessor, JobQueue, OrderIntents,
                                       PermanentJobError, StaleJobLease, order_job_handler)
from app.platform_workers.assignments import WorkerAssignments
from shared_lib.persistence.sql import Database
from venue_bridge import VenueExecutor, placed_orders


@pytest.fixture
def env(platform_db):
    WorkerAssignments(platform_db).upsert_accounts([("acct", "tenant", "binance", "DEMO")])
    venue = FakeVenue(c.BINANCE)
    venue.add_instrument("BTCUSDT", price=50_000)
    executor = VenueExecutor({"acct": venue})
    intents = OrderIntents(platform_db, settle_ms=0)
    return platform_db, venue, executor, intents


def _order_job(db, intents, key="sig-1:acct"):
    intents.prepare(idempotency_key=key, account_id="acct", tenant_id="tenant", venue="binance", symbol="BTCUSDT",
                    side="BUY", quantity="0.01")
    return JobQueue(db, lease_ms=200).enqueue(ORDER, key, {"intent_key": key}, account_id="acct", tenant_id="tenant")[0]


def test_enqueue_is_idempotent_and_conflicts_are_refused(platform_db):
    q = JobQueue(platform_db)
    first = q.enqueue(NOTIFICATION, "n1", {"user": 1})
    assert first[1] and q.enqueue(NOTIFICATION, "n1", {"user": 1}) == (first[0], False)
    with pytest.raises(ValueError, match="IDEMPOTENCY_CONFLICT"):
        q.enqueue(NOTIFICATION, "n1", {"user": 2})


def test_account_job_requires_tenant(platform_db):
    WorkerAssignments(platform_db).upsert_accounts([("acct", "tenant", "binance", "DEMO")])
    with pytest.raises(Exception):
        JobQueue(platform_db).enqueue(ORDER, "x", {}, account_id="acct", tenant_id=None)


def test_crash_before_claim_then_single_effect(env):
    db, venue, executor, intents = env
    job_id = _order_job(db, intents)            # "crash" here: nothing claimed it
    p = JobProcessor(JobQueue(db), "w1", {ORDER: order_job_handler(intents, executor)})
    assert p.run_once() == [(job_id, "SUCCEEDED")]
    assert p.run_once() == []
    assert placed_orders(venue, intents.load("sig-1:acct").client_order_id) == 1


def test_crash_after_claim_is_reclaimed_after_lease(env):
    db, venue, executor, intents = env
    job_id = _order_job(db, intents)
    q = JobQueue(db, lease_ms=200)
    [claimed] = q.claim("dead-worker")            # worker dies holding the job
    assert JobQueue(db, lease_ms=200).claim("w2") == []
    time.sleep(0.25)
    p = JobProcessor(JobQueue(db, lease_ms=200), "w2", {ORDER: order_job_handler(intents, executor)})
    assert p.run_once() == [(job_id, "SUCCEEDED")]
    assert q.get(job_id)["attempts"] == 2
    with pytest.raises(StaleJobLease):
        q.complete(claimed)                        # the dead worker's late completion is refused


def test_crash_before_exchange_submission_submits_once(env):
    db, venue, executor, intents = env
    _order_job(db, intents)
    intent = intents.load("sig-1:acct")
    assert intent.status == "PENDING"              # crash: prepared, never submitted
    p = JobProcessor(JobQueue(db), "w", {ORDER: order_job_handler(intents, executor)})
    p.run_once()
    assert executor.submits == 1 and intents.load("sig-1:acct").status == "ACKNOWLEDGED"


def test_lost_acknowledgement_recovers_by_readback(env):
    db, venue, executor, intents = env
    job_id = _order_job(db, intents)
    venue.fail_next("place_order", c.AdapterError(c.NETWORK_UNCERTAIN, "timeout"), after_effect=True)
    q = JobQueue(db)
    p = JobProcessor(q, "w", {ORDER: order_job_handler(intents, executor)})
    assert p.run_once() == [(job_id, "RETRY")]
    assert intents.load("sig-1:acct").status == "SUBMITTING"
    with db.transaction() as conn:                 # make the retry due now (the settle delay already passed)
        conn.execute("UPDATE platform_jobs SET next_attempt_at=0 WHERE job_id=?", (job_id,))
    assert p.run_once() == [(job_id, "SUCCEEDED")]
    final = intents.load("sig-1:acct")
    assert final.status == "ACKNOWLEDGED" and executor.submits == 1
    assert placed_orders(venue, final.client_order_id) == 1
    assert q.get(job_id)["attempts"] == 1          # waiting for an outcome is not a failed attempt


def test_retry_after_timeout_when_order_never_reached_venue(env):
    db, venue, executor, intents = env
    job_id = _order_job(db, intents)
    venue.fail_next("place_order", c.AdapterError(c.VENUE_UNAVAILABLE, "502"))
    q = JobQueue(db)
    p = JobProcessor(q, "w", {ORDER: order_job_handler(intents, executor)})
    assert p.run_once() == [(job_id, "RETRY")]
    assert intents.load("sig-1:acct").status == "NOT_SUBMITTED"
    with db.transaction() as conn:
        conn.execute("UPDATE platform_jobs SET next_attempt_at=0 WHERE job_id=?", (job_id,))
    assert p.run_once() == [(job_id, "SUCCEEDED")]
    assert executor.submits == 2 and placed_orders(venue, intents.load("sig-1:acct").client_order_id) == 1


def test_duplicate_delivery_and_out_of_order_recovery(env):
    """The same business intent delivered by two jobs (an upstream retry with a different job key) and processed
    in reverse order produces one venue order."""
    db, venue, executor, intents = env
    _order_job(db, intents)
    q = JobQueue(db)
    late = q.enqueue(ORDER, "redelivery-2", {"intent_key": "sig-1:acct"}, account_id="acct", tenant_id="tenant",
                     priority=10)[0]
    p = JobProcessor(q, "w", {ORDER: order_job_handler(intents, executor)})
    results = dict(p.run_once())
    assert set(results.values()) == {"SUCCEEDED"} and late in results
    assert executor.submits == 1
    assert placed_orders(venue, intents.load("sig-1:acct").client_order_id) == 1


def test_takeover_mid_job_with_slow_original_worker(env):
    db, venue, executor, intents = env
    job_id = _order_job(db, intents)
    q = JobQueue(db, lease_ms=150)
    [slow] = q.claim("slow")
    time.sleep(0.2)
    JobProcessor(JobQueue(db, lease_ms=150), "fast", {ORDER: order_job_handler(intents, executor)}).run_once()
    assert intents.execute("sig-1:acct", executor).status == "ACKNOWLEDGED"   # slow worker resumes: no-op
    with pytest.raises(StaleJobLease):
        q.complete(slow)
    assert executor.submits == 1 and q.get(job_id)["status"] == "SUCCEEDED"


def test_poison_message_goes_to_dead_letter_and_can_be_requeued(platform_db):
    q = JobQueue(platform_db)
    bad = q.enqueue(NOTIFICATION, "poison", {"malformed": True})[0]
    flaky = q.enqueue(NOTIFICATION, "always-fails", {}, max_attempts=3)[0]

    def handler(job):
        if job.payload.get("malformed"):
            raise PermanentJobError("payload schema invalid")
        raise RuntimeError("smtp down")

    p = JobProcessor(q, "w", {NOTIFICATION: handler})
    for _ in range(3):
        p.run_once()
        with platform_db.transaction() as conn:
            conn.execute("UPDATE platform_jobs SET next_attempt_at=0 WHERE status='RETRY'")
    assert q.get(bad)["status"] == "DEAD" and q.get(flaky)["status"] == "DEAD"
    assert q.get(flaky)["attempts"] == 3 and "smtp down" in q.get(flaky)["failure_reason"]
    assert q.backlog()["dead"] == 2
    q.requeue_dead(flaky, operator="ops@example.test", reason="SMTP restored")
    assert q.get(flaky)["status"] == "RETRY"


def test_lost_leases_count_toward_poison_limit(platform_db):
    q = JobQueue(platform_db, lease_ms=50)
    job = q.enqueue(NOTIFICATION, "crashes-worker", {}, max_attempts=2)[0]
    for _ in range(2):
        assert q.claim("w")
        time.sleep(0.07)                          # the worker crashes every time it runs this job
    assert q.claim("w") == []
    assert q.get(job)["status"] == "DEAD"


def test_notification_failure_retries_then_succeeds(platform_db):
    q = JobQueue(platform_db)
    job = q.enqueue(NOTIFICATION, "n", {})[0]
    calls = []

    def handler(j):
        calls.append(j.attempts)
        if len(calls) == 1:
            raise ConnectionError("push gateway 503")
        return {"delivered": True}

    p = JobProcessor(q, "w", {NOTIFICATION: handler})
    assert p.run_once() == [(job, "RETRY")]
    assert p.run_once() == []                      # backoff: not due yet
    with platform_db.transaction() as conn:
        conn.execute("UPDATE platform_jobs SET next_attempt_at=0 WHERE job_id=?", (job,))
    assert p.run_once() == [(job, "SUCCEEDED")] and q.get(job)["completion_evidence"] == {"delivered": True}


def test_delayed_shadow_comparison_waits_until_eligible(platform_db):
    q = JobQueue(platform_db)
    job = q.enqueue(SHADOW_COMPARISON, "cmp:2026-10-17", {"day": "2026-10-17"}, delay_ms=300)[0]
    p = JobProcessor(q, "w", {SHADOW_COMPARISON: lambda j: {"compared": j.payload["day"]}})
    assert p.run_once() == []
    time.sleep(0.35)
    assert p.run_once() == [(job, "SUCCEEDED")]


def test_shutdown_and_restart_loses_no_job_and_duplicates_none(database_url, platform_db):
    q = JobQueue(platform_db)
    ids = [q.enqueue(NOTIFICATION, f"n{i}", {"i": i})[0] for i in range(50)]
    effects = []
    JobProcessor(q, "w1", {NOTIFICATION: lambda j: effects.append(j.payload["i"])}).run_once(limit=20)
    platform_db.close()                            # process stops
    reopened = Database(database_url)              # new process, same database
    p = JobProcessor(JobQueue(reopened), "w2", {NOTIFICATION: lambda j: effects.append(j.payload["i"])})
    while p.run_once(limit=20):
        pass
    assert sorted(effects) == list(range(50))
    with reopened.transaction(write=False) as conn:
        assert conn.scalar("SELECT COUNT(*) FROM platform_jobs WHERE status='SUCCEEDED'") == len(ids)
    reopened.close()


def test_no_transaction_is_open_during_the_venue_call(env):
    db, venue, executor, intents = env
    _order_job(db, intents)
    observed = []

    def probe():
        # A second writer must get the write lock while the venue call is in flight.
        with db.transaction() as conn:
            conn.execute("UPDATE platform_accounts SET venue=venue WHERE account_id='acct'")
            if db.dialect == "postgresql":
                observed.append(conn.scalar(
                    "SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() "
                    "AND state='idle in transaction' AND pid <> pg_backend_pid()"))
            else:
                observed.append(0)

    executor.on_submit = probe
    db.pool._timeout = 3
    JobProcessor(JobQueue(db), "w", {ORDER: order_job_handler(intents, executor)}).run_once()
    assert observed == [0]
