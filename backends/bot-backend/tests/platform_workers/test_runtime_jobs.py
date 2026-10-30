"""Step 5.3 integration: the production job handlers (``app.platform_workers.runtime_jobs``).

Each handler is driven through the real queue (both dialects) under the failures the queue can deliver:
duplicate delivery, a crash mid-way, lease takeover, processing timeout, unknown broker acknowledgement,
out-of-order delivery, poison messages and permanent rejection. Exactly-once is proven from DURABLE state --
the fake venue's CREATE calls, the attempt history, the reservation, the outbox rows, the comparison rows --
never from the queue's own bookkeeping.
"""
from __future__ import annotations

import threading
import time

import pytest

from _runtime_jobs_kit import (ACCOUNT, TENANT, FakeServices, Platform, ShadowWorld, attempt_history, attempt_id_of,
                               creates, run_in_thread, unknown_entry, venue_reads, venue_shows_cancel,
                               venue_shows_fill)
from app.platform_workers import runtime_jobs as rj
from app.platform_workers.jobs import (NOTIFICATION, OPERATIONAL_RETRY, ORDER, RECONCILIATION, SHADOW_COMPARISON,
                                       JobQueue, StaleJobLease)


@pytest.fixture
def world(platform_db, tmp_path):
    h = unknown_entry(tmp_path)
    platform = Platform(platform_db)
    services = FakeServices(h.db, h=h)
    wid = platform.worker("w1")
    return h, platform, services, platform.runner(h.db, services, wid), wid


def _order_job(platform, h):
    created = rj.enqueue_unresolved_orders(JobQueue(platform.db), h.db, ACCOUNT)
    assert created == 1
    return JobQueue(platform.db).find(rj.order_resolution_key(ACCOUNT, attempt_id_of(h)))["job_id"]


def _status(platform, job_id):
    return JobQueue(platform.db).get(job_id)


# ══════════════════════════════════════════════════════════════════════════════════════════════ ORDER
def test_order_job_identity_is_stable_and_discovery_is_idempotent(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    assert rj.unresolved_attempts(h.db, ACCOUNT)[0]["execution_attempt_id"] == attempt_id_of(h)
    assert rj.enqueue_unresolved_orders(q, h.db, ACCOUNT) == 1
    for _ in range(3):                                    # every loop, every worker: still one job
        assert rj.enqueue_unresolved_orders(q, h.db, ACCOUNT) == 0
    jobs = platform.jobs(ORDER)
    assert len(jobs) == 1 and jobs[0]["idempotency_key"] == f"order-resolution:{ACCOUNT}:{attempt_id_of(h)}"
    assert jobs[0]["payload"] == {"op": rj.OP_RESOLVE_ATTEMPT, "trade_plan_id": h.plan.trade_plan_id,
                                  "execution_attempt_id": attempt_id_of(h)}
    assert jobs[0]["tenant_id"] == TENANT and jobs[0]["account_id"] == ACCOUNT


def test_unknown_acknowledgement_is_resolved_from_readback_and_never_resubmitted(world):
    """The fake venue timed out on every read after the submit; later its readback shows the fill."""
    h, platform, services, runner, _ = world
    job = _order_job(platform, h)
    assert runner.run_once() == [(job, "RETRY")]                       # venue still not answering
    row = _status(platform, job)
    assert row["attempts"] == 0 and "BROKER_STATE_STILL_UNKNOWN" in row["failure_reason"]
    assert attempt_history(h)[-1] == "SUBMIT_UNKNOWN" and h.reservation_status() == "RESOLUTION_PENDING"
    venue_shows_fill(h)
    platform.due_now()
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert attempt_history(h)[-1] == "RECONCILED_POSITION_EXISTS" and h.reservation_status() == "CONSUMED"
    assert creates(h) == 1                                              # the original submit only: 0 by the job
    assert _status(platform, job)["completion_evidence"]["outcome"] == "RESOLVED"


def test_duplicate_delivery_after_resolution_reads_nothing_and_changes_nothing(world):
    h, platform, services, runner, _ = world
    job = _order_job(platform, h)
    venue_shows_fill(h)
    assert runner.run_once() == [(job, "SUCCEEDED")]
    history, reads = attempt_history(h), venue_reads(h)
    # the same business work delivered again: a requeued job and a second job naming the same attempt
    q = JobQueue(platform.db)
    with platform.db.transaction() as conn:
        conn.execute("UPDATE platform_jobs SET status='RETRY', next_attempt_at=0 WHERE job_id=?", (job,))
    again = q.enqueue(ORDER, "redelivered-by-upstream", _status(platform, job)["payload"], account_id=ACCOUNT,
                      tenant_id=TENANT)[0]
    assert sorted(o for _, o in runner.run_once()) == ["SUCCEEDED", "SUCCEEDED"]
    for jid in (job, again):
        assert _status(platform, jid)["completion_evidence"]["outcome"] == "ALREADY_RESOLVED"
    assert attempt_history(h) == history and venue_reads(h) == reads and creates(h) == 1


def test_handler_raising_mid_way_is_retried_and_resolves_once(world):
    h, platform, services, runner, _ = world
    job = _order_job(platform, h)
    venue_shows_fill(h)
    services.runtime_failures = 1                                      # the venue connection drops mid-handler
    assert runner.run_once() == [(job, "RETRY")]
    assert _status(platform, job)["attempts"] == 1 and "ConnectionError" in _status(platform, job)["failure_reason"]
    platform.due_now()
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert attempt_history(h).count("RECONCILED_POSITION_EXISTS") == 1 and creates(h) == 1


def test_worker_crash_after_claim_is_reclaimed_after_lease_expiry(world):
    h, platform, services, runner, wid = world
    job = _order_job(platform, h)
    venue_shows_fill(h)
    [lost] = JobQueue(platform.db, lease_ms=60_000).claim(wid)          # the process dies holding the job
    assert runner.run_once() == []                                      # not claimable while the lease runs
    platform.expire_leases()
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert _status(platform, job)["attempts"] == 2                      # the lost claim counted
    with pytest.raises(StaleJobLease):
        JobQueue(platform.db).complete(lost)                            # the dead claim's late completion
    assert attempt_history(h).count("RECONCILED_POSITION_EXISTS") == 1 and creates(h) == 1


def test_lease_takeover_refuses_the_stale_token(world):
    h, platform, services, runner, wid = world
    job = _order_job(platform, h)
    venue_shows_fill(h)
    q = JobQueue(platform.db)
    handler = runner.handlers[ORDER]
    [stale] = q.claim("slow-worker")
    platform.expire_leases()
    [fresh] = q.claim(wid)                                              # the successor holds the lease now
    # the slow worker resumes while the entry is still unresolved: its lease check stops it before the venue
    with pytest.raises(StaleJobLease):
        handler(stale)
    assert venue_reads(h) == 0 and services.boundaries == 0
    evidence = handler(fresh)
    q.complete(fresh, evidence)
    assert evidence["outcome"] == "RESOLVED"
    # ... and once resolved, a late replay of the stale delivery reads only durable state, and is refused
    late = handler(stale)
    assert late["outcome"] == "ALREADY_RESOLVED" and late["venue_calls"] == 0
    for refuse in (lambda: q.complete(stale, late), lambda: q.fail(stale, "late"), lambda: q.heartbeat(stale)):
        with pytest.raises(StaleJobLease):
            refuse()
    assert _status(platform, job)["status"] == "SUCCEEDED" and _status(platform, job)["completion_evidence"] == evidence
    assert attempt_history(h).count("RECONCILED_POSITION_EXISTS") == 1 and creates(h) == 1


def test_processing_timeout_lets_the_account_owner_take_over_and_the_slow_worker_is_stale(platform_db, tmp_path):
    """Worker A hangs inside the venue read. Its keep-alive stops at the processing timeout, its account
    assignment expires, worker B takes over account and job and resolves the entry; when A wakes up its
    completion is refused and the attempt history holds one terminal record."""
    h = unknown_entry(tmp_path)
    ttl_s = 1.5
    platform = Platform(platform_db, ttl_ms=int(ttl_s * 1000))
    services = FakeServices(h.db, h=h)
    a = platform.worker("A")
    runner_a = platform.runner(h.db, services, a, lease_ms=300, max_processing_ms=200)
    job = _order_job(platform, h)
    venue_shows_fill(h)
    filled = h.client.get_order.side_effect
    entered, release = threading.Event(), threading.Event()

    def get_order(symbol, order_id):
        if threading.current_thread().name == "worker-A":
            entered.set()
            assert release.wait(20)
        return filled(symbol, order_id)

    h.client.get_order.side_effect = get_order
    platform.assignments.claim(a)                                       # A's last renewal before it hangs
    renewed = time.monotonic()
    thread, box = run_in_thread(runner_a.run_once, "worker-A")
    assert entered.wait(10)
    # A renews nothing from here: wait out its assignment TTL (and with it the keep-alive bound and job lease)
    time.sleep(max(0.0, renewed + ttl_s - time.monotonic()) + 0.3)
    b = platform.worker("B")
    assert platform.owns(b)
    runner_b = platform.runner(h.db, FakeServices(h.db, h=h), b, lease_ms=300)     # another process
    assert runner_b.run_once() == [(job, "SUCCEEDED")]
    release.set()
    thread.join(10)
    assert box["result"] == [(job, "STALE")]
    assert attempt_history(h).count("RECONCILED_POSITION_EXISTS") == 1
    assert h.reservation_status() == "CONSUMED" and creates(h) == 1
    assert _status(platform, job)["status"] == "SUCCEEDED"


def test_keepalive_holds_the_lease_inside_the_processing_timeout(platform_db, tmp_path):
    h = unknown_entry(tmp_path)
    platform = Platform(platform_db)
    services = FakeServices(h.db, h=h)
    a = platform.worker("A")
    runner = platform.runner(h.db, services, a, lease_ms=200, max_processing_ms=5_000)
    job = _order_job(platform, h)
    venue_shows_fill(h)
    services.on_runtime = lambda: time.sleep(0.6)                       # three leases long, inside the timeout
    other = JobQueue(platform.db, lease_ms=200)
    seen = []
    services_on = services.on_runtime

    def slow():
        services_on()
        seen.append(other.claim("intruder"))                           # the job is still leased: nothing to take
    services.on_runtime = slow
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert seen == [[]] and creates(h) == 1


def test_account_not_owned_is_retried_without_spending_an_attempt(world):
    h, platform, services, runner, _ = world
    job = _order_job(platform, h)
    stranger = platform.runner(h.db, services, platform.assignments.register_worker("w-other"))
    for _ in range(3):
        assert stranger.run_once() == [(job, "RETRY")]
        platform.due_now()
    row = _status(platform, job)
    assert row["attempts"] == 0 and "ACCOUNT_NOT_OWNED_BY_WORKER" in row["failure_reason"]
    assert services.boundaries == 0 and venue_reads(h) == 0


def test_a_fence_lost_before_the_venue_call_stops_the_handler(world):
    h, platform, services, runner, wid = world
    job = _order_job(platform, h)
    venue_shows_fill(h)
    reads = venue_reads(h)
    stale_fence = (ACCOUNT, wid, 0)                                     # an old fencing token
    runner.context.fence_for_account = lambda account_id: stale_fence
    assert runner.run_once() == [(job, "RETRY")]
    assert "ACCOUNT_ASSIGNMENT_LOST:ASSIGNMENT_TAKEN_OVER" in _status(platform, job)["failure_reason"]
    assert venue_reads(h) == reads and services.boundaries == 0


@pytest.mark.parametrize("payload", [
    {"intent_key": "sig-1:acct1"},                                      # the queue-submission path of jobs.py
    {"op": "SUBMIT", "trade_plan_id": "tp", "execution_attempt_id": "x"},
    {"op": rj.OP_RESOLVE_ATTEMPT, "trade_plan_id": "tp", "execution_attempt_id": "x", "side": "BUY",
     "quantity": "1"},
])
def test_an_order_job_asking_for_a_fresh_submission_is_dead_lettered(world, payload):
    h, platform, services, runner, _ = world
    job = JobQueue(platform.db).enqueue(ORDER, "fresh-order", payload, account_id=ACCOUNT, tenant_id=TENANT)[0]
    assert runner.run_once() == [(job, "DEAD")]
    row = _status(platform, job)
    assert rj.QUEUE_ORDER_SUBMISSION_NOT_PERMITTED in row["failure_reason"] and row["attempts"] == 1
    assert creates(h) == 1 and services.boundaries == 0 and venue_reads(h) == 0


@pytest.mark.parametrize("payload,code", [
    ({"op": rj.OP_RESOLVE_ATTEMPT, "trade_plan_id": "tp"}, "ORDER_JOB_MALFORMED:execution_attempt_id"),
    ({"op": "SOMETHING"}, "ORDER_JOB_UNSUPPORTED_OP"),
    ({"op": rj.OP_RESOLVE_ATTEMPT, "trade_plan_id": "tp", "execution_attempt_id": "nope"},
     "EXECUTION_ATTEMPT_NOT_FOUND"),
])
def test_poison_order_jobs_go_to_dead_letter_and_are_not_retried(world, payload, code):
    h, platform, services, runner, _ = world
    job = JobQueue(platform.db).enqueue(ORDER, f"poison-{code}", payload, account_id=ACCOUNT, tenant_id=TENANT)[0]
    assert runner.run_once() == [(job, "DEAD")]
    platform.due_now()
    assert runner.run_once() == []                                      # dead: never claimed again
    assert code in _status(platform, job)["failure_reason"] and services.boundaries == 0


def test_permanent_rejection_at_the_venue_releases_the_reservation_once(world):
    h, platform, services, runner, _ = world
    job = _order_job(platform, h)
    venue_shows_cancel(h)
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert attempt_history(h)[-1] == "RECONCILED_NO_POSITION" and h.reservation_status() == "RELEASED"
    assert creates(h) == 1 and rj.unresolved_attempts(h.db, ACCOUNT) == []


def test_reservation_left_pending_behind_a_terminal_attempt_is_surfaced_not_repaired(world):
    h, platform, services, runner, _ = world
    from dataclasses import replace
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    store = ExecutionAttemptStore(h.db)
    last = store.latest(ACCOUNT, attempt_id_of(h))
    from app.trading_intelligence.execution.boundary import CATIExecutionBoundary
    base = CATIExecutionBoundary._attempt_from_payload(h.plan, last["payload"])
    store.append(replace(base, status="FILLED"), len(attempt_history(h)))   # a crash between the two writes
    job = _order_job(platform, h)
    assert runner.run_once() == [(job, "DEAD")]
    assert "RESERVATION_PENDING_WITH_TERMINAL_ATTEMPT:FILLED" in _status(platform, job)["failure_reason"]
    assert h.reservation_status() == "RESOLUTION_PENDING" and creates(h) == 1


# ══════════════════════════════════════════════════════════════════════════════════════ RECONCILIATION
def test_reconciliation_runs_the_production_maintenance_and_raises_the_watermark(world):
    h, platform, services, runner, _ = world
    venue_shows_fill(h)
    q = JobQueue(platform.db)
    job, created = rj.enqueue_reconciliation(q, ACCOUNT, reason="TAKEOVER", requested_at=1_000)
    assert created and rj.enqueue_reconciliation(q, ACCOUNT, reason="TAKEOVER", requested_at=1_000) == (job, False)
    assert runner.run_once() == [(job, "SUCCEEDED")]
    evidence = _status(platform, job)["completion_evidence"]
    assert evidence["outcome"] == "RECONCILED" and evidence["maintained"] is True
    assert attempt_history(h)[-1] == "RECONCILED_POSITION_EXISTS" and h.reservation_status() == "CONSUMED"
    assert runner.context.watermark(rj.RECONCILIATION_SCOPE, ACCOUNT) == evidence["reconciled_from"]
    assert services.reconciliations == 1 and creates(h) == 1


def test_an_older_reconciliation_delivered_after_a_newer_one_is_superseded(world):
    h, platform, services, runner, _ = world
    venue_shows_fill(h)
    q = JobQueue(platform.db)
    newer = rj.enqueue_reconciliation(q, ACCOUNT, reason="FILL_EVENT")[0]       # requested now (db clock)
    assert runner.run_once() == [(newer, "SUCCEEDED")]
    mark = runner.context.watermark(rj.RECONCILIATION_SCOPE, ACCOUNT)
    older = rj.enqueue_reconciliation(q, ACCOUNT, reason="ORDER_EVENT", requested_at=mark - 5_000)[0]
    reads = venue_reads(h)
    assert runner.run_once() == [(older, "SUCCEEDED")]
    assert _status(platform, older)["completion_evidence"]["outcome"] == "SUPERSEDED"
    assert services.reconciliations == 1 and venue_reads(h) == reads
    assert runner.context.watermark(rj.RECONCILIATION_SCOPE, ACCOUNT) == mark      # never lowered


def test_the_watermark_only_rises(world):
    h, platform, services, runner, wid = world
    from app.platform_workers.jobs import Job
    fence = runner.context.fence_for_account(ACCOUNT)
    job = Job("j", "k", RECONCILIATION, 1, {}, ACCOUNT, TENANT, 1, 8, wid, 1, "CLAIMED")
    assert runner.context.advance_watermark("RECONCILIATION", job, fence, 5_000) == 5_000
    assert runner.context.advance_watermark("RECONCILIATION", job, fence, 4_000) == 5_000
    assert runner.context.advance_watermark("RECONCILIATION", job, fence, 6_000) == 6_000


def test_reconciliation_crash_before_the_watermark_repeats_harmlessly(world, monkeypatch):
    h, platform, services, runner, wid = world
    venue_shows_fill(h)
    job = rj.enqueue_reconciliation(JobQueue(platform.db), ACCOUNT, reason="RESTART", requested_at=1_000)[0]
    real = runner.context.advance_watermark

    def killed(*a, **k):
        raise RuntimeError("process killed after the venue work")
    monkeypatch.setattr(runner.context, "advance_watermark", killed)
    assert runner.run_once() == [(job, "RETRY")]                       # the work ran, the ack did not
    history = attempt_history(h)
    assert history.count("RECONCILED_POSITION_EXISTS") == 1 and h.reservation_status() == "CONSUMED"
    monkeypatch.setattr(runner.context, "advance_watermark", real)
    platform.due_now()
    assert runner.run_once() == [(job, "SUCCEEDED")]                   # runs again on current venue truth
    assert attempt_history(h) == history and creates(h) == 1 and services.reconciliations == 2


def test_reconciliation_crash_after_the_watermark_is_superseded_on_redelivery(world):
    h, platform, services, runner, wid = world
    venue_shows_fill(h)
    job = rj.enqueue_reconciliation(JobQueue(platform.db), ACCOUNT, reason="RESTART", requested_at=1_000)[0]
    [claimed] = JobQueue(platform.db).claim(wid)
    runner.handlers[RECONCILIATION](claimed)                            # the work and its watermark complete ...
    history, reads = attempt_history(h), venue_reads(h)                 # ... and the process dies before the ack
    platform.expire_leases()
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert _status(platform, job)["completion_evidence"]["outcome"] == "SUPERSEDED"
    assert attempt_history(h) == history and venue_reads(h) == reads
    assert creates(h) == 1 and services.reconciliations == 1


def test_poison_and_unowned_reconciliation_jobs(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    poison = q.enqueue(RECONCILIATION, "rec-poison", {"reason": "x"}, account_id=ACCOUNT, tenant_id=TENANT)[0]
    no_account = q.enqueue(RECONCILIATION, "rec-no-account", {"requested_at": 5})[0]
    results = dict(runner.run_once())
    assert results == {poison: "DEAD", no_account: "DEAD"}
    assert "RECONCILIATION_JOB_MALFORMED" in _status(platform, poison)["failure_reason"]
    assert "RECONCILIATION_JOB_WITHOUT_ACCOUNT" in _status(platform, no_account)["failure_reason"]
    job = rj.enqueue_reconciliation(q, ACCOUNT, reason="X", requested_at=7)[0]
    stranger = platform.runner(h.db, services, platform.assignments.register_worker("w-other"))
    assert stranger.run_once() == [(job, "RETRY")] and _status(platform, job)["attempts"] == 0
    assert services.reconciliations == 0


# ═════════════════════════════════════════════════════════════════════════════════════════ NOTIFICATION
def _ops_job(q, delivery="d1", **payload):
    body = {"kind": rj.KIND_OPS_ALERT, "alert_id": "a1", "delivery_id": delivery, "severity": "HIGH",
            "subject": "Worker outage", "text": "worker w1 silent", **payload}
    return q.enqueue(NOTIFICATION, f"ops-alert-delivery:{delivery}", body)


def test_ops_alert_job_is_delivered_once_through_the_existing_handler(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    job = _ops_job(q)[0]
    assert _ops_job(q)[1] is False                                      # one job per engine delivery
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert runner.run_once() == []
    assert services.ops_sent == [("Worker outage", "worker w1 silent")]


def test_ops_alert_channel_failure_retries_then_delivers_once(world):
    h, platform, services, runner, _ = world
    job = _ops_job(JobQueue(platform.db))[0]
    services.ops_failures = 1
    assert runner.run_once() == [(job, "RETRY")]
    platform.due_now()
    assert runner.run_once() == [(job, "SUCCEEDED")] and len(services.ops_sent) == 1


def test_ops_alert_poison_and_unknown_kind_are_dead(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    no_subject = q.enqueue(NOTIFICATION, "ops-alert-delivery:bad", {"kind": rj.KIND_OPS_ALERT, "text": "x"})[0]
    unknown = q.enqueue(NOTIFICATION, "n-unknown", {"kind": "carrier_pigeon"})[0]
    assert dict(runner.run_once()) == {no_subject: "DEAD", unknown: "DEAD"}
    assert services.ops_sent == []


def test_a_stale_notification_job_is_not_sent(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    job = _ops_job(q)[0]
    [stale] = q.claim("slow")
    platform.expire_leases()
    assert runner.run_once() == [(job, "SUCCEEDED")]
    with pytest.raises(StaleJobLease):
        runner.handlers[NOTIFICATION](stale)                            # the slow worker resumes: no second send
    assert len(services.ops_sent) == 1


def _outbox_row(h, *, key="plan-1"):
    from app.observability import user_events as ue
    with h.db.connect() as c:
        c.execute("INSERT OR REPLACE INTO notification_preferences (user_id,channel,category,is_enabled,updated_at) "
                  "VALUES (?,?,?,1,?)", (TENANT, "email", "trade", "2026-10-01"))
        c.execute("INSERT OR REPLACE INTO notification_endpoints (user_id,channel,recipient,status,created_at) "
                  "VALUES (?,?,?,'active',?)", (TENANT, "email", "u1@example.test", "2026-10-01"))
    event = ue.emit(h.db, ue.ENTRY_FILLED, event_id=ue.event_id_for(ue.ENTRY_FILLED, key), user_id=TENANT,
                    bot_id=h.plan.bot_instance_id, broker_account_id=ACCOUNT, environment="DEMO",
                    payload={"symbol": "BTCUSDT", "message": "filled"}, now_ms=h.now)
    assert event is not None
    [row] = [r for r in ue.outbox(h.db, event_ids=[event["event_id"]]) if r["channel"] == "email"]
    return row["outbox_id"]


def _outbox(h, outbox_id):
    from app.observability import user_events as ue
    return [r for r in ue.outbox(h.db) if r["outbox_id"] == outbox_id][0]


def test_outbox_row_is_delivered_once_and_redelivery_is_a_no_op(world):
    h, platform, services, runner, _ = world
    services.now = lambda: h.now + 1_000
    outbox_id = _outbox_row(h)
    q = JobQueue(platform.db)
    assert rj.enqueue_outbox_notifications(q, h.db, now_ms=h.now + 1_000) == 1
    assert rj.enqueue_outbox_notifications(q, h.db, now_ms=h.now + 1_000) == 0
    job = q.find(rj.outbox_notification_key(outbox_id))["job_id"]
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert _outbox(h, outbox_id)["status"] == "sent" and len(services.sent) == 1
    with platform.db.transaction() as conn:                             # delivered again (lost acknowledgement)
        conn.execute("UPDATE platform_jobs SET status='RETRY', next_attempt_at=0 WHERE job_id=?", (job,))
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert _status(platform, job)["completion_evidence"]["outcome"] == "ALREADY_SENT" and len(services.sent) == 1


def test_outbox_job_and_the_notification_worker_never_both_send(world):
    from app.observability import user_events as ue
    h, platform, services, runner, _ = world
    now = h.now + 1_000
    services.now = lambda: now
    outbox_id = _outbox_row(h)
    job = rj.enqueue_outbox_notification(JobQueue(platform.db), outbox_id)[0]
    worker_sent = []
    with h.db.connect() as c:                                           # the job claimed the row, then died
        assert ue._claim(c, outbox_id, now)
    counts = ue.deliver_pending(h.db, now_ms=now, sender=lambda *a: worker_sent.append(a) or True)
    assert counts["sent"] == 0 and worker_sent == []                    # the worker does not take a claimed row
    assert runner.run_once() == [(job, "RETRY")]                        # nor does another job: NOT_DUE
    assert _status(platform, job)["attempts"] == 0
    services.now = lambda: now + ue.DELIVERY_CLAIM_MS + 1              # the dead claim lapses
    platform.due_now()
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert len(services.sent) == 1 and _outbox(h, outbox_id)["status"] == "sent"
    later = ue.deliver_pending(h.db, now_ms=now + 10 * ue.DELIVERY_CLAIM_MS, sender=lambda *a: worker_sent.append(a) or True)
    assert later["sent"] == 0 and worker_sent == []


def test_outbox_failure_follows_the_outbox_backoff_then_dead_letters_when_the_outbox_gives_up(world):
    from app.observability import user_events as ue
    h, platform, services, runner, _ = world
    clock = [h.now + 1_000]
    services.now = lambda: clock[0]
    outbox_id = _outbox_row(h)
    job = rj.enqueue_outbox_notification(JobQueue(platform.db), outbox_id)[0]
    services.send_results = [False] * ue.MAX_ATTEMPTS
    for attempt in range(1, ue.MAX_ATTEMPTS):
        assert runner.run_once() == [(job, "RETRY")]
        row = _outbox(h, outbox_id)
        assert row["attempts"] == attempt and row["status"] == "pending"
        assert _status(platform, job)["attempts"] == 0                  # the outbox counts attempts, not the queue
        clock[0] = int(row["next_attempt_at"])
        platform.due_now()
    assert runner.run_once() == [(job, "DEAD")]
    assert _outbox(h, outbox_id)["status"] == "failed" and services.sent == []
    assert "OUTBOX_DELIVERY_FAILED" in _status(platform, job)["failure_reason"]


def test_outbox_poison_jobs(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    missing = q.enqueue(NOTIFICATION, "n-missing", {"kind": rj.KIND_USER_EVENT, "outbox_id": "obx_nope"})[0]
    malformed = q.enqueue(NOTIFICATION, "n-malformed", {"kind": rj.KIND_USER_EVENT})[0]
    assert dict(runner.run_once()) == {missing: "DEAD", malformed: "DEAD"}
    assert "OUTBOX_ROW_NOT_FOUND" in _status(platform, missing)["failure_reason"]


# ═══════════════════════════════════════════════════════════════════════════════════ SHADOW_COMPARISON
@pytest.fixture
def shadow(platform_db, tmp_path):
    from _exec import Harness  # noqa: F401  (the trading side is not needed: a shadow-only legacy database)
    import _step3
    db = _step3.fresh_db(tmp_path)
    world = ShadowWorld(db, tmp_path)
    platform = Platform(platform_db)
    services = FakeServices(db)
    services.shadow = world.source
    wid = platform.worker("w-shadow")
    runner = platform.runner(db, services, wid, categories=[SHADOW_COMPARISON])
    return world, platform, services, runner


def test_scheduler_enqueues_the_due_day_once_from_every_worker(shadow):
    world, platform, services, runner = shadow
    day = world.start + 1
    now = world.clock_for(day)
    queues = [JobQueue(platform.db) for _ in range(3)]
    results = [rj.schedule_shadow_comparison(q, now_ms=now) for q in queues for _ in range(2)]
    assert sum(created for _, created in results) == 1 and len({j for j, _ in results}) == 1
    [job] = platform.jobs(SHADOW_COMPARISON)
    assert job["idempotency_key"] == f"shadow-comparison:DAILY_TREND:DAILY_FROZEN_EVALUATOR:{world.DT.day_text(day)}"


def test_the_daily_comparison_is_recorded_once_by_the_existing_function(shadow):
    world, platform, services, runner = shadow
    day = world.start + 1
    assert world.run(world.start).status == "COMPLETED" and world.run(day).status == "COMPLETED"
    assert world.rows() == []                                           # the driver here does not compare
    services.now = lambda: world.clock_for(day)
    job = rj.schedule_shadow_comparison(JobQueue(platform.db), now_ms=world.clock_for(day))[0]
    assert runner.run_once() == [(job, "SUCCEEDED")]
    rows = world.rows(day)
    assert rows and {r[2] for r in rows} == {rj.SHADOW_KIND_DAILY} and all(r[3] == "MATCH" for r in rows)
    assert len({r[0] for r in rows}) == len(rows) == 2                  # one row per model book
    assert _status(platform, job)["completion_evidence"]["outcome"] == "RECORDED"
    with platform.db.transaction() as conn:                             # delivered again
        conn.execute("UPDATE platform_jobs SET status='RETRY', next_attempt_at=0 WHERE job_id=?", (job,))
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert _status(platform, job)["completion_evidence"]["outcome"] == "ALREADY_RECORDED" and world.rows(day) == rows


def test_a_day_the_inline_comparison_already_recorded_is_not_recomputed(shadow, monkeypatch):
    world, platform, services, runner = shadow
    day = world.start + 1
    world.run(world.start, compare_models=True)
    world.run(day, compare_models=True)
    rows = world.rows(day)
    assert rows
    from app.trading_intelligence.pipeline import daily_trend
    monkeypatch.setattr(daily_trend.DailyTrendPipeline, "compare_models",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("recomputed")))
    services.now = lambda: world.clock_for(day)
    job = rj.schedule_shadow_comparison(JobQueue(platform.db), now_ms=world.clock_for(day))[0]
    assert runner.run_once() == [(job, "SUCCEEDED")]
    assert _status(platform, job)["completion_evidence"]["outcome"] == "ALREADY_RECORDED" and world.rows(day) == rows


def test_a_comparison_waits_for_its_cycle_and_survives_a_restart(shadow, database_url):
    world, platform, services, runner = shadow
    day = world.start + 1
    services.now = lambda: world.clock_for(day)
    job = rj.schedule_shadow_comparison(JobQueue(platform.db), now_ms=world.clock_for(day))[0]
    assert runner.run_once() == [(job, "RETRY")]                        # the day's cycle has not run yet
    assert _status(platform, job)["attempts"] == 0
    assert "SHADOW_CYCLE_NOT_COMPLETED" in _status(platform, job)["failure_reason"]
    runner.request_stop()                                               # the process stops
    world.run(world.start)
    world.run(day)
    from shared_lib.persistence.sql import Database
    reopened = Database(database_url)
    try:
        restarted = rj.RuntimeJobRunner(world.db, reopened, worker_id=platform.worker("w-restarted"),
                                        fence_for_account=lambda a: None, services=services,
                                        categories=[SHADOW_COMPARISON], max_processing_ms=None)
        with reopened.transaction() as conn:
            conn.execute("UPDATE platform_jobs SET next_attempt_at=0 WHERE status='RETRY'")
        assert restarted.run_once() == [(job, "SUCCEEDED")]
        assert len(world.rows(day)) == 2
    finally:
        reopened.close()


def test_a_day_that_never_completes_is_dead_lettered_after_the_bound_and_poison_is_dead(shadow):
    world, platform, services, runner = shadow
    day = world.start + 1
    services.now = lambda: world.DT.close_ms(day) + rj.SHADOW_CYCLE_WAIT_BOUND_MS + 1
    q = JobQueue(platform.db)
    stuck = rj.enqueue_shadow_comparison(q, day=world.DT.day_text(day))[0]
    bad_day = q.enqueue(SHADOW_COMPARISON, "sc-bad", {"family_id": "DAILY_TREND", "kind": rj.SHADOW_KIND_DAILY,
                                                      "day": "yesterday"})[0]
    bad_kind = q.enqueue(SHADOW_COMPARISON, "sc-kind", {"family_id": "DAILY_TREND", "kind": "MONTHLY",
                                                        "day": world.DT.day_text(day)})[0]
    assert dict(runner.run_once()) == {stuck: "DEAD", bad_day: "DEAD", bad_kind: "DEAD"}
    assert "SHADOW_CYCLE_NOT_COMPLETED_WITHIN_BOUND" in _status(platform, stuck)["failure_reason"]
    assert world.rows() == []


# ═══════════════════════════════════════════════════════════════════════════════════ OPERATIONAL_RETRY
def test_operational_work_is_enqueued_only_when_due_with_stable_keys(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    assert rj.operational_work(h.db, ACCOUNT, now_ms=h.now + 60_000) == []      # pending, not yet overdue
    assert rj.schedule_operational_retries(q, h.db, ACCOUNT, now_ms=h.now + 60_000) == 0
    now = (h.now // rj.OPERATIONAL_PERIOD_MS + 4) * rj.OPERATIONAL_PERIOD_MS    # past the resolution deadline
    assert rj.operational_work(h.db, ACCOUNT, now_ms=now) == [rj.TASK_RECOVER_PENDING]
    assert rj.schedule_operational_retries(q, h.db, ACCOUNT, now_ms=now) == 1
    assert rj.schedule_operational_retries(q, h.db, ACCOUNT, now_ms=now + 1) == 0      # same period: same job
    [job] = platform.jobs(OPERATIONAL_RETRY)
    assert job["idempotency_key"] == f"operational-retry:{rj.TASK_RECOVER_PENDING}:{ACCOUNT}:{now // rj.OPERATIONAL_PERIOD_MS}"


def test_recover_pending_task_resolves_from_broker_truth_and_then_has_nothing_to_do(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    now = h.now + 60_000
    venue_shows_fill(h)
    first = rj.enqueue_operational_retry(q, rj.TASK_RECOVER_PENDING, ACCOUNT, bucket=1)[0]
    second = rj.enqueue_operational_retry(q, rj.TASK_RECOVER_PENDING, ACCOUNT, bucket=2)[0]
    # Two jobs enqueued in the same database millisecond have no defined claim order (job_id breaks the tie):
    # whichever runs first resolves from broker truth, the other finds nothing left to do.
    [(ran, outcome)] = runner.run_once(limit=1)
    assert ran in (first, second) and outcome == "SUCCEEDED"
    assert _status(platform, ran)["completion_evidence"]["results"][0]["status"] == "RECONCILED"
    other = second if ran == first else first
    reads = venue_reads(h)
    assert runner.run_once() == [(other, "SUCCEEDED")]
    assert _status(platform, other)["completion_evidence"]["outcome"] == "NOTHING_PENDING"
    assert venue_reads(h) == reads and creates(h) == 1 and h.reservation_status() == "CONSUMED"
    assert rj.operational_work(h.db, ACCOUNT, now_ms=now) == []


def test_recover_pending_escalates_an_overdue_unknown_and_keeps_ownership(world):
    h, platform, services, runner, _ = world
    services.now = lambda: h.now + 3_600_000                             # past the resolution deadline
    job = rj.enqueue_operational_retry(JobQueue(platform.db), rj.TASK_RECOVER_PENDING, ACCOUNT, bucket=1)[0]
    assert runner.run_once() == [(job, "SUCCEEDED")]
    [result] = _status(platform, job)["completion_evidence"]["results"]
    assert result["status"] == "STILL_UNKNOWN" and "SUBMIT_OUTCOME_UNRESOLVED" in result["reason_codes"]
    assert h.reservation_status() == "RESOLUTION_PENDING" and creates(h) == 1


def test_expire_stale_reservations_task_is_a_compare_and_set(world):
    h, platform, services, runner, _ = world
    from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
    with h.db.connect() as c:                                           # an ordinary reservation that lapsed
        c.execute("UPDATE cati_portfolio_reservations SET status='RESERVED', expires_at=created_at+1 "
                  "WHERE reservation_id=?", (h.plan.portfolio_reservation_id,))
        created = c.execute("SELECT created_at FROM cati_portfolio_reservations WHERE reservation_id=?",
                            (h.plan.portfolio_reservation_id,)).fetchone()[0]
    now = int(created) + 60_000
    services.now = lambda: now
    assert rj.TASK_EXPIRE_RESERVATIONS in rj.operational_work(h.db, ACCOUNT, now_ms=now)
    q = JobQueue(platform.db)
    jobs = [rj.enqueue_operational_retry(q, rj.TASK_EXPIRE_RESERVATIONS, ACCOUNT, bucket=b)[0] for b in (1, 2)]
    assert [o for _, o in runner.run_once()] == ["SUCCEEDED", "SUCCEEDED"]
    assert sorted(_status(platform, j)["completion_evidence"]["expired"] for j in jobs) == [0, 1]
    assert CATIReservationStore(h.db).get(h.plan.portfolio_reservation_id).status == "EXPIRED"


def test_verify_protection_task_only_reads_the_venue_when_a_position_is_uncertain(world):
    h, platform, services, runner, _ = world
    from app.execution import protection_state
    q = JobQueue(platform.db)
    idle = rj.enqueue_operational_retry(q, rj.TASK_VERIFY_PROTECTION, ACCOUNT, bucket=1)[0]
    assert runner.run_once() == [(idle, "SUCCEEDED")]
    assert _status(platform, idle)["completion_evidence"]["outcome"] == "NOTHING_TO_VERIFY"
    assert services.reconciliations == 0
    venue_shows_fill(h)
    protection_state.record_unknown(h.db, ACCOUNT, "BTCUSDT", h.plan.trade_plan_id, "READ_TIMEOUT", h.now)
    assert rj.TASK_VERIFY_PROTECTION in rj.operational_work(h.db, ACCOUNT, now_ms=h.now)
    busy = rj.enqueue_operational_retry(q, rj.TASK_VERIFY_PROTECTION, ACCOUNT, bucket=2)[0]
    assert runner.run_once() == [(busy, "SUCCEEDED")]
    assert _status(platform, busy)["completion_evidence"]["outcome"] == "RECONCILED"
    assert services.reconciliations == 1 and creates(h) == 1


def test_unknown_operational_task_is_dead(world):
    h, platform, services, runner, _ = world
    job = JobQueue(platform.db).enqueue(OPERATIONAL_RETRY, "op-bad", {"task": "REBOOT_EXCHANGE"}, account_id=ACCOUNT,
                                        tenant_id=TENANT)[0]
    assert runner.run_once() == [(job, "DEAD")]
    with pytest.raises(ValueError):
        rj.enqueue_operational_retry(JobQueue(platform.db), "REBOOT_EXCHANGE", ACCOUNT, bucket=1)


# ═════════════════════════════════════════════════════════════════════════════════ role, registry, drain
def test_an_api_process_cannot_build_the_runner_or_the_handlers(world, monkeypatch):
    h, platform, services, runner, wid = world
    monkeypatch.setenv("PROCESS_ROLE", "api")
    with pytest.raises(rj.RuntimeJobsRefused, match="RUNTIME_JOBS_REFUSED_IN_API_ROLE"):
        platform.runner(h.db, services, wid)
    with pytest.raises(rj.RuntimeJobsRefused):
        rj.production_handlers(h.db, platform.db, worker_id=wid, fence_for_account=lambda a: None)
    monkeypatch.setenv("PROCESS_ROLE", "trading")
    assert set(platform.runner(h.db, services, wid).handlers) == set(rj.CATEGORIES)


def test_production_gates_refuse_outside_the_production_profile(world):
    """The default services never reach a venue from a process without the production profile."""
    h, platform, services, runner, wid = world
    default = platform.runner(h.db, rj.RuntimeServices(h.db), wid)
    job = _order_job(platform, h)
    venue_shows_fill(h)
    assert default.run_once() == [(job, "RETRY")]
    assert "CATI_PRODUCTION_PROFILE_REQUIRED" in _status(platform, job)["failure_reason"]
    assert _status(platform, job)["attempts"] == 0 and venue_reads(h) == 0


def test_drain_hands_back_claimed_jobs_that_have_not_started(world):
    h, platform, services, runner, _ = world
    q = JobQueue(platform.db)
    jobs = [_ops_job(q, delivery=f"d{i}")[0] for i in range(3)]
    started = []

    def send_text(subject, text):
        started.append(subject)
        runner.request_stop()                                           # SIGTERM arrives during the first job
    services.ops_channels = lambda: __import__("types").SimpleNamespace(send_text=send_text)
    results = dict(runner.run_once())
    assert sorted(results.values()) == ["RELEASED", "RELEASED", "SUCCEEDED"] and len(started) == 1
    for jid, outcome in results.items():
        if outcome == "RELEASED":
            row = _status(platform, jid)
            assert row["status"] == "RETRY" and row["attempts"] == 0     # due now, no attempt spent
    assert runner.run_once() == [] and runner.schedule([(ACCOUNT, TENANT)]) == {"enqueued": 0, "errors": []}
    assert set(jobs) == set(results)
