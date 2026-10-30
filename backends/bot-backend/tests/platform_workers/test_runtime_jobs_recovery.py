"""Step 5.3 integration: crash and restart of the trading runtime's job processing, end to end.

One trading account on a fake venue, a disposable legacy runtime database and a platform database of each
dialect. A REAL boundary submit ends SUBMIT_UNKNOWN (the venue accepted the order, every read of it timed out);
the runtime enqueues its work (order resolution, reconciliation, a customer notification, the day's shadow
comparison, recovery); the process crashes holding all of it. A new process on new connections takes the
account over and finishes every job from durable state and broker truth. Nothing is lost, nothing duplicated:
the venue saw ONE order, the attempt resolved once, the notification was sent once, the comparison recorded
once per model book. Shutdown with jobs in flight loses nothing either.
"""
from __future__ import annotations

import threading
import time

import pytest

from _runtime_jobs_kit import (ACCOUNT, TENANT, FakeServices, Platform, ShadowWorld, attempt_history, attempt_id_of,
                               creates, run_in_thread, unknown_entry, venue_reads, venue_shows_fill)
from app.platform_workers import runtime_jobs as rj
from app.platform_workers.jobs import (NOTIFICATION, OPERATIONAL_RETRY, ORDER, RECONCILIATION, SHADOW_COMPARISON,
                                       JobQueue)
from app.platform_workers.worker import TradingWorker
from shared_lib.persistence.sql import Database

TRADING = [ORDER, RECONCILIATION, NOTIFICATION, OPERATIONAL_RETRY]


def _customer_event(h):
    from app.observability import user_events as ue
    with h.db.connect() as c:
        c.execute("INSERT OR REPLACE INTO notification_preferences (user_id,channel,category,is_enabled,updated_at) "
                  "VALUES (?,?,?,1,?)", (TENANT, "telegram", "trade", "2026-10-01"))
        c.execute("INSERT OR REPLACE INTO notification_endpoints (user_id,channel,recipient,status,created_at) "
                  "VALUES (?,?,?,'active',?)", (TENANT, "telegram", "chat-1", "2026-10-01"))
    event = ue.emit(h.db, ue.ENTRY_FILLED, event_id=ue.event_id_for(ue.ENTRY_FILLED, "crash-test"), user_id=TENANT,
                    bot_id=h.plan.bot_instance_id, broker_account_id=ACCOUNT, environment="DEMO",
                    payload={"symbol": "BTCUSDT", "message": "entry filled"}, now_ms=h.now)
    [row] = [r for r in ue.outbox(h.db, event_ids=[event["event_id"]]) if r["channel"] == "telegram"]
    return row["outbox_id"]


def _drain_all(runners, platform, *, rounds=8):
    """Run every runner until no job is left to do (backoffs are made due at once)."""
    out = []
    for _ in range(rounds):
        done = [r for runner in runners for r in runner.run_once(limit=50)]
        out += done
        platform.due_now()
        if not done:
            break
    return out


def test_crash_and_restart_finish_every_job_once_from_broker_truth(database_url, platform_db, tmp_path):
    # ── process 1: owns the account; the submit's outcome is unknown ─────────────────────────────────
    h = unknown_entry(tmp_path)
    shadow = ShadowWorld(h.db, tmp_path)
    day = shadow.start + 1
    assert shadow.run(shadow.start).status == "COMPLETED" and shadow.run(day).status == "COMPLETED"
    outbox_id = _customer_event(h)
    platform = Platform(platform_db, ttl_ms=300)
    later = h.now + 20 * 60_000                                          # the unknown outcome is past its deadline
    s1 = FakeServices(h.db, h=h, now=lambda: later)
    a = platform.worker("A")
    runner_a = platform.runner(h.db, s1, a, lease_ms=300, categories=TRADING)
    platform.assignments.claim(a)                                        # A's renewal right before its loop
    scheduled = runner_a.schedule([(ACCOUNT, TENANT)], now_ms=shadow.clock_for(day))
    assert scheduled["errors"] == []
    rj.enqueue_reconciliation(runner_a.queue, ACCOUNT, reason="SUBMIT_UNKNOWN")
    keys = {j["category"]: j["idempotency_key"] for j in platform.jobs()}
    assert set(keys) == {ORDER, RECONCILIATION, NOTIFICATION, SHADOW_COMPARISON, OPERATIONAL_RETRY}
    assert keys[ORDER] == rj.order_resolution_key(ACCOUNT, attempt_id_of(h))
    assert keys[NOTIFICATION] == rj.outbox_notification_key(outbox_id)
    assert keys[SHADOW_COMPARISON] == rj.shadow_comparison_key("DAILY_TREND", rj.SHADOW_KIND_DAILY,
                                                               shadow.DT.day_text(day))
    # the ORDER handler runs while the venue still does not answer, then the process crashes holding everything
    first = dict(runner_a.run_once(limit=1))
    assert list(first.values()) == ["RETRY"]
    platform.due_now()
    held = JobQueue(platform_db, lease_ms=300).claim(a, limit=50)
    assert {j.category for j in held} == {ORDER, RECONCILIATION, NOTIFICATION, SHADOW_COMPARISON, OPERATIONAL_RETRY}
    assert creates(h) == 1 and attempt_history(h)[-1] == "SUBMIT_UNKNOWN"
    platform_db.close()                                                  # process 1 is gone

    # ── the venue's truth meanwhile: the order filled ─────────────────────────────────────────────────
    venue_shows_fill(h)
    time.sleep(0.4)                                                      # A's account TTL and job leases lapse

    # ── process 2: new connections, takes the account over, finishes every job ───────────────────────
    from shared_lib.persistence.db import DB
    legacy = DB(h.db.path)
    reopened = Database(database_url)
    try:
        platform2 = Platform(reopened)
        b = platform2.worker("B")
        assert platform2.owns(b) and platform2.assignments.history(ACCOUNT)[-1][3] == "EXPIRED_TAKEOVER"
        s2 = FakeServices(legacy, h=h, now=lambda: later)
        trading = platform2.runner(legacy, s2, b, categories=TRADING)
        s_shadow = FakeServices(legacy, now=lambda: shadow.clock_for(day))
        s_shadow.shadow = shadow.source
        shadow_runner = platform2.runner(legacy, s_shadow, b, categories=[SHADOW_COMPARISON])
        assert trading.schedule([(ACCOUNT, TENANT)], now_ms=shadow.clock_for(day))["enqueued"] == 0   # same work
        results = _drain_all([trading, shadow_runner], platform2)
        assert results and all(outcome == "SUCCEEDED" for _, outcome in results)
        jobs = platform2.jobs()
        assert {j["status"] for j in jobs} == {"SUCCEEDED"} and JobQueue(reopened).backlog()["dead"] == 0

        # ── exactly once, from durable state ──────────────────────────────────────────────────────
        assert creates(h) == 1                                           # no order lost, none duplicated
        history = attempt_history(h)
        assert history[-1] == "RECONCILED_POSITION_EXISTS" and history.count("RECONCILED_POSITION_EXISTS") == 1
        assert h.reservation_status() == "CONSUMED" and rj.unresolved_attempts(legacy, ACCOUNT) == []
        from app.observability import user_events as ue
        [row] = [r for r in ue.outbox(legacy) if r["outbox_id"] == outbox_id]
        assert row["status"] == "sent" and row["attempts"] == 1
        assert s1.sent == [] and [s[1] for s in s2.sent] == ["chat-1"]   # delivered once
        rows = shadow.rows(day)
        assert len(rows) == 2 and len({r[0] for r in rows}) == 2 and all(r[3] == "MATCH" for r in rows)
        by_category = {j["category"]: j for j in jobs}
        assert by_category[ORDER]["completion_evidence"]["outcome"] in ("RESOLVED", "ALREADY_RESOLVED")
        assert by_category[RECONCILIATION]["completion_evidence"]["outcome"] in ("RECONCILED", "SUPERSEDED")
        assert by_category[ORDER]["attempts"] <= 3                       # A's try, A's lost claim, B's run

        # ── a further restart finds nothing to redo ───────────────────────────────────────────────
        reads = venue_reads(h)
        assert trading.schedule([(ACCOUNT, TENANT)], now_ms=shadow.clock_for(day))["enqueued"] == 0
        assert _drain_all([trading, shadow_runner], platform2) == []
        assert venue_reads(h) == reads and creates(h) == 1 and shadow.rows(day) == rows
    finally:
        reopened.close()


def test_worker_shutdown_keeps_unfinished_jobs_and_a_successor_finishes_them(platform_db, tmp_path):
    """The standalone worker and the production loop share one registration path (``TradingWorker`` with a
    ``RuntimeJobRunner``). Draining while a job is in flight: the in-flight job finishes, the jobs claimed with
    it but not started are handed back at once, and a successor finishes them; a job held by a killed process
    returns when its lease expires."""
    h = unknown_entry(tmp_path)
    platform = Platform(platform_db, ttl_ms=30_000)
    services = FakeServices(h.db, h=h)
    noop = lambda ctx: None                                              # noqa: E731
    worker = TradingWorker(platform_db, maintain=noop, recover=noop, worker_id="W1", assignments=platform.assignments,
                           job_runner=rj.for_worker(h.db, platform_db, services=services, lease_ms=30_000,
                                                    max_processing_ms=None, categories=TRADING))
    assert isinstance(worker.jobs, rj.RuntimeJobRunner) and worker.jobs.worker_id == "W1"
    q = JobQueue(platform_db)
    alerts = [q.enqueue(NOTIFICATION, f"ops-alert-delivery:d{i}", {"kind": rj.KIND_OPS_ALERT, "subject": f"s{i}",
                                                                    "text": "t"}, priority=70 - i)[0] for i in range(3)]
    entered, release = threading.Event(), threading.Event()

    def send_text(subject, text):                                        # the first alert's channel hangs
        services.ops_sent.append(subject)
        if subject == "s0":
            entered.set()
            assert release.wait(10)

    services.ops_channels = lambda: __import__("types").SimpleNamespace(send_text=send_text)
    loop, box = run_in_thread(worker.run_once, "W1-loop")               # claims the account, schedules, runs jobs
    assert entered.wait(10)
    drained, dbox = run_in_thread(worker.drain, "W1-sigterm")           # SIGTERM while a job is in flight
    time.sleep(0.3)
    assert drained.is_alive()                                            # accounts are not released under a job
    release.set()
    loop.join(10)
    drained.join(10)
    report = box["result"]
    outcomes = dict(report.jobs)
    assert outcomes[alerts[0]] == "SUCCEEDED"
    assert {outcomes[j] for j in alerts[1:]} == {"RELEASED"} and services.ops_sent == ["s0"]
    assert dbox["result"] == 1 and platform.assignments.owner_of(ACCOUNT) is None
    order_job = q.find(rj.order_resolution_key(ACCOUNT, attempt_id_of(h)))
    assert order_job is not None and order_job["status"] in ("PENDING", "RETRY")   # scheduled, never lost

    # a successor process takes the account and finishes everything the drained worker left
    venue_shows_fill(h)
    successor_services = FakeServices(h.db, h=h)
    successor_services.ops_channels = services.ops_channels
    successor = TradingWorker(platform_db, maintain=noop, recover=noop, worker_id="W2",
                              assignments=platform.assignments,
                              job_runner=rj.for_worker(h.db, platform_db, services=successor_services,
                                                       max_processing_ms=None, categories=TRADING))
    for _ in range(4):
        successor.run_once()
        platform.due_now()
    assert sorted(services.ops_sent) == ["s0", "s1", "s2"]               # each alert sent once
    assert q.get(order_job["job_id"])["status"] == "SUCCEEDED"
    assert attempt_history(h).count("RECONCILED_POSITION_EXISTS") == 1 and creates(h) == 1

    # a process killed while holding a job: the job comes back after its lease, to whoever owns the account
    stuck = rj.enqueue_reconciliation(q, ACCOUNT, reason="KILLED")[0]
    [held] = JobQueue(platform_db, lease_ms=3_000).claim("W2", limit=1)
    leased_until = time.monotonic() + 3.0
    assert held.job_id == stuck
    assert successor.run_once().jobs == [] and time.monotonic() < leased_until   # still leased to the dead claim
    time.sleep(max(0.0, leased_until - time.monotonic()) + 0.1)
    assert (stuck, "SUCCEEDED") in successor.run_once().jobs
    successor.drain()
