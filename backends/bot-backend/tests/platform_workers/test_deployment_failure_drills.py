"""Step 5.6 failure drills at the worker layer: what a deploy or a rollback does to accounts, jobs and the venue.

The deploy scripts (tests/deploy/test_staging_deploy_scripts.py) prove the stack-level control flow; these drills
prove the worker-level half on fake venues and throwaway databases (SQLite and PostgreSQL):

* the release being replaced drains through its real ``run_forever`` stop path and hands every account over at
  once; the successor recovers each account before it maintains it;
* the exchange is unreachable while the new release takes over: recovery fails, nothing is maintained, nothing is
  sent to the venue, and recovery is retried first on the next pass;
* a queue consumer is killed in the middle of a deploy: the durable queue survives the process, nobody runs the
  job twice while its lease is held, and the new release finishes it with exactly one venue order;
* after a rollback the old build takes the accounts back, reconciles first and finds protection intact.
"""
from __future__ import annotations

import threading
import time

from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.fake import FakeVenue
from app.exchange.platform_adapter.reconcile import MANUAL, RestReconciler
from app.platform_workers.assignments import WorkerAssignments
from app.platform_workers.jobs import ORDER, JobProcessor, JobQueue, OrderIntents, RetryLater, order_job_handler
from app.platform_workers.worker import TradingWorker
from shared_lib.persistence.sql import Database
from venue_bridge import ProtectedBook, VenueExecutor, placed_orders

N = 4


def _venues(n=N, *, protected=False):
    venues = {}
    for i in range(n):
        v = FakeVenue(c.BINANCE)
        v.add_instrument("BTCUSDT", price=50_000)
        v.external_trade("BTCUSDT", c.BUY, 0.01)
        if protected:
            v.place_order(c.OrderRequest(symbol="BTCUSDT", side=c.SELL, order_type=c.STOP_MARKET,
                                         client_order_id=f"stop-acct{i}", stop_price=45_000.0, close_position=True))
        venues[f"acct{i:04d}"] = v
    return venues


def _stops(v):
    return [o for o in v.open_orders("BTCUSDT") if o.order_type == c.STOP_MARKET]


def _worker(db, book, worker_id, ttl_ms=400):
    return TradingWorker(db, maintain=book.maintain, recover=book.recover, worker_id=worker_id,
                         assignments=WorkerAssignments(db, ttl_ms=ttl_ms, worker_liveness_ms=ttl_ms), max_concurrency=2)


def test_replaced_release_drains_on_its_stop_signal_and_the_successor_recovers_first(platform_db):
    venues = _venues()
    asg = WorkerAssignments(platform_db, ttl_ms=60_000, worker_liveness_ms=60_000)
    asg.upsert_accounts([(a, "tenant", "binance", "DEMO") for a in venues])
    book = ProtectedBook(venues)
    old = _worker(platform_db, book, "release-r1", ttl_ms=60_000)
    stop = threading.Event()
    loop = threading.Thread(target=old.run_forever, kwargs={"interval_s": 0.02, "stop": stop})
    loop.start()
    deadline = time.monotonic() + 10
    while set(book.maintained) != set(venues) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert set(book.maintained) == set(venues)
    stop.set()                                  # what SIGTERM does in run_forever: stop, then drain
    loop.join(timeout=30)
    assert not loop.is_alive()
    with platform_db.transaction(write=False) as conn:
        state = conn.scalar("SELECT state FROM platform_workers WHERE worker_id='release-r1'")
        reasons = {r[0] for r in conn.all("SELECT release_reason FROM platform_account_assignments")}
    assert state == "STOPPED" and reasons == {"DRAIN"}

    successor = ProtectedBook(venues)
    new = _worker(platform_db, successor, "release-r2", ttl_ms=60_000)
    report = new.run_once()                     # no TTL wait: a 60 s assignment was handed over at once
    assert report.owned == N and set(report.recovered) == set(venues) and not report.failed
    assert successor.recovered and successor.recovered[0] in venues and set(successor.maintained) == set(venues)
    for v in venues.values():
        assert len(_stops(v)) == 1 and not [g for g in v.protection_gaps("BTCUSDT") if g.open]
    new.drain()


def test_exchange_lost_during_takeover_blocks_maintenance_and_mutates_nothing(platform_db):
    venues = _venues(protected=True)
    asg = WorkerAssignments(platform_db, ttl_ms=400, worker_liveness_ms=400)
    asg.upsert_accounts([(a, "tenant", "binance", "DEMO") for a in venues])
    for v in venues.values():                   # the venue is unreachable for the next two passes
        v.fail_next("positions", c.AdapterError(c.VENUE_UNAVAILABLE, "connection lost"), times=2)
    baseline = {a: len(v.mutations) for a, v in venues.items()}
    book = ProtectedBook(venues)
    new = _worker(platform_db, book, "release-r2")
    for _ in range(2):
        report = new.run_once()
        assert report.owned == N and set(report.failed) == set(venues), report
        assert not report.maintained and not report.recovered
    assert not book.maintained and {a: len(v.mutations) for a, v in venues.items()} == baseline   # nothing sent
    report = new.run_once()                     # the venue is back: recovery first, then maintenance
    assert set(report.recovered) == set(venues) and set(report.maintained) == set(venues) and not report.failed
    for v in venues.values():
        assert len(_stops(v)) == 1              # the resting stop was found, not duplicated
    assert {a: len(v.mutations) for a, v in venues.items()} == baseline
    new.drain()


def test_queue_consumer_killed_mid_deploy_is_finished_once_by_the_new_release(database_url, platform_db):
    venues = _venues(2)
    accounts = sorted(venues)
    WorkerAssignments(platform_db).upsert_accounts([(a, "tenant", "binance", "DEMO") for a in accounts])
    old_intents = OrderIntents(platform_db, settle_ms=0)
    old_queue = JobQueue(platform_db, lease_ms=300)
    jobs = {}
    for a in accounts:
        key = f"deploy-drill:{a}"
        old_intents.prepare(idempotency_key=key, account_id=a, tenant_id="tenant", venue="binance",
                            symbol="BTCUSDT", side="BUY", quantity="0.001")
        jobs[old_queue.enqueue(ORDER, key, {"intent_key": key}, account_id=a, tenant_id="tenant")[0]] = key
    claimed = old_queue.claim("release-r1-consumer")
    assert {j.job_id for j in claimed} == set(jobs)
    # the old consumer reached the venue for the first job; the venue accepted it and the answer was lost; then
    # the process was killed by the deploy -- nothing it held was completed or released
    # (jobs enqueued in the same database millisecond have no defined claim order: cut the venue of the job that
    # was actually claimed first)
    first = jobs[claimed[0].job_id]
    venues[first.split(":", 1)[1]].fail_next("place_order", c.AdapterError(c.NETWORK_UNCERTAIN, "deploy cut"),
                                             after_effect=True)
    try:
        old_intents.execute(first, VenueExecutor(venues))
    except RetryLater:
        pass
    assert old_intents.load(first).status == "SUBMITTING"

    new_db = Database(database_url, pool_size=4)  # the new release's process: the queue lives in the database
    try:
        new_queue = JobQueue(new_db, lease_ms=300)
        assert new_queue.claim("release-r2-consumer") == []          # leases held: nothing runs twice
        time.sleep(0.35)
        new_intents = OrderIntents(new_db, settle_ms=0)
        executor = VenueExecutor(venues)
        proc = JobProcessor(new_queue, "release-r2-consumer", {ORDER: order_job_handler(new_intents, executor)})
        assert sorted(proc.run_once()) == sorted((j, "SUCCEEDED") for j in jobs)
        for job_id, key in jobs.items():
            intent = new_intents.load(key)
            assert intent.status == "ACKNOWLEDGED", key
            assert placed_orders(venues[intent.account_id], intent.client_order_id) == 1, key   # never twice
            assert new_queue.get(job_id)["attempts"] == 2
        assert executor.submits == 1            # the lost acknowledgement was resolved by read-back, not resent
        assert proc.run_once() == []
    finally:
        new_db.close()


def test_after_a_rollback_the_old_build_reconciles_first_and_finds_protection_intact(platform_db, tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    runtime = DB(path=str(tmp_path / "runtime.db"))
    legacy_migrate(runtime)
    venues = _venues()
    asg = WorkerAssignments(platform_db, ttl_ms=60_000, worker_liveness_ms=60_000)
    asg.upsert_accounts([(a, "tenant", "binance", "DEMO") for a in venues])
    new_book = ProtectedBook(venues)
    new = _worker(platform_db, new_book, "release-r2", ttl_ms=60_000)
    assert new.run_once().owned == N and all(len(_stops(v)) == 1 for v in venues.values())
    assert new.drain() == N                     # rollback.sh: the rejected release drains first

    old_book = ProtectedBook(venues)
    old = _worker(platform_db, old_book, "release-r1-again", ttl_ms=60_000)
    report = old.run_once()
    assert report.owned == N and set(report.recovered) == set(venues) and not report.failed
    assert set(old_book.recovered) == set(venues) and set(old_book.maintained) == set(venues)
    for account_id, v in venues.items():
        assert len(_stops(v)) == 1, account_id                            # protection was never duplicated
        result = RestReconciler(runtime, v, account_ref=account_id, symbols=["BTCUSDT"]).run(v.now_ms(), trigger=MANUAL)
        assert result.ok, account_id
    old.drain()
