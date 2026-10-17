"""Step 5.2: lease-based account assignment, fencing, takeover and recovery-before-submit."""
from __future__ import annotations

import threading
import time

import pytest

from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.fake import FakeVenue
from app.platform_workers.assignments import AssignmentLost, WorkerAssignments
from app.platform_workers.jobs import OrderIntents, RetryLater
from app.platform_workers.worker import TradingWorker
from shared_lib.persistence.sql import DatabaseUnavailable
from venue_bridge import ProtectedBook, VenueExecutor, placed_orders

TTL = 1_500


def _accounts(asg, n, *, tenant=lambda i: f"t{i % 7}"):
    asg.upsert_accounts([(f"acct{i:04d}", tenant(i), "binance", "DEMO") for i in range(n)])


def _venues(n, *, positions=True):
    venues = {}
    for i in range(n):
        v = FakeVenue(c.BINANCE)
        v.add_instrument("BTCUSDT", price=50_000)
        if positions:
            v.external_trade("BTCUSDT", c.BUY, 0.01)  # an open position that needs protection
        venues[f"acct{i:04d}"] = v
    return venues


def _owner_map(asg, n):
    return {f"acct{i:04d}": (asg.owner_of(f"acct{i:04d}") or (None,))[0] for i in range(n)}


def test_two_workers_split_accounts_without_overlap(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=TTL)
    _accounts(asg, 40)
    w1, w2 = asg.register_worker("w1"), asg.register_worker("w2")
    a1 = asg.claim(w1)
    a2 = asg.claim(w2)
    assert len(a1) == 20 and len(a2) == 20
    assert not {x.account_id for x in a1} & {x.account_id for x in a2}
    assert all(x.needs_recovery for x in a1 + a2)


def test_claim_uses_database_time_and_is_not_reentrant(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=TTL)
    _accounts(asg, 3)
    w1 = asg.register_worker("w1")
    first = asg.claim(w1, max_new=3)
    assert len(first) == 3
    w2 = asg.register_worker("w2")
    with platform_db.transaction(write=False) as conn:
        now = conn.now_ms()
    assert all(0 < a.expires_at - now <= TTL for a in first)
    assert asg.claim(w2, max_new=3) == []          # held, unexpired: nothing for the second worker
    assert {a.kind for a in asg.claim(w1)} == {"RENEWED"}


def test_killed_worker_is_taken_over_within_one_lease_period(platform_db):
    """A worker that dies without draining: its accounts move when the assignment expires. Measured on the real
    clock against the configured TTL."""
    n = 12
    asg = WorkerAssignments(platform_db, ttl_ms=TTL)
    _accounts(asg, n)
    venues = _venues(n)
    book = ProtectedBook(venues)
    dead = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="dead",
                         assignments=asg)
    dead.run_once()
    killed_at = time.monotonic()                   # the worker stops here: no drain, no release
    survivor = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="survivor",
                             assignments=WorkerAssignments(platform_db, ttl_ms=TTL, worker_liveness_ms=TTL))
    took_over = None
    while time.monotonic() - killed_at < 3 * TTL / 1000:
        report = survivor.run_once()
        if report.owned == n:
            took_over = (time.monotonic() - killed_at) * 1000
            break
        time.sleep(0.05)
    assert took_over is not None, "survivor never took every account"
    assert took_over <= TTL + 500, f"takeover {took_over:.0f} ms exceeds one lease period ({TTL} ms) + one poll"
    assert sorted(book.recovered).count("acct0000") == 2   # recovered by both owners in turn
    for account_id, venue in venues.items():
        assert not [g for g in venue.protection_gaps("BTCUSDT") if g.open], account_id
        stops = [o for o in venue.open_orders("BTCUSDT") if o.order_type == c.STOP_MARKET]
        assert len(stops) == 1, "exactly one protective stop: the successor did not duplicate it"
    assert all(asg.history(f"acct{i:04d}")[-1][3] == "EXPIRED_TAKEOVER" for i in range(n))


def test_stale_worker_cannot_mutate_after_takeover(platform_db):
    """Slow worker / network partition: it still holds its Assignment object, but the fencing check refuses."""
    asg = WorkerAssignments(platform_db, ttl_ms=300)
    _accounts(asg, 1)
    slow, fast = asg.register_worker("slow"), asg.register_worker("fast")
    [held] = asg.claim(slow)
    time.sleep(0.4)
    [taken] = asg.claim(fast)
    assert taken.kind == "TAKEOVER" and taken.fencing_token == held.fencing_token + 1
    with pytest.raises(AssignmentLost) as lost:
        asg.verify(held)
    assert lost.value.reason == "ASSIGNMENT_TAKEN_OVER"
    assert [a.account_id for a in asg.claim(slow)] == []   # its expired claim is not revived by renewal
    with pytest.raises(AssignmentLost):
        asg.mark_success(held)


def test_expired_claim_is_never_revived_even_if_nobody_took_it(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=200)
    _accounts(asg, 1)
    w = asg.register_worker("w")
    [held] = asg.claim(w)
    time.sleep(0.3)
    with pytest.raises(AssignmentLost) as lost:
        asg.verify(held)
    assert lost.value.reason == "ASSIGNMENT_EXPIRED"
    [again] = asg.claim(w)
    assert again.fencing_token == held.fencing_token + 1 and again.needs_recovery


def test_duplicate_worker_startup_never_double_owns(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=5_000)
    _accounts(asg, 100)
    ids = [asg.register_worker(f"w{i}") for i in range(4)]
    results = {}

    def go(wid):
        results[wid] = [a.account_id for a in asg.claim(wid)]

    threads = [threading.Thread(target=go, args=(w,)) for w in ids]
    [t.start() for t in threads]
    [t.join() for t in threads]
    owned = [a for v in results.values() for a in v]
    assert len(owned) == len(set(owned)) == 100


def test_rebalance_after_a_worker_joins(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=5_000)
    _accounts(asg, 30)
    venues = _venues(30, positions=False)
    book = ProtectedBook(venues)
    w1 = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="w1", assignments=asg)
    assert w1.run_once().owned == 30
    w2 = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="w2", assignments=asg)
    report = w1.run_once()
    assert len(report.released) == 15
    assert w2.run_once().owned == 15
    owners = _owner_map(asg, 30)
    assert sorted(owners.values()).count("w1") == 15 and sorted(owners.values()).count("w2") == 15


def test_drain_hands_over_immediately_during_maintenance(platform_db):
    """Deployment during maintenance: the running account finishes; the successor does not wait for expiry."""
    asg = WorkerAssignments(platform_db, ttl_ms=60_000)
    _accounts(asg, 4)
    venues = _venues(4)
    book = ProtectedBook(venues)
    gate = threading.Event()
    book.hook = lambda ctx: gate.wait(2) if ctx.account_id == "acct0000" else None
    old = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="old", assignments=asg)
    t = threading.Thread(target=old.run_once)
    t.start()
    time.sleep(0.2)
    drain = threading.Thread(target=old.drain)
    drain.start()
    gate.set()
    t.join()
    drain.join()
    assert "acct0000" in book.maintained          # the in-flight account finished
    book.hook = None
    new = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="new", assignments=asg)
    started = time.monotonic()
    assert new.run_once().owned == 4
    assert time.monotonic() - started < 5          # not the 60 s TTL
    assert all(len([o for o in v.open_orders("BTCUSDT") if o.order_type == c.STOP_MARKET]) == 1 for v in venues.values())


def test_takeover_during_unknown_order_submission_recovers_without_duplicate(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=300)
    _accounts(asg, 1)
    venues = _venues(1, positions=False)
    executor = VenueExecutor(venues)
    intents = OrderIntents(platform_db, asg, settle_ms=0)
    a = asg.register_worker("a")
    [held] = asg.claim(a)
    intent = intents.prepare(idempotency_key="sig1:acct0000", account_id="acct0000", tenant_id="t0", venue="binance",
                             symbol="BTCUSDT", side="BUY", quantity="0.01", fencing_token=held.fencing_token)
    venues["acct0000"].fail_next("place_order", c.AdapterError(c.NETWORK_UNCERTAIN, "timeout"), after_effect=True)
    with pytest.raises(RetryLater):
        intents.execute(intent.idempotency_key, executor, fence=(held.account_id, a, held.fencing_token))
    assert intents.load(intent.idempotency_key).status == "SUBMITTING"
    time.sleep(0.35)                                # worker a dies; its assignment expires
    book = ProtectedBook(venues, intents, executor)
    book.pending_intents = {"acct0000": [intent.idempotency_key]}
    b = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="b",
                      assignments=WorkerAssignments(platform_db, ttl_ms=300))
    report = b.run_once()
    assert report.recovered == ["acct0000"] and not report.failed
    final = intents.load(intent.idempotency_key)
    assert final.status == "ACKNOWLEDGED" and final.venue_order_id
    assert placed_orders(venues["acct0000"], final.client_order_id) == 1
    assert executor.submits == 1                    # recovery read the venue; it did not resubmit


def test_stale_fence_blocks_submission(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=200)
    _accounts(asg, 1)
    venues = _venues(1, positions=False)
    executor = VenueExecutor(venues)
    intents = OrderIntents(platform_db, asg)
    a, b = asg.register_worker("a"), asg.register_worker("b")
    [held] = asg.claim(a)
    intents.prepare(idempotency_key="k", account_id="acct0000", tenant_id="t0", venue="binance", symbol="BTCUSDT",
                    side="BUY", quantity="0.01")
    time.sleep(0.25)
    asg.claim(b)
    with pytest.raises(AssignmentLost):
        intents.execute("k", executor, fence=("acct0000", a, held.fencing_token))
    assert executor.submits == 0 and intents.load("k").status == "PENDING"


class _FlakyFactory:
    def __init__(self, real):
        self.real, self.down = real, False

    def __call__(self):
        if self.down:
            raise ConnectionError("database unreachable")
        return self.real()


def test_database_interruption_and_renewal_failure(platform_db):
    """While the database is unreachable the worker cannot renew and does no account work; when it returns
    after another worker took over, the old worker's fence refuses and it re-claims only its new share."""
    asg = WorkerAssignments(platform_db, ttl_ms=300)
    _accounts(asg, 6)
    venues = _venues(6)
    book = ProtectedBook(venues)
    w1 = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="w1", assignments=asg)
    first = w1.run_once()
    held = asg.claim("w1")
    pool = platform_db.pool
    flaky = _FlakyFactory(pool._factory)
    pool._factory = flaky
    pool.close()                                    # drop idle connections: next checkout needs the "network"
    flaky.down = True
    before = len(book.maintained)
    with pytest.raises(DatabaseUnavailable):
        w1.run_once()
    assert len(book.maintained) == before           # no account work without a renewed claim
    flaky.down = False
    time.sleep(0.35)
    w2 = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="w2",
                       assignments=WorkerAssignments(platform_db, ttl_ms=300, worker_liveness_ms=200))
    assert w2.run_once().owned == 6
    with pytest.raises(AssignmentLost):
        asg.verify(held[0])
    assert first.owned == 6
    for v in venues.values():
        assert len([o for o in v.open_orders("BTCUSDT") if o.order_type == c.STOP_MARKET]) == 1


def test_one_account_failure_is_isolated(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=5_000)
    _accounts(asg, 5)
    venues = _venues(5)
    book = ProtectedBook(venues)

    def hook(ctx):
        if ctx.account_id == "acct0002":
            raise RuntimeError("venue read failed")

    book.hook = hook
    w = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="w", assignments=asg)
    report = w.run_once()
    assert set(report.failed) == {"acct0002"} and len(report.maintained) == 4


def test_tenant_of_an_account_is_immutable(platform_db):
    asg = WorkerAssignments(platform_db)
    asg.upsert_accounts([("acct", "tenant-a", "binance", "DEMO")])
    with pytest.raises(ValueError, match="ACCOUNT_TENANT_IMMUTABLE"):
        asg.upsert_accounts([("acct", "tenant-b", "binance", "DEMO")])
