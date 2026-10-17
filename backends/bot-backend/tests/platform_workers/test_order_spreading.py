"""Step 5.5: deterministic sequencing, shared notional limits, stale-depth refusal, concurrent workers."""
from __future__ import annotations

import threading
from decimal import Decimal

import pytest

from app.platform_workers.assignments import WorkerAssignments
from app.trading_intelligence.scale.order_spreading import (BUY, SELL, BudgetRefused, DepthRefused, DepthSnapshot,
                                                            PlatformOrderBudget, SpreadPolicy, aggregate_cap,
                                                            limit_price, market_fallback_permitted, sequence)
from shared_lib.persistence.sql import Database

TICK = Decimal("0.1")


def _depth(now, *, levels=10, qty="2", age=0, mid=50_000):
    bids = [(mid - 1 - i * 5, qty) for i in range(levels)]
    asks = [(mid + i * 5, qty) for i in range(levels)]
    return DepthSnapshot.of("binance", "BTCUSDT", bids, asks, now - age)


def _now(db):
    with db.transaction(write=False) as conn:
        return conn.now_ms()


def _accounts(db, n):
    WorkerAssignments(db).upsert_accounts([(f"a{i:04d}", f"t{i % 13}", "binance", "DEMO") for i in range(n)])


def test_sequence_is_deterministic_complete_and_rotates_between_signals():
    ids = [f"a{i:04d}" for i in range(500)]
    s1 = sequence("sig-2026-10-16", ids)
    assert s1 == sequence("sig-2026-10-16", list(reversed(ids)))
    assert sorted(s1) == sorted(ids)
    s2 = sequence("sig-2026-10-17", ids)
    assert s1 != s2
    # fairness: the first tenth of one signal is not the first tenth of the next
    assert len(set(s1[:50]) & set(s2[:50])) < 25


def test_cap_from_depth_within_the_slippage_band():
    now = 1_000_000
    cap = aggregate_cap(_depth(now), BUY, SpreadPolicy(), now_ms=now)
    # best ask 50,000; 25 bps -> worst 50,125; asks every 5 up to 50,125 -> 26 levels? only 10 exist
    assert cap.reference_price == Decimal(50_000) and cap.worst_price == Decimal("50125.0000")
    assert cap.levels_used == 10
    assert cap.cap_notional == cap.band_notional * Decimal("0.10")


def test_stale_thin_and_crossed_depth_are_refused():
    now = 10_000_000
    with pytest.raises(DepthRefused) as e:
        aggregate_cap(_depth(now, age=5_001), BUY, SpreadPolicy(), now_ms=now)
    assert e.value.code == "DEPTH_STALE"
    with pytest.raises(DepthRefused) as e:
        aggregate_cap(_depth(now, levels=3), BUY, SpreadPolicy(), now_ms=now)
    assert e.value.code == "DEPTH_INSUFFICIENT_LEVELS"
    crossed = DepthSnapshot.of("binance", "BTCUSDT", [(50_010, 1)] * 6, [(50_000, 1)] * 6, now)
    with pytest.raises(DepthRefused) as e:
        aggregate_cap(crossed, BUY, SpreadPolicy(), now_ms=now)
    assert e.value.code == "DEPTH_CROSSED"


def test_calibrated_policy_needs_an_approval_reference():
    with pytest.raises(ValueError):
        SpreadPolicy(calibrated=True)
    assert not SpreadPolicy().calibrated


def test_limit_price_never_exceeds_the_worst_permitted_price():
    assert limit_price(Decimal("50125.07"), BUY, TICK) == Decimal("50125.0")
    assert limit_price(Decimal("49874.93"), SELL, TICK) == Decimal("49875.0")


def test_market_fallback_only_within_worst_price_and_only_when_allowed():
    now = 5_000_000
    depth = _depth(now)
    worst = Decimal("50020")
    assert market_fallback_permitted(depth, BUY, Decimal("1"), worst, policy=SpreadPolicy(), now_ms=now) == \
        (False, "MARKET_FALLBACK_DISABLED_BY_POLICY")
    allow = SpreadPolicy(allow_market_fallback=True)
    assert market_fallback_permitted(depth, BUY, Decimal("6"), worst, policy=allow, now_ms=now)[0]
    assert market_fallback_permitted(depth, BUY, Decimal("12"), worst, policy=allow,
                                     now_ms=now) == (False, "MARKET_WALK_EXCEEDS_WORST_PRICE")
    assert market_fallback_permitted(_depth(now, age=9_000), BUY, Decimal("1"), worst, policy=allow,
                                     now_ms=now) == (False, "DEPTH_STALE")


def test_allocation_respects_cap_in_rank_order_with_bounded_slots(platform_db):
    _accounts(platform_db, 40)
    budget = PlatformOrderBudget(platform_db, SpreadPolicy(max_orders_per_slot=5))
    bid = budget.open_budget(venue="binance", symbol="BTCUSDT", signal_id="sig1", side=BUY, depth=_depth(_now(platform_db)))
    for i in range(40):
        budget.request(bid, f"a{i:04d}", f"t{i % 13}", "5000")   # 40 x 5,000 > the 100,250 cap
    alloc = budget.allocate(bid, tick=TICK, min_notional=Decimal("100"))
    assert alloc.granted_total <= alloc.cap
    reserved = [s for s in alloc.slots if s.status == "RESERVED"]
    deferred = [s for s in alloc.slots if s.status == "DEFERRED"]
    assert reserved and deferred and len(reserved) + len(deferred) == 40
    order = sequence("sig1", [f"a{i:04d}" for i in range(40)])
    assert [s.account_id for s in sorted(alloc.slots, key=lambda s: s.sequence)] == order
    assert all(s.reason == "PLATFORM_SYMBOL_BUDGET_EXHAUSTED" for s in deferred)
    times = sorted({s.scheduled_at for s in reserved})
    assert all(sum(1 for s in reserved if s.scheduled_at == t) <= 5 for t in times)
    assert len({s.client_order_id for s in alloc.slots}) == 40           # per-customer identity preserved
    assert all(s.limit_price <= Decimal("50125") for s in reserved)


def test_allocation_is_idempotent_and_retry_safe(platform_db):
    _accounts(platform_db, 5)
    budget = PlatformOrderBudget(platform_db)
    bid = budget.open_budget(venue="binance", symbol="BTCUSDT", signal_id="s", side=BUY, depth=_depth(_now(platform_db)))
    for i in range(5):
        budget.request(bid, f"a{i:04d}", f"t{i % 13}", "100")
        budget.request(bid, f"a{i:04d}", f"t{i % 13}", "100")      # duplicate request: no second slot
    first = budget.allocate(bid, tick=TICK)
    second = budget.allocate(bid, tick=TICK)
    assert [(s.slot_id, s.status, s.granted) for s in first.slots] == [(s.slot_id, s.status, s.granted) for s in second.slots]
    assert len(first.slots) == 5


def test_two_signals_on_one_symbol_do_not_double_count_liquidity(platform_db):
    _accounts(platform_db, 20)
    budget = PlatformOrderBudget(platform_db)
    now = _now(platform_db)
    b1 = budget.open_budget(venue="binance", symbol="BTCUSDT", signal_id="fam-a", side=BUY, depth=_depth(now))
    b2 = budget.open_budget(venue="binance", symbol="BTCUSDT", signal_id="fam-b", side=BUY, depth=_depth(now))
    for i in range(10):
        budget.request(b1, f"a{i:04d}", "t", "5000")
        budget.request(b2, f"a{i + 10:04d}", "t", "5000")
    a1 = budget.allocate(b1, tick=TICK)
    a2 = budget.allocate(b2, tick=TICK)
    assert a1.granted_total + a2.granted_total <= a1.cap
    assert a2.used_elsewhere == a1.granted_total


def test_concurrent_workers_never_exceed_the_shared_cap(database_url, platform_db):
    _accounts(platform_db, 200)
    now = _now(platform_db)
    errors, budgets = [], []

    def worker(k):
        try:
            db = Database(database_url, pool_size=2)
            pb = PlatformOrderBudget(db)
            bid = pb.open_budget(venue="binance", symbol="BTCUSDT", signal_id="sig", side=BUY, depth=_depth(now))
            budgets.append(bid)
            for i in range(k, 200, 4):                      # each worker owns a quarter of the accounts
                pb.request(bid, f"a{i:04d}", f"t{i % 13}", "800")
            pb.allocate(bid, tick=TICK)
            db.close()
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors and len(set(budgets)) == 1          # all four workers share one budget
    pb = PlatformOrderBudget(platform_db)
    final = pb.allocate(budgets[0], tick=TICK)
    granted = sum((s.granted for s in final.slots if s.status == "RESERVED"), Decimal(0))
    assert granted <= final.cap
    assert len(final.slots) == 200 and all(s.status in ("RESERVED", "DEFERRED") for s in final.slots)


def test_release_frees_capacity_and_tenant_is_checked(platform_db):
    _accounts(platform_db, 3)
    pb = PlatformOrderBudget(platform_db, SpreadPolicy(absolute_cap_notional=Decimal("1000")))
    bid = pb.open_budget(venue="binance", symbol="BTCUSDT", signal_id="s", side=BUY, depth=_depth(_now(platform_db)))
    pb.request(bid, "a0000", "t0", "1000")
    pb.request(bid, "a0001", "t1", "1000")
    first = pb.allocate(bid, tick=TICK)
    winner = next(s for s in first.slots if s.status == "RESERVED")
    with pytest.raises(BudgetRefused):
        pb.mark(winner.slot_id, "RELEASED", tenant_id="someone-else")
    pb.mark(winner.slot_id, "RELEASED", tenant_id=winner.tenant_id)
    pb.request(bid, "a0002", "t2", "1000")
    after = pb.allocate(bid, tick=TICK)
    assert next(s for s in after.slots if s.account_id == "a0002").status == "RESERVED"


def test_stale_depth_is_refused_against_the_database_clock(platform_db):
    pb = PlatformOrderBudget(platform_db)
    with pytest.raises(DepthRefused):
        pb.open_budget(venue="binance", symbol="BTCUSDT", signal_id="s", side=BUY,
                       depth=_depth(_now(platform_db), age=60_000))
