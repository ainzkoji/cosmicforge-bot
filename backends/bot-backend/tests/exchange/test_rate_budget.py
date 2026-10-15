"""Step 4.5 -- the request budget per venue and server address, on a real SQLite file.

Proven here: the class ceilings and their order, the sliding window, sharing between accounts, threads and
separate processes, closure on 429 / 418 without retry or bypass, survival of a restart, failover to another
address, correction from the venue's reported usage, and the budgeted adapter view.

Not proven here: any venue's actual limit. The limits these tests use are the tests' own.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from typing import Any, List, Optional

import pytest

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter import rate_budget as RB
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB

NOW = 1_800_000_000_000
ADDRESS, OTHER_ADDRESS = "203.0.113.10", "203.0.113.20"
WINDOW = 60_000
#: capacity 100 per minute and full utilisation: the ceilings are 100 / 95 / 80 / 65 / 50
LIMITS = {venue: RB.VenueLimit(venue, 100.0, WINDOW, RB.WEIGHT, RB.UNVERIFIED, "test limit") for venue in C.VENUES}


def open_db(path) -> DB:
    database = DB(str(path))
    ensure_cati_platform_schema(database)
    return database


@pytest.fixture
def db(tmp_path):
    return open_db(tmp_path / "budget.db")


def budget(database, **over) -> RB.RateBudget:
    return RB.RateBudget(database, **{"limits": LIMITS, "utilisation": 1.0, **over})


def take(b: RB.RateBudget, priority: str, count: int, *, now: int = NOW, address: str = ADDRESS, weight: float = 1.0,
         account: Optional[str] = None) -> List[RB.Reservation]:
    return [b.reserve(C.BINANCE, address, now, priority=priority, weight=weight, account_ref=account)
            for _ in range(count)]


def granted(reservations) -> int:
    return sum(r.granted for r in reservations)


# ------------------------------------------------------------------------------------------- admission
def test_a_class_is_granted_up_to_its_ceiling_and_a_refusal_says_when_to_ask_again(db):
    b = budget(db)
    first = take(b, RB.MARKET_DATA, 50)
    assert granted(first) == 50 and all(r.retry_at_ms is None for r in first)
    refused = b.reserve(C.BINANCE, ADDRESS, NOW + 10, priority=RB.MARKET_DATA)
    assert not refused.granted and refused.reason == RB.REASON_EXHAUSTED
    assert (refused.used, refused.capacity, refused.ceiling) == (50.0, 100.0, 50.0)
    # the first grant leaves the sliding window one window after it was made, and not a millisecond earlier
    assert refused.retry_at_ms == NOW + WINDOW
    assert not b.reserve(C.BINANCE, ADDRESS, NOW + WINDOW - 1, priority=RB.MARKET_DATA).granted
    assert b.reserve(C.BINANCE, ADDRESS, NOW + WINDOW, priority=RB.MARKET_DATA).granted


def test_weights_are_counted_and_a_weight_no_window_can_hold_is_never_grantable(db):
    b = budget(db)
    assert b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.RECONCILIATION, weight=40).granted
    assert b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.RECONCILIATION, weight=40).granted
    refused = b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.RECONCILIATION, weight=5)
    assert not refused.granted and refused.retry_at_ms == NOW + WINDOW
    impossible = b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.MARKET_DATA, weight=51)
    assert not impossible.granted and impossible.reason == RB.REASON_WEIGHT_TOO_LARGE and impossible.retry_at_ms is None


def test_emergency_and_protection_outrank_reconciliation_then_entries_then_market_data(db):
    b = budget(db)
    assert granted(take(b, RB.MARKET_DATA, 60)) == 50          # market data stops at 50
    assert granted(take(b, RB.ENTRY, 30)) == 15                # entries may fill to 65
    assert granted(take(b, RB.MARKET_DATA, 1)) == 0
    assert granted(take(b, RB.RECONCILIATION, 30)) == 15       # reconciliation to 80
    assert granted(take(b, RB.ENTRY, 1)) == 0
    assert granted(take(b, RB.PROTECTION, 30)) == 15           # protection to 95
    assert granted(take(b, RB.RECONCILIATION, 1)) == 0
    assert granted(take(b, RB.EMERGENCY, 30)) == 5             # the last 5 belong to an emergency close alone
    assert granted(take(b, RB.PROTECTION, 1)) == 0
    snapshot = b.snapshot(C.BINANCE, ADDRESS, NOW)
    assert snapshot["used"] == 100.0
    assert snapshot["used_by_priority"] == {RB.MARKET_DATA: 50.0, RB.ENTRY: 15.0, RB.RECONCILIATION: 15.0,
                                            RB.PROTECTION: 15.0, RB.EMERGENCY: 5.0}
    assert snapshot["denials"][f"{RB.ENTRY}:{RB.REASON_EXHAUSTED}"] == 16


def test_safety_traffic_still_has_capacity_when_entries_and_market_data_have_spent_theirs(db):
    b = budget(db)
    take(b, RB.MARKET_DATA, 50)
    take(b, RB.ENTRY, 15)
    allowed, reason, retry_at = b.entries_allowed(C.BINANCE, ADDRESS, NOW)
    assert (allowed, reason, retry_at) == (False, RB.REASON_EXHAUSTED, NOW + WINDOW)
    # protection verification, unknown-submit read-back, position reconciliation, emergency close: all admitted
    assert b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.PROTECTION, weight=5).granted
    assert b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.RECONCILIATION, weight=5).granted
    assert b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.EMERGENCY, weight=5).granted


def test_the_window_slides(db):
    b = budget(db)
    take(b, RB.ENTRY, 40, now=NOW)
    take(b, RB.ENTRY, 25, now=NOW + 30_000)
    assert granted(take(b, RB.ENTRY, 1, now=NOW + 59_999)) == 0
    assert granted(take(b, RB.ENTRY, 50, now=NOW + 60_000)) == 40      # only the first 40 have left the window
    assert granted(take(b, RB.ENTRY, 50, now=NOW + 90_000)) == 25


# --------------------------------------------------------------------------------- sharing one address
def test_hundreds_of_accounts_behind_one_address_share_one_allowance(db):
    b = budget(db)
    results = [b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.ENTRY, account_ref=f"account-{i}") for i in range(300)]
    assert granted(results) == 65
    assert b.snapshot(C.BINANCE, ADDRESS, NOW)["used"] == 65.0
    # the allowance belongs to the venue and the address, not to the account or to another venue
    assert b.reserve(C.BYBIT, ADDRESS, NOW, priority=RB.ENTRY).granted
    assert b.reserve(C.BINANCE, OTHER_ADDRESS, NOW, priority=RB.ENTRY).granted


def test_threads_with_their_own_connections_never_exceed_the_ceiling(tmp_path):
    path = tmp_path / "threads.db"
    open_db(path)
    counts, errors = [], []

    def worker(index: int) -> None:
        try:
            own = budget(DB(str(path)), worker=f"w{index}")
            counts.append(granted(take(own, RB.RECONCILIATION, 40, account=f"account-{index}")))
        except Exception as exc:  # surfaced to the test, never swallowed
            errors.append(repr(exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120)
    assert not errors
    assert sum(counts) == 80    # 320 requests for 80 units: exactly the ceiling, never one more
    assert budget(DB(str(path))).snapshot(C.BINANCE, ADDRESS, NOW)["used"] == 80.0


_PROCESS_WORKER = """
import json, sys
from shared_lib.persistence.db import DB
from app.exchange.platform_adapter import contract as C, rate_budget as RB
path, worker, now = sys.argv[1], sys.argv[2], int(sys.argv[3])
limits = {v: RB.VenueLimit(v, 100.0, 60000, RB.WEIGHT, RB.UNVERIFIED, "test limit") for v in C.VENUES}
budget = RB.RateBudget(DB(path), limits=limits, utilisation=1.0, worker=worker)
print(json.dumps(sum(budget.reserve(C.BINANCE, "203.0.113.10", now, priority=RB.ENTRY).granted for _ in range(40))))
"""


def test_separate_processes_on_one_host_share_the_budget(tmp_path):
    path = tmp_path / "processes.db"
    open_db(path)
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    workers = [subprocess.Popen([sys.executable, "-c", _PROCESS_WORKER, str(path), f"process-{i}", str(NOW)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True) for i in range(4)]
    outputs = [worker.communicate(timeout=180) for worker in workers]
    assert all(worker.returncode == 0 for worker in workers), [err[-400:] for _, err in outputs]
    assert sum(json.loads(out.strip().splitlines()[-1]) for out, _ in outputs) == 65


def test_a_restart_forgets_neither_the_usage_nor_a_closure(tmp_path):
    path = tmp_path / "restart.db"
    first = budget(open_db(path))
    take(first, RB.ENTRY, 65)
    first.record_rejection(C.BYBIT, ADDRESS, NOW, status=429, retry_after_s=45)
    del first
    second = budget(DB(str(path)))  # a new process: nothing in memory
    assert not second.reserve(C.BINANCE, ADDRESS, NOW + 1_000, priority=RB.ENTRY).granted
    closed = second.reserve(C.BYBIT, ADDRESS, NOW + 1_000, priority=RB.EMERGENCY)
    assert not closed.granted and closed.retry_at_ms == NOW + 45_000


# ------------------------------------------------------------------------------------- 429 / 418
def test_429_closes_the_budget_until_retry_after_for_every_class_with_no_bypass(db):
    b = budget(db)
    assert b.record_rejection(C.BINANCE, ADDRESS, NOW, status=429, retry_after_s=30) == NOW + 30_000
    for priority in RB.PRIORITIES:  # the emergency class too: asking a venue that said wait cannot succeed
        refused = b.reserve(C.BINANCE, ADDRESS, NOW + 29_999, priority=priority)
        assert not refused.granted and refused.reason == RB.REASON_CLOSED_RATE_LIMITED
        assert refused.retry_at_ms == NOW + 30_000 and refused.retry_after_s(NOW + 29_000) == 1.0
    assert b.entries_allowed(C.BINANCE, ADDRESS, NOW + 1) == (False, RB.REASON_CLOSED_RATE_LIMITED, NOW + 30_000)
    # a later, shorter instruction never shortens a standing closure, and nothing in the API reopens one
    assert b.record_rejection(C.BINANCE, ADDRESS, NOW + 5_000, status=429, retry_after_s=1) == NOW + 30_000
    assert not any(name.startswith(("reopen", "reset", "clear", "bypass")) for name in dir(b))
    assert b.reserve(C.BINANCE, ADDRESS, NOW + 30_000, priority=RB.EMERGENCY).granted
    snapshot = b.snapshot(C.BINANCE, ADDRESS, NOW + 10_000)
    assert snapshot["closed"] and snapshot["closed_reason"] == C.RATE_LIMITED and snapshot["closures"] == 1


def test_418_closes_until_the_ban_ends_and_a_missing_instruction_uses_the_conservative_default(db):
    b = budget(db)
    until = b.record_rejection(C.BINANCE, ADDRESS, NOW, status=418, banned_until_ms=NOW + 7_200_000)
    assert until == NOW + 7_200_000
    refused = b.reserve(C.BINANCE, ADDRESS, NOW + 3_600_000, priority=RB.EMERGENCY)
    assert not refused.granted and refused.reason == RB.REASON_CLOSED_BANNED and refused.retry_at_ms == until
    # a 429 arriving during the ban neither shortens it nor turns it into an ordinary rate refusal
    b.record_rejection(C.BINANCE, ADDRESS, NOW + 1_000, status=429, retry_after_s=2)
    assert b.snapshot(C.BINANCE, ADDRESS, NOW + 2_000)["closed_reason"] == C.BANNED
    assert b.reserve(C.BINANCE, ADDRESS, until, priority=RB.ENTRY).granted
    assert b.record_rejection(C.BYBIT, ADDRESS, NOW, status=418) == NOW + RB.DEFAULT_BAN_CLOSURE_MS
    assert b.record_rejection(C.BINGX, ADDRESS, NOW, status=429) == NOW + RB.DEFAULT_RATE_LIMITED_CLOSURE_MS


def test_failover_to_another_address_has_its_own_budget(db):
    b = budget(db)
    b.record_rejection(C.BINANCE, ADDRESS, NOW, status=418, retry_after_s=600)
    assert not b.reserve(C.BINANCE, ADDRESS, NOW + 1, priority=RB.EMERGENCY).granted
    assert b.reserve(C.BINANCE, OTHER_ADDRESS, NOW + 1, priority=RB.EMERGENCY).granted
    assert b.snapshot(C.BINANCE, OTHER_ADDRESS, NOW + 1)["closed"] is False


def test_an_account_scoped_refusal_closes_only_that_account(db):
    b = budget(db)
    until = b.record_rejection(C.BYBIT, ADDRESS, NOW, status=429, retry_after_s=10, scope=RB.SCOPE_UID,
                               account_ref="account-1")
    refused = b.reserve(C.BYBIT, ADDRESS, NOW + 1, priority=RB.PROTECTION, account_ref="account-1")
    assert not refused.granted and refused.reason == RB.REASON_ACCOUNT_CLOSED and refused.retry_at_ms == until
    assert b.reserve(C.BYBIT, ADDRESS, NOW + 1, priority=RB.PROTECTION, account_ref="account-2").granted
    assert b.entries_allowed(C.BYBIT, ADDRESS, NOW + 1, account_ref="account-1")[0] is False
    with pytest.raises(ValueError):
        b.record_rejection(C.BYBIT, ADDRESS, NOW, status=429, scope=RB.SCOPE_UID)


# ------------------------------------------------------------------------------ what the venue reports
def observation(**over) -> C.RateLimitObservation:
    base = dict(venue=C.BINANCE, used_weight=None, limit=None, window_seconds=60, retry_after_s=None,
                banned_until_ms=None, observed_at_ms=NOW)
    return C.RateLimitObservation(**{**base, **over})


def test_usage_the_venue_reports_above_the_local_sum_is_added_and_never_subtracted(db):
    b = budget(db)
    take(b, RB.RECONCILIATION, 10)
    b.observe(observation(used_weight=60.0), ADDRESS)       # another process on this address used 50
    snapshot = b.snapshot(C.BINANCE, ADDRESS, NOW)
    assert snapshot["used"] == 60.0 and snapshot["used_by_priority"][RB.OBSERVED] == 50.0
    assert snapshot["observed_used"] == 60.0
    assert not b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.MARKET_DATA).granted
    b.observe(observation(used_weight=5.0, observed_at_ms=NOW + 1_000), ADDRESS)   # the venue's window restarted
    assert b.snapshot(C.BINANCE, ADDRESS, NOW + 1_000)["used"] == 60.0


def test_a_lower_limit_reported_by_the_venue_replaces_the_configured_one_and_a_higher_one_does_not(db):
    b = budget(db)
    b.observe(observation(limit=40.0), ADDRESS)
    assert b.snapshot(C.BINANCE, ADDRESS, NOW)["capacity"] == 40.0
    assert granted(take(b, RB.MARKET_DATA, 30)) == 20
    b.observe(observation(limit=10_000.0, observed_at_ms=NOW + 1), ADDRESS)
    assert b.snapshot(C.BINANCE, ADDRESS, NOW + 1)["capacity"] == 100.0


def test_an_instruction_to_wait_inside_an_observation_closes_the_budget(db):
    b = budget(db)
    b.observe(observation(retry_after_s=12.5), ADDRESS)
    assert b.reserve(C.BINANCE, ADDRESS, NOW + 12_499, priority=RB.EMERGENCY).retry_at_ms == NOW + 12_500


# -------------------------------------------------------------------------------- refusal by design
def test_nothing_is_unlimited_by_default(db):
    b = budget(db, limits={C.BINANCE: LIMITS[C.BINANCE]})
    with pytest.raises(ValueError, match="no rate limit"):
        b.reserve(C.BYBIT, ADDRESS, NOW, priority=RB.ENTRY)
    with pytest.raises(ValueError, match="server address"):
        b.reserve(C.BINANCE, "", NOW, priority=RB.ENTRY)
    with pytest.raises(ValueError):
        b.reserve(C.BINANCE, ADDRESS, NOW, priority="URGENT")
    with pytest.raises(ValueError):
        b.reserve(C.BINANCE, ADDRESS, NOW, priority=RB.ENTRY, weight=0)
    with pytest.raises(ValueError):  # a lower class may never be given more than a higher one
        budget(db, ceilings={**RB.DEFAULT_CEILINGS, RB.MARKET_DATA: 0.99})


def test_default_limits_say_which_are_venue_facts_and_which_are_placeholders():
    binance, bybit, bingx = (RB.DEFAULT_LIMITS[v] for v in (C.BINANCE, C.BYBIT, C.BINGX))
    assert (binance.capacity, binance.window_ms, binance.unit, binance.verification) == (
        2400.0, 60_000, RB.WEIGHT, RB.VERIFIED_BY_DOC)
    assert bybit.verification == bingx.verification == RB.UNVERIFIED
    # ceilings fall from the highest class to the lowest and the planned utilisation leaves headroom
    ordered = [RB.DEFAULT_CEILINGS[p] for p in RB.PRIORITIES]
    assert ordered == sorted(ordered, reverse=True) and ordered[0] == 1.0 and RB.DEFAULT_UTILISATION < 1.0
    default = RB.RateBudget(None)
    assert default.limits[C.BINANCE].capacity * default.utilisation == 1920.0


def test_retry_is_bounded_jittered_and_never_earlier_than_the_venue_allows():
    policy = RB.RetryPolicy(max_attempts=4, base_delay_ms=500, max_delay_ms=4_000, max_wait_ms=20_000,
                            jitter_fraction=0.25)
    assert [policy.delay_ms(n, now_ms=NOW, rng=lambda: 0.0) for n in (1, 2, 3)] == [500, 1_000, 2_000]
    assert policy.delay_ms(4, now_ms=NOW) is None and policy.delay_ms(0, now_ms=NOW) is None
    # the venue said 10 s: no draw of the jitter brings the retry forward, it only pushes it back
    for draw in (0.0, 0.5, 1.0):
        waited = policy.delay_ms(1, now_ms=NOW, retry_at_ms=NOW + 10_000, rng=lambda d=draw: d)
        assert waited is not None and 10_000 <= waited <= 12_500
    # a ban far longer than a caller may hold its work is not waited out: the caller fails now
    assert policy.delay_ms(1, now_ms=NOW, retry_at_ms=NOW + 3_600_000) is None


# -------------------------------------------------------------------------------- budgeted adapter view
@dataclass
class StubAdapter:
    """The few members the budgeted view touches. Counts every call that reaches it."""
    venue: str = C.BINANCE
    environment: str = C.DEMO
    adapter_version: str = "stub-1"
    calls: List[str] = field(default_factory=list)
    fail_with: Optional[C.AdapterError] = None
    reported: Optional[C.RateLimitObservation] = None

    def _call(self, name: str) -> Any:
        self.calls.append(name)
        if self.fail_with is not None:
            raise self.fail_with
        return name

    def positions(self, symbol=None):
        return self._call("positions")

    def open_orders(self, symbol=None):
        return self._call("open_orders" if symbol else "open_orders_all")

    def place_order(self, request):
        return self._call("place_order")

    def cancel_order(self, symbol, **kw):
        return self._call("cancel_order")

    def capabilities(self):
        return self._call("capabilities")

    def rate_limit(self):
        return self.reported or C.RateLimitObservation(self.venue, None, None, None, None, None, 0)


def view(database, adapter, priority=RB.RECONCILIATION, clock=lambda: NOW):
    return RB.BudgetedVenueAdapter(adapter, budget(database), server_address=ADDRESS, account_ref="account-1",
                                   clock=clock, priority=priority)


def entry_request(**over):
    return C.OrderRequest(**{"symbol": "BTCUSDT", "side": C.BUY, "order_type": C.MARKET, "client_order_id": "abc123",
                             "quantity": 0.01, **over})


def test_the_budgeted_view_reserves_by_method_weight_and_leaves_local_methods_alone(db):
    adapter = StubAdapter()
    v = view(db, adapter)
    assert v.positions() == "positions" and v.open_orders("BTCUSDT") == "open_orders"
    assert v.open_orders() == "open_orders_all" and v.capabilities() == "capabilities"
    snapshot = budget(db).snapshot(C.BINANCE, ADDRESS, NOW)
    assert snapshot["used_by_priority"] == {RB.RECONCILIATION: 5.0 + 1.0 + 40.0}   # capabilities() cost nothing
    assert (v.venue, v.environment, v.adapter_version) == (C.BINANCE, C.DEMO, "stub-1")


def test_a_refusal_by_the_budget_is_raised_before_the_venue_is_called(db):
    adapter = StubAdapter()
    take(budget(db), RB.EMERGENCY, 80)
    with pytest.raises(C.AdapterError) as refused:
        view(db, adapter).positions()
    assert refused.value.code == C.RATE_LIMITED and refused.value.venue_code == f"LOCAL_BUDGET:{RB.REASON_EXHAUSTED}"
    assert refused.value.retry_after_s == WINDOW / 1000 and adapter.calls == []
    assert view(db, adapter, priority=RB.EMERGENCY).positions() == "positions"   # a flatten still reads


def test_an_entry_can_never_borrow_a_higher_class_but_a_reducing_order_keeps_it(db):
    adapter = StubAdapter()
    take(budget(db), RB.EMERGENCY, 70)                # above the entry ceiling of 65
    emergency = view(db, adapter, priority=RB.EMERGENCY)
    with pytest.raises(C.AdapterError):
        emergency.place_order(entry_request())        # an order that adds exposure is an entry, whoever asks
    assert adapter.calls == []
    assert emergency.place_order(entry_request(side=C.SELL, reduce_only=True)) == "place_order"
    assert view(db, adapter, priority=RB.MARKET_DATA).cancel_order("BTCUSDT", client_order_id="abc123") == "cancel_order"
    used = budget(db).snapshot(C.BINANCE, ADDRESS, NOW)["used_by_priority"]
    assert used[RB.EMERGENCY] == 71.0 and used[RB.PROTECTION] == 1.0 and RB.ENTRY not in used


def test_a_venue_429_closes_the_budget_and_the_view_does_not_retry(db):
    adapter = StubAdapter(fail_with=C.AdapterError(C.RATE_LIMITED, venue=C.BINANCE, retry_after_s=20, http_status=429))
    v = view(db, adapter)
    with pytest.raises(C.AdapterError):
        v.positions()
    assert adapter.calls == ["positions"]             # exactly one request reached the venue
    adapter.fail_with = None
    with pytest.raises(C.AdapterError) as second:
        v.with_priority(RB.EMERGENCY).positions()
    assert second.value.venue_code == f"LOCAL_BUDGET:{RB.REASON_CLOSED_RATE_LIMITED}"
    assert second.value.retry_after_s == 20.0 and adapter.calls == ["positions"]
    banned = StubAdapter(venue=C.BYBIT, fail_with=C.AdapterError(C.BANNED, venue=C.BYBIT, banned_until_ms=NOW + 900_000))
    with pytest.raises(C.AdapterError):
        view(db, banned).positions()
    with pytest.raises(C.AdapterError) as third:
        view(db, banned).positions()
    assert third.value.code == C.BANNED and third.value.banned_until_ms == NOW + 900_000 and len(banned.calls) == 1


def test_the_budget_holds_no_transaction_while_the_venue_is_called(db):
    import sqlite3

    class Probing(StubAdapter):
        def positions(self, symbol=None):
            other = sqlite3.connect(db.path, timeout=0.2)
            try:
                other.execute("BEGIN IMMEDIATE")    # impossible if the view held a transaction across this call
                other.rollback()
            finally:
                other.close()
            return super().positions(symbol)

    adapter = Probing()
    assert view(db, adapter).positions() == "positions" and adapter.calls == ["positions"]


def test_the_view_feeds_the_venues_reported_usage_back_once(db):
    adapter = StubAdapter(reported=C.RateLimitObservation(C.BINANCE, 70.0, None, 60, None, None, NOW))
    v = view(db, adapter)
    v.positions()
    v.positions()                                     # the same observation again is not counted twice
    snapshot = budget(db).snapshot(C.BINANCE, ADDRESS, NOW)
    assert snapshot["used"] == 75.0 and snapshot["used_by_priority"][RB.OBSERVED] == 65.0
