"""Step 3.9: durable stop replacement with no unprotected interval (``pipeline.protection``).

Every scenario asserts on the venue's own record: ``FakeVenue.protection_gaps(symbol)`` lists each interval in
which a non-zero position had no resting stop covering it, measured in clock time and in state changes. A
scenario passes when it adds no interval to the ones that existed when it began (none at all on the profile that
takes a stop while flat). The first test shows that the same measure DOES catch the cancel-then-create sequence.

Venues:

``bybit``           the real ``FakeVenue`` profile with amend-in-place: protocol (a).
``binance``/``bingx``  the real profiles, one stop per position and no amend: no safe overlap exists today, so
                    the replacement is refused and the old stop stays (``REPLACEMENT_NOT_POSSIBLE_SAFELY``).
``overlap-*``       ``_step3_execution.OverlapVenue``: a HYPOTHETICAL capability (a second stop may rest beside
                    the first) that no adapter of this repository declares, so that protocol (b) and its failure
                    cases are exercised. What passes there proves the state machine, not a venue.
"""
from __future__ import annotations

import pytest

import _step3_execution as E
from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.contract import AdapterError, OrderRequest
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import protection as PR
from shared_lib.persistence.cati_execution_schema import REPLACEMENT_TABLE

BTC = E.BTC
QTY, OLD, NEW = 0.02, 49_000.0, 49_500.0
#: venues on which a gap-free replacement is declared: (profile, hypothetical overlap capability)
PROTOCOL_VENUES = {"bybit": (c.BYBIT, False), "overlap-binance": (c.BINANCE, True), "overlap-bingx": (c.BINGX, True)}
OVERLAP_VENUES = ("overlap-binance", "overlap-bingx")


def build(tmp_path_factory, tmp_path, name):
    profile, overlap = PROTOCOL_VENUES.get(name, (name, False))
    world = E.make_world(tmp_path_factory, tmp_path, profile=profile, overlap=overlap)
    world.baseline = E.open_long(world, BTC, QTY, OLD)
    return world


@pytest.fixture(params=sorted(PROTOCOL_VENUES))
def world(request, tmp_path_factory, tmp_path):
    return build(tmp_path_factory, tmp_path, request.param)


@pytest.fixture(params=OVERLAP_VENUES)
def overlap(request, tmp_path_factory, tmp_path):
    return build(tmp_path_factory, tmp_path, request.param)


def establishing(world) -> str:
    """The venue call that establishes the replacement on this venue."""
    return "amend_stop" if world.fake.capabilities().stop_amend_in_place else "place_order"


def replace(world, price=NEW, key="move-1", manager=None, reason=C.TRAIL):
    return (manager or world.protection()).replace(BTC, C.LONG, price, reason=reason, request_key=key)


def one_stop_at(world, price):
    """Exactly one stop rests, at ``price``, covering the whole position; nothing was added to the baseline."""
    (stop,) = world.stops(BTC)
    held = abs(world.signed(BTC))
    assert stop.stop_price == price and (stop.close_position or stop.quantity >= held)
    assert world.fake.protection_gaps(BTC) == world.baseline
    return stop


def never_cancelled_first(world):
    """No cancel reached the venue before the replacement it makes room for had been created."""
    methods = [e.method for e in world.fake.mutations if e.effect]     # what changed the venue, answered or not
    if "cancel_order" in methods:
        assert "place_order" in methods[:methods.index("cancel_order")]


def replacement_row(world, key="move-1"):
    (row,) = world.rows(REPLACEMENT_TABLE, "request_key=?", (key,))
    return row


# =================================================================================== the measure can fail
@pytest.mark.parametrize("profile", c.VENUES)
def test_the_naive_cancel_then_create_sequence_leaves_a_gap_the_fake_records(tmp_path_factory, tmp_path, profile):
    """What ``modify_protection`` does today. If this did not produce a gap, no assertion below would mean anything."""
    world = build(tmp_path_factory, tmp_path, profile)
    (old,) = world.stops(BTC)
    world.fake.cancel_order(BTC, venue_order_id=old.venue_order_id)
    world.fake.place_order(OrderRequest(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="naive-new",
                                        stop_price=NEW, quantity=QTY, reduce_only=True))
    gaps = world.fake.protection_gaps(BTC)
    assert len(gaps) == len(world.baseline) + 1 and not gaps[-1].open
    assert gaps[-1].start_event < gaps[-1].end_event              # the position WAS uncovered between the two calls


# ============================================================================================ the protocols
def test_a_replacement_moves_the_stop_with_no_unprotected_interval(world):
    result = replace(world)
    assert (result.state, result.outcome, result.protection) == ("DONE", PR.REPLACED, PR.PROTECTED)
    assert (result.old_stop_price, result.new_stop_price, result.replayed) == (OLD, NEW, False)
    one_stop_at(world, NEW)
    never_cancelled_first(world)
    row = replacement_row(world)
    assert (row["step"], row["state"]) == (PR.PERSISTED, "DONE")
    if world.fake.capabilities().stop_amend_in_place:
        assert result.protocol == PR.AMEND_IN_PLACE
        assert [e.method for e in world.fake.mutations] == ["amend_stop"]            # nothing created, nothing cancelled
    else:
        assert result.protocol == PR.CREATE_CONFIRM_RETIRE
        assert [e.method for e in world.fake.mutations] == ["place_order", "cancel_order"]
    # every mutation is journaled before and after it, and no transaction was open while the venue was called
    journal = world.protection().journal
    before = journal.events(event_type="VENUE_BEFORE")
    after = journal.events(event_type="VENUE_AFTER")
    assert len(before) == len(after) == len(world.fake.mutations) and journal.unanswered() == []
    assert world.adapter.open_transactions == []
    record = world.protection().record(BTC)
    assert (record.status, record.stop_price, record.position_quantity) == (PR.PROTECTED, NEW, QTY)


def test_several_moves_in_a_row_ratchet_without_a_gap(world):
    manager = world.protection()
    for n, price in enumerate((49_200.0, 49_400.0, 49_600.0)):
        assert replace(world, price, key=f"ratchet-{n}", manager=manager).outcome == PR.REPLACED
        one_stop_at(world, price)
    never_cancelled_first(world)


@pytest.mark.parametrize("profile", [c.BINANCE, c.BINGX])
def test_where_no_safe_overlap_exists_the_replacement_is_refused_and_the_old_stop_stays(tmp_path_factory, tmp_path, profile):
    """The Binance and BingX profiles as they are today: one stop per position, no amend. Never cancel-then-create."""
    world = build(tmp_path_factory, tmp_path, profile)
    result = replace(world)
    assert (result.state, result.outcome, result.protocol) == ("REFUSED", PR.REPLACEMENT_NOT_POSSIBLE_SAFELY, None)
    assert world.fake.mutations == []                              # nothing was cancelled, nothing was created
    one_stop_at(world, OLD)
    row = replacement_row(world)                                   # the refusal is durable
    assert (row["state"], row["outcome"], row["old_stop_price"]) == ("REFUSED", PR.REPLACEMENT_NOT_POSSIBLE_SAFELY, OLD)
    assert world.protection().record(BTC).reason == PR.REPLACEMENT_NOT_POSSIBLE_SAFELY
    events = world.protection().journal.events(event_type="PROTECTION")
    assert events[-1]["outcome"] == f"REFUSED:{PR.REPLACEMENT_NOT_POSSIBLE_SAFELY}"
    reads = len(world.fake.reads)
    again = replace(world)                                         # asked again: the recorded answer, no venue call
    assert again.replayed and again.outcome == PR.REPLACEMENT_NOT_POSSIBLE_SAFELY and len(world.fake.reads) == reads
    assert world.protection().protocol() == (None, PR.REPLACEMENT_NOT_POSSIBLE_SAFELY)
    assert world.protection().journal.active_blocks() == ()        # the old stop protects: nothing is uncertain


@pytest.mark.parametrize("name", sorted(PROTOCOL_VENUES) + [c.BINANCE, c.BINGX])
def test_a_widening_move_is_refused_everywhere(tmp_path_factory, tmp_path, name):
    world = build(tmp_path_factory, tmp_path, name)
    result = replace(world, 48_500.0)
    assert (result.state, result.outcome) == ("REFUSED", PR.STOP_WIDENING_REFUSED)
    assert world.fake.mutations == []
    one_stop_at(world, OLD)
    with pytest.raises(C.ContractViolation) as refused:            # and the contract refuses the intent itself
        C.StopReplaceIntent(symbol=BTC, direction=C.LONG, reason=C.TRAIL, new_stop_price=48_500.0, current_stop_price=OLD)
    assert refused.value.code == "STOP_WIDENING_REFUSED"


def test_a_stop_at_the_resting_price_changes_nothing(world):
    result = replace(world, OLD, reason=C.RESIZE)
    assert (result.state, result.outcome) == ("DONE", PR.NO_CHANGE) and world.fake.mutations == []
    one_stop_at(world, OLD)


def test_a_stop_that_would_not_be_protective_at_the_mark_is_refused(world):
    result = replace(world, 50_100.0)                               # above the market of a long
    assert (result.state, result.outcome) == ("REFUSED", PR.STOP_NOT_PROTECTIVE_AT_MARK) and world.fake.mutations == []
    one_stop_at(world, OLD)


# ========================================================================================= network timeout
def test_a_timeout_after_the_venue_acted_is_read_back_and_completed(world):
    world.fake.fail_next(establishing(world), c.NETWORK_UNCERTAIN, after_effect=True)
    result = replace(world)
    assert (result.state, result.outcome) == ("DONE", PR.REPLACED)
    one_stop_at(world, NEW)
    never_cancelled_first(world)
    assert world.protection().journal.active_blocks() == ()


def test_a_timeout_that_cannot_be_read_back_blocks_entries_until_it_is_reconciled(world):
    world.fake.fail_next(establishing(world), c.NETWORK_UNCERTAIN)  # the request never reached the venue
    result = replace(world)
    assert (result.state, result.outcome, result.protection) == ("OPEN", PR.OUTCOME_UNCERTAIN, PR.UNCERTAIN)
    one_stop_at(world, OLD)                                         # the old stop was never touched
    (block,) = world.protection().journal.active_blocks()
    assert (block["reason"], block["scope"], block["symbol"]) == (PR.BLOCK_PROTECTION_UNSETTLED, "ACCOUNT", BTC)
    row = replacement_row(world)
    assert (row["state"], row["step"], row["outcome"]) == ("OPEN", PR.ESTABLISHING, PR.OUTCOME_UNCERTAIN)
    assert world.protection().record(BTC).status == PR.UNCERTAIN    # durable: a new process finds it
    (resumed,) = world.protection().resume()                        # a new manager, as after a restart
    assert (resumed.state, resumed.outcome) == ("DONE", PR.REPLACED)
    one_stop_at(world, NEW)
    never_cancelled_first(world)
    assert world.protection().journal.active_blocks() == ()
    cleared = world.rows("cati_execution_blocks")
    assert [b["state"] for b in cleared] == ["CLEARED"] and "read-back" in cleared[0]["evidence"]


def test_a_read_back_the_venue_does_not_answer_is_not_taken_for_success(world):
    world.adapter.hook_after(establishing(world), lambda: world.fake.fail_next("open_orders", c.NETWORK_UNCERTAIN))
    result = replace(world)
    assert (result.state, result.outcome) == ("OPEN", PR.OUTCOME_UNCERTAIN)
    assert world.protection().journal.active_blocks() != ()
    assert world.fake.protection_gaps(BTC) == world.baseline        # whatever is true at the venue, nothing was removed
    (resumed,) = world.protection().resume()
    assert (resumed.state, resumed.outcome) == ("DONE", PR.REPLACED)
    one_stop_at(world, NEW)
    assert world.protection().journal.active_blocks() == ()


# ============================================================================= partial fill during replacement
def test_a_fill_during_the_replacement_is_covered_for_the_quantity_that_exists(world):
    fake = world.fake
    fake.place_order(OrderRequest(symbol=BTC, side=c.BUY, order_type=c.LIMIT, client_order_id="add-1", quantity=0.01,
                                  price=49_900.0))
    fake.mutations.clear()
    world.adapter.hook(establishing(world), lambda: fake.fill("add-1", 0.01))       # fills while the new stop is on its way
    result = replace(world)
    assert (result.state, result.outcome) == ("DONE", PR.REPLACED)
    assert world.signed(BTC) == pytest.approx(0.03)
    (stop,) = world.stops(BTC)
    assert stop.stop_price == NEW and (stop.close_position or stop.quantity == pytest.approx(0.03))
    assert world.protection().record(BTC).position_quantity == pytest.approx(0.03)
    never_cancelled_first(world)
    gaps = fake.protection_gaps(BTC)
    if fake.capabilities().quantity_stop:
        # a stop that carries a quantity is outgrown by the fill itself, before any replacement step: that
        # interval is the fill's, it ends when the stop for the new quantity rests, and no stop was retired in it
        assert len(gaps) == len(world.baseline) + 1 and not gaps[-1].open
        assert [e.method for e in fake.mutations] == ["place_order", "place_order", "cancel_order", "cancel_order"]
    else:
        assert gaps == world.baseline                               # a stop bound to the position covered it throughout


# ============================================================================== old-stop cancellation failure
def test_a_failed_retirement_leaves_both_stops_resting_and_is_retried_later(overlap):
    world = overlap
    world.fake.fail_next("cancel_order", c.VENUE_UNAVAILABLE)
    result = replace(world)
    assert (result.state, result.outcome, result.protection) == ("OPEN", PR.RETIREMENT_PENDING, PR.PROTECTED)
    assert world.stop_prices(BTC) == [OLD, NEW]                     # over-protected is safe
    assert world.fake.protection_gaps(BTC) == world.baseline
    assert world.protection().journal.active_blocks() == ()        # the replacement is confirmed: nothing is in doubt
    assert replacement_row(world)["retire_pending"] == 1
    (resumed,) = world.protection().resume()
    assert (resumed.state, resumed.outcome) == ("DONE", PR.REPLACED)
    one_stop_at(world, NEW)


def test_a_retirement_whose_acknowledgement_is_lost_is_settled_by_reading_the_venue(overlap):
    world = overlap
    world.fake.fail_next("cancel_order", c.NETWORK_UNCERTAIN, after_effect=True)      # cancelled; the answer was lost
    result = replace(world)
    assert result.outcome == PR.RETIREMENT_PENDING and world.stop_prices(BTC) == [NEW]
    (resumed,) = world.protection().resume()
    assert (resumed.state, resumed.outcome) == ("DONE", PR.REPLACED)
    one_stop_at(world, NEW)
    assert [e.method for e in world.fake.mutations].count("cancel_order") == 1       # it was not cancelled twice


# ===================================================================================== replacement rejection
@pytest.mark.parametrize("code", [c.STOP_WOULD_TRIGGER_IMMEDIATELY, c.INVALID_PRICE, c.POSITION_MODE_MISMATCH])
def test_a_rejected_replacement_keeps_the_old_stop(world, code):
    """A rejected trigger, a price the venue does not take, a venue order-mode conflict: nothing was created."""
    world.fake.fail_next(establishing(world), code)
    result = replace(world)
    assert (result.state, result.outcome, result.protection) == ("REFUSED", f"VENUE_REFUSED:{code}", PR.PROTECTED)
    one_stop_at(world, OLD)
    assert [e.method for e in world.fake.mutations if e.effect] == []
    assert world.protection().journal.active_blocks() == ()
    assert replace(world).replayed                                  # final: the same request is not tried again


def test_a_venue_that_declares_an_overlap_and_then_refuses_the_second_stop_loses_nothing(overlap):
    """Venue order-mode conflict against its own declaration: the old stop is only retired after a confirmed create."""
    world = overlap
    world.fake.fail_next("place_order", c.STOP_ALREADY_EXISTS)
    result = replace(world)
    assert (result.state, result.outcome) == ("REFUSED", f"VENUE_REFUSED:{c.STOP_ALREADY_EXISTS}")
    one_stop_at(world, OLD)
    assert [e.method for e in world.fake.mutations] == ["place_order"]               # no cancel was ever sent


# ======================================================================================= crash between steps
def venue_calls(tmp_path_factory, tmp_path, name) -> int:
    probe = build(tmp_path_factory, tmp_path / "probe", name)
    replace(probe)
    return len(probe.adapter.calls)


@pytest.mark.parametrize("name", sorted(PROTOCOL_VENUES))
@pytest.mark.parametrize("after_effect", [False, True])
def test_a_crash_at_any_venue_call_is_resumed_without_a_gap(tmp_path_factory, tmp_path, name, after_effect):
    (tmp_path / "probe").mkdir()
    total = venue_calls(tmp_path_factory, tmp_path, name)
    assert total >= 5
    for index in range(total):
        folder = tmp_path / f"crash_{index}"
        folder.mkdir()
        world = build(tmp_path_factory, folder, name)
        world.adapter.die_at(index, after_effect=after_effect)
        with pytest.raises(E.SimulatedCrash):
            replace(world)
        died_in = world.adapter.calls[-1]
        assert world.fake.protection_gaps(BTC) == world.baseline, (index, died_in)   # dead, and still covered
        row = replacement_row(world)
        assert row["state"] == "OPEN"                               # the step was written before it was taken
        journal = world.protection().journal
        assert len(journal.unanswered()) == (1 if died_in in c.MUTATING_METHODS else 0), (index, died_in)
        world.adapter.crash_at = None
        (resumed,) = world.protection().resume()                    # a new process
        assert (resumed.state, resumed.outcome) == ("DONE", PR.REPLACED), (index, died_in, resumed)
        one_stop_at(world, NEW)
        never_cancelled_first(world)
        assert world.adapter.open_transactions == []
        calls = len(world.adapter.calls)
        assert replace(world).replayed and len(world.adapter.calls) == calls         # and a retry changes nothing


# ========================================================================================== duplicate retry
def test_a_duplicate_retry_changes_nothing(world):
    first = replace(world)
    mutations, reads = len(world.fake.mutations), len(world.fake.reads)
    second = replace(world)
    assert second.replayed and (second.state, second.outcome, second.new_stop_price) == (first.state, first.outcome, NEW)
    assert (len(world.fake.mutations), len(world.fake.reads)) == (mutations, reads)
    conflicting = replace(world, 49_700.0)                          # the same key for another request is not a retry
    assert (conflicting.state, conflicting.outcome) == ("REFUSED", PR.CONFLICTING_REPLAY)
    assert (len(world.fake.mutations), len(world.fake.reads)) == (mutations, reads)
    one_stop_at(world, NEW)
    with pytest.raises(Exception):                                  # the database itself keeps a finished row final
        with world.db.connect() as conn:
            conn.execute(f"UPDATE {REPLACEMENT_TABLE} SET new_stop_price=1 WHERE request_key='move-1'")


# ============================================================================ position closes during replacement
def test_a_position_that_closes_during_the_replacement_leaves_no_stop_behind(world):
    fake = world.fake
    world.adapter.hook(establishing(world), lambda: fake.set_price(BTC, 48_900.0))   # the old stop fires first
    result = replace(world)
    assert (result.state, result.outcome, result.protection) == ("DONE", PR.POSITION_CLOSED, PR.FLAT)
    assert world.signed(BTC) == 0.0 and world.stops(BTC) == []      # every remaining stop of it was retired
    assert fake.protection_gaps(BTC) == world.baseline
    assert world.protection().journal.active_blocks() == ()


def test_a_position_that_was_already_closed_is_not_given_a_stop(world):
    world.fake.set_price(BTC, 48_900.0)
    world.fake.mutations.clear()
    result = replace(world, 48_000.0)
    assert (result.state, result.outcome) == ("DONE", PR.POSITION_CLOSED)
    assert world.stops(BTC) == [] and [e.method for e in world.fake.mutations if e.method != "cancel_order"] == []


def test_a_stop_that_vanished_while_the_position_is_open_is_reported_absent(world):
    """Somebody cancelled the stop at the venue. That is not this module's to repair silently: it is ABSENT, new
    entries are blocked, and the execution boundary closes the position."""
    (old,) = world.stops(BTC)
    world.adapter.hook(establishing(world), lambda: world.fake.cancel_order(BTC, venue_order_id=old.venue_order_id))
    result = replace(world)
    if world.fake.capabilities().stop_amend_in_place:
        assert (result.state, result.outcome, result.protection) == ("UNWOUND", PR.PROTECTION_ABSENT, PR.ABSENT)
        assert [b["reason"] for b in world.protection().journal.active_blocks()] == [PR.BLOCK_PROTECTION_ABSENT]
    else:
        # the replacement was created beside it and is confirmed: the position never lost its last stop
        assert (result.state, result.outcome, result.protection) == ("DONE", PR.REPLACED, PR.PROTECTED)
        assert world.stop_prices(BTC) == [NEW]


# ================================================================================== exchange rate limiting
def test_a_rate_limited_venue_is_not_asked_again_before_the_time_it_named(world):
    world.fake.rate_limit_for(30)
    result = replace(world)
    assert (result.state, result.outcome) == ("OPEN", PR.DEFERRED)
    assert result.retry_at_ms is not None and result.retry_at_ms > world.clock.t
    assert world.fake.mutations == [] and world.protection().journal.active_blocks() == ()   # the old stop is known to rest
    calls = len(world.adapter.calls)
    assert world.protection().resume() == () and replace(world).outcome == PR.DEFERRED
    assert len(world.adapter.calls) == calls                        # not one call before the retry time
    world.clock.advance(31_000)
    (resumed,) = world.protection().resume()
    assert (resumed.state, resumed.outcome) == ("DONE", PR.REPLACED)
    one_stop_at(world, NEW)


def test_a_rate_limit_on_the_replacement_itself_defers_it_and_keeps_the_old_stop(world):
    world.fake.fail_next(establishing(world), AdapterError(c.RATE_LIMITED, "too many orders", retry_after_s=10))
    result = replace(world)
    assert (result.state, result.outcome) == ("OPEN", PR.DEFERRED)
    assert result.retry_at_ms == pytest.approx(world.clock.t + 10_000, abs=50)
    one_stop_at(world, OLD)
    calls = len(world.adapter.calls)
    assert world.protection().resume() == () and len(world.adapter.calls) == calls
    world.clock.advance(10_100)
    (resumed,) = world.protection().resume()
    assert (resumed.state, resumed.outcome) == ("DONE", PR.REPLACED)
    one_stop_at(world, NEW)
    never_cancelled_first(world)


def test_a_rate_limit_on_the_retirement_leaves_both_stops_until_the_venue_allows_it(overlap):
    world = overlap
    world.fake.fail_next("cancel_order", AdapterError(c.RATE_LIMITED, "slow down", retry_after_s=5))
    assert replace(world).outcome == PR.RETIREMENT_PENDING and world.stop_prices(BTC) == [OLD, NEW]
    assert world.protection().resume() == ()
    world.clock.advance(5_100)
    (resumed,) = world.protection().resume()
    assert resumed.outcome == PR.REPLACED
    one_stop_at(world, NEW)


# ================================================================================ first stop and restoration
@pytest.mark.parametrize("profile", c.VENUES)
def test_a_position_without_a_stop_gets_one_for_the_whole_quantity_and_it_is_read_back(tmp_path_factory, tmp_path, profile):
    world = E.make_world(tmp_path_factory, tmp_path, profile=profile)
    world.fake.external_trade(BTC, c.BUY, QTY)
    outcome = world.protection().protect(BTC, C.LONG, OLD, origin="test")
    assert (outcome.status, outcome.stop_price, outcome.position_quantity) == (PR.PROTECTED, OLD, QTY)
    (stop,) = world.stops(BTC)
    assert stop.venue_order_id == outcome.venue_order_id            # persisted: not every venue keeps the client id
    assert not world.fake.protection_gaps(BTC)[-1].open
    again = world.protection().protect(BTC, C.LONG, OLD, origin="test")               # verified by read-back, nothing sent
    assert again.status == PR.PROTECTED and len(world.sent("place_order")) == 1
    world.fake.external_trade(BTC, c.BUY, 0.01)                     # the position grows: is it still covered?
    grown = world.protection().protect(BTC, C.LONG, OLD, origin="test")
    if world.protection().follows_position():
        assert grown.status == PR.PROTECTED and grown.position_quantity == pytest.approx(0.03)   # read back, not assumed
    else:
        assert (grown.status, grown.reason) == (PR.UNDER_PROTECTED, PR.REPLACEMENT_NOT_POSSIBLE_SAFELY)
        assert world.stop_prices(BTC) == [OLD]                      # the stop that rests was not taken away
        assert world.protection().journal.active_blocks() != ()


@pytest.mark.parametrize("code,status", [(c.STOP_WOULD_TRIGGER_IMMEDIATELY, PR.ABSENT), (c.PERMISSION_DENIED, PR.ABSENT),
                                         (c.VENUE_UNAVAILABLE, PR.PENDING), (c.RATE_LIMITED, PR.PENDING)])
def test_a_first_stop_that_cannot_be_placed_is_reported_for_what_it_is(tmp_path_factory, tmp_path, code, status):
    world = E.make_world(tmp_path_factory, tmp_path)
    world.fake.external_trade(BTC, c.BUY, QTY)
    world.fake.fail_next("place_order", code)
    outcome = world.protection().protect(BTC, C.LONG, OLD)
    assert outcome.status == status and world.stops(BTC) == []
    assert world.protection().journal.active_blocks() != ()         # either way no new entry until it is settled
    if status == PR.PENDING:
        world.clock.advance(61_000)
        (retried,) = world.protection().resume()
        assert retried.status == PR.PROTECTED and world.stop_prices(BTC) == [OLD]
        assert world.protection().journal.active_blocks() == ()
    else:
        assert world.protection().resume() == ()                    # ABSENT is the boundary's to act on, not retried here


def test_an_unanswered_first_stop_is_looked_for_before_anything_else_is_sent(tmp_path_factory, tmp_path):
    world = E.make_world(tmp_path_factory, tmp_path)
    world.fake.external_trade(BTC, c.BUY, QTY)
    world.fake.fail_next("place_order", c.NETWORK_UNCERTAIN, after_effect=True)       # it rests; the answer was lost
    outcome = world.protection().protect(BTC, C.LONG, OLD)
    assert outcome.status == PR.PROTECTED and world.stop_prices(BTC) == [OLD]
    assert len(world.sent("place_order")) == 1


def test_a_closed_position_has_its_resting_stop_retired(tmp_path_factory, tmp_path):
    world = build(tmp_path_factory, tmp_path, c.BINANCE)
    world.protection().protect(BTC, C.LONG, OLD)                    # adopt the stop that rests
    world.fake.place_order(OrderRequest(symbol=BTC, side=c.SELL, order_type=c.MARKET, client_order_id="close-1",
                                        quantity=QTY, reduce_only=True))
    assert world.stop_prices(BTC) == [OLD]                          # on this venue it outlives the position
    world.fake.fail_next("cancel_order", c.VENUE_UNAVAILABLE)
    pending = world.protection().retire(BTC, C.LONG)
    assert pending.status == PR.RETIRE_PENDING
    (block,) = world.protection().journal.active_blocks(BTC)        # a stale stop blocks new entries on ITS symbol only
    assert (block["reason"], block["scope"]) == (PR.BLOCK_STALE_PROTECTION, "SYMBOL")
    assert world.protection().journal.active_blocks(E.ETH) == ()
    (retired,) = world.protection().resume()
    assert retired.status == PR.FLAT and world.stops(BTC) == [] and world.protection().journal.active_blocks() == ()
