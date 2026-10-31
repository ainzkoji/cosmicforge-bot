"""Follow-up E-01 and the in-flight risk gap: a rebalance moves the stop with it, and a reservation never states
less risk than is enforceable while its order is in flight.

**Rebalance down.** Research rule I10 ratchets the stop at every close. A symbol carries one intent per decision,
so a rebalance-down day's ratchet rides on the reduction (``daily_trend._with_ratchet``); the portfolio gate
authorizes the tighter stop for what remains (``portfolio._remaining_stop``), and the boundary moves the venue
stop BEFORE the reduction leaves (``PortfolioExecutionBoundary._tighten_for_remainder``) by the same gap-free
replacement every stop move uses. Where the venue has no safe replacement (BingX) the resting stop stays and the
reduction still goes.

**Rebalance up.** The increase fills under the wider resting stop, so its reservation states its risk there
(``portfolio._sizing_stop``), and the boundary measures an executable price at that same stop
(``Reservation.risk_stop_price``). The earlier undercount of ``quantity x (ratchet - resting)`` is gone.

Every scenario asserts on the venue's own record: ``FakeVenue.protection_gaps`` lists each interval in which a
non-zero position had no resting stop covering it. The fake profiles follow each venue's documentation; that is
not evidence about the venue. A venue certification run is.
"""
from __future__ import annotations

import threading

import pytest

import _step3_execution as E
from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.contract import AdapterError
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import execution as X
from app.trading_intelligence.pipeline import protection as PR
from app.trading_intelligence.pipeline import reservations as R
from shared_lib.persistence import cati_execution_schema as schema

BTC = E.BTC
QTY, HALF, OLD, NEW = 0.02, 0.01, 49_000.0, 49_500.0
#: (profile, hypothetical overlap capability); see test_step3_stop_replacement for what each one is
VENUES = {"bybit": (c.BYBIT, False), "binance": (c.BINANCE, False), "bingx": (c.BINGX, False),
          "overlap-bingx": (c.BINGX, True)}
PROTOCOL_VENUES = ("binance", "bybit", "overlap-bingx")


def build(tmp_path_factory, tmp_path, name, **kw):
    profile, overlap = VENUES[name]
    world = E.make_world(tmp_path_factory, tmp_path, profile=profile, overlap=overlap, **kw)
    world.baseline = E.open_long(world, BTC, QTY, OLD)
    return world


@pytest.fixture(params=sorted(VENUES))
def world(request, tmp_path_factory, tmp_path):
    w = build(tmp_path_factory, tmp_path, request.param)
    w.name = request.param
    return w


@pytest.fixture(params=PROTOCOL_VENUES)
def safe(request, tmp_path_factory, tmp_path):
    w = build(tmp_path_factory, tmp_path, request.param)
    w.name = request.param
    return w


def down(stop=NEW, target=HALF, **kw):
    return C.RebalanceIntent(symbol=BTC, direction=C.LONG, current_quantity=QTY, target_quantity=target,
                             reference_price=E.PRICES[BTC], stop_price=stop, band=0.05, **kw)


def covered_now(world) -> bool:
    gaps = world.fake.protection_gaps(BTC)
    return not gaps or not gaps[-1].open


def no_new_gap(world) -> None:
    assert world.fake.protection_gaps(BTC) == world.baseline


def one_stop(world, price) -> None:
    (stop,) = world.stops(BTC)
    assert stop.stop_price == price and (stop.close_position or stop.quantity >= abs(world.signed(BTC)) - 1e-12)


def reductions_sent(world):
    return [r for r in world.orders_sent(BTC, protective=False) if r.reduce_only]


def replacement(world, intent_id):
    rows = world.rows(schema.REPLACEMENT_TABLE, "request_key=?", (f"reduce|{intent_id}",))
    return rows[0] if rows else None


def never_cancelled_first(world):
    methods = [e.method for e in world.fake.mutations if e.effect]
    if "cancel_order" in methods:
        assert "place_order" in methods[:methods.index("cancel_order")] or "amend_stop" in methods


# ===================================================================================== rebalance down: the move
def test_a_rebalance_down_moves_the_stop_for_what_remains_without_a_gap(world):
    result = world.run(down())
    outcome = result.outcome(BTC)
    assert (outcome.state, outcome.increases_risk) == (X.FILLED, False)
    assert world.signed(BTC) == pytest.approx(HALF)
    (reduction,) = reductions_sent(world)
    assert (reduction.side, reduction.quantity) == (c.SELL, HALF)
    if world.name == "bingx":
        # no gap-free replacement is declared: refused, the resting stop stays and still covers what remains
        assert f"STOP_REPLACEMENT:{PR.REPLACEMENT_NOT_POSSIBLE_SAFELY}" in outcome.reasons
        one_stop(world, OLD)
        assert replacement(world, outcome.intent_id)["state"] == schema.REPLACEMENT_REFUSED
    else:
        assert f"STOP_REPLACEMENT:{PR.REPLACED}" in outcome.reasons
        one_stop(world, NEW)
        row = replacement(world, outcome.intent_id)
        assert (row["state"], row["reason"], row["requested_stop_price"]) == (schema.REPLACEMENT_DONE, C.TIGHTEN, NEW)
        never_cancelled_first(world)
    no_new_gap(world)
    assert outcome.protection == PR.PROTECTED and covered_now(world)
    # the stop moved before the reduction left: the replacement's venue calls precede the closing order
    methods = [(e.method, getattr(e.arguments.get("request"), "reduce_only", None),
                getattr(e.arguments.get("request"), "protective", None)) for e in world.fake.mutations]
    close_at = methods.index(("place_order", True, False))
    if world.name != "bingx":
        assert any(m in ("amend_stop", "place_order") and p is not False for m, _r, p in methods[:close_at])


def test_the_protection_record_follows_the_tightened_stop(safe):
    """A stop that later has to be placed again on this position is placed at the tightened level, never back
    at the wider one (``ProtectionManager._confirmed``)."""
    safe.run(down())
    (record,) = safe.rows(schema.PROTECTION_TABLE, "symbol=?", (BTC,))
    assert (record["status"], record["resting_stop_price"], record["intended_stop_price"]) == (PR.PROTECTED, NEW, NEW)
    for stop in safe.stops(BTC):                                       # the stop disappears (cancelled elsewhere)
        safe.fake.cancel_order(BTC, venue_order_id=stop.venue_order_id)
    restored = safe.protection().verify(BTC, C.LONG)
    assert restored.protected and restored.stop_price == NEW and safe.stop_prices(BTC) == [NEW]


def test_a_rebalance_down_at_the_resting_stop_asks_for_no_replacement(world):
    result = world.run(down(stop=OLD))
    assert result.outcome(BTC).state == X.FILLED and world.signed(BTC) == pytest.approx(HALF)
    assert world.rows(schema.REPLACEMENT_TABLE) == []
    assert [e.method for e in world.fake.mutations if e.effect] == ["place_order"]     # the reduction only
    one_stop(world, OLD)
    no_new_gap(world)


def test_a_wider_stated_stop_is_never_taken_and_the_reduction_still_goes(world):
    result = world.run(down(stop=48_000.0))
    assert result.outcome(BTC).state == X.FILLED and world.signed(BTC) == pytest.approx(HALF)
    assert world.rows(schema.REPLACEMENT_TABLE) == []
    one_stop(world, OLD)
    no_new_gap(world)


def test_a_partial_reduction_leaves_the_tightened_stop_covering_what_remains(safe):
    """A reduction the venue fills in parts (the fake holds the market order open): the stop was moved first and
    covers every intermediate quantity; the read-back after each fill confirms it."""
    safe.fake.fill_market_orders = False
    result = safe.run(down())
    outcome = result.outcome(BTC)
    assert outcome.state == X.OPEN and safe.signed(BTC) == pytest.approx(QTY)
    one_stop(safe, NEW)
    (order_id,) = outcome.client_order_ids
    safe.fake.fill(order_id, 0.004)
    assert safe.boundary().recover().unresolved == ()
    assert safe.signed(BTC) == pytest.approx(0.016)
    one_stop(safe, NEW)
    cancelled = safe.boundary().cancel(outcome.intent_id, reason="TEST_CANCEL")
    assert (cancelled.state, cancelled.filled_quantity) == (X.PARTIAL, 0.004)
    one_stop(safe, NEW)
    no_new_gap(safe)
    assert len(reductions_sent(safe)) == 1 and len(safe.rows(schema.REPLACEMENT_TABLE, "state='DONE'")) >= 1


def test_a_full_close_by_rebalance_retires_the_stop_and_moves_nothing(world):
    result = world.run(C.ExitIntent(symbol=BTC, direction=C.LONG, reason="SIGNAL", reference_price=E.PRICES[BTC]))
    assert result.outcome(BTC).state == X.FILLED and world.signed(BTC) == 0.0
    assert world.rows(schema.REPLACEMENT_TABLE) == []                 # an exit moves no stop


def test_a_plain_reduce_keeps_the_resting_stop_covering_the_remainder(world):
    result = world.run(C.ReduceIntent(symbol=BTC, direction=C.LONG, reason="DE_RISK", fraction=0.5))
    assert result.outcome(BTC).state == X.FILLED and world.signed(BTC) == pytest.approx(HALF)
    one_stop(world, OLD)
    no_new_gap(world)


# ============================================================================ rebalance down: what can go wrong
def test_the_stop_firing_during_the_move_ends_the_rebalance_without_a_closing_order(safe):
    """The price falls between the two triggers while the tighter stop is being established: the venue closes
    the position. Nothing is left to reduce; no closing order is sent and no stop is left behind."""
    call = "amend_stop" if safe.fake.capabilities().stop_amend_in_place else "place_order"
    safe.adapter.hook_after(call, lambda: safe.fake.set_price(BTC, 49_400.0))
    result = safe.run(down())
    outcome = result.outcome(BTC)
    assert outcome.state == X.DONE and f"STOP_REPLACEMENT:{PR.POSITION_CLOSED}" in outcome.reasons
    assert safe.signed(BTC) == 0.0 and safe.stops(BTC) == [] and reductions_sent(safe) == []
    no_new_gap(safe)


def test_a_refused_replacement_keeps_the_old_stop_and_the_reduction_still_goes(tmp_path_factory, tmp_path):
    world = build(tmp_path_factory, tmp_path, "binance")
    world.fake.fail_next("place_order", c.INVALID_PRICE)               # the replacement stop is refused
    result = world.run(down())
    outcome = result.outcome(BTC)
    assert outcome.state == X.FILLED and f"STOP_REPLACEMENT:VENUE_REFUSED:{c.INVALID_PRICE}" in outcome.reasons
    assert world.signed(BTC) == pytest.approx(HALF)
    one_stop(world, OLD)
    no_new_gap(world)


def test_a_rate_limited_move_sends_nothing_and_the_same_decision_continues_after_it(safe):
    safe.fake.fail_next("positions", AdapterError(c.RATE_LIMITED, "slow down", retry_after_s=5))
    boundary = safe.boundary()
    batch = safe.decide(down(), ttl=E.HOUR)
    first = boundary.execute(batch, brake_state=E.NO_BRAKE)
    assert first.status in (X.BATCH_HALTED, X.BATCH_PROCESSED)
    assert reductions_sent(safe) == [] and safe.signed(BTC) == pytest.approx(QTY)
    one_stop(safe, OLD)
    safe.clock.advance(5_100)
    second = safe.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert second.outcome(BTC).state == X.FILLED and safe.signed(BTC) == pytest.approx(HALF)
    one_stop(safe, NEW)
    assert len(reductions_sent(safe)) == 1
    no_new_gap(safe)


def test_a_disconnected_venue_mid_move_is_reconciled_and_never_leaves_the_remainder_bare(safe):
    """A REST outage right after the venue acted on the move: the replacement is open and uncertain, entries are
    blocked, the reduction still goes (a reduction is never held back), and the next read settles both."""
    safe.fake.fail_next("user_stream", c.VENUE_UNAVAILABLE, times=1_000)
    call = "amend_stop" if safe.fake.capabilities().stop_amend_in_place else "place_order"
    safe.adapter.hook_after(call, lambda: safe.fake.fail_next("positions", c.VENUE_UNAVAILABLE))
    result = safe.run(down())
    assert f"STOP_REPLACEMENT:{PR.OUTCOME_UNCERTAIN}" in result.outcome(BTC).reasons
    assert safe.signed(BTC) == pytest.approx(HALF) and covered_now(safe)
    assert safe.boundary().entry_blocks() != ()
    report = safe.boundary().recover()
    assert report.unresolved == ()
    one_stop(safe, NEW)
    assert safe.boundary().entry_blocks() == ()
    no_new_gap(safe)


def test_an_unknown_reduction_outcome_is_looked_up_never_resent_and_the_stop_stays_moved(safe):
    """The answer to the closing order is lost after the venue filled it."""
    boundary = safe.boundary()
    original = boundary._send

    def lossy(run, row, request):
        if row.reduce_only and not request.protective:
            safe.fake.fail_next("place_order", c.NETWORK_UNCERTAIN, after_effect=True)
        return original(run, row, request)

    boundary._send = lossy
    result = boundary.execute(safe.decide(down()), brake_state=E.NO_BRAKE)
    assert result.outcome(BTC).state == X.FILLED                      # the lookup by client id found it filled
    assert len(reductions_sent(safe)) == 1 and safe.signed(BTC) == pytest.approx(HALF)
    one_stop(safe, NEW)
    no_new_gap(safe)


def test_a_replay_of_the_same_batch_sends_nothing_and_asks_for_no_second_move(world):
    batch = world.decide(down())
    world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    mutations = len(world.fake.mutations)
    again = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert again.outcome(BTC).state == X.FILLED and len(world.fake.mutations) == mutations
    moves = world.rows(schema.REPLACEMENT_TABLE, "request_key LIKE 'reduce|%' AND request_key NOT LIKE ?",
                       (f"%{PR.POSITION_BOUND_SUFFIX}",))
    assert len(moves) == 1


@pytest.mark.parametrize("name", PROTOCOL_VENUES)
@pytest.mark.parametrize("after_effect", [False, True])
def test_a_crash_at_any_venue_call_of_a_rebalance_down_is_recovered_without_a_gap(tmp_path_factory, tmp_path, name,
                                                                                   after_effect):
    (tmp_path / "probe").mkdir()
    probe = build(tmp_path_factory, tmp_path / "probe", name)
    batch = probe.decide(down())
    probe.adapter.calls.clear()
    probe.boundary().execute(batch, brake_state=E.NO_BRAKE)
    total = len(probe.adapter.calls)
    assert total >= 6
    for index in range(total):
        folder = tmp_path / f"crash_{index}"
        folder.mkdir()
        world = build(tmp_path_factory, folder, name)
        batch = world.decide(down())
        world.adapter.calls.clear()
        world.adapter.die_at(index, after_effect=after_effect)
        with pytest.raises(E.SimulatedCrash):
            world.boundary().execute(batch, brake_state=E.NO_BRAKE)
        where = (index, after_effect, world.adapter.calls[-1])
        no_new_gap(world)                                              # dead, and still covered
        world.adapter.crash_at = None
        world.boundary().recover()                                     # the new process, before anything else
        world.clock.advance(121_000)                                   # past the window of an order that never arrived
        assert world.boundary().recover().unresolved == (), where
        final = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
        assert final.status == X.BATCH_PROCESSED, where
        # exactly one reduction ever left; the position is either reduced or (an order proven absent) untouched
        sent = reductions_sent(world)
        assert len({r.client_order_id for r in sent}) <= 1, where
        assert world.signed(BTC) in (pytest.approx(HALF), pytest.approx(QTY)), where
        one_stop(world, NEW)
        no_new_gap(world)
        never_cancelled_first(world)
        assert world.rows(schema.REPLACEMENT_TABLE, "state='OPEN'") == [], where
        assert world.rows(schema.INTENT_TABLE, "state IN ('SUBMITTING', 'UNKNOWN', 'CANCELING')") == [], where
        assert world.adapter.open_transactions == [], where


def test_two_accounts_rebalance_down_concurrently_and_never_touch_each_other(tmp_path_factory, tmp_path):
    one = build(tmp_path_factory, tmp_path, "binance")
    two = E.make_world(tmp_path_factory, tmp_path, profile=c.BYBIT, account_id="acct-2", user_id="user-2", db=one.db,
                       clock=one.clock)
    two.baseline = E.open_long(two, BTC, QTY, OLD)
    batches = {id(w): w.decide(down()) for w in (one, two)}
    results, errors = {}, []

    def go(w):
        try:
            results[w.account.broker_account_id] = w.boundary().execute(batches[id(w)], brake_state=E.NO_BRAKE)
        except Exception as error:  # noqa: BLE001 -- reported below
            errors.append(error)

    threads = [threading.Thread(target=go, args=(w,)) for w in (one, two)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert errors == [] and set(results) == {E.ACCOUNT, "acct-2"}
    for w in (one, two):
        assert results[w.account.broker_account_id].outcome(BTC).state == X.FILLED
        assert w.signed(BTC) == pytest.approx(HALF)
        one_stop(w, NEW)
        no_new_gap(w)
    keys = {r["account_id"] for r in one.rows(schema.REPLACEMENT_TABLE)}
    assert keys == {E.ACCOUNT, "acct-2"}


# ================================================================================ rebalance up: in-flight risk
#: venues whose stop follows an increase at once (position-bound, or amended in place). A quantity stop that
#: cannot grow (the hypothetical overlap-bingx) is outgrown for an instant at every fill of an increase; that is
#: the existing behaviour of increases in general and is covered by test_step3_multi_position_execution
FOLLOWING_VENUES = ("binance", "bybit")


@pytest.fixture(params=FOLLOWING_VENUES)
def follows(request, tmp_path_factory, tmp_path):
    w = build(tmp_path_factory, tmp_path, request.param)
    w.name = request.param
    return w


def up(stop):
    return C.RebalanceIntent(symbol=BTC, direction=C.LONG, current_quantity=QTY, target_quantity=0.03,
                             reference_price=E.PRICES[BTC], stop_price=stop, band=0.05)


def test_a_rebalance_up_reserves_its_risk_at_the_stop_it_fills_under(follows):
    """The ratcheted increase is sized AND reserved at the resting 49,000 stop; after the fill the stop is
    tightened to 49,500 for the whole position. At no point is less risk reserved than the venue enforces."""
    result = follows.run(up(NEW))
    outcome = result.outcome(BTC)
    assert outcome.state == X.FILLED and outcome.increases_risk
    (reservation,) = follows.rows("cati_reservation_ledger", "symbol=?", (BTC,))
    quantity = reservation["quantity"]
    assert quantity > 0 and reservation["stop_price"] == NEW
    assert reservation["risk_amount"] == pytest.approx(quantity * (E.PRICES[BTC] - OLD))
    held = R.ReservationLedger(follows.db).get(E.ACCOUNT, reservation["reservation_id"])
    assert held.risk_stop_price == pytest.approx(OLD) and held.state == R.FILLED
    assert follows.signed(BTC) == pytest.approx(QTY + quantity)
    one_stop(follows, NEW)                                                # tightened after the fill
    no_new_gap(follows)


def test_a_rebalance_up_is_refused_when_the_mark_rose_above_the_reference(follows):
    """The executable-price check measures at the same resting stop the reservation states, so a price above
    the reference cannot spend more risk than was reserved."""
    follows.fake.set_price(BTC, 50_050.0)
    result = follows.run(up(NEW))
    assert result.outcome(BTC).state == X.REFUSED and "EXECUTABLE_PRICE_RISK_CHANGED" in " ".join(result.outcome(BTC).reasons)
    assert follows.signed(BTC) == pytest.approx(QTY) and follows.reservations()[BTC] == R.RELEASED


def test_a_rebalance_up_at_the_reference_is_admitted(follows):
    assert follows.run(up(NEW)).outcome(BTC).state == X.FILLED
