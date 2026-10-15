"""The venue adapter contract, run unchanged against every adapter (Step 4.1 acceptance).

One suite, six subjects: the Binance, Bybit and BingX adapters, each around its REAL raw client class with a
fake HTTP layer beneath it, and ``FakeVenue`` in its three profiles. No test here touches a network.

Read ``_platform_adapter_wire.py`` before trusting a green run. The answers the real adapters parse are
rendered by that module from the keys the raw clients read; it lists which shapes were recorded from a venue
(two, both Binance public endpoints) and which were not (all the others). A pass therefore proves that
requests are translated into each venue's dialect and answers of those shapes back into the contract, for
every method of the protocol. It does not prove that a venue behaves as the fake behind the wire does.

Two places where the subjects legitimately differ are written into the tests rather than hidden:

* a refusal the venue signals with a code the adapter has no evidence for arrives as ``UNMAPPED_VENUE_ERROR``
  (``rig.expected``); the fake raises the contract code itself;
* a stop that fired can no longer be read through the Binance and Bybit adapters (the raw clients expose only
  what rests); the fake and the BingX adapter still return it.
"""
from __future__ import annotations

import dataclasses

import pytest

from _platform_adapter_wire import (ALL_RIGS, BAN_S, BTC, ETH, RETRY_AFTER_S, SECRET, SECRET_KEY, build_rig, limit,
                                    market)
from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.contract import AdapterError, OrderRequest
from app.exchange.platform_adapter.factory import ADAPTER_VERSIONS

#: the capability matrix both the real adapters and the fake profiles must state
CAPABILITIES = {
    c.BINANCE: dict(close_position_stop=True, quantity_stop=False, stop_amend_in_place=False,
                    position_stop_endpoint=False, stop_client_order_id=True, client_order_id_max_length=36),
    c.BYBIT: dict(close_position_stop=True, quantity_stop=False, stop_amend_in_place=True,
                  position_stop_endpoint=True, stop_client_order_id=False, client_order_id_max_length=36),
    c.BINGX: dict(close_position_stop=False, quantity_stop=True, stop_amend_in_place=False,
                  position_stop_endpoint=False, stop_client_order_id=False, client_order_id_max_length=40),
}
COMMON_CAPABILITIES = dict(native_stop_market=True, native_take_profit=True, reduce_only=True,
                           multiple_stops_per_position=False, one_way_mode=True, hedge_mode=False, demo_environment=True)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """The Binance raw client sleeps between its own bounded read retries; a test never waits for it."""
    import time

    naps = []
    monkeypatch.setattr(time, "sleep", naps.append)
    return naps


@pytest.fixture(params=ALL_RIGS)
def rig(request, monkeypatch):
    return build_rig(request.param, monkeypatch)


def refused(call) -> AdapterError:
    with pytest.raises(AdapterError) as caught:
        call()
    return caught.value


def open_long(rig, quantity: float = 0.01):
    return rig.adapter.place_order(market(BTC, c.BUY, quantity, "entry-1"))


def resting_stops(rig, symbol: str = BTC):
    return [o for o in rig.adapter.open_orders(symbol) if o.order_type in (c.STOP_MARKET, c.TAKE_PROFIT_MARKET)]


def since(rig) -> int:
    return rig.fake.now_ms() - 1


# ================================================================================ identity and capabilities
def test_every_subject_is_a_venue_adapter_of_its_venue(rig):
    adapter = rig.adapter
    assert isinstance(adapter, c.VenueAdapter)
    assert (adapter.venue, adapter.environment) == (rig.venue, c.DEMO)
    assert all(callable(getattr(adapter, name)) for name in c.READ_METHODS + c.MUTATING_METHODS)
    if rig.real:
        assert adapter.adapter_version == ADAPTER_VERSIONS[rig.venue] and not adapter.adapter_version.startswith("fake")
    else:
        assert adapter.adapter_version.startswith("fake-")


def test_capabilities_state_the_same_matrix_for_the_adapter_and_its_fake_profile(rig):
    caps = rig.adapter.capabilities()
    assert (caps.venue, caps.environment) == (rig.venue, c.DEMO)
    stated = dataclasses.asdict(caps)
    for name, value in {**COMMON_CAPABILITIES, **CAPABILITIES[rig.venue]}.items():
        assert stated[name] == value, name
    # the only flag the fake may state differently: it can describe a (fake) stream, the adapters cannot
    assert caps.user_stream is (not rig.real or rig.venue != c.BINGX)
    assert caps.notes and all(isinstance(note, str) and note for note in caps.notes)


# ================================================================================================== reads
def test_server_time_reports_the_venue_clock_and_the_local_one(rig):
    assert rig.adapter.server_time().offset_ms == 0
    rig.fake.server_time_offset_ms = 2_500
    time = rig.adapter.server_time()
    assert isinstance(time, c.ServerTime) and time.offset_ms == 2_500 and time.local_ms == rig.fake.now_ms()


def test_instrument_rules_are_canonical_complete_and_follow_the_venue(rig):
    adapter = rig.adapter
    rules = adapter.instrument_rules()
    assert set(rules) == {BTC, ETH}
    btc = rules[BTC]
    assert (btc.venue, btc.environment, btc.symbol, btc.status) == (rig.venue, c.DEMO, BTC, c.TRADING)
    assert btc.venue_symbol == ("BTC-USDT" if rig.venue == c.BINGX else BTC)
    assert (btc.price_tick, btc.quantity_step, btc.min_quantity, btc.min_notional) == (0.1, 0.001, 0.001, 5.0)
    assert (btc.base_asset, btc.quote_asset, btc.margin_asset, btc.contract_size) == ("BTC", "USDT", "USDT", 1.0)
    assert btc.complete and btc.raw_hash and btc.fetched_at_ms == rig.fake.now_ms()
    assert (rules[ETH].price_tick, rules[ETH].quantity_step) == (0.01, 0.01)
    assert set(adapter.instrument_rules([BTC])) == {BTC}
    assert dict(adapter.instrument_rules(["DOGEUSDT"])) == {}        # not listed: absent, never invented
    rig.fake.set_rules(ETH, status=c.NOT_TRADING)
    rig.fake.set_rules(BTC, min_notional=10.0)
    changed = adapter.instrument_rules()
    assert changed[ETH].status == c.NOT_TRADING and changed[ETH].venue_status
    assert changed[BTC].min_notional == 10.0 and changed[BTC].raw_hash != btc.raw_hash


def test_a_listing_with_no_instrument_is_not_read_as_an_empty_market(rig):
    rig.fake.remove_instrument(BTC)
    rig.fake.remove_instrument(ETH)
    if rig.real:        # an empty listing is a failed read: the rule cache must not delist everything
        assert refused(rig.adapter.instrument_rules).code == c.MALFORMED_RESPONSE
    else:               # the fake publishes what it holds; the cache decides (it refuses an empty refresh)
        assert dict(rig.adapter.instrument_rules()) == {}


def test_fee_rates_and_funding(rig):
    fees = rig.adapter.fee_rates(BTC)
    assert (fees.venue, fees.symbol, fees.maker, fees.taker, fees.source) == (rig.venue, BTC, 0.0002, 0.0005, "ACCOUNT")
    funding = rig.adapter.funding(BTC)
    assert (funding.venue, funding.symbol, funding.rate, funding.mark_price) == (rig.venue, BTC, 0.0001, 50_000.0)
    assert funding.next_funding_ms > rig.fake.now_ms() and funding.observed_at_ms == rig.fake.now_ms()
    assert funding.interval_minutes in (None, 480)                   # None: this venue's answer does not carry it


def test_the_rate_limit_observation_is_local_and_claims_nothing_unknown(rig):
    observed = rig.adapter.rate_limit()
    assert isinstance(observed, c.RateLimitObservation)
    assert (observed.venue, observed.observed_at_ms) == (rig.venue, rig.fake.now_ms())
    assert observed.scope in ("IP", "UID") and observed.retry_after_s is None and observed.banned_until_ms is None
    if rig.real:
        assert rig.wire.calls == []                                  # nothing is asked of the venue


def test_user_stream_is_described_or_refused_for_unverified_bingx_demo(rig):
    if rig.real and rig.venue == c.BINGX:
        assert refused(rig.adapter.user_stream).code == c.UNSUPPORTED and rig.wire.calls == []
        return
    spec = rig.adapter.user_stream()
    assert spec.url.startswith("wss://" if rig.real else "fake://")
    assert spec.auth_mode in ("LISTEN_KEY_IN_URL", "AUTH_FRAME")
    assert spec.url not in repr(spec)


def test_reads_change_nothing_at_the_venue(rig):
    adapter = rig.adapter
    adapter.capabilities(), adapter.server_time(), adapter.instrument_rules(), adapter.balance()
    adapter.positions(), adapter.positions(BTC), adapter.open_orders(), adapter.open_orders(BTC)
    adapter.fills(BTC, since_ms=since(rig)), adapter.fee_rates(BTC), adapter.funding(BTC), adapter.rate_limit()
    assert refused(lambda: adapter.order(BTC, client_order_id="never-sent")).code == c.ORDER_NOT_FOUND
    assert rig.effects() == 0
    assert (rig.wire.mutations() if rig.real else rig.fake.mutations) == []


# ================================================================================================ entries
def test_a_market_entry_fills_and_shows_in_the_position_the_balance_and_the_fills(rig):
    adapter = rig.adapter
    order = open_long(rig)
    assert (order.status, order.filled_quantity, order.average_price, order.quantity) == (c.FILLED, 0.01, 50_000.0, 0.01)
    assert (order.venue, order.symbol, order.side, order.order_type) == (rig.venue, BTC, c.BUY, c.MARKET)
    assert order.client_order_id == "entry-1" and order.venue_order_id and order.final and not order.open
    (position,) = adapter.positions(BTC)
    assert (position.symbol, position.side, position.quantity, position.entry_price) == (BTC, c.LONG, 0.01, 50_000.0)
    assert (position.signed_quantity, position.leverage, position.position_mode) == (0.01, 5.0, "ONE_WAY")
    assert adapter.positions(ETH) == () and [p.symbol for p in adapter.positions()] == [BTC]
    balance = adapter.balance()
    assert balance.wallet_balance == pytest.approx(10_000 - 0.25)    # the taker fee of the entry
    assert balance.equity == pytest.approx(balance.wallet_balance + balance.unrealized_pnl)
    assert balance.available_balance == pytest.approx(balance.equity - 0.01 * 50_000.0 / 5)
    (fill,) = adapter.fills(BTC, since_ms=since(rig))
    assert (fill.symbol, fill.side, fill.quantity, fill.price) == (BTC, c.BUY, 0.01, 50_000.0)
    assert fill.venue_order_id == order.venue_order_id and fill.trade_id
    assert fill.fee == pytest.approx(0.25) and fill.fee_asset == "USDT" and fill.time_ms == rig.fake.now_ms()
    assert adapter.fills(BTC, since_ms=since(rig), venue_order_id=order.venue_order_id) == (fill,)
    assert adapter.fills(BTC, since_ms=since(rig), venue_order_id="999999") == ()
    assert adapter.fills(BTC, since_ms=rig.fake.now_ms() + 1) == ()


def test_a_repeated_client_order_id_returns_the_existing_order_and_creates_nothing(rig):
    adapter = rig.adapter
    first = open_long(rig)
    sent, effects = rig.mutations_sent(), rig.effects()
    again = open_long(rig)
    assert (again.venue_order_id, again.status, again.filled_quantity) == (first.venue_order_id, c.FILLED, 0.01)
    assert rig.effects() == effects and adapter.positions(BTC)[0].quantity == 0.01
    assert len(adapter.fills(BTC, since_ms=since(rig))) == 1
    if rig.real:
        assert rig.mutations_sent() == sent                          # the venue was asked, not told again
    else:
        assert rig.fake.mutations[-1].effect is False


def test_the_same_id_for_a_different_order_is_a_duplicate_never_a_second_order(rig):
    open_long(rig)
    effects = rig.effects()
    error = refused(lambda: rig.adapter.place_order(market(BTC, c.SELL, 0.01, "entry-1")))
    assert error.code == c.DUPLICATE_CLIENT_ORDER_ID and not error.uncertain
    assert rig.effects() == effects and rig.adapter.positions(BTC)[0].side == c.LONG


def test_a_limit_order_rests_fills_in_part_and_cancels(rig):
    adapter = rig.adapter
    order = adapter.place_order(limit(ETH, c.BUY, 0.1, 2_400.0, "limit-1"))
    assert (order.status, order.filled_quantity, order.quantity, order.order_type) == (c.NEW, 0.0, 0.1, c.LIMIT)
    assert order.open and not order.final
    (listed,) = adapter.open_orders(ETH)
    assert (listed.venue_order_id, listed.client_order_id, listed.side) == (order.venue_order_id, "limit-1", c.BUY)
    assert adapter.order(ETH, venue_order_id=order.venue_order_id).client_order_id == "limit-1"
    rig.fake.fill("limit-1", 0.04)
    partial = adapter.order(ETH, client_order_id="limit-1")
    assert (partial.status, partial.filled_quantity, partial.average_price) == (c.PARTIALLY_FILLED, 0.04, 2_400.0)
    assert partial.open
    cancelled = adapter.cancel_order(ETH, client_order_id="limit-1")
    assert (cancelled.status, cancelled.filled_quantity) == (c.CANCELED, 0.04) and cancelled.final
    assert adapter.open_orders(ETH) == () and adapter.positions(ETH)[0].quantity == 0.04
    sent, effects = rig.mutations_sent(), rig.effects()
    assert adapter.cancel_order(ETH, venue_order_id=order.venue_order_id).status == c.CANCELED
    assert rig.effects() == effects                                  # cancelling what is final changes nothing
    if rig.real:
        assert rig.mutations_sent() == sent                          # and is not even sent


def test_an_unknown_order_is_not_found_and_nothing_is_guessed(rig):
    adapter = rig.adapter
    for call in (lambda: adapter.order(BTC, client_order_id="never-sent"),
                 lambda: adapter.order(BTC, venue_order_id="424242"),
                 lambda: adapter.cancel_order(BTC, client_order_id="never-sent"),
                 lambda: adapter.cancel_order(BTC, venue_order_id="424242")):
        error = refused(call)
        assert error.code == c.ORDER_NOT_FOUND and not error.uncertain and not error.retryable
    with pytest.raises(c.ContractViolation):
        adapter.order(BTC)
    assert rig.effects() == 0
    if rig.real:
        assert rig.wire.mutations() == []                            # an order that is not there is never cancelled


def test_reduce_only_never_opens_and_never_flips_a_position(rig):
    adapter = rig.adapter
    error = refused(lambda: adapter.place_order(market(BTC, c.SELL, 0.01, "close-1", reduce_only=True)))
    assert error.code == rig.expected(c.REDUCE_ONLY_REJECTED) and not error.uncertain
    assert adapter.positions(BTC) == ()
    open_long(rig)
    error = refused(lambda: adapter.place_order(market(BTC, c.SELL, 0.02, "close-2", reduce_only=True)))
    assert error.code == rig.expected(c.REDUCE_ONLY_REJECTED)
    assert adapter.positions(BTC)[0].quantity == 0.01
    closed = adapter.place_order(market(BTC, c.SELL, 0.01, "close-3", reduce_only=True))
    assert closed.status == c.FILLED and closed.reduce_only and adapter.positions(BTC) == ()


@pytest.mark.parametrize("code,request_", [
    (c.INVALID_QUANTITY, market(BTC, c.BUY, 0.0015, "off-step")),
    (c.MIN_NOTIONAL, limit(BTC, c.BUY, 0.001, 4_000.0, "too-small")),
    (c.INSUFFICIENT_MARGIN, market(BTC, c.BUY, 5.0, "too-large")),
    (c.INSTRUMENT_UNKNOWN, market("DOGEUSDT", c.BUY, 10.0, "not-listed")),
])
def test_a_venue_rejection_is_a_definite_no(rig, code, request_):
    error = refused(lambda: rig.adapter.place_order(request_))
    assert error.code == rig.expected(code), (error.code, error.venue_code)
    assert not error.uncertain and not error.retryable and error.venue == rig.venue
    if error.code == c.UNMAPPED_VENUE_ERROR:
        assert error.venue_code                                      # the venue's own code travels with it
    assert rig.effects() == 0 and rig.adapter.positions() == ()


def test_an_instrument_that_is_not_trading_takes_no_order(rig):
    rig.fake.set_rules(ETH, status=c.NOT_TRADING)
    effects = rig.effects()
    error = refused(lambda: rig.adapter.place_order(market(ETH, c.BUY, 0.1, "entry-1")))
    assert error.code == rig.expected(c.INSTRUMENT_NOT_TRADING) and not error.uncertain
    assert rig.effects() == effects


def test_positions_and_orders_of_several_symbols_are_listed_together(rig):
    adapter = rig.adapter
    adapter.place_order(market(BTC, c.BUY, 0.01, "entry-btc"))
    adapter.place_order(market(ETH, c.SELL, 0.1, "entry-eth"))
    adapter.place_order(limit(ETH, c.SELL, 0.1, 2_600.0, "limit-eth"))
    adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-btc", 0.01))
    assert {p.symbol: (p.side, p.quantity) for p in adapter.positions()} == {BTC: (c.LONG, 0.01), ETH: (c.SHORT, 0.1)}
    assert {(o.symbol, o.order_type) for o in adapter.open_orders()} == {(ETH, c.LIMIT), (BTC, c.STOP_MARKET)}
    assert {o.symbol for o in adapter.open_orders(ETH)} == {ETH}
    assert adapter.positions(ETH)[0].signed_quantity == -0.1


def test_leverage_is_set_confirmed_and_bounded(rig):
    adapter = rig.adapter
    assert adapter.set_leverage(BTC, 10) == 10
    assert adapter.set_leverage(BTC, 10) == 10                       # already there is an answer, not a failure
    open_long(rig)
    assert adapter.positions(BTC)[0].leverage == 10.0
    error = refused(lambda: adapter.set_leverage(BTC, 500))
    assert error.code == rig.expected(c.LEVERAGE_NOT_ALLOWED) and not error.uncertain
    assert adapter.positions(BTC)[0].leverage == 10.0


# ======================================================================================= protective stops
def test_a_protective_stop_is_placed_in_the_venue_form_and_read_back(rig):
    adapter, caps = rig.adapter, rig.adapter.capabilities()
    open_long(rig)
    assert [gap.open for gap in rig.fake.protection_gaps(BTC)] == [True]
    stop = adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01))
    assert (stop.status, stop.order_type, stop.side, stop.stop_price) == (c.NEW, c.STOP_MARKET, c.SELL, 49_000.0)
    assert stop.client_order_id == "stop-1" and stop.venue_order_id and stop.open and stop.filled_quantity == 0
    assert stop.close_position is caps.close_position_stop and (stop.quantity == 0.01) is caps.quantity_stop
    assert [gap.open for gap in rig.fake.protection_gaps(BTC)] == [False]     # the venue behind it holds the stop
    for read in (adapter.order(BTC, venue_order_id=stop.venue_order_id), adapter.order(BTC, client_order_id="stop-1")):
        assert (read.venue_order_id, read.client_order_id) == (stop.venue_order_id, "stop-1")
        assert (read.status, read.order_type, read.side, read.stop_price) == (c.NEW, c.STOP_MARKET, c.SELL, 49_000.0)
    (listed,) = resting_stops(rig)
    assert (listed.venue_order_id, listed.stop_price) == (stop.venue_order_id, 49_000.0)
    assert [o.venue_order_id for o in adapter.open_orders()] == [stop.venue_order_id]


def test_a_reduce_only_stop_for_the_whole_position_is_taken_by_every_venue(rig):
    """The one form a caller can send without asking which venue it is on."""
    adapter, caps = rig.adapter, rig.adapter.capabilities()
    request_ = OrderRequest(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="stop-1",
                            stop_price=49_000.0, quantity=0.01, reduce_only=True)
    error = refused(lambda: adapter.place_order(request_))             # flat: there is nothing it could reduce
    assert error.code == rig.expected(c.REDUCE_ONLY_REJECTED) and rig.effects() == 0
    open_long(rig)
    stop = adapter.place_order(request_)
    assert (stop.status, stop.side, stop.stop_price, stop.client_order_id) == (c.NEW, c.SELL, 49_000.0, "stop-1")
    # what comes back is what rests: the venue's whole-position stop, or a stop of that quantity
    assert stop.close_position is (not caps.quantity_stop) and (stop.quantity == 0.01) is caps.quantity_stop
    read = adapter.order(BTC, venue_order_id=stop.venue_order_id)
    assert read.close_position or (read.reduce_only and read.quantity == 0.01)
    assert len(resting_stops(rig)) == 1 and not rig.fake.protection_gaps(BTC)[-1].open
    effects = rig.effects()
    assert adapter.place_order(request_).venue_order_id == stop.venue_order_id and rig.effects() == effects


def test_a_repeated_stop_request_is_the_same_stop(rig):
    open_long(rig)
    request_ = rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01)
    first = rig.adapter.place_order(request_)
    sent, effects = rig.mutations_sent(), rig.effects()
    again = rig.adapter.place_order(request_)
    assert (again.venue_order_id, again.status, again.stop_price) == (first.venue_order_id, c.NEW, 49_000.0)
    assert rig.effects() == effects and len(resting_stops(rig)) == 1
    if rig.real:
        assert rig.mutations_sent() == sent


def test_a_second_stop_is_refused_and_the_first_keeps_resting(rig):
    open_long(rig)
    first = rig.adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01))
    effects = rig.effects()
    error = refused(lambda: rig.adapter.place_order(rig.stop_request(BTC, c.SELL, 48_500.0, "stop-2", 0.01)))
    assert error.code == c.STOP_ALREADY_EXISTS and not error.uncertain
    (still,) = resting_stops(rig)
    assert (still.venue_order_id, still.stop_price) == (first.venue_order_id, 49_000.0)
    assert rig.effects() == effects and not rig.fake.protection_gaps(BTC)[-1].open


def test_a_stop_that_would_fire_at_once_is_refused(rig):
    open_long(rig)
    effects = rig.effects()
    error = refused(lambda: rig.adapter.place_order(rig.stop_request(BTC, c.SELL, 50_500.0, "stop-1", 0.01)))
    assert error.code == rig.expected(c.STOP_WOULD_TRIGGER_IMMEDIATELY) and not error.uncertain
    assert resting_stops(rig) == [] and rig.effects() == effects
    assert refused(lambda: rig.adapter.order(BTC, client_order_id="stop-1")).code == c.ORDER_NOT_FOUND


def test_the_stop_form_a_venue_does_not_have_is_unsupported_before_anything_is_sent(rig):
    open_long(rig)
    sent, effects = rig.mutations_sent(), rig.effects()
    error = refused(lambda: rig.adapter.place_order(rig.unsupported_stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01)))
    assert error.code == c.UNSUPPORTED and not error.uncertain
    assert rig.effects() == effects
    if rig.real:
        assert rig.mutations_sent() == sent


def test_a_stop_without_a_position(rig):
    request_ = rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01)
    if rig.venue == c.BINANCE:      # bound to whatever the position will be: the venue takes it while flat
        assert rig.adapter.place_order(request_).status == c.NEW
        return
    error = refused(lambda: rig.adapter.place_order(request_))
    assert error.code == rig.expected(c.REDUCE_ONLY_REJECTED) and not error.uncertain
    assert rig.effects() == 0


def test_amend_stop_moves_the_trigger_in_place_or_says_it_cannot(rig):
    adapter = rig.adapter
    open_long(rig)
    stop = adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01))
    gaps, sent, effects = rig.fake.protection_gaps(BTC), rig.mutations_sent(), rig.effects()
    if adapter.capabilities().stop_amend_in_place:
        amended = adapter.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_500.0)
        assert (amended.venue_order_id, amended.stop_price, amended.status) == (stop.venue_order_id, 49_500.0, c.NEW)
        assert adapter.order(BTC, venue_order_id=stop.venue_order_id).stop_price == 49_500.0
        assert rig.fake.protection_gaps(BTC) == gaps                 # never uncovered while the trigger moved
        assert len(resting_stops(rig)) == 1
        adapter.cancel_order(BTC, venue_order_id=stop.venue_order_id)
        missing = refused(lambda: adapter.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_600.0))
        assert missing.code == c.ORDER_NOT_FOUND and resting_stops(rig) == []    # an amend never creates a stop
        return
    error = refused(lambda: adapter.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_500.0))
    assert error.code == c.UNSUPPORTED and rig.effects() == effects
    assert resting_stops(rig)[0].stop_price == 49_000.0
    if rig.real:
        assert rig.mutations_sent() == sent


def test_a_stop_fires_and_closes_the_position(rig):
    adapter = rig.adapter
    open_long(rig)
    stop = adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01))
    rig.fake.set_price(BTC, 48_900.0)
    assert adapter.positions(BTC) == () and adapter.open_orders(BTC) == ()
    fills = adapter.fills(BTC, since_ms=since(rig))
    assert [(f.side, f.quantity, f.price) for f in fills] == [(c.BUY, 0.01, 50_000.0), (c.SELL, 0.01, 48_900.0)]
    assert adapter.balance().wallet_balance == pytest.approx(10_000 - 0.25 - 11.0 - 0.01 * 48_900.0 * 0.0005)
    if rig.name in (c.BINANCE, c.BYBIT):
        # the raw clients expose only stops that rest: a fired one cannot be told from one that never existed
        assert refused(lambda: adapter.order(BTC, venue_order_id=stop.venue_order_id)).code == c.ORDER_NOT_FOUND
    else:
        assert adapter.order(BTC, venue_order_id=stop.venue_order_id).status == c.FILLED


def test_a_stop_is_cancelled_by_either_identity(rig):
    adapter = rig.adapter
    open_long(rig)
    stop = adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01))
    cancelled = adapter.cancel_order(BTC, venue_order_id=stop.venue_order_id)
    assert cancelled.status == c.CANCELED and cancelled.venue_order_id == stop.venue_order_id
    assert resting_stops(rig) == [] and rig.fake.protection_gaps(BTC)[-1].open
    adapter.place_order(rig.stop_request(BTC, c.SELL, 49_100.0, "stop-2", 0.01))
    assert adapter.cancel_order(BTC, client_order_id="stop-2").status == c.CANCELED
    assert resting_stops(rig) == [] and adapter.positions(BTC)[0].quantity == 0.01


def test_a_take_profit_is_carried_like_a_stop(rig):
    open_long(rig)
    target = rig.adapter.place_order(rig.stop_request(BTC, c.SELL, 52_000.0, "target-1", 0.01, c.TAKE_PROFIT_MARKET))
    assert (target.status, target.order_type, target.stop_price) == (c.NEW, c.TAKE_PROFIT_MARKET, 52_000.0)
    assert rig.adapter.order(BTC, venue_order_id=target.venue_order_id).order_type == c.TAKE_PROFIT_MARKET
    assert rig.fake.protection_gaps(BTC)[-1].open                    # a take-profit is not protection
    rig.fake.set_price(BTC, 52_100.0)
    assert rig.adapter.positions(BTC) == ()


# =============================================================================================== failures
@pytest.mark.parametrize("kind,code", [("rate_limited", c.RATE_LIMITED), ("banned", c.BANNED)])
def test_a_read_the_venue_refuses_for_its_budget_is_asked_once(rig, kind, code):
    rig.fail(kind, method="balance", mutation=False)
    error = refused(rig.adapter.balance)
    assert error.code == code and not error.uncertain and error.retryable is (code == c.RATE_LIMITED)
    if code == c.RATE_LIMITED:
        assert error.retry_after_s is None or 0 < error.retry_after_s <= RETRY_AFTER_S
    else:
        assert error.banned_until_ms is None or rig.fake.now_ms() < error.banned_until_ms <= rig.fake.now_ms() + BAN_S * 1000
    if rig.real:
        assert len(rig.wire.calls) == 1                              # no retry, no sleep-and-retry
    else:
        assert error.retry_after_s == RETRY_AFTER_S or error.banned_until_ms


@pytest.mark.parametrize("kind,code", [("rate_limited", c.RATE_LIMITED), ("banned", c.BANNED)])
def test_a_mutation_the_venue_refuses_for_its_budget_is_sent_once_and_creates_nothing(rig, kind, code, no_sleep):
    rig.fail(kind, method="place_order", mutation=True)
    error = refused(lambda: open_long(rig))
    assert error.code == code and not error.uncertain
    assert rig.effects() == 0 and no_sleep == []
    if rig.real:
        assert len(rig.wire.mutations()) == 1
    if code == c.RATE_LIMITED:      # the caller, after the wait, repeats the SAME request
        assert open_long(rig).status == c.FILLED and rig.adapter.positions(BTC)[0].quantity == 0.01


@pytest.mark.parametrize("kind", ["timeout_after", "connection_lost", "garbage"])
def test_a_lost_answer_is_uncertain_and_the_order_is_found_not_sent_again(rig, kind):
    adapter = rig.adapter
    rig.fail(kind, method="place_order", mutation=True)
    error = refused(lambda: open_long(rig))
    assert error.code == c.NETWORK_UNCERTAIN and error.uncertain and not error.retryable
    found = adapter.order(BTC, client_order_id="entry-1")            # the caller's first and only permitted move
    assert (found.status, found.filled_quantity) == (c.FILLED, 0.01)
    sent, effects = rig.mutations_sent(), rig.effects()
    again = open_long(rig)                                           # even a blind repeat cannot double the order
    assert again.venue_order_id == found.venue_order_id and rig.effects() == effects
    assert adapter.positions(BTC)[0].quantity == 0.01 and len(adapter.fills(BTC, since_ms=since(rig))) == 1
    if rig.real:
        assert rig.mutations_sent() == sent


def test_a_lost_answer_to_a_stop_never_leaves_two_stops(rig):
    adapter = rig.adapter
    open_long(rig)
    request_ = rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01)
    rig.fail("timeout_after", method="place_order", mutation=True)
    assert refused(lambda: adapter.place_order(request_)).uncertain
    effects = rig.effects()
    again = adapter.place_order(request_)
    assert (again.status, again.stop_price) == (c.NEW, 49_000.0) and rig.effects() == effects
    assert [s.venue_order_id for s in resting_stops(rig)] == [again.venue_order_id]
    assert adapter.order(BTC, client_order_id="stop-1").venue_order_id == again.venue_order_id


def test_a_lost_answer_to_a_cancel_is_uncertain_and_the_order_reads_cancelled(rig):
    adapter = rig.adapter
    adapter.place_order(limit(ETH, c.BUY, 0.1, 2_400.0, "limit-1"))
    rig.fail("timeout_after", method="cancel_order", mutation=True)
    assert refused(lambda: adapter.cancel_order(ETH, client_order_id="limit-1")).uncertain
    assert adapter.order(ETH, client_order_id="limit-1").status == c.CANCELED
    assert adapter.cancel_order(ETH, client_order_id="limit-1").status == c.CANCELED


def test_a_request_that_never_left_is_unavailable_not_uncertain(rig):
    rig.fail("timeout_before", method="place_order", mutation=True)
    error = refused(lambda: open_long(rig))
    assert error.code == c.VENUE_UNAVAILABLE and not error.uncertain and error.retryable
    assert rig.effects() == 0
    assert refused(lambda: rig.adapter.order(BTC, client_order_id="entry-1")).code == c.ORDER_NOT_FOUND


def test_a_server_error_is_uncertain_for_a_mutation_and_unavailable_for_a_read(rig):
    rig.fail("server_error", method="place_order", mutation=True)
    error = refused(lambda: open_long(rig))
    assert error.code == c.NETWORK_UNCERTAIN and error.uncertain       # a 5xx does not prove the venue did nothing
    assert refused(lambda: rig.adapter.order(BTC, client_order_id="entry-1")).code == c.ORDER_NOT_FOUND
    rig.fail("server_error", method="balance", mutation=False)
    error = refused(rig.adapter.balance)
    assert error.code == c.VENUE_UNAVAILABLE and not error.uncertain and error.retryable
    rig.fail("garbage", method="balance", mutation=False)
    assert refused(rig.adapter.balance).code == c.MALFORMED_RESPONSE
    assert rig.effects() == 0


def test_no_credential_reaches_a_repr_an_error_or_a_returned_value(rig):
    adapter = rig.adapter
    order = open_long(rig)
    stop = adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01))
    seen = [repr(adapter), str(adapter), repr(adapter.capabilities()), repr(adapter.server_time()),
            repr(adapter.instrument_rules()), repr(adapter.balance()), repr(adapter.positions()), repr(order), repr(stop),
            repr(adapter.open_orders(BTC)), repr(adapter.fills(BTC, since_ms=since(rig))), repr(adapter.fee_rates(BTC)),
            repr(adapter.funding(BTC)), repr(adapter.rate_limit()), repr(vars(adapter).keys())]
    faults = (("timeout_before", "place_order", True, lambda: adapter.place_order(market(ETH, c.BUY, 0.1, "entry-2"))),
              ("timeout_after", "place_order", True, lambda: adapter.place_order(market(ETH, c.BUY, 0.1, "entry-3"))),
              ("connection_lost", "balance", False, adapter.balance), ("server_error", "balance", False, adapter.balance),
              ("garbage", "balance", False, adapter.balance), ("banned", "balance", False, adapter.balance))
    for kind, method, mutation, call in faults:
        rig.fail(kind, method=method, mutation=mutation)
        error = refused(call)
        assert error.__cause__ is None and error.__context__ is None  # nothing of the raw failure travels with it
        seen += [str(error), repr(error), error.message, repr(error.args), repr(vars(error))]
    try:
        seen.append(repr(adapter.user_stream()))
    except AdapterError as error:
        seen.append(str(error))
    for text in seen:
        assert SECRET_KEY not in text and SECRET not in text and "signature=" not in text, text[:120]


# ============================================================================ the values of the contract
def test_an_order_request_cannot_be_built_wrong():
    for bad in (dict(symbol="BTC-USDT", side=c.BUY, order_type=c.MARKET, client_order_id="a", quantity=1.0),
                dict(symbol=BTC, side="LONG", order_type=c.MARKET, client_order_id="a", quantity=1.0),
                dict(symbol=BTC, side=c.BUY, order_type="ICEBERG", client_order_id="a", quantity=1.0),
                dict(symbol=BTC, side=c.BUY, order_type=c.MARKET, client_order_id=" ", quantity=1.0),
                dict(symbol=BTC, side=c.BUY, order_type=c.MARKET, client_order_id="a"),
                dict(symbol=BTC, side=c.BUY, order_type=c.MARKET, client_order_id="a", quantity=0.0),
                dict(symbol=BTC, side=c.BUY, order_type=c.MARKET, client_order_id="a", quantity=float("nan")),
                dict(symbol=BTC, side=c.BUY, order_type=c.MARKET, client_order_id="a", quantity=1.0, price=10.0),
                dict(symbol=BTC, side=c.BUY, order_type=c.LIMIT, client_order_id="a", quantity=1.0),
                dict(symbol=BTC, side=c.BUY, order_type=c.MARKET, client_order_id="a", quantity=1.0, stop_price=9.0),
                dict(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="a", quantity=1.0, stop_price=9.0),
                dict(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="a", stop_price=9.0),
                dict(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="a", close_position=True),
                dict(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="a", quantity=1.0,
                     stop_price=9.0, close_position=True),
                dict(symbol=BTC, side=c.SELL, order_type=c.MARKET, client_order_id="a", close_position=True),
                dict(symbol=BTC, side=c.SELL, order_type=c.MARKET, client_order_id="a", quantity=1.0, working_type="LAST")):
        with pytest.raises(c.ContractViolation):
            OrderRequest(**bad)
    stop = OrderRequest(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="a", stop_price=9.0,
                        close_position=True)
    assert stop.protective and not market(BTC, c.BUY, 1.0, "b").protective


def test_values_refuse_states_that_cannot_exist():
    position = dict(venue=c.BINANCE, symbol=BTC, entry_price=None, mark_price=None, unrealized_pnl=None, leverage=None,
                    margin_mode=None, position_mode=None, observed_at_ms=1)
    for bad in (dict(side=c.FLAT, quantity=1.0), dict(side=c.LONG, quantity=0.0), dict(side="BUY", quantity=1.0),
                dict(side=c.LONG, quantity=-1.0)):
        with pytest.raises(c.ContractViolation):
            c.PositionInfo(**position, **bad)
    assert c.PositionInfo(**position, side=c.SHORT, quantity=2.0).signed_quantity == -2.0
    order = dict(venue=c.BINANCE, symbol=BTC, venue_status="", side=c.BUY, order_type=c.MARKET, quantity=1.0,
                 filled_quantity=0.0, average_price=None, stop_price=None, reduce_only=False, close_position=False,
                 updated_at_ms=0)
    with pytest.raises(c.ContractViolation):
        c.OrderInfo(**order, client_order_id=None, venue_order_id=None, status=c.NEW)
    with pytest.raises(c.ContractViolation):
        c.OrderInfo(**order, client_order_id="a", venue_order_id=None, status="PENDING")
    unknown = c.OrderInfo(**order, client_order_id="a", venue_order_id=None, status=c.UNKNOWN)
    assert not unknown.open and not unknown.final                    # never treated as open, never as closed
    fill = dict(venue=c.BINANCE, symbol=BTC, venue_order_id="1", client_order_id=None, side=c.BUY, quantity=1.0,
                price=1.0, fee=None, fee_asset=None, realized_pnl=None, is_maker=None, time_ms=1)
    with pytest.raises(c.ContractViolation):
        c.FillInfo(**fill, trade_id="")
    assert c.FillInfo(**fill, trade_id="t").fee is None              # an unreported fee stays unknown
    with pytest.raises(c.ContractViolation):
        c.AdapterCapabilities(venue="kraken", environment=c.DEMO, native_stop_market=True, native_take_profit=True,
                              reduce_only=True, close_position_stop=True, stop_amend_in_place=False,
                              multiple_stops_per_position=False, one_way_mode=True, hedge_mode=False, user_stream=False,
                              demo_environment=True, client_order_id_max_length=36)


def test_adapter_errors_keep_their_own_rules():
    assert c.STOP_ALREADY_EXISTS in c.ERROR_CODES and len(set(c.ERROR_CODES)) == len(c.ERROR_CODES)
    with pytest.raises(ValueError):
        AdapterError("NOT_A_CODE")
    with pytest.raises(ValueError):
        AdapterError(c.RATE_LIMITED, uncertain=True)                 # only NETWORK_UNCERTAIN may be uncertain
    uncertain = AdapterError(c.NETWORK_UNCERTAIN, "lost", venue=c.BYBIT)
    assert uncertain.uncertain and not uncertain.retryable and str(uncertain) == "bybit:NETWORK_UNCERTAIN: lost"
    assert [code for code in c.ERROR_CODES if AdapterError(code).retryable] == [
        c.RATE_LIMITED, c.TIMESTAMP_OUT_OF_SYNC, c.VENUE_UNAVAILABLE]


def test_symbols_and_client_order_ids_are_derived_deterministically():
    assert [c.canonical_symbol(s) for s in ("BTC-USDT", "btc_usdt", "BTC/USDT", "BTCUSDT")] == [BTC] * 4
    for bad in ("", None, "BTC USDT", "-"):
        with pytest.raises(c.ContractViolation):
            c.canonical_symbol(bad)
    derive = c.venue_client_order_id
    assert derive("cf-entry_01", max_length=36) == "cf-entry_01"     # fits: travels unchanged
    assert derive("a" * 36, max_length=36) == "a" * 36 and derive("a" * 37, max_length=36) != "a" * 37
    hashed = derive("Entry#1 with spaces", max_length=36)
    assert hashed == derive("Entry#1 with spaces", max_length=40, lowercase=True)    # one venue id on every venue
    assert len(hashed) == 32 and hashed.startswith("cf") and hashed == hashed.lower()
    assert set(hashed) <= set(c.CLIENT_ORDER_ID_ALPHABET) and len(hashed) <= c.PORTABLE_CLIENT_ORDER_ID_LENGTH
    assert derive("ABC", max_length=40) == "ABC" and derive("ABC", max_length=40, lowercase=True) != "abc"
    assert derive("ABC", max_length=40, lowercase=True) != derive("abc", max_length=40, lowercase=True)  # never folded
    with pytest.raises(c.ContractViolation):
        derive("  ", max_length=36)
    assert c.round_down_to_step(0.0129, 0.001) == 0.012 and c.round_down_to_step(5.0, 0.01) == 5.0
