"""``FakeVenue``: the rules other tests rely on when they run execution against it.

The contract suite already runs every protocol method against the fake's three profiles. This file pins down
what is particular to the fake: its journals (the proof of a shadow run), failure and rate-limit injection,
the exact matching, margin and reduce-only rules, the differences between the profiles, and
``protection_gaps``, which is what a stop-replacement test measures.
"""
from __future__ import annotations

import pytest

from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.contract import AdapterError, OrderRequest
from app.exchange.platform_adapter.fake import FAKE_ADAPTER_VERSION, FakeVenue, JournalEntry, ProtectionGap

BTC, ETH = "BTCUSDT", "ETHUSDT"
T0 = 1_700_000_000_000


def venue(profile: str = c.BINANCE, **kw) -> FakeVenue:
    fake = FakeVenue(profile, default_leverage=kw.pop("default_leverage", 5), **kw)
    fake.add_instrument(BTC, price=50_000.0, price_tick=0.1, quantity_step=0.001, min_notional=5.0, max_quantity=10.0)
    fake.add_instrument(ETH, price=2_500.0, price_tick=0.01, quantity_step=0.01, min_notional=5.0)
    return fake


def market(symbol, side, quantity, cid, **kw) -> OrderRequest:
    return OrderRequest(symbol=symbol, side=side, order_type=c.MARKET, client_order_id=cid, quantity=quantity, **kw)


def limit(symbol, side, quantity, price, cid, **kw) -> OrderRequest:
    return OrderRequest(symbol=symbol, side=side, order_type=c.LIMIT, client_order_id=cid, quantity=quantity, price=price, **kw)


def stop_for(fake: FakeVenue, symbol, side, trigger, cid, quantity=None, order_type=c.STOP_MARKET) -> OrderRequest:
    """A protective order in the form the profile has."""
    if fake.capabilities().close_position_stop:
        return OrderRequest(symbol=symbol, side=side, order_type=order_type, client_order_id=cid, stop_price=trigger,
                            close_position=True)
    return OrderRequest(symbol=symbol, side=side, order_type=order_type, client_order_id=cid, stop_price=trigger,
                        quantity=quantity, reduce_only=True)


def refused(call) -> AdapterError:
    with pytest.raises(AdapterError) as caught:
        call()
    return caught.value


def signed(fake: FakeVenue, symbol: str) -> float:
    held = fake.positions(symbol)
    return held[0].signed_quantity if held else 0.0


# ================================================================================================= identity
def test_the_fake_cannot_be_mistaken_for_a_venue():
    fake = venue()
    assert isinstance(fake, c.VenueAdapter) and fake.adapter_version == FAKE_ADAPTER_VERSION
    assert fake.adapter_version.startswith("fake-") and "FAKE VENUE" in fake.capabilities().notes[0]
    assert repr(fake) == "FakeVenue(venue='binance', environment='DEMO', adapter_version='fake-1.0.0')"
    assert fake.user_stream().url.startswith("fake://")
    for bad in (dict(profile="kraken"), dict(profile=c.BINANCE, environment="PAPER")):
        with pytest.raises(ValueError):
            FakeVenue(**bad)
    with pytest.raises(ValueError):
        fake.set_price("DOGEUSDT", 1.0)                                # a harness mistake, not a venue answer


def test_the_clock_is_the_one_it_is_given():
    fake = venue()
    assert fake.now_ms() == T0 and fake.advance(250) == T0 + 250 and fake.server_time().local_ms == T0 + 250
    fake.server_time_offset_ms = -1_500
    assert fake.server_time().offset_ms == -1_500
    with pytest.raises(ValueError):
        fake.advance(-1)
    ticks = iter(range(10, 10_000, 10))
    injected = FakeVenue(c.BYBIT, clock=lambda: next(ticks))
    assert (injected.now_ms(), injected.now_ms()) == (10, 20)
    with pytest.raises(ValueError):
        injected.advance(5)                                            # an injected clock is moved by its owner


# ================================================================================================= journals
def test_a_shadow_run_leaves_the_mutation_journal_empty():
    fake = venue()
    fake.capabilities(), fake.rate_limit()                             # local observations: not venue calls
    fake.server_time(), fake.instrument_rules([BTC]), fake.balance(), fake.positions(), fake.open_orders(BTC)
    fake.fills(BTC), fake.fee_rates(BTC), fake.funding(BTC), fake.user_stream()
    assert refused(lambda: fake.order(BTC, client_order_id="none")).code == c.ORDER_NOT_FOUND
    fake.set_price(BTC, 50_100.0)                                      # the market moving is not a mutation either
    assert fake.mutations == []
    assert [entry.method for entry in fake.reads] == [
        "server_time", "instrument_rules", "balance", "positions", "open_orders", "fills", "fee_rates", "funding",
        "user_stream", "order"]
    assert [entry.seq for entry in fake.reads] == list(range(1, 11))
    assert fake.reads[1].arguments == {"symbols": (BTC,)} and fake.reads[-1].outcome == c.ORDER_NOT_FOUND
    assert all(isinstance(entry, JournalEntry) and not entry.effect for entry in fake.reads)


def test_the_mutation_journal_keeps_every_call_its_arguments_its_outcome_and_its_effect():
    fake = venue()
    request = market(BTC, c.BUY, 0.01, "entry-1")
    fake.place_order(request)
    fake.place_order(request)                                          # a repeat: answered, nothing changed
    refused(lambda: fake.place_order(market(BTC, c.BUY, 0.0015, "off-step")))
    fake.set_leverage(ETH, 3)
    fake.set_leverage(ETH, 3)
    fake.advance(40)
    fake.cancel_order(BTC, client_order_id="entry-1")                  # already final: no effect
    fake.external_trade(BTC, c.SELL, 0.01)                             # the owner at the venue: not a call
    assert [(e.seq, e.method, e.outcome, e.effect) for e in fake.mutations] == [
        (1, "place_order", "OK", True), (2, "place_order", "OK", False), (3, "place_order", c.INVALID_QUANTITY, False),
        (4, "set_leverage", "OK", True), (5, "set_leverage", "OK", False), (6, "cancel_order", "OK", False)]
    assert fake.mutations[0].arguments == {"request": request} and fake.mutations[0].at_ms == T0
    assert fake.mutations[3].arguments == {"symbol": ETH, "leverage": 3} and fake.mutations[5].at_ms == T0 + 40
    assert fake.mutations[5].arguments == {"symbol": BTC, "client_order_id": "entry-1", "venue_order_id": None}


# ======================================================================================== failure injection
def test_fail_next_raises_the_chosen_error_the_chosen_number_of_times():
    fake = venue()
    fake.fail_next("balance", AdapterError(c.VENUE_UNAVAILABLE, "down", venue_code="X1", http_status=503), times=2)
    for _ in range(2):
        error = refused(fake.balance)
        assert (error.code, error.venue, error.venue_code, error.http_status, error.message) == (
            c.VENUE_UNAVAILABLE, c.BINANCE, "X1", 503, "down")
    assert fake.balance().wallet_balance == 10_000.0
    fake.fail_next("place_order", c.INSUFFICIENT_MARGIN)               # a code alone is enough
    assert refused(lambda: fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))).code == c.INSUFFICIENT_MARGIN
    assert fake.positions() == () and fake.place_order(market(BTC, c.BUY, 0.01, "entry-1")).status == c.FILLED


def test_fail_next_can_be_limited_to_one_symbol():
    fake = venue()
    fake.fail_next("place_order", c.VENUE_UNAVAILABLE, symbol=ETH, times=2)
    assert fake.place_order(market(BTC, c.BUY, 0.01, "entry-btc")).status == c.FILLED
    assert refused(lambda: fake.place_order(market(ETH, c.BUY, 0.1, "entry-eth"))).code == c.VENUE_UNAVAILABLE
    assert fake.fills(BTC) and refused(lambda: fake.place_order(market(ETH, c.BUY, 0.1, "entry-eth"))).retryable
    assert fake.place_order(market(ETH, c.BUY, 0.1, "entry-eth")).status == c.FILLED


def test_after_effect_models_an_accepted_mutation_whose_answer_was_lost():
    fake = venue()
    fake.fail_next("place_order", c.NETWORK_UNCERTAIN, after_effect=True)
    error = refused(lambda: fake.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert error.uncertain and signed(fake, BTC) == 0.01               # the venue did it
    assert (fake.mutations[-1].outcome, fake.mutations[-1].effect) == (c.NETWORK_UNCERTAIN, True)
    assert fake.order(BTC, client_order_id="entry-1").status == c.FILLED
    fake.fail_next("place_order", c.NETWORK_UNCERTAIN)                 # without it: the request never arrived
    assert refused(lambda: fake.place_order(market(ETH, c.BUY, 0.1, "entry-2"))).uncertain
    assert signed(fake, ETH) == 0.0 and fake.mutations[-1].effect is False
    fake.fail_next("cancel_order", c.NETWORK_UNCERTAIN, after_effect=True)
    fake.place_order(limit(ETH, c.BUY, 0.1, 2_400.0, "limit-1"))
    assert refused(lambda: fake.cancel_order(ETH, client_order_id="limit-1")).uncertain
    assert fake.order(ETH, client_order_id="limit-1").status == c.CANCELED


def test_fail_next_refuses_what_makes_no_sense():
    fake = venue()
    for bad in (dict(method="capabilities", error=c.VENUE_UNAVAILABLE), dict(method="rate_limit", error=c.BANNED),
                dict(method="teleport", error=c.BANNED), dict(method="balance", error=c.NETWORK_UNCERTAIN, after_effect=True),
                dict(method="balance", error=c.BANNED, times=0), dict(method="balance", error="NOT_A_CODE")):
        with pytest.raises(ValueError):
            fake.fail_next(**bad)
    fake.fail_next("balance", c.BANNED, times=5)
    fake.clear_failures()
    assert fake.balance().equity == 10_000.0


def test_a_rate_limit_refuses_every_venue_call_until_the_clock_has_passed():
    fake = venue()
    fake.rate_limit_for(5)
    for call in (fake.balance, fake.server_time, lambda: fake.place_order(market(BTC, c.BUY, 0.01, "entry-1")),
                 lambda: fake.set_leverage(BTC, 3)):
        error = refused(call)
        assert (error.code, error.retry_after_s, error.http_status) == (c.RATE_LIMITED, 5.0, 429) and error.retryable
    assert fake.capabilities().venue == c.BINANCE and fake.rate_limit().retry_after_s == 5.0    # local: still answered
    fake.advance(3_000)
    assert refused(fake.balance).retry_after_s == 2.0 and fake.rate_limit().retry_after_s == 2.0
    fake.advance(2_000)
    assert fake.rate_limit().retry_after_s is None and fake.balance().equity == 10_000.0
    assert fake.positions() == () and [e.effect for e in fake.mutations] == [False, False]
    fake.rate_limit_for(60, banned=True)
    error = refused(fake.balance)
    assert (error.code, error.banned_until_ms, error.http_status) == (c.BANNED, T0 + 65_000, 418) and not error.retryable
    assert fake.rate_limit().banned_until_ms == T0 + 65_000
    fake.clear_failures()
    assert fake.balance().equity == 10_000.0


# ==================================================================================================== rules
@pytest.mark.parametrize("code,request_", [
    (c.INVALID_QUANTITY, market(BTC, c.BUY, 0.0015, "off-step")),
    (c.INVALID_QUANTITY, market(BTC, c.BUY, 10.001, "above-max")),
    (c.INVALID_PRICE, limit(BTC, c.BUY, 0.01, 49_000.05, "off-tick")),
    (c.MIN_NOTIONAL, limit(BTC, c.BUY, 0.001, 4_000.0, "small")),
    (c.INSUFFICIENT_MARGIN, market(BTC, c.BUY, 2.0, "too-large")),
    (c.INSTRUMENT_UNKNOWN, market("DOGEUSDT", c.BUY, 10.0, "unlisted")),
])
def test_instrument_rules_are_enforced(code, request_):
    fake = venue()
    error = refused(lambda: fake.place_order(request_))
    assert error.code == code and not error.uncertain
    assert fake.positions() == () and fake.open_orders() == () and fake.mutations[-1].effect is False


def test_rules_can_change_and_an_instrument_can_stop_trading_or_be_delisted():
    fake = venue()
    before = fake.instrument_rules()[BTC]
    fake.set_rules(BTC, min_quantity=0.01, quantity_step=0.01)
    after = fake.instrument_rules()[BTC]
    assert after.raw_hash != before.raw_hash and after.min_quantity == 0.01
    assert refused(lambda: fake.place_order(market(BTC, c.BUY, 0.005, "below-min"))).code == c.INVALID_QUANTITY
    fake.set_rules(BTC, min_notional=None)                             # a rule the venue stopped publishing
    assert fake.instrument_rules()[BTC].min_notional is None and not fake.instrument_rules()[BTC].complete
    fake.place_order(market(ETH, c.BUY, 0.1, "entry-1"))
    fake.set_rules(ETH, status=c.NOT_TRADING)
    assert refused(lambda: fake.place_order(market(ETH, c.BUY, 0.1, "entry-2"))).code == c.INSTRUMENT_NOT_TRADING
    fake.remove_instrument(ETH)
    assert set(fake.instrument_rules()) == {BTC} and dict(fake.instrument_rules([ETH])) == {}
    assert refused(lambda: fake.fee_rates(ETH)).code == c.INSTRUMENT_UNKNOWN
    assert signed(fake, ETH) == 0.1                                    # the position is still there to be reconciled
    assert fake.place_order(market(ETH, c.BUY, 0.1, "entry-1")).status == c.FILLED     # a repeat is still answered


# =============================================================================== positions, margin and money
def test_positions_on_several_symbols_share_one_margin_account():
    fake = venue()
    fake.place_order(market(BTC, c.BUY, 0.02, "long-btc"))
    fake.place_order(market(ETH, c.SELL, 0.4, "short-eth"))
    assert {p.symbol: p.signed_quantity for p in fake.positions()} == {BTC: 0.02, ETH: -0.4}
    fees = (0.02 * 50_000 + 0.4 * 2_500) * 0.0005
    balance = fake.balance()
    assert balance.wallet_balance == pytest.approx(10_000 - fees) and balance.unrealized_pnl == 0.0
    assert balance.available_balance == pytest.approx(balance.equity - (1_000 + 1_000) / 5)
    fake.set_price(BTC, 51_000.0)
    fake.set_price(ETH, 2_600.0)
    balance = fake.balance()
    assert balance.unrealized_pnl == pytest.approx(0.02 * 1_000 - 0.4 * 100)
    assert balance.equity == pytest.approx(balance.wallet_balance + balance.unrealized_pnl)
    assert fake.positions(ETH)[0].unrealized_pnl == pytest.approx(-40.0) and fake.positions(ETH)[0].mark_price == 2_600.0


def test_leverage_decides_how_much_margin_an_entry_needs():
    fake = venue(default_leverage=1)
    assert refused(lambda: fake.place_order(market(BTC, c.BUY, 0.3, "entry-1"))).code == c.INSUFFICIENT_MARGIN
    assert fake.set_leverage(BTC, 2) == 2
    assert fake.place_order(market(BTC, c.BUY, 0.3, "entry-1")).status == c.FILLED
    assert fake.positions(BTC)[0].leverage == 2.0
    assert refused(lambda: fake.set_leverage(BTC, 0)).code == c.LEVERAGE_NOT_ALLOWED
    assert refused(lambda: fake.set_leverage(BTC, 101)).code == c.LEVERAGE_NOT_ALLOWED
    assert refused(lambda: fake.set_leverage("DOGEUSDT", 2)).code == c.INSTRUMENT_UNKNOWN


def test_entries_average_closes_realise_and_an_oversized_order_flips_the_position():
    fake = venue()
    fake.place_order(market(BTC, c.BUY, 0.01, "buy-1"))
    fake.set_price(BTC, 52_000.0)
    fake.place_order(market(BTC, c.BUY, 0.01, "buy-2"))
    assert fake.positions(BTC)[0].entry_price == pytest.approx(51_000.0)
    fake.set_price(BTC, 53_000.0)
    closed = fake.place_order(market(BTC, c.SELL, 0.01, "sell-1", reduce_only=True))
    (fill,) = fake.fills(BTC, venue_order_id=closed.venue_order_id)
    assert fill.realized_pnl == pytest.approx(0.01 * 2_000) and fill.fee == pytest.approx(0.01 * 53_000 * 0.0005)
    assert fake.positions(BTC)[0].entry_price == pytest.approx(51_000.0)     # a partial close keeps the entry
    fake.place_order(market(BTC, c.SELL, 0.03, "sell-2"))             # not reduce-only: through flat and short
    position = fake.positions(BTC)[0]
    assert (position.side, position.quantity, position.entry_price) == (c.SHORT, 0.02, 53_000.0)
    paid = (0.01 * 50_000 + 0.01 * 52_000 + 0.01 * 53_000 + 0.03 * 53_000) * 0.0005
    assert fake.balance().wallet_balance == pytest.approx(10_000 + 0.02 * 2_000 - paid)


# ============================================================================================ order matching
def test_market_orders_can_be_made_to_wait_and_fill_in_parts():
    fake = venue(fill_market_orders=False)
    order = fake.place_order(market(BTC, c.BUY, 0.03, "entry-1"))
    assert (order.status, order.filled_quantity) == (c.NEW, 0.0) and fake.positions() == ()
    first = fake.fill("entry-1", 0.01)
    assert (first.quantity, first.price, first.is_maker) == (0.01, 50_000.0, False)
    assert fake.order(BTC, client_order_id="entry-1").status == c.PARTIALLY_FILLED
    fake.fill("entry-1", 0.01, price=50_100.0)
    done = fake.fill("entry-1")                                        # the rest, at the current price
    assert done.quantity == pytest.approx(0.01)
    final = fake.order(BTC, client_order_id="entry-1")
    assert (final.status, final.filled_quantity) == (c.FILLED, 0.03)
    assert final.average_price == pytest.approx((50_000 + 50_100 + 50_000) / 3)
    assert [f.trade_id for f in fake.fills(BTC)] == sorted({f.trade_id for f in fake.fills(BTC)}, key=lambda t: int(t[1:]))
    for bad in (lambda: fake.fill("entry-1"), lambda: fake.fill("unknown"), lambda: fake.fill("entry-1", 0.0)):
        with pytest.raises(ValueError):
            bad()


def test_limit_orders_rest_until_the_price_crosses_and_marketable_ones_fill_at_once():
    fake = venue()
    resting = fake.place_order(limit(BTC, c.BUY, 0.01, 49_500.0, "bid-1"))
    assert resting.status == c.NEW and fake.set_price(BTC, 49_600.0) == ()
    (fill,) = fake.set_price(BTC, 49_400.0)                            # through the bid: filled at ITS price
    assert (fill.price, fill.is_maker, fill.fee) == (49_500.0, True, pytest.approx(0.01 * 49_500 * 0.0002))
    assert fake.order(BTC, client_order_id="bid-1").status == c.FILLED
    crossing = fake.place_order(limit(BTC, c.SELL, 0.01, 49_000.0, "ask-1"))
    assert (crossing.status, crossing.average_price) == (c.FILLED, 49_400.0)     # marketable: taken at the market
    assert fake.fills(BTC)[-1].is_maker is False and fake.positions(BTC) == ()
    with pytest.raises(ValueError):
        fake.fill("ask-1")                                             # nothing is left of it


def test_a_resting_reduce_only_order_never_takes_more_than_is_open():
    fake = venue()
    fake.place_order(market(BTC, c.BUY, 0.03, "entry-1"))
    fake.place_order(limit(BTC, c.SELL, 0.03, 51_000.0, "exit-1", reduce_only=True))
    assert refused(lambda: fake.place_order(limit(BTC, c.SELL, 0.04, 51_500.0, "exit-2", reduce_only=True))).code == (
        c.REDUCE_ONLY_REJECTED)
    fake.place_order(market(BTC, c.SELL, 0.02, "trim-1", reduce_only=True))      # 0.01 is left
    (fill,) = fake.set_price(BTC, 51_100.0)
    assert fill.quantity == 0.01 and fake.positions(BTC) == ()         # it took what was open and did not go short
    exit_order = fake.order(BTC, client_order_id="exit-1")
    assert (exit_order.status, exit_order.filled_quantity, exit_order.quantity) == (c.CANCELED, 0.01, 0.03)
    fake.place_order(market(BTC, c.BUY, 0.01, "entry-2"))
    fake.place_order(limit(BTC, c.SELL, 0.01, 52_000.0, "exit-3", reduce_only=True))
    fake.external_trade(BTC, c.SELL, 0.01)                             # the owner closes it by hand
    assert fake.set_price(BTC, 52_100.0) == ()
    assert fake.order(BTC, client_order_id="exit-3").status == c.EXPIRED and fake.positions(BTC) == ()
    fake.place_order(market(BTC, c.BUY, 0.01, "entry-3"))
    fake.place_order(limit(BTC, c.SELL, 0.01, 53_000.0, "exit-4", reduce_only=True))
    fake.external_trade(BTC, c.SELL, 0.01)
    assert fake.fill("exit-4") is None and fake.order(BTC, client_order_id="exit-4").status == c.EXPIRED


@pytest.mark.parametrize("entry_side,kind,trigger,fires_at,holds_at,refused_at", [
    (c.BUY, c.STOP_MARKET, 49_000.0, 48_999.9, 49_000.1, 50_000.0),
    (c.SELL, c.STOP_MARKET, 51_000.0, 51_000.0, 50_999.9, 50_000.0),
    (c.BUY, c.TAKE_PROFIT_MARKET, 52_000.0, 52_000.0, 51_999.9, 49_999.9),
    (c.SELL, c.TAKE_PROFIT_MARKET, 48_000.0, 47_999.9, 48_000.1, 50_000.1),
])
def test_trigger_orders_fire_in_their_direction_and_are_refused_when_already_reached(
        entry_side, kind, trigger, fires_at, holds_at, refused_at):
    for profile in c.VENUES:
        fake = venue(profile)
        fake.place_order(market(BTC, entry_side, 0.01, "entry-1"))
        exit_side = c.SELL if entry_side == c.BUY else c.BUY
        error = refused(lambda: fake.place_order(stop_for(fake, BTC, exit_side, refused_at, "early", 0.01, kind)))
        assert error.code == c.STOP_WOULD_TRIGGER_IMMEDIATELY
        fake.place_order(stop_for(fake, BTC, exit_side, trigger, "exit-1", 0.01, kind))
        assert fake.set_price(BTC, holds_at) == () and signed(fake, BTC) != 0
        (fill,) = fake.set_price(BTC, fires_at)
        assert (fill.side, fill.quantity, fill.price, fill.is_maker) == (exit_side, 0.01, fires_at, False)
        assert fake.positions(BTC) == () and fake.order(BTC, client_order_id="exit-1").status == c.FILLED
        assert refused(lambda: fake.place_order(stop_for(fake, BTC, exit_side, trigger + 0.05, "tick", 0.01, kind))).code in (
            c.INVALID_PRICE, c.STOP_WOULD_TRIGGER_IMMEDIATELY)


# ================================================================================================= profiles
def test_one_stop_form_works_on_every_profile_and_the_others_follow_the_profile():
    def sized(quantity, cid):
        return OrderRequest(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id=cid, stop_price=49_000.0,
                            quantity=quantity, reduce_only=True)

    whole = OrderRequest(symbol=BTC, side=c.SELL, order_type=c.STOP_MARKET, client_order_id="whole", stop_price=49_000.0,
                         close_position=True)
    for profile, bound_to_position in ((c.BINANCE, True), (c.BYBIT, True), (c.BINGX, False)):
        fake = venue(profile)
        assert refused(lambda: fake.place_order(sized(0.02, "flat"))).code == c.REDUCE_ONLY_REJECTED
        fake.place_order(market(BTC, c.BUY, 0.02, "entry-1"))
        placed = fake.place_order(sized(0.02, "full"))                 # reduce-only, the whole position: everywhere
        assert placed.status == c.NEW and not fake.protection_gaps(BTC)[-1].open
        again = stop_for(fake, BTC, c.SELL, 48_500.0, "second", 0.02)
        assert refused(lambda: fake.place_order(again)).code == c.STOP_ALREADY_EXISTS       # one stop of a kind
        target = stop_for(fake, BTC, c.SELL, 52_000.0, "target", 0.02, c.TAKE_PROFIT_MARKET)
        assert fake.place_order(target).status == c.NEW                # a take-profit is another kind
        fake.cancel_order(BTC, client_order_id="full")
        if bound_to_position:           # it rests as the venue's whole-position stop, and says so
            assert (placed.close_position, placed.quantity, placed.reduce_only) == (True, None, False)
            assert refused(lambda: fake.place_order(sized(0.01, "part"))).code == c.UNSUPPORTED
            assert fake.place_order(whole).close_position
        else:
            assert (placed.close_position, placed.quantity, placed.reduce_only) == (False, 0.02, True)
            assert refused(lambda: fake.place_order(whole)).code == c.UNSUPPORTED
            part = fake.place_order(sized(0.01, "part"))               # this venue can protect part of a position...
            assert part.quantity == 0.01 and fake.protection_gaps(BTC)[-1].open       # ...which leaves the rest open


def test_the_binance_profile_takes_a_stop_while_flat_and_leaves_it_resting_after_the_position_closes():
    fake = venue(c.BINANCE)
    placed = fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1"))
    assert placed.status == c.NEW and fake.protection_gaps(BTC) == ()
    fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    assert fake.protection_gaps(BTC) == ()                             # covered from the first instant
    fake.place_order(market(BTC, c.SELL, 0.01, "close-1", reduce_only=True))
    assert [o.client_order_id for o in fake.open_orders(BTC)] == ["stop-1"]      # obsolete protection: cancel it
    assert fake.set_price(BTC, 48_900.0) == () and fake.order(BTC, client_order_id="stop-1").status == c.EXPIRED
    assert refused(lambda: fake.amend_stop(BTC, client_order_id="stop-1", new_stop_price=48_000.0)).code == c.UNSUPPORTED


def test_the_bybit_profile_ties_the_stop_to_the_position_and_amends_it_in_place():
    fake = venue(c.BYBIT)
    assert refused(lambda: fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1"))).code == c.REDUCE_ONLY_REJECTED
    fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    wrong_side = stop_for(fake, BTC, c.BUY, 51_000.0, "stop-0")
    assert refused(lambda: fake.place_order(wrong_side)).code == c.REDUCE_ONLY_REJECTED
    placed = fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1"))
    amended = fake.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_500.0)
    assert (amended.venue_order_id, amended.stop_price, amended.status) == (placed.venue_order_id, 49_500.0, c.NEW)
    for bad, code in ((49_500.05, c.INVALID_PRICE), (50_000.0, c.STOP_WOULD_TRIGGER_IMMEDIATELY)):
        assert refused(lambda: fake.amend_stop(BTC, client_order_id="stop-1", new_stop_price=bad)).code == code
    assert refused(lambda: fake.amend_stop(BTC, client_order_id="nobody", new_stop_price=49_000.0)).code == c.ORDER_NOT_FOUND
    fake.place_order(stop_for(fake, BTC, c.SELL, 52_000.0, "target-1", order_type=c.TAKE_PROFIT_MARKET))
    fake.place_order(market(BTC, c.SELL, 0.01, "close-1", reduce_only=True))
    assert fake.open_orders(BTC) == ()                                 # the position took its stops with it
    assert {fake.order(BTC, client_order_id=cid).status for cid in ("stop-1", "target-1")} == {c.CANCELED}
    assert refused(lambda: fake.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_000.0)).code == c.ORDER_NOT_FOUND


def test_the_bingx_profile_stop_reduces_at_most_its_own_quantity():
    fake = venue(c.BINGX)
    def place(quantity):
        return fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-0", quantity))

    assert refused(lambda: place(0.01)).code == c.REDUCE_ONLY_REJECTED         # flat: nothing to reduce
    fake.place_order(market(BTC, c.BUY, 0.03, "entry-1"))
    assert refused(lambda: place(0.04)).code == c.REDUCE_ONLY_REJECTED         # more than is open
    fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1", 0.03))
    fake.place_order(market(BTC, c.SELL, 0.02, "trim-1", reduce_only=True))
    (fill,) = fake.set_price(BTC, 48_900.0)
    fired = fake.order(BTC, client_order_id="stop-1")
    assert fill.quantity == 0.01 and (fired.status, fired.filled_quantity, fired.quantity) == (c.CANCELED, 0.01, 0.03)
    assert fake.positions(BTC) == ()
    assert refused(lambda: fake.amend_stop(BTC, client_order_id="stop-1", new_stop_price=48_000.0)).code == c.UNSUPPORTED


# ========================================================================================= protection gaps
def test_a_gap_runs_from_the_fill_to_the_stop_and_is_measured_on_the_clock():
    for profile in c.VENUES:
        fake = venue(profile)
        assert fake.protection_gaps(BTC) == ()
        fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
        (gap,) = fake.protection_gaps(BTC)
        assert gap.open and (gap.start_ms, gap.end_ms, gap.duration_ms) == (T0, None, None)
        fake.advance(120)
        fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1", 0.01))
        (gap,) = fake.protection_gaps(BTC)
        assert isinstance(gap, ProtectionGap) and not gap.open
        assert (gap.symbol, gap.start_ms, gap.end_ms, gap.duration_ms) == (BTC, T0, T0 + 120, 120)
        assert gap.start_event < gap.end_event and fake.protection_gaps(ETH) == ()
        fake.advance(1_000)
        fake.place_order(market(BTC, c.SELL, 0.01, "close-1", reduce_only=True))
        assert fake.protection_gaps(BTC) == (gap,)                     # flat is not a gap


def test_cancel_then_create_at_the_same_instant_is_still_a_gap():
    for profile in c.VENUES:
        fake = venue(profile)
        fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
        fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1", 0.01))
        fake.advance(500)
        fake.cancel_order(BTC, client_order_id="stop-1")
        fake.place_order(stop_for(fake, BTC, c.SELL, 49_500.0, "stop-2", 0.01))      # the clock did not move
        first, replaced = fake.protection_gaps(BTC)
        assert (replaced.start_ms, replaced.end_ms, replaced.duration_ms) == (T0 + 500, T0 + 500, 0)
        assert replaced.start_event < replaced.end_event               # zero time, but the position WAS uncovered


def test_an_amend_in_place_leaves_no_gap_and_a_refused_replacement_keeps_the_old_stop():
    fake = venue(c.BYBIT)
    fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1"))
    gaps = fake.protection_gaps(BTC)
    for trigger in (49_200.0, 49_400.0, 49_600.0):
        fake.advance(100)
        fake.amend_stop(BTC, client_order_id="stop-1", new_stop_price=trigger)
    assert refused(lambda: fake.place_order(stop_for(fake, BTC, c.SELL, 49_700.0, "stop-2"))).code == c.STOP_ALREADY_EXISTS
    fake.fail_next("amend_stop", c.NETWORK_UNCERTAIN, after_effect=True)
    assert refused(lambda: fake.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_800.0)).uncertain
    assert fake.order(BTC, client_order_id="stop-1").stop_price == 49_800.0     # it was amended; only the answer was lost
    assert fake.protection_gaps(BTC) == gaps


def test_only_a_stop_that_covers_the_whole_position_on_its_closing_side_counts():
    fake = venue(c.BINGX)
    fake.place_order(market(BTC, c.BUY, 0.02, "entry-1"))
    fake.place_order(stop_for(fake, BTC, c.SELL, 52_000.0, "target-1", 0.02, c.TAKE_PROFIT_MARKET))
    assert fake.protection_gaps(BTC)[-1].open                          # a take-profit protects nothing
    fake.advance(10)
    fake.place_order(stop_for(fake, BTC, c.SELL, 49_000.0, "stop-1", 0.02))
    assert not fake.protection_gaps(BTC)[-1].open
    fake.advance(10)
    fake.place_order(market(BTC, c.BUY, 0.01, "add-1"))               # the position outgrew its stop
    assert len(fake.protection_gaps(BTC)) == 2 and fake.protection_gaps(BTC)[-1].open
    fake.advance(10)
    fake.place_order(market(BTC, c.SELL, 0.01, "trim-1", reduce_only=True))
    assert [(g.start_ms - T0, g.duration_ms) for g in fake.protection_gaps(BTC)] == [(0, 10), (20, 10)]
    fake.place_order(market(ETH, c.SELL, 0.1, "short-eth"))
    assert fake.protection_gaps(ETH)[-1].open and len(fake.protection_gaps(BTC)) == 2      # each symbol on its own
    fake.advance(5)
    fake.place_order(stop_for(fake, ETH, c.BUY, 2_600.0, "stop-eth", 0.1))
    assert fake.protection_gaps(ETH)[-1].duration_ms == 5
    fake.set_price(BTC, 48_900.0)                                      # the stop fires: flat, and still two gaps
    assert fake.positions(BTC) == () and len(fake.protection_gaps(BTC)) == 2


# ============================================================================================ identity rules
def test_client_order_ids_are_idempotent_across_the_whole_account():
    fake = venue()
    first = fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    assert fake.place_order(market(BTC, c.BUY, 0.02, "entry-1")).venue_order_id == first.venue_order_id
    for other in (market(BTC, c.SELL, 0.01, "entry-1"), market(ETH, c.BUY, 0.1, "entry-1"),
                  limit(BTC, c.BUY, 0.01, 49_000.0, "entry-1")):
        assert refused(lambda: fake.place_order(other)).code == c.DUPLICATE_CLIENT_ORDER_ID
    assert signed(fake, BTC) == 0.01 and signed(fake, ETH) == 0.0 and len(fake.fills(BTC)) == 1
    assert refused(lambda: fake.order(ETH, client_order_id="entry-1")).code == c.ORDER_NOT_FOUND    # another symbol
    assert fake.order(BTC, venue_order_id=first.venue_order_id).client_order_id == "entry-1"
    with pytest.raises(c.ContractViolation):
        fake.cancel_order(BTC)


def test_a_trade_made_by_hand_at_the_venue_shows_in_the_position_and_the_fills_only():
    fake = venue()
    fake.place_order(market(BTC, c.BUY, 0.02, "entry-1"))
    journal = len(fake.mutations)
    fill = fake.external_trade(BTC, c.SELL, 0.005, price=50_500.0)
    assert (fill.client_order_id, fill.quantity, fill.price) == (None, 0.005, 50_500.0)
    assert fill.realized_pnl == pytest.approx(0.005 * 500) and signed(fake, BTC) == pytest.approx(0.015)
    assert len(fake.mutations) == journal and fake.fills(BTC)[-1] == fill
    assert fake.order(BTC, venue_order_id=fill.venue_order_id).client_order_id is None
