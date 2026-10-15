"""What is particular to each venue adapter: the requests it sends, the error codes it maps, and the factory.

The contract suite (``test_platform_adapter_contract.py``) shows the adapters agree on behaviour. This file
looks at the wire: which endpoint and which fields each adapter sends through its REAL raw client, how each
venue's failure surface becomes a contract error, and that the platform's own gates still stand in front of
every mutation. No test here touches a network.

The limits of these tests are the limits stated in ``_platform_adapter_wire.py``. In particular the tests of
error-code mapping feed a venue code to the adapter and check the contract code that comes out; they show the
table is applied, not that the venue sends that code for that condition. Two answers are recorded venue
responses and are marked VERIFIED where they are used; every other answer is not.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from _platform_adapter_wire import (BAN_S, BTC, ETH, REAL_RIGS, RETRY_AFTER_S, WIRES, BybitWire, Reply, build_rig,
                                    limit, market, new_fake, raw_client)
from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.binance import BinanceAdapter
from app.exchange.platform_adapter.bingx import BingXAdapter
from app.exchange.platform_adapter.bybit import BybitAdapter
from app.exchange.platform_adapter.contract import AdapterError, OrderRequest
from app.exchange.platform_adapter.factory import MAX_FILL_WINDOWS, adapter_for
from app.exchange.platform_adapter.fake import FakeVenue
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url

RECORDED = json.loads((Path(__file__).resolve().parents[1] / "trading_intelligence" / "fixtures"
                       / "binance_demo_recorded.json").read_text(encoding="utf-8"))
WEEK_MS = 7 * 86_400_000


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    import time

    naps = []
    monkeypatch.setattr(time, "sleep", naps.append)
    return naps


@pytest.fixture(params=REAL_RIGS)
def rig(request, monkeypatch):
    return build_rig(request.param, monkeypatch)


def refused(call) -> AdapterError:
    with pytest.raises(AdapterError) as caught:
        call()
    return caught.value


def stop(symbol=BTC, trigger=49_000.0, client_order_id="stop-1", **kw) -> OrderRequest:
    return OrderRequest(symbol=symbol, side=c.SELL, order_type=c.STOP_MARKET, client_order_id=client_order_id,
                        stop_price=trigger, **kw)


def restarted(rig):
    """A new adapter on the same account and venue: what a process restart leaves of the adapter's memory."""
    return adapter_for(rig.client, rig.venue, c.DEMO, clock=rig.fake.now_ms)


# ================================================================================================== factory
def test_the_factory_builds_each_venues_adapter_and_never_the_fake(monkeypatch):
    for venue, expected in ((c.BINANCE, BinanceAdapter), (c.BYBIT, BybitAdapter), (c.BINGX, BingXAdapter)):
        adapter = build_rig(venue, monkeypatch).adapter
        assert type(adapter) is expected and not isinstance(adapter, FakeVenue)
        assert not adapter.adapter_version.startswith("fake")
    client = build_rig(c.BYBIT, monkeypatch).client
    assert type(adapter_for(client, " ByBit ", "demo")) is BybitAdapter      # names are normalised, not guessed
    for venue in ("fake", "fake-binance", "kraken", "", None):
        with pytest.raises(c.ContractViolation):
            adapter_for(client, venue, c.DEMO)
    with pytest.raises(c.ContractViolation):                                  # a fake venue is not a raw client
        adapter_for(FakeVenue(c.BINANCE), c.BINANCE, c.DEMO)


@pytest.mark.parametrize("venue", REAL_RIGS)
def test_an_adapter_is_bound_to_the_environment_of_its_account(venue, monkeypatch):
    demo = raw_client(venue, WIRES[venue](new_fake(venue)), monkeypatch, c.DEMO)
    live = raw_client(venue, WIRES[venue](new_fake(venue, c.LIVE)), monkeypatch, c.LIVE)
    assert demo.base_url == resolve_base_url(venue, BrokerEnvironment.DEMO) != live.base_url
    assert adapter_for(demo, venue, c.DEMO).capabilities().environment == c.DEMO
    assert adapter_for(live, venue, c.LIVE).environment == c.LIVE
    for client, wrong in ((demo, c.LIVE), (live, c.DEMO), (demo, "PAPER"), (demo, "")):
        with pytest.raises(c.ContractViolation):
            adapter_for(client, venue, wrong)


class _NoCredentials:
    """A raw client as the adapter sees it: any reach for a credential fails the test."""

    def __init__(self, client):
        object.__setattr__(self, "_client", client)

    def __getattr__(self, name):
        assert "key" not in name.lower() and "secret" not in name.lower(), f"the adapter read {name}"
        return getattr(self._client, name)


def test_an_adapter_never_reads_a_credential(rig):
    adapter = adapter_for(_NoCredentials(rig.client), rig.venue, c.DEMO, clock=rig.fake.now_ms)
    adapter.server_time(), adapter.instrument_rules(), adapter.balance(), adapter.fee_rates(BTC), adapter.funding(BTC)
    adapter.set_leverage(BTC, 5)
    adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    placed = adapter.place_order(rig.stop_request(BTC, c.SELL, 49_000.0, "stop-1", 0.01))
    adapter.positions(), adapter.open_orders(), adapter.fills(BTC, since_ms=rig.fake.now_ms() - 1), adapter.rate_limit()
    adapter.cancel_order(BTC, venue_order_id=placed.venue_order_id)
    assert rig.fake.mutations                                         # the flow really ran against the venue


def test_the_platform_gate_still_stands_in_front_of_every_mutation(rig, monkeypatch):
    """SHADOW ONLY: with the production profile and its order-submission flags at their defaults (off), an
    adapter mutation is refused by the raw client's own gate. Nothing reaches the venue and no flag is touched."""
    from app.core import config
    from app.core.config import Settings

    monkeypatch.setattr(config, "settings", Settings(
        _env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production",
        EXECUTION_MODE="live", DEMO_ORDER_SUBMISSION_ENABLED=False, LIVE_ORDER_SUBMISSION_ENABLED=False))
    assert Settings.model_fields["DEMO_ORDER_SUBMISSION_ENABLED"].default is False
    assert Settings.model_fields["LIVE_ORDER_SUBMISSION_ENABLED"].default is False
    assert config.settings.production and not config.settings.DEMO_ORDER_SUBMISSION_ENABLED
    assert not config.settings.LIVE_ORDER_SUBMISSION_ENABLED
    adapter = rig.adapter
    assert adapter.balance().wallet_balance == 10_000.0               # reads still work
    for mutation in (lambda: adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")),
                     lambda: adapter.set_leverage(BTC, 3)):
        error = refused(mutation)
        assert error.code == c.PERMISSION_DENIED and not error.uncertain and not error.retryable
    assert rig.wire.mutations() == [] and rig.effects() == 0


def test_a_key_the_venue_echoes_back_in_its_error_text_is_masked(rig):
    from _platform_adapter_wire import SECRET, SECRET_KEY

    text = f"Invalid API-key {SECRET_KEY} for this request; string to sign was a=1&signature={SECRET}"
    path, reply = {
        c.BINANCE: ("/fapi/v2/account", Reply(401, {"code": -2015, "msg": text})),
        c.BYBIT: ("/v5/account/wallet-balance", BybitWire.refuse(10003, text)),
        c.BINGX: ("/user/balance", Reply(200, {"code": 100413, "msg": text})),
    }[rig.venue]
    rig.wire.answer_next("GET", path, reply)
    error = refused(rig.adapter.balance)
    assert error.code == c.AUTH_FAILED and "Invalid API-key" in error.message      # the sentence survives
    for shown in (str(error), repr(error), error.message, repr(vars(error))):
        assert SECRET_KEY not in shown and SECRET not in shown


@pytest.mark.parametrize("client_order_id", ["Entry#1 / with spaces", "x" * 64, "ENTRY-UPPER-1"])
def test_an_id_the_venue_cannot_store_travels_as_its_hash_and_comes_back_as_the_callers(rig, client_order_id):
    adapter = rig.adapter
    venue_id = c.venue_client_order_id(client_order_id, max_length=adapter.capabilities().client_order_id_max_length,
                                       lowercase=rig.venue == c.BINGX)
    order = adapter.place_order(limit(ETH, c.BUY, 0.1, 2_400.0, client_order_id))
    create = rig.wire.mutations()[-1][2]
    assert venue_id in (create.get("newClientOrderId"), create.get("orderLinkId"), create.get("clientOrderId"))
    if client_order_id == "ENTRY-UPPER-1":
        assert (venue_id == client_order_id) is (rig.venue != c.BINGX)     # only BingX folds case
    else:
        assert venue_id != client_order_id and len(venue_id) == 32
    assert order.client_order_id == client_order_id
    assert adapter.order(ETH, client_order_id=client_order_id).venue_order_id == order.venue_order_id
    sent = rig.mutations_sent()
    assert adapter.place_order(limit(ETH, c.BUY, 0.1, 2_400.0, client_order_id)).venue_order_id == order.venue_order_id
    assert rig.mutations_sent() == sent
    assert adapter.cancel_order(ETH, client_order_id=client_order_id).status == c.CANCELED


def test_only_gtc_is_carried_and_old_fills_are_refused_rather_than_cut_short(rig):
    request = OrderRequest(symbol=ETH, side=c.BUY, order_type=c.LIMIT, client_order_id="ioc-1", quantity=0.1,
                           price=2_400.0, time_in_force="IOC")
    assert refused(lambda: rig.adapter.place_order(request)).code == c.UNSUPPORTED
    now = rig.fake.now_ms()
    assert refused(lambda: rig.adapter.fills(BTC, since_ms=now - (MAX_FILL_WINDOWS + 1) * WEEK_MS)).code == c.UNSUPPORTED
    assert rig.wire.calls == []
    assert rig.adapter.fills(BTC, since_ms=now - WEEK_MS - 5) == ()           # nine days: two weekly reads
    assert len([call for call in rig.wire.calls if call[0] == "GET"]) == 2


# ================================================================================================== Binance
@pytest.fixture
def binance(monkeypatch):
    return build_rig(c.BINANCE, monkeypatch)


#: code -> contract error. "repo": this repository already handles the code; "doc": the venue's error list only
BINANCE_CODES = [
    (-2019, c.INSUFFICIENT_MARGIN), (-2018, c.INSUFFICIENT_MARGIN), (-4164, c.MIN_NOTIONAL), (-4023, c.INVALID_QUANTITY),
    (-4003, c.INVALID_QUANTITY), (-4005, c.INVALID_QUANTITY), (-4014, c.INVALID_PRICE), (-1121, c.INSTRUMENT_UNKNOWN),
    (-4140, c.INSTRUMENT_NOT_TRADING), (-2022, c.REDUCE_ONLY_REJECTED), (-2021, c.STOP_WOULD_TRIGGER_IMMEDIATELY),
    (-2015, c.AUTH_FAILED), (-1003, c.RATE_LIMITED), (-1015, c.RATE_LIMITED),                               # repo
    (-1022, c.AUTH_FAILED), (-2014, c.AUTH_FAILED), (-4061, c.POSITION_MODE_MISMATCH), (-4028, c.LEVERAGE_NOT_ALLOWED),  # doc
    (-1111, c.UNMAPPED_VENUE_ERROR), (-4999, c.UNMAPPED_VENUE_ERROR),                                       # no evidence
]


@pytest.mark.parametrize("code,expected", BINANCE_CODES)
def test_binance_error_codes_on_a_mutation(binance, code, expected):
    binance.wire.answer_next("POST", "/fapi/v1/order", Reply(400, {"code": code, "msg": "venue text"}))
    error = refused(lambda: binance.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert (error.code, error.venue_code, error.http_status) == (expected, str(code), 400)
    assert not error.uncertain and binance.effects() == 0 and len(binance.wire.mutations()) == 1


def test_binance_table_is_exactly_what_these_tests_and_the_documentation_list():
    tested = {code for code, expected in BINANCE_CODES if expected != c.UNMAPPED_VENUE_ERROR}
    # the remaining entries are exercised by their own tests below and in the contract suite
    assert set(BinanceAdapter._CODES) - tested == {-1021, -2013, -2011, -4111, -4115, -4116, -4130}


@pytest.mark.parametrize("status,code", [(400, -1007), (400, -1000), (400, -1006), (500, -1001), (503, -1008), (408, -1007)])
def test_binance_an_answer_that_does_not_say_what_happened_is_uncertain_for_a_mutation(binance, status, code):
    binance.wire.answer_next("POST", "/fapi/v1/order", Reply(status, {"code": code, "msg": "Send status unknown."}))
    error = refused(lambda: binance.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert error.code == c.NETWORK_UNCERTAIN and error.uncertain and error.venue_code == str(code)
    binance.wire.answer_next("GET", "/fapi/v2/account", Reply(status, {"code": code, "msg": "Internal error."}))
    error = refused(binance.adapter.balance)
    assert error.code == c.VENUE_UNAVAILABLE and not error.uncertain


def test_binance_a_definite_code_under_a_server_status_proves_nothing(binance):
    """As ``fill_resolution.client_order_absent``: -2013 means "no such order" only in a 4xx answer."""
    binance.wire.answer_next("GET", "/fapi/v1/order", Reply(500, {"code": -2013, "msg": "Order does not exist."}))
    error = refused(lambda: binance.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert error.code == c.VENUE_UNAVAILABLE and binance.wire.mutations() == []   # the lookup failed: nothing is sent


def test_binance_a_duplicate_id_refusal_is_answered_by_looking_the_order_up(binance):
    adapter, wire = binance.adapter, binance.wire
    missing = Reply(400, {"code": -2013, "msg": "Order does not exist."})
    duplicate = Reply(400, {"code": -4116, "msg": "ClientOrderId is duplicated."})
    # the order exists at the venue (a first send whose answer was lost) but the lookup before sending missed it
    binance.fake.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    wire.answer_next("GET", "/fapi/v1/order", missing)
    found = adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    assert found.status == c.FILLED and len(wire.mutations()) == 1 and adapter.positions(BTC)[0].quantity == 0.01
    # the venue says duplicate and then cannot show the order: the refusal stands, nothing is assumed
    wire.answer_next("POST", "/fapi/v1/order", duplicate)
    error = refused(lambda: adapter.place_order(market(ETH, c.BUY, 0.1, "entry-2")))
    assert error.code == c.DUPLICATE_CLIENT_ORDER_ID and adapter.positions(ETH) == ()


def test_binance_a_timestamp_refusal_is_resent_once_by_the_raw_client_and_then_surfaces(binance):
    """Inherited behaviour, stated here so nobody is surprised: on -1021 the raw client re-reads the venue clock
    and sends the request once more (the venue did not process the first). The adapter adds no retry of its own."""
    skew = {"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."}
    binance.wire.answer_next("POST", "/fapi/v1/order", Reply(400, skew))
    binance.wire.answer_next("POST", "/fapi/v1/order", Reply(400, skew))
    error = refused(lambda: binance.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert error.code == c.TIMESTAMP_OUT_OF_SYNC and error.retryable and not error.uncertain
    assert len(binance.wire.sent("POST", "/fapi/v1/order")) == 2 and len(binance.wire.sent("GET", "/fapi/v1/time")) == 1
    assert binance.effects() == 0


def test_binance_a_rate_limited_read_reports_the_wait_and_nothing_more_is_sent_until_it_passes(binance, no_sleep):
    adapter, wire = binance.adapter, binance.wire
    wire.inject("rate_limited", mutation=False)
    error = refused(adapter.balance)
    assert error.code == c.RATE_LIMITED and RETRY_AFTER_S - 1 < error.retry_after_s <= RETRY_AFTER_S
    assert len(wire.calls) == 1 and no_sleep == []                    # the venue's Retry-After is not slept through
    for call in (adapter.positions, adapter.balance, lambda: adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))):
        again = refused(call)                                         # refused locally: asking would lengthen the ban
        assert again.code == c.RATE_LIMITED and again.retry_after_s > 0
    assert len(wire.calls) == 1 and binance.effects() == 0
    observed = adapter.rate_limit()
    assert 0 < observed.retry_after_s <= RETRY_AFTER_S and observed.banned_until_ms is None and observed.scope == "IP"


def test_binance_a_ban_says_until_when(binance):
    binance.wire.inject("banned", mutation=False)
    error = refused(binance.adapter.balance)
    now = binance.fake.now_ms()
    assert error.code == c.BANNED and not error.retryable and error.http_status == 418
    assert now + (BAN_S - 1) * 1000 < error.banned_until_ms <= now + BAN_S * 1000
    assert binance.adapter.rate_limit().banned_until_ms == error.banned_until_ms
    assert refused(binance.adapter.positions).code == c.BANNED and len(binance.wire.calls) == 1


def test_binance_the_used_weight_is_what_the_venue_last_said(binance):
    assert binance.adapter.rate_limit().used_weight is None           # nothing asked yet: nothing known
    binance.adapter.balance()
    observed = binance.adapter.rate_limit()
    assert (observed.used_weight, observed.window_seconds, observed.limit) == (1.0, 60, None)


def test_binance_entry_requests(binance):
    adapter, wire = binance.adapter, binance.wire
    adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    assert wire.sent("GET", "/fapi/v1/order")[0][2] == {"symbol": BTC, "origClientOrderId": "entry-1"}
    assert wire.mutations()[-1] == ("POST", "/fapi/v1/order", {
        "symbol": BTC, "side": "BUY", "type": "MARKET", "quantity": "0.01", "newOrderRespType": "RESULT",
        "newClientOrderId": "entry-1"})
    adapter.place_order(market(BTC, c.SELL, 0.01, "close-1", reduce_only=True))
    assert wire.mutations()[-1][2]["reduceOnly"] == "true"
    adapter.place_order(limit(ETH, c.BUY, 0.1, 2_400.0, "limit-1"))   # shape from the venue's documentation
    assert wire.mutations()[-1] == ("POST", "/fapi/v1/order", {
        "symbol": ETH, "side": "BUY", "type": "LIMIT", "quantity": "0.1", "price": "2400", "timeInForce": "GTC",
        "newOrderRespType": "RESULT", "newClientOrderId": "limit-1"})
    adapter.cancel_order(ETH, client_order_id="limit-1")
    assert wire.mutations()[-1][:2] == ("DELETE", "/fapi/v1/order") and set(wire.mutations()[-1][2]) == {"symbol", "orderId"}
    assert adapter.set_leverage(BTC, 7) == 7
    assert wire.mutations()[-1] == ("POST", "/fapi/v1/leverage", {"symbol": BTC, "leverage": "7"})


def test_binance_stops_are_conditional_orders_in_their_own_id_space(binance):
    adapter, wire = binance.adapter, binance.wire
    adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    placed = adapter.place_order(stop(close_position=True, working_type="CONTRACT_PRICE"))
    assert wire.mutations()[-1] == ("POST", "/fapi/v1/algoOrder", {
        "algoType": "CONDITIONAL", "symbol": BTC, "side": "SELL", "type": "STOP_MARKET", "triggerPrice": "49000",
        "closePosition": "true", "workingType": "CONTRACT_PRICE", "clientAlgoId": "stop-1"})
    assert placed.venue_order_id.startswith("algo:") and placed.close_position and placed.quantity is None
    again = refused(lambda: adapter.place_order(stop(trigger=48_500.0, client_order_id="stop-2", close_position=True)))
    assert (again.code, again.venue_code) == (c.STOP_ALREADY_EXISTS, "-4130")
    assert refused(lambda: adapter.fills(BTC, venue_order_id=placed.venue_order_id)).code == c.UNSUPPORTED
    adapter.cancel_order(BTC, client_order_id="stop-1")
    # a quantity is carried only when it is the whole open position, and then as the same closePosition order
    sent = len(wire.mutations())
    assert refused(lambda: adapter.place_order(stop(client_order_id="part", quantity=0.004, reduce_only=True))).code == (
        c.UNSUPPORTED)
    assert len(wire.mutations()) == sent
    # the id of the cancelled stop cannot be reused: the venue refuses it and no open order answers for it
    assert refused(lambda: adapter.place_order(stop(quantity=0.01, reduce_only=True))).code == c.DUPLICATE_CLIENT_ORDER_ID
    placed = adapter.place_order(stop(client_order_id="stop-3", quantity=0.01, reduce_only=True))
    assert wire.mutations()[-1][2]["closePosition"] == "true" and "quantity" not in wire.mutations()[-1][2]
    assert (placed.close_position, placed.quantity, placed.reduce_only) == (True, None, False)
    wire.calls.clear()
    assert [o.venue_order_id for o in adapter.open_orders()] == [placed.venue_order_id]
    assert wire.calls == [("GET", "/fapi/v1/openOrders", {}), ("GET", "/fapi/v1/openAlgoOrders", {})]   # every symbol
    adapter.cancel_order(BTC, client_order_id="stop-3")
    assert wire.mutations()[-1] == ("DELETE", "/fapi/v1/algoOrder", {"symbol": BTC, "algoId": placed.venue_order_id[5:]})
    assert refused(lambda: adapter.order(BTC, venue_order_id="not-a-number")).code == c.ORDER_NOT_FOUND
    hedged = [{"symbol": BTC, "positionAmt": "0.01", "positionSide": "LONG", "entryPrice": "50000"}]
    wire.answer_next("GET", "/fapi/v2/positionRisk", Reply(200, hedged))
    error = refused(lambda: adapter.place_order(stop(client_order_id="stop-4", quantity=0.01, reduce_only=True)))
    assert error.code == c.POSITION_MODE_MISMATCH                      # orders are sent for a one-way account


def test_binance_a_list_of_conditional_orders_that_is_not_a_list_is_never_read_as_no_stops(binance):
    for body in ({"unexpected": True}, {"algoOrders": None}, "nonsense"):
        binance.wire.answer_next("GET", "/fapi/v1/openAlgoOrders", Reply(200, body))
        assert refused(binance.adapter.open_orders).code == c.MALFORMED_RESPONSE
        binance.wire.answer_next("GET", "/fapi/v1/openAlgoOrders", Reply(200, body))
        assert refused(lambda: binance.adapter.open_orders(BTC)).code == c.MALFORMED_RESPONSE
    binance.wire.answer_next("GET", "/fapi/v1/openAlgoOrders", Reply(200, {"algoOrders": []}))
    assert binance.adapter.open_orders() == ()


def test_binance_recorded_instrument_and_funding_answers_are_read_as_recorded(binance):
    """VERIFIED shapes: these two answers were recorded from demo-fapi.binance.com (public endpoints)."""
    records = [RECORDED["symbols"][s]["exchange_info_symbol"] for s in (BTC, ETH)]
    binance.wire.answer_next("GET", "/fapi/v1/exchangeInfo", Reply(200, {"serverTime": 1, "symbols": records}))
    rules = binance.adapter.instrument_rules()[BTC]
    assert (rules.status, rules.venue_status, rules.base_asset, rules.quote_asset, rules.margin_asset) == (
        c.TRADING, "TRADING", "BTC", "USDT", "USDT")
    assert (rules.price_tick, rules.quantity_step, rules.min_quantity, rules.max_quantity) == (0.1, 0.0001, 0.0001, 1000.0)
    assert (rules.min_notional, rules.market_max_quantity, rules.max_leverage) == (50.0, 120.0, None)
    assert rules.complete and rules.raw_hash
    recorded = RECORDED["symbols"][BTC]["premium_index"]
    binance.wire.answer_next("GET", "/fapi/v1/premiumIndex", Reply(200, recorded))
    funding = binance.adapter.funding(BTC)
    assert (funding.rate, funding.next_funding_ms, funding.mark_price) == (0.0001, 1790208000000, 84370.74503623)
    assert funding.interval_minutes is None                           # published elsewhere; not invented here


def test_binance_an_order_state_the_adapter_cannot_map_is_unknown(binance):
    row = {"symbol": BTC, "orderId": 77, "clientOrderId": "entry-1", "side": "BUY", "type": "MARKET", "origQty": "0.01",
           "executedQty": "0", "avgPrice": "0", "stopPrice": "0", "reduceOnly": False, "closePosition": False}
    binance.wire.answer_next("GET", "/fapi/v1/order", Reply(200, {**row, "status": "PENDING_SOMETHING_NEW"}))
    order = binance.adapter.order(BTC, client_order_id="entry-1")
    assert (order.status, order.venue_status) == (c.UNKNOWN, "PENDING_SOMETHING_NEW") and not order.open and not order.final
    assert (order.average_price, order.stop_price, order.updated_at_ms) == (None, None, 0)    # 0 means "not set"
    binance.wire.answer_next("GET", "/fapi/v1/order", Reply(200, {**row, "status": "EXPIRED_IN_MATCH"}))
    assert binance.adapter.order(BTC, client_order_id="entry-1").status == c.EXPIRED
    binance.wire.answer_next("GET", "/fapi/v1/order", Reply(200, {**row, "side": ""}))
    assert refused(lambda: binance.adapter.order(BTC, client_order_id="entry-1")).code == c.MALFORMED_RESPONSE


def test_binance_missing_numbers_are_a_malformed_answer_and_a_full_fill_page_is_not_trusted(binance):
    binance.wire.answer_next("GET", "/fapi/v2/account", Reply(200, {"totalWalletBalance": "100"}))
    assert refused(binance.adapter.balance).code == c.MALFORMED_RESPONSE          # never a balance of zero
    trade = {"id": 1, "orderId": 2, "symbol": BTC, "side": "BUY", "qty": "0.01", "price": "50000", "time": 5}
    binance.wire.answer_next("GET", "/fapi/v1/userTrades", Reply(200, [{**trade, "id": i} for i in range(1000)]))
    assert refused(lambda: binance.adapter.fills(BTC)).code == c.UNSUPPORTED
    binance.wire.answer_next("GET", "/fapi/v1/userTrades", Reply(200, [trade]))
    (fill,) = binance.adapter.fills(BTC)
    assert (fill.fee, fill.fee_asset, fill.is_maker, fill.realized_pnl) == (None, None, None, None)   # not reported
    binance.wire.answer_next("POST", "/fapi/v1/leverage", Reply(200, {"symbol": BTC}))
    assert refused(lambda: binance.adapter.set_leverage(BTC, 3)).uncertain        # accepted, but not confirmed


# ==================================================================================================== Bybit
@pytest.fixture
def bybit(monkeypatch):
    return build_rig(c.BYBIT, monkeypatch)


#: all from the venue's published error list; none handled elsewhere in this repository
BYBIT_CODES = [
    (10002, c.TIMESTAMP_OUT_OF_SYNC), (10003, c.AUTH_FAILED), (10004, c.AUTH_FAILED), (10005, c.PERMISSION_DENIED),
    (10010, c.PERMISSION_DENIED), (10006, c.RATE_LIMITED), (10018, c.RATE_LIMITED), (10009, c.BANNED),
    (110004, c.INSUFFICIENT_MARGIN), (110007, c.INSUFFICIENT_MARGIN), (110012, c.INSUFFICIENT_MARGIN),
    (110017, c.REDUCE_ONLY_REJECTED), (110001, c.ORDER_NOT_FOUND),
    (10001, c.UNMAPPED_VENUE_ERROR), (110094, c.UNMAPPED_VENUE_ERROR), (110092, c.UNMAPPED_VENUE_ERROR),
]


@pytest.mark.parametrize("code,expected", BYBIT_CODES)
def test_bybit_error_codes_on_a_mutation(bybit, code, expected):
    bybit.wire.answer_next("POST", "/v5/order/create", BybitWire.refuse(code, "venue text"))
    error = refused(lambda: bybit.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert (error.code, error.venue_code) == (expected, str(code)) and not error.uncertain
    assert bybit.effects() == 0 and len(bybit.wire.mutations()) == 1


def test_bybit_table_is_exactly_what_these_tests_list():
    tested = {code for code, expected in BYBIT_CODES if expected != c.UNMAPPED_VENUE_ERROR}
    assert set(BybitAdapter._CODES) - tested == {110072}              # the duplicate id: tested below


@pytest.mark.parametrize("code", [10000, 10016])
def test_bybit_a_server_side_failure_is_uncertain_for_a_mutation_and_unavailable_for_a_read(bybit, code):
    bybit.wire.answer_next("POST", "/v5/order/create", BybitWire.refuse(code, "Server error."))
    assert refused(lambda: bybit.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))).uncertain
    bybit.wire.answer_next("GET", "/v5/account/wallet-balance", BybitWire.refuse(code, "Server error."))
    assert refused(bybit.adapter.balance).code == c.VENUE_UNAVAILABLE


@pytest.mark.parametrize("status,expected", [(401, c.AUTH_FAILED), (403, c.BANNED), (404, c.UNMAPPED_VENUE_ERROR),
                                              (429, c.RATE_LIMITED), (418, c.BANNED)])
def test_bybit_http_statuses(bybit, status, expected):
    bybit.wire.answer_next("GET", "/v5/account/wallet-balance", Reply(status, {"retCode": 10003, "retMsg": "refused"}))
    error = refused(bybit.adapter.balance)
    # the raw client keeps the status and the text of such an answer, not its retCode
    assert (error.code, error.http_status, error.venue_code) == (expected, status, None)
    assert error.retry_after_s is None and error.banned_until_ms is None      # the raw client discards the headers
    observed = bybit.adapter.rate_limit()
    assert (observed.scope, observed.used_weight, observed.retry_after_s, observed.banned_until_ms) == ("UID", None, None, None)


def test_bybit_a_duplicate_id_refusal_is_answered_by_looking_the_order_up(bybit):
    bybit.wire.answer_next("POST", "/v5/order/create", BybitWire.refuse(110072, "OrderLinkedID is duplicate"))
    error = refused(lambda: bybit.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert error.code == c.DUPLICATE_CLIENT_ORDER_ID and bybit.effects() == 0   # the venue cannot show it: no order


def test_bybit_a_refused_leverage_change_is_an_error_not_a_silent_success(bybit):
    """The raw ``set_leverage`` returns such an answer as if it were fine; the adapter reads ``retCode``."""
    refusal = BybitWire.refuse(10001, "leverage invalid")
    bybit.wire.answer_next("POST", "/v5/position/set-leverage", refusal)
    assert bybit.client.set_leverage(BTC, 3)["retCode"] == 10001       # the raw client does not raise
    bybit.wire.answer_next("POST", "/v5/position/set-leverage", refusal)
    assert refused(lambda: bybit.adapter.set_leverage(BTC, 3)).code == c.UNMAPPED_VENUE_ERROR
    bybit.wire.answer_next("POST", "/v5/position/set-leverage", BybitWire.refuse(110043, "leverage not modified"))
    assert bybit.adapter.set_leverage(BTC, 3) == 3                     # already there: an answer, not a failure
    assert bybit.wire.mutations()[-1][2] == {"category": "linear", "symbol": BTC, "buyLeverage": "3", "sellLeverage": "3"}


def test_bybit_an_entry_is_looked_up_sent_once_and_read_back(bybit, monkeypatch):
    adapter, wire = bybit.adapter, bybit.wire
    order = adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    assert [(m, p, {k: v for k, v in q.items() if k in ("orderLinkId", "orderId")}) for m, p, q in wire.calls] == [
        ("GET", "/v5/order/realtime", {"orderLinkId": "entry-1"}), ("GET", "/v5/order/history", {"orderLinkId": "entry-1"}),
        ("POST", "/v5/order/create", {"orderLinkId": "entry-1"}),
        ("GET", "/v5/order/realtime", {"orderId": order.venue_order_id}),
        ("GET", "/v5/order/history", {"orderId": order.venue_order_id})]
    assert wire.mutations()[0][2] == {"category": "linear", "symbol": BTC, "side": "Buy", "orderType": "Market",
                                      "qty": "0.01", "positionIdx": 0, "orderLinkId": "entry-1"}
    assert (order.status, order.venue_status) == (c.FILLED, "Filled")
    adapter.place_order(limit(ETH, c.SELL, 0.1, 2_600.0, "limit-1"))
    create = wire.mutations()[-1][2]
    assert (create["orderType"], create["price"], create["timeInForce"], create["side"]) == ("Limit", "2600", "GTC", "Sell")

    # when the read after the acknowledgement fails, the order is reported as acknowledged: never as NEW or FILLED
    find = adapter._find

    def read_back_fails(venue_symbol, **identity):
        if "orderId" in identity:
            raise AdapterError(c.VENUE_UNAVAILABLE, venue=c.BYBIT)
        return find(venue_symbol, **identity)

    monkeypatch.setattr(adapter, "_find", read_back_fails)
    acknowledged = adapter.place_order(market(BTC, c.BUY, 0.01, "entry-2"))
    assert (acknowledged.status, acknowledged.venue_status) == (c.UNKNOWN, "ACKNOWLEDGED")
    assert acknowledged.venue_order_id and not acknowledged.open and not acknowledged.final
    assert bybit.fake.order(BTC, client_order_id="entry-2").status == c.FILLED    # it did go through, once


def test_bybit_the_stop_is_the_positions_own(bybit):
    adapter, wire = bybit.adapter, bybit.wire
    adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    placed = adapter.place_order(stop(close_position=True))
    assert wire.mutations()[-1] == ("POST", "/v5/position/trading-stop", {
        "category": "linear", "symbol": BTC, "tpslMode": "Full", "positionIdx": 0, "stopLoss": "49000",
        "slTriggerBy": "MarkPrice"})
    assert (placed.venue_order_id, placed.close_position, placed.quantity) == ("POSITION_SL:BTCUSDT", True, None)
    assert refused(lambda: adapter.fills(BTC, venue_order_id=placed.venue_order_id)).code == c.UNSUPPORTED
    # the venue lists its own row for that stop among the open orders; the adapter reports the stop once
    assert len(bybit.client.open_orders(BTC)) == 1 and len(adapter.open_orders(BTC)) == 1
    adapter.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_500.0)
    assert wire.mutations()[-1][2] == {"category": "linear", "symbol": BTC, "tpslMode": "Full", "positionIdx": 0,
                                       "stopLoss": "49500"}             # the trigger only: the price basis is kept
    adapter.amend_stop(BTC, client_order_id="stop-1", new_stop_price=49_500.0)   # 34040, not modified: fine
    adapter.cancel_order(BTC, venue_order_id=placed.venue_order_id)
    assert wire.mutations()[-1][2]["stopLoss"] == "0" and adapter.open_orders(BTC) == ()
    adapter.place_order(stop(client_order_id="stop-2", close_position=True, working_type="CONTRACT_PRICE"))
    assert wire.mutations()[-1][2]["slTriggerBy"] == "LastPrice"
    target = OrderRequest(symbol=BTC, side=c.SELL, order_type=c.TAKE_PROFIT_MARKET, client_order_id="target-1",
                          stop_price=52_000.0, close_position=True)
    assert adapter.place_order(target).venue_order_id == "POSITION_TP:BTCUSDT"
    assert (wire.mutations()[-1][2]["takeProfit"], wire.mutations()[-1][2]["tpTriggerBy"]) == ("52000", "MarkPrice")
    assert adapter.amend_stop(BTC, client_order_id="target-1", new_stop_price=52_500.0).order_type == c.TAKE_PROFIT_MARKET
    assert wire.mutations()[-1][2]["takeProfit"] == "52500" and "stopLoss" not in wire.mutations()[-1][2]
    wrong_side = OrderRequest(symbol=BTC, side=c.BUY, order_type=c.STOP_MARKET, client_order_id="stop-3",
                              stop_price=51_000.0, close_position=True)
    assert refused(lambda: adapter.place_order(wrong_side)).code == c.REDUCE_ONLY_REJECTED
    sent = len(wire.mutations())
    assert refused(lambda: adapter.place_order(stop(client_order_id="part", quantity=0.004, reduce_only=True))).code == (
        c.UNSUPPORTED)                                                 # part of a position: this venue has no such stop
    assert len(wire.mutations()) == sent


def test_bybit_after_a_restart_the_stop_is_known_by_its_venue_id_and_never_overwritten(bybit):
    bybit.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    bybit.adapter.place_order(stop(close_position=True))
    fresh = restarted(bybit)
    assert fresh.order(BTC, venue_order_id="POSITION_SL:BTCUSDT").stop_price == 49_000.0
    # the venue keeps no client id for it, and the new adapter has no memory: by that id it is not found
    assert refused(lambda: fresh.order(BTC, client_order_id="stop-1")).code == c.ORDER_NOT_FOUND
    sent = bybit.mutations_sent()
    assert refused(lambda: fresh.place_order(stop(trigger=48_000.0, close_position=True))).code == c.STOP_ALREADY_EXISTS
    assert fresh.place_order(stop(close_position=True)).venue_order_id == "POSITION_SL:BTCUSDT"   # same trigger: same stop
    assert bybit.mutations_sent() == sent and fresh.order(BTC, client_order_id="stop-1").stop_price == 49_000.0


def test_bybit_a_hedge_mode_position_is_reported_and_not_driven(bybit):
    row = {"symbol": BTC, "side": "Buy", "size": "0.01", "avgPrice": "50000", "markPrice": "50000", "unrealisedPnl": "0",
           "leverage": "5", "tradeMode": 1, "positionIdx": 1, "stopLoss": "", "takeProfit": "", "updatedTime": "5"}
    answer = Reply(200, {"retCode": 0, "retMsg": "OK", "result": {"list": [row], "nextPageCursor": ""}})
    bybit.wire.answer_next("GET", "/v5/position/list", answer)
    (position,) = bybit.adapter.positions(BTC)
    assert (position.position_mode, position.margin_mode, position.side) == ("HEDGE", "ISOLATED", c.LONG)
    bybit.wire.answer_next("GET", "/v5/position/list", answer)
    assert refused(lambda: bybit.adapter.place_order(stop(close_position=True))).code == c.POSITION_MODE_MISMATCH
    assert bybit.wire.mutations() == []


def test_bybit_funding_settlements_are_not_fills_and_instruments_are_read_across_pages(bybit):
    bybit.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    since = bybit.fake.now_ms() - 1
    assert len(bybit.client.user_trades(BTC, start_time_ms=since)) == 2      # the raw client reports the funding row
    (fill,) = bybit.adapter.fills(BTC, since_ms=since)
    assert (fill.trade_id, fill.client_order_id, fill.is_maker, fill.realized_pnl) == ("T2", "entry-1", False, None)
    bybit.wire.calls.clear()
    assert set(bybit.adapter.instrument_rules()) == {BTC, ETH}
    assert [q.get("cursor") for _, _, q in bybit.wire.sent("GET", "/v5/market/instruments-info")] == [None, "page-2"]
    rules = bybit.adapter.instrument_rules()[BTC]
    assert (rules.max_leverage, rules.venue_status) == (100.0, "Trading")


def test_bybit_an_account_without_a_wallet_is_a_malformed_answer_not_a_zero_balance(bybit):
    def answer(rows):
        return Reply(200, {"retCode": 0, "retMsg": "OK", "result": {"list": rows}})

    wallet = {"totalEquity": "101", "totalWalletBalance": "100", "totalPerpUPL": "1", "totalAvailableBalance": "90"}
    bybit.wire.answer_next("GET", "/v5/account/wallet-balance", answer([]))        # nothing under UNIFIED...
    bybit.wire.answer_next("GET", "/v5/account/wallet-balance", answer([wallet]))  # ...an older account: CONTRACT
    assert bybit.adapter.balance().equity == 101.0
    assert [q["accountType"] for _, _, q in bybit.wire.calls] == ["UNIFIED", "CONTRACT"]
    bybit.wire._routes = lambda: {("GET", "/v5/account/wallet-balance"): lambda p: {"list": []}}
    assert bybit.client.account()["totalWalletBalance"] == 0.0         # the raw client reports zeros
    assert refused(bybit.adapter.balance).code == c.MALFORMED_RESPONSE
    bybit.wire.answer_next("GET", "/v5/account/wallet-balance", answer([{**wallet, "totalAvailableBalance": ""}]))
    assert refused(bybit.adapter.balance).code == c.MALFORMED_RESPONSE  # a total the venue left blank is not zero


# ==================================================================================================== BingX
@pytest.fixture
def bingx(monkeypatch):
    return build_rig(c.BINGX, monkeypatch)


#: all from the venue's published error list; none handled elsewhere in this repository
BINGX_CODES = [
    (100001, c.AUTH_FAILED), (100412, c.AUTH_FAILED), (100413, c.AUTH_FAILED), (100004, c.PERMISSION_DENIED),
    (100419, c.PERMISSION_DENIED), (100421, c.TIMESTAMP_OUT_OF_SYNC), (100410, c.RATE_LIMITED),
    (101204, c.INSUFFICIENT_MARGIN), (101415, c.INSTRUMENT_NOT_TRADING), (101414, c.LEVERAGE_NOT_ALLOWED),
    (80016, c.ORDER_NOT_FOUND), (80014, c.UNMAPPED_VENUE_ERROR), (101209, c.UNMAPPED_VENUE_ERROR),
]


@pytest.mark.parametrize("code,expected", BINGX_CODES)
def test_bingx_error_codes_on_a_mutation(bingx, code, expected):
    bingx.wire.answer_next("POST", "/trade/order", Reply(200, {"code": code, "msg": "venue text"}))
    error = refused(lambda: bingx.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1")))
    assert (error.code, error.venue_code) == (expected, str(code)) and not error.uncertain
    assert bingx.effects() == 0 and len(bingx.wire.mutations()) == 1


def test_bingx_table_is_exactly_what_these_tests_list():
    assert set(BingXAdapter._CODES) == {code for code, expected in BINGX_CODES if expected != c.UNMAPPED_VENUE_ERROR}


@pytest.mark.parametrize("code", [100500, 80012, 101500])
def test_bingx_a_server_side_failure_is_uncertain_for_a_mutation_and_unavailable_for_a_read(bingx, code):
    bingx.wire.answer_next("POST", "/trade/order", Reply(200, {"code": code, "msg": "Internal system error"}))
    assert refused(lambda: bingx.adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))).uncertain
    bingx.wire.answer_next("GET", "/user/balance", Reply(200, {"code": code, "msg": "Internal system error"}))
    assert refused(bingx.adapter.balance).code == c.VENUE_UNAVAILABLE


@pytest.mark.parametrize("status,expected", [(401, c.AUTH_FAILED), (429, c.RATE_LIMITED), (418, c.BANNED),
                                              (403, c.UNMAPPED_VENUE_ERROR)])
def test_bingx_http_statuses(bingx, status, expected):
    bingx.wire.answer_next("GET", "/user/balance", Reply(status, None, text="<html>refused</html>"))
    error = refused(bingx.adapter.balance)
    assert (error.code, error.http_status) == (expected, status) and error.retry_after_s is None


def test_bingx_a_network_failure_the_raw_client_hid_is_still_told_apart(bingx, monkeypatch):
    def hidden(method, path, payload=None):
        raise RuntimeError("BingX Network Error: something went wrong")     # no requests exception behind it

    monkeypatch.setattr(bingx.client, "_request", hidden)
    assert refused(bingx.adapter.balance).code == c.VENUE_UNAVAILABLE
    assert refused(lambda: bingx.adapter.set_leverage(BTC, 3)).uncertain    # a mutation: it may have left


def test_bingx_symbols_ids_and_entries_are_translated(bingx):
    adapter, wire = bingx.adapter, bingx.wire
    order = adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    assert order.symbol == BTC and (order.status, order.venue_status) == (c.FILLED, "FILLED")
    assert [(m, p.rsplit("/", 2)[-2:], q) for m, p, q in wire.calls] == [
        ("GET", ["trade", "order"], {"symbol": "BTC-USDT", "clientOrderId": "entry-1"}),
        ("POST", ["trade", "order"], {"symbol": "BTC-USDT", "side": "BUY", "positionSide": "BOTH", "type": "MARKET",
                                      "quantity": "0.01", "clientOrderId": "entry-1"}),
        ("GET", ["trade", "order"], {"symbol": "BTC-USDT", "orderId": order.venue_order_id})]   # acknowledged, read back
    adapter.place_order(limit(ETH, c.SELL, 0.1, 2_600.0, "limit-1"))
    assert (wire.mutations()[-1][2]["type"], wire.mutations()[-1][2]["price"]) == ("LIMIT", "2600")
    adapter.place_order(market(BTC, c.SELL, 0.01, "close-1", reduce_only=True))
    assert wire.mutations()[-1][2]["reduceOnly"] == "true"
    assert adapter.set_leverage(BTC, 7) == 7
    assert [q["side"] for _, p, q in wire.mutations() if p.endswith("/trade/leverage")] == ["LONG", "SHORT"]
    adapter.instrument_rules()
    assert adapter._to_venue("NCFXEUR2USDUSDT") == "NCFXEUR2USD-USDT"        # not listed: the raw client's own rule
    assert adapter._to_venue(BTC) == "BTC-USDT"


def test_bingx_a_stop_carries_its_quantity_and_no_client_id(bingx):
    adapter, wire = bingx.adapter, bingx.wire
    adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    placed = adapter.place_order(stop(quantity=0.01, reduce_only=True))
    assert wire.mutations()[-1][2] == {"symbol": "BTC-USDT", "side": "SELL", "positionSide": "BOTH", "type": "STOP_MARKET",
                                       "quantity": "0.01", "stopPrice": "49000", "reduceOnly": "true",
                                       "workingType": "MARK_PRICE"}
    assert (placed.quantity, placed.reduce_only, placed.close_position, placed.status) == (0.01, True, False, c.NEW)
    assert placed.venue_status == "ACCEPTED"                           # accepted as resting; the caller reads it back
    fresh = restarted(bingx)                                           # the binding of "stop-1" is gone
    assert refused(lambda: fresh.order(BTC, client_order_id="stop-1")).code == c.ORDER_NOT_FOUND
    assert fresh.order(BTC, venue_order_id=placed.venue_order_id).stop_price == 49_000.0
    sent = bingx.mutations_sent()
    assert fresh.place_order(stop(quantity=0.01, reduce_only=True)).venue_order_id == placed.venue_order_id
    assert bingx.mutations_sent() == sent                              # the same geometry is the same stop
    assert fresh.order(BTC, client_order_id="stop-1").venue_order_id == placed.venue_order_id
    assert refused(lambda: fresh.place_order(stop(trigger=48_000.0, client_order_id="stop-2", quantity=0.01,
                                                  reduce_only=True))).code == c.STOP_ALREADY_EXISTS
    assert bingx.mutations_sent() == sent
    assert fresh.cancel_order(BTC, client_order_id="stop-1").venue_status == "CANCELLED"


def test_bingx_what_the_venue_did_not_report_stays_unknown(bingx):
    adapter, wire = bingx.adapter, bingx.wire
    wallet = {"asset": "VST", "balance": "100", "equity": "101", "availableMargin": "90", "usedMargin": "11"}
    wire.answer_next("GET", "/user/balance", Reply(200, {"code": 0, "data": {"balance": wallet}}))
    assert refused(adapter.balance).code == c.MALFORMED_RESPONSE       # no unrealised profit under either spelling
    wire.answer_next("GET", "/user/balance", Reply(200, {"code": 0, "data": {"balance": {**wallet, "unrealizedProfit": "1"}}}))
    balance = adapter.balance()
    assert (balance.margin_asset, balance.unrealized_pnl, balance.equity) == ("VST", 1.0, 101.0)
    incomplete = {k: v for k, v in wallet.items() if k != "equity"}
    wire.answer_next("GET", "/user/balance", Reply(200, {"code": 0, "data": {"balance": incomplete}}))
    assert bingx.client.account()["totalMarginBalance"] == 0.0         # the raw client reports an equity of zero
    wire.answer_next("GET", "/user/balance", Reply(200, {"code": 0, "data": {"balance": {**incomplete, "unrealisedPNL": "0"}}}))
    assert refused(adapter.balance).code == c.MALFORMED_RESPONSE
    fill = {"tradeId": "f1", "orderId": 9, "symbol": "BTC-USDT", "side": "BUY", "qty": "0.01", "price": "50000", "time": 5}
    wire.answer_next("GET", "/trade/allFillOrders", Reply(200, {"code": 0, "data": [fill]}))
    (unpriced,) = adapter.fills(BTC)
    assert (unpriced.fee, unpriced.fee_asset, unpriced.is_maker, unpriced.client_order_id) == (None, None, None, None)
    wire.answer_next("GET", "/trade/allFillOrders", Reply(200, {"code": 0, "data": [{**fill, "tradeId": None}]}))
    assert refused(lambda: adapter.fills(BTC)).code == c.MALFORMED_RESPONSE   # a fill that cannot be deduplicated


def test_bingx_an_answer_without_the_order_list_is_never_read_as_no_orders(bingx):
    wire = bingx.wire
    wire.answer_next("GET", "/trade/openOrders", Reply(200, {"code": 0, "data": {"orders": None}}))
    assert bingx.adapter.open_orders() == ()                           # the list is there and empty
    for data in ({}, None, {"orders": "x"}):
        wire.answer_next("GET", "/trade/openOrders", Reply(200, {"code": 0, "data": data}))
        assert refused(lambda: bingx.adapter.open_orders(BTC)).code == c.MALFORMED_RESPONSE
    wire.answer_next("GET", "/user/positions", Reply(200, {"code": 0, "data": None}))
    assert bingx.adapter.positions() == ()
    wire.answer_next("GET", "/user/positions", Reply(200, {"code": 0}))
    assert refused(bingx.adapter.positions).code == c.MALFORMED_RESPONSE
    wire.answer_next("GET", "/trade/order", Reply(200, {"code": 0, "data": {"order": {}}}))
    assert refused(lambda: bingx.adapter.order(BTC, client_order_id="x")).code == c.ORDER_NOT_FOUND


def test_bingx_listing_state_and_position_mode_come_from_the_venue(bingx):
    adapter, wire = bingx.adapter, bingx.wire
    contract = wire.contract_record(bingx.fake._rules[BTC])
    wire.answer_next("GET", "/quote/contracts", Reply(200, {"code": 0, "data": [{**contract, "apiStateOpen": "false"}]}))
    closed = adapter.instrument_rules()[BTC]
    assert (closed.status, closed.venue_status) == (c.NOT_TRADING, "1/apiStateOpen=false")
    assert (closed.max_leverage, closed.max_quantity) == (None, None)   # not published in the contract list
    adapter.place_order(market(BTC, c.BUY, 0.01, "entry-1"))
    wire.calls.clear()
    assert adapter.positions(BTC)[0].position_mode == "ONE_WAY"
    assert [p for _, p, _ in wire.calls] == ["/openApi/swap/v2/user/positions", "/openApi/swap/v1/positionSide/dual"]
    wire.answer_next("GET", "/positionSide/dual", Reply(200, {"code": 0, "data": {"dualSidePosition": "true"}}))
    assert adapter.positions(BTC)[0].position_mode == "HEDGE"
    wire.answer_next("GET", "/positionSide/dual", Reply(200, {"code": 0, "data": {}}))
    assert adapter.positions(BTC)[0].position_mode is None             # not said: not assumed


def test_bingx_live_and_demo_are_two_hosts_and_the_adapter_follows_the_account():
    assert resolve_base_url(c.BINGX, BrokerEnvironment.LIVE) == "https://open-api.bingx.com"
    assert resolve_base_url(c.BINGX, BrokerEnvironment.DEMO) == "https://open-api-vst.bingx.com"
