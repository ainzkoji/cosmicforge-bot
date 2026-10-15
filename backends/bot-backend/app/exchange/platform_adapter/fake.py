"""A deterministic in-memory venue that speaks the venue adapter contract (test infrastructure, Step 4.1).

``FakeVenue`` is what execution, stop-replacement, shadow and certification tests run against when no venue
may be touched. It is not a simulator of any exchange: it is a small state machine with exact, documented
rules, so that a test which passes says something precise.

It cannot be mistaken for a venue. ``adapter_version`` starts with ``fake-``, ``factory.adapter_for`` never
returns it, it performs no input or output of any kind, and its only clock is the one it is given.

The rules it follows
--------------------
* **Profiles.** ``binance``, ``bybit`` and ``bingx`` reproduce the capability matrix of the three real
  adapters (``capabilities()`` is equal to theirs except for ``user_stream`` and ``notes``):

  - ``binance``: a stop is bound to the whole position (``close_position``), may be placed while flat, stays
    resting after the position closes, cannot be amended.
  - ``bybit``: a stop is an attribute of the position (``close_position``), needs an open position, is
    amended in place by ``amend_stop`` and disappears when the position closes.
  - ``bingx``: a stop carries its own quantity (``reduce_only``), needs a position at least that large, stays
    resting after the position closes, cannot be amended.

  One form works on every profile: ``reduce_only`` with the quantity of the WHOLE open position. On
  ``binance`` and ``bybit`` it is placed as the whole-position stop and reads back with ``close_position``
  True; a quantity that is only part of the position is ``UNSUPPORTED`` there, and ``close_position`` is
  ``UNSUPPORTED`` on ``bingx``. On every profile a second resting trigger order of the same kind and side is
  refused with ``STOP_ALREADY_EXISTS``.
* **Instrument rules are enforced.** Quantity on the step, at least the minimum, at most the maximum; limit
  and trigger prices on the tick; notional at least the minimum (reduce-only orders are exempt); nothing new
  on an instrument that is not trading.
* **One-way positions**, one per symbol, with cross margin: ``available = equity - sum(|qty| * entry /
  leverage)``. An order that opens or enlarges a position needs its initial margin; a resting order
  reserves nothing.
* **Market orders** fill at once, in full, at the current price (``fill_market_orders=False`` makes them
  wait for ``fill``). **Limit orders** rest and fill at their own price when ``set_price`` crosses it, or in
  parts through ``fill``. **Trigger orders** fire when ``set_price`` reaches the trigger; one that would
  fire at the current price is refused with ``STOP_WOULD_TRIGGER_IMMEDIATELY``. There is one price per
  symbol, so ``working_type`` changes nothing.
* **Reduce-only never opens or flips a position.** It is refused when nothing can be reduced or when it asks
  for more than the open quantity; a trigger order that fires executes at most what is open (``FILLED`` when
  that was its whole quantity, ``CANCELED`` when less, ``EXPIRED`` when nothing was open).
* **Client order ids are idempotent.** The same id with the same symbol, side and type returns the existing
  order and changes nothing; with anything else it is ``DUPLICATE_CLIENT_ORDER_ID``.
* **Fees** are the taker rate on market and trigger fills and the maker rate on resting limit fills, paid
  from the wallet.
* **Journals.** ``mutations`` holds every call of a mutating method and ``reads`` every venue read, each
  with its arguments, its outcome and whether venue state changed. A shadow run is proved by
  ``fake.mutations == []``. ``capabilities`` and ``rate_limit`` are local observations and are not journaled;
  the harness controls (``set_price``, ``fill``, ``external_trade``) are the market and the operator, not
  the adapter, and are not journaled either.
* **Failures.** ``fail_next`` raises a chosen ``AdapterError`` from the next matching call. With
  ``after_effect=True`` the venue performs the mutation and only the answer is lost, which is what a timeout
  after acceptance looks like. ``rate_limit_for`` refuses every venue call until the clock has passed.
* **Protection gaps.** ``protection_gaps(symbol)`` lists every interval in which a non-zero position had no
  resting ``STOP_MARKET`` on its closing side covering the whole of it. Intervals are measured in clock
  time and in state changes, so a cancel followed by a create at the same instant is still a gap.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from . import contract as c
from .contract import (AccountBalance, AdapterCapabilities, AdapterError, FeeRates, FillInfo, FundingInfo,
                       InstrumentRules, OrderInfo, OrderRequest, PositionInfo, RateLimitObservation, ServerTime,
                       UserStreamSpec)

FAKE_ADAPTER_VERSION = "fake-1.0.0"
_ZERO = Decimal("0")
_FUNDING_INTERVAL_MS = 8 * 3_600_000
#: read methods that ask the venue (journaled, rate limited, open to failure injection)
_VENUE_READS = tuple(m for m in c.READ_METHODS if m not in ("capabilities", "rate_limit"))

#: profile -> (close_position_stop, quantity_stop, stop_amend_in_place, position_stop_endpoint,
#:             stop_client_order_id, client_order_id_max_length)
_PROFILES: Mapping[str, Tuple[bool, bool, bool, bool, bool, int]] = {
    c.BINANCE: (True, False, False, False, True, 36),
    c.BYBIT: (True, False, True, True, False, 36),
    c.BINGX: (False, True, False, False, False, 40),
}


def _d(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class JournalEntry:
    """One call of a contract method: what was asked, what came back and whether the venue changed."""
    seq: int
    at_ms: int
    method: str
    arguments: Mapping[str, Any]
    outcome: str                        # "OK" or the AdapterError code that was raised
    effect: bool                        # venue state changed (true for an accepted call whose answer was lost)


@dataclass(frozen=True)
class ProtectionGap:
    """An interval in which a non-zero position had no resting stop covering it. ``end_ms`` is None while open."""
    symbol: str
    start_ms: int
    end_ms: Optional[int]
    start_event: int                    # the state change that left the position uncovered
    end_event: Optional[int]

    @property
    def open(self) -> bool:
        return self.end_ms is None

    @property
    def duration_ms(self) -> Optional[int]:
        return None if self.end_ms is None else int(self.end_ms) - int(self.start_ms)


@dataclass
class _Order:
    venue_order_id: str
    client_order_id: Optional[str]
    symbol: str
    side: str
    order_type: str
    quantity: Optional[Decimal]
    price: Optional[Decimal]
    stop_price: Optional[Decimal]
    reduce_only: bool
    close_position: bool
    updated_at_ms: int
    status: str = c.NEW
    filled: Decimal = _ZERO
    notional: Decimal = _ZERO

    @property
    def open(self) -> bool:
        return self.status in c.OPEN_STATUSES

    @property
    def protective(self) -> bool:
        return self.order_type in (c.STOP_MARKET, c.TAKE_PROFIT_MARKET)


@dataclass
class _Position:
    quantity: Decimal = _ZERO           # signed: long > 0, short < 0
    entry: Decimal = _ZERO
    leverage: int = 1


@dataclass
class _Failure:
    method: str
    template: AdapterError
    after_effect: bool
    remaining: int
    symbol: Optional[str]

    def raise_now(self, venue: str) -> None:
        t = self.template
        raise AdapterError(t.code, t.message, venue=venue, retry_after_s=t.retry_after_s,
                           banned_until_ms=t.banned_until_ms, venue_code=t.venue_code, http_status=t.http_status)


class FakeVenue:
    """One connected account on an in-memory venue. Implements ``VenueAdapter``; see the module docstring."""

    def __init__(self, profile: str = c.BINANCE, *, environment: str = c.DEMO,
                 clock: Optional[Callable[[], int]] = None, start_ms: int = 1_700_000_000_000,
                 wallet_balance: float = 10_000.0, margin_asset: str = "USDT", maker_fee: float = 0.0002,
                 taker_fee: float = 0.0005, default_leverage: int = 1, fill_market_orders: bool = True) -> None:
        if profile not in _PROFILES:
            raise ValueError(f"unknown fake venue profile {profile!r}")
        if environment not in c.ENVIRONMENTS:
            raise ValueError(f"unknown environment {environment!r}")
        self.venue, self.environment, self.adapter_version = profile, environment, FAKE_ADAPTER_VERSION
        (self._close_stop, self._quantity_stop, self._amend, self._position_stop, self._stop_cid,
         self._cid_length) = _PROFILES[profile]
        self._clock, self._now = clock, int(start_ms)
        self.margin_asset, self.maker_fee, self.taker_fee = margin_asset, float(maker_fee), float(taker_fee)
        self.default_leverage, self.fill_market_orders = int(default_leverage), bool(fill_market_orders)
        #: added to the clock in ``server_time`` (models local clock drift)
        self.server_time_offset_ms = 0
        self.mutations: List[JournalEntry] = []
        self.reads: List[JournalEntry] = []
        self._wallet = _d(wallet_balance)
        self._rules: Dict[str, InstrumentRules] = {}
        self._prices: Dict[str, Decimal] = {}
        self._funding: Dict[str, float] = {}
        self._positions: Dict[str, _Position] = {}
        self._orders: Dict[str, _Order] = {}
        self._by_client: Dict[str, _Order] = {}
        self._fills: List[FillInfo] = []
        self._failures: List[_Failure] = []
        self._gaps: Dict[str, List[ProtectionGap]] = {}
        self._gap_open: Dict[str, Tuple[int, int]] = {}
        self._events = self._calls = self._ids = 0
        self._limited_until_ms = self._banned_until_ms = 0

    def __repr__(self) -> str:
        return (f"FakeVenue(venue={self.venue!r}, environment={self.environment!r}, "
                f"adapter_version={self.adapter_version!r})")

    # ------------------------------------------------------------------------------------ harness controls
    def now_ms(self) -> int:
        return int(self._clock()) if self._clock is not None else self._now

    def advance(self, ms: int) -> int:
        """Move the built-in clock forward. With an injected clock, move that clock instead."""
        if self._clock is not None:
            raise ValueError("this fake venue reads an injected clock; advance that clock")
        if int(ms) < 0:
            raise ValueError("the clock does not go backwards")
        self._now += int(ms)
        return self._now

    def add_instrument(self, symbol: str, *, price: float, price_tick: float = 0.1, quantity_step: float = 0.001,
                       min_quantity: Optional[float] = None, min_notional: Optional[float] = 5.0,
                       max_quantity: Optional[float] = None, max_leverage: Optional[float] = 100.0,
                       status: str = c.TRADING, funding_rate: float = 0.0001) -> InstrumentRules:
        symbol = c.canonical_symbol(symbol)
        quote = self.margin_asset if symbol.endswith(self.margin_asset) else ""
        base = symbol[:-len(quote)] if quote else symbol
        venue_symbol = f"{base}-{quote}" if self.venue == c.BINGX and quote else symbol
        self._prices[symbol], self._funding[symbol] = _d(price), float(funding_rate)
        return self._store_rules(InstrumentRules(
            venue=self.venue, environment=self.environment, symbol=symbol, venue_symbol=venue_symbol, status=status,
            venue_status=status, base_asset=base, quote_asset=quote or self.margin_asset,
            margin_asset=self.margin_asset, price_tick=price_tick, quantity_step=quantity_step,
            min_quantity=quantity_step if min_quantity is None else min_quantity, max_quantity=max_quantity,
            min_notional=min_notional, max_leverage=max_leverage, contract_size=1.0, fetched_at_ms=self.now_ms()))

    def set_rules(self, symbol: str, **changes: Any) -> InstrumentRules:
        """Change published rules (``status=NOT_TRADING``, a new step, a rule set to None...)."""
        return self._store_rules(replace(self._known(symbol), **changes))

    def remove_instrument(self, symbol: str) -> None:
        """Delist: the venue no longer publishes the instrument. Its orders and position are left alone."""
        self._known(symbol)
        del self._rules[symbol]

    def set_price(self, symbol: str, price: float) -> Tuple[FillInfo, ...]:
        """Move the market. Fires trigger orders that the new price reaches, then fills crossed limit orders."""
        self._known(symbol)
        self._prices[symbol] = px = _d(price)
        fills: List[FillInfo] = []
        for order in [o for o in self._orders.values() if o.symbol == symbol]:
            if not order.open:
                continue
            if order.protective:
                if self._reached(order.order_type, order.side, order.stop_price, px):
                    fills.extend(self._fill_capped(order, order.quantity, px, maker=False))
            elif order.order_type == c.LIMIT and ((order.side == c.BUY and px <= order.price)
                                                  or (order.side == c.SELL and px >= order.price)):
                fills.extend(self._fill_capped(order, order.quantity - order.filled, order.price, maker=True))
        self._recheck(symbol)
        return tuple(fills)

    def fill(self, client_order_id: str, quantity: Optional[float] = None,
             price: Optional[float] = None) -> Optional[FillInfo]:
        """Fill part (or the rest) of an open market or limit order. Trigger orders fire through ``set_price``.

        Returns None when a reduce-only order found nothing left to reduce and expired instead."""
        order = self._by_client.get(client_order_id)
        if order is None or not order.open or order.protective:
            raise ValueError(f"no open market or limit order with client order id {client_order_id!r}")
        remaining = order.quantity - order.filled
        qty = remaining if quantity is None else _d(quantity)
        if qty <= 0 or qty > remaining:
            raise ValueError(f"cannot fill {qty} of an order with {remaining} remaining")
        px = _d(price) if price is not None else (order.price if order.price is not None else self._prices[order.symbol])
        fills = self._fill_capped(order, qty, px, maker=order.order_type == c.LIMIT)
        self._recheck(order.symbol)
        return fills[0] if fills else None

    def external_trade(self, symbol: str, side: str, quantity: float, price: Optional[float] = None) -> FillInfo:
        """A trade the account owner made at the venue by hand: no client order id, no rule check, no journal."""
        self._known(symbol)
        order = self._new_order(symbol, side, c.MARKET, None, _d(quantity), None, None, False, False)
        fill = self._execute(order, order.quantity, _d(price) if price is not None else self._prices[symbol], maker=False)
        self._recheck(symbol)
        return fill

    def fail_next(self, method: str, error: Union[str, AdapterError], *, after_effect: bool = False, times: int = 1,
                  symbol: Optional[str] = None) -> None:
        """Make the next ``times`` calls of ``method`` (for ``symbol`` only, when given) raise ``error``.

        ``after_effect=False``: the venue never acted. ``after_effect=True`` (mutations only): the venue
        performed the call and the answer was lost; pair it with ``NETWORK_UNCERTAIN``."""
        if method not in c.MUTATING_METHODS and method not in _VENUE_READS:
            raise ValueError(f"{method!r} is not a venue call of the contract")
        if after_effect and method not in c.MUTATING_METHODS:
            raise ValueError("only a mutation can take effect before its answer is lost")
        if int(times) < 1:
            raise ValueError("times must be at least 1")
        template = error if isinstance(error, AdapterError) else AdapterError(str(error), "injected failure")
        self._failures.append(_Failure(method, template, bool(after_effect), int(times),
                                       c.canonical_symbol(symbol) if symbol else None))

    def clear_failures(self) -> None:
        self._failures.clear()
        self._limited_until_ms = self._banned_until_ms = 0

    def rate_limit_for(self, seconds: float, *, banned: bool = False) -> None:
        """Refuse every venue call with ``RATE_LIMITED`` (``BANNED`` when ``banned``) until the clock passes."""
        until = self.now_ms() + int(float(seconds) * 1000)
        if banned:
            self._banned_until_ms = until
        else:
            self._limited_until_ms = until

    def protection_gaps(self, symbol: str) -> Tuple[ProtectionGap, ...]:
        symbol = c.canonical_symbol(symbol)
        gaps = list(self._gaps.get(symbol, ()))
        if symbol in self._gap_open:
            start_ms, start_event = self._gap_open[symbol]
            gaps.append(ProtectionGap(symbol, start_ms, None, start_event, None))
        return tuple(gaps)

    # ------------------------------------------------------------------------------------------ read only
    def capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            venue=self.venue, environment=self.environment, native_stop_market=True, native_take_profit=True,
            reduce_only=True, close_position_stop=self._close_stop, stop_amend_in_place=self._amend,
            multiple_stops_per_position=False, one_way_mode=True, hedge_mode=False, user_stream=True,
            demo_environment=True, client_order_id_max_length=self._cid_length,
            position_stop_endpoint=self._position_stop, quantity_stop=self._quantity_stop,
            stop_client_order_id=self._stop_cid,
            notes=("FAKE VENUE: an in-memory test double; nothing here is evidence about a real venue",))

    def rate_limit(self) -> RateLimitObservation:
        now = self.now_ms()
        return RateLimitObservation(
            venue=self.venue, used_weight=float(self._calls), limit=None, window_seconds=None,
            retry_after_s=(self._limited_until_ms - now) / 1000.0 if self._limited_until_ms > now else None,
            banned_until_ms=self._banned_until_ms if self._banned_until_ms > now else None, observed_at_ms=now)

    def server_time(self) -> ServerTime:
        return self._run("server_time", None, {}, lambda: ServerTime(
            server_ms=self.now_ms() + int(self.server_time_offset_ms), local_ms=self.now_ms()))

    def instrument_rules(self, symbols: Optional[Sequence[str]] = None) -> Mapping[str, InstrumentRules]:
        def read() -> Mapping[str, InstrumentRules]:
            wanted = None if symbols is None else {c.canonical_symbol(s) for s in symbols}
            now = self.now_ms()
            return {s: replace(r, fetched_at_ms=now) for s, r in self._rules.items() if wanted is None or s in wanted}
        return self._run("instrument_rules", None, {"symbols": None if symbols is None else tuple(symbols)}, read)

    def balance(self) -> AccountBalance:
        def read() -> AccountBalance:
            unrealized = sum((self._unrealized(s) for s in self._positions), _ZERO)
            used = sum((abs(p.quantity) * p.entry / p.leverage for p in self._positions.values()), _ZERO)
            equity = self._wallet + unrealized
            return AccountBalance(venue=self.venue, margin_asset=self.margin_asset, wallet_balance=float(self._wallet),
                                  unrealized_pnl=float(unrealized), equity=float(equity),
                                  available_balance=float(equity - used), observed_at_ms=self.now_ms())
        return self._run("balance", None, {}, read)

    def positions(self, symbol: Optional[str] = None) -> Tuple[PositionInfo, ...]:
        def read() -> Tuple[PositionInfo, ...]:
            now = self.now_ms()
            return tuple(PositionInfo(
                venue=self.venue, symbol=s, side=c.LONG if p.quantity > 0 else c.SHORT, quantity=float(abs(p.quantity)),
                entry_price=float(p.entry), mark_price=float(self._prices[s]), unrealized_pnl=float(self._unrealized(s)),
                leverage=float(p.leverage), margin_mode="CROSSED", position_mode="ONE_WAY", observed_at_ms=now)
                for s, p in self._positions.items() if p.quantity != 0 and symbol in (None, s))
        return self._run("positions", symbol, {"symbol": symbol}, read)

    def open_orders(self, symbol: Optional[str] = None) -> Tuple[OrderInfo, ...]:
        return self._run("open_orders", symbol, {"symbol": symbol}, lambda: tuple(
            self._info(o) for o in self._orders.values() if o.open and symbol in (None, o.symbol)))

    def order(self, symbol: str, *, client_order_id: Optional[str] = None,
              venue_order_id: Optional[str] = None) -> OrderInfo:
        return self._run("order", symbol, {"symbol": symbol, "client_order_id": client_order_id,
                                           "venue_order_id": venue_order_id},
                         lambda: self._info(self._find(symbol, client_order_id, venue_order_id)))

    def fills(self, symbol: str, *, since_ms: Optional[int] = None,
              venue_order_id: Optional[str] = None) -> Tuple[FillInfo, ...]:
        return self._run("fills", symbol, {"symbol": symbol, "since_ms": since_ms, "venue_order_id": venue_order_id},
                         lambda: tuple(f for f in self._fills if f.symbol == symbol
                                       and (since_ms is None or f.time_ms >= int(since_ms))
                                       and venue_order_id in (None, f.venue_order_id)))

    def fee_rates(self, symbol: str) -> FeeRates:
        def read() -> FeeRates:
            self._published(symbol)
            return FeeRates(venue=self.venue, symbol=symbol, maker=self.maker_fee, taker=self.taker_fee, source="ACCOUNT")
        return self._run("fee_rates", symbol, {"symbol": symbol}, read)

    def funding(self, symbol: str) -> FundingInfo:
        def read() -> FundingInfo:
            self._published(symbol)
            now = self.now_ms()
            return FundingInfo(venue=self.venue, symbol=symbol, rate=self._funding[symbol],
                               next_funding_ms=(now // _FUNDING_INTERVAL_MS + 1) * _FUNDING_INTERVAL_MS,
                               interval_minutes=_FUNDING_INTERVAL_MS // 60_000, mark_price=float(self._prices[symbol]),
                               observed_at_ms=now)
        return self._run("funding", symbol, {"symbol": symbol}, read)

    def user_stream(self) -> UserStreamSpec:
        return self._run("user_stream", None, {}, lambda: UserStreamSpec(
            venue=self.venue, url=f"fake://{self.venue}/{self.environment.lower()}/user-stream", auth_mode="AUTH_FRAME",
            keepalive_seconds=20, expires_after_seconds=None))

    # ------------------------------------------------------------------------------------------ mutations
    def set_leverage(self, symbol: str, leverage: int) -> int:
        def act() -> int:
            rules = self._published(symbol)
            if int(leverage) < 1 or (rules.max_leverage is not None and int(leverage) > rules.max_leverage):
                raise self._error(c.LEVERAGE_NOT_ALLOWED, f"leverage {leverage} is outside 1..{rules.max_leverage}")
            position = self._positions.setdefault(symbol, _Position(leverage=self.default_leverage))
            if position.leverage != int(leverage):
                position.leverage = int(leverage)
                self._events += 1
            return position.leverage
        return self._run("set_leverage", symbol, {"symbol": symbol, "leverage": leverage}, act, mutation=True)

    def place_order(self, request: OrderRequest) -> OrderInfo:
        return self._run("place_order", request.symbol, {"request": request}, lambda: self._place(request),
                         mutation=True)

    def cancel_order(self, symbol: str, *, client_order_id: Optional[str] = None,
                     venue_order_id: Optional[str] = None) -> OrderInfo:
        def act() -> OrderInfo:
            order = self._find(symbol, client_order_id, venue_order_id)
            if order.open:
                self._close(order, c.CANCELED)
                self._recheck(symbol)
            return self._info(order)
        return self._run("cancel_order", symbol, {"symbol": symbol, "client_order_id": client_order_id,
                                                  "venue_order_id": venue_order_id}, act, mutation=True)

    def amend_stop(self, symbol: str, *, client_order_id: str, new_stop_price: float) -> OrderInfo:
        def act() -> OrderInfo:
            if not self._amend:
                raise self._error(c.UNSUPPORTED, "this venue cannot change the trigger of a resting stop")
            order = self._find(symbol, client_order_id, None)
            if not order.open or not order.protective:
                raise self._error(c.ORDER_NOT_FOUND, "no resting stop with this client order id")
            self._check_trigger(self._published(symbol), order.order_type, order.side, _d(new_stop_price))
            if order.stop_price != _d(new_stop_price):
                order.stop_price, order.updated_at_ms = _d(new_stop_price), self.now_ms()
                self._events += 1
            self._recheck(symbol)
            return self._info(order)
        return self._run("amend_stop", symbol, {"symbol": symbol, "client_order_id": client_order_id,
                                                "new_stop_price": new_stop_price}, act, mutation=True)

    # ------------------------------------------------------------------------------------------- internals
    def _error(self, code: str, message: str = "", **kw: Any) -> AdapterError:
        return AdapterError(code, message, venue=self.venue, **kw)

    def _run(self, method: str, symbol: Optional[str], arguments: Mapping[str, Any], action: Callable[[], Any], *,
             mutation: bool = False) -> Any:
        journal = self.mutations if mutation else self.reads
        self._calls += 1
        before, at = self._events, self.now_ms()
        try:
            failure = self._gate(method, symbol)
            if failure is not None and not failure.after_effect:
                failure.raise_now(self.venue)
            result = action()
            if failure is not None:
                failure.raise_now(self.venue)
        except AdapterError as error:
            journal.append(JournalEntry(len(journal) + 1, at, method, dict(arguments), error.code, self._events != before))
            raise
        journal.append(JournalEntry(len(journal) + 1, at, method, dict(arguments), "OK", self._events != before))
        return result

    def _gate(self, method: str, symbol: Optional[str]) -> Optional[_Failure]:
        now = self.now_ms()
        if self._banned_until_ms > now:
            raise self._error(c.BANNED, "the venue has banned this address", banned_until_ms=self._banned_until_ms,
                              http_status=418)
        if self._limited_until_ms > now:
            raise self._error(c.RATE_LIMITED, "the request budget is spent",
                              retry_after_s=(self._limited_until_ms - now) / 1000.0, http_status=429)
        for failure in self._failures:
            if failure.method == method and failure.symbol in (None, symbol):
                failure.remaining -= 1
                if failure.remaining <= 0:
                    self._failures.remove(failure)
                return failure
        return None

    def _store_rules(self, rules: InstrumentRules) -> InstrumentRules:
        body = repr((rules.symbol, rules.venue_symbol, rules.status, rules.price_tick, rules.quantity_step,
                     rules.min_quantity, rules.max_quantity, rules.min_notional, rules.max_leverage,
                     rules.market_max_quantity))
        rules = replace(rules, fetched_at_ms=self.now_ms(), raw_hash=hashlib.sha256(body.encode("utf-8")).hexdigest())
        self._rules[rules.symbol] = rules
        return rules

    def _known(self, symbol: str) -> InstrumentRules:
        if symbol not in self._rules:
            raise ValueError(f"the fake venue has no instrument {symbol!r}; call add_instrument first")
        return self._rules[symbol]

    def _published(self, symbol: str) -> InstrumentRules:
        if symbol not in self._rules:
            raise self._error(c.INSTRUMENT_UNKNOWN, f"{symbol} is not listed")
        return self._rules[symbol]

    def _unrealized(self, symbol: str) -> Decimal:
        position = self._positions[symbol]
        return position.quantity * (self._prices[symbol] - position.entry) if position.quantity != 0 else _ZERO

    def _signed(self, symbol: str) -> Decimal:
        position = self._positions.get(symbol)
        return position.quantity if position is not None else _ZERO

    def _reducible(self, symbol: str, side: str) -> Decimal:
        """How much an order on ``side`` can take off the open position (0 when it would add to it)."""
        signed = self._signed(symbol)
        return abs(signed) if (signed > 0 and side == c.SELL) or (signed < 0 and side == c.BUY) else _ZERO

    def _find(self, symbol: str, client_order_id: Optional[str], venue_order_id: Optional[str]) -> _Order:
        if not (client_order_id or venue_order_id):
            raise c.ContractViolation("an order is looked up by its client order id or its venue order id")
        order = self._orders.get(str(venue_order_id)) if venue_order_id else self._by_client.get(str(client_order_id))
        if order is None or order.symbol != symbol:
            raise self._error(c.ORDER_NOT_FOUND, "the venue holds no such order")
        return order

    def _info(self, order: _Order) -> OrderInfo:
        return OrderInfo(
            venue=self.venue, symbol=order.symbol, client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id, status=order.status, venue_status=order.status, side=order.side,
            order_type=order.order_type, quantity=None if order.quantity is None else float(order.quantity),
            filled_quantity=float(order.filled),
            average_price=float(order.notional / order.filled) if order.filled > 0 else None,
            stop_price=None if order.stop_price is None else float(order.stop_price), reduce_only=order.reduce_only,
            close_position=order.close_position, updated_at_ms=order.updated_at_ms)

    def _new_order(self, symbol: str, side: str, order_type: str, client_order_id: Optional[str],
                   quantity: Optional[Decimal], price: Optional[Decimal], stop_price: Optional[Decimal],
                   reduce_only: bool, close_position: bool) -> _Order:
        self._ids += 1
        order = _Order(str(1000 + self._ids), client_order_id, symbol, side, order_type, quantity, price, stop_price,
                       reduce_only, close_position, self.now_ms())
        self._orders[order.venue_order_id] = order
        if client_order_id is not None:
            self._by_client[client_order_id] = order
        self._events += 1
        return order

    def _close(self, order: _Order, status: str) -> None:
        order.status, order.updated_at_ms = status, self.now_ms()
        self._events += 1

    @staticmethod
    def _multiple(value: Decimal, unit: Optional[float]) -> bool:
        return unit is None or value % _d(unit) == 0

    @staticmethod
    def _reached(order_type: str, side: str, trigger: Decimal, price: Decimal) -> bool:
        falling = (order_type == c.STOP_MARKET) == (side == c.SELL)     # a sell stop and a buy take-profit fire downwards
        return price <= trigger if falling else price >= trigger

    def _check_trigger(self, rules: InstrumentRules, order_type: str, side: str, trigger: Decimal) -> None:
        if not self._multiple(trigger, rules.price_tick):
            raise self._error(c.INVALID_PRICE, f"trigger {trigger} is not on the price tick {rules.price_tick}")
        if self._reached(order_type, side, trigger, self._prices[rules.symbol]):
            raise self._error(c.STOP_WOULD_TRIGGER_IMMEDIATELY,
                              f"a {side} {order_type} at {trigger} would fire at the current price")

    def _place(self, request: OrderRequest) -> OrderInfo:
        existing = self._by_client.get(request.client_order_id)
        if existing is not None:
            if (existing.symbol, existing.side, existing.order_type) != (request.symbol, request.side, request.order_type):
                raise self._error(c.DUPLICATE_CLIENT_ORDER_ID, "this client order id belongs to a different order")
            return self._info(existing)
        rules = self._published(request.symbol)
        if rules.status != c.TRADING:
            raise self._error(c.INSTRUMENT_NOT_TRADING, f"{request.symbol} is {rules.venue_status}")
        order = self._place_trigger(request, rules) if request.protective else self._place_entry(request, rules)
        self._recheck(request.symbol)
        return self._info(order)

    def _check_quantity(self, rules: InstrumentRules, quantity: Decimal) -> None:
        if (not self._multiple(quantity, rules.quantity_step)
                or (rules.min_quantity is not None and quantity < _d(rules.min_quantity))
                or (rules.max_quantity is not None and quantity > _d(rules.max_quantity))):
            raise self._error(c.INVALID_QUANTITY, f"quantity {quantity} breaks the step, minimum or maximum")

    def _place_entry(self, request: OrderRequest, rules: InstrumentRules) -> _Order:
        symbol, market = request.symbol, self._prices[request.symbol]
        quantity = _d(request.quantity)
        self._check_quantity(rules, quantity)
        if request.order_type == c.LIMIT and not self._multiple(_d(request.price), rules.price_tick):
            raise self._error(c.INVALID_PRICE, f"price {request.price} is not on the price tick {rules.price_tick}")
        reference = _d(request.price) if request.order_type == c.LIMIT else market
        reducible = self._reducible(symbol, request.side)
        if request.reduce_only:
            if reducible == 0 or quantity > reducible:
                raise self._error(c.REDUCE_ONLY_REJECTED, f"only {reducible} can be reduced on {symbol}")
        else:
            if rules.min_notional is not None and quantity * reference < _d(rules.min_notional):
                raise self._error(c.MIN_NOTIONAL, f"notional {quantity * reference} is below {rules.min_notional}")
            position = self._positions.setdefault(symbol, _Position(leverage=self.default_leverage))
            opening = max(quantity - reducible, _ZERO)
            unrealized = sum((self._unrealized(s) for s in self._positions), _ZERO)
            used = sum((abs(p.quantity) * p.entry / p.leverage for p in self._positions.values()), _ZERO)
            if opening * reference / position.leverage > self._wallet + unrealized - used:
                raise self._error(c.INSUFFICIENT_MARGIN, f"margin for {opening} {symbol} is not available")
        order = self._new_order(symbol, request.side, request.order_type, request.client_order_id, quantity,
                                None if request.price is None else _d(request.price), None, request.reduce_only, False)
        crossing = request.order_type == c.LIMIT and (
            (request.side == c.BUY and reference >= market) or (request.side == c.SELL and reference <= market))
        if (request.order_type == c.MARKET and self.fill_market_orders) or crossing:
            self._execute(order, quantity, market, maker=False)
        return order

    def _place_trigger(self, request: OrderRequest, rules: InstrumentRules) -> _Order:
        symbol, whole = request.symbol, request.close_position
        reducible = self._reducible(symbol, request.side)
        if whole and not self._close_stop:
            raise self._error(c.UNSUPPORTED, "this venue has no stop bound to the whole position; send a quantity")
        if not whole and not self._quantity_stop:
            # the venue's stop is bound to the whole position: a quantity is carried only when it IS that position
            if reducible == 0:
                raise self._error(c.REDUCE_ONLY_REJECTED, f"there is no open {symbol} position this stop could close")
            if _d(request.quantity) != reducible:
                raise self._error(c.UNSUPPORTED, "this venue has no stop for part of a position")
            whole = True
        self._check_trigger(rules, request.order_type, request.side, _d(request.stop_price))
        quantity = None if whole else _d(request.quantity)
        if quantity is not None:
            self._check_quantity(rules, quantity)
        if (self._position_stop or quantity is not None) and (reducible == 0 or (quantity or _ZERO) > reducible):
            raise self._error(c.REDUCE_ONLY_REJECTED, f"there is no open {symbol} position this stop could reduce")
        if any(o.open and o.symbol == symbol and (o.order_type, o.side) == (request.order_type, request.side)
               for o in self._orders.values()):
            raise self._error(c.STOP_ALREADY_EXISTS, f"a {request.side} {request.order_type} already rests on {symbol}")
        return self._new_order(symbol, request.side, request.order_type, request.client_order_id, quantity, None,
                               _d(request.stop_price), not whole, whole)

    def _fill_capped(self, order: _Order, wanted: Optional[Decimal], price: Decimal, *, maker: bool) -> List[FillInfo]:
        """Execute ``wanted`` (None = whatever is open) but never more than a reducing order may take off the
        position: with nothing to reduce it expires, with less than it wanted the remainder is cancelled."""
        if not (order.reduce_only or order.close_position):
            return [self._execute(order, wanted, price, maker=maker)]
        reducible = self._reducible(order.symbol, order.side)
        quantity = reducible if wanted is None else min(wanted, reducible)
        if quantity == 0:
            self._close(order, c.EXPIRED)
            return []
        fill = self._execute(order, quantity, price, maker=maker)
        if order.open and (wanted is None or quantity < wanted):
            self._close(order, c.CANCELED)
        return [fill]

    def _execute(self, order: _Order, quantity: Decimal, price: Decimal, *, maker: bool) -> FillInfo:
        position = self._positions.setdefault(order.symbol, _Position(leverage=self.default_leverage))
        signed = quantity if order.side == c.BUY else -quantity
        closing = min(quantity, self._reducible(order.symbol, order.side))
        realized = closing * (price - position.entry) * (1 if position.quantity > 0 else -1) if closing > 0 else _ZERO
        after = position.quantity + signed
        if after == 0:
            position.entry = _ZERO
        elif closing == 0:
            position.entry = (abs(position.quantity) * position.entry + quantity * price) / abs(after)
        elif closing < quantity:                         # the order went through flat and opened the other side
            position.entry = price
        position.quantity = after
        fee = quantity * price * _d(self.maker_fee if maker else self.taker_fee)
        self._wallet += realized - fee
        order.filled += quantity
        order.notional += quantity * price
        order.updated_at_ms = self.now_ms()
        if order.quantity is None or order.filled == order.quantity:
            order.status = c.FILLED
        else:
            order.status = c.PARTIALLY_FILLED
        self._ids += 1
        self._events += 1
        fill = FillInfo(venue=self.venue, symbol=order.symbol, trade_id=f"T{self._ids}",
                        venue_order_id=order.venue_order_id, client_order_id=order.client_order_id, side=order.side,
                        quantity=float(quantity), price=float(price), fee=float(fee), fee_asset=self.margin_asset,
                        realized_pnl=float(realized) if closing > 0 else None, is_maker=maker, time_ms=self.now_ms())
        self._fills.append(fill)
        if after == 0 and self._position_stop:           # a position-level stop does not outlive its position
            for other in self._orders.values():
                if other.open and other.protective and other.symbol == order.symbol and other is not order:
                    self._close(other, c.CANCELED)
        return fill

    def _recheck(self, symbol: str) -> None:
        """Open or close the protection gap of ``symbol`` after a state change."""
        signed = self._signed(symbol)
        closing_side = c.SELL if signed > 0 else c.BUY
        covered = signed == 0 or any(
            o.open and o.symbol == symbol and o.order_type == c.STOP_MARKET and o.side == closing_side
            and (o.close_position or o.quantity >= abs(signed)) for o in self._orders.values())
        if not covered and symbol not in self._gap_open:
            self._gap_open[symbol] = (self.now_ms(), self._events)
        elif covered and symbol in self._gap_open:
            start_ms, start_event = self._gap_open.pop(symbol)
            closed = ProtectionGap(symbol, start_ms, self.now_ms(), start_event, self._events)
            self._gaps.setdefault(symbol, []).append(closed)


__all__ = ["FAKE_ADAPTER_VERSION", "FakeVenue", "JournalEntry", "ProtectionGap"]
