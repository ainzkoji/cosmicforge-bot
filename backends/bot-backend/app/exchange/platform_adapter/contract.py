"""The broker-neutral venue adapter contract of the CATI platform (Section H, Step 4.1).

One typed surface for Binance USD-M, Bybit linear and BingX perpetual swap. CATI code above this line never
sees a venue symbol format, a venue order-type string, a venue error code or a venue response dictionary:
adapters translate all of them, in both directions, and the same contract suite
(``tests/exchange/test_platform_adapter_contract.py``) runs against every adapter.

What the contract fixes:

* **Symbols** are canonical (``BTCUSDT``). An adapter maps to and from its venue form (BingX ``BTC-USDT``).
* **Quantities** are in base units of the instrument, never in contracts. An adapter converts with the
  instrument's contract size.
* **Sides** are ``BUY`` / ``SELL`` for orders and ``LONG`` / ``SHORT`` / ``FLAT`` for positions.
* **Every mutation carries a client order id** chosen by the caller. A repeat of the same id must never create
  a second order: the adapter returns the existing order or raises ``DUPLICATE_CLIENT_ORDER_ID``.
* **Errors** are ``AdapterError`` with a code from ``ERROR_CODES``. ``uncertain=True`` means the venue may
  have accepted the mutation (a timeout after the request left); the caller must look the order up by its
  client order id before doing anything else and must not send it again blindly.
* **Nothing is defaulted silently.** A rule the venue did not report is ``None``; a caller that needs it
  refuses. An adapter never invents a tick size, a step or a leverage limit.
* **No credentials** appear in any value of this module, in ``repr`` or in an error.

An adapter belongs to one connected customer account and one environment. There is no platform-owned
trading account anywhere in this contract.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence, Tuple, runtime_checkable

CONTRACT_VERSION = "1.0.0"

BINANCE, BYBIT, BINGX = "binance", "bybit", "bingx"
VENUES: Tuple[str, ...] = (BINANCE, BYBIT, BINGX)
LIVE, DEMO = "LIVE", "DEMO"
ENVIRONMENTS: Tuple[str, ...] = (LIVE, DEMO)

BUY, SELL = "BUY", "SELL"
LONG, SHORT, FLAT = "LONG", "SHORT", "FLAT"

MARKET, LIMIT, STOP_MARKET, TAKE_PROFIT_MARKET = "MARKET", "LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"
ORDER_TYPES: Tuple[str, ...] = (MARKET, LIMIT, STOP_MARKET, TAKE_PROFIT_MARKET)

NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED = (
    "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED")
#: the venue answered but the state could not be mapped; never treated as open or as closed
UNKNOWN = "UNKNOWN"
ORDER_STATUSES: Tuple[str, ...] = (NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED, UNKNOWN)
OPEN_STATUSES: Tuple[str, ...] = (NEW, PARTIALLY_FILLED)
FINAL_STATUSES: Tuple[str, ...] = (FILLED, CANCELED, REJECTED, EXPIRED)

TRADING, NOT_TRADING = "TRADING", "NOT_TRADING"

# ---------------------------------------------------------------------------------------------------- errors
RATE_LIMITED = "RATE_LIMITED"                      # 429: back off for retry_after_s, never retry sooner
BANNED = "BANNED"                                  # 418 / IP ban: stop until banned_until_ms
AUTH_FAILED = "AUTH_FAILED"
PERMISSION_DENIED = "PERMISSION_DENIED"
TIMESTAMP_OUT_OF_SYNC = "TIMESTAMP_OUT_OF_SYNC"
INSUFFICIENT_MARGIN = "INSUFFICIENT_MARGIN"
INVALID_QUANTITY = "INVALID_QUANTITY"
INVALID_PRICE = "INVALID_PRICE"
MIN_NOTIONAL = "MIN_NOTIONAL"
INSTRUMENT_NOT_TRADING = "INSTRUMENT_NOT_TRADING"
INSTRUMENT_UNKNOWN = "INSTRUMENT_UNKNOWN"
ORDER_NOT_FOUND = "ORDER_NOT_FOUND"
DUPLICATE_CLIENT_ORDER_ID = "DUPLICATE_CLIENT_ORDER_ID"
REDUCE_ONLY_REJECTED = "REDUCE_ONLY_REJECTED"
POSITION_MODE_MISMATCH = "POSITION_MODE_MISMATCH"
STOP_WOULD_TRIGGER_IMMEDIATELY = "STOP_WOULD_TRIGGER_IMMEDIATELY"
LEVERAGE_NOT_ALLOWED = "LEVERAGE_NOT_ALLOWED"
NETWORK_UNCERTAIN = "NETWORK_UNCERTAIN"            # the request may have reached the venue
VENUE_UNAVAILABLE = "VENUE_UNAVAILABLE"            # the request did not reach the venue / 5xx before acceptance
UNSUPPORTED = "UNSUPPORTED"                        # the venue or the environment has no such operation
MALFORMED_RESPONSE = "MALFORMED_RESPONSE"
UNMAPPED_VENUE_ERROR = "UNMAPPED_VENUE_ERROR"
#: a protective stop of this kind already rests on the position and the venue holds only one
#: (``multiple_stops_per_position`` is False): the new stop was NOT created and the old one still rests
STOP_ALREADY_EXISTS = "STOP_ALREADY_EXISTS"
ERROR_CODES: Tuple[str, ...] = (
    RATE_LIMITED, BANNED, AUTH_FAILED, PERMISSION_DENIED, TIMESTAMP_OUT_OF_SYNC, INSUFFICIENT_MARGIN,
    INVALID_QUANTITY, INVALID_PRICE, MIN_NOTIONAL, INSTRUMENT_NOT_TRADING, INSTRUMENT_UNKNOWN, ORDER_NOT_FOUND,
    DUPLICATE_CLIENT_ORDER_ID, REDUCE_ONLY_REJECTED, POSITION_MODE_MISMATCH, STOP_WOULD_TRIGGER_IMMEDIATELY,
    LEVERAGE_NOT_ALLOWED, NETWORK_UNCERTAIN, VENUE_UNAVAILABLE, UNSUPPORTED, MALFORMED_RESPONSE,
    UNMAPPED_VENUE_ERROR, STOP_ALREADY_EXISTS)
#: a caller may repeat the SAME request (same client order id) after the stated wait
RETRYABLE_CODES: Tuple[str, ...] = (RATE_LIMITED, TIMESTAMP_OUT_OF_SYNC, VENUE_UNAVAILABLE)


class AdapterError(Exception):
    """A venue or transport failure in the platform's own vocabulary."""

    def __init__(self, code: str, message: str = "", *, venue: str = "", uncertain: bool = False,
                 retry_after_s: Optional[float] = None, banned_until_ms: Optional[int] = None,
                 venue_code: Optional[str] = None, http_status: Optional[int] = None) -> None:
        if code not in ERROR_CODES:
            raise ValueError(f"unknown adapter error code {code!r}")
        if uncertain and code != NETWORK_UNCERTAIN:
            raise ValueError("only NETWORK_UNCERTAIN may be uncertain")
        super().__init__(f"{venue or 'venue'}:{code}" + (f": {message}" if message else ""))
        self.code, self.venue, self.message = code, venue, message
        self.uncertain = bool(uncertain) or code == NETWORK_UNCERTAIN
        self.retryable = code in RETRYABLE_CODES
        self.retry_after_s, self.banned_until_ms = retry_after_s, banned_until_ms
        self.venue_code, self.http_status = venue_code, http_status


class ContractViolation(ValueError):
    """A value that cannot exist under this contract (raised on construction)."""


def _finite_positive(name: str, value: Any, *, allow_none: bool = False, allow_zero: bool = False) -> None:
    if value is None:
        if allow_none:
            return
        raise ContractViolation(f"{name} is required")
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise ContractViolation(f"{name} is not a number: {value!r}") from None
    if not math.isfinite(v) or v < 0 or (v == 0 and not allow_zero):
        raise ContractViolation(f"{name} must be a finite {'non-negative' if allow_zero else 'positive'} number: {value!r}")


def _one_of(name: str, value: Any, allowed: Sequence[str]) -> None:
    if value not in allowed:
        raise ContractViolation(f"{name} {value!r} is not one of {tuple(allowed)}")


def canonical_symbol(symbol: str) -> str:
    """``BTC-USDT`` / ``btc_usdt`` / ``BTCUSDT`` -> ``BTCUSDT``. Empty or non-alphanumeric input is refused."""
    s = str(symbol or "").upper().replace("-", "").replace("_", "").replace("/", "")
    if not s or not s.isalnum():
        raise ContractViolation(f"not a symbol: {symbol!r}")
    return s


# ---------------------------------------------------------------------------------------------------- values
@dataclass(frozen=True)
class AdapterCapabilities:
    venue: str
    environment: str
    native_stop_market: bool            # a resting, venue-held stop order exists
    native_take_profit: bool
    reduce_only: bool                   # reduce-only orders are honoured
    close_position_stop: bool           # a stop can be bound to "the whole position" without a quantity
    stop_amend_in_place: bool           # the trigger of a live stop can be changed without cancel + create
    multiple_stops_per_position: bool   # a second protective stop may rest beside the first (create-then-cancel)
    one_way_mode: bool
    hedge_mode: bool
    user_stream: bool                   # an authenticated push stream of orders, fills and balances exists
    demo_environment: bool              # the venue offers a no-capital environment for this product
    client_order_id_max_length: int
    position_stop_endpoint: bool = False  # the stop is an attribute of the position (Bybit trading-stop)
    notes: Tuple[str, ...] = ()
    #: a stop may protect PART of a position (carry a quantity smaller than it). False: the venue's stop is bound
    #: to the whole position; a reduce-only stop is then accepted only when its quantity IS the open quantity,
    #: and it comes back as what rests (``close_position`` True, no quantity).
    quantity_stop: bool = False
    #: the venue stores the caller's client order id on a protective stop, so a lookup by that id survives a
    #: restart. False: the adapter knows the binding only while it lives; persist ``venue_order_id``.
    stop_client_order_id: bool = False

    def __post_init__(self) -> None:
        _one_of("venue", self.venue, VENUES)
        _one_of("environment", self.environment, ENVIRONMENTS)
        if self.client_order_id_max_length < 16:
            raise ContractViolation("a client order id shorter than 16 characters cannot carry an identity")


@dataclass(frozen=True)
class InstrumentRules:
    venue: str
    environment: str
    symbol: str                         # canonical
    venue_symbol: str
    status: str                         # TRADING | NOT_TRADING
    venue_status: str                   # verbatim
    base_asset: str
    quote_asset: str
    margin_asset: str
    price_tick: Optional[float]
    quantity_step: Optional[float]      # base units
    min_quantity: Optional[float]       # base units
    max_quantity: Optional[float]
    min_notional: Optional[float]       # quote units
    max_leverage: Optional[float]
    contract_size: float                # base units per venue contract (1.0 when the venue trades base units)
    fetched_at_ms: int
    market_max_quantity: Optional[float] = None
    raw_hash: str = ""                  # hash of the venue record these rules were read from

    def __post_init__(self) -> None:
        _one_of("venue", self.venue, VENUES)
        _one_of("environment", self.environment, ENVIRONMENTS)
        _one_of("status", self.status, (TRADING, NOT_TRADING))
        if canonical_symbol(self.symbol) != self.symbol:
            raise ContractViolation(f"symbol {self.symbol!r} is not canonical")
        for name in ("price_tick", "quantity_step", "min_quantity", "max_quantity", "min_notional", "max_leverage",
                     "market_max_quantity"):
            _finite_positive(name, getattr(self, name), allow_none=True)
        _finite_positive("contract_size", self.contract_size)
        if int(self.fetched_at_ms) <= 0:
            raise ContractViolation("fetched_at_ms is required")

    @property
    def complete(self) -> bool:
        """Every rule an order needs is known. An incomplete record never sizes an order."""
        return None not in (self.price_tick, self.quantity_step, self.min_quantity, self.min_notional)


@dataclass(frozen=True)
class AccountBalance:
    venue: str
    margin_asset: str
    wallet_balance: float               # deposits + realised result
    unrealized_pnl: float
    equity: float                       # wallet_balance + unrealized_pnl, as the venue reports margin balance
    available_balance: float
    observed_at_ms: int

    def __post_init__(self) -> None:
        for name in ("wallet_balance", "unrealized_pnl", "equity", "available_balance"):
            if not math.isfinite(float(getattr(self, name))):
                raise ContractViolation(f"{name} is not finite")


@dataclass(frozen=True)
class PositionInfo:
    venue: str
    symbol: str
    side: str                           # LONG | SHORT | FLAT
    quantity: float                     # base units, >= 0
    entry_price: Optional[float]
    mark_price: Optional[float]
    unrealized_pnl: Optional[float]
    leverage: Optional[float]
    margin_mode: Optional[str]          # CROSSED | ISOLATED
    position_mode: Optional[str]        # ONE_WAY | HEDGE
    observed_at_ms: int

    def __post_init__(self) -> None:
        _one_of("side", self.side, (LONG, SHORT, FLAT))
        _finite_positive("quantity", self.quantity, allow_zero=True)
        if (self.side == FLAT) != (float(self.quantity) == 0.0):
            raise ContractViolation("a FLAT position has quantity 0 and only a FLAT position has")

    @property
    def signed_quantity(self) -> float:
        return float(self.quantity) * (1.0 if self.side == LONG else -1.0 if self.side == SHORT else 0.0)


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: str                           # BUY | SELL
    order_type: str
    client_order_id: str
    quantity: Optional[float] = None    # base units; None only with close_position
    price: Optional[float] = None       # LIMIT
    stop_price: Optional[float] = None  # STOP_MARKET / TAKE_PROFIT_MARKET trigger
    reduce_only: bool = False
    close_position: bool = False        # the venue closes whatever the position is when triggered
    time_in_force: Optional[str] = None
    working_type: str = "MARK_PRICE"    # MARK_PRICE | CONTRACT_PRICE (last)

    def __post_init__(self) -> None:
        if canonical_symbol(self.symbol) != self.symbol:
            raise ContractViolation(f"symbol {self.symbol!r} is not canonical")
        _one_of("side", self.side, (BUY, SELL))
        _one_of("order_type", self.order_type, ORDER_TYPES)
        _one_of("working_type", self.working_type, ("MARK_PRICE", "CONTRACT_PRICE"))
        if not self.client_order_id or not str(self.client_order_id).strip():
            raise ContractViolation("every order carries a client order id")
        if self.close_position:
            if self.order_type not in (STOP_MARKET, TAKE_PROFIT_MARKET) or self.quantity is not None:
                raise ContractViolation("close_position belongs to a trigger order and carries no quantity")
        else:
            _finite_positive("quantity", self.quantity)
        if self.order_type == LIMIT:
            _finite_positive("price", self.price)
        elif self.price is not None:
            raise ContractViolation("only a LIMIT order carries a price")
        if self.order_type in (STOP_MARKET, TAKE_PROFIT_MARKET):
            _finite_positive("stop_price", self.stop_price)
            if not (self.reduce_only or self.close_position):
                raise ContractViolation("a trigger order of this contract only reduces or closes a position")
        elif self.stop_price is not None:
            raise ContractViolation("only a trigger order carries a stop price")

    @property
    def protective(self) -> bool:
        return self.order_type in (STOP_MARKET, TAKE_PROFIT_MARKET)


@dataclass(frozen=True)
class OrderInfo:
    venue: str
    symbol: str
    client_order_id: Optional[str]
    venue_order_id: Optional[str]
    status: str
    venue_status: str
    side: str
    order_type: str
    quantity: Optional[float]
    filled_quantity: float
    average_price: Optional[float]
    stop_price: Optional[float]
    reduce_only: bool
    close_position: bool
    updated_at_ms: int

    def __post_init__(self) -> None:
        _one_of("status", self.status, ORDER_STATUSES)
        _one_of("side", self.side, (BUY, SELL))
        _finite_positive("filled_quantity", self.filled_quantity, allow_zero=True)
        if not (self.client_order_id or self.venue_order_id):
            raise ContractViolation("an order without any identity cannot be reconciled")

    @property
    def open(self) -> bool:
        return self.status in OPEN_STATUSES

    @property
    def final(self) -> bool:
        return self.status in FINAL_STATUSES


@dataclass(frozen=True)
class FillInfo:
    venue: str
    symbol: str
    trade_id: str
    venue_order_id: Optional[str]
    client_order_id: Optional[str]
    side: str
    quantity: float
    price: float
    fee: Optional[float]                # positive = paid; None = the venue did not report it
    fee_asset: Optional[str]
    realized_pnl: Optional[float]
    is_maker: Optional[bool]
    time_ms: int

    def __post_init__(self) -> None:
        _one_of("side", self.side, (BUY, SELL))
        _finite_positive("quantity", self.quantity)
        _finite_positive("price", self.price)
        if not self.trade_id:
            raise ContractViolation("a fill without a trade id cannot be deduplicated")


@dataclass(frozen=True)
class FeeRates:
    venue: str
    symbol: str
    maker: float
    taker: float
    source: str                         # ACCOUNT (the account's own tier) | SCHEDULE (published default)


@dataclass(frozen=True)
class FundingInfo:
    venue: str
    symbol: str
    rate: Optional[float]               # the rate of the current interval, as a fraction
    next_funding_ms: Optional[int]
    interval_minutes: Optional[int]
    mark_price: Optional[float]
    observed_at_ms: int


@dataclass(frozen=True)
class ServerTime:
    server_ms: int
    local_ms: int

    @property
    def offset_ms(self) -> int:
        return int(self.server_ms) - int(self.local_ms)


@dataclass(frozen=True)
class RateLimitObservation:
    """What the venue said about the budget in its last response. ``None`` = the venue did not say."""
    venue: str
    used_weight: Optional[float]
    limit: Optional[float]
    window_seconds: Optional[int]
    retry_after_s: Optional[float]
    banned_until_ms: Optional[int]
    observed_at_ms: int
    scope: str = "IP"                   # IP | UID


@dataclass(frozen=True)
class UserStreamSpec:
    """How to open the authenticated push stream (Step 4.4). The token is a short-lived stream key issued by
    the venue or an authentication frame the transport sends; it is never logged or stored."""
    venue: str
    url: str
    auth_mode: str                      # LISTEN_KEY_IN_URL | AUTH_FRAME
    keepalive_seconds: int
    expires_after_seconds: Optional[int]
    subscribe_frames: Tuple[Mapping[str, Any], ...] = field(default_factory=tuple)

    def __repr__(self) -> str:  # the URL may carry a listen key
        return f"UserStreamSpec(venue={self.venue!r}, auth_mode={self.auth_mode!r})"


# -------------------------------------------------------------------------------------------------- protocol
@runtime_checkable
class VenueAdapter(Protocol):
    """One connected customer account on one venue and one environment."""

    venue: str
    environment: str
    adapter_version: str

    # -- read only ----------------------------------------------------------------------------------------
    def capabilities(self) -> AdapterCapabilities: ...
    def server_time(self) -> ServerTime: ...
    def instrument_rules(self, symbols: Optional[Sequence[str]] = None) -> Mapping[str, InstrumentRules]: ...
    def balance(self) -> AccountBalance: ...
    def positions(self, symbol: Optional[str] = None) -> Tuple[PositionInfo, ...]: ...
    def open_orders(self, symbol: Optional[str] = None) -> Tuple[OrderInfo, ...]: ...
    def order(self, symbol: str, *, client_order_id: Optional[str] = None,
              venue_order_id: Optional[str] = None) -> OrderInfo: ...
    def fills(self, symbol: str, *, since_ms: Optional[int] = None,
              venue_order_id: Optional[str] = None) -> Tuple[FillInfo, ...]: ...
    def fee_rates(self, symbol: str) -> FeeRates: ...
    def funding(self, symbol: str) -> FundingInfo: ...
    def rate_limit(self) -> RateLimitObservation: ...
    def user_stream(self) -> UserStreamSpec: ...

    # -- mutations ----------------------------------------------------------------------------------------
    def set_leverage(self, symbol: str, leverage: int) -> int: ...
    def place_order(self, request: OrderRequest) -> OrderInfo: ...
    def cancel_order(self, symbol: str, *, client_order_id: Optional[str] = None,
                     venue_order_id: Optional[str] = None) -> OrderInfo: ...
    def amend_stop(self, symbol: str, *, client_order_id: str, new_stop_price: float) -> OrderInfo: ...


#: the methods that change something at the venue; everything else is a read
MUTATING_METHODS: Tuple[str, ...] = ("set_leverage", "place_order", "cancel_order", "amend_stop")
READ_METHODS: Tuple[str, ...] = ("capabilities", "server_time", "instrument_rules", "balance", "positions",
                                 "open_orders", "order", "fills", "fee_rates", "funding", "rate_limit", "user_stream")


def round_down_to_step(value: float, step: float) -> float:
    """Largest multiple of ``step`` not above ``value`` (never rounds an order up)."""
    _finite_positive("step", step)
    units = math.floor(float(value) / float(step) + 1e-9)
    decimals = max(0, -int(math.floor(math.log10(float(step)))) + 2)
    return round(units * float(step), decimals)


# ------------------------------------------------------------------------------------- client order identity
#: characters every venue of this contract accepts in a client order id
CLIENT_ORDER_ID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
#: an id of at most this many characters of the lower-case part of the alphabet reaches every venue unchanged
PORTABLE_CLIENT_ORDER_ID_LENGTH = 32


def venue_client_order_id(client_order_id: str, *, max_length: int, lowercase: bool = False) -> str:
    """The identifier an adapter sends to its venue for the caller's ``client_order_id``.

    The id travels unchanged when the venue can store it exactly: at most ``max_length`` characters of
    ``CLIENT_ORDER_ID_ALPHABET`` and, where the venue folds case (``lowercase``), no upper-case letter.
    Any other id is replaced by ``cf`` followed by the first 30 hexadecimal digits of its SHA-256: 32
    lower-case characters, valid on every venue. The mapping is a pure function of the caller's id, so the
    same request always carries the same venue id and a lookup after a restart finds the order. An id is
    never truncated and never case-folded: both would let two different orders share one venue id."""
    cid = str(client_order_id or "")
    if not cid.strip():
        raise ContractViolation("every order carries a client order id")
    portable = len(cid) <= int(max_length) and all(ch in CLIENT_ORDER_ID_ALPHABET for ch in cid)
    if portable and not (lowercase and cid != cid.lower()):
        return cid
    return "cf" + hashlib.sha256(cid.encode("utf-8")).hexdigest()[:30]


__all__ = [n for n in dir() if n.isupper() or n in (
    "AdapterError", "ContractViolation", "AdapterCapabilities", "InstrumentRules", "AccountBalance", "PositionInfo",
    "OrderRequest", "OrderInfo", "FillInfo", "FeeRates", "FundingInfo", "ServerTime", "RateLimitObservation",
    "UserStreamSpec", "VenueAdapter", "canonical_symbol", "round_down_to_step", "venue_client_order_id")]
