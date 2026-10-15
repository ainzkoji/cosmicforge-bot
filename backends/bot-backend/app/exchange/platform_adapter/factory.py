"""How a venue adapter is built, and the part of the translation the three adapters share (Step 4.1).

``adapter_for(client, venue, environment)`` is the only way an adapter of this package is created. It takes a
raw venue client that the connection resolver has already built for ONE connected customer account, and the
environment of that account. An adapter never receives, reads, stores or prints a credential: signing stays
inside the raw client, and the adapter only calls it. There is no platform-owned trading account, and the
factory never returns the in-memory ``FakeVenue``.

The environment is checked, not trusted: the client's host must be the canonical host of that venue and
environment (``shared_lib.broker.environment``), otherwise no adapter is built. A DEMO adapter can therefore
not be pointed at a LIVE account by a wrong argument.

``AdapterBase`` holds what does not depend on the venue:

* **Failure translation.** Every raw-client call goes through ``_call``. What comes out is an
  ``AdapterError`` and nothing else, raised with neither cause nor context, because the raw text can carry
  a signed URL or a request header.

  - HTTP 418 is ``BANNED`` and HTTP 429 is ``RATE_LIMITED``. The adapter never sleeps and never sends the
    request again.
  - A mutation whose outcome cannot be proved is ``NETWORK_UNCERTAIN``: a read timeout, a lost connection,
    HTTP 5xx or 408, an unreadable body, a venue code that means "the server does not know". The caller
    must look the order up before doing anything else. Only a connect timeout proves that nothing was sent
    and is ``VENUE_UNAVAILABLE``. The same events on a read are ``VENUE_UNAVAILABLE`` or
    ``MALFORMED_RESPONSE``: a read changes nothing.
  - A venue code is mapped only through the adapter's own table; anything else is ``UNMAPPED_VENUE_ERROR``
    carrying the venue code.
  - The platform's own refusals (order submission disabled, entry authority missing) are
    ``PERMISSION_DENIED``: the request was never sent.

* **Idempotent creation.** Before an order is created the venue is asked for an order with that client order
  id. An order that exists is returned (or ``DUPLICATE_CLIENT_ORDER_ID`` when it is a different order);
  nothing is sent twice. A venue that itself refuses the id as a duplicate is asked again for the order.

* **Truthful values.** A number the venue did not report is ``None``; an order state the adapter cannot map
  is ``UNKNOWN``; a reply that only acknowledges is reported as an acknowledgement, never as a fill.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests

from shared_lib.broker.capabilities import Capability, CapabilityState, declared_profile
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
from shared_lib.core.security.redaction import redact_text

from . import contract as c
from .contract import (AdapterCapabilities, AdapterError, FillInfo, InstrumentRules, OrderInfo, OrderRequest,
                       PositionInfo, RateLimitObservation, ServerTime)

ADAPTER_VERSIONS: Mapping[str, str] = {
    c.BINANCE: "binance-usdm-1.1.0", c.BYBIT: "bybit-linear-1.1.0", c.BINGX: "bingx-swap-1.1.0"}

_WEEK_MS = 7 * 86_400_000
#: fills(since_ms) is read in windows of one week (the longest range the three venues document for a fill
#: query); a request older than this many windows is refused rather than answered slowly or partially
MAX_FILL_WINDOWS = 13
#: refusals the platform itself raises inside a raw client before anything is sent
_PLATFORM_REFUSALS = ("CATI_ENTRY_AUTHORITY_REQUIRED", "BROKER_ENVIRONMENT_MISMATCH")
_MALFORMED = (KeyError, IndexError, TypeError, ValueError, AttributeError, ArithmeticError)
_STATUSES: Mapping[str, str] = {
    "NEW": c.NEW, "PARTIALLY_FILLED": c.PARTIALLY_FILLED, "FILLED": c.FILLED, "CANCELED": c.CANCELED,
    "REJECTED": c.REJECTED, "EXPIRED": c.EXPIRED, "EXPIRED_IN_MATCH": c.EXPIRED}


def number(value: Any) -> Optional[float]:
    """A finite float, or None when the venue did not report one."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def positive(value: Any) -> Optional[float]:
    """A finite float above zero, or None: venues print 0 for "not set" (no stop, no average price yet)."""
    out = number(value)
    return out if out is not None and out > 0 else None


def decimal_text(value: Any) -> str:
    """A number as plain decimal text (``1e-05`` is not a quantity any venue accepts)."""
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except InvalidOperation:
        raise c.ContractViolation(f"not a number: {value!r}") from None


def truthy(value: Any) -> bool:
    return value is True or str(value).strip().lower() == "true"


def record_hash(record: Any) -> str:
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


_IDENTIFIER = re.compile(r"[A-Za-z0-9_\-]{16,}")


def _mask_identifier(match: "re.Match[str]") -> str:
    run = match.group()
    return "***" if any(ch.isdigit() for ch in run) and any(ch.isalpha() for ch in run) else run


def safe_text(text: Any) -> str:
    """Venue text for an error message: one line, redacted, bounded.

    The adapter never holds a credential, so it cannot look for one. Instead every long run that mixes letters
    and digits is masked: that is what a key, a signature or an echoed identifier looks like, and no sentence a
    venue writes needs one to be understood."""
    return _IDENTIFIER.sub(_mask_identifier, redact_text(" ".join(str(text or "").split())))[:240]


def json_error(body: str, code_key: str, message_key: str) -> Tuple[Optional[int], str]:
    """``(code, message)`` of a JSON error body, ``(None, "")`` when the body is not one."""
    try:
        data = json.loads(body)
    except ValueError:
        return None, ""
    if not isinstance(data, Mapping):
        return None, ""
    code = data.get(code_key)
    return (code if isinstance(code, int) and not isinstance(code, bool) else None), str(data.get(message_key) or "")


class AdapterBase:
    """One connected customer account on one venue and one environment. See the module docstring."""

    venue: str = ""
    #: venue error code -> contract error code (each adapter states its evidence beside its table)
    _CODES: Mapping[int, str] = {}
    #: HTTP status -> contract error code, used when the body carried no mapped code
    _STATUS_CODES: Mapping[int, str] = {401: c.AUTH_FAILED}
    #: venue codes that mean "the server failed or timed out": the outcome of a mutation is not known
    _OUTCOME_UNKNOWN: frozenset = frozenset()
    _CID_MAX_LENGTH = 36
    _CID_LOWERCASE = False
    _RATE_SCOPE = "IP"
    #: capability flags of the venue (``AdapterCapabilities`` field -> value) and the notes that qualify them
    _CAPABILITIES: Mapping[str, bool] = {}
    _NOTES: Tuple[str, ...] = ()

    def __init__(self, client: Any, environment: str, *, clock: Optional[Callable[[], int]] = None) -> None:
        if environment not in c.ENVIRONMENTS:
            raise c.ContractViolation(f"environment {environment!r} is not one of {c.ENVIRONMENTS}")
        expected = resolve_base_url(self.venue, BrokerEnvironment(environment.lower()))
        if str(getattr(client, "base_url", "") or "").rstrip("/") != expected.rstrip("/"):
            raise c.ContractViolation(f"{self.venue}: the client is not connected to the {environment} host")
        self._client, self.environment = client, environment
        self.adapter_version = ADAPTER_VERSIONS[self.venue]
        self._clock = clock
        #: canonical symbol -> venue symbol, learned from the venue's own instrument list
        self._venue_symbols: Dict[str, str] = {}
        #: client order id -> (canonical symbol, venue order id) of protective orders the venue stores no id for
        self._stop_ids: Dict[str, Tuple[str, str]] = {}

    def __repr__(self) -> str:
        return (f"{type(self).__name__}(venue={self.venue!r}, environment={self.environment!r}, "
                f"adapter_version={self.adapter_version!r})")

    # ------------------------------------------------------------------------------------------- plumbing
    def _now(self) -> int:
        return int(self._clock()) if self._clock is not None else int(time.time() * 1000)

    def _error(self, code: str, message: str = "", **kw: Any) -> AdapterError:
        return AdapterError(code, safe_text(message), venue=self.venue, **kw)

    def _call(self, fn: Callable[..., Any], *args: Any, mutation: bool = False, **kwargs: Any) -> Any:
        """Call the raw client. Whatever fails comes out as an ``AdapterError`` that carries neither a cause nor
        a context: the translated error is raised after the handler has ended, so the raw exception (whose
        text can name a signed URL or a request header) is not attached to it in any way."""
        try:
            return fn(*args, **kwargs)
        except AdapterError:
            raise
        except Exception as exc:
            error = self._translate(exc, mutation)
        raise error

    def _translate(self, exc: BaseException, mutation: bool) -> AdapterError:
        if isinstance(exc, PermissionError):
            return self._error(c.PERMISSION_DENIED, f"the platform refused to send it: {type(exc).__name__}")
        transport = self._transport_cause(exc)
        if transport is not None:
            return self._transport_error(transport, mutation)
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            return self._fail(int(exc.response.status_code), None, "", mutation)
        classified = self._classify(exc, mutation)
        if classified is not None:
            return classified
        if isinstance(exc, ValueError) and str(exc) in _PLATFORM_REFUSALS:
            return self._error(c.PERMISSION_DENIED, f"the platform refused to send it: {exc}")
        if mutation:
            return self._error(c.NETWORK_UNCERTAIN, f"the outcome is not known ({type(exc).__name__})")
        if isinstance(exc, _MALFORMED):
            return self._error(c.MALFORMED_RESPONSE, f"the venue's answer could not be read ({type(exc).__name__})")
        return self._error(c.UNMAPPED_VENUE_ERROR, f"unrecognised failure ({type(exc).__name__})")

    def _transport_cause(self, exc: BaseException) -> Optional[BaseException]:
        """The transport exception behind ``exc``, when there is one."""
        return exc if isinstance(exc, (requests.RequestException, TimeoutError, ConnectionError)) else None

    def _transport_error(self, exc: BaseException, mutation: bool) -> AdapterError:
        kind = type(exc).__name__
        if isinstance(exc, requests.exceptions.JSONDecodeError):
            # the venue answered with a body that is not JSON: for a mutation that proves nothing either way
            return self._error(c.NETWORK_UNCERTAIN if mutation else c.MALFORMED_RESPONSE, f"unreadable answer ({kind})")
        if isinstance(exc, requests.exceptions.ConnectTimeout):
            return self._error(c.VENUE_UNAVAILABLE, f"no connection was made ({kind})")
        return self._unanswered(mutation, kind)

    def _unanswered(self, mutation: bool, detail: str, **kw: Any) -> AdapterError:
        if mutation:
            return self._error(c.NETWORK_UNCERTAIN, f"the request may have reached the venue ({detail})", **kw)
        return self._error(c.VENUE_UNAVAILABLE, f"the venue did not answer ({detail})", **kw)

    def _classify(self, exc: BaseException, mutation: bool) -> Optional[AdapterError]:
        """Read the raw client's own error text. Each adapter knows the surfaces of its client."""
        raise NotImplementedError

    def _fail(self, status: Optional[int], code: Optional[int], message: str, mutation: bool) -> AdapterError:
        """The contract error for a venue answer: an HTTP status and/or a venue code."""
        text = message or (f"HTTP {status}" if status is not None else "")
        kw: Dict[str, Any] = {"venue_code": None if code is None else str(code), "http_status": status}
        if status == 418:
            return self._error(c.BANNED, text, banned_until_ms=self._banned_until_ms(), **kw)
        if status == 429:
            return self._error(c.RATE_LIMITED, text, retry_after_s=self._retry_after_s(), **kw)
        if (status is not None and (status >= 500 or status == 408)) or code in self._OUTCOME_UNKNOWN:
            return self._unanswered(mutation, text or "server error", **kw)
        mapped = self._CODES.get(code) if code is not None else None
        mapped = mapped or self._STATUS_CODES.get(status) or c.UNMAPPED_VENUE_ERROR
        if mapped == c.RATE_LIMITED:
            kw["retry_after_s"] = self._retry_after_s()
        elif mapped == c.BANNED:
            kw["banned_until_ms"] = self._banned_until_ms()
        return self._error(mapped, text, **kw)

    def _retry_after_s(self) -> Optional[float]:
        """Seconds the venue asked for, when the raw client kept them (it keeps none unless overridden)."""
        return None

    def _banned_until_ms(self) -> Optional[int]:
        return None

    def _used_weight(self) -> Optional[float]:
        return None

    def _unsupported(self, message: str) -> AdapterError:
        return self._error(c.UNSUPPORTED, message)

    def _unconfirmed(self, what: str) -> AdapterError:
        """The venue answered a mutation without an error and without the ``what`` that would confirm it."""
        return self._error(c.NETWORK_UNCERTAIN, f"the venue's answer carried no {what}: the outcome is not known")

    def _mapping(self, value: Any, what: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise self._error(c.MALFORMED_RESPONSE, f"{what}: the venue did not answer with a record")
        return value

    @staticmethod
    def _parsed(parser: Callable[..., Any], record: Any, **kw: Any) -> Any:
        """The repository's discovery parser applied to one venue record; None when the record defeats it."""
        try:
            return parser(record, **kw)
        except _MALFORMED:
            return None

    def _need(self, record: Any, key: str, what: str) -> float:
        """A number the contract cannot do without: its absence is a malformed answer, never a zero."""
        value = number(record.get(key)) if isinstance(record, Mapping) else None
        if value is None:
            raise self._error(c.MALFORMED_RESPONSE, f"{what}: the venue did not report {key}")
        return value

    def _rows(self, value: Any, what: str) -> List[Any]:
        if not isinstance(value, list):
            raise self._error(c.MALFORMED_RESPONSE, f"{what}: the venue did not answer with a list")
        return value

    def _time_ms(self, value: Any, what: str) -> int:
        """Epoch milliseconds from a venue timestamp (a number, or ISO-8601 text)."""
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
        try:
            moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            raise self._error(c.MALFORMED_RESPONSE, f"{what}: unreadable time") from None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return int(moment.timestamp() * 1000)

    # ---------------------------------------------------------------------------------------------- symbols
    def _to_venue(self, symbol: str) -> str:
        symbol = c.canonical_symbol(symbol)
        return self._venue_symbols.get(symbol) or self._default_venue_symbol(symbol)

    def _default_venue_symbol(self, symbol: str) -> str:
        return symbol

    def _client_id(self, client_order_id: str) -> str:
        return c.venue_client_order_id(client_order_id, max_length=self._CID_MAX_LENGTH, lowercase=self._CID_LOWERCASE)

    @staticmethod
    def _identity(client_order_id: Optional[str], venue_order_id: Optional[str]) -> None:
        if not (client_order_id or venue_order_id):
            raise c.ContractViolation("an order is looked up by its client order id or its venue order id")

    def _publish(self, rules: Iterable[Optional[InstrumentRules]],
                 symbols: Optional[Sequence[str]]) -> Mapping[str, InstrumentRules]:
        """Rules by canonical symbol. A canonical name claimed by two venue symbols is ambiguous and is left out."""
        out: Dict[str, InstrumentRules] = {}
        ambiguous = set()
        for rule in rules:
            if rule is None or rule.symbol in ambiguous:
                continue
            known = out.get(rule.symbol)
            if known is not None and known.venue_symbol != rule.venue_symbol:
                ambiguous.add(rule.symbol)
                del out[rule.symbol]
            else:
                out[rule.symbol] = rule
        if not out:
            # an empty list is a failed read, never "the venue delisted everything"
            raise self._error(c.MALFORMED_RESPONSE, "the venue returned no usable instrument")
        self._venue_symbols = {s: r.venue_symbol for s, r in out.items()}
        if symbols is None:
            return out
        wanted = {c.canonical_symbol(s) for s in symbols}
        return {s: r for s, r in out.items() if s in wanted}

    def _rules(self, record: Any, instrument: Any, *, base: str, quote: str, venue_status: str,
               market_max: Optional[float] = None) -> Optional[InstrumentRules]:
        """Contract rules from the repository's own discovery parser output. None: not expressible here."""
        if instrument is None:
            return None
        try:
            return InstrumentRules(
                venue=self.venue, environment=self.environment, symbol=c.canonical_symbol(instrument.venue_symbol),
                venue_symbol=instrument.venue_symbol, status=c.TRADING if instrument.api_tradable else c.NOT_TRADING,
                venue_status=venue_status, base_asset=str(base or "").upper(), quote_asset=str(quote or "").upper(),
                margin_asset=instrument.settlement_asset, price_tick=instrument.tick_size,
                quantity_step=instrument.qty_step, min_quantity=instrument.min_qty, max_quantity=instrument.max_qty,
                min_notional=instrument.min_notional, max_leverage=instrument.max_leverage,
                contract_size=float(instrument.contract_multiplier), fetched_at_ms=self._now(),
                market_max_quantity=market_max, raw_hash=record_hash(record))
        except c.ContractViolation:
            return None

    # ----------------------------------------------------------------------------------------------- orders
    def _order_info(self, view: Mapping[str, Any], *, symbol: Optional[str] = None,
                    client_order_id: Optional[str] = None, venue_status: Optional[str] = None,
                    venue_order_id: Optional[str] = None) -> OrderInfo:
        """An order from a Binance-shaped record (all three raw clients normalise to that shape)."""
        side = str(view.get("side") or "").upper()
        if side not in (c.BUY, c.SELL):
            raise self._error(c.MALFORMED_RESPONSE, "the venue returned an order without a side")
        reported = view.get("status")
        order_id = venue_order_id or view.get("orderId")
        close_position = truthy(view.get("closePosition"))
        try:
            return OrderInfo(
                venue=self.venue, symbol=symbol or c.canonical_symbol(view.get("symbol")),
                client_order_id=client_order_id or view.get("clientOrderId") or None,
                venue_order_id=None if order_id in (None, "") else str(order_id),
                status=_STATUSES.get(str(reported).upper(), c.UNKNOWN) if reported else c.UNKNOWN,
                venue_status=str(venue_status if venue_status is not None else (reported or "")),
                side=side, order_type=str(view.get("type") or "").upper(),
                quantity=None if close_position else positive(view.get("origQty")),
                filled_quantity=number(view.get("executedQty")) or 0.0, average_price=positive(view.get("avgPrice")),
                stop_price=positive(view.get("stopPrice")), reduce_only=truthy(view.get("reduceOnly")),
                close_position=close_position, updated_at_ms=int(number(view.get("updateTime")) or 0))
        except c.ContractViolation as bad:
            raise self._error(c.MALFORMED_RESPONSE, f"an order this contract cannot hold: {bad}") from None

    def _position(self, venue_symbol: Any, signed_quantity: float, *, entry: Any, mark: Any, unrealized: Any,
                  leverage: Any, margin_mode: Optional[str], position_mode: Optional[str]) -> PositionInfo:
        try:
            return PositionInfo(
                venue=self.venue, symbol=c.canonical_symbol(venue_symbol),
                side=c.LONG if signed_quantity > 0 else c.SHORT, quantity=abs(float(signed_quantity)),
                entry_price=positive(entry), mark_price=positive(mark), unrealized_pnl=number(unrealized),
                leverage=positive(leverage), margin_mode=margin_mode, position_mode=position_mode,
                observed_at_ms=self._now())
        except c.ContractViolation as bad:
            raise self._error(c.MALFORMED_RESPONSE, f"a position this contract cannot hold: {bad}") from None

    def _fill_info(self, symbol: str, *, trade_id: Any, venue_order_id: Any, client_order_id: Any, side: Any,
                   quantity: Any, price: Any, fee: Optional[float], fee_asset: Any, realized_pnl: Optional[float],
                   is_maker: Optional[bool], time: Any) -> FillInfo:
        try:
            return FillInfo(
                venue=self.venue, symbol=symbol, trade_id="" if trade_id in (None, "") else str(trade_id),
                venue_order_id=None if venue_order_id in (None, "") else str(venue_order_id),
                client_order_id=client_order_id or None, side=str(side or "").upper(), quantity=number(quantity),
                price=number(price), fee=fee, fee_asset=str(fee_asset) if fee_asset else None,
                realized_pnl=realized_pnl, is_maker=is_maker, time_ms=self._time_ms(time, "fill"))
        except c.ContractViolation as bad:
            raise self._error(c.MALFORMED_RESPONSE, f"a fill this contract cannot hold: {bad}") from None

    def _acknowledged(self, request: OrderRequest, venue_order_id: str, *, status: str, venue_status: str,
                      whole_position: bool = False) -> OrderInfo:
        """The order as the request describes it, for a venue that answered with an identity and nothing else.
        ``whole_position``: what was placed is the venue's whole-position stop, whatever form the request had."""
        whole = whole_position or request.close_position
        return OrderInfo(
            venue=self.venue, symbol=request.symbol, client_order_id=request.client_order_id,
            venue_order_id=str(venue_order_id), status=status, venue_status=venue_status, side=request.side,
            order_type=request.order_type, quantity=None if whole else request.quantity, filled_quantity=0.0,
            average_price=None, stop_price=request.stop_price, reduce_only=request.reduce_only and not whole,
            close_position=whole, updated_at_ms=self._now())

    def _whole_position_stop(self, request: OrderRequest, signed_quantity: float) -> None:
        """Admit a stop on a venue whose stop is bound to the whole position (``signed_quantity`` is what is open).

        Such a stop needs a position it would close. A request that names a quantity is carried only when the
        quantity is the whole open position: the stop then covers exactly what was asked, and more if the
        position grows. A stop for part of a position does not exist here and is refused, never widened."""
        closes = (signed_quantity > 0 and request.side == c.SELL) or (signed_quantity < 0 and request.side == c.BUY)
        if not closes:
            raise self._error(c.REDUCE_ONLY_REJECTED, "there is no open position this stop could close")
        if not request.close_position and decimal_text(abs(signed_quantity)) != decimal_text(request.quantity):
            raise self._unsupported("a stop for part of a position is not carried for this venue: its stop is bound to "
                                    "the whole position")

    def _same_order(self, request: OrderRequest, found: OrderInfo) -> OrderInfo:
        """The existing order when it is this request's order, ``DUPLICATE_CLIENT_ORDER_ID`` when it is another."""
        if (found.symbol, found.side, found.order_type) != (request.symbol, request.side, request.order_type):
            raise self._error(c.DUPLICATE_CLIENT_ORDER_ID, "this client order id belongs to a different order")
        return found

    def _idempotent(self, request: OrderRequest, lookup: Callable[[], OrderInfo],
                    send: Callable[[], OrderInfo]) -> OrderInfo:
        """Create at most once: look the id up, send only when the venue holds no such order."""
        try:
            return self._same_order(request, lookup())
        except AdapterError as missing:
            if missing.code != c.ORDER_NOT_FOUND:
                raise
        try:
            return send()
        except AdapterError as refused:
            if refused.code != c.DUPLICATE_CLIENT_ORDER_ID:
                raise
            try:
                found = lookup()                         # a first send whose answer was lost
            except AdapterError:
                raise refused from None
            return self._same_order(request, found)

    def _read_back(self, lookup: Callable[[], OrderInfo], fallback: OrderInfo) -> OrderInfo:
        """One read of an order the venue has just acknowledged without stating its state. A read that fails
        does not undo the acknowledgement: the caller then gets ``fallback`` (state ``UNKNOWN``)."""
        try:
            return lookup()
        except AdapterError:
            return fallback

    # A venue that stores no client order id on a protective stop cannot answer a lookup by that id. The
    # adapter remembers which stop it placed for which id, for as long as it lives; after a restart the
    # caller uses the ``venue_order_id`` it persisted (``capabilities().stop_client_order_id`` is False).
    def _bind(self, client_order_id: str, symbol: str, venue_order_id: str) -> None:
        for known in [k for k, target in self._stop_ids.items() if target == (symbol, venue_order_id)]:
            del self._stop_ids[known]
        self._stop_ids[client_order_id] = (symbol, venue_order_id)

    def _unbind(self, symbol: str, venue_order_id: str) -> None:
        for known in [k for k, target in self._stop_ids.items() if target == (symbol, venue_order_id)]:
            del self._stop_ids[known]

    def _bound(self, client_order_id: Optional[str], symbol: str) -> Optional[str]:
        target = self._stop_ids.get(client_order_id or "")
        return target[1] if target is not None and target[0] == symbol else None

    def _bound_client_id(self, symbol: str, venue_order_id: Optional[str]) -> Optional[str]:
        return next((k for k, target in self._stop_ids.items() if target == (symbol, venue_order_id)), None)

    def _entry_only_gtc(self, request: OrderRequest) -> None:
        if request.time_in_force not in (None, "GTC"):
            raise self._unsupported(f"time in force {request.time_in_force!r} is not carried by this adapter (GTC only)")

    def _fill_windows(self, since_ms: Optional[int]) -> List[Tuple[Optional[int], Optional[int]]]:
        """Query ranges for ``fills``: the venue's default recent window, or weeks from ``since_ms`` to now."""
        if since_ms is None:
            return [(None, None)]
        now, start = self._now(), int(since_ms)
        windows: List[Tuple[Optional[int], Optional[int]]] = []
        while start <= now:
            end = min(start + _WEEK_MS - 1, now)
            windows.append((start, end))
            start = end + 1
        if len(windows) > MAX_FILL_WINDOWS:
            raise self._unsupported(f"fills older than {MAX_FILL_WINDOWS} weeks must be reconciled from an account ledger")
        return windows

    # --------------------------------------------------------------------------- contract methods in common
    def capabilities(self) -> AdapterCapabilities:
        demo = declared_profile(self.venue).state(Capability.DEMO_ENVIRONMENT) == CapabilityState.SUPPORTED
        from .stream_credentials import supported
        capabilities = {**self._CAPABILITIES, "user_stream": supported(self.venue, self.environment)}
        return AdapterCapabilities(
            venue=self.venue, environment=self.environment, demo_environment=demo,
            client_order_id_max_length=self._CID_MAX_LENGTH, notes=self._NOTES, **capabilities)

    def server_time(self) -> ServerTime:
        before = self._now()
        server = self._call(self._client.server_time)
        try:
            return ServerTime(server_ms=int(server), local_ms=(before + self._now()) // 2)
        except (TypeError, ValueError):
            raise self._error(c.MALFORMED_RESPONSE, "the venue's time could not be read") from None

    def rate_limit(self) -> RateLimitObservation:
        """What the raw client kept of the venue's last statement about the budget. Nothing is sent."""
        return RateLimitObservation(
            venue=self.venue, used_weight=self._used_weight(), limit=None,
            window_seconds=60 if self._used_weight() is not None else None, retry_after_s=self._retry_after_s(),
            banned_until_ms=self._banned_until_ms(), observed_at_ms=self._now(), scope=self._RATE_SCOPE)

    def user_stream(self) -> Any:
        from .stream_credentials import specification
        return specification(self)

    def amend_stop(self, symbol: str, *, client_order_id: str, new_stop_price: float) -> OrderInfo:
        raise self._unsupported("this venue's stop cannot be changed in place: cancel it and place a new one")


def adapter_for(client: Any, venue: str, environment: str, *, clock: Optional[Callable[[], int]] = None) -> AdapterBase:
    """The adapter of ``venue`` around an already-built raw client of one connected account.

    ``environment`` is ``LIVE`` or ``DEMO`` and must be the environment of the account the client was built
    for; the client's host is checked against it. ``clock`` returns epoch milliseconds (tests)."""
    from .binance import BinanceAdapter
    from .bingx import BingXAdapter
    from .bybit import BybitAdapter

    adapters = {c.BINANCE: BinanceAdapter, c.BYBIT: BybitAdapter, c.BINGX: BingXAdapter}
    key = str(venue or "").strip().lower()
    if key not in adapters:
        raise c.ContractViolation(f"no venue adapter for {venue!r}; supported: {c.VENUES}")
    return adapters[key](client, str(environment or "").strip().upper(), clock=clock)


__all__ = ["ADAPTER_VERSIONS", "MAX_FILL_WINDOWS", "AdapterBase", "adapter_for"]
