"""Authenticated user-data streams of the three venues (Section H, Step 4.4).

**The stream is a hint and REST is the truth.** A frame tells this module that something changed and, where
its shape is documented, what; it never settles a disputed position or an unknown order. On connect, on
reconnect, after a restart, on a detected gap, on an order nobody announced, on a frame that cannot be read
and while the stream is down, ``reconcile.RestReconciler`` asks the venue and its answer replaces the hints.

What is here:

* ``StreamTransport``: the connection is injected. This module imports no websocket library; the transport
  that speaks to a real venue is not part of this build (see ``docs/exchanges/EXCHANGE_OPERATIONS.md``).
* Parsers from venue frames to normalised events. A field is read only where the venue's documentation of the
  frame is certain; ``VERIFICATION`` says which shapes those are. A frame whose shape is not certain is not
  parsed at all: it becomes a ``RECONCILE_HINT`` and the venue is asked. No field is ever guessed.
* ``UserStreamManager``: one connection per account, held under a lease in the database so two workers never
  open two; subscription, authentication acknowledgement, heartbeat, listen-key renewal and expiry, reconnect
  with bounded backoff, duplicate suppression through the durable fill ledger, gap detection, and a health
  state other code reads to decide whether new entries are safe.

Gap detection. None of the three user streams documents a gap-free sequence number, so a gap is found by what
it leaves behind: a lost connection, silence longer than the heartbeat allows, or an order whose cumulative
filled quantity is larger than the fills received for it. Bybit's ``seq`` orders position updates; it does not
count them.

Secrets. A stream URL can carry a listen key and a subscribe frame can carry a signature. Neither is logged,
stored or put in an exception by this module: the specification lives in memory for the life of one
connection, and a transport failure is recorded by the name of its exception type only.
"""
from __future__ import annotations

import json
import math
import zlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Set, Tuple, runtime_checkable

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter import reconcile as R

from shared_lib.persistence.cati_exchange_schema import STREAM_STATE_TABLE

LISTEN_KEY_IN_URL, AUTH_FRAME = "LISTEN_KEY_IN_URL", "AUTH_FRAME"

# ----------------------------------------------------------------------------------------------- health
LIVE = "LIVE"                               # connected, acknowledged, reconciled, and heard from recently
STALE = "STALE"                             # the connection went silent: nothing it said last can be trusted
DISCONNECTED = "DISCONNECTED"
RECONCILE_REQUIRED = "RECONCILE_REQUIRED"   # connected, but the venue has not yet confirmed the state
HEALTH_STATES: Tuple[str, ...] = (LIVE, STALE, DISCONNECTED, RECONCILE_REQUIRED)

# ----------------------------------------------------------------------------------------------- events
ORDER, FILL, POSITION, BALANCE, CONTROL = "ORDER", "FILL", "POSITION", "BALANCE", "CONTROL"
LISTEN_KEY_EXPIRED, AUTH_OK, AUTH_FAILED, SUBSCRIBED, SUBSCRIBE_FAILED, PONG, RECONCILE_HINT = (
    "LISTEN_KEY_EXPIRED", "AUTH_OK", "AUTH_FAILED", "SUBSCRIBED", "SUBSCRIBE_FAILED", "PONG", "RECONCILE_HINT")

VERIFIED_BY_DOC, UNVERIFIED = "VERIFIED_BY_DOC", "UNVERIFIED"
#: which frame shapes are read field by field, and which only wake the reconciler
VERIFICATION: Mapping[str, str] = {
    "binance.ORDER_TRADE_UPDATE": VERIFIED_BY_DOC, "binance.ACCOUNT_UPDATE": VERIFIED_BY_DOC,
    "binance.listenKeyExpired": VERIFIED_BY_DOC, "binance.ALGO_UPDATE": UNVERIFIED,
    "bybit.op": VERIFIED_BY_DOC, "bybit.order": VERIFIED_BY_DOC, "bybit.execution": VERIFIED_BY_DOC,
    "bybit.position": VERIFIED_BY_DOC, "bybit.wallet": VERIFIED_BY_DOC, "bybit.execution.fast": UNVERIFIED,
    "bingx.ORDER_TRADE_UPDATE": UNVERIFIED, "bingx.ACCOUNT_UPDATE": UNVERIFIED, "bingx.listenKeyExpired": UNVERIFIED,
    "bingx.gzip_and_text_ping": UNVERIFIED,
}

#: a decompressed frame larger than this is not a user-data frame
MAX_FRAME_BYTES = 1_048_576
_HEARTBEAT, _TEXT, _JSON, _MALFORMED = "HEARTBEAT", "TEXT", "JSON", "MALFORMED"


class StreamClosed(Exception):
    """The transport lost or closed the connection. A transport raises it without a URL in its message."""


class TransportHeartbeat:
    """What a transport returns from ``receive`` when it saw a protocol-level ping or pong."""

    def __repr__(self) -> str:
        return "HEARTBEAT"


HEARTBEAT = TransportHeartbeat()


@runtime_checkable
class StreamTransport(Protocol):
    """One websocket-like connection. ``close`` is safe on a transport that is not connected."""

    def connect(self, spec: C.UserStreamSpec) -> None: ...
    def send(self, frame: Any) -> None: ...
    def receive(self) -> Any: ...       # the next frame without waiting: str | bytes | Mapping | HEARTBEAT | None
    def ping(self) -> None: ...         # a protocol-level ping; its pong comes back from ``receive`` as HEARTBEAT
    def close(self) -> None: ...


@dataclass(frozen=True)
class OrderUpdate:
    venue: str
    symbol: str
    client_order_id: Optional[str]
    venue_order_id: str
    status: str                         # contract vocabulary; UNKNOWN when the venue's word is not mapped
    venue_status: str
    side: str
    filled_quantity: float              # cumulative
    average_price: Optional[float]
    event_time_ms: int


@dataclass(frozen=True)
class PositionUpdate:
    venue: str
    symbol: str
    side: str
    quantity: float
    entry_price: Optional[float]
    unrealized_pnl: Optional[float]
    event_time_ms: int
    sequence: Optional[int] = None      # orders updates where the venue provides one; never counts them


@dataclass(frozen=True)
class BalanceUpdate:
    venue: str
    asset: str
    wallet_balance: float
    event_time_ms: int


@dataclass(frozen=True)
class StreamEvent:
    kind: str
    venue: str
    order: Optional[OrderUpdate] = None
    fill: Optional[C.FillInfo] = None
    position: Optional[PositionUpdate] = None
    balance: Optional[BalanceUpdate] = None
    control: Optional[str] = None
    detail: str = ""


# ---------------------------------------------------------------------------------------------- decoding
def decode_frame(raw: Any) -> Tuple[str, Any]:
    """``(kind, payload)``: a transport heartbeat, a bare text frame, a JSON object, or malformed.

    Bytes that begin with the gzip magic number are decompressed first (bounded). That is done for every
    venue because it needs no knowledge of any: a frame either is gzip or it is not.
    """
    if raw is HEARTBEAT:
        return _HEARTBEAT, None
    if isinstance(raw, Mapping):
        return _JSON, raw
    if isinstance(raw, (bytes, bytearray)):
        data = bytes(raw)
        if data[:2] == b"\x1f\x8b":
            try:
                inflater = zlib.decompressobj(wbits=31)
                data = inflater.decompress(data, MAX_FRAME_BYTES)
            except zlib.error:
                return _MALFORMED, None
            if inflater.unconsumed_tail:
                return _MALFORMED, None
        try:
            raw = data.decode("utf-8")
        except UnicodeDecodeError:
            return _MALFORMED, None
    if not isinstance(raw, str):
        return _MALFORMED, None
    text = raw.strip()
    if text.lower() in ("ping", "pong"):
        return _TEXT, text
    try:
        payload = json.loads(text)
    except ValueError:
        return _MALFORMED, None
    return (_JSON, payload) if isinstance(payload, Mapping) else (_MALFORMED, None)


class _Malformed(Exception):
    """A frame of a known type lacks a field its documentation promises, or carries a value that cannot be one."""


def _number(source: Mapping[str, Any], key: str, *, optional: bool = False) -> Optional[float]:
    value = source.get(key)
    if value is None or value == "":
        if optional:
            return None
        raise _Malformed(key)
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise _Malformed(key) from None
    if not math.isfinite(number):
        raise _Malformed(key)
    return number


def _text(source: Mapping[str, Any], key: str, *, optional: bool = False) -> Optional[str]:
    value = source.get(key)
    if value is None or value == "":
        if optional:
            return None
        raise _Malformed(key)
    return str(value)


def _millis(source: Mapping[str, Any], key: str) -> int:
    number = _number(source, key)
    if number is None or number <= 0:
        raise _Malformed(key)
    return int(number)


def _positive(value: Optional[float]) -> Optional[float]:
    return value if value is not None and value > 0 else None


def _objects(value: Any, key: str) -> List[Mapping[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise _Malformed(key)
    return list(value)


def _hint(venue: str, detail: str) -> StreamEvent:
    label = "".join(ch for ch in str(detail)[:60] if ch.isalnum() or ch in "_.:")
    return StreamEvent(CONTROL, venue, control=RECONCILE_HINT, detail=label)


# ----------------------------------------------------------------------------------------------- Binance
_BINANCE_STATUS = {"NEW": C.NEW, "PARTIALLY_FILLED": C.PARTIALLY_FILLED, "FILLED": C.FILLED, "CANCELED": C.CANCELED,
                   "EXPIRED": C.EXPIRED, "EXPIRED_IN_MATCH": C.EXPIRED}
#: event types that announce no change of an order, a fill, a position or a balance of this platform's own
_BINANCE_IGNORED = ("TRADE_LITE", "MARGIN_CALL", "STRATEGY_UPDATE", "GRID_UPDATE")


def _binance_order(payload: Mapping[str, Any]) -> List[StreamEvent]:
    body = payload.get("o")
    if not isinstance(body, Mapping):
        raise _Malformed("o")
    symbol = C.canonical_symbol(_text(body, "s") or "")
    side = _text(body, "S")
    if side not in (C.BUY, C.SELL):
        raise _Malformed("S")
    venue_status = _text(body, "X") or ""
    order_id, client_id = _text(body, "i") or "", _text(body, "c", optional=True)
    events: List[StreamEvent] = []
    last_quantity = _number(body, "l", optional=True)
    if _text(body, "x") == "TRADE" and last_quantity is not None and last_quantity > 0:
        maker = body.get("m")
        events.append(StreamEvent(FILL, C.BINANCE, fill=C.FillInfo(
            venue=C.BINANCE, symbol=symbol, trade_id=_text(body, "t") or "", venue_order_id=order_id,
            client_order_id=client_id, side=side, quantity=last_quantity, price=_number(body, "L") or 0.0,
            fee=_number(body, "n", optional=True), fee_asset=_text(body, "N", optional=True),
            realized_pnl=_number(body, "rp", optional=True), is_maker=maker if isinstance(maker, bool) else None,
            time_ms=_millis(body, "T"))))
    events.append(StreamEvent(ORDER, C.BINANCE, order=OrderUpdate(
        C.BINANCE, symbol, client_id, order_id, _BINANCE_STATUS.get(venue_status, C.UNKNOWN), venue_status, side,
        _number(body, "z") or 0.0, _positive(_number(body, "ap", optional=True)), _millis(payload, "E"))))
    return events


def _binance_account(payload: Mapping[str, Any]) -> List[StreamEvent]:
    body = payload.get("a")
    if not isinstance(body, Mapping):
        raise _Malformed("a")
    event_ms = _millis(payload, "E")
    events = [StreamEvent(BALANCE, C.BINANCE, balance=BalanceUpdate(
        C.BINANCE, _text(item, "a") or "", _number(item, "wb") or 0.0, event_ms)) for item in _objects(body.get("B"), "B")]
    for item in _objects(body.get("P"), "P"):
        if _text(item, "ps", optional=True) not in (None, "BOTH"):
            # two positions on one symbol cannot be one position of this contract: ask the venue
            events.append(_hint(C.BINANCE, "HEDGE_MODE_POSITION"))
            continue
        amount = _number(item, "pa") or 0.0
        events.append(StreamEvent(POSITION, C.BINANCE, position=PositionUpdate(
            C.BINANCE, C.canonical_symbol(_text(item, "s") or ""),
            C.LONG if amount > 0 else C.SHORT if amount < 0 else C.FLAT, abs(amount),
            _positive(_number(item, "ep", optional=True)), _number(item, "up", optional=True), event_ms)))
    return events


def parse_binance_frame(payload: Mapping[str, Any]) -> List[StreamEvent]:
    """Binance USD-M futures user data stream: ``ORDER_TRADE_UPDATE``, ``ACCOUNT_UPDATE``, ``listenKeyExpired``.
    Any other event type that is not known to be irrelevant wakes the reconciler without being read."""
    kind = payload.get("e")
    if not isinstance(kind, str):
        return []  # an answer to a request, not an event
    try:
        if kind == "listenKeyExpired":
            return [StreamEvent(CONTROL, C.BINANCE, control=LISTEN_KEY_EXPIRED)]
        if kind == "ORDER_TRADE_UPDATE":
            return _binance_order(payload)
        if kind == "ACCOUNT_UPDATE":
            return _binance_account(payload)
    except (_Malformed, C.ContractViolation):
        return [_hint(C.BINANCE, f"MALFORMED:{kind}")]
    return [] if kind in _BINANCE_IGNORED else [_hint(C.BINANCE, f"UNPARSED:{kind}")]


# ------------------------------------------------------------------------------------------------- Bybit
_BYBIT_STATUS = {"New": C.NEW, "PartiallyFilled": C.PARTIALLY_FILLED, "Filled": C.FILLED, "Cancelled": C.CANCELED,
                 "PartiallyFilledCanceled": C.CANCELED, "Rejected": C.REJECTED, "Deactivated": C.CANCELED,
                 "Untriggered": C.NEW}
_BYBIT_SIDE = {"Buy": C.BUY, "Sell": C.SELL}
#: execution types that are a trade of the position; ``Funding`` is not a fill; anything else is asked about
_BYBIT_TRADES = ("Trade", "AdlTrade", "BustTrade", "BlockTrade")


def _bybit_side(item: Mapping[str, Any]) -> str:
    side = _BYBIT_SIDE.get(str(item.get("side")))
    if side is None:
        raise _Malformed("side")
    return side


def _bybit_order(item: Mapping[str, Any]) -> StreamEvent:
    venue_status = _text(item, "orderStatus") or ""
    return StreamEvent(ORDER, C.BYBIT, order=OrderUpdate(
        C.BYBIT, C.canonical_symbol(_text(item, "symbol") or ""), _text(item, "orderLinkId", optional=True),
        _text(item, "orderId") or "", _BYBIT_STATUS.get(venue_status, C.UNKNOWN), venue_status, _bybit_side(item),
        _number(item, "cumExecQty") or 0.0, _positive(_number(item, "avgPrice", optional=True)),
        _millis(item, "updatedTime")))


def _bybit_execution(item: Mapping[str, Any]) -> Optional[StreamEvent]:
    kind = _text(item, "execType")
    if kind == "Funding":
        return None
    if kind not in _BYBIT_TRADES:
        return _hint(C.BYBIT, f"UNPARSED:execType.{kind}")
    maker = item.get("isMaker")
    return StreamEvent(FILL, C.BYBIT, fill=C.FillInfo(
        venue=C.BYBIT, symbol=C.canonical_symbol(_text(item, "symbol") or ""), trade_id=_text(item, "execId") or "",
        venue_order_id=_text(item, "orderId", optional=True), client_order_id=_text(item, "orderLinkId", optional=True),
        side=_bybit_side(item), quantity=_number(item, "execQty") or 0.0, price=_number(item, "execPrice") or 0.0,
        fee=_number(item, "execFee", optional=True), fee_asset=None, realized_pnl=None,
        is_maker=maker if isinstance(maker, bool) else None, time_ms=_millis(item, "execTime")))


def _bybit_position(item: Mapping[str, Any]) -> StreamEvent:
    if str(item.get("positionIdx", 0)) != "0":
        return _hint(C.BYBIT, "HEDGE_MODE_POSITION")
    size = _number(item, "size") or 0.0
    sequence = _number(item, "seq", optional=True)
    side = C.FLAT if size == 0 else {C.BUY: C.LONG, C.SELL: C.SHORT}[_bybit_side(item)]
    return StreamEvent(POSITION, C.BYBIT, position=PositionUpdate(
        C.BYBIT, C.canonical_symbol(_text(item, "symbol") or ""), side, size,
        _positive(_number(item, "entryPrice", optional=True)), _number(item, "unrealisedPnl", optional=True),
        _millis(item, "updatedTime"), None if sequence is None else int(sequence)))


def parse_bybit_frame(payload: Mapping[str, Any]) -> List[StreamEvent]:
    """Bybit V5 private stream: the ``auth`` / ``subscribe`` / ``pong`` answers and the ``order``,
    ``execution``, ``position`` and ``wallet`` topics of the linear category."""
    operation = payload.get("op")
    if isinstance(operation, str):
        accepted = payload.get("success") is True
        if operation == "auth":
            return [StreamEvent(CONTROL, C.BYBIT, control=AUTH_OK if accepted else AUTH_FAILED)]
        if operation == "subscribe":
            return [StreamEvent(CONTROL, C.BYBIT, control=SUBSCRIBED if accepted else SUBSCRIBE_FAILED)]
        return [StreamEvent(CONTROL, C.BYBIT, control=PONG)] if operation in ("pong", "ping") else []
    topic = payload.get("topic")
    if not isinstance(topic, str):
        return []
    root = topic[:-len(".linear")] if topic.endswith(".linear") else topic
    if root not in ("order", "execution", "position", "wallet"):
        return [_hint(C.BYBIT, f"UNPARSED:{topic}")]
    try:
        items = _objects(payload.get("data"), "data")
        events: List[StreamEvent] = []
        if root == "wallet":
            event_ms = _millis(payload, "creationTime")
            for account in items:
                events += [StreamEvent(BALANCE, C.BYBIT, balance=BalanceUpdate(
                    C.BYBIT, _text(coin, "coin") or "", _number(coin, "walletBalance") or 0.0, event_ms))
                    for coin in _objects(account.get("coin"), "coin")]
            return events
        for item in items:
            if item.get("category") != "linear":
                continue  # another product of the same account: not this adapter's
            event = (_bybit_order(item) if root == "order" else _bybit_execution(item) if root == "execution"
                     else _bybit_position(item))
            if event is not None:
                events.append(event)
        return events
    except (_Malformed, C.ContractViolation):
        return [_hint(C.BYBIT, f"MALFORMED:{topic}")]


# ------------------------------------------------------------------------------------------------- BingX
def parse_bingx_frame(payload: Mapping[str, Any]) -> List[StreamEvent]:
    """BingX perpetual swap user data stream. No frame shape of this stream could be substantiated for this
    build, so no field of an order, a fill, a position or a balance is read: every frame is a hint and the
    venue is asked over REST. The one exception changes nothing about the account: a frame that names itself
    ``listenKeyExpired`` renews the key early instead of waiting for the connection to drop."""
    if payload.get("e") == "listenKeyExpired":
        return [StreamEvent(CONTROL, C.BINGX, control=LISTEN_KEY_EXPIRED)]
    return [_hint(C.BINGX, "UNVERIFIED_FRAME_SHAPE")]


@dataclass(frozen=True)
class StreamProfile:
    venue: str
    parser: Callable[[Mapping[str, Any]], List[StreamEvent]]
    ping_frame: Optional[Mapping[str, Any]]     # the application ping the venue documents; None = protocol ping
    answers_text_ping: bool                     # a bare "Ping" text frame is answered with "Pong"
    acknowledges_subscription: bool             # topics flow only after the venue confirmed the subscription


PROFILES: Mapping[str, StreamProfile] = {
    C.BINANCE: StreamProfile(C.BINANCE, parse_binance_frame, None, False, False),
    C.BYBIT: StreamProfile(C.BYBIT, parse_bybit_frame, {"op": "ping"}, False, True),
    C.BINGX: StreamProfile(C.BINGX, parse_bingx_frame, None, True, False),
}


# ----------------------------------------------------------------------------------------------- manager
#: how many orders closed by a frame are remembered between two passes, so that a replay is recognised
_FINISHED_ORDERS_REMEMBERED = 10_000
#: when several things ask for a pass at once, the pass is recorded under the most serious of them
_TRIGGER_RANK: Mapping[str, int] = {
    R.CONNECT: 3, R.RECONNECT: 3, R.RESTART: 3, R.GAP: 2, R.UNKNOWN_ORDER: 2, R.STREAM_DOWN: 1, R.HINT: 0,
    R.PERIODIC: 0, R.MANUAL: 0}


@dataclass(frozen=True)
class StreamConfig:
    heartbeat_interval_ms: int = 15_000
    stale_after_ms: int = 45_000                # silence after which the connection is abandoned
    auth_timeout_ms: int = 10_000
    reconcile_interval_ms: int = 300_000        # while LIVE: bounds how long a wrong hint can stand
    fallback_reconcile_interval_ms: int = 15_000    # while the stream is down: REST alone keeps the state
    triggered_reconcile_min_interval_ms: int = 2_000
    reconcile_retry_ms: int = 5_000
    renew_retry_ms: int = 5_000
    fill_gap_grace_ms: int = 2_000              # an order update may precede its own fill by this long
    clock_tolerance_ms: int = 2_000             # a frame this much older than the venue's answer is history
    backoff_base_ms: int = 1_000
    backoff_max_ms: int = 60_000
    lease_ms: int = 60_000
    max_frames_per_step: int = 500
    state_flush_ms: int = 5_000


@dataclass(frozen=True)
class StreamStatus:
    venue: str
    environment: str
    account_ref: str
    health: str
    reason: str
    entries_allowed: bool               # True only when LIVE: every other state blocks NEW entries, nothing else
    owner: Optional[str]
    last_frame_at_ms: Optional[int]
    last_reconcile_at_ms: Optional[int]
    reconnect_attempts: int
    next_connect_at_ms: Optional[int]


def stream_status(db: Any, venue: str, environment: str, account_ref: str, now_ms: int) -> StreamStatus:
    """The stream health of an account as any process may read it. A row that says LIVE while its owner has
    stopped renewing the lease is reported STALE: a dead worker cannot keep a stream alive on paper."""
    with db.connect() as conn:
        row = conn.execute(f"SELECT * FROM {STREAM_STATE_TABLE} WHERE venue=? AND environment=? AND account_ref=?",
                           (venue, environment, str(account_ref))).fetchone()
    if row is None:
        return StreamStatus(venue, environment, str(account_ref), DISCONNECTED, "NO_STREAM_STATE", False, None, None,
                            None, 0, None)
    health, reason = row["health"], row["health_reason"]
    if health == LIVE and int(row["lease_until_ms"]) <= int(now_ms):
        health, reason = STALE, "STREAM_OWNER_NOT_REPORTING"
    return StreamStatus(venue, environment, str(account_ref), health, reason, health == LIVE, row["owner"],
                        row["last_frame_at_ms"], row["last_reconcile_at_ms"], int(row["reconnect_attempts"]),
                        int(row["next_connect_at_ms"]) or None)


def stream_allows_entries(db: Any, venue: str, environment: str, account_ref: str, now_ms: int) -> Tuple[bool, str]:
    """``(allowed, reason)`` for a caller that blocks new entries on a degraded stream. Position maintenance,
    protection and flatten never consult this."""
    status = stream_status(db, venue, environment, account_ref, now_ms)
    return status.entries_allowed, "" if status.entries_allowed else f"USER_STREAM_{status.health}:{status.reason}"


class UserStreamManager:
    """Drives one account's stream. The caller calls ``step(now_ms)`` in a loop; nothing here sleeps, starts
    a thread or reads a clock."""

    def __init__(self, db: Any, adapter: Any, transport: StreamTransport, *, account_ref: str, owner: str,
                 symbols: Sequence[str] = (), config: Optional[StreamConfig] = None,
                 reconciler: Optional[R.RestReconciler] = None,
                 on_fill: Optional[Callable[[C.FillInfo], None]] = None,
                 on_event: Optional[Callable[[StreamEvent], None]] = None) -> None:
        if adapter.venue not in PROFILES:
            raise ValueError(f"no user-stream profile for venue {adapter.venue!r}")
        if not str(account_ref or "").strip() or not str(owner or "").strip():
            raise ValueError("a stream belongs to one connected account and is held by one named worker")
        self.db, self.adapter, self.transport = db, adapter, transport
        self.venue, self.environment, self.account_ref = adapter.venue, adapter.environment, str(account_ref)
        self.owner, self.config, self.profile = str(owner), config or StreamConfig(), PROFILES[adapter.venue]
        self.reconciler = reconciler or R.RestReconciler(db, adapter, account_ref=self.account_ref, symbols=symbols)
        self.ledger = self.reconciler.ledger
        self.on_fill, self.on_event = on_fill, on_event
        self._spec: Optional[C.UserStreamSpec] = None
        self._connected = self._authenticated = self._subscribed = False
        self._connect_trigger: Optional[str] = None
        self._health, self._reason = DISCONNECTED, "NEVER_CONNECTED"
        self._attempts = self._connects = self._connects_before = 0
        self._next_connect_at = self._auth_deadline = 0
        self._connected_at: Optional[int] = None
        self._last_frame_at: Optional[int] = None
        self._last_ping_at = self._last_renew_at = self._last_renew_ok_at = 0
        self._pending: Optional[str] = None
        self._reconcile_not_before = self._reconcile_floor = self._reconcile_failures = 0
        self._last_reconcile_at: Optional[int] = None
        self._last_reconcile_status: Optional[str] = None
        self._state = R.AccountState(self.venue, self.environment, self.account_ref)
        self._balances: Dict[str, BalanceUpdate] = {}
        self._expected: Set[str] = set()
        self._touched: Set[str] = set()
        self._baseline: Dict[str, float] = {}
        self._finished: Dict[str, int] = {}     # orders a frame closed since the last pass, and when
        self._gaps: Dict[str, Tuple[str, str, float, int]] = {}
        self._position_sequence: Dict[str, int] = {}
        self._lease_checked_until = self._last_flush = 0
        self._dirty = True
        self.counters: Dict[str, int] = dict(frames=0, duplicate_fills=0, hints=0, malformed=0, stale_frames=0,
                                             reconciliations=0)

    # ------------------------------------------------------------------------------------------- reading
    @property
    def state(self) -> R.AccountState:
        """Open orders and positions as last confirmed by the venue, with stream hints applied since.
        Each entry says by its ``source`` whether it is the venue's answer or a hint."""
        return self._state

    @property
    def balances(self) -> Mapping[str, BalanceUpdate]:
        return dict(self._balances)

    def status(self, now_ms: int) -> StreamStatus:
        return StreamStatus(self.venue, self.environment, self.account_ref, self._health, self._reason,
                            self._health == LIVE, self.owner, self._last_frame_at, self._last_reconcile_at,
                            self._attempts, None if self._connected else self._next_connect_at)

    def expect_order(self, client_order_id: str) -> None:
        """Announce an order this platform is about to send, so its first frame is not an unknown order."""
        self._expected.add(str(client_order_id))

    # --------------------------------------------------------------------------------------------- lease
    def _hold_lease(self, now_ms: int) -> bool:
        if now_ms < self._lease_checked_until:
            return True
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                f"INSERT OR IGNORE INTO {STREAM_STATE_TABLE} (venue, environment, account_ref, health, health_reason, "
                "updated_at_ms) VALUES (?,?,?,?,?,?)",
                (self.venue, self.environment, self.account_ref, DISCONNECTED, "NEVER_CONNECTED", now_ms))
            row = conn.execute(
                f"SELECT owner, lease_until_ms, connects FROM {STREAM_STATE_TABLE} WHERE venue=? AND environment=? "
                "AND account_ref=?", (self.venue, self.environment, self.account_ref)).fetchone()
            if row["owner"] not in (None, self.owner) and int(row["lease_until_ms"]) > now_ms:
                return False
            if self._connects == 0:
                self._connects_before = int(row["connects"])
            conn.execute(
                f"UPDATE {STREAM_STATE_TABLE} SET owner=?, lease_until_ms=?, updated_at_ms=? WHERE venue=? AND "
                "environment=? AND account_ref=?",
                (self.owner, now_ms + self.config.lease_ms, now_ms, self.venue, self.environment, self.account_ref))
        self._lease_checked_until = now_ms + self.config.lease_ms // 3
        return True

    # ---------------------------------------------------------------------------------------------- step
    def step(self, now_ms: int) -> StreamStatus:
        """Advance once: hold the lease, connect when due, read what arrived, keep the connection alive,
        detect silence and gaps, and reconcile through REST when anything asked for it."""
        now_ms = int(now_ms)
        if not self._hold_lease(now_ms):
            # another worker holds this account's stream: this one keeps no connection and asks nothing
            self._drop()
            self._set_health(DISCONNECTED, "STREAM_HELD_BY_ANOTHER_WORKER")
            return self.status(now_ms)
        if not self._connected and now_ms >= self._next_connect_at:
            self._connect(now_ms)
        if self._connected:
            self._read(now_ms)
        if self._connected:
            self._keep_alive(now_ms)
        if self._connected:
            self._watch(now_ms)
        self._schedule_periodic(now_ms)
        if self._pending is not None and now_ms >= max(self._reconcile_not_before, self._reconcile_floor):
            self._reconcile(now_ms)
        self._flush(now_ms)
        return self.status(now_ms)

    def close(self, now_ms: int) -> None:
        """Close the connection and give the lease back."""
        self._drop()
        self._set_health(DISCONNECTED, "CLOSED")
        self._flush(int(now_ms), release=True)
        self._lease_checked_until = 0

    # ------------------------------------------------------------------------------------------ connect
    def _drop(self) -> None:
        try:
            self.transport.close()
        except (StreamClosed, OSError):
            pass  # it is already gone, which is what was wanted
        self._spec = None
        self._connected = self._authenticated = self._subscribed = False
        self._connect_trigger = None
        self._gaps.clear()

    def _ready(self) -> bool:
        """Connected, authenticated and, where the venue confirms subscriptions, subscribed."""
        return self._connected and self._authenticated and (
            self._subscribed or not self.profile.acknowledges_subscription)

    def _reconcile_once_ready(self, now_ms: int) -> None:
        """The pass that follows a connection is made once the venue is actually sending: a snapshot taken
        before the subscription is active could miss what happens between the two."""
        if self._connect_trigger is not None and self._ready():
            trigger, self._connect_trigger = self._connect_trigger, None
            self._request(now_ms, trigger, blocking=False, immediate=True)

    def _disconnect(self, now_ms: int, reason: str, *, health: str = DISCONNECTED, immediate: bool = False,
                    error: Optional[C.AdapterError] = None) -> None:
        """Leave the connection and say when the next one may be tried. The delay doubles per failure up to
        the maximum; a venue instruction to wait is never undercut. While down, REST keeps the state."""
        self._drop()
        if immediate:
            self._next_connect_at = now_ms
        else:
            self._attempts += 1
            delay = min(self.config.backoff_base_ms * (2 ** min(self._attempts - 1, 30)), self.config.backoff_max_ms)
            self._next_connect_at = now_ms + delay
            if error is not None:
                if error.retry_after_s is not None:
                    self._next_connect_at = max(self._next_connect_at, now_ms + int(float(error.retry_after_s) * 1000) + 1)
                if error.banned_until_ms is not None:
                    self._next_connect_at = max(self._next_connect_at, int(error.banned_until_ms))
        self._set_health(health, reason)
        self._request(now_ms, R.STREAM_DOWN, blocking=False)

    def _connect(self, now_ms: int) -> None:
        self._drop()  # whatever was open is closed before anything is opened: never two connections
        try:
            spec = self.adapter.user_stream()
        except C.AdapterError as error:
            self._disconnect(now_ms, f"STREAM_SPEC_REFUSED:{error.code}", error=error)
            return
        if spec.auth_mode not in (LISTEN_KEY_IN_URL, AUTH_FRAME):
            self._disconnect(now_ms, "STREAM_AUTH_MODE_NOT_SUPPORTED")
            return
        try:
            self.transport.connect(spec)
            for frame in spec.subscribe_frames:
                if spec.auth_mode != AUTH_FRAME or frame.get("op") == "auth":
                    self.transport.send(frame)
        except (StreamClosed, OSError) as failure:
            self._disconnect(now_ms, f"CONNECT_FAILED:{type(failure).__name__}")
            return
        first = self._connects == 0
        self._spec, self._connected = spec, True
        self._authenticated = spec.auth_mode == LISTEN_KEY_IN_URL
        self._connected_at = self._last_frame_at = now_ms
        self._last_ping_at = self._last_renew_at = self._last_renew_ok_at = now_ms
        self._auth_deadline = now_ms + self.config.auth_timeout_ms
        self._connects += 1
        self._set_health(RECONCILE_REQUIRED, "CONNECTED_AWAITING_RECONCILIATION")
        self._connect_trigger = R.RECONNECT if not first else (R.RESTART if self._connects_before else R.CONNECT)
        self._reconcile_once_ready(now_ms)

    # ---------------------------------------------------------------------------------------------- read
    def _send(self, now_ms: int, frame: Any) -> bool:
        try:
            self.transport.send(frame)
            return True
        except (StreamClosed, OSError) as failure:
            self._disconnect(now_ms, f"CONNECTION_LOST:{type(failure).__name__}")
            return False

    def _read(self, now_ms: int) -> None:
        for _ in range(self.config.max_frames_per_step):
            try:
                raw = self.transport.receive()
            except (StreamClosed, OSError) as failure:
                self._disconnect(now_ms, f"CONNECTION_LOST:{type(failure).__name__}")
                return
            if raw is None:
                return
            self._last_frame_at = now_ms
            self.counters["frames"] += 1
            kind, payload = decode_frame(raw)
            if kind == _HEARTBEAT:
                continue
            if kind == _TEXT:
                if self.profile.answers_text_ping and payload.lower() == "ping" and not self._send(now_ms, "Pong"):
                    return
                continue
            if kind == _MALFORMED:
                self.counters["malformed"] += 1
                self._hint(now_ms)
                continue
            for event in self.profile.parser(payload):
                self._handle(event, now_ms)
                if not self._connected:
                    return

    def _handle(self, event: StreamEvent, now_ms: int) -> None:
        if self.on_event is not None:
            self.on_event(event)
        if event.kind == CONTROL:
            if event.control == LISTEN_KEY_EXPIRED:
                self._disconnect(now_ms, "LISTEN_KEY_EXPIRED", immediate=True)
            elif event.control in (AUTH_OK, SUBSCRIBED):
                if event.control == AUTH_OK and not self._authenticated and self._spec is not None:
                    for frame in self._spec.subscribe_frames:
                        if frame.get("op") != "auth" and not self._send(now_ms, frame):
                            return
                self._authenticated = self._authenticated or event.control == AUTH_OK
                self._subscribed = self._subscribed or event.control == SUBSCRIBED
                self._reconcile_once_ready(now_ms)
            elif event.control in (AUTH_FAILED, SUBSCRIBE_FAILED):
                self._attempts = max(self._attempts, 30)  # a refused credential is not retried quickly
                self._disconnect(now_ms, event.control)
            elif event.control == RECONCILE_HINT:
                self._hint(now_ms)
        elif event.kind == FILL and event.fill is not None:
            self._on_fill(event.fill, now_ms)
        elif event.kind == ORDER and event.order is not None:
            self._on_order(event.order, now_ms)
        elif event.kind == POSITION and event.position is not None:
            self._on_position(event.position)
        elif event.kind == BALANCE and event.balance is not None:
            self._balances[event.balance.asset] = event.balance

    def _on_fill(self, fill: C.FillInfo, now_ms: int) -> None:
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            outcome = self.ledger.record(conn, fill, R.STREAM, now_ms)
        self._touched.add(fill.symbol)
        if outcome != R.NEW_FILL:
            self.counters["duplicate_fills"] += 1
        elif self.on_fill is not None:
            self.on_fill(fill)

    def _on_order(self, update: OrderUpdate, now_ms: int) -> None:
        key = R.order_key(update.venue_order_id, update.client_order_id)
        orders = dict(self._state.open_orders)
        held = orders.get(key)
        self._touched.add(update.symbol)
        # A frame older than what the venue has since answered is history: it must not reopen an order the
        # venue no longer lists, nor take a newer state back.
        answered_at = (held.updated_at_ms if held is not None
                       else self._state.observed_at_ms - self.config.clock_tolerance_ms)
        if update.event_time_ms < answered_at or update.event_time_ms <= self._finished.get(key, -1):
            self.counters["stale_frames"] += 1
            return
        if update.status == C.UNKNOWN:
            self._request(now_ms, R.UNKNOWN_ORDER)  # a state the venue named and this module cannot map
            return
        if held is None and update.client_order_id not in self._expected:
            self._request(now_ms, R.UNKNOWN_ORDER)
        if update.status in C.OPEN_STATUSES:
            orders[key] = R.OrderView(update.symbol, update.client_order_id, update.venue_order_id, update.status,
                                      update.filled_quantity, update.event_time_ms, R.STREAM)
        else:
            orders.pop(key, None)
            self._expected.discard(update.client_order_id or "")
            if len(self._finished) >= _FINISHED_ORDERS_REMEMBERED:
                self._finished.clear()  # a forgotten order costs one REST pass if its last frame is replayed
            self._finished[key] = update.event_time_ms
        self._state = R.AccountState(self.venue, self.environment, self.account_ref, orders, self._state.positions,
                                     self._state.observed_at_ms)
        if update.filled_quantity > self._received(update.symbol, update.venue_order_id, key) * (1 + 1e-9) + 1e-12:
            first_seen = self._gaps[key][3] if key in self._gaps else now_ms
            self._gaps[key] = (update.symbol, update.venue_order_id, update.filled_quantity, first_seen)
        else:
            self._gaps.pop(key, None)

    def _received(self, symbol: str, venue_order_id: str, key: str) -> float:
        return max(self.ledger.filled_quantity(symbol, venue_order_id), self._baseline.get(key, 0.0))

    def _on_position(self, update: PositionUpdate) -> None:
        positions = dict(self._state.positions)
        held = positions.get(update.symbol)
        last_sequence = self._position_sequence.get(update.symbol)
        self._touched.add(update.symbol)
        answered_at = (self._state.observed_at_ms if held is None else held.updated_at_ms) - self.config.clock_tolerance_ms
        if update.event_time_ms < answered_at or (
                update.sequence is not None and last_sequence is not None and update.sequence < last_sequence):
            self.counters["stale_frames"] += 1
            return
        if update.sequence is not None:
            self._position_sequence[update.symbol] = update.sequence
        if update.side == C.FLAT:
            positions.pop(update.symbol, None)
        else:
            positions[update.symbol] = R.PositionView(update.symbol, update.side, update.quantity, update.entry_price,
                                                      update.event_time_ms, R.STREAM)
        self._state = R.AccountState(self.venue, self.environment, self.account_ref, self._state.open_orders,
                                     positions, self._state.observed_at_ms)

    # --------------------------------------------------------------------------------- liveness and gaps
    def _keep_alive(self, now_ms: int) -> None:
        spec = self._spec
        if spec is None:
            return
        keepalive_ms = max(1, int(spec.keepalive_seconds)) * 1000
        listen_key = spec.auth_mode == LISTEN_KEY_IN_URL
        interval = self.config.heartbeat_interval_ms if listen_key else min(self.config.heartbeat_interval_ms,
                                                                            keepalive_ms)
        if now_ms - self._last_ping_at >= interval:
            self._last_ping_at = now_ms
            if self.profile.ping_frame is not None:
                if not self._send(now_ms, dict(self.profile.ping_frame)):
                    return
            else:
                try:
                    self.transport.ping()
                except (StreamClosed, OSError) as failure:
                    self._disconnect(now_ms, f"CONNECTION_LOST:{type(failure).__name__}")
                    return
        if not listen_key:
            return
        if now_ms - self._last_renew_at >= keepalive_ms:
            self._renew(now_ms, keepalive_ms)
        if (self._connected and spec.expires_after_seconds is not None
                and now_ms - self._last_renew_ok_at >= int(spec.expires_after_seconds) * 1000):
            self._disconnect(now_ms, "LISTEN_KEY_EXPIRED_UNRENEWED", immediate=True)

    def _renew(self, now_ms: int, keepalive_ms: int) -> None:
        """Extend the listen key by asking the adapter for the stream again. The same address back means the
        key lives on; a different one means the venue issued a new key and the connection moves to it."""
        try:
            fresh = self.adapter.user_stream()
        except C.AdapterError:
            self._last_renew_at = now_ms - keepalive_ms + self.config.renew_retry_ms
            return
        self._last_renew_at = now_ms
        if self._spec is not None and fresh.url != self._spec.url:
            self._disconnect(now_ms, "LISTEN_KEY_REPLACED", immediate=True)
        else:
            self._last_renew_ok_at = now_ms

    def _watch(self, now_ms: int) -> None:
        if not self._ready() and now_ms >= self._auth_deadline:
            self._disconnect(now_ms, "STREAM_NOT_ACKNOWLEDGED")
            return
        if self._last_frame_at is not None and now_ms - self._last_frame_at >= self.config.stale_after_ms:
            # not even an answer to a ping: nothing this connection said last can be relied on
            self._disconnect(now_ms, "STREAM_SILENT", health=STALE)
            return
        for key, (symbol, order_id, reported, first_seen) in list(self._gaps.items()):
            if reported <= self._received(symbol, order_id, key) * (1 + 1e-9) + 1e-12:
                del self._gaps[key]
            elif now_ms - first_seen >= self.config.fill_gap_grace_ms:
                self._gaps.clear()
                self._request(now_ms, R.GAP)
                return

    # ------------------------------------------------------------------------------------ reconciliation
    def _hint(self, now_ms: int) -> None:
        self.counters["hints"] += 1
        self._request(now_ms, R.HINT, blocking=False)

    def _request(self, now_ms: int, trigger: str, *, blocking: bool = True, immediate: bool = False) -> None:
        """Ask for a REST pass. ``blocking`` takes the stream out of LIVE until the venue has answered: a gap
        or an order nobody announced means the hints can no longer be believed. Requests that arrive
        together make one pass, named after the most serious of them; a pass asked for by the stream follows
        the previous one by at least ``triggered_reconcile_min_interval_ms``."""
        if blocking and self._health == LIVE:
            self._set_health(RECONCILE_REQUIRED, trigger)
        earliest = now_ms if immediate or self._last_reconcile_at is None else max(
            now_ms, self._last_reconcile_at + self.config.triggered_reconcile_min_interval_ms)
        if self._pending is None:
            self._pending, self._reconcile_not_before = trigger, earliest
            return
        self._reconcile_not_before = min(self._reconcile_not_before, earliest)
        if _TRIGGER_RANK[trigger] > _TRIGGER_RANK[self._pending]:
            self._pending = trigger

    def _schedule_periodic(self, now_ms: int) -> None:
        if self._pending is not None or self._last_reconcile_at is None:
            return
        interval = (self.config.reconcile_interval_ms if self._health == LIVE
                    else self.config.fallback_reconcile_interval_ms)
        if now_ms - self._last_reconcile_at >= interval:
            self._pending = R.PERIODIC if self._connected else R.STREAM_DOWN
            self._reconcile_not_before = now_ms

    def _reconcile(self, now_ms: int) -> None:
        trigger = self._pending or R.PERIODIC
        result = self.reconciler.run(now_ms, trigger=trigger, extra_symbols=sorted(self._touched),
                                     previous=self._state)
        self.counters["reconciliations"] += 1
        self._last_reconcile_at, self._last_reconcile_status = now_ms, result.status
        self._dirty = True
        if not result.ok or result.state is None:
            # a failed pass is repeated after a delay that doubles up to the reconnect maximum, and never
            # before the venue's own instruction to wait
            self._reconcile_failures += 1
            delay = min(self.config.reconcile_retry_ms * (2 ** min(self._reconcile_failures - 1, 30)),
                        self.config.backoff_max_ms)
            self._reconcile_floor = max(now_ms + delay, result.retry_at_ms or 0)
            if self._health == LIVE:
                self._set_health(RECONCILE_REQUIRED, f"RECONCILIATION_FAILED:{result.error_code}")
            return
        self._pending, self._reconcile_failures = None, 0
        self._state = result.state  # the venue's answer replaces every hint
        self._touched.clear()
        self._gaps.clear()
        self._finished.clear()
        self._baseline = {key: max(float(view.filled_quantity or 0.0),
                                   self.ledger.filled_quantity(view.symbol, view.venue_order_id or ""))
                          for key, view in result.state.open_orders.items()}
        if self.on_fill is not None:
            for fill in result.new_fills:
                self.on_fill(fill)
        if self._ready():
            self._attempts = 0
            self._set_health(LIVE, "")

    # ------------------------------------------------------------------------------------------- state
    def _set_health(self, health: str, reason: str) -> None:
        if (health, reason) != (self._health, self._reason):
            self._health, self._reason, self._dirty = health, reason, True

    def _flush(self, now_ms: int, *, release: bool = False) -> None:
        """Publish the health for other processes. Written when it changed, and otherwise at most once per
        ``state_flush_ms``. Only the lease holder writes."""
        if not (self._dirty or release or now_ms - self._last_flush >= self.config.state_flush_ms):
            return
        with self.db.connect() as conn:
            conn.execute(
                f"UPDATE {STREAM_STATE_TABLE} SET health=?, health_reason=?, connected_at_ms=?, last_frame_at_ms=?, "
                "last_reconcile_at_ms=?, last_reconcile_status=?, reconnect_attempts=?, next_connect_at_ms=?, "
                "connects=?, lease_until_ms=CASE WHEN ? THEN 0 ELSE lease_until_ms END, "
                "owner=CASE WHEN ? THEN NULL ELSE owner END, updated_at_ms=? "
                "WHERE venue=? AND environment=? AND account_ref=? AND owner=?",
                (self._health, self._reason, self._connected_at, self._last_frame_at, self._last_reconcile_at,
                 self._last_reconcile_status, self._attempts, 0 if self._connected else self._next_connect_at,
                 self._connects_before + self._connects, int(release), int(release), now_ms,
                 self.venue, self.environment, self.account_ref, self.owner))
        self._dirty, self._last_flush = False, now_ms


__all__ = ["LISTEN_KEY_IN_URL", "AUTH_FRAME", "LIVE", "STALE", "DISCONNECTED", "RECONCILE_REQUIRED", "HEALTH_STATES",
           "ORDER", "FILL", "POSITION", "BALANCE", "CONTROL", "LISTEN_KEY_EXPIRED", "AUTH_OK", "AUTH_FAILED",
           "SUBSCRIBED", "SUBSCRIBE_FAILED", "PONG", "RECONCILE_HINT", "VERIFIED_BY_DOC", "UNVERIFIED", "VERIFICATION",
           "MAX_FRAME_BYTES", "HEARTBEAT", "StreamClosed", "TransportHeartbeat", "StreamTransport", "OrderUpdate",
           "PositionUpdate", "BalanceUpdate", "StreamEvent", "StreamProfile", "PROFILES", "StreamConfig",
           "StreamStatus", "decode_frame", "parse_binance_frame", "parse_bybit_frame", "parse_bingx_frame",
           "stream_status", "stream_allows_entries", "UserStreamManager"]
