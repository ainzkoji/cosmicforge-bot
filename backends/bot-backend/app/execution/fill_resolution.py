"""Broker-authoritative fill resolution for a submitted order.

A create-order response is not always final broker truth. Binance futures
answers an order sent without ``newOrderRespType`` with an ACK: status NEW,
executedQty 0 -- even when the MARKET order has already filled. On 2026-09-13
ten Binance demo entries came back that way. The runner raised
BROKER_FILL_QUANTITY_UNAVAILABLE, skipped lifecycle registration, and the
positions existed only at the broker until reconciliation adopted them; the
slot count never saw them.

Resolution order, first source that proves the fill wins:

    1. INITIAL_ORDER_RESPONSE  the response already carries executedQty > 0
    2. ORDER_STATUS_QUERY      GET the order by broker id (or client order id)
    3. TRADE_FILL_QUERY        the order's own fills: quantity, weighted price

Fees always come from the fills when the broker returns them. Requested
quantity is historical intent and is never used as executed quantity.

Finality, as the executor acts on it:

    FILLED / PARTIALLY_FILLED   a fill exists -> lifecycle registration is mandatory
    CANCELED / EXPIRED /
    REJECTED with zero fill     nothing exists -> release the entry
    ORDER_PENDING               accepted, nothing filled yet -> hold the slot
    BROKER_STATE_UNKNOWN        no broker answer -> hold the slot, never "flat"
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)

INITIAL_ORDER_RESPONSE = "INITIAL_ORDER_RESPONSE"
ORDER_STATUS_QUERY = "ORDER_STATUS_QUERY"
TRADE_FILL_QUERY = "TRADE_FILL_QUERY"
RECONCILIATION = "RECONCILIATION"

FILLED = "FILLED"
PARTIALLY_FILLED = "PARTIALLY_FILLED"
ORDER_PENDING = "ORDER_PENDING"
CANCELED = "CANCELED"
EXPIRED = "EXPIRED"
REJECTED = "REJECTED"
BROKER_STATE_UNKNOWN = "BROKER_STATE_UNKNOWN"

_ZERO_FILL_TERMINAL = {CANCELED, EXPIRED, REJECTED}
_TERMINAL = {FILLED} | _ZERO_FILL_TERMINAL
_STATUS_ALIASES = {"EXPIRED_IN_MATCH": EXPIRED, "CANCELLED": CANCELED}
_KNOWN_STATUSES = {"NEW", PARTIALLY_FILLED} | _TERMINAL


def _scalar(value: Any) -> Any:
    """A real broker value, or None. Test doubles and objects are not values."""
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, (str, int, float, Decimal)) and not isinstance(value, bool):
        return value
    return None


def _dec(value: Any) -> Decimal:
    value = _scalar(value)
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


@dataclass(frozen=True)
class OrderView:
    """One broker order snapshot, whatever shape the client returned."""

    status: str | None
    executed_qty: Decimal
    avg_price: Decimal
    order_id: str | None
    client_order_id: str | None

    @property
    def answered(self) -> bool:
        return self.status is not None or self.executed_qty > 0


def order_view(obj: Any) -> OrderView | None:
    if obj is None:
        return None
    if isinstance(obj, dict):
        def get(key: str) -> Any:
            return obj.get(key)
    else:
        def get(key: str) -> Any:
            return getattr(obj, key, None)

    def first(*keys: str) -> Any:
        for key in keys:
            value = _scalar(get(key))
            if value not in (None, ""):
                return value
        return None

    raw_status = first("status")
    status = None
    if raw_status is not None:
        status = str(raw_status).upper()
        status = _STATUS_ALIASES.get(status, status)
        if status not in _KNOWN_STATUSES:
            status = None
    order_id = first("orderId", "broker_order_id", "order_id")
    client_order_id = first("clientOrderId", "client_order_id")
    return OrderView(
        status=status,
        executed_qty=_dec(first("executedQty", "qty_filled", "executed_qty", "filledQty", "filled_qty")),
        avg_price=_dec(first("avgPrice", "avg_fill_price", "avg_price")),
        order_id=str(order_id) if order_id not in (None, "") else None,
        client_order_id=str(client_order_id) if client_order_id not in (None, "") else None,
    )


@dataclass(frozen=True)
class FillResolution:
    status: str
    executed_qty: float
    avg_price: float
    fees: float | None
    fee_asset: str | None
    source: str | None
    initial_status: str | None
    initial_executed_qty: float
    broker_order_id: str | None
    client_order_id: str | None
    fill_count: int = 0
    detail: str = ""

    @property
    def has_fill(self) -> bool:
        return self.executed_qty > 0

    @property
    def zero_fill_terminal(self) -> bool:
        return not self.has_fill and self.status in _ZERO_FILL_TERMINAL

    @property
    def unresolved(self) -> bool:
        return not self.has_fill and not self.zero_fill_terminal

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "executed_qty": self.executed_qty,
            "avg_price": self.avg_price,
            "fees": self.fees,
            "fee_asset": self.fee_asset,
            "source": self.source,
            "initial_status": self.initial_status,
            "initial_executed_qty": self.initial_executed_qty,
            "broker_order_id": self.broker_order_id,
            "client_order_id": self.client_order_id,
            "fill_count": self.fill_count,
            "detail": self.detail,
        }


# ── Venue error classification ───────────────────────────────────────────────
# The Binance futures client raises ``RuntimeError("Binance HTTP <status>: <body>")``
# for every signed call that the venue answered with an error. Only that exact
# shape is ever read as a venue ANSWER; a timeout, a connection error, a 5xx or
# an unparseable body says nothing about whether the request was processed.
_VENUE_ERROR = re.compile(r"Binance HTTP (\d{3}): (.*)", re.DOTALL)
#: -10xx are "general server or network issues". Most of them mean the venue
#: does not know (or will not say) whether the request was executed, so only
#: the ones that are documented as a refusal of THIS request are definitive.
_DEFINITIVE_10XX = frozenset({-1002, -1003, -1013, -1014, -1015, -1020, -1021, -1022, -1023})
#: Definitive, but about the REQUEST (clock skew, request / order rate), not
#: about the order: the venue did not process it, and the identical order may
#: well be accepted a little later. They prove "no order was created" exactly
#: like any other definitive code; callers that retry (the reduce-only close)
#: space such retries out and count them against a separate, larger budget so
#: a few bad cycles cannot exhaust the intent.
TRANSIENT_NOT_PROCESSED_CODES = frozenset({-1003, -1015, -1021})
#: HTTP statuses that are 4xx but do not prove the request was refused:
#: 408 = backend timeout ("send status unknown"), 418/429 = rate limiting
#: (kept UNKNOWN on purpose: read-back decides, never the status alone).
_AMBIGUOUS_4XX = frozenset({408, 418, 429})
#: HTTP statuses with which the venue says "rate limited / banned".
RATE_LIMIT_STATUSES = frozenset({418, 429})
#: The venue's own "no order with this id exists".
ORDER_DOES_NOT_EXIST = -2013
#: "An order / transaction with this client id already exists." The request was
#: refused, but an order carrying that id MAY EXIST (for instance a first send
#: whose answer was lost), so this is never proof that nothing was created:
#: the outcome stays UNKNOWN and the read-back by client order id decides.
#: Futures answers -4116 / -4115 (ClientOrderId is duplicated) or -4111 (client
#: tran id); the legacy shape is -2010 "Duplicate order sent." -- hence the
#: message check as well.
DUPLICATE_CLIENT_ID_CODES = frozenset({-4111, -4115, -4116})


def _venue_answer(exc: Any) -> tuple[int | None, int | None, str]:
    """``(http_status, venue_code, venue_message)`` of a venue-answered error.

    Reads the exception itself and its EXPLICIT causes only (``raise X from
    err``: the executor wraps the client's error that way and also embeds its
    text). The implicit ``__context__`` is never followed: it is merely "the
    exception that was being handled when this one was raised". The fail-safe
    close runs inside the ``except`` block of a failed protection call, so a
    close POST that TIMED OUT there carries the protection error as its
    context -- reading that error's venue code would record a definitive
    REJECT for a request whose outcome is in fact unknown, and the next cycle
    would re-send without read-back.
    """
    seen = 0
    while exc is not None and seen < 6:
        match = _VENUE_ERROR.search(str(exc))
        if match:
            code, message = None, ""
            body = match.group(2)
            try:
                data = json.loads(body)
            except ValueError:
                # The wrapped message may carry trailing text after the body.
                inner = re.search(r"\{.*\}", body, re.DOTALL)
                try:
                    data = json.loads(inner.group(0)) if inner else None
                except ValueError:
                    data = None
            if isinstance(data, dict) and isinstance(data.get("code"), int) and not isinstance(data.get("code"), bool):
                code = data["code"]
            if isinstance(data, dict) and isinstance(data.get("msg"), str):
                message = data["msg"]
            return int(match.group(1)), code, message
        exc = getattr(exc, "__cause__", None)
        seen += 1
    return None, None, ""


def venue_error(exc: Any) -> tuple[int | None, int | None]:
    """``(http_status, venue_code)`` of a venue-answered error, else ``(None, None)``.

    The exception itself and its explicit ``__cause__`` chain; never the
    implicit ``__context__`` (see ``_venue_answer``).
    """
    status, code, _ = _venue_answer(exc)
    return status, code


def duplicate_client_order_id(exc: Any) -> bool:
    """True when the venue refused the request because its client order id is
    already in use. An order with that id may exist: never a terminal reject."""
    status, code, message = _venue_answer(exc)
    if status is None or not 400 <= status < 500:
        return False
    text = message.lower()
    # Over-matching is harmless here: it only keeps an outcome UNKNOWN.
    return code in DUPLICATE_CLIENT_ID_CODES or "duplicate order" in text or ("duplicat" in text and "id" in text)


def definitive_rejection(exc: Any) -> int | None:
    """The venue code when ``exc`` PROVES the request was refused, else None.

    Definitive means: the venue answered HTTP 4xx (not 408/418/429) with a
    parseable error code that is not one of the "outcome unknown" codes. In
    that case no order was created by this request. Everything else -- a
    timeout, a dropped connection, a 5xx, an unparseable body, a "duplicate
    client order id" refusal (an order with that id may exist) -- returns None
    and must be treated as UNKNOWN by the caller (never as "nothing happened").
    """
    status, code, _ = _venue_answer(exc)
    if status is None or code is None or not 400 <= status < 500 or status in _AMBIGUOUS_4XX:
        return None
    if code >= 0 or (-1100 < code <= -1000 and code not in _DEFINITIVE_10XX):
        return None
    if duplicate_client_order_id(exc):
        return None
    return code


def client_order_absent(client: Any, symbol: str, client_order_id: str | None) -> bool:
    """True only when the venue AUTHORITATIVELY answers that no order with this
    client order id exists. Any other outcome (order found, network error,
    another error code, a client without the lookup) is False: absence is
    never inferred from a failed read."""
    if not client_order_id or not hasattr(client, "get_order_by_client_order_id"):
        return False
    try:
        client.get_order_by_client_order_id(symbol, client_order_id)
    except Exception as exc:
        status, code = venue_error(exc)
        return status == 400 and code == ORDER_DOES_NOT_EXIST
    return False


def _query_order(client: Any, symbol: str, order_id: str | None,
                 client_order_id: str | None) -> OrderView | None:
    try:
        if order_id and hasattr(client, "get_order"):
            try:
                raw = client.get_order(symbol, int(order_id))
            except (TypeError, ValueError):
                raw = client.get_order(symbol, order_id)
        elif client_order_id and hasattr(client, "get_order_by_client_order_id"):
            raw = client.get_order_by_client_order_id(symbol, client_order_id)
        else:
            return None
    except Exception as exc:
        logger.warning("[FILL_RESOLUTION] %s order %s query failed: %s", symbol, order_id, exc)
        return None
    view = order_view(raw)
    return view if view is not None and view.answered else None


def _query_fills(client: Any, symbol: str, order_id: str | None) -> list[tuple[Decimal, Decimal, Decimal, str | None]] | None:
    """The order's own fills as (qty, price, commission, asset), or None if unavailable."""
    if not order_id:
        return None
    source = client if hasattr(client, "user_trades") else getattr(client, "_client", None)
    if source is None or not hasattr(source, "user_trades"):
        return None
    try:
        trades = source.user_trades(symbol, limit=200)
        rows = list(trades) if isinstance(trades, (list, tuple)) else None
    except Exception as exc:
        logger.warning("[FILL_RESOLUTION] %s fills query failed: %s", symbol, exc)
        return None
    if rows is None:
        return None
    fills = []
    for row in rows:
        if not isinstance(row, dict) or str(_scalar(row.get("orderId"))) != str(order_id):
            continue
        qty = _dec(row.get("qty"))
        if qty <= 0:
            continue
        asset = _scalar(row.get("commissionAsset"))
        fills.append((qty, _dec(row.get("price")), _dec(row.get("commission")),
                      str(asset) if asset is not None else None))
    return fills


def resolve_order_fill(
    client: Any,
    *,
    symbol: str,
    order_response: Any,
    client_order_id: str | None = None,
    attempts: int = 3,
    backoff_s: float = 0.25,
    sleep: Callable[[float], None] = time.sleep,
) -> FillResolution:
    """Resolve what the broker actually executed for one submitted order."""
    initial = order_view(order_response) or OrderView(None, Decimal("0"), Decimal("0"), None, None)
    order_id = initial.order_id
    coid = initial.client_order_id or client_order_id
    # Only the broker answering AFTER submission counts as knowing the state;
    # an ACK that said NEW says nothing about now.
    view, source, answered = initial, None, False
    notes: list[str] = []

    if initial.executed_qty > 0 and initial.avg_price > 0 and initial.status in (FILLED, None):
        source = INITIAL_ORDER_RESPONSE
        if initial.status is None:
            notes.append("status absent from the order response")
    else:
        for i in range(max(1, int(attempts))):
            queried = _query_order(client, symbol, order_id, coid)
            if queried is not None:
                answered = True
                view = queried
                order_id = queried.order_id or order_id
                if queried.status in _TERMINAL and not (
                    queried.status == FILLED and queried.executed_qty <= 0
                ):
                    break
            if i < attempts - 1:
                sleep(backoff_s * (i + 1))
        if view is not initial and view.executed_qty > 0:
            source = ORDER_STATUS_QUERY
        elif initial.executed_qty > 0:
            source = INITIAL_ORDER_RESPONSE

    qty, avg = view.executed_qty, view.avg_price
    fees = fee_asset = None
    fills = _query_fills(client, symbol, order_id)
    fill_count = len(fills or [])
    if fills:
        answered = True
        fill_qty = sum(f[0] for f in fills)
        fill_notional = sum(f[0] * f[1] for f in fills)
        fees = float(sum(f[2] for f in fills))
        fee_asset = next((f[3] for f in fills if f[3]), None)
        if qty <= 0 or avg <= 0:
            if qty <= 0:
                qty = fill_qty
            if fill_qty > 0 and fill_notional > 0:
                avg = fill_notional / fill_qty
            source = TRADE_FILL_QUERY
        elif fill_qty != qty:
            notes.append(f"fills sum {fill_qty} differs from order executedQty {qty}; order kept")

    status = view.status
    if qty > 0:
        if status in (None, "NEW"):
            final = PARTIALLY_FILLED if status == "NEW" else FILLED
        else:
            final = status  # FILLED, PARTIALLY_FILLED, or CANCELED/EXPIRED after a partial fill
        if avg <= 0:
            notes.append("average fill price not returned by the broker")
    elif status in _ZERO_FILL_TERMINAL:
        final = status
    elif answered and status in ("NEW", PARTIALLY_FILLED):
        final = ORDER_PENDING
    else:
        final = BROKER_STATE_UNKNOWN
        if status == FILLED:
            notes.append("broker reports FILLED but no executed quantity was resolvable")

    resolution = FillResolution(
        status=final,
        executed_qty=float(qty),
        avg_price=float(avg),
        fees=fees,
        fee_asset=fee_asset,
        source=source if qty > 0 else None,
        initial_status=initial.status,
        initial_executed_qty=float(initial.executed_qty),
        broker_order_id=order_id,
        client_order_id=coid,
        fill_count=fill_count,
        detail="; ".join(notes),
    )
    logger.info(
        "[FILL_RESOLUTION] %s order=%s initial=%s/%s -> %s qty=%s avg=%s source=%s",
        symbol, order_id, initial.status, initial.executed_qty, final, qty, avg, resolution.source,
    )
    return resolution
