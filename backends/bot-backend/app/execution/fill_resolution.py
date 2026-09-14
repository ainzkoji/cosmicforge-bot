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

import logging
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
