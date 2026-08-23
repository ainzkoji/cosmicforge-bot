from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
import uuid


PAPER_ORDER_CREATED = "PAPER_ORDER_CREATED"
PAPER_FILLED = "PAPER_FILLED"
PAPER_POSITION_OPENED = "PAPER_POSITION_OPENED"
PAPER_POSITION_CLOSED = "PAPER_POSITION_CLOSED"
PAPER_ERROR = "PAPER_ERROR"


@dataclass
class PaperExecutionResult:
    status: str
    action: str
    order_id: str | None
    avg_price: float | None
    filled_qty: float
    fee: float
    success: bool
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


def paper_id(prefix: str) -> str:
    return f"paper_{prefix}_{uuid.uuid4().hex}"


def latest_reference_price(client: Any, symbol: str, fallback_price: float | None = None) -> float:
    """Best-effort reference price for simulation. Never submits orders."""
    if fallback_price and fallback_price > 0:
        return float(fallback_price)
    try:
        prices = client.get_prices([symbol])
        price = float(prices.get(symbol, 0.0) or 0.0)
        if price > 0:
            return price
    except Exception:
        pass
    try:
        ticker = client.get_ticker(symbol)
        for key in ("lastPrice", "markPrice", "price", "close"):
            price = float(ticker.get(key, 0.0) or 0.0)
            if price > 0:
                return price
        bid = float(ticker.get("bidPrice", 0.0) or 0.0)
        ask = float(ticker.get("askPrice", 0.0) or 0.0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
    except Exception:
        pass
    try:
        klines = client.klines(symbol=symbol, interval="15m", limit=2)
        if klines:
            last = klines[-1]
            close = last.get("close") if isinstance(last, dict) else last[4]
            price = float(close or 0.0)
            if price > 0:
                return price
    except Exception:
        pass
    raise RuntimeError(f"No reference price available for paper execution: {symbol}")


class PaperExecutor:
    """Internal paper-mode simulator. It never calls exchange order APIs."""

    def __init__(
        self,
        client: Any = None,
        *,
        slippage_bps: float = 2.0,
        fee_bps: float = 4.0,
    ) -> None:
        self.client = client
        self.slippage_bps = max(0.0, float(slippage_bps or 0.0))
        self.fee_bps = max(0.0, float(fee_bps or 0.0))

    def _apply_slippage(self, side: str, price: float) -> float:
        bps = self.slippage_bps / 10_000.0
        side_upper = side.upper()
        if side_upper in {"BUY", "LONG"}:
            return price * (1.0 + bps)
        return price * (1.0 - bps)

    def _fee(self, qty: float, price: float) -> float:
        return abs(float(qty) * float(price)) * (self.fee_bps / 10_000.0)

    def open_position(
        self,
        *,
        symbol: str,
        side: str,
        notional_usdt: float,
        fallback_price: float | None = None,
        quantity: float | None = None,
        sl_price: float | None = None,
        tp_price: float | None = None,
    ) -> PaperExecutionResult:
        try:
            reference_price = latest_reference_price(self.client, symbol, fallback_price)
            fill_price = self._apply_slippage(side, reference_price)
            qty = float(quantity) if quantity and quantity > 0 else float(notional_usdt) / fill_price
            order_id = paper_id("order")
            position_id = paper_id("position")
            fee = self._fee(qty, fill_price)
            return PaperExecutionResult(
                status=PAPER_POSITION_OPENED,
                action="ORDER_PLACED",
                order_id=order_id,
                avg_price=fill_price,
                filled_qty=qty,
                fee=fee,
                success=True,
                details={
                    "symbol": symbol,
                    "side": side.upper(),
                    "mode": "paper",
                    "order_id": order_id,
                    "fill_id": paper_id("fill"),
                    "position_id": position_id,
                    "status": PAPER_FILLED,
                    "execution_status": PAPER_POSITION_OPENED,
                    "reference_price": reference_price,
                    "avg_price": fill_price,
                    "filled_qty": qty,
                    "qty": qty,
                    "fee": fee,
                    "slippage_bps": self.slippage_bps,
                    "fee_bps": self.fee_bps,
                    "sl_price": sl_price,
                    "tp_price": tp_price,
                    "protection": {
                        "sl_order_id": paper_id("sl") if sl_price else None,
                        "tp_order_id": paper_id("tp") if tp_price else None,
                        "status": "PAPER_PROTECTION_ATTACHED",
                    },
                },
            )
        except Exception as exc:
            return PaperExecutionResult(
                status=PAPER_ERROR,
                action="OPEN",
                order_id=None,
                avg_price=None,
                filled_qty=0.0,
                fee=0.0,
                success=False,
                error=str(exc),
                details={"symbol": symbol, "side": side.upper(), "mode": "paper"},
            )

    def close_position(
        self,
        *,
        symbol: str,
        position_side: str | None = None,
        quantity: float | None = None,
        fallback_price: float | None = None,
    ) -> PaperExecutionResult:
        try:
            reference_price = latest_reference_price(self.client, symbol, fallback_price)
            close_side = "SELL" if str(position_side or "").upper() == "LONG" else "BUY"
            fill_price = self._apply_slippage(close_side, reference_price)
            qty = float(quantity or 0.0)
            fee = self._fee(qty, fill_price)
            order_id = paper_id("order")
            return PaperExecutionResult(
                status="CLOSED_POSITION",
                action=PAPER_POSITION_CLOSED,
                order_id=order_id,
                avg_price=fill_price,
                filled_qty=qty,
                fee=fee,
                success=True,
                details={
                    "symbol": symbol,
                    "mode": "paper",
                    "order_id": order_id,
                    "fill_id": paper_id("fill"),
                    "status": PAPER_FILLED,
                    "execution_status": PAPER_POSITION_CLOSED,
                    "position_before": str(position_side or "UNKNOWN").upper(),
                    "reference_price": reference_price,
                    "avg_price": fill_price,
                    "filled_qty": qty,
                    "qty": qty,
                    "fee": fee,
                    "slippage_bps": self.slippage_bps,
                    "fee_bps": self.fee_bps,
                    "normalized": {
                        "order_id": order_id,
                        "avg_price": fill_price,
                        "executed_qty": qty,
                        "status": PAPER_FILLED,
                    },
                },
            )
        except Exception as exc:
            return PaperExecutionResult(
                status=PAPER_ERROR,
                action="CLOSE",
                order_id=None,
                avg_price=None,
                filled_qty=0.0,
                fee=0.0,
                success=False,
                error=str(exc),
                details={"symbol": symbol, "mode": "paper"},
            )
