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

# Phase 4 close-contract failures. A paper close never guesses a side and never
# infers a zero quantity: it fails closed with one of these reason codes.
PAPER_CLOSE_SIDE_UNKNOWN = "PAPER_CLOSE_SIDE_UNKNOWN"
PAPER_CLOSE_QUANTITY_UNKNOWN = "PAPER_CLOSE_QUANTITY_UNKNOWN"
PAPER_CLOSE_POSITION_NOT_FOUND = "PAPER_CLOSE_POSITION_NOT_FOUND"
PAPER_CLOSE_QUANTITY_MISMATCH = "PAPER_CLOSE_QUANTITY_MISMATCH"


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


def _direction(side: Any) -> str | None:
    """LONG or SHORT, from any of the vocabularies used for a side."""
    value = str(side or "").upper()
    if value in {"LONG", "BUY"}:
        return "LONG"
    if value in {"SHORT", "SELL"}:
        return "SHORT"
    return None


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
        self._positions: dict[str, dict[str, float | str]] = {}
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

    def seed_position(
        self,
        symbol: str,
        side: str,
        quantity: float,
        entry_price: float,
        *,
        position_id: str | None = None,
        original_quantity: float | None = None,
        realized_quantity: float = 0.0,
        realized_pnl: float = 0.0,
        fees: float = 0.0,
    ) -> None:
        """Restore a persisted simulated position after a process restart."""
        normalized_side = str(side or "").upper()
        qty = float(quantity or 0.0)
        if normalized_side not in {"LONG", "SHORT", "BUY", "SELL"} or qty <= 0:
            return
        self._positions[symbol.upper()] = {
            "side": normalized_side,
            "original_qty": max(qty, float(original_quantity or qty)),
            "remaining_qty": qty,
            "realized_qty": max(0.0, float(realized_quantity or 0.0)),
            "realized_pnl": float(realized_pnl or 0.0),
            "fees": max(0.0, float(fees or 0.0)),
            "entry_price": float(entry_price or 0.0),
            "position_id": position_id or paper_id("position"),
            "status": "OPEN",
        }

    def get_position(self, symbol: str) -> dict[str, float | str] | None:
        """Return the authoritative simulated position for ``symbol``, if any."""
        saved = self._positions.get(symbol.upper())
        return dict(saved) if saved else None

    def remaining_quantity(self, symbol: str) -> float | None:
        """Authoritative remaining quantity, or None when no position is held.

        ``None`` and ``0.0`` are deliberately different: ``None`` means "unknown",
        which must fail a close closed rather than be treated as a flat position.
        """
        saved = self._positions.get(symbol.upper())
        if saved is None:
            return None
        return float(saved.get("remaining_qty") or 0.0)

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
            self._positions[symbol.upper()] = {
                "side": side.upper(),
                "original_qty": qty,
                "remaining_qty": qty,
                "realized_qty": 0.0,
                "realized_pnl": 0.0,
                "fees": fee,
                "entry_price": fill_price,
                "position_id": position_id,
                "status": "OPEN",
            }
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
            saved = self._positions.get(symbol.upper())
            resolved_side = str(position_side or (saved or {}).get("side") or "").upper()
            saved_qty = float((saved or {}).get("remaining_qty") or 0.0)
            resolved_qty = float(quantity if quantity is not None else saved_qty)
            if resolved_side not in {"LONG", "SHORT", "BUY", "SELL"}:
                raise ValueError(PAPER_CLOSE_SIDE_UNKNOWN)
            if resolved_qty <= 0:
                raise ValueError(PAPER_CLOSE_QUANTITY_UNKNOWN)
            if saved is None:
                raise ValueError(PAPER_CLOSE_POSITION_NOT_FOUND)
            if abs(resolved_qty - saved_qty) > max(1e-12, saved_qty * 1e-9):
                # The caller must close the authoritative remainder, never the
                # original quantity. Diverging quantities are a lifecycle defect.
                raise ValueError(
                    f"{PAPER_CLOSE_QUANTITY_MISMATCH}: requested={resolved_qty} remaining={saved_qty}"
                )
            reference_price = latest_reference_price(self.client, symbol, fallback_price)
            close_side = "SELL" if resolved_side in {"LONG", "BUY"} else "BUY"
            fill_price = self._apply_slippage(close_side, reference_price)
            qty = resolved_qty
            fee = self._fee(qty, fill_price)
            order_id = paper_id("order")
            result = PaperExecutionResult(
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
                    "position_before": resolved_side,
                    "position_id": saved.get("position_id"),
                    "remaining_qty": 0.0,
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
            self._positions.pop(symbol.upper(), None)
            return result
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
                details={
                    "symbol": symbol,
                    "mode": "paper",
                    "reason_code": str(exc).split(":", 1)[0],
                },
            )

    def partial_close(
        self,
        *,
        symbol: str,
        position_side: str,
        quantity: float,
        fallback_price: float | None = None,
    ) -> PaperExecutionResult:
        """Simulate a reduce-only partial close and retain authoritative remainder."""
        try:
            saved = self._positions.get(symbol.upper())
            if saved is None:
                raise ValueError("paper_partial_close_position_not_found")
            # BUY/LONG and SELL/SHORT are the same direction, and the code
            # around this uses both vocabularies: the book records whatever the
            # opening call passed, while the runner holds SymbolState.position.
            # Comparing the raw strings failed a legitimate TP1 partial with a
            # "side mismatch" whenever the two happened to disagree.
            saved_side = _direction(saved.get("side"))
            requested_side = _direction(position_side)
            if requested_side is None or requested_side != saved_side:
                raise ValueError("paper_partial_close_side_mismatch")
            qty = float(quantity or 0.0)
            remaining = float(saved.get("remaining_qty") or 0.0)
            if qty <= 0 or qty >= remaining:
                raise ValueError("paper_partial_close_quantity_invalid")
            reference_price = latest_reference_price(self.client, symbol, fallback_price)
            close_side = "SELL" if saved_side in {"LONG", "BUY"} else "BUY"
            fill_price = self._apply_slippage(close_side, reference_price)
            fee = self._fee(qty, fill_price)
            entry_price = float(saved.get("entry_price") or 0.0)
            pnl = (fill_price - entry_price) * qty if close_side == "SELL" else (entry_price - fill_price) * qty
            new_remaining = remaining - qty
            saved["remaining_qty"] = new_remaining
            saved["realized_qty"] = float(saved.get("realized_qty") or 0.0) + qty
            saved["realized_pnl"] = float(saved.get("realized_pnl") or 0.0) + pnl
            saved["fees"] = float(saved.get("fees") or 0.0) + fee
            saved["status"] = "PARTIALLY_CLOSED"
            order_id = paper_id("order")
            return PaperExecutionResult(
                status="PAPER_PARTIAL_CLOSE",
                action="PAPER_PARTIAL_CLOSE",
                order_id=order_id,
                avg_price=fill_price,
                filled_qty=qty,
                fee=fee,
                success=True,
                details={
                    "symbol": symbol,
                    "side": close_side,
                    "position_side": saved_side,
                    "position_id": saved.get("position_id"),
                    "mode": "paper",
                    "order_id": order_id,
                    "fill_id": paper_id("fill"),
                    "status": PAPER_FILLED,
                    "fill_type": "TP1",
                    "filled_qty": qty,
                    "avg_price": fill_price,
                    "fee": fee,
                    "realized_pnl": pnl,
                    "remaining_qty": new_remaining,
                },
            )
        except Exception as exc:
            return PaperExecutionResult(
                status=PAPER_ERROR,
                action="PAPER_PARTIAL_CLOSE",
                order_id=None,
                avg_price=None,
                filled_qty=0.0,
                fee=0.0,
                success=False,
                error=str(exc),
                details={"symbol": symbol, "mode": "paper"},
            )
