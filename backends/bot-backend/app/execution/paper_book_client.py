"""In paper mode, the exchange *is* the paper book.

Found by driving a real ``PaperRunner`` through a paper lifecycle: the position
opened, and on the very next cycle it was gone.

    lifecycle: phase=FLAT  reconciliation_reason=PM_RESTORE:exchange_flat
    blocked broker calls:  ['cancel_all_orders', 'cancel_all_orders']

``TradeExecutor.ensure_protection`` asks ``client.get_position_amt(symbol)``
and, seeing zero, concludes the position is flat, cancels leftover orders and
lets the caller mark the PositionManager flat. In paper mode that reading is
always zero, because the fill only ever existed in ``PaperExecutor`` — no order
was sent to the broker. Two consequences, both real:

1. A paper position is flattened in the PositionManager on the cycle after it
   opens, so TP1, break-even, trailing and the runner phase can never advance.
   The Phase 12 lifecycle could only ever be demonstrated by calling the
   evidence writers directly, which is what the earlier smoke did.
2. ``cancel_all_orders`` is a broker order endpoint, and a paper bot was
   calling it every cycle.

Rather than teach a dozen reconciliation call sites about paper mode — and miss
the ones nobody has found yet — this wraps the client. Market data passes
straight through to the real client; questions about *positions and orders* are
answered by the paper book, which is the authority for them in paper mode.

Only installed when the effective execution mode is paper. In broker mode the
real client is used unchanged.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PaperBookClient:
    """Market data from the real client; positions and orders from the book."""

    #: Marker so a wrapped client is never wrapped twice.
    is_paper_book = True

    def __init__(self, inner: Any, paper_executor: Any) -> None:
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "_paper", paper_executor)

    # Anything not defined here is the real client's job (klines, prices,
    # exchange info, account, server time, ...).
    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_inner"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in {"_inner", "_paper"}:
            object.__setattr__(self, name, value)
        else:
            setattr(object.__getattribute__(self, "_inner"), name, value)

    @property
    def inner(self) -> Any:
        return object.__getattribute__(self, "_inner")

    # ── Position truth ──────────────────────────────────────────────────────

    def _book(self, symbol: str) -> dict | None:
        try:
            return object.__getattribute__(self, "_paper").get_position(symbol)
        except Exception:
            return None

    def get_position_amt(self, symbol: str) -> float:
        """Signed remaining quantity, from the paper book."""
        position = self._book(symbol)
        if not position:
            return 0.0
        qty = float(position.get("remaining_qty") or 0.0)
        side = str(position.get("side") or "").upper()
        return -qty if side in {"SHORT", "SELL"} else qty

    def get_position_info(self, symbol: str) -> dict | None:
        position = self._book(symbol)
        if not position:
            return None
        return self._as_risk_row(symbol, position)

    def position_risk(self, symbol: str | None = None) -> list:
        if symbol:
            position = self._book(symbol)
            return [self._as_risk_row(symbol, position)] if position else []
        return self.position_risk_all()

    def position_risk_all(self) -> list:
        rows = []
        book = getattr(object.__getattribute__(self, "_paper"), "_positions", {}) or {}
        for symbol, position in book.items():
            if float(position.get("remaining_qty") or 0.0) > 0:
                rows.append(self._as_risk_row(symbol, position))
        return rows

    def get_positions(self) -> list:
        return self.position_risk_all()

    def _as_risk_row(self, symbol: str, position: dict) -> dict:
        qty = float(position.get("remaining_qty") or 0.0)
        side = str(position.get("side") or "").upper()
        amount = -qty if side in {"SHORT", "SELL"} else qty
        entry = float(position.get("entry_price") or 0.0)
        try:
            mark = float(self.inner.last_price(symbol))
        except Exception:
            mark = entry
        return {
            "symbol": str(symbol).upper(),
            "positionAmt": str(amount),
            "entryPrice": str(entry),
            "markPrice": str(mark),
            "unRealizedProfit": str((mark - entry) * amount),
            "leverage": "1",
            "positionSide": "BOTH",
            "source": "paper_book",
        }

    # ── Order management: never reaches the broker ──────────────────────────

    def cancel_all_orders(self, symbol: str) -> dict:
        """No-op. A paper position has no broker orders to cancel."""
        logger.debug("[PAPER_BOOK] %s: cancel_all_orders suppressed (paper mode)", symbol)
        return {"status": "PAPER_NOOP", "symbol": str(symbol).upper()}

    def open_orders(self, symbol: str | None = None) -> list:
        return []

    def get_algo_orders(self, symbol: str, raise_on_error: bool = False) -> list:
        return []


def wrap_for_paper(client: Any, paper_executor: Any, execution_mode: str | None) -> Any:
    """Return a paper-book-backed client when the mode is paper, else the client."""
    if client is None or paper_executor is None:
        return client
    if str(execution_mode or "").lower() != "paper":
        return client
    if getattr(client, "is_paper_book", False):
        return client
    return PaperBookClient(client, paper_executor)
