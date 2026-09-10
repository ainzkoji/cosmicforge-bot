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


PAPER_BROKER_MUTATION_FORBIDDEN = "PAPER_BROKER_MUTATION_FORBIDDEN"

#: Broker-side mutations, by exact name. In paper mode none of these may reach
#: the broker: routing already keeps them away (the executor's paper branch
#: returns first), and this is the second line of defence against a future call
#: site that forgets to ask which mode it is in.
_MUTATION_METHODS = frozenset({
    "place_order",
    "place_protection",
    "place_algo_order",
    "close_position_market",
    "cancel_all_orders",
    "cancel_order",
    "cancel_algo_order",
    "cancel_open_orders",
    "new_order",
    "create_order",
    "submit_order",
    "amend_order",
    "modify_order",
    "replace_order",
    "reduce_position",
    "reduce_only_close",
    "set_leverage",
    "change_leverage",
    "set_margin_type",
    "change_margin_type",
    "set_position_mode",
    "change_position_mode",
    "_signed_post",
    "_signed_put",
    "_signed_delete",
})

#: Name families that mutate broker state on every adapter the project ships.
_MUTATION_PREFIXES = (
    "place_", "cancel_", "close_position", "new_order", "create_order",
    "submit_order", "amend_order", "modify_order", "replace_order",
    "set_leverage", "change_leverage", "set_margin", "change_margin",
    "set_position_mode", "change_position_mode",
    "_signed_post", "_signed_put", "_signed_delete",
)


def is_broker_mutation(name: str) -> bool:
    return name in _MUTATION_METHODS or name.startswith(_MUTATION_PREFIXES)


class PaperBrokerMutationForbidden(RuntimeError):
    """A broker-side mutation was attempted through the paper client."""

    code = PAPER_BROKER_MUTATION_FORBIDDEN

    def __init__(self, method: str) -> None:
        self.method = method
        super().__init__(
            f"[{PAPER_BROKER_MUTATION_FORBIDDEN}] {method}() is a broker-side "
            f"mutation and was refused in paper mode. Paper execution is local: "
            f"PaperExecutor and the canonical evidence tables are the only "
            f"authority. Nothing was forwarded to the broker."
        )


class PaperBookClient:
    """Market data from the real client; positions and orders from the book."""

    #: Marker so a wrapped client is never wrapped twice.
    is_paper_book = True

    def __init__(self, inner: Any, paper_executor: Any) -> None:
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "_paper", paper_executor)

    # Anything not defined here is the real client's job (klines, prices,
    # exchange info, account, server time, ...) -- except a broker mutation,
    # which is refused rather than forwarded.
    def __getattr__(self, name: str) -> Any:
        if is_broker_mutation(name):
            logger.error("[PAPER_BOOK] refused broker mutation %s() in paper mode", name)
            raise PaperBrokerMutationForbidden(name)
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
        """Refused. ``cancel_all_orders`` is a broker order endpoint.

        This used to be a silent no-op, which hid every paper call site that
        should never have made the call. Paper routing no longer reaches it
        (``ensure_protection`` returns for paper first), so a call here is a
        regression and is reported as one.
        """
        logger.error("[PAPER_BOOK] %s: refused cancel_all_orders in paper mode", symbol)
        raise PaperBrokerMutationForbidden("cancel_all_orders")

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
