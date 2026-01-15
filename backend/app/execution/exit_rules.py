from __future__ import annotations
from typing import Optional, Tuple

from app.execution.position_manager import should_exit


def should_close_position(
    position: str,
    entry_price: Optional[float],
    price: float,
) -> Tuple[bool, str]:
    """
    Backward-compatible wrapper.
    Calls the unified exit policy with minimal inputs.
    """
    # last_trade_ms and signal are not available here, so pass safe defaults
    return should_exit(
        position=position,
        entry_price=entry_price,
        price=price,
        last_trade_ms=0,
        signal="HOLD",
    )


def _validate_sl_tp(
    side: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
) -> None:
    """
    Validate SL/TP invariants.
    LONG: stop_loss < entry_price < take_profit
    SHORT: take_profit < entry_price < stop_loss
    Raises ValueError if invalid.
    """
    side_u = (side or "").upper()

    if side_u == "LONG":
        if not (stop_loss < entry_price < take_profit):
            raise ValueError("Invalid SL/TP for LONG")
        return

    if side_u == "SHORT":
        if not (take_profit < entry_price < stop_loss):
            raise ValueError("Invalid SL/TP for SHORT")
        return

    raise ValueError(f"Invalid side: {side}")
