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
