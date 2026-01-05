from __future__ import annotations

import time
from typing import Optional, Tuple

from app.core.config import settings


def _move_pct(entry: float, price: float, side: str) -> float:
    """
    Returns profit move as a decimal:
      LONG:  + when price > entry
      SHORT: + when price < entry
    """
    if not entry or entry <= 0:
        return 0.0
    if side == "LONG":
        return (price - entry) / entry
    else:
        return (entry - price) / entry


def should_exit(
    position: str,
    entry_price: Optional[float],
    price: float,
    last_trade_ms: int,
    signal: str,
) -> Tuple[bool, str]:
    """
    Broker-agnostic exit policy (scalable).

    Uses your existing config keys:
      STOP_LOSS_PCT (e.g. 0.7 means 0.7%)
      TAKE_PROFIT_PCT (e.g. 1.2 means 1.2%)
      TRADE_MODE ("flip" usually implies opposite-signal exits)
      COOLDOWN_SECONDS (already used elsewhere)

    Returns: (exit_now, reason)
    """
    if position not in {"LONG", "SHORT"}:
        return (False, "NO_POSITION")

    if not entry_price or entry_price <= 0:
        return (False, "NO_ENTRY_PRICE")

    move = _move_pct(entry_price, price, position)

    # Convert percent config to decimal
    tp = float(settings.TAKE_PROFIT_PCT or 0.0) / 100.0
    sl = float(settings.STOP_LOSS_PCT or 0.0) / 100.0

    # Take profit
    if tp > 0 and move >= tp:
        return (True, f"TAKE_PROFIT_{move:.5f}")

    # Stop loss
    if sl > 0 and move <= -sl:
        return (True, f"STOP_LOSS_{move:.5f}")

    # Optional: time-based exit (safe default: disabled)
    max_hold_min = int(getattr(settings, "MAX_HOLD_MINUTES", 0) or 0)
    if max_hold_min > 0:
        held_ms = int(time.time() * 1000) - int(last_trade_ms or 0)
        if held_ms > max_hold_min * 60 * 1000:
            return (True, "MAX_HOLD_TIMEOUT")

    # Opposite signal exit (recommended for flip mode)
    # This keeps it scalable across brokers.
    if getattr(settings, "EXIT_ON_OPPOSITE_SIGNAL", True):
        if position == "LONG" and signal == "SELL":
            return (True, "OPPOSITE_SIGNAL")
        if position == "SHORT" and signal == "BUY":
            return (True, "OPPOSITE_SIGNAL")

    return (False, "HOLD")
