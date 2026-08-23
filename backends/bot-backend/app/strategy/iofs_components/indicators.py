from __future__ import annotations

import math

from app.strategy.iofs_components.models import Candle, validate_candles


def calculate_atr(candles: list[Candle], period: int = 14) -> float | None:
    """Calculate Wilder-smoothed ATR, returning None when unavailable."""
    if not isinstance(candles, list) or not isinstance(period, int) or period <= 0:
        return None
    if len(candles) < period + 1:
        return None

    try:
        validate_candles(candles)
        true_ranges = [
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
            for previous, current in zip(candles, candles[1:])
        ]
        atr = sum(true_ranges[:period]) / period
        for true_range in true_ranges[period:]:
            atr = ((atr * (period - 1)) + true_range) / period
    except (TypeError, ValueError, OverflowError):
        return None

    if not math.isfinite(atr) or atr <= 0:
        return None
    return atr
