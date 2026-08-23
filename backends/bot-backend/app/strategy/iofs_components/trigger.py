from __future__ import annotations

import math

from app.strategy.iofs_components.models import Candle, TriggerResult, validate_candles


def check_trigger_candle(
    candles_15m: list[Candle],
    level: float,
    direction: str,
    atr: float,
) -> TriggerResult:
    """Confirm an engulfing or pin-bar trigger near the retested level."""
    if not isinstance(candles_15m, list):
        return TriggerResult(False, "NONE", 0.0, None, None, "BAD_CANDLES")
    if len(candles_15m) < 2:
        return TriggerResult(False, "NONE", 0.0, None, None, "INSUFFICIENT_CANDLES")

    normalized_direction = str(direction).upper()
    if normalized_direction not in {"UP", "DOWN"}:
        return TriggerResult(False, "NONE", 0.0, None, None, "INVALID_DIRECTION")
    try:
        atr = float(atr)
    except (TypeError, ValueError, OverflowError):
        return TriggerResult(False, "NONE", 0.0, None, None, "INVALID_ATR")
    if not math.isfinite(atr) or atr <= 0:
        return TriggerResult(False, "NONE", 0.0, None, None, "INVALID_ATR")
    try:
        level = float(level)
    except (TypeError, ValueError, OverflowError):
        return TriggerResult(False, "NONE", 0.0, None, None, "INVALID_LEVEL")
    if not math.isfinite(level):
        return TriggerResult(False, "NONE", 0.0, None, None, "INVALID_LEVEL")

    previous, current = candles_15m[-2:]
    try:
        validate_candles([previous, current])
    except (TypeError, ValueError):
        return TriggerResult(False, "NONE", 0.0, None, None, "BAD_CANDLES")

    body = abs(current.close - current.open)
    if body == 0:
        return TriggerResult(
            False, "NONE", 0.0, current.low, current.high, "ZERO_BODY"
        )
    if abs(current.close - level) > 0.40 * atr:
        return TriggerResult(
            False, "NONE", 0.0, current.low, current.high, "TOO_FAR_FROM_LEVEL"
        )

    candle_range = current.high - current.low
    if normalized_direction == "UP":
        wick = min(current.open, current.close) - current.low
        closes_in_zone = current.close >= current.low + 0.40 * candle_range
        engulfing = (
            current.open < previous.close
            and current.close > previous.open
            and current.close > current.open
            and body >= 0.50 * atr
        )
    else:
        wick = current.high - max(current.open, current.close)
        closes_in_zone = current.close <= current.high - 0.40 * candle_range
        engulfing = (
            current.open > previous.close
            and current.close < previous.open
            and current.close < current.open
            and body >= 0.50 * atr
        )

    wick_ratio = wick / body
    if engulfing and closes_in_zone:
        return TriggerResult(
            True, "ENGULFING", wick_ratio, current.low, current.high
        )
    if wick_ratio >= 2.0 and closes_in_zone:
        return TriggerResult(True, "PIN_BAR", wick_ratio, current.low, current.high)
    return TriggerResult(
        False, "NONE", wick_ratio, current.low, current.high, "NO_PATTERN"
    )
