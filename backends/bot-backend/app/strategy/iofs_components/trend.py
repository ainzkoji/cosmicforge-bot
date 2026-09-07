from __future__ import annotations

import math

from app.strategy.iofs_components.models import Candle, TrendResult, validate_candles
from app.strategy.regime import calculate_adx, calculate_ema

ADX_THRESHOLDS = {
    "conservative": 25.0,
    "balanced": 22.0,
    "aggressive": 20.0,
}


def check_4h_trend(
    candles_4h: list[Candle],
    adx_minimum: float = 22.0,
) -> TrendResult:
    """Check EMA20/50/200 alignment and ADX14 strength."""
    failed = TrendResult(False, "NONE", 0.0, 0.0, "INVALID_INPUT")
    if not isinstance(candles_4h, list):
        return TrendResult(False, "NONE", 0.0, 0.0, "BAD_CANDLES")
    if len(candles_4h) < 200:
        return TrendResult(False, "NONE", 0.0, 0.0, "INSUFFICIENT_CANDLES")
    try:
        adx_minimum = float(adx_minimum)
    except (TypeError, ValueError, OverflowError):
        return failed
    if not math.isfinite(adx_minimum) or adx_minimum < 0:
        return failed

    try:
        validate_candles(candles_4h)
        closes = [candle.close for candle in candles_4h]
        highs = [candle.high for candle in candles_4h]
        lows = [candle.low for candle in candles_4h]
        ema20_values = calculate_ema(closes, 20)
        ema50_values = calculate_ema(closes, 50)
        ema200_values = calculate_ema(closes, 200)
        if not ema20_values or not ema50_values or not ema200_values:
            return TrendResult(False, "NONE", 0.0, 0.0, "EMA_UNAVAILABLE")

        ema20 = ema20_values[-1]
        ema50 = ema50_values[-1]
        ema200 = ema200_values[-1]
        if not all(math.isfinite(value) for value in (ema20, ema50, ema200)):
            return TrendResult(False, "NONE", 0.0, 0.0, "EMA_UNAVAILABLE")
        if ema200 == 0:
            return TrendResult(False, "NONE", 0.0, 0.0, "EMA200_ZERO")

        adx, _, _ = calculate_adx(highs, lows, closes, period=14)
        if not math.isfinite(adx):
            return TrendResult(False, "NONE", 0.0, 0.0, "ADX_UNAVAILABLE")
    except (TypeError, ValueError, OverflowError):
        return TrendResult(False, "NONE", 0.0, 0.0, "BAD_CANDLES")

    ema_sep_pct = abs(ema20 - ema200) / abs(ema200)
    bullish = ema20 > ema50 > ema200 and adx >= adx_minimum
    bearish = ema20 < ema50 < ema200 and adx >= adx_minimum
    if bullish:
        return TrendResult(True, "UP", adx, ema_sep_pct)
    if bearish:
        return TrendResult(True, "DOWN", adx, ema_sep_pct)

    reason = "ADX_BELOW_MINIMUM" if adx < adx_minimum else "EMA_NOT_ALIGNED"
    return TrendResult(False, "NONE", adx, ema_sep_pct, reason)
