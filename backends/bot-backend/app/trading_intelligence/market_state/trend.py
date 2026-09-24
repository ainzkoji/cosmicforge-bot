"""Trend state (Section 9.4) -- one state family, not seven indicators voting.

Seed features: multi-horizon normalized return slope, robust slope agreement,
efficiency ratio, directional persistence, ATR-normalized extension and
structural alignment. EMA/SuperTrend-style values may appear only as
``diagnostics`` -- they are never independent votes.
"""
from __future__ import annotations

from typing import List, Optional

import numpy as np

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import CandleSeries, TrendState, safe_float, clamp01
from app.trading_intelligence.market_state.indicators import (
    atr,
    efficiency_ratio,
    linreg_slope_normalized,
)

MIN_HISTORY = 30
SHORT_HORIZON = 10
MEDIUM_HORIZON = 20
LONG_HORIZON = 50
EFFICIENCY_PERIOD = 20


def _sma(values: np.ndarray, period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return float(np.mean(values[-period:]))


def compute_trend_state(series: CandleSeries) -> TrendState:
    n = len(series)
    if n < MIN_HISTORY:
        return TrendState(
            direction="UNCERTAIN",
            strength=0.0,
            age=0,
            maturity="UNKNOWN",
            acceleration=None,
            extension_atr=None,
            retracement_depth=None,
            efficiency=None,
            available=False,
            reason_codes=(ReasonCode.INSUFFICIENT_HISTORY.value,),
        )

    high = np.array([float(v) for v in series.high], dtype=float)
    low = np.array([float(v) for v in series.low], dtype=float)
    close = np.array([float(v) for v in series.close], dtype=float)

    if not (np.all(np.isfinite(high)) and np.all(np.isfinite(low)) and np.all(np.isfinite(close))):
        return TrendState(
            direction="UNCERTAIN",
            strength=0.0,
            age=0,
            maturity="UNKNOWN",
            acceleration=None,
            extension_atr=None,
            retracement_depth=None,
            efficiency=None,
            available=False,
            reason_codes=(ReasonCode.NONFINITE_FEATURE.value,),
        )

    reason_codes: List[str] = []

    long_h = min(LONG_HORIZON, n)
    med_h = min(MEDIUM_HORIZON, n)
    short_h = min(SHORT_HORIZON, n)

    slope_long = linreg_slope_normalized(close[-long_h:])
    slope_med = linreg_slope_normalized(close[-med_h:])
    slope_short = linreg_slope_normalized(close[-short_h:])

    slopes = [s for s in (slope_long, slope_med, slope_short) if s is not None]
    if not slopes:
        reason_codes.append(ReasonCode.NONFINITE_FEATURE.value)
        direction = "UNCERTAIN"
        agreement = 0.0
    else:
        signs = [1 if s > 0.05 else (-1 if s < -0.05 else 0) for s in slopes]
        if all(s == 1 for s in signs):
            direction = "UP"
            agreement = 1.0
        elif all(s == -1 for s in signs):
            direction = "DOWN"
            agreement = 1.0
        else:
            positive = sum(1 for s in signs if s == 1)
            negative = sum(1 for s in signs if s == -1)
            if positive == 0 and negative == 0:
                direction = "FLAT"
                agreement = 1.0
            elif positive > negative:
                direction = "UP"
                agreement = positive / len(signs)
            elif negative > positive:
                direction = "DOWN"
                agreement = negative / len(signs)
            else:
                direction = "UNCERTAIN"
                agreement = 0.0

    efficiency = efficiency_ratio(close, EFFICIENCY_PERIOD)

    # -- directional persistence / age: consecutive trailing bars whose
    # short causal slope keeps the same sign as the current direction.
    age = 0
    if direction in ("UP", "DOWN") and n > short_h + 1:
        target_sign = 1 if direction == "UP" else -1
        max_lookback = min(n - short_h, 200)
        for back in range(max_lookback):
            end = n - back
            window = close[end - short_h : end]
            if len(window) < short_h:
                break
            s = linreg_slope_normalized(window)
            if s is None:
                break
            sign = 1 if s > 0.05 else (-1 if s < -0.05 else 0)
            if sign != target_sign:
                break
            age += 1

    atr_series = atr(high, low, close, period=14)
    current_atr = safe_float(atr_series[-1])

    sma_ref = _sma(close, med_h)
    extension_atr = None
    if current_atr and current_atr > 0 and sma_ref is not None:
        extension_atr = safe_float((close[-1] - sma_ref) / current_atr)
    else:
        reason_codes.append(ReasonCode.NONFINITE_FEATURE.value)

    retracement_depth = None
    if direction in ("UP", "DOWN"):
        window = close[-long_h:]
        if direction == "UP":
            peak = float(np.max(window))
            trough_after_peak = float(np.min(window[np.argmax(window):])) if np.argmax(window) < len(window) - 1 else close[-1]
            move = peak - float(window[0])
            retrace = peak - close[-1]
            retracement_depth = safe_float(retrace / move) if move > 0 else None
        else:
            trough = float(np.min(window))
            move = float(window[0]) - trough
            retrace = close[-1] - trough
            retracement_depth = safe_float(retrace / move) if move > 0 else None
        if retracement_depth is not None:
            retracement_depth = clamp01(retracement_depth) if retracement_depth >= 0 else 0.0

    acceleration = None
    if slope_short is not None and slope_med is not None:
        acceleration = safe_float(slope_short - slope_med)

    # -- strength: agreement + efficiency + bounded extension, no exhaustion penalty here
    strength_components = [agreement]
    if efficiency is not None:
        strength_components.append(clamp01(efficiency))
    if extension_atr is not None:
        strength_components.append(clamp01(min(abs(extension_atr) / 3.0, 1.0)))
    strength = clamp01(float(np.mean(strength_components))) if direction in ("UP", "DOWN") else 0.0

    # -- maturity: age relative to persistence + extension
    if direction not in ("UP", "DOWN"):
        maturity = "UNKNOWN"
    elif age < 5:
        maturity = "EARLY"
    elif age < 20:
        maturity = "MID"
    else:
        maturity = "LATE"
    if extension_atr is not None and abs(extension_atr) > 4.0 and maturity != "EARLY":
        maturity = "LATE"

    diagnostics = {}
    ema20 = _sma(close, med_h)
    if ema20 is not None:
        diagnostics["sma_medium"] = ema20
    if slope_long is not None:
        diagnostics["slope_long_normalized"] = slope_long
    if slope_med is not None:
        diagnostics["slope_medium_normalized"] = slope_med
    if slope_short is not None:
        diagnostics["slope_short_normalized"] = slope_short

    return TrendState(
        direction=direction,
        strength=strength,
        age=age,
        maturity=maturity,
        acceleration=acceleration,
        extension_atr=extension_atr,
        retracement_depth=retracement_depth,
        efficiency=safe_float(efficiency),
        diagnostics=diagnostics,
        available=True,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )
