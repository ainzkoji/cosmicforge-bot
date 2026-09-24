"""Momentum state (Section 9.6) -- factual/evidence state, not trade direction.

V1 features: normalized short/medium return, momentum acceleration,
price-progress efficiency, price/participation divergence and an exhaustion
proxy. None of these authorize a BUY/SELL decision on their own.
"""
from __future__ import annotations

from typing import List

import numpy as np

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import CandleSeries, MomentumState, safe_float, clamp01
from app.trading_intelligence.market_state.indicators import atr, efficiency_ratio, linreg_slope_normalized

MIN_HISTORY = 25
SHORT_HORIZON = 5
MEDIUM_HORIZON = 15
EFFICIENCY_PERIOD = 14


def compute_momentum_state(series: CandleSeries) -> MomentumState:
    n = len(series)
    if n < MIN_HISTORY:
        return MomentumState(
            short_return=None,
            medium_return=None,
            acceleration=None,
            efficiency=None,
            participation_divergence=None,
            exhaustion_proxy=None,
            available=False,
            reason_codes=(ReasonCode.INSUFFICIENT_HISTORY.value,),
        )

    high = np.array([float(v) for v in series.high], dtype=float)
    low = np.array([float(v) for v in series.low], dtype=float)
    close = np.array([float(v) for v in series.close], dtype=float)
    volume = np.array([float(v) for v in series.volume], dtype=float)

    if not (np.all(np.isfinite(high)) and np.all(np.isfinite(low)) and np.all(np.isfinite(close))):
        return MomentumState(
            short_return=None,
            medium_return=None,
            acceleration=None,
            efficiency=None,
            participation_divergence=None,
            exhaustion_proxy=None,
            available=False,
            reason_codes=(ReasonCode.NONFINITE_FEATURE.value,),
        )

    reason_codes: List[str] = []

    atr_series = atr(high, low, close, period=14)
    current_atr = safe_float(atr_series[-1])

    short_h = min(SHORT_HORIZON, n - 1)
    med_h = min(MEDIUM_HORIZON, n - 1)

    short_return = None
    medium_return = None
    if current_atr and current_atr > 0:
        short_return = safe_float((close[-1] - close[-1 - short_h]) / current_atr)
        medium_return = safe_float((close[-1] - close[-1 - med_h]) / current_atr)
    else:
        reason_codes.append(ReasonCode.NONFINITE_FEATURE.value)

    acceleration = None
    if short_return is not None and medium_return is not None:
        # Compare per-bar pace, not raw magnitude, so horizons of different
        # length are comparable.
        short_pace = short_return / short_h if short_h else None
        medium_pace = medium_return / med_h if med_h else None
        if short_pace is not None and medium_pace is not None:
            acceleration = safe_float(short_pace - medium_pace)

    efficiency = safe_float(efficiency_ratio(close, min(EFFICIENCY_PERIOD, n - 1)))

    participation_divergence = None
    if np.all(np.isfinite(volume)) and len(volume) >= med_h * 2:
        price_slope = linreg_slope_normalized(close[-med_h:])
        volume_slope = linreg_slope_normalized(volume[-med_h:])
        if price_slope is not None and volume_slope is not None:
            participation_divergence = safe_float(price_slope - volume_slope)
    else:
        reason_codes.append(ReasonCode.INSUFFICIENT_HISTORY.value)

    exhaustion_proxy = None
    if medium_return is not None and acceleration is not None and abs(medium_return) > 1e-9:
        decelerating_against_move = (medium_return > 0 and acceleration < 0) or (
            medium_return < 0 and acceleration > 0
        )
        if decelerating_against_move:
            medium_pace = abs(medium_return) / med_h if med_h else None
            if medium_pace and medium_pace > 1e-9:
                exhaustion_proxy = clamp01(abs(acceleration) / medium_pace)
            else:
                exhaustion_proxy = 0.0
        else:
            exhaustion_proxy = 0.0

    return MomentumState(
        short_return=short_return,
        medium_return=medium_return,
        acceleration=acceleration,
        efficiency=efficiency,
        participation_divergence=participation_divergence,
        exhaustion_proxy=exhaustion_proxy,
        available=True,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )
