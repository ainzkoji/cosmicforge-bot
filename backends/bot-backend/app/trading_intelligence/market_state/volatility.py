"""Volatility state (Section 9.5).

``compression`` has exactly one canonical V1 definition here -- the trailing
percentile rank of ATR (low percentile = compressed) -- not five competing
scores. ``expansion`` is current true range over trailing median true range.
Both are documented so a future version bump is a deliberate, versioned
change, not silent drift.
"""
from __future__ import annotations

from typing import List

import numpy as np

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import CandleSeries, VolatilityState, safe_float
from app.trading_intelligence.market_state.indicators import (
    atr,
    log_returns,
    percentile_rank_of_last,
    rolling_std_causal,
    true_range,
)

MIN_HISTORY = 30
RV_WINDOW = 20
RV_SHORT_WINDOW = 10
PERCENTILE_WINDOW = 100
#: current true range / trailing median true range beyond this is a shock.
SHOCK_EXPANSION_MULTIPLE = 3.0
SHOCK_PERCENTILE = 0.98


def compute_volatility_state(series: CandleSeries) -> VolatilityState:
    n = len(series)
    if n < MIN_HISTORY:
        return VolatilityState(
            realized_vol=None,
            percentile=None,
            atr_percentile=None,
            vol_acceleration=None,
            vol_of_vol=None,
            compression=None,
            expansion=None,
            shock_state=False,
            available=False,
            reason_codes=(ReasonCode.INSUFFICIENT_HISTORY.value,),
        )

    high = np.array([float(v) for v in series.high], dtype=float)
    low = np.array([float(v) for v in series.low], dtype=float)
    close = np.array([float(v) for v in series.close], dtype=float)

    if not (np.all(np.isfinite(high)) and np.all(np.isfinite(low)) and np.all(np.isfinite(close))):
        return VolatilityState(
            realized_vol=None,
            percentile=None,
            atr_percentile=None,
            vol_acceleration=None,
            vol_of_vol=None,
            compression=None,
            expansion=None,
            shock_state=False,
            available=False,
            reason_codes=(ReasonCode.NONFINITE_FEATURE.value,),
        )

    reason_codes: List[str] = []

    returns = log_returns(close)
    rv_series = rolling_std_causal(returns, min(RV_WINDOW, n - 1))
    realized_vol = safe_float(rv_series[-1])

    rv_short_series = rolling_std_causal(returns, min(RV_SHORT_WINDOW, n - 1))
    rv_short = safe_float(rv_short_series[-1])

    vol_acceleration = None
    if realized_vol and realized_vol > 0 and rv_short is not None:
        vol_acceleration = safe_float(rv_short / realized_vol)

    percentile = percentile_rank_of_last(rv_series, window=PERCENTILE_WINDOW)
    if percentile is None:
        reason_codes.append(ReasonCode.INSUFFICIENT_HISTORY.value)

    vol_of_vol = None
    finite_rv = rv_series[np.isfinite(rv_series)]
    if len(finite_rv) >= 5:
        tail = finite_rv[-PERCENTILE_WINDOW:]
        vol_of_vol = safe_float(np.std(tail))

    atr_series = atr(high, low, close, period=14)
    atr_pct_series = np.where((close > 0) & np.isfinite(atr_series), atr_series / np.where(close == 0, np.nan, close), np.nan)
    atr_percentile = percentile_rank_of_last(atr_pct_series, window=PERCENTILE_WINDOW)
    if atr_percentile is None:
        reason_codes.append(ReasonCode.INSUFFICIENT_HISTORY.value)

    compression = atr_percentile  # canonical V1 compression measure (see module docstring)

    tr = true_range(high, low, close)
    trailing_window = tr[-(min(20, n)) : -1] if n > 1 else tr[:0]
    trailing_median_tr = float(np.median(trailing_window)) if len(trailing_window) else None
    expansion = None
    if trailing_median_tr and trailing_median_tr > 0:
        expansion = safe_float(tr[-1] / trailing_median_tr)

    shock_state = False
    if expansion is not None and expansion >= SHOCK_EXPANSION_MULTIPLE:
        shock_state = True
    if percentile is not None and percentile >= SHOCK_PERCENTILE:
        shock_state = True

    return VolatilityState(
        realized_vol=realized_vol,
        percentile=percentile,
        atr_percentile=atr_percentile,
        vol_acceleration=vol_acceleration,
        vol_of_vol=vol_of_vol,
        compression=compression,
        expansion=expansion,
        shock_state=shock_state,
        available=True,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )
