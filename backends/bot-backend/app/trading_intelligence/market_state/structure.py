"""Structure detector (Section 9.3) -- objective structural facts only.

Never emits BUY/SELL. Uses causal swing-pivot detection with an explicit
confirmation lag: a pivot at index ``i`` is only confirmed once
``i + lag`` bars have closed, so no pivot is ever recognized using a bar
that had not closed yet at decision time.
"""
from __future__ import annotations

from typing import List, Tuple

import numpy as np

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import CandleSeries, StructureState, safe_float, clamp01
from app.trading_intelligence.market_state.indicators import atr

#: Bars required on each side of a candidate pivot before it is confirmed.
PIVOT_CONFIRMATION_LAG = 2
#: Minimum bars of causal history required to attempt structure detection.
MIN_HISTORY = 30
#: Lookback window for range high/low and failed-break counting.
RANGE_LOOKBACK = 50


def _confirmed_pivots(high: np.ndarray, low: np.ndarray, lag: int) -> Tuple[List[int], List[int]]:
    """Indices of confirmed swing highs and swing lows.

    A candidate at index i is a pivot high if high[i] is the strict max of
    high[i-lag:i+lag+1]; only indices with i+lag <= last_index are ever
    evaluated, so the pivot is never confirmed using a future,
    not-yet-closed bar.
    """
    n = len(high)
    last_confirmable = n - 1 - lag
    highs_idx: List[int] = []
    lows_idx: List[int] = []
    for i in range(lag, last_confirmable + 1):
        window_h = high[i - lag : i + lag + 1]
        if high[i] == np.max(window_h) and np.argmax(window_h) == lag:
            highs_idx.append(i)
        window_l = low[i - lag : i + lag + 1]
        if low[i] == np.min(window_l) and np.argmin(window_l) == lag:
            lows_idx.append(i)
    return highs_idx, lows_idx


def compute_structure_state(series: CandleSeries) -> StructureState:
    n = len(series)
    if n < MIN_HISTORY:
        return StructureState(
            swing_sequence="UNRESOLVED",
            last_bos_direction="NONE",
            choch_direction="NONE",
            range_high=None,
            range_low=None,
            distance_to_support_atr=None,
            distance_to_resistance_atr=None,
            failed_break_count=0,
            structure_integrity=0.0,
            invalidation_reference=None,
            pivot_confirmation_lag=PIVOT_CONFIRMATION_LAG,
            available=False,
            reason_codes=(ReasonCode.INSUFFICIENT_HISTORY.value,),
        )

    high = np.array([float(v) for v in series.high], dtype=float)
    low = np.array([float(v) for v in series.low], dtype=float)
    close = np.array([float(v) for v in series.close], dtype=float)

    if not (np.all(np.isfinite(high)) and np.all(np.isfinite(low)) and np.all(np.isfinite(close))):
        return StructureState(
            swing_sequence="UNRESOLVED",
            last_bos_direction="NONE",
            choch_direction="NONE",
            range_high=None,
            range_low=None,
            distance_to_support_atr=None,
            distance_to_resistance_atr=None,
            failed_break_count=0,
            structure_integrity=0.0,
            invalidation_reference=None,
            pivot_confirmation_lag=PIVOT_CONFIRMATION_LAG,
            available=False,
            reason_codes=(ReasonCode.NONFINITE_FEATURE.value,),
        )

    atr_series = atr(high, low, close, period=14)
    current_atr = safe_float(atr_series[-1])

    pivot_highs, pivot_lows = _confirmed_pivots(high, low, PIVOT_CONFIRMATION_LAG)

    reason_codes: List[str] = []

    # -- swing sequence from the last two confirmed pivots of each kind -----
    swing_sequence = "UNRESOLVED"
    if len(pivot_highs) >= 2 and len(pivot_lows) >= 2:
        higher_high = high[pivot_highs[-1]] > high[pivot_highs[-2]]
        higher_low = low[pivot_lows[-1]] > low[pivot_lows[-2]]
        lower_high = high[pivot_highs[-1]] < high[pivot_highs[-2]]
        lower_low = low[pivot_lows[-1]] < low[pivot_lows[-2]]
        if higher_high and higher_low:
            swing_sequence = "HH_HL"
        elif lower_high and lower_low:
            swing_sequence = "LH_LL"
        else:
            swing_sequence = "MIXED"
    # Fewer than two confirmed pivots of each kind is a soft, expected
    # outcome (e.g. a near-monotonic run) -- swing_sequence="UNRESOLVED"
    # and structure_integrity=0.0 already say so. This is NOT the critical
    # "not enough candles at all" case (that early-returns available=False
    # above with INSUFFICIENT_HISTORY), so it must never contribute that
    # critical reason code here.

    # -- last confirmed swing extremes, for BOS/CHOCH and support/resistance -
    last_swing_high = float(high[pivot_highs[-1]]) if pivot_highs else None
    last_swing_low = float(low[pivot_lows[-1]]) if pivot_lows else None
    prev_swing_high = float(high[pivot_highs[-2]]) if len(pivot_highs) >= 2 else None
    prev_swing_low = float(low[pivot_lows[-2]]) if len(pivot_lows) >= 2 else None

    current_close = float(close[-1])

    last_bos_direction = "NONE"
    if last_swing_high is not None and current_close > last_swing_high:
        last_bos_direction = "UP"
    elif last_swing_low is not None and current_close < last_swing_low:
        last_bos_direction = "DOWN"

    # CHOCH: prior character was down (LH_LL) and price breaks the most
    # recent swing high, or prior character was up (HH_HL) and price breaks
    # the most recent swing low.
    choch_direction = "NONE"
    if swing_sequence == "LH_LL" and last_swing_high is not None and current_close > last_swing_high:
        choch_direction = "UP"
    elif swing_sequence == "HH_HL" and last_swing_low is not None and current_close < last_swing_low:
        choch_direction = "DOWN"

    # -- range and failed-break count over the trailing lookback ------------
    lookback = min(RANGE_LOOKBACK, n)
    range_high = float(np.max(high[-lookback:]))
    range_low = float(np.min(low[-lookback:]))

    failed_break_count = 0
    if pivot_highs or pivot_lows:
        relevant_highs = [h for h in pivot_highs if h >= n - lookback]
        relevant_lows = [l for l in pivot_lows if l >= n - lookback]
        for idx in relevant_highs:
            level = high[idx]
            after = close[idx + 1 : idx + 1 + PIVOT_CONFIRMATION_LAG + 3]
            if len(after) and np.any(after > level) and after[-1] < level:
                failed_break_count += 1
        for idx in relevant_lows:
            level = low[idx]
            after = close[idx + 1 : idx + 1 + PIVOT_CONFIRMATION_LAG + 3]
            if len(after) and np.any(after < level) and after[-1] > level:
                failed_break_count += 1

    distance_to_support_atr = None
    distance_to_resistance_atr = None
    if current_atr and current_atr > 0:
        if last_swing_low is not None:
            distance_to_support_atr = safe_float((current_close - last_swing_low) / current_atr)
        if last_swing_high is not None:
            distance_to_resistance_atr = safe_float((last_swing_high - current_close) / current_atr)
    else:
        reason_codes.append(ReasonCode.NONFINITE_FEATURE.value)

    # -- structure integrity: consistency + ATR-normalized swing prominence -
    integrity = 0.0
    if swing_sequence in ("HH_HL", "LH_LL") and current_atr and current_atr > 0:
        if prev_swing_high is not None and prev_swing_low is not None:
            amplitude = abs(last_swing_high - last_swing_low) if last_swing_high and last_swing_low else 0.0
            prominence = clamp01(amplitude / (current_atr * 4.0)) if current_atr else 0.0
            consistency = 1.0 - clamp01(failed_break_count / 3.0)
            integrity = clamp01(0.6 * prominence + 0.4 * consistency)
    elif swing_sequence == "MIXED":
        integrity = clamp01(0.2 - 0.05 * failed_break_count)

    invalidation_reference = None
    if last_bos_direction == "UP":
        invalidation_reference = last_swing_low
    elif last_bos_direction == "DOWN":
        invalidation_reference = last_swing_high
    elif last_swing_low is not None and last_swing_high is not None:
        invalidation_reference = last_swing_low if abs(current_close - last_swing_low) < abs(
            current_close - last_swing_high
        ) else last_swing_high

    return StructureState(
        swing_sequence=swing_sequence,
        last_bos_direction=last_bos_direction,
        choch_direction=choch_direction,
        range_high=range_high,
        range_low=range_low,
        distance_to_support_atr=distance_to_support_atr,
        distance_to_resistance_atr=distance_to_resistance_atr,
        failed_break_count=failed_break_count,
        structure_integrity=integrity,
        invalidation_reference=invalidation_reference,
        pivot_confirmation_lag=PIVOT_CONFIRMATION_LAG,
        available=True,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )
