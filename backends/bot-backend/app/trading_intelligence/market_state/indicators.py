"""Shared causal math for the feature-family calculators.

Every function here only ever reads the values already given to it -- callers
are responsible for passing a causally-cut ``CandleSeries`` (Section 9.1). No
function reaches outside its arguments for "current time" or fetches
anything; that is what makes them trivially replay/live deterministic
(P1 causality, P2 determinism).

This is intentionally independent of ``app/strategy/regime.py``'s hand-rolled
ADX/ATR: that module is tightly coupled to the V2 threshold/strategy path
(P5 -- one alpha authority), and CATI must not create a hidden runtime
dependency on it that a V2 change could silently break CATI through, or vice
versa.
"""
from __future__ import annotations

from typing import Optional

import numpy as np


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    prev_close = np.concatenate(([close[0]], close[:-1]))
    a = high - low
    b = np.abs(high - prev_close)
    c = np.abs(low - prev_close)
    return np.maximum(a, np.maximum(b, c))


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """Wilder-smoothed ATR, causal at every index (index i only uses data <= i)."""
    tr = true_range(high, low, close)
    n = len(tr)
    out = np.full(n, np.nan)
    if n < period:
        return out
    out[period - 1] = np.mean(tr[:period])
    for i in range(period, n):
        out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
    return out


def log_returns(close: np.ndarray) -> np.ndarray:
    """Length-preserving log returns; index 0 is NaN (no prior close)."""
    out = np.full(len(close), np.nan)
    if len(close) < 2:
        return out
    with np.errstate(divide="ignore", invalid="ignore"):
        prev = close[:-1]
        cur = close[1:]
        valid = (prev > 0) & (cur > 0)
        ratios = np.full(len(prev), np.nan)
        ratios[valid] = cur[valid] / prev[valid]
        out[1:] = np.log(ratios, where=~np.isnan(ratios), out=np.full(len(ratios), np.nan))
    return out


def rolling_std_causal(values: np.ndarray, window: int) -> np.ndarray:
    """out[i] = std(values[i-window+1 : i+1]); NaN until enough history."""
    n = len(values)
    out = np.full(n, np.nan)
    for i in range(window - 1, n):
        segment = values[i - window + 1 : i + 1]
        if np.all(np.isfinite(segment)):
            out[i] = np.std(segment, ddof=0)
    return out


def percentile_rank_of_last(values: np.ndarray, window: Optional[int] = None) -> Optional[float]:
    """Percentile (0..1) of the last finite value within its own trailing
    window of finite history -- never looks past the last index."""
    finite = values[np.isfinite(values)]
    if window is not None:
        finite = finite[-window:] if len(finite) > window else finite
    if len(finite) < 2:
        return None
    last = finite[-1]
    rank = float(np.sum(finite <= last)) / float(len(finite))
    return rank


def linreg_slope_normalized(y: np.ndarray) -> Optional[float]:
    """OLS slope of y against 0..n-1, normalized by mean|y| so it is
    comparable across instruments/price scales. None if degenerate."""
    finite_mask = np.isfinite(y)
    if int(np.sum(finite_mask)) < 3:
        return None
    x = np.arange(len(y))[finite_mask]
    yy = y[finite_mask]
    mean_abs = np.mean(np.abs(yy))
    if mean_abs == 0 or not np.isfinite(mean_abs):
        return None
    slope = np.polyfit(x, yy, 1)[0]
    normalized = slope * len(yy) / mean_abs
    if not np.isfinite(normalized):
        return None
    return float(normalized)


def efficiency_ratio(close: np.ndarray, period: int) -> Optional[float]:
    """Kaufman efficiency ratio over the trailing ``period`` closes:
    |net change| / sum(|bar-to-bar change|). 1.0 = perfectly efficient
    (straight line), ~0 = pure noise."""
    if len(close) < period + 1:
        return None
    window = close[-(period + 1) :]
    net_change = abs(window[-1] - window[0])
    diffs = np.abs(np.diff(window))
    total_movement = float(np.sum(diffs))
    if total_movement == 0:
        return None
    ratio = net_change / total_movement
    if not np.isfinite(ratio):
        return None
    return float(ratio)
