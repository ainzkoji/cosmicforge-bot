"""Daily trend signal rules: pure, vectorised, causal.

Every function takes day x symbol matrices (rows in time order, NaN = no bar) and returns a matrix of the same
shape whose row t depends ONLY on rows 0..t. Nothing here knows about accounts, orders or dates: the research
evaluator and the future pipeline call the same functions on the same kind of input.

"Previous L days" always means the L rows BEFORE row t. A window that contains a missing bar has no value, so a
rule that needs it cannot fire -- missing data never produces a signal and is never filled.
"""
from __future__ import annotations

from typing import Sequence, Tuple

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from .spec import LOOKBACKS, SPECIFICATION


def exit_lookback(lookback: int) -> int:
    """SPEC: "the previous L/2 days (rounded down, minimum 5)"."""
    return max(5, int(lookback) // 2)


def _previous_window(x: np.ndarray, n: int, reducer) -> np.ndarray:
    """``reducer`` over rows t-n .. t-1 for every row t; NaN where that window is incomplete or has a NaN."""
    out = np.full(x.shape, np.nan)
    if x.shape[0] > n:
        windows = sliding_window_view(x, n, axis=0)          # windows[k] covers rows k .. k+n-1
        out[n:] = reducer(windows[:-1], axis=-1)             # row t uses rows t-n .. t-1 (NaN propagates)
    return out


def previous_high(x: np.ndarray, n: int) -> np.ndarray:
    return _previous_window(x, n, np.max)


def previous_low(x: np.ndarray, n: int) -> np.ndarray:
    return _previous_window(x, n, np.min)


def subsignal(close: np.ndarray, lookback: int) -> np.ndarray:
    """One breakout sub-signal as a boolean matrix.

    ON when the close is at or above the highest close of the previous L days; OFF when it is below the lowest
    close of the previous L/2 days; otherwise unchanged. It starts OFF and a day without a bar resets it to OFF.
    The two conditions cannot both hold (the exit window lies inside the entry window)."""
    close = np.asarray(close, dtype=np.float64)
    with np.errstate(invalid="ignore"):
        turn_on = close >= previous_high(close, lookback)
        turn_off = close < previous_low(close, exit_lookback(lookback))
    event = np.where(np.isnan(close) | turn_off, -1, np.where(turn_on, 1, 0))
    rows = np.arange(close.shape[0])[:, None]
    last = np.maximum.accumulate(np.where(event != 0, rows, -1), axis=0)     # row of the latest event, or -1
    return (last >= 0) & (np.take_along_axis(event, np.maximum(last, 0), axis=0) == 1)


def strength(close: np.ndarray, lookbacks: Sequence[int] = LOOKBACKS) -> np.ndarray:
    """Fraction of the sub-signals that are ON: 0, 1/7, ... 1. The divisor is always the number of lookbacks."""
    total = np.zeros(np.asarray(close).shape, dtype=np.float64)
    for lookback in lookbacks:
        total += subsignal(close, lookback)
    return total / float(len(lookbacks))


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    """max(high - low, |high - previous close|, |low - previous close|); NaN without a previous close."""
    prev = np.full(close.shape, np.nan)
    prev[1:] = close[:-1]
    with np.errstate(invalid="ignore"):
        tr = np.maximum(high - low, np.maximum(np.abs(high - prev), np.abs(low - prev)))
    return np.where(np.isnan(prev), np.nan, tr)


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, days: int = SPECIFICATION["atr_days"]) -> np.ndarray:
    """Simple mean of the ``days`` true ranges ending on row t (needs days + 1 consecutive bars)."""
    tr = true_range(high, low, close)
    out = np.full(tr.shape, np.nan)
    if tr.shape[0] >= days:
        out[days - 1:] = sliding_window_view(tr, days, axis=0).mean(axis=-1)
    return out


def stop_distance(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    """d = 3 x ATR(20) / close, floored at 5% and capped at 40%; NaN where ATR is undefined."""
    s = SPECIFICATION
    with np.errstate(invalid="ignore", divide="ignore"):
        d = s["stop_atr_multiple"] * atr(high, low, close) / close
    return np.where(np.isnan(d), np.nan, np.clip(d, s["stop_floor"], s["stop_cap"]))


def consecutive_bars(close: np.ndarray) -> np.ndarray:
    """Number of consecutive bars ending on row t (0 on a day without a bar)."""
    valid = ~np.isnan(close)
    rows = np.arange(close.shape[0])[:, None]
    last_gap = np.maximum.accumulate(np.where(valid, -1, rows), axis=0)       # row of the latest missing bar
    return np.where(valid, rows - last_gap, 0)


def previous_median(x: np.ndarray, n: int, chunk: int = 64) -> np.ndarray:
    """Median of rows t-n .. t-1; NaN where that window is incomplete. Chunked by column to bound memory."""
    out = np.full(x.shape, np.nan)
    if x.shape[0] > n:
        for c in range(0, x.shape[1], chunk):
            windows = sliding_window_view(x[:, c:c + chunk], n, axis=0)
            med = np.median(windows[:-1], axis=-1)
            out[n:, c:c + chunk] = np.where(np.isnan(windows[:-1]).any(axis=-1), np.nan, med)
    return out


def universe(close: np.ndarray, quote_volume: np.ndarray, in_scope: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Point-in-time membership ``(member, volume)``.

    On each day: among in-scope contracts with a bar on each of the 120 days ending that day, the 20 with the
    highest median daily quote volume over the previous 30 days. Ties go to the lower column index (the caller
    orders columns by symbol). ``in_scope`` is the per-symbol static filter (stablecoin pairs, non-crypto)."""
    s = SPECIFICATION
    volume = previous_median(quote_volume, s["universe_volume_days"])
    eligible = (consecutive_bars(close) >= s["universe_min_history_days"]) & np.asarray(in_scope, dtype=bool)[None, :]
    eligible &= ~np.isnan(volume)
    score = np.where(eligible, volume, -np.inf)
    order = np.argsort(-score, axis=1, kind="stable")[:, :s["universe_size"]]
    member = np.zeros(close.shape, dtype=bool)
    np.put_along_axis(member, order, True, axis=1)
    return member & eligible, volume


def funding_annualised(funding_total: np.ndarray, days: int = SPECIFICATION["funding_overlay"]["trailing_days"]) -> np.ndarray:
    """Mean daily funding over the ``days`` rows ending on row t, times 365 (equal to the mean rate per event
    times the number of events per year when events are regular)."""
    out = np.full(funding_total.shape, np.nan)
    if funding_total.shape[0] >= days:
        out[days - 1:] = sliding_window_view(funding_total, days, axis=0).mean(axis=-1) * 365.0
    return out


def funding_overlay_factor(annualised: np.ndarray) -> np.ndarray:
    """Secondary test: 0.5 above 30%, 0 above 60%, 1 otherwise (and where the rate is unknown)."""
    o = SPECIFICATION["funding_overlay"]
    with np.errstate(invalid="ignore"):
        return np.where(annualised > o["zero_above_annualised"], 0.0,
                        np.where(annualised > o["halve_above_annualised"], 0.5, 1.0))


__all__ = ["exit_lookback", "previous_high", "previous_low", "subsignal", "strength", "true_range", "atr",
           "stop_distance", "consecutive_bars", "previous_median", "universe", "funding_annualised",
           "funding_overlay_factor"]
