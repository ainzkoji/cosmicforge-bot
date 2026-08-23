"""
Pure metric calculation functions for the Market Reaction Layer.

All functions are stateless — no DB or exchange dependencies.
The tracker calls these after snapshots have been collected.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# ATR
# ---------------------------------------------------------------------------

def compute_atr(candles: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    """
    Compute ATR from a list of candle dicts with keys:
    candle_high, candle_low, candle_close (and optionally the previous close).
    Returns None when fewer than 2 candles are available.
    """
    if len(candles) < 2:
        return None
    trs = []
    for i in range(1, len(candles)):
        h = candles[i].get("candle_high")
        lo = candles[i].get("candle_low")
        prev_c = candles[i - 1].get("candle_close")
        if None in (h, lo, prev_c):
            continue
        tr = max(h - lo, abs(h - prev_c), abs(lo - prev_c))
        trs.append(tr)
    if not trs:
        return None
    return sum(trs[-period:]) / len(trs[-period:])


# ---------------------------------------------------------------------------
# Realized volatility
# ---------------------------------------------------------------------------

def compute_realized_vol(
    prices: List[float],
    snapshot_interval_minutes: float = 1.0,
) -> Optional[float]:
    """
    Annualized realized volatility from a list of close prices.
    Returns None when fewer than 3 prices are available.
    """
    if len(prices) < 3:
        return None
    log_returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0 and prices[i] > 0
    ]
    if len(log_returns) < 2:
        return None
    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
    std = math.sqrt(variance)
    # Annualize: periods per year = 365 * 24 * 60 / interval_minutes
    periods_per_year = 365 * 24 * 60 / snapshot_interval_minutes
    return std * math.sqrt(periods_per_year)


# ---------------------------------------------------------------------------
# Price move metrics
# ---------------------------------------------------------------------------

def compute_price_moves(
    price_before: float,
    prices_during: List[float],
    price_after_5m: Optional[float],
    price_after_15m: Optional[float],
    price_after_30m: Optional[float],
    price_after_60m: Optional[float],
) -> Dict[str, Any]:
    """
    Compute net_move_pct, max_move_pct, min_move_pct and direction.
    All moves are expressed as % relative to price_before.
    """
    if price_before <= 0:
        return {}

    def _pct(p: Optional[float]) -> Optional[float]:
        if p is None:
            return None
        return (p - price_before) / price_before * 100.0

    all_prices = [p for p in prices_during if p is not None]
    for p in [price_after_5m, price_after_15m, price_after_30m, price_after_60m]:
        if p is not None:
            all_prices.append(p)

    max_move = max((_pct(p) for p in all_prices), default=None)
    min_move = min((_pct(p) for p in all_prices), default=None)
    net_move = _pct(price_after_60m) if price_after_60m else _pct(price_after_30m)

    direction: str
    if net_move is None:
        direction = "FLAT"
    elif net_move > 0.2:
        direction = "UP"
    elif net_move < -0.2:
        direction = "DOWN"
    else:
        direction = "FLAT"

    return {
        "max_move_pct": max_move,
        "min_move_pct": min_move,
        "net_move_pct": net_move,
        "direction_after_event": direction,
        "price_after_5m": price_after_5m,
        "price_after_15m": price_after_15m,
        "price_after_30m": price_after_30m,
        "price_after_60m": price_after_60m,
    }


def compute_continuation_or_reversal(
    price_before: float,
    price_at_event: Optional[float],
    price_after_60m: Optional[float],
) -> str:
    """
    Compare initial direction (before → at event) to 60m direction.
    Returns CONTINUATION, REVERSAL, or NEUTRAL.
    """
    if price_before <= 0 or price_at_event is None or price_after_60m is None:
        return "NEUTRAL"

    initial_move = price_at_event - price_before
    final_move = price_after_60m - price_before

    if abs(final_move) / price_before * 100 < 0.2:
        return "NEUTRAL"

    if (initial_move >= 0 and final_move >= 0) or (initial_move < 0 and final_move < 0):
        return "CONTINUATION"
    return "REVERSAL"


# ---------------------------------------------------------------------------
# Volatility expansion
# ---------------------------------------------------------------------------

def compute_volatility_expansion(
    atr_before: Optional[float],
    atr_after: Optional[float],
) -> Optional[float]:
    """volatility_expansion_ratio = atr_after / atr_before"""
    if not atr_before or not atr_after or atr_before <= 0:
        return None
    return atr_after / atr_before


def compute_candle_range(candles: List[Dict[str, Any]]) -> Optional[float]:
    """Median (high-low) candle range across provided candles."""
    ranges = []
    for c in candles:
        h = c.get("candle_high")
        lo = c.get("candle_low")
        if h is not None and lo is not None:
            ranges.append(h - lo)
    if not ranges:
        return None
    ranges.sort()
    mid = len(ranges) // 2
    return ranges[mid]


# ---------------------------------------------------------------------------
# Volume metrics
# ---------------------------------------------------------------------------

def compute_volume_metrics(
    pre_volumes: List[float],
    event_volume: Optional[float],
    vol_spike_threshold: float = 3.0,
) -> Dict[str, Any]:
    pre_clean = [v for v in pre_volumes if v is not None and v > 0]
    avg_before = sum(pre_clean) / len(pre_clean) if pre_clean else None

    spike_ratio: Optional[float] = None
    abnormal_score: Optional[float] = None
    if avg_before and avg_before > 0 and event_volume is not None:
        spike_ratio = event_volume / avg_before
        abnormal_score = spike_ratio / vol_spike_threshold

    return {
        "average_volume_before": avg_before,
        "event_volume": event_volume,
        "volume_spike_ratio": spike_ratio,
        "abnormal_volume_score": abnormal_score,
    }


# ---------------------------------------------------------------------------
# Spread / liquidity metrics
# ---------------------------------------------------------------------------

def compute_spread_metrics(
    spread_before: Optional[float],
    spread_during: Optional[float],
    spread_after: Optional[float],
    threshold: float = 2.0,
) -> Dict[str, Any]:
    widening: Optional[float] = None
    if spread_before and spread_before > 0 and spread_during is not None:
        widening = spread_during / spread_before
    return {
        "spread_before": spread_before,
        "spread_during": spread_during,
        "spread_after": spread_after,
        "spread_widening_ratio": widening,
    }


# ---------------------------------------------------------------------------
# Confidence score / data quality
# ---------------------------------------------------------------------------

def compute_confidence_and_quality(
    has_pre60: bool,
    has_event: bool,
    has_post30: bool,
    has_post60: bool,
    pre60_count: int,
    event_count: int,
    post60_count: int,
    exchange_error: bool = False,
) -> Tuple[float, str]:
    """
    Returns (confidence_score 0.0–1.0, data_quality label).
    """
    if exchange_error:
        return 0.0, "EXCHANGE_DATA_ERROR"

    score = 1.0
    if not has_pre60:
        score -= 0.3
    if not has_event:
        score -= 0.4
    if not has_post60:
        score -= 0.2
    if not has_post30:
        score -= 0.1

    # Penalize sparse windows (fewer than 3 snapshots per expected window)
    for count in (pre60_count, event_count, post60_count):
        if 0 < count < 3:
            score -= 0.05
    score = max(0.0, score)

    if not has_pre60:
        quality = "MISSING_PRE_EVENT_DATA"
    elif not has_post60 and not has_post30:
        quality = "MISSING_POST_EVENT_DATA"
    elif score < 0.5:
        quality = "LOW_CONFIDENCE"
    elif score < 1.0:
        quality = "PARTIAL"
    else:
        quality = "COMPLETE"

    return round(score, 3), quality
