"""
Rule-based reaction classifier for the Phase 2 Market Reaction Layer.

This layer stays observational and uses a deliberately small taxonomy:
  NO_REACTION
  VOL_SPIKE
  TREND_CONTINUATION
  REVERSAL
  WHIPSAW
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def classify_reaction(
    *,
    data_quality: str,
    reaction_type_current: str = "NO_REACTION",
    volatility_expansion_ratio: Optional[float],
    volume_spike_ratio: Optional[float],
    spread_widening_ratio: Optional[float],
    net_move_pct: Optional[float],
    max_move_pct: Optional[float],
    min_move_pct: Optional[float],
    direction_after_event: Optional[str],
    continuation_or_reversal: Optional[str],
    price_before_event: Optional[float],
    price_after_5m: Optional[float],
    price_after_15m: Optional[float],
    price_after_30m: Optional[float],
    price_after_60m: Optional[float],
    atr_before: Optional[float],
    atr_after: Optional[float],
    # thresholds — injected from config so tests can override
    vol_spike_threshold: float = 2.5,
    volume_spike_threshold: float = 3.0,
    spread_widening_threshold: float = 2.0,
) -> str:
    """Return the reaction type string for one event+symbol."""

    ve = volatility_expansion_ratio or 0.0
    net = net_move_pct or 0.0
    max_m = max_move_pct or 0.0
    min_m = min_move_pct or 0.0
    vol_ratio = volume_spike_ratio or 0.0
    cont = continuation_or_reversal or "NEUTRAL"
    max_abs_move = max(abs(max_m), abs(min_m))

    # Missing or degraded data is handled by data_quality/confidence, not a separate label.
    if data_quality == "EXCHANGE_DATA_ERROR":
        return "NO_REACTION"

    # 1. NO_REACTION — nothing meaningful happened
    if (
        ve < 1.3
        and vol_ratio < 1.5
        and abs(net) < 0.3
    ):
        return "NO_REACTION"

    # 2. WHIPSAW — big movement / vol but price returned near origin quickly
    if price_before_event and price_after_15m is not None:
        move_back_pct = abs(price_after_15m - price_before_event) / price_before_event * 100
        if (ve > vol_spike_threshold or max_abs_move > 1.0) and move_back_pct < 0.5:
            return "WHIPSAW"

    # 3. REVERSAL — initial move and settled move disagree meaningfully
    if (
        (cont == "REVERSAL" or _signs_differ(max_m, net))
        and max_abs_move > 1.0
        and abs(net) > 0.5
    ):
        return "REVERSAL"

    # 4. TREND_CONTINUATION — directional move held after the event
    if (
        abs(net) > 0.5
        and cont == "CONTINUATION"
        and direction_after_event in ("UP", "DOWN")
        and _confirmed_at_both(price_before_event, price_after_30m, price_after_60m, direction_after_event)
    ):
        return "TREND_CONTINUATION"

    # 5. VOL_SPIKE — elevated volatility or volume without a durable directional story
    if (
        ve > vol_spike_threshold
        or vol_ratio > volume_spike_threshold
        or (spread_widening_ratio or 0.0) > spread_widening_threshold
    ):
        return "VOL_SPIKE"

    return "NO_REACTION"


def _signs_differ(a: float, b: float) -> bool:
    """True when a and b have opposite signs (treating near-zero as positive)."""
    if abs(a) < 0.01 or abs(b) < 0.01:
        return False
    return (a > 0) != (b > 0)


def _confirmed_at_both(
    price_before: Optional[float],
    price_after_30m: Optional[float],
    price_after_60m: Optional[float],
    direction: str,
) -> bool:
    """Both 30m and 60m prices confirm the expected direction vs price_before."""
    if price_before is None or price_after_30m is None or price_after_60m is None:
        return False
    if price_before <= 0:
        return False
    move_30 = price_after_30m - price_before
    move_60 = price_after_60m - price_before
    if direction == "UP":
        return move_30 > 0 and move_60 > 0
    if direction == "DOWN":
        return move_30 < 0 and move_60 < 0
    return False


def build_natural_language_summary(
    event_title: str,
    symbol: str,
    reaction_type: str,
    volatility_expansion_ratio: Optional[float],
    net_move_pct: Optional[float],
    price_after_15m: Optional[float],
    price_before_event: Optional[float],
    data_quality: str,
) -> str:
    """Produce a one-sentence human-readable reaction summary for the admin UI."""
    ve_str = f"{volatility_expansion_ratio:.1f}x" if volatility_expansion_ratio else "unknown"
    net_str = f"{net_move_pct:+.2f}%" if net_move_pct is not None else "unknown"

    minutes_to_return: Optional[float] = None
    if (
        reaction_type in ("WHIPSAW", "FAKEOUT")
        and price_before_event
        and price_after_15m is not None
    ):
        if abs(price_after_15m - price_before_event) / price_before_event * 100 < 0.5:
            minutes_to_return = 15.0

    summaries: Dict[str, str] = {
        "NO_REACTION": f"{event_title}: {symbol} showed NO significant reaction (net {net_str}).",
        "VOL_SPIKE": (
            f"{event_title}: {symbol} showed a volatility spike with {ve_str} "
            f"ATR expansion (net move {net_str})."
        ),
        "TREND_CONTINUATION": (
            f"{event_title}: {symbol} continued its trend after the event "
            f"(net {net_str}, {ve_str} volatility expansion)."
        ),
        "REVERSAL": (
            f"{event_title}: {symbol} reversed after the event — "
            f"initial spike then held in the opposite direction (net {net_str})."
        ),
        "WHIPSAW": (
            f"{event_title}: {symbol} showed WHIPSAW behavior with "
            f"{ve_str} volatility expansion"
            + (f", returning near pre-event level within {minutes_to_return:.0f} minutes." if minutes_to_return else ".")
        ),
    }

    base = summaries.get(reaction_type, f"{event_title}: {symbol} reaction: {reaction_type}.")
    if data_quality in ("PARTIAL", "LOW_CONFIDENCE", "MISSING_PRE_EVENT_DATA", "MISSING_POST_EVENT_DATA"):
        base += f" [Data quality: {data_quality}]"
    return base
