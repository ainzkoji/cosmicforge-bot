"""Cohort dimensions and deterministic hierarchical backoff (Sections 12.5,
12.6). The backoff sequence follows the spec's own example verbatim (audit
found no existing canonical ordering to prefer instead) -- explicit,
versioned via ``COHORT_SCHEMA_VERSION``, and recorded on every forecast as
``backoff_level``.
"""
from __future__ import annotations

from typing import Any, Mapping, Optional, Tuple

from app.trading_intelligence.versions import COHORT_SCHEMA_VERSION

#: Every cohort dimension, in canonical order. A cohort signature always
#: lists retained dimensions in this order, regardless of backoff level.
DIMENSION_ORDER: Tuple[str, ...] = (
    "setup_family",
    "side",
    "dominant_regime",
    "volatility_bucket",
    "liquidity_bucket",
    "trend_maturity",
    "htf_alignment",
    "funding_crowding_bucket",
    "instrument_group",
)

#: Section 12.6's explicit example sequence, used verbatim.
BACKOFF_LEVELS: Tuple[Tuple[str, ...], ...] = (
    DIMENSION_ORDER,  # level 0: all dimensions
    tuple(d for d in DIMENSION_ORDER if d != "instrument_group"),  # level 1
    tuple(d for d in DIMENSION_ORDER if d not in ("instrument_group", "funding_crowding_bucket")),  # level 2
    tuple(d for d in DIMENSION_ORDER if d not in ("instrument_group", "funding_crowding_bucket", "liquidity_bucket")),  # level 3
    tuple(
        d for d in DIMENSION_ORDER
        if d not in ("instrument_group", "funding_crowding_bucket", "liquidity_bucket", "htf_alignment")
    ),  # level 4
    ("setup_family", "side", "dominant_regime"),  # level 5
    ("setup_family", "side"),  # level 6
    ("setup_family",),  # level 7
)

UNKNOWN = "UNKNOWN"


def volatility_bucket(volatility_state: Any) -> str:
    if not volatility_state.available or volatility_state.percentile is None:
        return UNKNOWN
    p = volatility_state.percentile
    if p < 0.33:
        return "LOW"
    if p < 0.67:
        return "MEDIUM"
    return "HIGH"


def liquidity_bucket(liquidity_state: Any) -> str:
    if not liquidity_state.available or liquidity_state.spread_percentile is None:
        return "UNVERIFIED"
    return "GOOD" if liquidity_state.spread_percentile < 0.5 else "DEGRADED"


def htf_alignment_bucket(higher_timeframe_state: Any) -> str:
    if not higher_timeframe_state.available or higher_timeframe_state.structure_alignment is None:
        return UNKNOWN
    return higher_timeframe_state.structure_alignment


def funding_crowding_bucket(derivatives_state: Any) -> str:
    if not derivatives_state.available or derivatives_state.crowding_state is None:
        return UNKNOWN
    return derivatives_state.crowding_state


def derive_cohort_dimensions(
    *, setup_family: str, side: str, market_state: Any, regime_distribution: Any, instrument_group: Optional[str] = None
) -> Mapping[str, str]:
    """The one place cohort dimensions are computed -- used identically at
    labeling time (building the library) and at forecast time (looking a
    candidate up), so the two can never silently drift apart."""
    return {
        "setup_family": setup_family,
        "side": side,
        "dominant_regime": regime_distribution.dominant_regime,
        "volatility_bucket": volatility_bucket(market_state.volatility_state),
        "liquidity_bucket": liquidity_bucket(market_state.liquidity_state),
        "trend_maturity": market_state.trend_state.maturity if market_state.trend_state.available else UNKNOWN,
        "htf_alignment": htf_alignment_bucket(market_state.higher_timeframe_state),
        "funding_crowding_bucket": funding_crowding_bucket(market_state.derivatives_state),
        "instrument_group": instrument_group or UNKNOWN,
    }


def cohort_signature(dims: Mapping[str, str], level_dims: Tuple[str, ...]) -> str:
    return "|".join(f"{d}={dims.get(d, UNKNOWN)}" for d in DIMENSION_ORDER if d in level_dims)


__all__ = [
    "DIMENSION_ORDER",
    "BACKOFF_LEVELS",
    "UNKNOWN",
    "volatility_bucket",
    "liquidity_bucket",
    "htf_alignment_bucket",
    "funding_crowding_bucket",
    "derive_cohort_dimensions",
    "cohort_signature",
]
