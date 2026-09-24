"""Bounded, interpretable evidence features derived exclusively from
MarketState (Section 10.4). Every value here is in [0, 1] and each has a
one-line, checkable definition -- no future returns, no fitted magic
constants hiding inside this module (those live in ``policy.py``, versioned).
"""
from __future__ import annotations

from typing import Dict

from app.trading_intelligence.contracts.market_state import MarketState, clamp01


def _bump(value: float, low: float, high: float) -> float:
    """0 below ``low``, 1 at/after ``high``, linear in between."""
    if high <= low:
        return 0.0
    return clamp01((value - low) / (high - low))


def compute_evidence_features(market_state: MarketState) -> Dict[str, float]:
    structure = market_state.structure_state
    trend = market_state.trend_state
    volatility = market_state.volatility_state
    momentum = market_state.momentum_state
    participation = market_state.participation_state
    liquidity = market_state.liquidity_state
    htf = market_state.higher_timeframe_state
    uncertainty = market_state.state_uncertainty

    features: Dict[str, float] = {}

    trend_available = trend.available
    trend_directional = trend_available and trend.direction in ("UP", "DOWN")
    structure_directional = structure.available and structure.last_bos_direction != "NONE"

    features["structure_direction_coherent"] = (
        1.0 if (trend_directional and structure_directional and structure.last_bos_direction == trend.direction) else 0.0
    )
    features["trend_strength"] = trend.strength if trend_available else 0.0
    features["efficiency"] = clamp01(trend.efficiency) if trend.efficiency is not None else 0.0
    features["persistence"] = clamp01(trend.age / 30.0) if trend_available else 0.0
    features["moderate_extension"] = (
        _bump(abs(trend.extension_atr), 0.3, 1.5) * (1.0 - _bump(abs(trend.extension_atr), 3.0, 5.0))
        if trend.extension_atr is not None
        else 0.0
    )
    features["low_exhaustion"] = 1.0 - clamp01(momentum.exhaustion_proxy) if momentum.exhaustion_proxy is not None else 0.5
    if htf.available and htf.structure_alignment == "ALIGNED":
        features["htf_alignment"] = 1.0
    elif htf.available and htf.structure_alignment == "CONFLICT":
        features["htf_alignment"] = 0.0
    else:
        features["htf_alignment"] = 0.5

    features["mixed_balanced_structure"] = 1.0 if (structure.available and structure.swing_sequence == "MIXED") else 0.0
    features["stable_range"] = (
        clamp01(structure.structure_integrity) if features["mixed_balanced_structure"] else 0.0
    )
    features["weak_persistence"] = 1.0 - features["persistence"]
    features["low_efficiency"] = 1.0 - features["efficiency"]
    features["mean_reversion_repetition"] = clamp01(structure.failed_break_count / 3.0) if structure.available else 0.0

    features["prior_compression"] = clamp01(volatility.compression) if volatility.compression is not None else 0.0
    features["current_expansion"] = clamp01((volatility.expansion or 0.0) / 3.0) if volatility.available else 0.0
    features["not_expanding"] = 1.0 - features["current_expansion"]
    features["volatility_acceleration"] = (
        clamp01((volatility.vol_acceleration - 1.0)) if volatility.vol_acceleration is not None else 0.0
    )
    features["structural_breakout"] = 1.0 if structure_directional else 0.0
    features["participation_expansion"] = (
        clamp01((participation.volume_acceleration - 1.0)) if participation.volume_acceleration is not None else 0.0
    )

    features["late_maturity"] = {"LATE": 1.0, "MID": 0.5}.get(trend.maturity, 0.0) if trend_available else 0.0
    features["high_extension"] = clamp01((abs(trend.extension_atr) or 0.0) / 6.0) if trend.extension_atr is not None else 0.0
    features["deceleration"] = clamp01(momentum.exhaustion_proxy) if momentum.exhaustion_proxy is not None else 0.0
    features["deteriorating_participation"] = (
        clamp01(1.0 - participation.volume_acceleration) if participation.volume_acceleration is not None else 0.0
    )
    features["failed_continuation"] = features["mean_reversion_repetition"]
    features["choch_opposite_pressure"] = 1.0 if (structure.available and structure.choch_direction != "NONE") else 0.0

    features["extreme_realized_vol"] = clamp01(volatility.percentile) if volatility.percentile is not None else 0.0
    features["extreme_true_range"] = clamp01((volatility.expansion or 0.0) / 4.0) if volatility.available else 0.0
    features["liquidity_deterioration"] = (
        clamp01(liquidity.spread_percentile) if (liquidity.available and liquidity.spread_percentile is not None) else 0.0
    )
    features["shock_flags"] = 1.0 if volatility.shock_state else 0.0

    features["high_contradiction"] = clamp01(uncertainty.components.get("contradictory_state", 0.0))
    features["unresolved_structure"] = 1.0 if (structure.available and structure.swing_sequence == "UNRESOLVED") else 0.0
    features["insufficient_quality"] = clamp01(uncertainty.components.get("insufficient_history", 0.0))
    features["regime_ambiguity"] = clamp01(max(features["high_contradiction"], uncertainty.value))

    return features
