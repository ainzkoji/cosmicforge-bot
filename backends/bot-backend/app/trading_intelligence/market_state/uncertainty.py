"""Market state uncertainty (Section 9.11) -- deterministic evidence
uncertainty, NOT a probability/confidence score.

Composed from explicit, named penalty components so the resulting number is
always explainable. A critical data fault must never be hidden behind a
merely-elevated uncertainty value -- that is enforced by the caller checking
``DataQuality.is_usable`` before trusting this value at all (Section 9.11:
"MarketState should be invalid/unusable ... according to a clear fail-closed
contract").
"""
from __future__ import annotations

from app.trading_intelligence.contracts.data_quality import DataQuality, DataQualityLevel, FeatureAvailability, CapabilityState
from app.trading_intelligence.contracts.market_state import (
    HigherTimeframeState,
    MomentumState,
    ParticipationState,
    StateUncertainty,
    StructureState,
    TrendState,
    VolatilityState,
    clamp01,
)

_WEIGHTS = {
    "missing_critical_feature": 0.30,
    "stale_data": 0.15,
    "contradictory_state": 0.20,
    "insufficient_history": 0.15,
    "extreme_outlier": 0.10,
    "unsupported_capability": 0.10,
}


def compute_state_uncertainty(
    *,
    structure_state: StructureState,
    trend_state: TrendState,
    volatility_state: VolatilityState,
    momentum_state: MomentumState,
    participation_state: ParticipationState,
    higher_timeframe_state: HigherTimeframeState,
    data_quality: DataQuality,
    feature_availability: FeatureAvailability,
) -> StateUncertainty:
    components: dict = {}
    reason_codes = []

    # -- missing critical feature ------------------------------------------
    critical_unavailable = 0
    for family in (structure_state, trend_state, volatility_state, momentum_state):
        if not family.available:
            critical_unavailable += 1
    if data_quality.level == DataQualityLevel.INVALID:
        components["missing_critical_feature"] = 1.0
    elif critical_unavailable:
        components["missing_critical_feature"] = clamp01(critical_unavailable / 4.0)
    else:
        components["missing_critical_feature"] = 0.0

    # -- stale data ----------------------------------------------------------
    components["stale_data"] = 1.0 if data_quality.level == DataQualityLevel.DEGRADED and any(
        "STALE" in code for code in data_quality.reason_codes
    ) else 0.0

    # -- contradictory state-family penalty ----------------------------------
    contradiction = 0.0
    if structure_state.available and trend_state.available and trend_state.direction in ("UP", "DOWN"):
        if structure_state.last_bos_direction != "NONE" and structure_state.last_bos_direction != trend_state.direction:
            contradiction = max(contradiction, 0.5)
    if higher_timeframe_state.available and higher_timeframe_state.structure_alignment == "CONFLICT":
        contradiction = max(contradiction, 1.0)
    components["contradictory_state"] = clamp01(contradiction)

    # -- insufficient history -------------------------------------------------
    insufficient = sum(
        1
        for family in (structure_state, trend_state, volatility_state, momentum_state, participation_state)
        if "INSUFFICIENT_HISTORY" in (family.reason_codes or ())
    )
    components["insufficient_history"] = clamp01(insufficient / 5.0)

    # -- extreme/outlier feature distance -------------------------------------
    extreme = 0.0
    if trend_state.extension_atr is not None and abs(trend_state.extension_atr) > 5.0:
        extreme = max(extreme, 1.0)
    elif trend_state.extension_atr is not None and abs(trend_state.extension_atr) > 3.0:
        extreme = max(extreme, 0.5)
    if volatility_state.shock_state:
        extreme = max(extreme, 1.0)
    components["extreme_outlier"] = clamp01(extreme)

    # -- unsupported capability -----------------------------------------------
    unsupported_count = sum(
        1 for state in feature_availability.states.values() if state == CapabilityState.UNSUPPORTED.value
    )
    components["unsupported_capability"] = clamp01(unsupported_count / 8.0)

    value = clamp01(sum(_WEIGHTS[k] * components[k] for k in _WEIGHTS))

    for key, magnitude in components.items():
        if magnitude > 0:
            reason_codes.append(key.upper())

    return StateUncertainty(value=value, components=components, reason_codes=tuple(reason_codes))
