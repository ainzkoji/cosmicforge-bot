"""Deterministic V1 regime engine (Section 10.3) and specialist-routing
eligibility (Section 10.5).

``compute_regime_distribution`` is a pure function of ``(market_state,
policy, history)``: no clock, no network, no tenant input. An invalid
MarketState fails closed into an all-but-certain ``TRANSITION_UNKNOWN``
distribution rather than fabricating a confident regime out of untrustworthy
data (P7).
"""
from __future__ import annotations

import math
from typing import Dict, Mapping, Sequence

from app.trading_intelligence.contracts.market_state import MarketState, clamp01
from app.trading_intelligence.regime.contracts import (
    REGIME_CLASSES,
    RegimeClass,
    RegimeDistribution,
    RoutingEligibility,
    SPECIALIST_NAMES,
    SpecialistEligibility,
)
from app.trading_intelligence.regime.evidence import compute_evidence_features
from app.trading_intelligence.regime.policy import RegimePolicy, default_policy
from app.trading_intelligence.versions import REGIME_MODEL_VERSION

#: A regime is considered "high"/dominant enough to drive routing decisions
#: above this weight. Six uniform classes would each sit at ~0.167, so 0.35
#: requires meaningfully concentrated evidence, not a bare plurality.
DOMINANCE_THRESHOLD = 0.35

#: How many of the most recent history entries feed recent-regime instability.
INSTABILITY_LOOKBACK = 5

_ELIGIBILITY_ORDER = {
    SpecialistEligibility.ELIGIBLE.value: 0,
    SpecialistEligibility.PENALIZED.value: 1,
    SpecialistEligibility.DISABLED.value: 2,
}


def _raw_scores(features: Mapping[str, float], policy: RegimePolicy) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    for regime in REGIME_CLASSES:
        weights = policy.weights_for(regime)
        if not weights:
            scores[regime.value] = policy.score_floor
            continue
        total = sum(w * features.get(name, 0.0) for name, w in weights.items())
        scores[regime.value] = (total / len(weights)) + policy.score_floor
    return scores


def _softmax(scores: Mapping[str, float], temperature: float) -> Dict[str, float]:
    values = list(scores.values())
    max_v = max(values)
    exps = {k: math.exp((v - max_v) / temperature) for k, v in scores.items()}
    total = sum(exps.values())
    return {k: v / total for k, v in exps.items()}


def _normalized_entropy(weights: Mapping[str, float]) -> float:
    k = len(weights)
    if k <= 1:
        return 0.0
    h = -sum(p * math.log(p) for p in weights.values() if p > 0)
    return clamp01(h / math.log(k))


def _recent_instability(history: Sequence[RegimeDistribution]) -> float:
    """Fraction of consecutive dominant-regime *changes* among the most
    recent entries -- 0 if fewer than two are available."""
    tail = list(history)[-INSTABILITY_LOOKBACK:]
    if len(tail) < 2:
        return 0.0
    changes = sum(1 for a, b in zip(tail, tail[1:]) if a.dominant_regime != b.dominant_regime)
    return clamp01(changes / (len(tail) - 1))


def _fallback_distribution(market_state: MarketState, policy: RegimePolicy, reason_codes: Sequence[str]) -> RegimeDistribution:
    """All but certain TRANSITION_UNKNOWN -- the fail-closed contract for an
    unusable MarketState. Weights still sum to 1 and every weight stays
    strictly positive (score_floor), so downstream consumers never see a
    malformed distribution."""
    floor = policy.score_floor
    n_others = len(REGIME_CLASSES) - 1
    remaining = max(1.0 - (floor * n_others) - 0.97, 0.0)
    weights = {r.value: floor for r in REGIME_CLASSES}
    weights[RegimeClass.TRANSITION_UNKNOWN.value] = 0.97 + remaining
    total = sum(weights.values())
    weights = {k: v / total for k, v in weights.items()}
    return RegimeDistribution.build(
        market_state_id=market_state.market_state_id,
        instrument_key=market_state.instrument_key,
        timeframe=market_state.timeframe,
        decision_time=market_state.decision_time,
        model_version=REGIME_MODEL_VERSION,
        policy_hash=policy.policy_hash,
        weights=weights,
        entropy=_normalized_entropy(weights),
        transition_uncertainty=1.0,
        evidence_by_regime={RegimeClass.TRANSITION_UNKNOWN.value: {"invalid_market_state": 1.0}},
        reason_codes=tuple(reason_codes),
        data_quality_level=market_state.data_quality.level.value,
    )


def compute_regime_distribution(
    market_state: MarketState,
    policy: RegimePolicy = None,
    *,
    history: Sequence[RegimeDistribution] = (),
) -> RegimeDistribution:
    policy = policy or default_policy()

    if not market_state.is_usable:
        return _fallback_distribution(market_state, policy, market_state.data_quality.reason_codes)

    features = compute_evidence_features(market_state)
    raw_scores = _raw_scores(features, policy)
    weights = _softmax(raw_scores, policy.temperature)
    entropy = _normalized_entropy(weights)

    contradiction = clamp01(market_state.state_uncertainty.components.get("contradictory_state", 0.0))
    instability = _recent_instability(history)
    state_uncertainty = market_state.state_uncertainty.value
    transition_uncertainty = clamp01((entropy + contradiction + instability + state_uncertainty) / 4.0)

    evidence_by_regime = {
        regime.value: {name: features.get(name, 0.0) for name in policy.weights_for(regime)} for regime in REGIME_CLASSES
    }

    return RegimeDistribution.build(
        market_state_id=market_state.market_state_id,
        instrument_key=market_state.instrument_key,
        timeframe=market_state.timeframe,
        decision_time=market_state.decision_time,
        model_version=REGIME_MODEL_VERSION,
        policy_hash=policy.policy_hash,
        weights=weights,
        entropy=entropy,
        transition_uncertainty=transition_uncertainty,
        evidence_by_regime=evidence_by_regime,
        reason_codes=market_state.data_quality.reason_codes,
        data_quality_level=market_state.data_quality.level.value,
    )


def _set_state(eligibility: Dict[str, str], reasons: Dict[str, str], name: str, state: SpecialistEligibility, reason: str) -> None:
    """Restrictions only ever escalate (ELIGIBLE -> PENALIZED -> DISABLED),
    never relax a specialist that an earlier rule already restricted."""
    if _ELIGIBILITY_ORDER[state.value] > _ELIGIBILITY_ORDER.get(eligibility[name], 0):
        eligibility[name] = state.value
        reasons[name] = reason


def compute_routing_eligibility(distribution: RegimeDistribution) -> RoutingEligibility:
    """Specialist eligibility only (Section 10.5) -- never a strategy,
    never an order. Section 11 specialists do not exist yet; this only
    records what a future specialist would be allowed to do."""
    w = distribution.weights
    eligibility: Dict[str, str] = {name: SpecialistEligibility.ELIGIBLE.value for name in SPECIALIST_NAMES}
    reasons: Dict[str, str] = {}

    if w.get(RegimeClass.SHOCK.value, 0.0) >= DOMINANCE_THRESHOLD:
        for name in SPECIALIST_NAMES:
            _set_state(eligibility, reasons, name, SpecialistEligibility.DISABLED, "SHOCK_ABSTENTION_BIAS")

    if w.get(RegimeClass.TRANSITION_UNKNOWN.value, 0.0) >= DOMINANCE_THRESHOLD:
        for name in SPECIALIST_NAMES:
            _set_state(eligibility, reasons, name, SpecialistEligibility.PENALIZED, "TRANSITION_UNKNOWN_ABSTENTION_BIAS")

    if w.get(RegimeClass.EXHAUSTION_REVERSAL.value, 0.0) >= DOMINANCE_THRESHOLD:
        _set_state(eligibility, reasons, "TREND_PULLBACK", SpecialistEligibility.PENALIZED, "EXHAUSTION_REVERSAL_PENALIZES_CONTINUATION")
        _set_state(eligibility, reasons, "MOMENTUM_CONTINUATION", SpecialistEligibility.PENALIZED, "EXHAUSTION_REVERSAL_PENALIZES_CONTINUATION")

    if w.get(RegimeClass.VOL_EXPANSION.value, 0.0) >= DOMINANCE_THRESHOLD:
        # Continuation is only eligible under vol expansion if a late-entry
        # veto (Section 14, not implemented yet) permits it -- conservatively
        # penalized until that veto exists.
        _set_state(eligibility, reasons, "TREND_PULLBACK", SpecialistEligibility.PENALIZED, "VOL_EXPANSION_LATE_ENTRY_VETO_NOT_YET_IMPLEMENTED")
        _set_state(eligibility, reasons, "MOMENTUM_CONTINUATION", SpecialistEligibility.PENALIZED, "VOL_EXPANSION_LATE_ENTRY_VETO_NOT_YET_IMPLEMENTED")

    if w.get(RegimeClass.RANGE_EQUILIBRIUM.value, 0.0) >= DOMINANCE_THRESHOLD:
        _set_state(eligibility, reasons, "TREND_PULLBACK", SpecialistEligibility.PENALIZED, "RANGE_EQUILIBRIUM_RESTRICTS_CONTINUATION")
        _set_state(eligibility, reasons, "MOMENTUM_CONTINUATION", SpecialistEligibility.PENALIZED, "RANGE_EQUILIBRIUM_RESTRICTS_CONTINUATION")

    return RoutingEligibility(
        market_state_id=distribution.market_state_id,
        regime_distribution_id=distribution.regime_distribution_id,
        eligibility=eligibility,
        reasons=reasons,
    )


__all__ = ["DOMINANCE_THRESHOLD", "compute_regime_distribution", "compute_routing_eligibility"]
