"""Versioned regime-engine policy (Section 10.3). All weights/constants used
by the deterministic V1 regime engine live here, never scattered as magic
numbers through ``engine.py``. Bumping ``POLICY_VERSION`` is required
whenever a weight or the feature set backing it changes meaning -- the same
discipline ``app/threshold/policy.py`` uses for ``POLICY_VERSION``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.regime.contracts import RegimeClass

POLICY_VERSION = "1.0.0"

#: raw_score[r] = mean(weight[i, r] * bounded_feature[i]) over regime r's terms.
#: Each entry maps directly to the evidence bullets in Section 10.4.
DEFAULT_FEATURE_WEIGHTS: Dict[str, Dict[str, float]] = {
    RegimeClass.TREND_CONTINUATION.value: {
        "structure_direction_coherent": 1.0,
        "trend_strength": 1.0,
        "efficiency": 1.0,
        "persistence": 1.0,
        "moderate_extension": 1.0,
        "low_exhaustion": 1.0,
        "htf_alignment": 1.0,
    },
    RegimeClass.RANGE_EQUILIBRIUM.value: {
        "mixed_balanced_structure": 1.0,
        "stable_range": 1.0,
        "weak_persistence": 1.0,
        "low_efficiency": 1.0,
        "mean_reversion_repetition": 1.0,
        "not_expanding": 1.0,
    },
    RegimeClass.VOL_EXPANSION.value: {
        "prior_compression": 1.0,
        "current_expansion": 1.0,
        "volatility_acceleration": 1.0,
        "structural_breakout": 1.0,
        "participation_expansion": 1.0,
    },
    RegimeClass.EXHAUSTION_REVERSAL.value: {
        "late_maturity": 1.0,
        "high_extension": 1.0,
        "deceleration": 1.0,
        "deteriorating_participation": 1.0,
        "failed_continuation": 1.0,
        "choch_opposite_pressure": 1.0,
    },
    RegimeClass.SHOCK.value: {
        "extreme_realized_vol": 1.0,
        "extreme_true_range": 1.0,
        "liquidity_deterioration": 1.0,
        "shock_flags": 1.5,
    },
    RegimeClass.TRANSITION_UNKNOWN.value: {
        "high_contradiction": 1.0,
        "unresolved_structure": 1.0,
        "regime_ambiguity": 1.0,
        "insufficient_quality": 1.0,
    },
}

#: Softmax temperature. Higher = flatter (more entropy) distribution for the
#: same raw scores; lower = sharper. Chosen so a single strongly-evidenced
#: regime (mean bounded-feature ~0.8) dominates but does not collapse to a
#: one-hot vector for merely-plausible evidence (~0.5).
DEFAULT_TEMPERATURE = 0.35

#: A tiny floor added to every regime's raw score so a regime with literally
#: zero evidence still receives a non-zero (but negligible) softmax weight --
#: keeps every weight strictly positive, which the sum-to-1 and "every weight
#: finite and 0..1" tests rely on, without ever making a floor-only regime
#: dominant.
SCORE_FLOOR = 1e-6


@dataclass(frozen=True)
class RegimePolicy:
    policy_version: str = POLICY_VERSION
    temperature: float = DEFAULT_TEMPERATURE
    feature_weights: Mapping[str, Mapping[str, float]] = field(
        default_factory=lambda: {k: dict(v) for k, v in DEFAULT_FEATURE_WEIGHTS.items()}
    )
    score_floor: float = SCORE_FLOOR

    @property
    def policy_hash(self) -> str:
        payload = {
            "policy_version": self.policy_version,
            "temperature": self.temperature,
            "feature_weights": {k: dict(v) for k, v in self.feature_weights.items()},
            "score_floor": self.score_floor,
        }
        return stable_hash(payload)

    def weights_for(self, regime: RegimeClass) -> Mapping[str, float]:
        return self.feature_weights.get(regime.value, {})


def default_policy() -> RegimePolicy:
    return RegimePolicy()


__all__ = ["POLICY_VERSION", "DEFAULT_FEATURE_WEIGHTS", "DEFAULT_TEMPERATURE", "SCORE_FLOOR", "RegimePolicy", "default_policy"]
