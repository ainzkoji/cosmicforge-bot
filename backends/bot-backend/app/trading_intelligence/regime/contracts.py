"""Regime distribution contracts (Section 10.2).

These are named "regime weights/beliefs" throughout, deliberately never
"probabilities" -- Section 10.8 forbids claiming calibrated probability
until reliability has actually been measured. The full distribution is
authoritative; nothing downstream may collapse it to ``dominant_regime``
alone (Section 10.6).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import REGIME_SCHEMA_VERSION


class RegimeClass(str, Enum):
    TREND_CONTINUATION = "TREND_CONTINUATION"
    RANGE_EQUILIBRIUM = "RANGE_EQUILIBRIUM"
    VOL_EXPANSION = "VOL_EXPANSION"
    EXHAUSTION_REVERSAL = "EXHAUSTION_REVERSAL"
    SHOCK = "SHOCK"
    TRANSITION_UNKNOWN = "TRANSITION_UNKNOWN"


REGIME_CLASSES: Tuple[RegimeClass, ...] = (
    RegimeClass.TREND_CONTINUATION,
    RegimeClass.RANGE_EQUILIBRIUM,
    RegimeClass.VOL_EXPANSION,
    RegimeClass.EXHAUSTION_REVERSAL,
    RegimeClass.SHOCK,
    RegimeClass.TRANSITION_UNKNOWN,
)

WEIGHT_SUM_TOLERANCE = 1e-6


class SpecialistEligibility(str, Enum):
    ELIGIBLE = "ELIGIBLE"
    PENALIZED = "PENALIZED"
    DISABLED = "DISABLED"


#: Specialist names the router expresses eligibility for. These name future
#: Section 11 setup specialists -- the router only expresses eligibility,
#: it never instantiates or runs one (Section 10.5).
SPECIALIST_NAMES: Tuple[str, ...] = (
    "TREND_PULLBACK",
    "MOMENTUM_CONTINUATION",
    "BREAKOUT_VOL_EXPANSION",
    "RANGE_MEAN_REVERSION",
)


_stable_hash = stable_hash  # local alias, kept for readability in this module


@dataclass(frozen=True)
class RegimeDistribution:
    """The regime-weight verdict for one MarketState under one policy."""

    regime_distribution_id: str
    market_state_id: str
    instrument_key: InstrumentKey
    timeframe: str
    decision_time: int

    model_version: str
    policy_hash: str

    weights: Mapping[str, float]  # RegimeClass.value -> weight, sums to 1

    dominant_regime: str
    dominant_weight: float

    entropy: float  # normalized 0..1
    transition_uncertainty: float  # 0..1

    evidence_by_regime: Mapping[str, Mapping[str, float]]
    reason_codes: Tuple[str, ...] = ()
    data_quality_level: str = "VALID"
    schema_version: str = REGIME_SCHEMA_VERSION

    def canonical_payload(self) -> dict:
        return {
            "market_state_id": self.market_state_id,
            "instrument_key": {
                "asset_class": self.instrument_key.asset_class,
                "base_asset": self.instrument_key.base_asset,
                "quote_asset": self.instrument_key.quote_asset,
                "canonical_symbol": self.instrument_key.canonical_symbol,
                "venue": self.instrument_key.venue,
                "venue_symbol": self.instrument_key.venue_symbol,
                "contract_type": self.instrument_key.contract_type,
            },
            "timeframe": self.timeframe,
            "decision_time": self.decision_time,
            "model_version": self.model_version,
            "policy_hash": self.policy_hash,
            "weights": dict(self.weights),
            "dominant_regime": self.dominant_regime,
            "dominant_weight": self.dominant_weight,
            "entropy": self.entropy,
            "transition_uncertainty": self.transition_uncertainty,
            "evidence_by_regime": {k: dict(v) for k, v in self.evidence_by_regime.items()},
            "reason_codes": list(self.reason_codes),
            "data_quality_level": self.data_quality_level,
            "schema_version": self.schema_version,
        }

    @property
    def canonical_hash(self) -> str:
        return _stable_hash(self.canonical_payload())

    @classmethod
    def build(
        cls,
        *,
        market_state_id: str,
        instrument_key: InstrumentKey,
        timeframe: str,
        decision_time: int,
        model_version: str,
        policy_hash: str,
        weights: Mapping[str, float],
        entropy: float,
        transition_uncertainty: float,
        evidence_by_regime: Mapping[str, Mapping[str, float]],
        reason_codes: Tuple[str, ...] = (),
        data_quality_level: str = "VALID",
    ) -> "RegimeDistribution":
        dominant_regime = max(weights, key=lambda k: weights[k])
        dominant_weight = weights[dominant_regime]
        payload = {
            "market_state_id": market_state_id,
            "instrument_key": {
                "asset_class": instrument_key.asset_class,
                "base_asset": instrument_key.base_asset,
                "quote_asset": instrument_key.quote_asset,
                "canonical_symbol": instrument_key.canonical_symbol,
                "venue": instrument_key.venue,
                "venue_symbol": instrument_key.venue_symbol,
                "contract_type": instrument_key.contract_type,
            },
            "timeframe": timeframe,
            "decision_time": decision_time,
            "model_version": model_version,
            "policy_hash": policy_hash,
            "weights": dict(weights),
            "dominant_regime": dominant_regime,
            "dominant_weight": dominant_weight,
            "entropy": entropy,
            "transition_uncertainty": transition_uncertainty,
            "evidence_by_regime": {k: dict(v) for k, v in evidence_by_regime.items()},
            "reason_codes": list(reason_codes),
            "data_quality_level": data_quality_level,
            "schema_version": REGIME_SCHEMA_VERSION,
        }
        content_hash = _stable_hash(payload)
        return cls(
            regime_distribution_id=f"rgm_{content_hash[:24]}",
            market_state_id=market_state_id,
            instrument_key=instrument_key,
            timeframe=timeframe,
            decision_time=decision_time,
            model_version=model_version,
            policy_hash=policy_hash,
            weights=dict(weights),
            dominant_regime=dominant_regime,
            dominant_weight=dominant_weight,
            entropy=entropy,
            transition_uncertainty=transition_uncertainty,
            evidence_by_regime={k: dict(v) for k, v in evidence_by_regime.items()},
            reason_codes=tuple(reason_codes),
            data_quality_level=data_quality_level,
        )


@dataclass(frozen=True)
class RoutingEligibility:
    """Specialist eligibility only -- never a strategy, never an order."""

    market_state_id: str
    regime_distribution_id: str
    eligibility: Mapping[str, str]  # specialist name -> SpecialistEligibility.value
    reasons: Optional[Mapping[str, str]] = None  # specialist name -> short reason

    def state_of(self, specialist: str) -> SpecialistEligibility:
        return SpecialistEligibility(self.eligibility.get(specialist, SpecialistEligibility.DISABLED.value))


__all__ = [
    "RegimeClass",
    "REGIME_CLASSES",
    "WEIGHT_SUM_TOLERANCE",
    "SpecialistEligibility",
    "SPECIALIST_NAMES",
    "RegimeDistribution",
    "RoutingEligibility",
]
