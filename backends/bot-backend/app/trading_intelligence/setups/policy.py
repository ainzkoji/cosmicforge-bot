"""Versioned setup-discovery policies (Section 11.7).

RESEARCH DEFAULTS: every threshold below exists to exercise the system, not
because replay has calibrated it. They are centralized here, versioned via
``policy_hash``, and easy to replace -- never scattered as magic numbers
through the specialist files. None of these are Section 13 economic
admission gates; they only decide whether a *hypothesis* exists at all.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import SETUP_POLICY_SCHEMA_VERSION


@dataclass(frozen=True)
class TrendPullbackPolicy:
    schema_version: str = SETUP_POLICY_SCHEMA_VERSION
    #: RESEARCH DEFAULT eligibility floor on RegimeDistribution's
    #: TREND_CONTINUATION weight -- a discovery floor, not a Section 13
    #: admission threshold, and unrelated to V2's adaptive entry threshold.
    #: Deliberately below the 6-class uniform baseline (~0.167): Section
    #: 10's TREND_CONTINUATION evidence includes "structural breakout"
    #: (a fresh BOS), which naturally fades *during* a genuine pullback --
    #: exactly the market condition this specialist looks for -- so a
    #: pullback candidate must not be required to also show fresh-breakout
    #: evidence.
    regime_eligibility_floor: float = 0.18
    min_history_bars: int = 40
    max_age_bars: int = 60
    max_extension_atr: float = 3.5
    min_retracement_depth: float = 0.15
    max_retracement_depth: float = 0.75
    min_target_room_R: float = 0.5
    require_htf_non_conflict: bool = True
    candidate_validity_bars: int = 3
    #: A projected target expressed as a multiple of the structural risk
    #: distance -- used because MarketState does not (yet) expose a discrete
    #: forward structural resistance/support level for trend continuation.
    #: This is an explicit, versioned geometry choice, not a fabricated
    #: "real" structural target.
    target_r_multiple: float = 2.0

    @property
    def policy_hash(self) -> str:
        return stable_hash({"family": "TREND_PULLBACK_V2", **self.__dict__})


@dataclass(frozen=True)
class BreakoutVolExpansionPolicy:
    schema_version: str = SETUP_POLICY_SCHEMA_VERSION
    #: See TrendPullbackPolicy.regime_eligibility_floor for why this sits
    #: below a naive "well above uniform" guess: by the time this
    #: specialist's own causal BOS confirmation lag has elapsed, some of
    #: the regime engine's own "current_expansion"/"structural_breakout"
    #: evidence has already started to fade back toward baseline.
    regime_eligibility_floor: float = 0.20
    min_history_bars: int = 40
    min_compression_percentile: float = 0.55
    #: Risk distance (trigger to invalidation boundary) must not exceed this
    #: multiple of the range width, else the breakout is already too extended
    #: to be a fresh discovery (measured in range-widths, since MarketState
    #: does not expose a raw ATR price unit to specialists).
    max_late_entry_extension_range_fraction: float = 1.0
    min_boundary_touch_count: int = 2
    min_target_room_R: float = 0.5
    candidate_validity_bars: int = 3

    @property
    def policy_hash(self) -> str:
        return stable_hash({"family": "BREAKOUT_VOL_EXPANSION_V2", **self.__dict__})


@dataclass(frozen=True)
class RangeMeanReversionPolicy:
    schema_version: str = SETUP_POLICY_SCHEMA_VERSION
    #: See TrendPullbackPolicy.regime_eligibility_floor: a clean, well-
    #: respected range that has NOT recently had a failed break attempt
    #: scores lower on RANGE_EQUILIBRIUM's "mean_reversion_repetition"
    #: evidence than one with visible failed breaks, even though a clean
    #: range is arguably the *better* mean-reversion candidate.
    regime_eligibility_floor: float = 0.22
    min_history_bars: int = 40
    min_range_width_atr: float = 2.0
    max_vol_expansion_weight: float = 0.35
    #: Six-class uniform baseline is ~0.167, so a SHOCK weight merely at or
    #: near baseline (no real shock evidence) must not by itself disqualify
    #: an otherwise clean range read.
    max_shock_weight: float = 0.22
    #: Fraction of range width considered "near a boundary".
    boundary_zone_fraction: float = 0.18
    #: 0 = informational only (see geometry_features["failed_break_count"]);
    #: a smooth, well-respected range legitimately shows zero failed breaks
    #: since a proper boundary test need not ever close beyond the level.
    min_boundary_respect_count: int = 0
    min_target_room_R: float = 0.4
    candidate_validity_bars: int = 3
    #: Invalidation sits this fraction of the range width beyond the
    #: boundary -- a boundary touch alone must not be structurally fatal.
    boundary_invalidation_buffer_fraction: float = 0.08

    @property
    def policy_hash(self) -> str:
        return stable_hash({"family": "RANGE_MEAN_REVERSION_V2", **self.__dict__})


@dataclass(frozen=True)
class MomentumContinuationPolicy:
    schema_version: str = SETUP_POLICY_SCHEMA_VERSION
    regime_eligibility_floor: float = 0.28
    min_history_bars: int = 30
    max_age_bars: int = 40
    max_extension_atr: float = 3.0
    max_exhaustion_proxy: float = 0.6
    min_target_room_R: float = 0.4
    candidate_validity_bars: int = 3
    #: See TrendPullbackPolicy.target_r_multiple for rationale.
    target_r_multiple: float = 1.5

    @property
    def policy_hash(self) -> str:
        return stable_hash({"family": "MOMENTUM_CONTINUATION_V1", **self.__dict__})


def default_policies() -> dict:
    return {
        "TREND_PULLBACK_V2": TrendPullbackPolicy(),
        "BREAKOUT_VOL_EXPANSION_V2": BreakoutVolExpansionPolicy(),
        "RANGE_MEAN_REVERSION_V2": RangeMeanReversionPolicy(),
        "MOMENTUM_CONTINUATION_V1": MomentumContinuationPolicy(),
    }


__all__ = [
    "TrendPullbackPolicy",
    "BreakoutVolExpansionPolicy",
    "RangeMeanReversionPolicy",
    "MomentumContinuationPolicy",
    "default_policies",
]
