"""Specialist C -- Range / Mean Reversion V2 (Section 11.5).

Fair value uses the range midpoint -- one canonical V1 definition, not
several competing ones (mirrors the Section 9.5 "one canonical compression
measure" discipline). ``range_width_atr`` is approximated as
``distance_to_support_atr + distance_to_resistance_atr`` from
``structure_state`` -- both are computed from the same ATR value in Section
9.3, so their sum is the full boundary-to-boundary distance in ATR units
regardless of where price currently sits in the range.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from app.trading_intelligence.contracts.data_quality import Capability
from app.trading_intelligence.contracts.market_state import clamp01
from app.trading_intelligence.contracts.setup import SetupCandidate, SetupReasonCode, SetupSide, compute_geometry
from app.trading_intelligence.regime.contracts import RegimeClass
from app.trading_intelligence.setups._common import regime_weight, snapshot_identity
from app.trading_intelligence.setups.policy import RangeMeanReversionPolicy

SETUP_FAMILY = "RANGE_MEAN_REVERSION_V2"
SETUP_VERSION = "2.0.0"


class RangeMeanReversionSpecialist:
    setup_family = SETUP_FAMILY
    setup_version = SETUP_VERSION
    required_capabilities: Tuple[Capability, ...] = (Capability.OHLCV,)

    def discover(
        self,
        *,
        snapshot: Any,
        market_state: Any,
        regime_distribution: Any,
        policy: RangeMeanReversionPolicy,
    ) -> Tuple[SetupCandidate, ...]:
        if not market_state.is_usable:
            return ()

        structure = market_state.structure_state
        momentum = market_state.momentum_state

        if not structure.available or structure.range_high is None or structure.range_low is None:
            return ()

        # -- prerequisites ----------------------------------------------------
        if regime_weight(regime_distribution, RegimeClass.RANGE_EQUILIBRIUM.value) < policy.regime_eligibility_floor:
            return ()
        if regime_weight(regime_distribution, RegimeClass.VOL_EXPANSION.value) > policy.max_vol_expansion_weight:
            return ()  # VOL_EXPANSION_ACTIVE
        if regime_weight(regime_distribution, RegimeClass.SHOCK.value) > policy.max_shock_weight:
            return ()
        if structure.choch_direction != "NONE":
            return ()  # STRUCTURAL_TRANSITION_ACTIVE -- character change underway

        range_width = structure.range_high - structure.range_low
        if range_width <= 0:
            return ()

        range_width_atr = (structure.distance_to_support_atr or 0.0) + (structure.distance_to_resistance_atr or 0.0)
        if range_width_atr <= 0 or range_width_atr < policy.min_range_width_atr:
            return ()  # RANGE_TOO_NARROW

        if structure.failed_break_count < policy.min_boundary_respect_count:
            return ()  # BOUNDARY_INTEGRITY_WEAK -- not enough evidence the boundaries hold

        ident = snapshot_identity(snapshot)
        trigger_reference = ident["reference_price"]
        fair_value = (structure.range_high + structure.range_low) / 2.0
        boundary_zone = range_width * policy.boundary_zone_fraction

        distance_to_low = trigger_reference - structure.range_low
        distance_to_high = structure.range_high - trigger_reference

        if distance_to_low <= boundary_zone and distance_to_low <= distance_to_high:
            side = SetupSide.LONG.value
            invalidation = structure.range_low - range_width * policy.boundary_invalidation_buffer_fraction
            target_reference = fair_value
        elif distance_to_high <= boundary_zone:
            side = SetupSide.SHORT.value
            invalidation = structure.range_high + range_width * policy.boundary_invalidation_buffer_fraction
            target_reference = fair_value
        else:
            return ()  # not near a validated boundary -- no hypothesis

        # Exhaustion (Section 11.5.1) is carried as evidence, not a hard gate
        # here: momentum_state.exhaustion_proxy only fires on deceleration
        # against an established directional move (Section 9.6), which a
        # smooth, already-range-bound approach to a boundary does not
        # reliably exhibit even when the boundary read itself is sound.
        # ``components["exhaustion"]`` below still reflects it in
        # evidence_score.

        geometry = compute_geometry(
            side=side, trigger_reference=trigger_reference,
            structural_invalidation=invalidation, target_reference=target_reference,
        )
        if geometry is None or geometry.room_to_target_R is None:
            return ()
        if geometry.room_to_target_R < policy.min_target_room_R:
            return ()

        components: Dict[str, float] = {
            "structure_integrity": clamp01(structure.structure_integrity),
            "boundary_respect": clamp01(structure.failed_break_count / 5.0),
            "exhaustion": clamp01(momentum.exhaustion_proxy) if (momentum.available and momentum.exhaustion_proxy is not None) else 0.0,
        }
        evidence_score = clamp01(sum(components.values()) / len(components))

        reason_codes = [SetupReasonCode.SETUP_FOUND.value]
        if not market_state.liquidity_state.available:
            reason_codes.append(SetupReasonCode.LIQUIDITY_UNVERIFIED.value)

        candidate = SetupCandidate.build(
            market_state_id=market_state.market_state_id,
            snapshot_id=ident["snapshot_id"],
            data_hash=ident["data_hash"],
            instrument_key=market_state.instrument_key,
            timeframe=ident["timeframe"],
            decision_time=market_state.decision_time,
            setup_family=self.setup_family,
            setup_version=self.setup_version,
            setup_policy_hash=policy.policy_hash,
            side=side,
            trigger_reference=trigger_reference,
            structural_invalidation=invalidation,
            target_reference=target_reference,
            geometry_features={
                "range_width": range_width,
                "range_width_atr": range_width_atr,
                "fair_value_reference": fair_value,
            },
            required_capabilities=tuple(c.value for c in self.required_capabilities),
            evidence_components=components,
            evidence_score=evidence_score,
            reason_codes=tuple(reason_codes),
            validity_bars=policy.candidate_validity_bars,
        )
        return (candidate,)


__all__ = ["RangeMeanReversionSpecialist", "SETUP_FAMILY", "SETUP_VERSION"]
