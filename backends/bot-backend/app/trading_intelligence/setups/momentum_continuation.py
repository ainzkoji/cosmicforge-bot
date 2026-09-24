"""Specialist D -- Momentum Continuation (Section 11.6).

Unlike Trend Pullback, this specialist does not require a retracement --
it discovers a hypothesis directly on ongoing directional strength. Missing
optional OI/funding/crowding data is never treated as "no crowding" (P7):
a crowding check only fires when ``derivatives_state`` actually has a
crowding_state to read.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from app.trading_intelligence.contracts.data_quality import Capability
from app.trading_intelligence.contracts.market_state import clamp01
from app.trading_intelligence.contracts.setup import SetupCandidate, SetupReasonCode, SetupSide, compute_geometry
from app.trading_intelligence.regime.contracts import RegimeClass
from app.trading_intelligence.setups._common import htf_conflicts, regime_weight, snapshot_identity
from app.trading_intelligence.setups.policy import MomentumContinuationPolicy

SETUP_FAMILY = "MOMENTUM_CONTINUATION_V1"
SETUP_VERSION = "1.0.0"


class MomentumContinuationSpecialist:
    setup_family = SETUP_FAMILY
    setup_version = SETUP_VERSION
    required_capabilities: Tuple[Capability, ...] = (Capability.OHLCV,)

    def discover(
        self,
        *,
        snapshot: Any,
        market_state: Any,
        regime_distribution: Any,
        policy: MomentumContinuationPolicy,
    ) -> Tuple[SetupCandidate, ...]:
        if not market_state.is_usable:
            return ()

        trend = market_state.trend_state
        structure = market_state.structure_state
        momentum = market_state.momentum_state
        participation = market_state.participation_state
        derivatives = market_state.derivatives_state

        if not trend.available or trend.direction not in ("UP", "DOWN"):
            return ()
        if not momentum.available or momentum.short_return is None or momentum.acceleration is None:
            return ()

        side = SetupSide.LONG.value if trend.direction == "UP" else SetupSide.SHORT.value

        eligible = (
            regime_weight(regime_distribution, RegimeClass.TREND_CONTINUATION.value) >= policy.regime_eligibility_floor
            or regime_weight(regime_distribution, RegimeClass.VOL_EXPANSION.value) >= policy.regime_eligibility_floor
        )
        if not eligible:
            return ()

        # -- positive directional acceleration/persistence --------------------
        aligned_momentum = (trend.direction == "UP" and momentum.short_return > 0) or (
            trend.direction == "DOWN" and momentum.short_return < 0
        )
        if not aligned_momentum:
            return ()
        decelerating = (trend.direction == "UP" and momentum.acceleration < 0) or (
            trend.direction == "DOWN" and momentum.acceleration > 0
        )
        if decelerating and abs(momentum.acceleration) > 0.4:
            return ()  # momentum is decelerating materially

        # -- maturity / extension / exhaustion ---------------------------------
        if trend.maturity == "LATE" and trend.age > policy.max_age_bars:
            return ()
        if trend.extension_atr is not None and abs(trend.extension_atr) > policy.max_extension_atr:
            return ()
        if momentum.exhaustion_proxy is not None and momentum.exhaustion_proxy > policy.max_exhaustion_proxy:
            return ()

        # -- participation confirmation ----------------------------------------
        if not participation.available or participation.relative_volume is None:
            return ()  # WEAK_PARTICIPATION -- cannot confirm without data
        if participation.relative_volume < 0.8:
            return ()

        # -- crowding: only when reliable data exists --------------------------
        if derivatives.available and derivatives.crowding_state is not None:
            crowded_with_trade = (side == SetupSide.LONG.value and derivatives.crowding_state == "CROWDED_LONG") or (
                side == SetupSide.SHORT.value and derivatives.crowding_state == "CROWDED_SHORT"
            )
            if crowded_with_trade:
                return ()  # CROWDING_EXTREME

        if htf_conflicts(market_state, side):
            return ()

        # -- geometry -------------------------------------------------------------
        ident = snapshot_identity(snapshot)
        trigger_reference = ident["reference_price"]
        invalidation = structure.invalidation_reference if structure.available else None
        if invalidation is None and structure.available:
            invalidation = structure.range_low if side == SetupSide.LONG.value else structure.range_high
        if invalidation is None:
            return ()
        if side == SetupSide.LONG.value and invalidation >= trigger_reference:
            return ()
        if side == SetupSide.SHORT.value and invalidation <= trigger_reference:
            return ()

        geometry = compute_geometry(
            side=side, trigger_reference=trigger_reference,
            structural_invalidation=invalidation, target_reference=None,
        )
        if geometry is None:
            return ()

        direction_sign = 1.0 if side == SetupSide.LONG.value else -1.0
        target_reference = trigger_reference + direction_sign * geometry.risk_distance * policy.target_r_multiple
        geometry = compute_geometry(
            side=side, trigger_reference=trigger_reference,
            structural_invalidation=invalidation, target_reference=target_reference,
        )
        if geometry is None or geometry.room_to_target_R is None:
            return ()
        if geometry.room_to_target_R < policy.min_target_room_R:
            return ()

        components: Dict[str, float] = {
            "momentum_strength": clamp01(abs(momentum.short_return)),
            "efficiency": clamp01(momentum.efficiency) if momentum.efficiency is not None else 0.0,
            "participation_confirmation": clamp01(participation.relative_volume / 2.0),
            "low_exhaustion": 1.0 - clamp01(momentum.exhaustion_proxy) if momentum.exhaustion_proxy is not None else 0.5,
        }
        evidence_score = clamp01(sum(components.values()) / len(components))

        reason_codes = [SetupReasonCode.SETUP_FOUND.value]
        if not market_state.liquidity_state.available:
            reason_codes.append(SetupReasonCode.LIQUIDITY_UNVERIFIED.value)
        if not derivatives.available:
            reason_codes.append(SetupReasonCode.REQUIRED_CAPABILITY_MISSING.value)

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
                "momentum_short_return": momentum.short_return,
                "trend_age_bars": float(trend.age),
                "extension_atr": trend.extension_atr or 0.0,
            },
            required_capabilities=tuple(c.value for c in self.required_capabilities),
            evidence_components=components,
            evidence_score=evidence_score,
            reason_codes=tuple(reason_codes),
            validity_bars=policy.candidate_validity_bars,
        )
        return (candidate,)


__all__ = ["MomentumContinuationSpecialist", "SETUP_FAMILY", "SETUP_VERSION"]
