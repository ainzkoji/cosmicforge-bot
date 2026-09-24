"""Specialist A -- Trend Pullback V2 (Section 11.3).

Discovers a continuation-after-retracement hypothesis. Consumes MarketState's
already-causal ``trend_state``/``structure_state``/``momentum_state``/
``participation_state`` rather than re-deriving impulse/retracement geometry
from raw candles -- Section 9 already computed those causally, and
duplicating that logic here would risk it drifting out of sync.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from app.trading_intelligence.contracts.data_quality import Capability
from app.trading_intelligence.contracts.market_state import clamp01
from app.trading_intelligence.contracts.setup import SetupCandidate, SetupReasonCode, SetupSide, compute_geometry
from app.trading_intelligence.regime.contracts import RegimeClass
from app.trading_intelligence.setups._common import htf_conflicts, regime_weight, snapshot_identity
from app.trading_intelligence.setups.policy import TrendPullbackPolicy

SETUP_FAMILY = "TREND_PULLBACK_V2"
SETUP_VERSION = "2.0.0"


class TrendPullbackSpecialist:
    setup_family = SETUP_FAMILY
    setup_version = SETUP_VERSION
    required_capabilities: Tuple[Capability, ...] = (Capability.OHLCV,)

    def discover(
        self,
        *,
        snapshot: Any,
        market_state: Any,
        regime_distribution: Any,
        policy: TrendPullbackPolicy,
    ) -> Tuple[SetupCandidate, ...]:
        if not market_state.is_usable:
            return ()

        trend = market_state.trend_state
        structure = market_state.structure_state
        momentum = market_state.momentum_state
        participation = market_state.participation_state

        if not trend.available or trend.direction not in ("UP", "DOWN"):
            return ()

        side = SetupSide.LONG.value if trend.direction == "UP" else SetupSide.SHORT.value

        # -- prerequisites --------------------------------------------------
        if regime_weight(regime_distribution, RegimeClass.TREND_CONTINUATION.value) < policy.regime_eligibility_floor:
            return ()
        if not structure.available:
            return ()
        structure_aligned = structure.last_bos_direction == "NONE" or structure.last_bos_direction == trend.direction
        if not structure_aligned:
            return ()
        if policy.require_htf_non_conflict and htf_conflicts(market_state, side):
            return ()

        # -- rejection: maturity / extension ---------------------------------
        if trend.maturity == "LATE" and trend.age > policy.max_age_bars:
            return ()
        if trend.extension_atr is not None and abs(trend.extension_atr) > policy.max_extension_atr:
            return ()

        # -- rejection: retracement geometry ---------------------------------
        retracement_depth = trend.retracement_depth
        if retracement_depth is None:
            return ()
        if retracement_depth < policy.min_retracement_depth:
            return ()  # no meaningful pullback has happened yet -- not a rejection, just no setup
        if retracement_depth > policy.max_retracement_depth:
            return ()  # PULLBACK_STRUCTURALLY_BROKEN -- too deep to be a continuation pullback

        # -- rejection: countertrend momentum strengthening ------------------
        if momentum.available and momentum.acceleration is not None and momentum.short_return is not None:
            countertrend = (trend.direction == "UP" and momentum.short_return < 0) or (
                trend.direction == "DOWN" and momentum.short_return > 0
            )
            strengthening_against_us = (trend.direction == "UP" and momentum.acceleration < 0) or (
                trend.direction == "DOWN" and momentum.acceleration > 0
            )
            if countertrend and strengthening_against_us and abs(momentum.acceleration) > 0.5:
                return ()

        # -- structural invalidation ------------------------------------------
        ident = snapshot_identity(snapshot)
        trigger_reference = ident["reference_price"]
        invalidation = structure.invalidation_reference
        if invalidation is None:
            invalidation = structure.range_low if side == SetupSide.LONG.value else structure.range_high
        if invalidation is None:
            return ()
        # A stale/wrong-sided invalidation must not silently pass through.
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

        # -- trigger evidence (diagnostic only) -------------------------------
        components: Dict[str, float] = {
            "structure_integrity": clamp01(structure.structure_integrity),
            "impulse_efficiency": clamp01(trend.efficiency) if trend.efficiency is not None else 0.0,
            "low_exhaustion": 1.0 - clamp01(momentum.exhaustion_proxy) if momentum.exhaustion_proxy is not None else 0.5,
            "retracement_quality": 1.0 - abs(retracement_depth - 0.382) / 0.618,
        }
        if participation.available and participation.relative_volume is not None:
            components["participation_recovery"] = clamp01(participation.relative_volume / 2.0)
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
                "impulse_distance_atr": trend.extension_atr or 0.0,
                "impulse_duration_bars": float(trend.age),
                "retracement_depth": retracement_depth,
            },
            required_capabilities=tuple(c.value for c in self.required_capabilities),
            evidence_components=components,
            evidence_score=evidence_score,
            reason_codes=tuple(reason_codes),
            validity_bars=policy.candidate_validity_bars,
        )
        return (candidate,)


__all__ = ["TrendPullbackSpecialist", "SETUP_FAMILY", "SETUP_VERSION"]
