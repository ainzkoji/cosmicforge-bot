"""Specialist B -- Breakout / Volatility Expansion V2 (Section 11.4).

Regime eligibility on ``VOL_EXPANSION`` already synthesizes prior compression,
current expansion, volatility acceleration, structural breakout and
participation expansion (Section 10.4's evidence design mirrors Section
11.4's prerequisites almost exactly) -- this specialist does not re-derive
that evidence, it consumes the regime weight plus the causal, already-
confirmed break direction from ``structure_state``.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from app.trading_intelligence.contracts.data_quality import Capability
from app.trading_intelligence.contracts.market_state import clamp01
from app.trading_intelligence.contracts.setup import SetupCandidate, SetupReasonCode, SetupSide, compute_geometry
from app.trading_intelligence.regime.contracts import RegimeClass
from app.trading_intelligence.setups._common import regime_weight, snapshot_identity
from app.trading_intelligence.setups.policy import BreakoutVolExpansionPolicy

SETUP_FAMILY = "BREAKOUT_VOL_EXPANSION_V2"
SETUP_VERSION = "2.0.0"


class BreakoutVolExpansionSpecialist:
    setup_family = SETUP_FAMILY
    setup_version = SETUP_VERSION
    required_capabilities: Tuple[Capability, ...] = (Capability.OHLCV,)

    def discover(
        self,
        *,
        snapshot: Any,
        market_state: Any,
        regime_distribution: Any,
        policy: BreakoutVolExpansionPolicy,
    ) -> Tuple[SetupCandidate, ...]:
        if not market_state.is_usable:
            return ()

        structure = market_state.structure_state
        volatility = market_state.volatility_state
        participation = market_state.participation_state

        if not structure.available or structure.range_high is None or structure.range_low is None:
            return ()

        # -- 11.4.1: direction must follow the confirmed causal break --------
        if structure.last_bos_direction not in ("UP", "DOWN"):
            return ()  # NO_CONFIRMED_BREAK
        side = SetupSide.LONG.value if structure.last_bos_direction == "UP" else SetupSide.SHORT.value

        if regime_weight(regime_distribution, RegimeClass.VOL_EXPANSION.value) < policy.regime_eligibility_floor:
            return ()

        range_width = structure.range_high - structure.range_low
        if range_width <= 0:
            return ()

        ident = snapshot_identity(snapshot)
        trigger_reference = ident["reference_price"]
        # invalidation_reference already resolves to the last swing low/high
        # opposite the break direction (Section 9.3) -- NOT range_high/
        # range_low, which include the breakout bar's own wick and can sit
        # on the wrong side of the current close.
        invalidation = structure.invalidation_reference
        if invalidation is None:
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

        # -- late-entry extension (measured in range-widths) ------------------
        if geometry.risk_distance > policy.max_late_entry_extension_range_fraction * range_width:
            return ()  # BREAKOUT_TOO_EXTENDED

        direction_sign = 1.0 if side == SetupSide.LONG.value else -1.0
        target_reference = trigger_reference + direction_sign * range_width  # classic measured-move projection

        geometry = compute_geometry(
            side=side, trigger_reference=trigger_reference,
            structural_invalidation=invalidation, target_reference=target_reference,
        )
        if geometry is None or geometry.room_to_target_R is None:
            return ()
        if geometry.room_to_target_R < policy.min_target_room_R:
            return ()

        if structure.failed_break_count > policy.min_boundary_touch_count:
            return ()  # RANGE_TRANSITION_RISK-style: too many prior failed breaks at this boundary

        # -- diagnostic evidence only -----------------------------------------
        components: Dict[str, float] = {
            "structure_integrity": clamp01(structure.structure_integrity),
            "expansion_strength": clamp01((volatility.expansion or 0.0) / 3.0) if volatility.available else 0.0,
            "compression_release": clamp01(volatility.compression) if volatility.compression is not None else 0.0,
        }
        if participation.available and participation.volume_acceleration is not None:
            components["participation_confirmation"] = clamp01(participation.volume_acceleration / 2.0)
        evidence_score = clamp01(sum(components.values()) / len(components))

        reason_codes = [SetupReasonCode.SETUP_FOUND.value]
        if structure.failed_break_count > 0:
            reason_codes.append(SetupReasonCode.RANGE_TRANSITION_RISK.value)
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
                "failed_break_count": float(structure.failed_break_count),
                "expansion": volatility.expansion or 0.0,
            },
            required_capabilities=tuple(c.value for c in self.required_capabilities),
            evidence_components=components,
            evidence_score=evidence_score,
            reason_codes=tuple(reason_codes),
            validity_bars=policy.candidate_validity_bars,
        )
        return (candidate,)


__all__ = ["BreakoutVolExpansionSpecialist", "SETUP_FAMILY", "SETUP_VERSION"]
