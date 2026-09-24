"""Deterministic ExitDecision engine (Sections 19.10-19.18). ADVISORY ONLY.

ONE canonical action per evaluation, first match wins:

 1. hard thesis invalidation (structural level broken / core thesis FAIL) -> EXIT
 2. mandatory event / shock / liquidity / system de-risk          -> EXIT | REDUCE
    (thesis UNKNOWN or untrustworthy evidence stops here           -> NO_CHANGE_FALLBACK)
 3. conservative remaining edge < exit floor                      -> EXIT
 4. marginal edge / weakened thesis without profit to harvest     -> REDUCE
 5. profitable path, positive residual edge, decayed/target zone  -> TAKE_PARTIAL
 6. a strictly tighter, venue-sane stop exists                    -> TIGHTEN_PROTECTION
 7. VALID thesis, edge >= hold floor, protection valid            -> HOLD
 8. otherwise                                                     -> NO_CHANGE_FALLBACK

Nothing here touches a broker, PositionManager, quantity or leverage.
REDUCE / TAKE_PARTIAL carry a FRACTION (policy-controlled, 0 < f < 1); the
executor converts it to a venue quantity later. TIGHTEN can never widen:
``protection_is_tighter`` is enforced here AND in ExitDecision itself.
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

from app.trading_intelligence.contracts.forecast import CalibrationStatus
from app.trading_intelligence.contracts.position import (
    ConditionKind as K, ConditionResult as C, DeRiskBehavior as D, ExitAction as A, ExitDecision, ExitPolicy,
    InsufficientSupportBehavior, PositionConditionCode as PC, PositionForecast, PositionPathSnapshot,
    PositionReasonCode as PR, ProtectionState, ThesisStatus as TS, protection_is_tighter,
)
from app.trading_intelligence.contracts.trade_plan import ThesisCode as T, TradePlan
from app.trading_intelligence.versions import EXIT_ENGINE_VERSION


def validate_suggested_protection(*, side: str, existing_stop: Optional[float], proposed: Optional[float],
                                  current_price: float, R: float, policy: ExitPolicy) -> Tuple[bool, str]:
    """A suggestion is valid only when it strictly tightens the EXISTING stop
    by at least the policy minimum and stays a sane distance from price."""
    if existing_stop is None or proposed is None:
        return False, PR.PROTECTION_STATE_INVALID.value
    if not protection_is_tighter(side, existing_stop, proposed):
        return False, PR.PROTECTION_WIDENING_REJECTED.value
    improvement = abs(float(proposed) - float(existing_stop)) / R
    if improvement < policy.tighten_protection_min_improvement_R:
        return False, "PROTECTION_IMPROVEMENT_TOO_SMALL"
    room = (current_price - proposed) if side == "LONG" else (proposed - current_price)
    if room < policy.tighten_min_distance_from_price_R * R:
        return False, "PROTECTION_TOO_CLOSE_TO_PRICE"
    return True, ""


def suggest_tighter_stop(plan: TradePlan, path: PositionPathSnapshot, market_state: Any,
                         policy: ExitPolicy) -> Tuple[Optional[float], Optional[str]]:
    existing = path.current_stop_price
    if existing is None:
        return None, None
    R, side = path.original_R_reference, plan.side
    candidates = []
    s = getattr(market_state, "structure_state", None) if market_state is not None else None
    if s is not None and s.available and s.invalidation_reference is not None:
        candidates.append((float(s.invalidation_reference), PR.PROTECTION_TIGHTENED_STRUCTURAL.value))
    if path.mfe_R >= policy.break_even_trigger_R:
        candidates.append((float(path.entry_price), PR.PROTECTION_TIGHTENED_BREAK_EVEN.value))
    valid = [(px, why) for px, why in candidates
             if validate_suggested_protection(side=side, existing_stop=existing, proposed=px,
                                              current_price=path.current_price, R=R, policy=policy)[0]]
    if not valid:
        return None, None
    return (max if side == "LONG" else min)(valid, key=lambda c: c[0])


def adaptive_evidence(forecast: PositionForecast, policy: ExitPolicy) -> Tuple[bool, List[str]]:
    why: List[str] = []
    if not forecast.trustworthy:
        why.append(PR.POSITION_FORECAST_SUPPORT_INSUFFICIENT.value if forecast.status ==
                   "POSITION_FORECAST_SUPPORT_INSUFFICIENT" else forecast.status)
    if forecast.current_forecast_uncertainty > policy.maximum_forecast_uncertainty:
        why.append(PR.FORECAST_UNCERTAINTY_TOO_HIGH.value)
    if forecast.current_OOD_score > policy.maximum_OOD:
        why.append(PR.OOD_TOO_HIGH.value)
    if forecast.updated_ESS < policy.minimum_ESS:
        why.append(PR.ESS_TOO_LOW.value)
    if policy.require_calibrated_library and forecast.calibration_status != CalibrationStatus.CALIBRATED.value:
        why.append(PR.LIBRARY_NOT_CALIBRATED.value)
    return (not why), why


def _fails(forecast: PositionForecast, code) -> bool:
    c = code.value if hasattr(code, "value") else code
    return any(r.code == c and r.result == C.FAIL.value for r in forecast.thesis_condition_results)


def _has_reason(forecast: PositionForecast, code, reason) -> bool:
    c = code.value if hasattr(code, "value") else code
    return any(r.code == c and reason.value in r.reason_codes for r in forecast.thesis_condition_results)


def _in_target_zone(plan: TradePlan, path: PositionPathSnapshot) -> bool:
    for z in plan.target_zones:
        if plan.side == "LONG" and path.current_price >= z.price_low:
            return True
        if plan.side == "SHORT" and path.current_price <= z.price_high:
            return True
    return False


def decide_exit(*, plan: TradePlan, path: PositionPathSnapshot, forecast: PositionForecast, market_state: Any = None,
                policy: Optional[ExitPolicy] = None, evaluation_mode: str = "SHADOW",
                extra_reasons: Tuple[str, ...] = ()) -> ExitDecision:
    policy = policy or ExitPolicy()
    reasons: List[str] = list(extra_reasons)
    ts, E = forecast.thesis_status, forecast.conservative_remaining_edge_R
    fraction: Optional[float] = None
    protect: Optional[float] = None

    def done(action: str) -> ExitDecision:
        return ExitDecision.build(
            position_forecast_id=forecast.position_forecast_id, position_id=path.position_id,
            trade_plan_id=plan.trade_plan_id, position_path_id=path.position_path_id, user_id=path.user_id,
            broker_account_id=path.broker_account_id, bot_instance_id=path.bot_instance_id, action=action,
            requested_fraction=fraction, suggested_protection_price=protect,
            existing_protection_price=path.current_stop_price, side=plan.side, decision_time=path.current_time,
            conservative_remaining_edge_R=E, thesis_status=ts, reason_codes=tuple(dict.fromkeys(reasons)),
            policy_version=policy.schema_version, policy_hash=policy.policy_hash, engine_version=EXIT_ENGINE_VERSION,
            lineage=(("trade_plan", plan.trade_plan_id), ("trade_plan_hash", plan.trade_plan_hash),
                     ("position", path.position_id), ("position_path", path.position_path_id),
                     ("market_state", str(forecast.market_state_id)),
                     ("regime_distribution", str(forecast.regime_distribution_id)),
                     ("position_forecast", forecast.position_forecast_id)),
            evaluation_mode=evaluation_mode,
        )

    # 1. hard thesis invalidation ------------------------------------------------------------
    if _fails(forecast, PC.STRUCTURAL_INVALIDATION_INTACT):
        reasons += [PR.STRUCTURAL_INVALIDATION_BROKEN.value, PR.THESIS_INVALIDATED.value]
        return done(A.EXIT.value)
    if ts == TS.INVALIDATED.value:
        reasons.append(PR.THESIS_INVALIDATED.value)
        reasons += [f"FAILED:{r.code}" for r in forecast.thesis_condition_results
                    if r.result == C.FAIL.value and r.kind == K.CORE.value]
        return done(A.EXIT.value)

    # 2. mandatory de-risk ----------------------------------------------------------------------
    behaviours: List[Tuple[str, str]] = []
    if _fails(forecast, PC.NO_SHOCK_STATE):
        behaviours.append((policy.shock_de_risk_behavior, PR.SHOCK_DE_RISK_REQUIRED.value))
    if _fails(forecast, T.EVENT_CONTEXT_ACCEPTABLE):
        if _has_reason(forecast, T.EVENT_CONTEXT_ACCEPTABLE, PR.SYSTEM_DE_RISK_REQUIRED):
            behaviours.append((D.EXIT.value, PR.SYSTEM_DE_RISK_REQUIRED.value))
        else:
            behaviours.append((policy.event_de_risk_behavior, PR.EVENT_DE_RISK_REQUIRED.value))
    if _fails(forecast, T.LIQUIDITY_NOT_DEGRADED):
        behaviours.append((policy.liquidity_de_risk_behavior, PR.LIQUIDITY_DE_RISK_REQUIRED.value))
    if _fails(forecast, T.BROKER_HEALTH_ACCEPTABLE):
        behaviours.append((D.EXIT.value, PR.SYSTEM_DE_RISK_REQUIRED.value))
    active = [(b, r) for b, r in behaviours if b != D.NONE.value]
    if active:
        reasons += [r for _b, r in active]
        if any(b == D.EXIT.value for b, _r in active):
            return done(A.EXIT.value)
        fraction = policy.default_reduce_fraction
        return done(A.REDUCE.value)

    # evidence gate for every adaptive action ------------------------------------------------------
    if ts == TS.UNKNOWN.value:
        reasons += [PR.THESIS_UNKNOWN.value, PR.NO_TRUSTWORTHY_INTENT.value]
        return done(A.NO_CHANGE_FALLBACK.value)
    ok, why = adaptive_evidence(forecast, policy)
    if not ok:
        reasons += why
        if policy.insufficient_support_behavior == InsufficientSupportBehavior.THESIS_BASED.value \
                and ts == TS.WEAKENED.value:
            reasons.append(PR.THESIS_WEAKENED.value)
            fraction = policy.default_reduce_fraction
            return done(A.REDUCE.value)
        reasons.append(PR.NO_TRUSTWORTHY_INTENT.value)
        return done(A.NO_CHANGE_FALLBACK.value)

    # 3. remaining-edge exit -------------------------------------------------------------------------
    if E < policy.exit_edge_floor:
        reasons.append(PR.REMAINING_EDGE_BELOW_EXIT_FLOOR.value)
        before_costs = E + forecast.remaining_cost_R
        if before_costs >= policy.exit_edge_floor:
            reasons.append(PR.HOLDING_COSTS_DESTROY_EDGE.value)
        return done(A.EXIT.value)

    weakened = ts == TS.WEAKENED.value
    if weakened:
        reasons.append(PR.THESIS_WEAKENED.value)
    enough_partials = path.partial_exit_count < policy.max_partial_exits
    if not enough_partials:
        reasons.append(PR.MAX_PARTIALS_REACHED.value)
    progress = (path.current_R >= policy.partial_min_progress_R
                and path.elapsed_bars >= policy.minimum_elapsed_bars_before_partial and enough_partials)
    marginal = E < policy.hold_edge_floor or weakened

    # 4. REDUCE (risk reduction) -----------------------------------------------------------------------
    if marginal and (not progress or E <= 0):
        reasons.append(PR.REMAINING_EDGE_MARGINAL.value)
        fraction = policy.default_reduce_fraction
        return done(A.REDUCE.value)

    # 5. TAKE_PARTIAL (profit harvest) -----------------------------------------------------------------
    target_zone = _in_target_zone(plan, path)
    if progress and E > 0 and (E < policy.hold_edge_floor + policy.partial_edge_band or weakened or target_zone):
        reasons.append(PR.PROFIT_HARVEST.value)
        if target_zone:
            reasons.append(PR.TARGET_ZONE_REACHED.value)
        fraction = min(policy.take_partial_fraction, policy.maximum_partial_fraction)
        return done(A.TAKE_PARTIAL.value)

    protection_ok = (path.protection_state == ProtectionState.PROTECTED.value and path.current_stop_price is not None
                     and (path.remaining_risk_distance or 0.0) > 0)

    # 6. TIGHTEN_PROTECTION ------------------------------------------------------------------------------
    if protection_ok:
        px, why_t = suggest_tighter_stop(plan, path, market_state, policy)
        if px is not None:
            protect = px
            reasons.append(why_t)
            return done(A.TIGHTEN_PROTECTION.value)

    # 7. HOLD -----------------------------------------------------------------------------------------------
    if ts == TS.VALID.value and E >= policy.hold_edge_floor and protection_ok:
        reasons.append(PR.REMAINING_EDGE_ABOVE_HOLD_FLOOR.value)
        return done(A.HOLD.value)

    # 8. fallback --------------------------------------------------------------------------------------------
    if not protection_ok:
        reasons.append(PR.PROTECTION_STATE_INVALID.value)
    reasons.append(PR.NO_TRUSTWORTHY_INTENT.value)
    return done(A.NO_CHANGE_FALLBACK.value)


__all__ = ["validate_suggested_protection", "suggest_tighter_stop", "adaptive_evidence", "decide_exit"]
