"""VetoEngine (Section 14). Pure function of already-computed CATI evidence
plus typed context objects: no clock, no network, no broker client, no
capital, no position slots, no V2 threshold.

Section 13 answers "is this economically admissible?". This answers "even
if it is, should current conditions veto or downgrade it?". It can never
UPGRADE a Section-13 insufficiency -- an opportunity whose admission status
is not ECONOMICALLY_ADMISSIBLE cannot become APPROVE_FOR_RANKING.
"""
from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.economics import AdmissionStatus, CostEstimate, EconomicOpportunity
from app.trading_intelligence.contracts.forecast import OutcomeForecast
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.contracts.events import (
    EventSourceState, MaintenanceSourceState, event_affects,
)
from app.trading_intelligence.contracts.system_health import BrokerHealthStatus
from app.trading_intelligence.contracts.veto import (
    EventRiskContext,
    FAMILY_ORDER,
    SystemHealthContext,
    VetoCheckResult,
    VetoCheckStatus as S,
    VetoDecision,
    VetoFamily as F,
    VetoOutcome,
    VetoPolicy,
    VetoReasonCode as R,
    VetoStage,
)

_HARD, _WATCH, _INFO = "HARD", "WATCH", "INFO"


def _chk(family, check, status, severity, observed, required, reason, **evidence) -> VetoCheckResult:
    return VetoCheckResult(
        family=family.value, check=check, status=status.value, severity=severity,
        observed_value=None if observed is None else float(observed),
        policy_value=None if required is None else float(required),
        reason_code=reason.value if status not in (S.PASS,) else R.OK.value,
        evidence=dict(evidence),
    )


def _graded(family, check, observed, watch_at, reject_at, reason) -> VetoCheckResult:
    """Higher observed = worse. FAIL >= reject_at, WATCH >= watch_at."""
    if observed is None:
        return _chk(family, check, S.NOT_EVALUATED, _INFO, None, watch_at, reason)
    if observed >= reject_at:
        return _chk(family, check, S.FAIL, _HARD, observed, reject_at, reason)
    if observed >= watch_at:
        return _chk(family, check, S.WATCH, _WATCH, observed, watch_at, reason)
    return _chk(family, check, S.PASS, _INFO, observed, watch_at, reason)


def _policy_status(name: str, *, allow_not_evaluated: bool = True) -> S:
    """Map a policy string to a check status. Unknown strings FAIL CLOSED
    (never silently NOT_EVALUATED). ``allow_not_evaluated=False`` is used
    where an evidence gap must never be waved through as "not evaluated"."""
    key = str(name).upper()
    if key == "NOT_EVALUATED":
        return S.NOT_EVALUATED if allow_not_evaluated else S.WATCH
    return {"WATCH": S.WATCH, "REJECT": S.FAIL, "FAIL": S.FAIL}.get(key, S.FAIL)


def _sev(status: S) -> str:
    return {S.FAIL: _HARD, S.WATCH: _WATCH}.get(status, _INFO)


# -- families -----------------------------------------------------------------
def _state_checks(market_state, regime, p: VetoPolicy) -> List[VetoCheckResult]:
    out = []
    if not market_state.is_usable:
        out.append(_chk(F.STATE, "market_state_usable", S.FAIL, _HARD, 0.0, 1.0, R.MARKET_STATE_INVALID))
        return out
    out.append(_chk(F.STATE, "market_state_usable", S.PASS, _INFO, 1.0, 1.0, R.MARKET_STATE_INVALID))
    shock = bool(market_state.volatility_state.shock_state)
    if shock and p.reject_on_shock_state:
        out.append(_chk(F.STATE, "shock_state", S.FAIL, _HARD, 1.0, 0.0, R.SHOCK_STATE))
    elif shock:
        out.append(_chk(F.STATE, "shock_state", S.WATCH, _WATCH, 1.0, 0.0, R.SHOCK_STATE))
    else:
        out.append(_chk(F.STATE, "shock_state", S.PASS, _INFO, 0.0, 0.0, R.SHOCK_STATE))
    out.append(_graded(F.STATE, "state_uncertainty", market_state.state_uncertainty.value,
                       p.state_uncertainty_watch, p.state_uncertainty_reject, R.STATE_UNCERTAINTY_HIGH))
    out.append(_graded(F.STATE, "transition_uncertainty", regime.transition_uncertainty,
                       p.transition_uncertainty_watch, p.transition_uncertainty_reject, R.TRANSITION_HIGH))
    return out


def _evidence_checks(opportunity: EconomicOpportunity, forecast: OutcomeForecast, p: VetoPolicy) -> List[VetoCheckResult]:
    out = []
    status = opportunity.admission_status
    if status == AdmissionStatus.ECONOMICALLY_ADMISSIBLE.value:
        out.append(_chk(F.EVIDENCE, "economic_admission", S.PASS, _INFO, 1.0, 1.0, R.ECONOMICALLY_INADMISSIBLE))
    elif status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value:
        st = _policy_status(p.insufficient_evidence_outcome, allow_not_evaluated=False)
        reason = R.INSUFFICIENT_SUPPORT if opportunity.raw_support < 1 or "INSUFFICIENT_SUPPORT" in opportunity.reason_codes \
            or "FORECAST_UNAVAILABLE" in opportunity.reason_codes else R.INSUFFICIENT_EVIDENCE
        out.append(_chk(F.EVIDENCE, "economic_admission", st, _sev(st), 0.0, 1.0, reason,
                        section13_status=status, section13_reasons=list(opportunity.reason_codes)))
    else:
        out.append(_chk(F.EVIDENCE, "economic_admission", S.FAIL, _HARD, 0.0, 1.0, R.ECONOMICALLY_INADMISSIBLE,
                        section13_reasons=list(opportunity.reason_codes)))
    if forecast is not None and forecast.is_usable:
        width = forecast.credible_interval_high - forecast.credible_interval_low
        out.append(_graded(F.EVIDENCE, "credible_interval_width", width,
                           p.credible_interval_width_watch, p.credible_interval_width_reject, R.WIDE_CREDIBLE_INTERVAL))
    else:
        out.append(_chk(F.EVIDENCE, "credible_interval_width", S.NOT_EVALUATED, _INFO, None, None, R.INSUFFICIENT_SUPPORT))
    cal = forecast.calibration_status if forecast is not None else "UNCALIBRATED"
    if cal in p.calibration_statuses_allowed_for_approval:
        out.append(_chk(F.EVIDENCE, "calibration_status", S.PASS, _INFO, None, None, R.UNCALIBRATED_PROBABILITY, calibration_status=cal))
    else:
        st = _policy_status(p.uncalibrated_outcome, allow_not_evaluated=False)
        out.append(_chk(F.EVIDENCE, "calibration_status", st, _sev(st), None, None, R.UNCALIBRATED_PROBABILITY, calibration_status=cal))
    return out


def _ood_checks(forecast: OutcomeForecast, p: VetoPolicy) -> List[VetoCheckResult]:
    shift = forecast.distribution_shift_assessment if forecast is not None else None
    if shift is None:
        return [_chk(F.OOD, "distribution_shift", S.NOT_EVALUATED, _INFO, None, p.ood_score_watch, R.DISTRIBUTION_SHIFT_HIGH)]
    out = [_graded(F.OOD, "distribution_shift", shift.ood_score, p.ood_score_watch, p.ood_score_reject, R.DISTRIBUTION_SHIFT_HIGH)]
    if shift.unseen_categories:
        st = _policy_status(p.unseen_bucket_outcome)
        out.append(_chk(F.OOD, "unseen_state_bucket", st, _sev(st), float(len(shift.unseen_categories)), 0.0,
                        R.UNSEEN_STATE_BUCKET, unseen=list(shift.unseen_categories)))
    else:
        out.append(_chk(F.OOD, "unseen_state_bucket", S.PASS, _INFO, 0.0, 0.0, R.UNSEEN_STATE_BUCKET))
    if shift.capability_shift_flags:
        st = _policy_status(p.capability_shift_outcome)
        out.append(_chk(F.OOD, "capability_pattern_shift", st, _sev(st), float(len(shift.capability_shift_flags)), 0.0,
                        R.CAPABILITY_PATTERN_SHIFT, flags=list(shift.capability_shift_flags)))
    else:
        out.append(_chk(F.OOD, "capability_pattern_shift", S.PASS, _INFO, 0.0, 0.0, R.CAPABILITY_PATTERN_SHIFT))
    return out


def _geometry_checks(candidate: SetupCandidate, market_state, p: VetoPolicy) -> List[VetoCheckResult]:
    out = []
    room = candidate.room_to_target_R
    if room is None or room < p.min_room_to_target_r:
        out.append(_chk(F.GEOMETRY, "room_to_target_r", S.FAIL, _HARD, room, p.min_room_to_target_r, R.POOR_REWARD_GEOMETRY))
    else:
        out.append(_chk(F.GEOMETRY, "room_to_target_r", S.PASS, _INFO, room, p.min_room_to_target_r, R.POOR_REWARD_GEOMETRY))
    frac = candidate.initial_structural_risk / candidate.trigger_reference if candidate.trigger_reference else None
    if frac is None:
        out.append(_chk(F.GEOMETRY, "structural_risk_width", S.NOT_EVALUATED, _INFO, None, p.max_structural_risk_fraction, R.STRUCTURAL_INVALIDATION_TOO_WIDE))
    elif frac > p.max_structural_risk_fraction:
        out.append(_chk(F.GEOMETRY, "structural_risk_width", S.FAIL, _HARD, frac, p.max_structural_risk_fraction, R.STRUCTURAL_INVALIDATION_TOO_WIDE))
    else:
        out.append(_chk(F.GEOMETRY, "structural_risk_width", S.PASS, _INFO, frac, p.max_structural_risk_fraction, R.STRUCTURAL_INVALIDATION_TOO_WIDE))
    ext = market_state.trend_state.extension_atr if market_state.trend_state.available else None
    out.append(_graded(F.GEOMETRY, "late_entry_extension_atr", None if ext is None else abs(ext),
                       p.late_entry_extension_atr_watch, p.late_entry_extension_atr_reject, R.LATE_ENTRY))
    return out


def _cost_checks(cost: CostEstimate, p: VetoPolicy) -> List[VetoCheckResult]:
    out = []
    for check, value, limit, reason in (
        ("spread_r", cost.spread_R, p.max_spread_r, R.SPREAD_ANOMALY),
        ("slippage_r", cost.slippage_R, p.max_slippage_r, R.SLIPPAGE_TOO_HIGH),
        ("funding_r", cost.funding_R + cost.carry_R, p.max_funding_r, R.FUNDING_COST_EXCESSIVE),
    ):
        st = S.FAIL if value > limit else S.PASS
        out.append(_chk(F.COST, check, st, _sev(st), value, limit, reason))
    st = S.WATCH if cost.cost_uncertainty_R > p.cost_uncertainty_r_watch else S.PASS
    out.append(_chk(F.COST, "cost_uncertainty_r", st, _sev(st), cost.cost_uncertainty_R, p.cost_uncertainty_r_watch, R.COST_UNCERTAINTY_HIGH))
    return out


def _liquidity_checks(market_state, p: VetoPolicy) -> List[VetoCheckResult]:
    liq = market_state.liquidity_state
    if not liq.available:
        st = _policy_status(p.missing_liquidity_status)
        return [_chk(F.LIQUIDITY, "book_capability", st, _sev(st), None, None, R.LIQUIDITY_UNVERIFIED)]
    out = []
    if liq.stale_book:
        st = _policy_status(p.stale_book_outcome)
        out.append(_chk(F.LIQUIDITY, "book_freshness", st, _sev(st), None, None, R.STALE_BOOK))
    else:
        out.append(_chk(F.LIQUIDITY, "book_freshness", S.PASS, _INFO, None, None, R.STALE_BOOK))
    if liq.top_book_depth is None:  # missing depth is NOT zero depth
        out.append(_chk(F.LIQUIDITY, "depth", S.NOT_EVALUATED, _INFO, None, p.min_top_book_depth, R.DEPTH_INSUFFICIENT))
    elif liq.top_book_depth < p.min_top_book_depth:
        out.append(_chk(F.LIQUIDITY, "depth", S.FAIL, _HARD, liq.top_book_depth, p.min_top_book_depth, R.DEPTH_INSUFFICIENT))
    else:
        out.append(_chk(F.LIQUIDITY, "depth", S.PASS, _INFO, liq.top_book_depth, p.min_top_book_depth, R.DEPTH_INSUFFICIENT))
    out.append(_graded(F.LIQUIDITY, "spread_percentile", liq.spread_percentile,
                       p.spread_percentile_watch, p.spread_percentile_reject, R.LIQUIDITY_DETERIORATION))
    return out


def _crowding_checks(market_state, side: str, p: VetoPolicy) -> List[VetoCheckResult]:
    d = market_state.derivatives_state
    if not d.available or (d.funding_current is None and d.crowding_state is None):
        st = _policy_status(p.missing_crowding_status)
        return [_chk(F.CROWDING, "derivatives_capability", st, _sev(st), None, None, R.CROWDING_UNVERIFIED)]
    out = []
    pct = d.funding_percentile
    extreme = False
    if pct is not None:
        extreme = (side == "LONG" and pct >= p.funding_percentile_extreme) or (
            side == "SHORT" and pct <= 1.0 - p.funding_percentile_extreme)
    st = S.FAIL if extreme else (S.NOT_EVALUATED if pct is None else S.PASS)
    out.append(_chk(F.CROWDING, "funding_extreme", st, _sev(st), pct, p.funding_percentile_extreme, R.FUNDING_EXTREME))
    crowded = (side == "LONG" and d.crowding_state == "CROWDED_LONG") or (side == "SHORT" and d.crowding_state == "CROWDED_SHORT")
    st = S.WATCH if crowded else (S.NOT_EVALUATED if d.crowding_state is None else S.PASS)
    out.append(_chk(F.CROWDING, "oi_crowding", st, _sev(st), None, None, R.OI_CROWDING, crowding_state=d.crowding_state))
    return out


def _event_checks(candidate: SetupCandidate, ctx: Optional[EventRiskContext], p: VetoPolicy) -> List[VetoCheckResult]:
    """Calendar events and venue maintenance are separate capabilities, each
    with an explicit source state. Neither an absent nor a stale source is
    ever read as "no event" / "no maintenance"."""
    out: List[VetoCheckResult] = []
    key = candidate.instrument_key
    start, end = candidate.decision_time, candidate.decision_time + p.event_lookahead_ms

    # -- maintenance capability --------------------------------------------------------
    maint_ctx = ctx.maintenance if ctx is not None else None
    m_state = maint_ctx.state if maint_ctx is not None else MaintenanceSourceState.UNAVAILABLE.value
    if m_state == MaintenanceSourceState.AVAILABLE.value:
        policy = ctx.scope_policy
        maint = [w for w in maint_ctx.windows if w.overlaps(start, end) and event_affects(w, key, policy)]
        out.append(_chk(F.EVENT, "maintenance_window", S.FAIL if maint else S.PASS, _HARD if maint else _INFO,
                        float(len(maint)), 0.0, R.EXCHANGE_MAINTENANCE, events=[w.event_id for w in maint]))
    elif m_state == MaintenanceSourceState.STALE.value:
        st = _policy_status(p.maintenance_stale_status)
        out.append(_chk(F.EVENT, "maintenance_source", st, _sev(st), None, None, R.MAINTENANCE_SOURCE_STALE,
                        maintenance_state=m_state))
    else:
        st = _policy_status(p.maintenance_unavailable_status)
        out.append(_chk(F.EVENT, "maintenance_source", st, _sev(st), None, None, R.MAINTENANCE_SOURCE_UNAVAILABLE,
                        maintenance_state=m_state))

    # -- scheduled-event calendar -------------------------------------------------------
    state = ctx.source_state if ctx is not None else EventSourceState.UNAVAILABLE.value
    if state == EventSourceState.AVAILABLE.value:
        hits = [e for e in ctx.events
                if not e.is_maintenance and e.overlaps(start, end) and event_affects(e, key, ctx.scope_policy)]
        out.append(_chk(F.EVENT, "event_risk_window", S.FAIL if hits else S.PASS, _HARD if hits else _INFO,
                        float(len(hits)), 0.0, R.EVENT_RISK_WINDOW, events=[e.event_id for e in hits],
                        types=sorted({e.event_type for e in hits})))
    elif state == EventSourceState.STALE.value:
        st = _policy_status(p.event_stale_status)
        out.append(_chk(F.EVENT, "event_source", st, _sev(st), None, None, R.EVENT_SOURCE_STALE, source_state=state))
    else:
        st = _policy_status(p.event_unavailable_status)
        out.append(_chk(F.EVENT, "event_source", st, _sev(st), None, None, R.EVENT_SOURCE_UNAVAILABLE, source_state=state))
    return out


def _portfolio_checks(stage: str, findings: Sequence[str]) -> List[VetoCheckResult]:
    if stage != VetoStage.PORTFOLIO_STAGE.value:
        # Do not reject merely because portfolio context is not yet available.
        return [_chk(F.PORTFOLIO, "portfolio_context", S.NOT_EVALUATED, _INFO, None, None, R.PORTFOLIO_CONTEXT_PENDING)]
    if not findings:
        return [_chk(F.PORTFOLIO, "portfolio_context", S.PASS, _INFO, 0.0, 0.0, R.DUPLICATE_EXPOSURE)]
    out = []
    for code in findings:
        try:
            reason = R(code)
        except ValueError:
            reason = R.PORTFOLIO_DATA_QUALITY_FAULT
        out.append(_chk(F.PORTFOLIO, "portfolio_finding", S.FAIL, _HARD, 1.0, 0.0, reason))
    return out


def _broker_health_check(ctx: Optional[SystemHealthContext], decision_time: int, p: VetoPolicy) -> VetoCheckResult:
    bh = ctx.broker_health if ctx is not None else None
    if bh is None:
        st = _policy_status(p.missing_broker_health_status, allow_not_evaluated=False)
        return _chk(F.SYSTEM, "broker_health", st, _sev(st), None, None, R.BROKER_HEALTH_NOT_PROVIDED)
    ev = dict(broker_status=bh.status, health_source=bh.source, broker_account_id=bh.broker_account_id, venue=bh.venue,
              environment=bh.environment, source_reasons=list(bh.reason_codes))
    stale = bh.freshness_ms is not None and bh.freshness_ms > p.broker_health_max_age_ms
    if bh.status == BrokerHealthStatus.UNAVAILABLE.value:
        return _chk(F.SYSTEM, "broker_health", S.FAIL, _HARD, None, None, R.BROKER_UNAVAILABLE, **ev)
    if bh.status == BrokerHealthStatus.UNKNOWN.value or stale:
        st = _policy_status(p.unknown_broker_health_status, allow_not_evaluated=False)
        return _chk(F.SYSTEM, "broker_health", st, _sev(st), bh.freshness_ms, p.broker_health_max_age_ms,
                    R.BROKER_HEALTH_STALE if stale and bh.status != BrokerHealthStatus.UNKNOWN.value else R.BROKER_UNKNOWN, **ev)
    if bh.status == BrokerHealthStatus.DEGRADED.value:
        severe = any(r in p.broker_degraded_reject_reasons for r in bh.reason_codes)
        st = S.FAIL if severe else _policy_status(p.broker_degraded_status, allow_not_evaluated=False)
        return _chk(F.SYSTEM, "broker_health", st, _sev(st), None, None, R.BROKER_DEGRADED, **ev)
    return _chk(F.SYSTEM, "broker_health", S.PASS, _INFO, None, None, R.BROKER_DEGRADED, **ev)


def _system_checks(market_state, ctx: Optional[SystemHealthContext], decision_time: int, p: VetoPolicy) -> List[VetoCheckResult]:
    out = []
    dq = market_state.data_quality.level.value
    fault = dq == "INVALID" or (ctx is not None and ctx.data_quality_fault)
    out.append(_chk(F.SYSTEM, "data_quality", S.FAIL if fault else S.PASS, _HARD if fault else _INFO,
                    0.0 if fault else 1.0, 1.0, R.DATA_QUALITY_FAULT, market_state_quality=dq))
    out.append(_broker_health_check(ctx, decision_time, p))
    errs = ctx.component_errors if ctx is not None else ()
    out.append(_chk(F.SYSTEM, "component_errors", S.FAIL if errs else S.PASS, _HARD if errs else _INFO,
                    float(len(errs)), 0.0, R.CATI_COMPONENT_ERROR, errors=list(errs)))
    return out


def _resolve(checks: Sequence[VetoCheckResult], opportunity: EconomicOpportunity) -> str:
    if any(c.status == S.FAIL.value for c in checks):
        return VetoOutcome.REJECT.value
    if opportunity.admission_status != AdmissionStatus.ECONOMICALLY_ADMISSIBLE.value:
        # Defensive: even if the policy chose a soft status for the Section-13
        # insufficiency, it can never become APPROVE_FOR_RANKING.
        return VetoOutcome.WATCH.value
    if any(c.status == S.WATCH.value for c in checks):
        return VetoOutcome.WATCH.value
    return VetoOutcome.APPROVE_FOR_RANKING.value


def evaluate_veto(
    *,
    opportunity: EconomicOpportunity,
    candidate: SetupCandidate,
    market_state: Any,
    regime_distribution: Any,
    forecast: OutcomeForecast,
    cost_estimate: CostEstimate,
    policy: Optional[VetoPolicy] = None,
    event_context: Optional[EventRiskContext] = None,
    system_context: Optional[SystemHealthContext] = None,
    stage: str = VetoStage.PRE_RANKING.value,
    portfolio_findings: Sequence[str] = (),
    user_id: Optional[str] = None,
    broker_account_id: Optional[str] = None,
    bot_instance_id: Optional[str] = None,
    run_id: Optional[str] = None,
    cycle_id: Optional[str] = None,
) -> VetoDecision:
    p = (policy or VetoPolicy()).for_family(candidate.setup_family)
    checks: List[VetoCheckResult] = []
    checks += _state_checks(market_state, regime_distribution, p)
    checks += _evidence_checks(opportunity, forecast, p)
    checks += _ood_checks(forecast, p)
    checks += _geometry_checks(candidate, market_state, p)
    checks += _cost_checks(cost_estimate, p)
    checks += _liquidity_checks(market_state, p)
    checks += _crowding_checks(market_state, candidate.side, p)
    checks += _event_checks(candidate, event_context, p)
    checks += _portfolio_checks(stage, portfolio_findings)
    checks += _system_checks(market_state, system_context, candidate.decision_time, p)

    order = {name: i for i, name in enumerate(FAMILY_ORDER)}
    checks.sort(key=lambda c: order[c.family])  # stable: preserves intra-family order

    reasons = tuple(dict.fromkeys(
        c.reason_code for c in checks if c.status in (S.FAIL.value, S.WATCH.value)
    ))
    outcome = _resolve(checks, opportunity)
    tenant = any(v is not None for v in (user_id, broker_account_id, bot_instance_id))
    return VetoDecision(
        veto_decision_id=VetoDecision.build_id(
            economic_opportunity_id=opportunity.economic_opportunity_id, veto_policy_hash=p.policy_hash,
            stage=stage, portfolio_findings=tuple(portfolio_findings),
        ),
        economic_opportunity_id=opportunity.economic_opportunity_id,
        setup_candidate_id=candidate.setup_candidate_id,
        forecast_id=opportunity.forecast_id,
        market_state_id=market_state.market_state_id,
        instrument_key=candidate.instrument_key, side=candidate.side, decision_time=candidate.decision_time,
        outcome=outcome, stage=stage, checks=tuple(checks), reason_codes=reasons,
        veto_policy_version=p.schema_version, veto_policy_hash=p.policy_hash,
        data_quality=market_state.data_quality.level.value,
        created_at=candidate.decision_time,
        user_id=user_id if tenant else None, broker_account_id=broker_account_id if tenant else None,
        bot_instance_id=bot_instance_id if tenant else None, run_id=run_id if tenant else None,
        cycle_id=cycle_id if tenant else None,
    )


def component_error_decision(
    *, opportunity: EconomicOpportunity, candidate: SetupCandidate, market_state: Any, error: str,
    policy: Optional[VetoPolicy] = None,
) -> VetoDecision:
    """Terminal REJECT recording an internal CATI fault (Section 15.2's
    'explicit CATI_COMPONENT_ERROR' terminal state)."""
    p = policy or VetoPolicy()
    check = _chk(F.SYSTEM, "component_errors", S.FAIL, _HARD, 1.0, 0.0, R.CATI_COMPONENT_ERROR, errors=[error])
    return VetoDecision(
        veto_decision_id=VetoDecision.build_id(
            economic_opportunity_id=opportunity.economic_opportunity_id, veto_policy_hash=p.policy_hash,
            stage=VetoStage.PRE_RANKING.value, portfolio_findings=("CATI_COMPONENT_ERROR",),
        ),
        economic_opportunity_id=opportunity.economic_opportunity_id, setup_candidate_id=candidate.setup_candidate_id,
        forecast_id=opportunity.forecast_id, market_state_id=market_state.market_state_id,
        instrument_key=candidate.instrument_key, side=candidate.side, decision_time=candidate.decision_time,
        outcome=VetoOutcome.REJECT.value, stage=VetoStage.PRE_RANKING.value, checks=(check,),
        reason_codes=(R.CATI_COMPONENT_ERROR.value,), veto_policy_version=p.schema_version,
        veto_policy_hash=p.policy_hash, data_quality=market_state.data_quality.level.value,
        created_at=candidate.decision_time,
    )


__all__ = ["evaluate_veto", "component_error_decision"]
