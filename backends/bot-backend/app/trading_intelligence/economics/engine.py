"""Section 13 economic admission engine -- EV equations, penalties, gate
evaluation, assembled into one immutable ``EconomicOpportunity``.

Never imports the V2 threshold/confidence stack (Section 13.16) -- verified
by ``test_economics.py``'s import-boundary test, not just this docstring.
Pure function of (candidate, market_state, forecast, cost_estimate, policy):
no clock, no network, no broker client, no capital.
"""
from __future__ import annotations

from typing import Any, Optional

from app.trading_intelligence.contracts.economics import (
    AdmissionPolicy,
    AdmissionStatus,
    CostEstimate,
    EconomicOpportunity,
    EconomicsReasonCode,
)
from app.trading_intelligence.contracts.forecast import ForecastStatus, OutcomeForecast
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.economics.gates import evaluate_all_gates
from app.trading_intelligence.economics.policy import (
    default_admission_policy,
    distribution_shift_penalty_r,
    execution_uncertainty_penalty_r,
    uncertainty_penalty_r,
)

#: Gate failures here mean "we don't have enough/trustworthy evidence to
#: judge this candidate" -- distinct from "we judged it and it's not good".
_EVIDENCE_GATES = frozenset({
    "SUPPORT", "PROBABILITY_UNCERTAINTY", "FORECAST_UNCERTAINTY",
    "DISTRIBUTION_SHIFT", "DATA_QUALITY", "STATE_UNCERTAINTY", "COST_QUALITY",
})


def _unavailable_opportunity(
    candidate: SetupCandidate, policy: AdmissionPolicy, *, forecast_id: str, cost_estimate_id: str, reason_codes,
) -> EconomicOpportunity:
    return EconomicOpportunity(
        economic_opportunity_id=EconomicOpportunity.build_id(
            setup_candidate_id=candidate.setup_candidate_id, forecast_id=forecast_id,
            cost_estimate_id=cost_estimate_id, admission_policy_hash=policy.policy_hash,
        ),
        setup_candidate_id=candidate.setup_candidate_id, forecast_id=forecast_id, cost_estimate_id=cost_estimate_id,
        instrument_key=candidate.instrument_key, side=candidate.side, decision_time=candidate.decision_time,
        setup_family=candidate.setup_family,
        p_net_profitable_mean=0.0, credible_interval_low=0.0, credible_interval_high=1.0,
        p_target_before_stop=0.0, p_stop_before_target=0.0, p_timeout=1.0,
        ev_gross_r=0.0, fee_R=0.0, spread_R=0.0, slippage_R=0.0, funding_R=0.0, carry_R=0.0, cost_r=0.0,
        ev_net_r=0.0, uncertainty_penalty_r=0.0, distribution_shift_penalty_r=0.0, execution_uncertainty_penalty_r=0.0,
        conservative_edge_r=0.0, room_to_target_r=candidate.room_to_target_R, cost_share=None,
        raw_support=0, ess=0.0, backoff_level=0,
        state_uncertainty=1.0, forecast_uncertainty=1.0, ood_score=1.0,
        gate_results=(), admission_status=AdmissionStatus.INSUFFICIENT_EVIDENCE.value,
        reason_codes=tuple(reason_codes),
        admission_policy_version=policy.schema_version, admission_policy_hash=policy.policy_hash,
    )


def evaluate_economic_opportunity(
    candidate: SetupCandidate,
    market_state: Any,
    forecast: Optional[OutcomeForecast],
    cost_estimate: Optional[CostEstimate],
    *,
    policy: Optional[AdmissionPolicy] = None,
    user_id: Optional[str] = None,
    broker_account_id: Optional[str] = None,
    bot_instance_id: Optional[str] = None,
    run_id: Optional[str] = None,
    cycle_id: Optional[str] = None,
) -> EconomicOpportunity:
    base_policy = (policy or default_admission_policy()).for_family(candidate.setup_family)

    if forecast is None or not forecast.is_usable:
        reason = EconomicsReasonCode.FORECAST_UNAVAILABLE.value
        if forecast is not None and forecast.status == ForecastStatus.INSUFFICIENT_SUPPORT.value:
            reason = EconomicsReasonCode.INSUFFICIENT_SUPPORT.value
        return _unavailable_opportunity(
            candidate, base_policy,
            forecast_id=forecast.forecast_id if forecast else "none",
            cost_estimate_id=cost_estimate.cost_estimate_id if cost_estimate else "none",
            reason_codes=(reason,),
        )
    if cost_estimate is None:
        return _unavailable_opportunity(
            candidate, base_policy, forecast_id=forecast.forecast_id, cost_estimate_id="none",
            reason_codes=(EconomicsReasonCode.COST_ESTIMATE_UNAVAILABLE.value,),
        )

    # -- EV equations (Section 13.2) ------------------------------------------
    e_r_target = forecast.e_r_given_target if forecast.e_r_given_target is not None else candidate.room_to_target_R
    e_r_stop = forecast.e_r_given_stop if forecast.e_r_given_stop is not None else -1.0
    e_r_timeout = forecast.e_r_given_timeout if forecast.e_r_given_timeout is not None else 0.0
    e_r_target = e_r_target if e_r_target is not None else 0.0

    ev_gross_r = (
        forecast.p_target_before_stop * e_r_target
        + forecast.p_stop_before_target * e_r_stop
        + forecast.p_timeout * e_r_timeout
    )
    cost_r = cost_estimate.total_cost_R
    ev_net_r = ev_gross_r - cost_r  # costs subtracted exactly once

    shift = forecast.distribution_shift_assessment
    ood_score = shift.ood_score if shift else 1.0
    severity = shift.severity if shift else "HIGH"

    u_penalty = uncertainty_penalty_r(
        forecast_uncertainty=forecast.forecast_uncertainty,
        credible_interval_width=forecast.credible_interval_high - forecast.credible_interval_low,
        coefficient=base_policy.uncertainty_penalty_coefficient,
    )
    d_penalty = distribution_shift_penalty_r(ood_score=ood_score, severity=severity, coefficient=base_policy.distribution_shift_penalty_coefficient)
    liquidity_quality = 1.0 if market_state.liquidity_state.available else 0.0
    x_penalty = execution_uncertainty_penalty_r(
        cost_uncertainty_r=cost_estimate.cost_uncertainty_R, liquidity_quality=liquidity_quality,
        coefficient=base_policy.execution_uncertainty_penalty_coefficient,
    )
    conservative_edge_r = ev_net_r - u_penalty - d_penalty - x_penalty

    gate_results, cost_share = evaluate_all_gates(
        candidate=candidate, market_state=market_state, forecast=forecast, cost_estimate=cost_estimate,
        ev_gross_r=ev_gross_r, ev_net_r=ev_net_r, conservative_edge_r=conservative_edge_r, policy=base_policy,
    )

    failed = [g for g in gate_results if not g.passed]
    if not failed:
        status = AdmissionStatus.ECONOMICALLY_ADMISSIBLE.value
    elif any(g.gate in _EVIDENCE_GATES for g in failed):
        status = AdmissionStatus.INSUFFICIENT_EVIDENCE.value
    else:
        status = AdmissionStatus.ECONOMICALLY_INADMISSIBLE.value

    reason_codes = tuple(dict.fromkeys(g.reason_code for g in failed))

    return EconomicOpportunity(
        economic_opportunity_id=EconomicOpportunity.build_id(
            setup_candidate_id=candidate.setup_candidate_id, forecast_id=forecast.forecast_id,
            cost_estimate_id=cost_estimate.cost_estimate_id, admission_policy_hash=base_policy.policy_hash,
        ),
        setup_candidate_id=candidate.setup_candidate_id,
        forecast_id=forecast.forecast_id,
        cost_estimate_id=cost_estimate.cost_estimate_id,
        instrument_key=candidate.instrument_key,
        side=candidate.side,
        decision_time=candidate.decision_time,
        setup_family=candidate.setup_family,
        p_net_profitable_mean=forecast.p_net_profitable_mean,
        credible_interval_low=forecast.credible_interval_low,
        credible_interval_high=forecast.credible_interval_high,
        p_target_before_stop=forecast.p_target_before_stop,
        p_stop_before_target=forecast.p_stop_before_target,
        p_timeout=forecast.p_timeout,
        ev_gross_r=ev_gross_r,
        fee_R=cost_estimate.fee_R, spread_R=cost_estimate.spread_R, slippage_R=cost_estimate.slippage_R,
        funding_R=cost_estimate.funding_R, carry_R=cost_estimate.carry_R, cost_r=cost_r,
        ev_net_r=ev_net_r,
        uncertainty_penalty_r=u_penalty,
        distribution_shift_penalty_r=d_penalty,
        execution_uncertainty_penalty_r=x_penalty,
        conservative_edge_r=conservative_edge_r,
        room_to_target_r=candidate.room_to_target_R,
        cost_share=cost_share,
        raw_support=forecast.raw_support,
        ess=forecast.ess,
        backoff_level=forecast.backoff_level,
        state_uncertainty=market_state.state_uncertainty.value,
        forecast_uncertainty=forecast.forecast_uncertainty,
        ood_score=ood_score,
        gate_results=gate_results,
        lower_net_quantile_r=(forecast.gross_R_lower_quantile - cost_r) if forecast.gross_R_lower_quantile is not None else None,
        admission_status=status,
        reason_codes=reason_codes,
        admission_policy_version=base_policy.schema_version,
        admission_policy_hash=base_policy.policy_hash,
        user_id=user_id if cost_estimate.cost_scope == "ACCOUNT" else None,
        broker_account_id=broker_account_id if cost_estimate.cost_scope == "ACCOUNT" else None,
        bot_instance_id=bot_instance_id if cost_estimate.cost_scope == "ACCOUNT" else None,
        run_id=run_id if cost_estimate.cost_scope == "ACCOUNT" else None,
        cycle_id=cycle_id if cost_estimate.cost_scope == "ACCOUNT" else None,
    )


__all__ = ["evaluate_economic_opportunity"]
