"""Admission gate evaluation (Section 13.5-13.13). Every gate emits its own
typed ``AdmissionGateResult`` -- no single hidden aggregate score ever
replaces this evidence.
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

from app.trading_intelligence.contracts.economics import (
    AdmissionGate,
    AdmissionGateResult,
    AdmissionPolicy,
    CostEstimate,
    EconomicsReasonCode,
)
from app.trading_intelligence.contracts.forecast import OutcomeForecast
from app.trading_intelligence.contracts.setup import SetupCandidate


def _gate(gate: AdmissionGate, passed: bool, observed: Optional[float], required: Optional[float], reason: EconomicsReasonCode) -> AdmissionGateResult:
    return AdmissionGateResult(
        gate=gate.value, passed=passed, observed_value=observed, required_value=required,
        reason_code=reason.value if not passed else "OK",
    )


def support_gate(forecast: OutcomeForecast, policy: AdmissionPolicy) -> AdmissionGateResult:
    passed = forecast.raw_support >= policy.minimum_raw_support and forecast.ess >= policy.minimum_ess
    reason = EconomicsReasonCode.INSUFFICIENT_ESS if forecast.ess < policy.minimum_ess else EconomicsReasonCode.INSUFFICIENT_SUPPORT
    return _gate(AdmissionGate.SUPPORT, passed, float(forecast.raw_support), float(policy.minimum_raw_support), reason)


def probability_uncertainty_gate(forecast: OutcomeForecast, policy: AdmissionPolicy) -> AdmissionGateResult:
    width = forecast.credible_interval_high - forecast.credible_interval_low
    passed = width <= policy.maximum_credible_interval_width
    return _gate(AdmissionGate.PROBABILITY_UNCERTAINTY, passed, width, policy.maximum_credible_interval_width, EconomicsReasonCode.CREDIBLE_INTERVAL_TOO_WIDE)


def net_edge_gate(ev_gross_r: float, ev_net_r: float, policy: AdmissionPolicy) -> AdmissionGateResult:
    if ev_gross_r <= 0:
        return _gate(AdmissionGate.NET_EDGE, False, ev_gross_r, 0.0, EconomicsReasonCode.NEGATIVE_GROSS_EV)
    passed = ev_net_r > policy.minimum_ev_net_r
    return _gate(AdmissionGate.NET_EDGE, passed, ev_net_r, policy.minimum_ev_net_r, EconomicsReasonCode.NET_EDGE_BELOW_FLOOR)


def conservative_edge_gate(conservative_edge_r: float, policy: AdmissionPolicy) -> AdmissionGateResult:
    passed = conservative_edge_r > policy.minimum_conservative_edge_r
    return _gate(AdmissionGate.CONSERVATIVE_EDGE, passed, conservative_edge_r, policy.minimum_conservative_edge_r, EconomicsReasonCode.CONSERVATIVE_EDGE_BELOW_FLOOR)


def reward_geometry_gate(candidate: SetupCandidate, policy: AdmissionPolicy) -> AdmissionGateResult:
    if candidate.initial_structural_risk <= 0:
        return _gate(AdmissionGate.REWARD_GEOMETRY, False, candidate.initial_structural_risk, 0.0, EconomicsReasonCode.INVALID_GEOMETRY)
    room = candidate.room_to_target_R
    if room is None:
        return _gate(AdmissionGate.REWARD_GEOMETRY, False, None, policy.minimum_room_to_target_r, EconomicsReasonCode.INVALID_GEOMETRY)
    passed = room >= policy.minimum_room_to_target_r
    return _gate(AdmissionGate.REWARD_GEOMETRY, passed, room, policy.minimum_room_to_target_r, EconomicsReasonCode.INSUFFICIENT_TARGET_ROOM)


def cost_share_gate(ev_gross_r: float, cost_r: float, policy: AdmissionPolicy) -> Tuple[AdmissionGateResult, Optional[float]]:
    if ev_gross_r <= 0:
        return _gate(AdmissionGate.COST_SHARE, False, None, policy.maximum_cost_share, EconomicsReasonCode.COST_NOT_VIABLE), None
    cost_share = cost_r / ev_gross_r
    passed = cost_share <= policy.maximum_cost_share
    return _gate(AdmissionGate.COST_SHARE, passed, cost_share, policy.maximum_cost_share, EconomicsReasonCode.COST_SHARE_TOO_HIGH), cost_share


def distribution_shift_gate(forecast: OutcomeForecast, policy: AdmissionPolicy) -> AdmissionGateResult:
    ood_score = forecast.distribution_shift_assessment.ood_score if forecast.distribution_shift_assessment else 1.0
    passed = ood_score <= policy.maximum_ood_score
    return _gate(AdmissionGate.DISTRIBUTION_SHIFT, passed, ood_score, policy.maximum_ood_score, EconomicsReasonCode.DISTRIBUTION_SHIFT_TOO_HIGH)


def data_quality_gate(market_state: Any, policy: AdmissionPolicy) -> AdmissionGateResult:
    level = market_state.data_quality.level.value
    passed = level in policy.allowed_data_quality
    return _gate(AdmissionGate.DATA_QUALITY, passed, None, None, EconomicsReasonCode.DATA_QUALITY_UNACCEPTABLE)


def state_uncertainty_gate(market_state: Any, policy: AdmissionPolicy) -> AdmissionGateResult:
    value = market_state.state_uncertainty.value
    passed = value <= policy.maximum_state_uncertainty
    return _gate(AdmissionGate.STATE_UNCERTAINTY, passed, value, policy.maximum_state_uncertainty, EconomicsReasonCode.STATE_UNCERTAINTY_TOO_HIGH)


def forecast_uncertainty_gate(forecast: OutcomeForecast, policy: AdmissionPolicy) -> AdmissionGateResult:
    passed = forecast.forecast_uncertainty <= policy.maximum_forecast_uncertainty
    return _gate(AdmissionGate.FORECAST_UNCERTAINTY, passed, forecast.forecast_uncertainty, policy.maximum_forecast_uncertainty, EconomicsReasonCode.FORECAST_UNCERTAINTY_TOO_HIGH)


def cost_quality_gate(cost_estimate: CostEstimate) -> AdmissionGateResult:
    passed = EconomicsReasonCode.COST_NOT_VIABLE.value not in cost_estimate.reason_codes
    return _gate(AdmissionGate.COST_QUALITY, passed, None, None, EconomicsReasonCode.COST_UNBOUNDED)


def evaluate_all_gates(
    *,
    candidate: SetupCandidate,
    market_state: Any,
    forecast: OutcomeForecast,
    cost_estimate: CostEstimate,
    ev_gross_r: float,
    ev_net_r: float,
    conservative_edge_r: float,
    policy: AdmissionPolicy,
) -> Tuple[Tuple[AdmissionGateResult, ...], Optional[float]]:
    results: List[AdmissionGateResult] = [
        support_gate(forecast, policy),
        probability_uncertainty_gate(forecast, policy),
        net_edge_gate(ev_gross_r, ev_net_r, policy),
        conservative_edge_gate(conservative_edge_r, policy),
        reward_geometry_gate(candidate, policy),
        distribution_shift_gate(forecast, policy),
        data_quality_gate(market_state, policy),
        state_uncertainty_gate(market_state, policy),
        forecast_uncertainty_gate(forecast, policy),
        cost_quality_gate(cost_estimate),
    ]
    cost_share_result, cost_share_value = cost_share_gate(ev_gross_r, cost_estimate.total_cost_R, policy)
    results.append(cost_share_result)
    return tuple(results), cost_share_value


__all__ = [
    "support_gate",
    "probability_uncertainty_gate",
    "net_edge_gate",
    "conservative_edge_gate",
    "reward_geometry_gate",
    "cost_share_gate",
    "distribution_shift_gate",
    "data_quality_gate",
    "state_uncertainty_gate",
    "forecast_uncertainty_gate",
    "cost_quality_gate",
    "evaluate_all_gates",
]
