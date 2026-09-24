"""Conditioned-analog PositionForecast (Sections 19.3-19.6, 19.9) -- V1,
interpretable, no ML.

The entry-time OutcomeForecast is NOT assumed valid forever. From NOW the
engine re-asks the HistoricalOutcomeLibrary a conditioned question:

    "Of historical analogs of this setup family/side whose entry state looked
     like the CURRENT MarketState/regime, that were still alive after the
     bars this position has already lived, and whose excursions reached at
     least this position's current MFE / MAE buckets -- how did they end,
     measured from where this position is NOW, under its CURRENT stop?"

Path conditioning (always applied):
    * survival      terminal event index >= elapsed_bars (not already over)
    * MFE bucket    analog mfe_R >= lower edge of the current MFE bucket
    * MAE bucket    analog mae_R >= lower edge of the current MAE bucket
    * resolved      VALID rows, or CENSORED rows whose target/stop was
                    observed (a censored TIMEOUT has no known ending)
State conditioning uses the canonical cohort dimensions derived from the
CURRENT state, with the canonical hierarchical backoff (forecast/cohorts.py).
Insufficient conditioned support -> POSITION_FORECAST_SUPPORT_INSUFFICIENT;
precise probabilities are never fabricated.

Current geometry: an analog that stopped out ends at the CURRENT stop; a
timeout ending beyond the current stop, or an analog whose adverse excursion
crossed a tightened stop, is also a stop (conservative, same-bar-adverse
spirit of Section 12.2.3).

Remaining EV = mean over analogs of (final_R - current_R): expected future
PnL from NOW, in ORIGINAL-R units. Only FUTURE costs are subtracted.
"""
from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.forecast import LabelQuality, TerminalOutcome
from app.trading_intelligence.contracts.position import (
    REMAINING_R_BASIS, ExitPolicy, PositionForecast, PositionForecastStatus as FS, PositionPathSnapshot,
    PositionReasonCode as PR, ThesisConditionResult,
)
from app.trading_intelligence.contracts.trade_plan import TradePlan
from app.trading_intelligence.economics.policy import (
    distribution_shift_penalty_r, execution_uncertainty_penalty_r, uncertainty_penalty_r,
)
from app.trading_intelligence.forecast.cohorts import BACKOFF_LEVELS, cohort_signature, derive_cohort_dimensions
from app.trading_intelligence.forecast.distributions import quantile_map
from app.trading_intelligence.forecast.ood import assess_distribution_shift
from app.trading_intelligence.forecast.posterior import (
    beta_binomial_posterior, dirichlet_multinomial_posterior, effective_sample_size, weighted_mean,
)
from app.trading_intelligence.forecast.uncertainty import compute_forecast_uncertainty
from app.trading_intelligence.position.path import bar_ms
from app.trading_intelligence.position.thesis import aggregate_thesis_status, economic_condition
from app.trading_intelligence.venue.cost_model import build_remaining_cost_estimate
from app.trading_intelligence.versions import POSITION_FORECAST_ENGINE_VERSION

TGT, STP, TMO = (TerminalOutcome.TARGET_BEFORE_STOP.value, TerminalOutcome.STOP_BEFORE_TARGET.value,
                 TerminalOutcome.TIMEOUT.value)
_CATEGORICAL = ("dominant_regime", "volatility_bucket", "liquidity_bucket", "trend_maturity", "htf_alignment",
                "funding_crowding_bucket", "instrument_group")
_Q = (0.1, 0.5, 0.9)


def bucket_floor(value: float, edges: Sequence[float]) -> float:
    """Lower edge of the bucket ``value`` falls in (0.0 below the first edge)."""
    floor = 0.0
    for e in sorted(edges):
        if value >= e:
            floor = e
    return floor


def _terminal_index(label) -> int:
    if label.terminal_outcome == TGT and label.time_to_target_bars is not None:
        return int(label.time_to_target_bars)
    if label.terminal_outcome == STP and label.time_to_stop_bars is not None:
        return int(label.time_to_stop_bars)
    return int(label.terminal_horizon_bars)


def path_filters(path: PositionPathSnapshot, policy: ExitPolicy):
    mfe_floor = bucket_floor(path.mfe_R, policy.mfe_bucket_edges_R)
    mae_floor = bucket_floor(path.mae_R, policy.mae_bucket_edges_R)
    k = int(path.elapsed_bars)
    names = (f"survived_bars>={k}", f"mfe_R>={mfe_floor:g}", f"mae_R>={mae_floor:g}", "outcome_resolved")

    def ok(row) -> bool:
        lb = row.label
        # A censored row is still RESOLVED when its target/stop was observed; only a
        # censored TIMEOUT (horizon not fully seen) has an unknown ending. INVALID never votes.
        resolved = lb.label_quality == LabelQuality.VALID.value or (
            lb.label_quality == LabelQuality.CENSORED.value and lb.terminal_outcome in (TGT, STP))
        return resolved and _terminal_index(lb) >= k and lb.mfe_R >= mfe_floor and lb.mae_R >= mae_floor
    return ok, names


def current_geometry_outcome(label, *, stop_R_now: float, stop_adverse_R: float) -> Tuple[str, float]:
    """(outcome, final_R from entry) of one analog under the CURRENT stop."""
    if label.terminal_outcome == STP:
        return STP, stop_R_now
    crossed = stop_adverse_R > 0 and label.mae_R >= stop_adverse_R
    if label.terminal_outcome == TGT:
        return (STP, stop_R_now) if crossed and stop_adverse_R < 1.0 else (TGT, float(label.gross_R))
    final = float(label.gross_R)
    if final <= stop_R_now or crossed:
        return STP, stop_R_now
    return TMO, final


def _with_economic(results, economic) -> tuple:
    """The plan's ECONOMIC_EDGE_POSITIVE condition, re-evaluated from NOW."""
    return tuple(r for r in results if r.code != economic.code) + (economic,)


def _empty(status: str, *, plan, path, market_state, regime, thesis_results, thesis_status, policy, reasons,
           evaluation_mode, library_hash="none", calibration="UNKNOWN", remaining_costs=None,
           support=0, ess=0.0, level=len(BACKOFF_LEVELS) - 1, signature="none", conditioning=()) -> PositionForecast:
    return PositionForecast.build(
        position_id=path.position_id, trade_plan_id=plan.trade_plan_id,
        market_state_id=getattr(market_state, "market_state_id", None),
        regime_distribution_id=getattr(regime, "regime_distribution_id", None), position_path_id=path.position_path_id,
        user_id=path.user_id, broker_account_id=path.broker_account_id, bot_instance_id=path.bot_instance_id,
        forecast_time=path.current_time, status=status, thesis_status=thesis_status,
        remaining_horizon_bars=0, remaining_horizon_ms=0,
        p_positive_from_now=0.0, p_target_from_now=0.0, p_stop_from_now=0.0, p_timeout_from_now=0.0,
        remaining_gross_EV_R=0.0, remaining_cost_R=0.0, remaining_net_EV_R=0.0, remaining_uncertainty_penalty_R=0.0,
        remaining_ood_penalty_R=0.0, remaining_execution_uncertainty_R=0.0, conservative_remaining_edge_R=0.0,
        remaining_MFE_R_quantiles={}, remaining_MAE_R_quantiles={}, time_to_target_remaining_quantiles={},
        time_to_stop_remaining_quantiles={}, updated_support=support, updated_ESS=ess, updated_backoff_level=level,
        cohort_signature=signature, path_conditioning=tuple(conditioning),
        current_state_uncertainty=float(getattr(getattr(market_state, "state_uncertainty", None), "value", 1.0)),
        current_forecast_uncertainty=1.0, current_OOD_score=1.0,
        thesis_condition_results=_with_economic(thesis_results, economic_condition(None, policy)),
        remaining_costs=remaining_costs,
        costs_already_realized_R=remaining_costs.costs_already_realized_R if remaining_costs else 0.0,
        original_R_reference=path.original_R_reference, remaining_risk_distance=path.remaining_risk_distance,
        remaining_net_EV_per_remaining_risk=None, remaining_R_basis=REMAINING_R_BASIS,
        library_hash=library_hash, calibration_status=calibration, reason_codes=tuple(dict.fromkeys(reasons)),
        forecast_policy_version=policy.schema_version, forecast_policy_hash=policy.policy_hash,
        engine_version=POSITION_FORECAST_ENGINE_VERSION, evaluation_mode=evaluation_mode,
    )


def build_position_forecast(
    *,
    plan: TradePlan,
    path: PositionPathSnapshot,
    market_state: Any,
    regime: Any,
    library: Any,
    venue_observation: Any,
    thesis_results: Sequence[ThesisConditionResult],
    policy: Optional[ExitPolicy] = None,
    venue_policy: Any = None,
    instrument_group: Optional[str] = None,
    evaluation_mode: str = "SHADOW",
) -> PositionForecast:
    policy = policy or ExitPolicy()
    thesis_status = aggregate_thesis_status(thesis_results, policy)
    common = dict(plan=plan, path=path, market_state=market_state, regime=regime, thesis_results=thesis_results,
                  thesis_status=thesis_status, policy=policy, evaluation_mode=evaluation_mode)
    if library is None:
        return _empty(FS.OUTCOME_LIBRARY_UNAVAILABLE.value, reasons=(PR.OUTCOME_LIBRARY_UNAVAILABLE.value,), **common)
    lib_meta = dict(library_hash=library.library_hash, calibration=library.calibration_status)
    if market_state is None or not market_state.is_usable or regime is None:
        return _empty(FS.INVALID_INPUT.value, reasons=(PR.MARKET_STATE_INVALID.value,), **common, **lib_meta)

    # -- conditioned analog selection ---------------------------------------------------------
    ok, conditioning = path_filters(path, policy)
    # family AND side are never backed off: an opposite-side analog says nothing about this position
    survivors = [r for r in library.rows if r.label.setup_family == plan.setup_family
                 and r.cohort_dimensions.get("side") == plan.side and ok(r)]
    dims = derive_cohort_dimensions(setup_family=plan.setup_family, side=plan.side, market_state=market_state,
                                    regime_distribution=regime, instrument_group=instrument_group)
    matched: List[Any] = []
    level, level_dims = len(BACKOFF_LEVELS) - 1, BACKOFF_LEVELS[-1]
    for lvl, dims_at in enumerate(BACKOFF_LEVELS):
        sig = cohort_signature(dims, dims_at)
        rows = [r for r in survivors if cohort_signature(r.cohort_dimensions, dims_at) == sig]
        if len(rows) >= policy.minimum_support:
            matched, level, level_dims = rows, lvl, dims_at
            break
        if rows:
            matched, level, level_dims = rows, lvl, dims_at
    signature = cohort_signature(dims, level_dims)
    reasons = [PR.PATH_CONDITIONED.value] + ([PR.HIERARCHICAL_BACKOFF.value] if level > 0 else [])
    n = len(matched)
    weights = [1.0] * n
    ess = effective_sample_size(weights)
    if n < policy.minimum_support or ess < policy.minimum_ESS:
        reasons.append(PR.POSITION_FORECAST_SUPPORT_INSUFFICIENT.value)
        if ess < policy.minimum_ESS:
            reasons.append(PR.ESS_TOO_LOW.value)
        return _empty(FS.POSITION_FORECAST_SUPPORT_INSUFFICIENT.value, reasons=reasons, support=n, ess=ess, level=level,
                      signature=signature, conditioning=conditioning, **common, **lib_meta)

    # -- per-analog outcome from NOW under the CURRENT stop -----------------------------------------
    sign = 1.0 if plan.side == "LONG" else -1.0
    R0, entry = path.original_R_reference, path.entry_price
    stop_px = path.current_stop_price if path.current_stop_price is not None else plan.structural_invalidation_price
    stop_R_now = sign * (float(stop_px) - entry) / R0
    stop_adverse = -stop_R_now
    k = int(path.elapsed_bars)
    outcomes, finals, rem_bars, tt_t, tt_s, rem_mfe, rem_mae = [], [], [], [], [], [], []
    for r in matched:
        oc, final = current_geometry_outcome(r.label, stop_R_now=stop_R_now, stop_adverse_R=stop_adverse)
        outcomes.append(oc)
        finals.append(final)
        remaining = max(1, _terminal_index(r.label) - k + (1 if r.label.terminal_outcome in (TGT, STP) else 0))
        rem_bars.append(float(remaining))
        (tt_t if oc == TGT else tt_s if oc == STP else []).append(float(remaining))
        rem_mfe.append(max(0.0, r.label.mfe_R - path.current_R))
        rem_mae.append(max(0.0, min(path.current_R - stop_R_now, path.current_R + r.label.mae_R)))
    remaining_gross = [f - path.current_R for f in finals]

    bar = bar_ms(path.timeframe)
    horizon_bars = int(round(sorted(rem_bars)[len(rem_bars) // 2]))
    max_bars = int(max(rem_bars))
    rc = build_remaining_cost_estimate(path=path, observation=venue_observation,
                                       expected_remaining_holding_ms=horizon_bars * bar,
                                       max_remaining_holding_ms=max_bars * bar, policy=venue_policy)
    cost_R = rc.expected_future_holding_and_exit_costs_R

    counts = {c: sum(1 for o in outcomes if o == c) / n for c in (TGT, STP, TMO)}
    three = dirichlet_multinomial_posterior(weighted_counts=counts, ess=ess,
                                            prior={c: policy.prior_strength / 3.0 for c in (TGT, STP, TMO)})
    positive = [1.0 if g - cost_R > 0 else 0.0 for g in remaining_gross]
    beta = beta_binomial_posterior(weighted_win_rate=weighted_mean(positive, weights), ess=ess,
                                   prior_alpha=policy.prior_strength / 2.0, prior_beta=policy.prior_strength / 2.0,
                                   credible_interval_level=policy.credible_interval_level)
    gross_ev = float(weighted_mean(remaining_gross, weights) or 0.0)
    net_ev = gross_ev - cost_R

    reference_categories = {d: tuple(sorted({r.cohort_dimensions.get(d, "UNKNOWN") for r in library.rows}))
                            for d in _CATEGORICAL}
    shift = assess_distribution_shift(
        continuous_features={}, reference_distributions={},
        candidate_categories={k2: v for k2, v in dims.items() if k2 in _CATEGORICAL},
        reference_categories=reference_categories, required_capabilities_seen_in_reference=(),
        capabilities_available_now=(), raw_support=n, ess=ess, backoff_level=level)
    ci_width = beta.credible_interval_high - beta.credible_interval_low
    f_unc, _components = compute_forecast_uncertainty(credible_interval_width=ci_width, ess=ess, backoff_level=level,
                                                      ood_score=shift.ood_score, missing_capability_count=0)
    u_pen = uncertainty_penalty_r(forecast_uncertainty=f_unc, credible_interval_width=ci_width,
                                  coefficient=policy.uncertainty_penalty_coefficient)
    d_pen = distribution_shift_penalty_r(ood_score=shift.ood_score, severity=shift.severity,
                                         coefficient=policy.distribution_shift_penalty_coefficient)
    liq_q = 1.0 if market_state.liquidity_state.available else 0.0
    x_pen = execution_uncertainty_penalty_r(cost_uncertainty_r=rc.remaining_cost_uncertainty_R, liquidity_quality=liq_q,
                                            coefficient=policy.execution_uncertainty_penalty_coefficient)
    conservative = net_ev - u_pen - d_pen - x_pen

    status = FS.VALID.value
    if not rc.usable:
        status = FS.REMAINING_COST_UNAVAILABLE.value
        reasons.append(PR.REMAINING_COST_UNAVAILABLE.value)
    reasons.extend(shift.reason_codes)
    rrd = path.remaining_risk_distance
    per_remaining = (net_ev * R0 / rrd) if rrd else None
    return PositionForecast.build(
        position_id=path.position_id, trade_plan_id=plan.trade_plan_id, market_state_id=market_state.market_state_id,
        regime_distribution_id=regime.regime_distribution_id, position_path_id=path.position_path_id,
        user_id=path.user_id, broker_account_id=path.broker_account_id, bot_instance_id=path.bot_instance_id,
        forecast_time=path.current_time, status=status, thesis_status=thesis_status,
        remaining_horizon_bars=horizon_bars, remaining_horizon_ms=horizon_bars * bar,
        p_positive_from_now=beta.p_mean, p_target_from_now=three[TGT], p_stop_from_now=three[STP],
        p_timeout_from_now=three[TMO], remaining_gross_EV_R=gross_ev, remaining_cost_R=cost_R, remaining_net_EV_R=net_ev,
        remaining_uncertainty_penalty_R=u_pen, remaining_ood_penalty_R=d_pen, remaining_execution_uncertainty_R=x_pen,
        conservative_remaining_edge_R=conservative if status == FS.VALID.value else 0.0,
        remaining_MFE_R_quantiles=quantile_map(rem_mfe, weights, _Q),
        remaining_MAE_R_quantiles=quantile_map(rem_mae, weights, _Q),
        time_to_target_remaining_quantiles=quantile_map(tt_t, [1.0] * len(tt_t), _Q),
        time_to_stop_remaining_quantiles=quantile_map(tt_s, [1.0] * len(tt_s), _Q),
        updated_support=n, updated_ESS=ess, updated_backoff_level=level, cohort_signature=signature,
        path_conditioning=tuple(conditioning), current_state_uncertainty=float(market_state.state_uncertainty.value),
        current_forecast_uncertainty=f_unc, current_OOD_score=shift.ood_score,
        thesis_condition_results=_with_economic(
            thesis_results, economic_condition(conservative if status == FS.VALID.value else None, policy)),
        remaining_costs=rc, costs_already_realized_R=rc.costs_already_realized_R,
        original_R_reference=R0, remaining_risk_distance=rrd, remaining_net_EV_per_remaining_risk=per_remaining,
        remaining_R_basis=REMAINING_R_BASIS, library_hash=library.library_hash,
        calibration_status=library.calibration_status, reason_codes=tuple(dict.fromkeys(reasons)),
        forecast_policy_version=policy.schema_version, forecast_policy_hash=policy.policy_hash,
        engine_version=POSITION_FORECAST_ENGINE_VERSION, evaluation_mode=evaluation_mode,
    )


__all__ = ["bucket_floor", "path_filters", "current_geometry_outcome", "build_position_forecast"]
