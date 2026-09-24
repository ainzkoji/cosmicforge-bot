"""Metric emitters per CATI stage family (Sections 21.4-21.14).

Each emitter reads an already-built CATI object and records BOUNDED metrics
only (asset_class / venue / setup_family / side / regime / outcome /
reason_family / bucket). Ids stay in evidence rows and structured logs.
Every emitter is exception-proof: observability never changes a decision.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional

from app.trading_intelligence.observability.metrics import METRICS, reason_family


def _safe(fn):
    def wrapper(*a, **k):
        try:
            return fn(*a, **k)
        except Exception:
            return None
    wrapper.__name__ = fn.__name__
    return wrapper


def _prob_bucket(p: float) -> str:
    return f"p{min(9, max(0, int(float(p) * 10)))}"


@_safe
def observe_market_data(market_state: Any, *, now_ms: Optional[int] = None, cache_hit: Optional[bool] = None) -> None:
    """21.4 snapshot freshness, missing capability, stale snapshot, cache, data-quality faults."""
    ac = market_state.instrument_key.asset_class
    if now_ms is not None:
        METRICS.observe("cati_snapshot_freshness_ms", max(0, now_ms - market_state.latest_closed_candle_time),
                        asset_class=ac)
    states = dict(getattr(market_state.feature_availability, "states", {}) or {})
    missing = [k for k, v in states.items() if v != "AVAILABLE"]
    METRICS.inc("cati_snapshots_total", asset_class=ac)
    if missing:
        METRICS.inc("cati_missing_capability_total", len(missing), asset_class=ac)
    level = str(getattr(market_state.data_quality.level, "value", market_state.data_quality.level))
    if level != "VALID":
        METRICS.inc("cati_data_quality_fault_total", asset_class=ac, status=level)
    if any("STALE" in str(c) for c in market_state.reason_codes):
        METRICS.inc("cati_stale_snapshot_total", asset_class=ac)
    if cache_hit is not None:
        METRICS.inc("cati_shared_cache_hits_total" if cache_hit else "cati_shared_cache_misses_total", asset_class=ac)


@_safe
def observe_market_state(market_state: Any, regime: Any) -> None:
    """21.5 uncertainty distribution, shock-state rate, transition-unknown rate."""
    ac = market_state.instrument_key.asset_class
    METRICS.observe("cati_state_uncertainty", market_state.state_uncertainty.value, edges=(0.1, 0.25, 0.5, 0.75, 1.0),
                    asset_class=ac)
    if market_state.volatility_state.available and market_state.volatility_state.shock_state:
        METRICS.inc("cati_shock_state_total", asset_class=ac)
    if regime is not None:
        METRICS.inc("cati_regime_total", regime=regime.dominant_regime, asset_class=ac)
        if regime.dominant_regime == "TRANSITION_UNKNOWN":
            METRICS.inc("cati_transition_unknown_total", asset_class=ac)


@_safe
def observe_symbol_evaluation(evaluation: Any) -> None:
    """21.6-21.9 per evaluated symbol: setups, forecasts, economics, veto."""
    METRICS.inc("cati_symbol_evaluations_total", status=evaluation.kind)
    seen = set()
    for ev in evaluation.opportunities:
        c, f, cost, o, v = ev.candidate, ev.forecast, ev.cost_estimate, ev.opportunity, ev.veto
        ac, fam, side = c.instrument_key.asset_class, c.setup_family, c.side
        regime = getattr(ev.regime, "dominant_regime", "UNKNOWN")
        venue = c.instrument_key.venue
        # setup
        METRICS.inc("cati_setup_candidates_total", setup_family=fam, side=side, asset_class=ac)
        key = (fam, side)
        if key in seen:
            METRICS.inc("cati_setup_duplicate_hypothesis_total", setup_family=fam)
        seen.add(key)
        # forecast (calibration-curve material: bucketed probability now, realized outcome joined later)
        METRICS.observe("cati_forecast_raw_support", f.raw_support, setup_family=fam)
        METRICS.observe("cati_forecast_ess", f.ess, setup_family=fam)
        METRICS.inc("cati_forecast_backoff_total", bucket=f"level{int(f.backoff_level)}", setup_family=fam)
        METRICS.observe("cati_forecast_interval_width", f.credible_interval_high - f.credible_interval_low,
                        edges=(0.05, 0.1, 0.2, 0.4, 1.0), setup_family=fam)
        METRICS.observe("cati_forecast_uncertainty", f.forecast_uncertainty, edges=(0.1, 0.25, 0.5, 0.75, 1.0),
                        setup_family=fam)
        ood = f.distribution_shift_assessment.ood_score if f.distribution_shift_assessment else 1.0
        METRICS.observe("cati_forecast_ood", ood, edges=(0.1, 0.25, 0.5, 0.75, 1.0), setup_family=fam)
        METRICS.inc("cati_forecast_probability_bucket_total", bucket=_prob_bucket(f.p_net_profitable_mean),
                    setup_family=fam, status=f.status)
        # economics
        for name, val in (("cati_ev_gross_r", o.ev_gross_r), ("cati_ev_net_r", o.ev_net_r),
                          ("cati_conservative_edge_r", o.conservative_edge_r), ("cati_cost_fee_r", cost.fee_R),
                          ("cati_cost_spread_r", cost.spread_R), ("cati_cost_slippage_r", cost.slippage_R),
                          ("cati_cost_funding_carry_r", cost.funding_R + cost.carry_R),
                          ("cati_cost_uncertainty_r", cost.cost_uncertainty_R), ("cati_cost_share", o.cost_share)):
            METRICS.observe(name, val, edges=(-1.0, -0.25, 0.0, 0.1, 0.25, 0.5, 1.0, 2.0), venue=venue, asset_class=ac)
        # veto (21.9) -- counts by bounded dimensions; abstention = WATCH
        METRICS.inc("cati_veto_outcomes_total", outcome=v.outcome, setup_family=fam, venue=venue, asset_class=ac,
                    regime=regime)
        for code in v.reason_codes[:5]:
            METRICS.inc("cati_veto_reasons_total", reason_family=reason_family(code), outcome=v.outcome)
            if v.outcome != "APPROVE_FOR_RANKING":
                METRICS.inc("cati_setup_rejection_reasons_total", reason_family=reason_family(code), setup_family=fam)


@_safe
def observe_ranking(result: Any) -> None:
    """21.10 approved count per batch, top / second score, spread, not-top-ranked."""
    b = result.batch
    METRICS.observe("cati_ranking_approved_per_batch", len(b.approved_opportunity_ids), edges=(0, 1, 2, 5, 10, 50))
    METRICS.inc("cati_ranking_batches_total", status="COMPLETE" if b.batch_complete else "INCOMPLETE")
    scores = sorted((r.rank_score for r in result.ranked), reverse=True)
    if scores:
        METRICS.observe("cati_ranking_top_score", scores[0])
    if len(scores) > 1:
        METRICS.observe("cati_ranking_second_score", scores[1])
        METRICS.observe("cati_ranking_top_second_spread", scores[0] - scores[1])
        METRICS.inc("cati_ranking_not_top_ranked_total", len(scores) - 1)
    for r in result.ranked:
        METRICS.inc("cati_ranking_positions_total", bucket=f"rank{min(r.rank_position, 10)}", setup_family=r.setup_family)


@_safe
def observe_portfolio(decision: Any) -> None:
    """21.11 selected / not selected, penalties, duplicate exposure, conflicts, expiry."""
    METRICS.inc("cati_portfolio_selected_total", len(decision.selected_opportunity_ids))
    METRICS.inc("cati_portfolio_not_selected_total", len(decision.rejected_candidates))
    sb = decision.score_breakdown
    for name in ("correlation_penalty", "factor_penalty", "sector_penalty", "liquidity_penalty"):
        METRICS.observe(f"cati_portfolio_{name}", getattr(sb, name, None), edges=(0.0, 0.1, 0.25, 0.5, 1.0, 2.0))
    for rc in decision.rejected_candidates:
        METRICS.inc("cati_portfolio_rejections_total", reason_family=reason_family(rc.reason_code))
    status = str(decision.reservation_status)
    METRICS.inc("cati_portfolio_reservations_total", status=status if len(status) <= 24 else "OTHER")
    for code in decision.reason_codes:
        if "DUPLICATE_EXPOSURE" in code:
            METRICS.inc("cati_portfolio_duplicate_exposure_total")
        if "CONFLICT" in code:
            METRICS.inc("cati_portfolio_reservation_conflict_total")
        if "EXPIRED" in code:
            METRICS.inc("cati_portfolio_reservation_expiry_total")


def required_metric_families() -> Iterable[str]:
    """One representative metric per required family (tested for emission)."""
    return ("cati_snapshots_total", "cati_state_uncertainty", "cati_setup_candidates_total", "cati_forecast_ess",
            "cati_ev_net_r", "cati_veto_outcomes_total", "cati_ranking_approved_per_batch",
            "cati_portfolio_selected_total", "cati_hard_risk_rejections_total", "cati_execution_attempts_total",
            "cati_exit_decisions_total", "cati_stage_latency_ms")


__all__ = ["observe_market_data", "observe_market_state", "observe_symbol_evaluation", "observe_ranking",
           "observe_portfolio", "required_metric_families"]
