"""Deterministic cross-universe ranking (Section 15.6-15.13).

RankScore =
    w_edge * norm(ConservativeEdge_R) + w_tail * norm(lower NET quantile)
  + w_support * support_quality + w_liq * liquidity_quality
  - w_unc * uncertainty - w_ood * ood - w_exec * execution_uncertainty

Ranks on CONSERVATIVE ECONOMICS only -- never on Master Ensemble/legacy
confidence, raw setup evidence_score, or posterior win rate alone. Pure:
no clock, no I/O, no reservation, no order, no V2 import.
"""
from __future__ import annotations

from typing import Iterable, List, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.economics import AdmissionStatus
from app.trading_intelligence.contracts.ranking import EvaluatedOpportunity, RankedOpportunity, RankingPolicy


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def normalize(value: float, floor: float, cap: float) -> float:
    """Policy-defined, monotonic, bounded [0,1]."""
    if cap <= floor:
        return 0.0
    return _clamp01((value - floor) / (cap - floor))


def support_quality(ev: EvaluatedOpportunity, p: RankingPolicy) -> float:
    o = ev.opportunity
    width = ev.forecast.credible_interval_high - ev.forecast.credible_interval_low
    parts = (
        p.support_w_ess * _clamp01(o.ess / p.ess_cap),
        p.support_w_raw * _clamp01(o.raw_support / p.raw_support_cap),
        p.support_w_ci * (1.0 - _clamp01(width / p.ci_width_cap)),
        p.support_w_backoff * (1.0 - _clamp01(o.backoff_level / p.max_backoff_level)),
    )
    return _clamp01(sum(parts))


def liquidity_quality(ev: EvaluatedOpportunity, p: RankingPolicy) -> float:
    liq = ev.market_state.liquidity_state
    if not liq.available:
        return p.missing_liquidity_quality  # conservative, never a fabricated high score
    spread_q = 1.0 - _clamp01(liq.spread_percentile) if liq.spread_percentile is not None else p.missing_liquidity_quality
    slip_q = 1.0 - _clamp01(ev.cost_estimate.slippage_R / p.slippage_cap_r)
    q = p.liq_w_spread_percentile * spread_q + p.liq_w_slippage * slip_q
    if liq.stale_book:
        q *= p.stale_book_multiplier
    return _clamp01(q)


def uncertainty(ev: EvaluatedOpportunity, p: RankingPolicy) -> float:
    o = ev.opportunity
    width = o.credible_interval_high - o.credible_interval_low
    return _clamp01(p.unc_w_state * _clamp01(o.state_uncertainty) + p.unc_w_forecast * _clamp01(o.forecast_uncertainty)
                    + p.unc_w_ci * _clamp01(width / p.ci_width_cap))


def _lower_net_tail(ev: EvaluatedOpportunity) -> float:
    o = ev.opportunity
    if o.lower_net_quantile_r is not None:
        return o.lower_net_quantile_r
    # Fallback (older opportunities): derive once from the forecast, never subtract cost twice.
    lower = ev.forecast.gross_R_lower_quantile
    return (lower - o.cost_r) if lower is not None else -1.0 - o.cost_r


def score_components(ev: EvaluatedOpportunity, p: RankingPolicy) -> dict:
    o = ev.opportunity
    n_edge = normalize(o.conservative_edge_r, p.edge_floor_r, p.edge_cap_r)
    n_tail = normalize(_lower_net_tail(ev), p.tail_floor_r, p.tail_cap_r)
    supp = support_quality(ev, p)
    liq = liquidity_quality(ev, p)
    unc = uncertainty(ev, p)
    ood = _clamp01(o.ood_score)
    exe = _clamp01(ev.cost_estimate.cost_uncertainty_R / p.exec_uncertainty_cap_r)
    score = (p.w_edge * n_edge + p.w_tail * n_tail + p.w_support * supp + p.w_liq * liq
             - p.w_unc * unc - p.w_ood * ood - p.w_exec * exe)
    return dict(score=score, n_edge=n_edge, n_tail=n_tail, supp=supp, liq=liq, unc=unc, ood=ood, exe=exe)


def eligible_for_ranking(ev: EvaluatedOpportunity) -> bool:
    """Only APPROVE_FOR_RANKING of an ECONOMICALLY_ADMISSIBLE opportunity."""
    return ev.veto.approved_for_ranking and ev.opportunity.admission_status == AdmissionStatus.ECONOMICALLY_ADMISSIBLE.value


def _sort_key(item: Tuple[EvaluatedOpportunity, dict]):
    ev, c = item
    return (-round(c["score"], 12), -round(ev.opportunity.conservative_edge_r, 12), -round(ev.opportunity.ess, 12),
            round(ev.opportunity.cost_r, 12), ev.candidate.setup_candidate_id)


def rank_opportunities(
    evaluated: Iterable[EvaluatedOpportunity], policy: Optional[RankingPolicy] = None, *,
    bot_instance_id: Optional[str] = None, broker_account_id: Optional[str] = None, cycle_id: Optional[str] = None,
) -> Tuple[Tuple[RankedOpportunity, ...], Tuple[str, ...]]:
    """Returns (ranked, excluded_economic_opportunity_ids). WATCH/REJECT (or
    anything not admissible) is excluded and can never appear in the output."""
    p = policy or RankingPolicy()
    included: List[Tuple[EvaluatedOpportunity, dict]] = []
    excluded: List[str] = []
    for ev in evaluated:
        if eligible_for_ranking(ev):
            included.append((ev, score_components(ev, p)))
        else:
            excluded.append(ev.opportunity.economic_opportunity_id)
    included.sort(key=_sort_key)  # total order; never relies on input or dict order
    ranked = tuple(
        RankedOpportunity(
            ranked_opportunity_id=RankedOpportunity.build_id(
                economic_opportunity_id=ev.opportunity.economic_opportunity_id,
                veto_decision_id=ev.veto.veto_decision_id, ranking_policy_hash=p.policy_hash),
            economic_opportunity_id=ev.opportunity.economic_opportunity_id, veto_decision_id=ev.veto.veto_decision_id,
            setup_candidate_id=ev.candidate.setup_candidate_id, bot_instance_id=bot_instance_id,
            broker_account_id=broker_account_id, cycle_id=cycle_id, instrument_key=ev.candidate.instrument_key,
            side=ev.candidate.side, setup_family=ev.candidate.setup_family, rank_score=c["score"],
            normalized_conservative_edge=c["n_edge"], normalized_lower_tail=c["n_tail"], support_quality=c["supp"],
            liquidity_quality=c["liq"], uncertainty_penalty_component=p.w_unc * c["unc"],
            ood_penalty_component=p.w_ood * c["ood"], execution_uncertainty_component=p.w_exec * c["exe"],
            rank_position=i + 1, ranking_policy_version=p.schema_version, ranking_policy_hash=p.policy_hash,
            reason_codes=(("MISSING_LIQUIDITY_CONSERVATIVE",) if not ev.market_state.liquidity_state.available else ()),
        )
        for i, (ev, c) in enumerate(included)
    )
    return ranked, tuple(sorted(excluded))


__all__ = ["normalize", "support_quality", "liquidity_quality", "uncertainty", "score_components",
           "eligible_for_ranking", "rank_opportunities"]
