"""The hard boundaries every CATI ML estimate is clamped to (Section 23.4-23.9).

These functions are the ONLY way a (promoted) estimate reaches a decision.
Each keeps every deterministic authority above the model:

* REGIME   -- unusable market data => UNAVAILABLE, whatever the model says
* OUTCOME  -- the estimate re-enters canonical Economic Admission AND Veto
* SLIPPAGE -- can only RAISE the deterministic cost; it never approves
* RANKING  -- reorders only veto-approved opportunities; adds nothing
* EXIT     -- can only TIGHTEN a stop, never widen risk
* OOD      -- can only INCREASE uncertainty; unavailable => abstain (1.0)
"""
from __future__ import annotations

import dataclasses
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


def regime_estimate(market_state: Any, deterministic: Any, ml_distribution: Optional[Mapping[str, float]], *,
                    ml_ood_score: Optional[float] = None, max_ood: float = 0.75) -> Dict[str, Any]:
    if market_state is None or not getattr(market_state, "is_usable", False):
        return {"source": "UNAVAILABLE", "distribution": None, "reason": "DATA_QUALITY_GATE"}
    if not ml_distribution or (ml_ood_score is not None and ml_ood_score > max_ood):
        return {"source": "DETERMINISTIC", "distribution": deterministic,
                "reason": "ML_OOD_OR_UNAVAILABLE" if ml_distribution else "ML_UNAVAILABLE"}
    total = sum(max(0.0, float(v)) for v in ml_distribution.values())
    if total <= 0:
        return {"source": "DETERMINISTIC", "distribution": deterministic, "reason": "ML_DEGENERATE"}
    dist = {k: max(0.0, float(v)) / total for k, v in ml_distribution.items()}
    entropy = -sum(p * math.log(p) for p in dist.values() if p > 0) / math.log(max(2, len(dist)))
    return {"source": "ML", "distribution": dist, "uncertainty": entropy}


def outcome_through_admission(evaluated: Any, ml_p_net_profitable: float, *, admission_policy: Any = None,
                              veto_policy: Any = None) -> Tuple[Any, Any]:
    """Replace ONLY the probability estimate, then run the canonical admission
    and veto on it. A model can never approve what those gates reject."""
    from app.trading_intelligence.contracts.veto import VetoPolicy
    from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
    from app.trading_intelligence.economics.policy import default_admission_policy
    from app.trading_intelligence.veto.engine import evaluate_veto

    p = min(1.0, max(0.0, float(ml_p_net_profitable)))
    f = evaluated.forecast
    half = (f.credible_interval_high - f.credible_interval_low) / 2.0
    forecast = dataclasses.replace(f, p_net_profitable_mean=p, credible_interval_low=max(0.0, p - half),
                                   credible_interval_high=min(1.0, p + half))
    policy = admission_policy or default_admission_policy()
    opp = evaluate_economic_opportunity(evaluated.candidate, evaluated.market_state, forecast,
                                        evaluated.cost_estimate, policy=policy)
    veto = evaluate_veto(opportunity=opp, candidate=evaluated.candidate, market_state=evaluated.market_state,
                         regime_distribution=evaluated.regime, forecast=forecast,
                         cost_estimate=evaluated.cost_estimate, policy=veto_policy or VetoPolicy())
    return opp, veto


def slippage_cost_bps(deterministic_bps: float, ml_bps: Optional[float]) -> float:
    """Conservative: the model may add cost, never remove it. The executor
    still validates and executes; this is only a number."""
    if ml_bps is None or not math.isfinite(float(ml_bps)):
        return float(deterministic_bps)
    return max(float(deterministic_bps), float(ml_bps))


def rerank_approved(approved_in_order: Sequence[str], ml_scores: Mapping[str, float]) -> List[str]:
    """Stable reorder of the ALREADY-approved set only; unscored items keep
    their deterministic order after scored ones. Nothing is added."""
    base = {rid: i for i, rid in enumerate(approved_in_order)}
    return sorted(approved_in_order, key=lambda rid: (-(ml_scores.get(rid, float("-inf"))), base[rid]))


def exit_stop(side: str, deterministic_stop: float, ml_stop: Optional[float]) -> float:
    """Tighten only: LONG stop can rise, SHORT stop can fall -- never widen."""
    if ml_stop is None:
        return float(deterministic_stop)
    if str(side).upper() in ("LONG", "BUY"):
        return max(float(deterministic_stop), float(ml_stop))
    return min(float(deterministic_stop), float(ml_stop))


def combine_ood(deterministic: Optional[float], ml: Optional[float]) -> float:
    """OOD only ever increases; an unavailable deterministic score abstains."""
    if deterministic is None:
        return 1.0
    return max(float(deterministic), float(ml)) if ml is not None else float(deterministic)


__all__ = ["regime_estimate", "outcome_through_admission", "slippage_cost_bps", "rerank_approved", "exit_stop",
           "combine_ood"]
