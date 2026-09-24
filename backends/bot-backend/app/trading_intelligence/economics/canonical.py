"""THE canonical CATI economics path (pre-Section-22 closure).

    Section 17 VenueEconomicObservation (canonical broker-account identity)
    -> venue/account CostEstimate          (venue/cost_model.build_venue_cost_estimate)
    -> Section 13 EconomicOpportunity      (economics/engine.evaluate_economic_opportunity)

Every certifiable caller -- the whole-universe cycle shadow (through
``CATIController.evaluate_symbol`` with a venue context) and any replay /
certification harness replaying recorded venue observations -- calls THIS
function, so there is exactly one cost contract and one admission authority.

The Section 13 *reference* cost model (``economics/costs.build_cost_estimate``,
``CostScope.REFERENCE_RESEARCH``) remains available only for the explicitly
diagnostic per-symbol hook and for historical research labeling. It is never
certification-equivalent: ``economics_basis`` reports REFERENCE_DIAGNOSTIC for
it, and the Section 18 TradePlan builder refuses any opportunity without a
matching VenueEconomicObservation, so reference economics can never become a
plan or an order.
"""
from __future__ import annotations

from typing import Any, Optional, Tuple

from app.trading_intelligence.contracts.economics import CostScope
from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
from app.trading_intelligence.venue.cost_model import build_venue_cost_estimate

CANONICAL_VENUE = "CANONICAL_VENUE"
REFERENCE_DIAGNOSTIC = "REFERENCE_DIAGNOSTIC"
CANONICAL_COST_SCOPES = frozenset({CostScope.VENUE.value, CostScope.ACCOUNT.value})


def canonical_economics(candidate: Any, market_state: Any, forecast: Any, observation: Any, *,
                        venue_policy: Any = None, reference_notional: Optional[float] = None,
                        admission_policy: Any = None, user_id: Optional[str] = None,
                        broker_account_id: Optional[str] = None, bot_instance_id: Optional[str] = None,
                        run_id: Optional[str] = None, cycle_id: Optional[str] = None) -> Tuple[Any, Any]:
    """(CostEstimate, EconomicOpportunity) from ONE Section 17 observation.
    Costs are subtracted exactly once, inside the Section 13 engine."""
    if observation is None:
        raise ValueError("canonical economics requires a Section 17 VenueEconomicObservation")
    cost = build_venue_cost_estimate(candidate, observation, forecast=forecast, policy=venue_policy,
                                     reference_notional=reference_notional)
    opportunity = evaluate_economic_opportunity(
        candidate, market_state, forecast, cost, policy=admission_policy, user_id=user_id,
        broker_account_id=broker_account_id, bot_instance_id=bot_instance_id, run_id=run_id, cycle_id=cycle_id)
    return cost, opportunity


def economics_basis(evaluated: Any) -> str:
    """CANONICAL_VENUE only when the cost estimate is a venue/account estimate
    built from the SAME observation the evaluation carries."""
    obs = getattr(evaluated, "venue_observation", None)
    cost = getattr(evaluated, "cost_estimate", None)
    if obs is not None and cost is not None and cost.cost_scope in CANONICAL_COST_SCOPES \
            and cost.venue_observation_id == obs.observation_id:
        return CANONICAL_VENUE
    return REFERENCE_DIAGNOSTIC


__all__ = ["CANONICAL_VENUE", "REFERENCE_DIAGNOSTIC", "CANONICAL_COST_SCOPES", "canonical_economics",
           "economics_basis"]
