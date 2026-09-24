"""CATIController (Section 6.1.B) -- shadow-only implementation of the
pipeline built so far (Sections 9-14):

MarketState -> RegimeDistribution -> SetupCandidate[] -> OutcomeForecast[]
-> [Section 17 VenueEconomicObservation ->] CostEstimate -> EconomicOpportunity[]
-> VetoDecision[]

With a ``venue_context`` the CostEstimate is the Section 17 venue/account
estimate (``venue/cost_model.py``); without one it is the Section 13
reference estimate. Either way the SAME Section 13 engine consumes it.

Ranking (15), portfolio selection (16) and everything downstream happen at
CYCLE level in ``ranking/coordinator.py`` -- never inside this per-symbol
evaluation. Nothing here accepts a broker client, execution mode, or
capital/position reference, and nothing returns an order confirmation.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Sequence, Union

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.trading_intelligence.contracts.economics import AdmissionPolicy, EconomicOpportunity
from app.trading_intelligence.contracts.ranking import EvaluatedOpportunity, SymbolEvalKind, SymbolEvaluation
from app.trading_intelligence.contracts.veto import EventRiskContext, SystemHealthContext, VetoPolicy
from app.trading_intelligence.economics.costs import build_cost_estimate
from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
from app.trading_intelligence.economics.policy import default_admission_policy
from app.trading_intelligence.forecast.engine import build_outcome_forecast
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary
from app.trading_intelligence.integration.snapshot_adapter import evaluate_market_state
from app.trading_intelligence.portfolio.groups import static_group_for
from app.trading_intelligence.regime.engine import compute_regime_distribution
from app.trading_intelligence.regime.policy import RegimePolicy, default_policy
from app.trading_intelligence.setups.policy import default_policies
from app.trading_intelligence.setups.registry import discover_all
from app.trading_intelligence.venue.context import VenueEconomicContext
from app.trading_intelligence.venue.cost_model import build_venue_cost_estimate
from app.trading_intelligence.veto.engine import evaluate_veto

logger = logging.getLogger(__name__)


class CATIController:
    """Implements ``CATIControllerProtocol``.

    ``outcome_library`` is optional: with none, every forecast is
    ``OUTCOME_LIBRARY_UNAVAILABLE`` and every opportunity
    ``INSUFFICIENT_EVIDENCE`` (correct fail-closed behavior, never fabricated).
    """

    def __init__(
        self,
        *,
        outcome_library: Optional[HistoricalOutcomeLibrary] = None,
        setup_policies: Optional[dict] = None,
        regime_policy: Optional[RegimePolicy] = None,
        admission_policy: Optional[AdmissionPolicy] = None,
        veto_policy: Optional[VetoPolicy] = None,
        cost_model: Optional[CostModel] = None,
    ) -> None:
        self._outcome_library = outcome_library
        self._setup_policies = setup_policies if setup_policies is not None else default_policies()
        self._regime_policy = regime_policy or default_policy()
        self._admission_policy = admission_policy or default_admission_policy()
        self._veto_policy = veto_policy or VetoPolicy()
        # Modeled (research-assumption) venue cost, never a zero-cost default: a
        # zero-cost model fails the Section 13 cost-quality gate by design.
        self._cost_model = cost_model or BINANCE_FUTURES_STANDARD

    def evaluate_symbol(
        self,
        *,
        snapshot: Any,
        venue: str,
        source: str,
        event_context: Optional[EventRiskContext] = None,
        system_context: Optional[SystemHealthContext] = None,
        user_id: Optional[str] = None,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        run_id: Optional[str] = None,
        cycle_id: Optional[str] = None,
        base_asset: Optional[str] = None,
        quote_asset: Optional[str] = None,
        asset_class: str = "CRYPTO",
        venue_context: Optional[Union[VenueEconomicContext, Callable[[], VenueEconomicContext]]] = None,
    ) -> SymbolEvaluation:
        """One instrument's full Section 9-14 evaluation to a TERMINAL state.
        Never raises: an internal fault becomes an explicit
        CATI_COMPONENT_ERROR terminal record (Section 15.2)."""
        instrument = str(snapshot.symbol).upper()
        try:
            market_state = evaluate_market_state(
                snapshot, venue=venue, source=source, base_asset=base_asset, quote_asset=quote_asset,
                asset_class=asset_class,
            )
            if not market_state.is_usable:
                return SymbolEvaluation(instrument, SymbolEvalKind.CATI_COMPONENT_ERROR.value, error="MARKET_STATE_INVALID")
            regime = compute_regime_distribution(market_state, self._regime_policy)
            candidates = discover_all(
                snapshot=snapshot, market_state=market_state, regime_distribution=regime, policies=self._setup_policies,
            )
            rows = tuple(snapshot.candles)
            if not candidates:
                return SymbolEvaluation(instrument, SymbolEvalKind.NO_CANDIDATES.value, candle_rows=rows)
            group = static_group_for(instrument)
            # Section 17: ONE side-independent venue observation per instrument,
            # shared by every candidate on it. A callable context is resolved
            # only here, so symbols without candidates cost no venue requests.
            if callable(venue_context):
                venue_context = venue_context()
            observation = (venue_context.observe(market_state.instrument_key, market_state.decision_time)
                           if venue_context is not None else None)
            evaluated = []
            for candidate in candidates:
                forecast = build_outcome_forecast(candidate, market_state, regime, self._outcome_library, instrument_group=group)
                if observation is not None:
                    cost = build_venue_cost_estimate(candidate, observation, forecast=forecast,
                                                     policy=venue_context.cost_policy,
                                                     reference_notional=venue_context.reference_notional)
                else:
                    cost = build_cost_estimate(candidate, cost_model=self._cost_model, liquidity_verified=market_state.liquidity_state.available)
                opportunity = evaluate_economic_opportunity(
                    candidate, market_state, forecast, cost, policy=self._admission_policy, user_id=user_id,
                    broker_account_id=broker_account_id, bot_instance_id=bot_instance_id, run_id=run_id, cycle_id=cycle_id,
                )
                veto = evaluate_veto(
                    opportunity=opportunity, candidate=candidate, market_state=market_state, regime_distribution=regime,
                    forecast=forecast, cost_estimate=cost, policy=self._veto_policy, event_context=event_context,
                    system_context=system_context, user_id=user_id, broker_account_id=broker_account_id,
                    bot_instance_id=bot_instance_id, run_id=run_id, cycle_id=cycle_id,
                )
                evaluated.append(EvaluatedOpportunity(candidate, market_state, regime, forecast, cost, opportunity, veto,
                                                      venue_observation=observation))
            return SymbolEvaluation(instrument, SymbolEvalKind.EVALUATED.value, opportunities=tuple(evaluated), candle_rows=rows)
        except Exception as exc:  # explicit terminal state, recorded -- not swallowed silently
            from app.trading_intelligence.integration.errors import record_component_error

            logger.error("[CATI] %s: component error during symbol evaluation (%s)", instrument, type(exc).__name__)
            rec = record_component_error("controller.evaluate_symbol", exc, cycle_id=cycle_id,
                                         bot_instance_id=bot_instance_id, broker_account_id=broker_account_id,
                                         symbol=instrument)
            return SymbolEvaluation(instrument, SymbolEvalKind.CATI_COMPONENT_ERROR.value,
                                    error=f"{rec.exception_class}: {rec.message}")

    def run_cycle(self, *, snapshot: Any, venue: str, source: str) -> Sequence[EconomicOpportunity]:
        """Single-symbol convenience (Sections 9-13 view): the EconomicOpportunity
        records for shadow evidence. Whole-universe ranking is the
        coordinator's job, not this method's."""
        evaluation = self.evaluate_symbol(snapshot=snapshot, venue=venue, source=source)
        return tuple(e.opportunity for e in evaluation.opportunities)


__all__ = ["CATIController"]
