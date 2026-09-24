"""VenueEconomicContext -- what the shadow controller needs to run Section 17
for one instrument: a resolved adapter, ONE captured credential-free raw
snapshot, and the canonical account identity. Built by
``integration/venue_context.py`` from safe runtime metadata; never holds a
client, key, secret or header."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.contracts.venue_economics import VenueEconomicObservation
from app.trading_intelligence.venue.adapter import BaseVenueEconomicAdapter, VenueEconomicRequest, VenueRawSnapshot
from app.trading_intelligence.venue.policy import VenueCostPolicy, default_venue_cost_policy


@dataclass(frozen=True)
class VenueEconomicContext:
    adapter: BaseVenueEconomicAdapter
    raw: VenueRawSnapshot
    environment: str
    decision_time: int
    user_id: Optional[str] = None
    broker_account_id: Optional[str] = None
    bot_instance_id: Optional[str] = None
    run_id: Optional[str] = None
    cycle_id: Optional[str] = None
    broker_health: Optional[BrokerHealthContext] = None
    policy: Any = None  # VenueCostPolicy
    #: provisional economic size for cost estimation only (never a quantity)
    reference_notional: Optional[float] = None

    @property
    def cost_policy(self) -> VenueCostPolicy:
        return self.policy or default_venue_cost_policy()

    def observe(self, instrument_key: InstrumentKey, market_decision_time: int) -> VenueEconomicObservation:
        request = VenueEconomicRequest(
            instrument_key=instrument_key, environment=self.environment,
            decision_time=self.adapter.causal_decision_time(self.raw, max(self.decision_time, market_decision_time)),
            user_id=self.user_id, broker_account_id=self.broker_account_id, bot_instance_id=self.bot_instance_id,
            run_id=self.run_id, cycle_id=self.cycle_id, broker_health=self.broker_health,
        )
        return self.adapter.observe(request, self.raw)


__all__ = ["VenueEconomicContext"]
