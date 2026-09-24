"""Deterministic Section 11 specialist registry (Section 11.8).

Exactly the four V1 specialists -- no reversal specialist yet. Iteration
order here is a dict/tuple's natural order and carries no trade-priority
meaning: every specialist runs, every candidate is collected, and nothing
downstream may treat one specialist's candidates as preferred over another's
because it happened to run first (P4 whole-universe selection compatibility).
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.setups.breakout_expansion import BreakoutVolExpansionSpecialist
from app.trading_intelligence.setups.momentum_continuation import MomentumContinuationSpecialist
from app.trading_intelligence.setups.policy import default_policies
from app.trading_intelligence.setups.range_mean_reversion import RangeMeanReversionSpecialist
from app.trading_intelligence.setups.trend_pullback import TrendPullbackSpecialist

SPECIALIST_REGISTRY: Dict[str, Any] = {
    "TREND_PULLBACK_V2": TrendPullbackSpecialist(),
    "BREAKOUT_VOL_EXPANSION_V2": BreakoutVolExpansionSpecialist(),
    "RANGE_MEAN_REVERSION_V2": RangeMeanReversionSpecialist(),
    "MOMENTUM_CONTINUATION_V1": MomentumContinuationSpecialist(),
}


def discover_all(
    *, snapshot: Any, market_state: Any, regime_distribution: Any, policies: Dict[str, Any] = None
) -> Tuple[SetupCandidate, ...]:
    """Runs every registered specialist and collects all candidates.

    A specialist that raises is NOT swallowed here -- a bug in one
    specialist's discovery logic must be visible, not silently dropped
    (this differs from the shadow_hook boundary, which exists specifically
    to protect the *live V2 loop* from a CATI fault; internal CATI research
    code should fail loud so it gets fixed, not silently degrade)."""
    policies = policies if policies is not None else default_policies()
    candidates = []
    for family, specialist in SPECIALIST_REGISTRY.items():
        policy = policies[family]
        candidates.extend(specialist.discover(
            snapshot=snapshot, market_state=market_state,
            regime_distribution=regime_distribution, policy=policy,
        ))
    return tuple(candidates)


__all__ = ["SPECIALIST_REGISTRY", "discover_all"]
