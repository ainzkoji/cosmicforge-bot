"""Minimal typed service boundaries closing the Section 6/7 architectural gaps.

Interfaces only -- no business logic lives here. Each is deliberately unable
to place an order, touch capital, or mutate a position slot: none of their
methods accept a broker client, an execution mode, or a capital reference,
and none return anything resembling an order id.
"""
from __future__ import annotations

from typing import Any, Protocol, Sequence, runtime_checkable

from app.trading_intelligence.contracts.exposure import AccountExposureSnapshot
from app.trading_intelligence.contracts.portfolio import AccountPortfolioReservation


@runtime_checkable
class CATIControllerProtocol(Protocol):
    """Future responsibility (Section 6.1.B): for one bot cycle, receive
    market intelligence, discover setup candidates, forecast their outcomes,
    adapt venue costs, veto, collect, rank, and request portfolio selection.

    Only Sections 9-13 are implemented behind this protocol today (MarketState
    through EconomicOpportunity). Veto (14), ranking (15) and portfolio
    selection (16) are future responsibilities NOT implemented here.

    MUST NOT: place orders, call broker execution, reserve position slots,
    alter capital, bypass risk, or modify existing V2 decisions -- enforced
    structurally, since ``run_cycle`` never receives a broker client,
    execution mode, or capital/position reference.
    """

    def run_cycle(self, *, snapshot: Any, venue: str, source: str) -> Sequence[Any]:
        """Run the current CATI pipeline for one causally-pinned snapshot.
        Returns the collected EconomicOpportunity records for shadow
        evidence only -- the return value must never be fed back into V2."""
        ...


@runtime_checkable
class AccountPortfolioService(Protocol):
    """Future responsibility (Section 16, NOT implemented here): account-wide
    opportunity compatibility, cross-bot account exposure, selection and
    reservation. This task defines the shape only -- no portfolio optimizer,
    no correlation optimizer, no slot mutation, no capital mutation, no
    execution."""

    def select(
        self,
        exposure_snapshot: AccountExposureSnapshot,
        candidates: Sequence[Any],
    ) -> AccountPortfolioReservation:
        ...


@runtime_checkable
class PositionIntelligenceService(Protocol):
    """Future responsibility (Section 19, NOT implemented here): remaining
    expected edge, hypothesis health, exit intent. Placeholder interface
    only -- no economic exit logic exists yet, and none should be added
    here just to fill the file out."""

    def assess(self, *, position: Any, market_state: Any) -> Any:
        ...


__all__ = ["CATIControllerProtocol", "AccountPortfolioService", "PositionIntelligenceService"]
