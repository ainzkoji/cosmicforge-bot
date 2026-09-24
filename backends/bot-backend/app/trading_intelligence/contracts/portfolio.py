"""AccountPortfolioReservation (Section 7.4 / A3) -- the missing serialization
boundary for FUTURE account-wide opportunity selection.

This is a typed contract only. It does NOT:

- implement Section 16 portfolio optimization
- replace ``app.risk.capital_ledger.AccountMarginReservations``
- replace ``app.execution.position_slots`` slot reservation
- introduce another funding/capital authority

The repo already has three independent, correctly narrow, resource-specific
locks (margin, position slots, daily risk budget -- see
``app/risk/capital_ledger.py``, ``app/execution/position_slots.py``,
``app/risk/adaptive_daily_budget.py``). None of them serialize "which
EconomicOpportunity candidates were selected together this cycle for
portfolio purposes" -- that is a distinct resource (candidate-selection
race protection, not margin or slot affordability), so a fourth, narrowly
scoped reservation *contract* is appropriate here. No lock is implemented in
this task; ``AccountPortfolioLock`` below is the interface a future
concrete implementation should satisfy, and it should wrap an existing
lock primitive (e.g. a ``threading.Lock`` keyed by ``broker_account_id``,
the same pattern ``AccountMarginReservations`` already uses) rather than
inventing new locking machinery.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol, Sequence, Tuple

from app.trading_intelligence.versions import PORTFOLIO_RESERVATION_SCHEMA_VERSION


class ReservationStatus(str, Enum):
    RESERVED = "RESERVED"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"
    CONSUMED = "CONSUMED"


_VALID_STATUSES = frozenset(s.value for s in ReservationStatus)


@dataclass(frozen=True)
class AccountPortfolioReservation:
    """Serializes one cycle's account-wide candidate selection.

    Immutable: a status transition (e.g. RESERVED -> CONSUMED) produces a
    *new* record via :meth:`with_status`, it never mutates in place -- the
    same "no mutation of historical library semantics in place" discipline
    Section 12.4 requires of the outcome library.
    """

    reservation_id: str
    broker_account_id: str
    cycle_id: str
    selected_candidate_ids: Tuple[str, ...]
    created_at: int
    expires_at: int
    reservation_version: str = PORTFOLIO_RESERVATION_SCHEMA_VERSION
    status: str = ReservationStatus.RESERVED.value
    bot_instance_id: str = ""
    #: (canonical_symbol, venue, venue_symbol, side) per selected candidate.
    selected_instruments: Tuple[Tuple[str, str, str, str], ...] = ()
    #: SHADOW today. An ACTIVE reservation would be consulted by execution --
    #: nothing in this task creates one.
    mode: str = "SHADOW"

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(f"unknown reservation status: {self.status!r}")
        if not self.broker_account_id:
            raise ValueError("broker_account_id is required -- this contract is account-scoped")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")

    def with_status(self, status: ReservationStatus) -> "AccountPortfolioReservation":
        from dataclasses import replace

        return replace(self, status=status.value)


class AccountPortfolioLock(Protocol):
    """Future account-scoped reservation boundary (Section 7.4). A concrete
    implementation is NOT provided by this task -- define it only when
    Section 16 actually needs to serialize concurrent portfolio selection."""

    def reserve(
        self, *, broker_account_id: str, cycle_id: str, candidate_ids: Sequence[str], ttl_seconds: int
    ) -> AccountPortfolioReservation:
        ...

    def release(self, reservation_id: str) -> None:
        ...


__all__ = ["ReservationStatus", "AccountPortfolioReservation", "AccountPortfolioLock"]
