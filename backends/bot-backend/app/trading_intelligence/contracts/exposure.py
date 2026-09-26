"""AccountExposureSnapshot (Section 7.3) -- the missing Section 6/7 contract.

A typed, immutable, FUTURE portfolio-selection input. It does NOT replace:

- ``app.risk.capital_ledger.AccountMarginReservations`` (margin authority)
- ``app.execution.position_slots`` (slot authority)
- ``app.risk.capital_ledger.CapitalLedger`` (capital/allocation authority)

Those remain the sole authorities for their respective resources. This
contract exists only so a future Section 16 account portfolio selector has a
stable, versioned shape to consume -- nothing in this task computes or
persists one from live DB state.

Tenant/account scoped by construction: ``broker_account_id`` is required at
the snapshot level, and every ``ExposureRecord`` additionally carries its own
``bot_instance_id`` so cross-bot attribution survives aggregation. This must
NEVER be an input to ``SharedMarketIntelligenceService``/``MarketState`` --
P3 tenant isolation forbids shared market facts from depending on account
state.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.market_state import new_correlation_id
from app.trading_intelligence.versions import EXPOSURE_SNAPSHOT_SCHEMA_VERSION


class ExposureSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class ExposureStatus(str, Enum):
    OPEN = "OPEN"
    PENDING_ENTRY = "PENDING_ENTRY"
    PENDING_EXIT = "PENDING_EXIT"
    #: An ACTIVE CATI portfolio reservation (SHADOW mode). Visible to other
    #: CATI selections on the same account; never consulted by production
    #: execution/risk.
    SHADOW_RESERVED = "SHADOW_RESERVED"


_VALID_SIDES = frozenset(s.value for s in ExposureSide)
_VALID_STATUSES = frozenset(s.value for s in ExposureStatus)


@dataclass(frozen=True)
class ExposureRecord:
    """One open or pending exposure, attributable to exactly one bot."""

    bot_instance_id: str
    instrument_key: InstrumentKey
    side: str
    quantity: float
    notional: float
    entry_reference: float
    exposure_status: str
    #: Optional future portfolio-correlation inputs -- unused until Section 16.
    reference_beta: Optional[float] = None
    common_factor: Optional[str] = None
    sector_or_group: Optional[str] = None

    def __post_init__(self) -> None:
        if self.side not in _VALID_SIDES:
            raise ValueError(f"unknown exposure side: {self.side!r}")
        if self.exposure_status not in _VALID_STATUSES:
            raise ValueError(f"unknown exposure_status: {self.exposure_status!r}")
        if not self.bot_instance_id:
            raise ValueError("bot_instance_id is required")


@dataclass(frozen=True)
class AccountExposureSnapshot:
    """One point-in-time view of everything one broker account holds/intends
    across all its bots.

    ``exposure_snapshot_id`` is a random correlation id (mirrors
    ``MarketSnapshot.market_snapshot_id``), not a content hash: this is a
    live-state capture, not a deterministic analytical output, so there is
    no P2 determinism obligation on its identity -- only on records CATI
    computes from versioned inputs (MarketState, RegimeDistribution,
    SetupCandidate, OutcomeForecast, EconomicOpportunity).
    """

    exposure_snapshot_id: str
    broker_account_id: str
    as_of_time: int
    open_exposures: Tuple[ExposureRecord, ...] = ()
    pending_exposures: Tuple[ExposureRecord, ...] = ()
    schema_version: str = EXPOSURE_SNAPSHOT_SCHEMA_VERSION
    source_version: str = "1.0.0"
    #: Active CATI reservations (status SHADOW_RESERVED) for this account.
    reservation_exposures: Tuple[ExposureRecord, ...] = ()

    @property
    def all_exposures(self) -> Tuple[ExposureRecord, ...]:
        return self.open_exposures + self.pending_exposures + self.reservation_exposures

    @property
    def currency_exposure(self):
        from app.trading_intelligence.portfolio.currency_exposure import account_currency_exposure
        return account_currency_exposure(self)

    def duplicate_instruments(self) -> Tuple[str, ...]:
        """Canonical instruments held/intended by MORE THAN ONE bot on this
        account (separate attributable exposures, surfaced -- not merged)."""
        bots: dict = {}
        for r in self.all_exposures:
            bots.setdefault(r.instrument_key.canonical_symbol, set()).add(r.bot_instance_id)
        return tuple(sorted(k for k, v in bots.items() if len(v) > 1))

    def __post_init__(self) -> None:
        if not self.broker_account_id:
            raise ValueError("broker_account_id is required -- this contract is account-scoped")

    @classmethod
    def build(
        cls,
        *,
        broker_account_id: str,
        as_of_time: int,
        open_exposures: Tuple[ExposureRecord, ...] = (),
        pending_exposures: Tuple[ExposureRecord, ...] = (),
        source_version: str = "1.0.0",
        reservation_exposures: Tuple[ExposureRecord, ...] = (),
    ) -> "AccountExposureSnapshot":
        return cls(
            exposure_snapshot_id=new_correlation_id("axs"),
            broker_account_id=broker_account_id,
            as_of_time=as_of_time,
            open_exposures=tuple(open_exposures),
            pending_exposures=tuple(pending_exposures),
            source_version=source_version,
            reservation_exposures=tuple(reservation_exposures),
        )


__all__ = ["ExposureSide", "ExposureStatus", "ExposureRecord", "AccountExposureSnapshot"]
