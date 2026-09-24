"""Normalized, asset-class-neutral event-risk contracts (pre-Section-17 closure).

Events do not predict direction. They only mark periods when historical
conditional distributions may be unreliable (Section 14 EVENT family).

* ``MarketEvent``   -- one scheduled event with explicit scope: currencies,
                       non-currency assets, instruments, venues, asset classes.
* ``EventRiskContext`` -- the events plus the SOURCE STATE (AVAILABLE / STALE /
                       UNAVAILABLE). An unavailable or stale calendar is an
                       explicit state -- never an empty-but-trusted list.
* ``MaintenanceContext`` -- separate capability for venue/contract
                       maintenance. A venue with no maintenance feed is
                       ``UNAVAILABLE``; that is NOT "no maintenance scheduled".

Scope matching (``event_affects``) uses canonical ``InstrumentKey`` legs
(base/quote/settlement), never symbol-string parsing. FX pairs inherit both
currency legs, so a USD event affects EURUSD but not EURJPY unless the event
metadata names JPY/EUR (or the instrument) explicitly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import FrozenSet, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import FX, InstrumentKey
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import (
    EVENT_RISK_SCHEMA_VERSION,
    EVENT_SCOPE_POLICY_VERSION,
    MARKET_EVENT_SCHEMA_VERSION,
)


class MarketEventType(str, Enum):
    MACRO = "MACRO"
    CENTRAL_BANK = "CENTRAL_BANK"
    INFLATION = "INFLATION"
    EMPLOYMENT = "EMPLOYMENT"
    GDP = "GDP"
    PMI = "PMI"
    RATE_DECISION = "RATE_DECISION"
    EXCHANGE_MAINTENANCE = "EXCHANGE_MAINTENANCE"
    CONTRACT_MAINTENANCE = "CONTRACT_MAINTENANCE"
    INSTRUMENT_CHANGE = "INSTRUMENT_CHANGE"
    OTHER = "OTHER"


MAINTENANCE_EVENT_TYPES: FrozenSet[str] = frozenset({
    MarketEventType.EXCHANGE_MAINTENANCE.value, MarketEventType.CONTRACT_MAINTENANCE.value,
})


class EventImportance(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class EventSourceState(str, Enum):
    AVAILABLE = "AVAILABLE"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"


class MaintenanceSourceState(str, Enum):
    AVAILABLE = "AVAILABLE"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class EventScopePolicy:
    """Versioned scope rules. ``currency_aliases`` maps settlement tokens to
    the fiat currency they track (a USD-pegged stablecoin carries USD event
    exposure). Policy data, not engine logic: extend it, don't hardcode."""

    currency_aliases: Mapping[str, str] = field(default_factory=lambda: {
        "USDT": "USD", "USDC": "USD", "FDUSD": "USD", "BUSD": "USD", "USD1": "USD", "TUSD": "USD", "DAI": "USD",
    })
    version: str = EVENT_SCOPE_POLICY_VERSION

    @property
    def policy_hash(self) -> str:
        return stable_hash({"aliases": dict(sorted(self.currency_aliases.items())), "version": self.version})


DEFAULT_EVENT_SCOPE_POLICY = EventScopePolicy()


def instrument_legs(key: InstrumentKey, policy: EventScopePolicy = DEFAULT_EVENT_SCOPE_POLICY) -> Tuple[FrozenSet[str], FrozenSet[str]]:
    """(currency legs, asset legs) of an instrument from its canonical identity.

    FX: both base and quote are currencies. Other classes: the quote /
    settlement asset contributes a currency leg (after alias mapping), the
    base contributes an asset leg (e.g. BTC, a commodity root)."""
    alias = {k.upper(): v.upper() for k, v in policy.currency_aliases.items()}

    def cur(code: Optional[str]) -> Optional[str]:
        if not code:
            return None
        c = str(code).upper()
        return alias.get(c, c)

    if key.asset_class == FX:
        currencies = {c for c in (cur(key.base_asset), cur(key.quote_asset)) if c}
        return frozenset(currencies), frozenset()
    currencies = {c for c in (cur(key.quote_asset), cur(key.settlement_asset)) if c}
    assets = {str(key.base_asset).upper()} if key.base_asset else set()
    return frozenset(currencies), frozenset(assets)


@dataclass(frozen=True)
class MarketEvent:
    event_id: str
    source: str
    event_type: str  # MarketEventType
    scheduled_time: int  # epoch ms
    importance: str  # EventImportance
    affected_assets: Tuple[str, ...] = ()  # non-currency assets: BTC, ETH, CL, ...
    affected_currencies: Tuple[str, ...] = ()  # USD, EUR, JPY, ...
    affected_instruments: Tuple[str, ...] = ()  # canonical symbols
    affected_venues: Tuple[str, ...] = ()  # restricts scope when non-empty
    affected_asset_classes: Tuple[str, ...] = ()  # whole-class scope (e.g. a global blackout)
    pre_event_window_ms: int = 30 * 60_000
    post_event_window_ms: int = 30 * 60_000
    source_updated_at: Optional[int] = None
    source_quality: str = "VALID"
    version: str = MARKET_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.event_type not in {t.value for t in MarketEventType}:
            raise ValueError(f"unknown event_type {self.event_type!r}")
        if self.pre_event_window_ms < 0 or self.post_event_window_ms < 0:
            raise ValueError("event windows must be non-negative")

    @property
    def window_start(self) -> int:
        return self.scheduled_time - self.pre_event_window_ms

    @property
    def window_end(self) -> int:
        return self.scheduled_time + self.post_event_window_ms

    @property
    def is_maintenance(self) -> bool:
        return self.event_type in MAINTENANCE_EVENT_TYPES

    def overlaps(self, start: int, end: int) -> bool:
        """Closed-interval overlap: [window_start, window_end] vs [start, end]."""
        return self.window_start <= end and self.window_end >= start


def event_affects(event: MarketEvent, key: InstrumentKey, policy: EventScopePolicy = DEFAULT_EVENT_SCOPE_POLICY) -> bool:
    if event.affected_venues and key.venue.lower() not in {v.lower() for v in event.affected_venues}:
        return False
    if key.canonical_symbol in event.affected_instruments or key.venue_symbol in event.affected_instruments:
        return True
    if key.asset_class in event.affected_asset_classes:
        return True
    currencies, assets = instrument_legs(key, policy)
    if currencies & {c.upper() for c in event.affected_currencies}:
        return True
    if assets & {a.upper() for a in event.affected_assets}:
        return True
    # A venue-scoped event with no narrower scope covers everything on that venue.
    return bool(event.affected_venues) and not (
        event.affected_instruments or event.affected_asset_classes or event.affected_currencies or event.affected_assets)


@dataclass(frozen=True)
class MaintenanceContext:
    state: str  # MaintenanceSourceState
    windows: Tuple[MarketEvent, ...] = ()
    source: str = "none"
    as_of: Optional[int] = None
    reason_codes: Tuple[str, ...] = ()

    @classmethod
    def unavailable(cls, as_of: Optional[int] = None, reason: str = "NO_MAINTENANCE_SOURCE") -> "MaintenanceContext":
        return cls(state=MaintenanceSourceState.UNAVAILABLE.value, as_of=as_of, reason_codes=(reason,))


@dataclass(frozen=True)
class EventRiskContext:
    """Events + explicit source state. ``source_state != AVAILABLE`` means
    there is no reliable calendar -- which is NOT "no event risk"."""

    source_state: str = EventSourceState.UNAVAILABLE.value
    events: Tuple[MarketEvent, ...] = ()
    maintenance: MaintenanceContext = field(default_factory=MaintenanceContext.unavailable)
    as_of: Optional[int] = None
    source: str = "none"
    reason_codes: Tuple[str, ...] = ()
    scope_policy: EventScopePolicy = DEFAULT_EVENT_SCOPE_POLICY
    schema_version: str = EVENT_RISK_SCHEMA_VERSION

    @property
    def source_available(self) -> bool:
        return self.source_state == EventSourceState.AVAILABLE.value


__all__ = [
    "MarketEventType", "MAINTENANCE_EVENT_TYPES", "EventImportance", "EventSourceState", "MaintenanceSourceState",
    "EventScopePolicy", "DEFAULT_EVENT_SCOPE_POLICY", "instrument_legs", "MarketEvent", "event_affects",
    "MaintenanceContext", "EventRiskContext",
]
