"""Contracts for the broker-derived market universe.

Unknown is a value here, not a default: a metric an adapter cannot supply is
``None`` and is reported as such. Nothing downstream may treat a missing volume
as a small one or a missing spread as a tight one.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping

from app.universe.identity import CanonicalInstrument


class UniverseMode:
    """Where a bot's markets come from."""

    #: The connected broker account's eligible markets (the default).
    BROKER = "BROKER"
    #: An explicit user restriction to a named list.
    ALLOWLIST = "ALLOWLIST"

    ALL = frozenset({BROKER, ALLOWLIST})
    _ALIASES = {
        "broker": BROKER,
        "auto": BROKER,
        "dynamic": BROKER,
        "allowlist": ALLOWLIST,
        "custom": ALLOWLIST,
        "static": ALLOWLIST,
    }

    @classmethod
    def normalize(cls, value: Any) -> str | None:
        """Canonical mode, ``None`` when unset. Unknown values are an error."""
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        mode = cls._ALIASES.get(text.lower())
        if mode is None:
            raise ValueError(f"unknown universe mode {value!r}; expected one of {sorted(cls.ALL)}")
        return mode


class Product:
    PERPETUAL = "PERPETUAL"
    DELIVERY = "DELIVERY"
    SPOT = "SPOT"
    OTHER = "OTHER"


class Exclusion:
    """Deterministic reasons an instrument is not a new-entry candidate."""

    # Hard eligibility -- the instrument cannot be traded by this bot at all.
    NOT_FUTURES = "NOT_FUTURES"
    NOT_TRADING = "NOT_TRADING"
    UNSUPPORTED_CONTRACT = "UNSUPPORTED_CONTRACT"
    QUOTE_UNSUPPORTED = "QUOTE_UNSUPPORTED"
    INVALID_INSTRUMENT_FILTERS = "INVALID_INSTRUMENT_FILTERS"
    MIN_NOTIONAL_UNSUPPORTED = "MIN_NOTIONAL_UNSUPPORTED"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    INVALID_PRICE = "INVALID_PRICE"
    STALE_DATA = "STALE_DATA"
    # Market quality -- tradable, but not worth an entry today.
    MARKET_STATS_UNKNOWN = "MARKET_STATS_UNKNOWN"
    LOW_LIQUIDITY = "LOW_LIQUIDITY"
    SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
    RANK_BELOW_ACTIVE_LIMIT = "RANK_BELOW_ACTIVE_LIMIT"
    # Circumstance.
    RATE_LIMIT_DEFERRED = "RATE_LIMIT_DEFERRED"
    USER_ALLOWLIST_EXCLUDED = "USER_ALLOWLIST_EXCLUDED"
    UNDERLYING_ALREADY_OPEN = "UNDERLYING_ALREADY_OPEN"

    HARD = frozenset({
        NOT_FUTURES, NOT_TRADING, UNSUPPORTED_CONTRACT, QUOTE_UNSUPPORTED,
        INVALID_INSTRUMENT_FILTERS, MIN_NOTIONAL_UNSUPPORTED, INSUFFICIENT_HISTORY,
        INVALID_PRICE, STALE_DATA,
    })


@dataclass(frozen=True)
class InstrumentMeta:
    """One broker instrument, normalised. Filter values ``None`` = not published."""

    venue: str
    venue_symbol: str
    canonical: CanonicalInstrument
    status: str
    tradable: bool
    product: str
    underlying_type: str | None
    quote_asset: str
    margin_asset: str
    tick_size: float | None
    step_size: float | None
    min_qty: float | None
    min_notional: float | None
    listed_at_ms: int | None = None
    expires_at_ms: int | None = None

    @property
    def symbol(self) -> str:
        return self.venue_symbol


@dataclass(frozen=True)
class MarketStats:
    """Cheap, batched market metadata. Every field may be UNKNOWN (``None``)."""

    quote_volume_24h: float | None = None
    trade_count_24h: int | None = None
    last_price: float | None = None
    spread_bps: float | None = None
    stats_time_ms: int | None = None
    open_interest: float | None = None


@dataclass(frozen=True)
class UniverseMember:
    """One active new-entry candidate and the evidence that selected it."""

    symbol: str
    rank: int
    canonical_id: str
    underlying: str
    quote_volume_24h: float | None
    spread_bps: float | None
    trade_count_24h: int | None
    last_price: float | None
    selection_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UniverseSnapshot:
    """One refresh of one bot's universe."""

    broker_account_id: str
    venue: str
    mode: str
    generated_at: str
    discovered_count: int
    eligible_count: int
    ranked_count: int
    active: tuple[UniverseMember, ...] = ()
    excluded: Mapping[str, str] = field(default_factory=dict)
    stale: bool = False
    error: str | None = None
    metadata_age_seconds: float | None = None
    stats_age_seconds: float | None = None
    capabilities: Mapping[str, bool] = field(default_factory=dict)

    @property
    def active_symbols(self) -> tuple[str, ...]:
        return tuple(m.symbol for m in self.active)

    @property
    def active_count(self) -> int:
        return len(self.active)

    @property
    def excluded_count(self) -> int:
        return len(self.excluded)

    @property
    def excluded_by_reason(self) -> dict[str, int]:
        return dict(sorted(Counter(self.excluded.values()).items()))


__all__ = [
    "Exclusion",
    "InstrumentMeta",
    "MarketStats",
    "Product",
    "UniverseMember",
    "UniverseMode",
    "UniverseSnapshot",
]
