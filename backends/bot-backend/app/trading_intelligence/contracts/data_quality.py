"""Data capability, quality and manifest contracts (Sections 8.2-8.4, 9.12).

Fail-closed by construction (P7): a capability that was not fetched is
``OPTIONAL_NOT_REQUESTED``, one that was fetched but absent is
``UNAVAILABLE``, one the venue does not offer is ``UNSUPPORTED``, one that
came back too old is ``STALE``, and one that failed a sanity check (NaN/inf,
negative depth, etc.) is ``INVALID``. None of these states is ever silently
converted to a zero/neutral value -- callers must branch on
``FeatureAvailability`` before reading a feature.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.hashing import stable_hash


class Capability(str, Enum):
    OHLCV = "OHLCV"
    TOP_OF_BOOK = "TOP_OF_BOOK"
    DEPTH = "DEPTH"
    TRADES_AGGRESSOR = "TRADES_AGGRESSOR"
    OPEN_INTEREST = "OPEN_INTEREST"
    FUNDING = "FUNDING"
    BASIS = "BASIS"
    LIQUIDATIONS = "LIQUIDATIONS"


class CapabilityState(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    UNSUPPORTED = "UNSUPPORTED"
    STALE = "STALE"
    INVALID = "INVALID"
    OPTIONAL_NOT_REQUESTED = "OPTIONAL_NOT_REQUESTED"


#: Stable reason-code vocabulary (Section 9.12). New codes are added here,
#: never scattered as ad-hoc strings through feature calculators, following
#: the same one-vocabulary convention as ``app/decision/reasons.py``.
class ReasonCode(str, Enum):
    PRIMARY_CANDLES_MISSING = "PRIMARY_CANDLES_MISSING"
    PRIMARY_CANDLES_STALE = "PRIMARY_CANDLES_STALE"
    HTF_MISALIGNED = "HTF_MISALIGNED"
    HTF_UNAVAILABLE = "HTF_UNAVAILABLE"
    AUXILIARY_DATA_MISSING = "AUXILIARY_DATA_MISSING"
    TOP_BOOK_UNAVAILABLE = "TOP_BOOK_UNAVAILABLE"
    TOP_BOOK_STALE = "TOP_BOOK_STALE"
    DEPTH_UNSUPPORTED = "DEPTH_UNSUPPORTED"
    FUNDING_UNAVAILABLE = "FUNDING_UNAVAILABLE"
    OPEN_INTEREST_UNAVAILABLE = "OPEN_INTEREST_UNAVAILABLE"
    NONFINITE_FEATURE = "NONFINITE_FEATURE"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    OUT_OF_CAUSAL_BOUNDARY = "OUT_OF_CAUSAL_BOUNDARY"
    DATA_SOURCE_MISMATCH = "DATA_SOURCE_MISMATCH"


class DataQualityLevel(str, Enum):
    VALID = "VALID"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


#: Missing/degraded state on these capabilities can never be waved through as
#: VALID -- primary OHLCV integrity is the one thing every MarketState needs.
_CRITICAL_REASON_CODES = frozenset(
    {
        ReasonCode.PRIMARY_CANDLES_MISSING,
        ReasonCode.PRIMARY_CANDLES_STALE,
        ReasonCode.OUT_OF_CAUSAL_BOUNDARY,
        ReasonCode.INSUFFICIENT_HISTORY,
        ReasonCode.NONFINITE_FEATURE,
        ReasonCode.DATA_SOURCE_MISMATCH,
    }
)


@dataclass(frozen=True)
class DataQuality:
    """The one fail-closed verdict a MarketState carries.

    ``level`` is derived, never asserted directly by a feature calculator:
    use :func:`derive_data_quality` so the critical/optional distinction is
    enforced in one place.
    """

    level: DataQualityLevel
    reason_codes: Tuple[str, ...] = ()

    @property
    def is_usable(self) -> bool:
        return self.level != DataQualityLevel.INVALID


def derive_data_quality(reason_codes: Tuple[str, ...]) -> DataQuality:
    codes = tuple(dict.fromkeys(reason_codes))  # de-dup, keep order
    if not codes:
        return DataQuality(level=DataQualityLevel.VALID, reason_codes=())
    if any(ReasonCode(c) in _CRITICAL_REASON_CODES for c in codes):
        return DataQuality(level=DataQualityLevel.INVALID, reason_codes=codes)
    return DataQuality(level=DataQualityLevel.DEGRADED, reason_codes=codes)


@dataclass(frozen=True)
class FeatureAvailability:
    """Per-capability state + reason, for every capability CATI knows about.

    Always fully populated (one entry per ``Capability`` member) so a
    consumer can ask "is DEPTH available" without a KeyError, and so an
    unpopulated/forgotten capability cannot be silently read as available.
    """

    states: Mapping[str, str]  # Capability.value -> CapabilityState.value
    reasons: Mapping[str, str] = field(default_factory=dict)  # Capability.value -> ReasonCode.value

    def state_of(self, capability: Capability) -> CapabilityState:
        return CapabilityState(self.states.get(capability.value, CapabilityState.UNAVAILABLE.value))

    def is_available(self, capability: Capability) -> bool:
        return self.state_of(capability) == CapabilityState.AVAILABLE

    @staticmethod
    def build(
        available: Tuple[Capability, ...] = (),
        unavailable: Mapping[Capability, ReasonCode] = None,
        unsupported: Tuple[Capability, ...] = (),
        not_requested: Tuple[Capability, ...] = (),
    ) -> "FeatureAvailability":
        unavailable = unavailable or {}
        states: dict = {}
        reasons: dict = {}
        for cap in available:
            states[cap.value] = CapabilityState.AVAILABLE.value
        for cap, reason in unavailable.items():
            states[cap.value] = CapabilityState.UNAVAILABLE.value
            reasons[cap.value] = reason.value
        for cap in unsupported:
            states[cap.value] = CapabilityState.UNSUPPORTED.value
        for cap in not_requested:
            states[cap.value] = CapabilityState.OPTIONAL_NOT_REQUESTED.value
        for cap in Capability:
            states.setdefault(cap.value, CapabilityState.UNAVAILABLE.value)
        return FeatureAvailability(states=states, reasons=reasons)




@dataclass(frozen=True)
class DataManifest:
    """Exactly which raw data fed one MarketState (Section 8.3).

    Built entirely from causal *input* identity -- never from computed
    features -- so its hash can be (and is) known before the feature engine
    runs at all, which is what makes it usable as part of the shared cache
    key (Section 8.4): two requests with the same manifest hash are
    guaranteed to feed the engine identical data, so the engine never needs
    to run twice.

    ``manifest_hash`` is deterministic: identical inputs always produce the
    identical hash, and it excludes nothing that affects analytical meaning
    and includes nothing volatile (no wall-clock, no random id).
    """

    source: str
    venue: str
    canonical_symbol: str
    primary_timeframe: str
    primary_last_closed_candle_time: int
    primary_data_hash: str
    schema_version: str
    htf_timeframe: Optional[str] = None
    htf_last_closed_candle_time: Optional[int] = None
    htf_data_hash: Optional[str] = None
    auxiliary_timeframes: Tuple[str, ...] = ()
    auxiliary_data_hash: Optional[str] = None
    derivatives_as_of: Optional[int] = None
    book_as_of: Optional[int] = None

    @property
    def manifest_hash(self) -> str:
        payload = {
            "source": self.source,
            "venue": self.venue,
            "canonical_symbol": self.canonical_symbol,
            "primary_timeframe": self.primary_timeframe,
            "primary_last_closed_candle_time": self.primary_last_closed_candle_time,
            "primary_data_hash": self.primary_data_hash,
            "schema_version": self.schema_version,
            "htf_timeframe": self.htf_timeframe,
            "htf_last_closed_candle_time": self.htf_last_closed_candle_time,
            "htf_data_hash": self.htf_data_hash,
            "auxiliary_timeframes": list(self.auxiliary_timeframes),
            "auxiliary_data_hash": self.auxiliary_data_hash,
            "derivatives_as_of": self.derivatives_as_of,
            "book_as_of": self.book_as_of,
        }
        return stable_hash(payload)


@dataclass(frozen=True)
class SharedStateCacheKey:
    """The only identity a shared MarketState computation may be cached by.

    Never ``(user_id, symbol)`` or ``(bot_id, symbol)`` and never a bare
    symbol -- see Section 8.4. Two requests with an identical key must have
    identical causal market input, and therefore must produce byte-equivalent
    MarketState (P2 determinism).
    """

    source_venue_or_provider: str
    canonical_instrument_id: str
    timeframe: str
    latest_closed_candle_time: int
    market_state_schema_version: str
    data_manifest_hash: str

    def as_tuple(self) -> tuple:
        return (
            self.source_venue_or_provider,
            self.canonical_instrument_id,
            self.timeframe,
            self.latest_closed_candle_time,
            self.market_state_schema_version,
            self.data_manifest_hash,
        )

    def __hash__(self) -> int:
        return hash(self.as_tuple())


__all__ = [
    "Capability",
    "CapabilityState",
    "ReasonCode",
    "DataQualityLevel",
    "DataQuality",
    "derive_data_quality",
    "FeatureAvailability",
    "DataManifest",
    "SharedStateCacheKey",
]
