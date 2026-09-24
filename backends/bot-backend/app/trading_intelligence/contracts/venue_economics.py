"""Section 17 contracts -- normalized, causal, credential-free venue/account
economic evidence.

A canonical market hypothesis is not automatically economically equivalent
across venues or accounts. These contracts carry the RAW, native-semantics
observations a venue adapter made at decision time (fees, top of book, depth,
perpetual funding, FX swap/rollover, dated-futures basis, instrument
precision, execution capabilities, session). ``venue/cost_model.py`` turns one
observation into the EXISTING Section 13 ``CostEstimate``; the Section 13
engine stays the only EV/admission authority.

Nothing here holds, or may hold, an API key, secret, signature or
authorization header -- adapters only ever see public market data and
normalized broker-account metadata.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import (
    VENUE_CAPABILITY_SCHEMA_VERSION,
    VENUE_ECONOMIC_OBSERVATION_SCHEMA_VERSION,
)


class AdapterValidationStatus(str, Enum):
    """Section 17.17 -- what an adapter has actually PROVEN, not what exists.

    * UNVALIDATED: an adapter class exists; its economics are not trusted and
      fail the Section 13 cost-quality gate.
    * SHADOW_VALIDATED: passes the venue economic contract suite on recorded
      venue payloads; usable for shadow evidence with adapter uncertainty.
    * DEMO_VALIDATED: additionally verified against the live demo venue's
      market-data payloads.
    * PRODUCTION_VALIDATED: planned-vs-realized execution cost calibrated on a
      real account. Nothing holds this status yet.
    """

    UNVALIDATED = "UNVALIDATED"
    SHADOW_VALIDATED = "SHADOW_VALIDATED"
    DEMO_VALIDATED = "DEMO_VALIDATED"
    PRODUCTION_VALIDATED = "PRODUCTION_VALIDATED"


class VenueEnvironment(str, Enum):
    DEMO = "DEMO"
    TESTNET = "TESTNET"
    REAL = "REAL"
    UNKNOWN = "UNKNOWN"


class FeeModel(str, Enum):
    PERCENT_NOTIONAL = "PERCENT_NOTIONAL"  # crypto maker/taker
    PER_CONTRACT = "PER_CONTRACT"  # futures commission (+ exchange/clearing)
    SPREAD_ONLY = "SPREAD_ONLY"  # FX account paid through the spread only
    SPREAD_PLUS_COMMISSION = "SPREAD_PLUS_COMMISSION"  # FX raw-spread + commission


class FeeSource(str, Enum):
    OBSERVED_ACCOUNT_TIER = "OBSERVED_ACCOUNT_TIER"
    BROKER_METADATA = "BROKER_METADATA"
    VENUE_DEFAULT = "VENUE_DEFAULT"
    CONSERVATIVE_CONFIGURED_FALLBACK = "CONSERVATIVE_CONFIGURED_FALLBACK"
    UNAVAILABLE = "UNAVAILABLE"


class SpreadSource(str, Enum):
    LIVE_TOP_OF_BOOK = "LIVE_TOP_OF_BOOK"
    RECENT_VENUE_DISTRIBUTION = "RECENT_VENUE_DISTRIBUTION"
    LIQUIDITY_BUCKET_HISTORICAL = "LIQUIDITY_BUCKET_HISTORICAL"
    CONSERVATIVE_VENUE_FALLBACK = "CONSERVATIVE_VENUE_FALLBACK"
    UNAVAILABLE = "UNAVAILABLE"


class SlippageSource(str, Enum):
    DEPTH_WALK = "DEPTH_WALK"
    HISTORICAL_INSTRUMENT = "HISTORICAL_INSTRUMENT"
    HISTORICAL_LIQUIDITY_BUCKET = "HISTORICAL_LIQUIDITY_BUCKET"
    VENUE_ASSET_CLASS_DEFAULT = "VENUE_ASSET_CLASS_DEFAULT"
    CONSERVATIVE_CONFIGURED_FALLBACK = "CONSERVATIVE_CONFIGURED_FALLBACK"
    UNAVAILABLE = "UNAVAILABLE"


class FundingSource(str, Enum):
    PREDICTED_RATE = "PREDICTED_RATE"
    CURRENT_RATE_SCHEDULE = "CURRENT_RATE_SCHEDULE"
    CONSERVATIVE_CONFIGURED_FALLBACK = "CONSERVATIVE_CONFIGURED_FALLBACK"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNAVAILABLE = "UNAVAILABLE"


class FinancingSource(str, Enum):
    BROKER_SWAP_RATES = "BROKER_SWAP_RATES"
    CONSERVATIVE_CONFIGURED_FALLBACK = "CONSERVATIVE_CONFIGURED_FALLBACK"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNAVAILABLE = "UNAVAILABLE"


class CarrySource(str, Enum):
    OBSERVED_BASIS = "OBSERVED_BASIS"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNAVAILABLE = "UNAVAILABLE"


class SwapUnit(str, Enum):
    #: annual rate on notional (OANDA-style longRate/shortRate); sign as broker reports
    ANNUAL_RATE = "ANNUAL_RATE"
    #: price points per base unit per night (MetaTrader-style swap points)
    PRICE_POINTS_PER_UNIT = "PRICE_POINTS_PER_UNIT"


class SessionStatus(str, Enum):
    ALWAYS_OPEN = "ALWAYS_OPEN"
    OPEN = "OPEN"
    THIN = "THIN"
    ROLLOVER = "ROLLOVER"
    CLOSED = "CLOSED"
    UNKNOWN = "UNKNOWN"


class VenueReasonCode(str, Enum):
    STALE_BOOK = "STALE_BOOK"
    SPREAD_FALLBACK_USED = "SPREAD_FALLBACK_USED"
    SPREAD_UNAVAILABLE = "SPREAD_UNAVAILABLE"
    NON_CAUSAL_OBSERVATION = "NON_CAUSAL_OBSERVATION"
    DEPTH_UNAVAILABLE = "DEPTH_UNAVAILABLE"
    DEPTH_STALE = "DEPTH_STALE"
    DEPTH_INSUFFICIENT_FOR_SIZE = "DEPTH_INSUFFICIENT_FOR_SIZE"
    LARGE_ORDER_VS_DEPTH = "LARGE_ORDER_VS_DEPTH"
    SLIPPAGE_FALLBACK_USED = "SLIPPAGE_FALLBACK_USED"
    FEE_TIER_UNKNOWN = "FEE_TIER_UNKNOWN"
    FEE_FALLBACK_USED = "FEE_FALLBACK_USED"
    FEE_UNAVAILABLE = "FEE_UNAVAILABLE"
    ZERO_FEE_REFUSED = "ZERO_FEE_REFUSED"
    ZERO_EXECUTION_COST_REFUSED = "ZERO_EXECUTION_COST_REFUSED"
    FUNDING_UNAVAILABLE = "FUNDING_UNAVAILABLE"
    FUNDING_STALE = "FUNDING_STALE"
    FUNDING_FALLBACK_USED = "FUNDING_FALLBACK_USED"
    FUNDING_SCHEDULE_UNKNOWN = "FUNDING_SCHEDULE_UNKNOWN"
    PREDICTED_FUNDING_UNAVAILABLE = "PREDICTED_FUNDING_UNAVAILABLE"
    FUNDING_INCOME_NOT_CREDITED = "FUNDING_INCOME_NOT_CREDITED"
    MARK_INDEX_UNAVAILABLE = "MARK_INDEX_UNAVAILABLE"
    FINANCING_UNAVAILABLE = "FINANCING_UNAVAILABLE"
    #: the expected/maximum hold crosses a rollover and no swap data or
    #: certified fallback exists: missing swap is not zero swap (P7)
    FINANCING_REQUIRED_UNAVAILABLE = "FINANCING_REQUIRED_UNAVAILABLE"
    FINANCING_FALLBACK_USED = "FINANCING_FALLBACK_USED"
    FINANCING_INCOME_NOT_CREDITED = "FINANCING_INCOME_NOT_CREDITED"
    CARRY_UNAVAILABLE = "CARRY_UNAVAILABLE"
    CARRY_BENEFIT_NOT_CREDITED = "CARRY_BENEFIT_NOT_CREDITED"
    EXPIRY_WITHIN_HOLD = "EXPIRY_WITHIN_HOLD"
    MARKET_CLOSED = "MARKET_CLOSED"
    SESSION_UNKNOWN = "SESSION_UNKNOWN"
    THIN_SESSION = "THIN_SESSION"
    ROLLOVER_WINDOW = "ROLLOVER_WINDOW"
    ADAPTER_UNVALIDATED = "ADAPTER_UNVALIDATED"
    UNSUPPORTED_VENUE = "UNSUPPORTED_VENUE"
    UNSUPPORTED_ASSET_CLASS = "UNSUPPORTED_ASSET_CLASS"
    ENVIRONMENT_UNKNOWN = "ENVIRONMENT_UNKNOWN"
    INSTRUMENT_METADATA_UNAVAILABLE = "INSTRUMENT_METADATA_UNAVAILABLE"
    INSTRUMENT_MAPPING_MISMATCH = "INSTRUMENT_MAPPING_MISMATCH"
    BROKER_DEGRADED = "BROKER_DEGRADED"
    BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"
    CURRENCY_CONVERSION_UNAVAILABLE = "CURRENCY_CONVERSION_UNAVAILABLE"
    BELOW_MIN_NOTIONAL_AT_REFERENCE = "BELOW_MIN_NOTIONAL_AT_REFERENCE"
    COLLECTION_ERROR = "COLLECTION_ERROR"


#: Reasons that make an observation untrustworthy as cost evidence: the cost
#: model then marks the estimate COST_NOT_VIABLE (Section 13 COST_QUALITY gate).
FATAL_VENUE_REASONS = frozenset({
    VenueReasonCode.MARKET_CLOSED.value, VenueReasonCode.SESSION_UNKNOWN.value,
    VenueReasonCode.ADAPTER_UNVALIDATED.value, VenueReasonCode.UNSUPPORTED_VENUE.value,
    VenueReasonCode.UNSUPPORTED_ASSET_CLASS.value, VenueReasonCode.ENVIRONMENT_UNKNOWN.value,
    VenueReasonCode.INSTRUMENT_METADATA_UNAVAILABLE.value, VenueReasonCode.INSTRUMENT_MAPPING_MISMATCH.value,
    VenueReasonCode.FEE_UNAVAILABLE.value, VenueReasonCode.BROKER_UNAVAILABLE.value,
    VenueReasonCode.CURRENCY_CONVERSION_UNAVAILABLE.value, VenueReasonCode.NON_CAUSAL_OBSERVATION.value,
    VenueReasonCode.SPREAD_UNAVAILABLE.value, VenueReasonCode.FINANCING_REQUIRED_UNAVAILABLE.value,
    VenueReasonCode.EXPIRY_WITHIN_HOLD.value,
})


# ---------------------------------------------------------------------------
# Component observations (native semantics)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FeeObservation:
    fee_model: str  # FeeModel
    source: str  # FeeSource
    maker_fee_rate: Optional[float] = None
    taker_fee_rate: Optional[float] = None
    #: per-contract charges, in ``commission_currency``; one "contract" is
    #: ``commission_contract_size`` venue quantity units (1 for futures, a lot for FX)
    commission_per_contract: Optional[float] = None
    exchange_fee_per_contract: Optional[float] = None
    clearing_fee_per_contract: Optional[float] = None
    commission_contract_size: float = 1.0
    commission_currency: Optional[str] = None
    #: quote-currency units per one ``commission_currency`` unit, when they differ
    commission_to_quote_rate: Optional[float] = None
    #: fraction of notional charged by the broker per side (FX commission accounts)
    broker_commission_rate: Optional[float] = None
    other_charge_per_trade: Optional[float] = None
    #: FX P&L conversion charge, fraction of notional (charged once)
    currency_conversion_fee_rate: Optional[float] = None
    fee_tier: Optional[str] = None
    observed_at: Optional[int] = None
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class SpreadObservation:
    source: str  # SpreadSource (the hierarchy level actually used)
    spread_absolute: Optional[float]
    spread_bps: Optional[float]
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    book_as_of: Optional[int] = None
    stale: bool = False
    fallback_level: int = 0
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class SlippageObservation:
    source: str  # SlippageSource
    #: ((price, quantity), ...) best-first, in venue quantity units
    depth_bids: Tuple[Tuple[float, float], ...] = ()
    depth_asks: Tuple[Tuple[float, float], ...] = ()
    depth_as_of: Optional[int] = None
    #: resolved per-side adverse slippage in bps when not walking depth
    per_side_bps: Optional[float] = None
    #: visible top-of-book quantities (used for the size-vs-depth check)
    top_bid_quantity: Optional[float] = None
    top_ask_quantity: Optional[float] = None
    liquidity_bucket: Optional[str] = None
    fallback_level: int = 0
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class FundingObservation:
    """Perpetual-futures funding, venue-native semantics (Section 17.7)."""

    applicable: bool
    source: str  # FundingSource
    current_funding_rate: Optional[float] = None
    predicted_funding_rate: Optional[float] = None
    predicted_as_of: Optional[int] = None
    last_funding_time: Optional[int] = None
    next_funding_time: Optional[int] = None
    funding_interval_ms: Optional[int] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    observed_at: Optional[int] = None
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class FinancingObservation:
    """FX swap / rollover / financing, broker-native semantics (Section 17.8).
    Sign convention is the broker's: positive = credited to the trader,
    negative = charged. Never re-expressed as a perpetual funding rate."""

    applicable: bool
    source: str  # FinancingSource
    swap_long: Optional[float] = None
    swap_short: Optional[float] = None
    swap_unit: Optional[str] = None  # SwapUnit
    point_size: Optional[float] = None
    rollover_hour_local: int = 17
    rollover_timezone: str = "America/New_York"
    #: weekday (Mon=0) whose rollover is charged three times, when the broker says so
    triple_swap_weekday: Optional[int] = None
    #: weekdays whose rollover is charged at all (default Mon-Fri)
    financing_days_of_week: Tuple[int, ...] = (0, 1, 2, 3, 4)
    observed_at: Optional[int] = None
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class CarryObservation:
    """Dated-futures basis / carry (Section 17.9). Holding economics, not an
    execution cost and not alpha."""

    applicable: bool
    source: str  # CarrySource
    expiry_ms: Optional[int] = None
    time_to_expiry_ms: Optional[int] = None
    reference_spot_price: Optional[float] = None
    futures_price: Optional[float] = None
    basis_absolute: Optional[float] = None
    observed_at: Optional[int] = None
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class InstrumentMetadata:
    """Venue precision/contract facts. ``contract_multiplier`` is the VENUE
    price multiplier (notional = price x quantity x multiplier): 1 for Binance
    USD-M (price is already per venue unit, even for 1000PEPE), 50 for ES."""

    venue_symbol: str
    canonical_symbol: str
    asset_class: str
    contract_type: str
    base_currency: str
    quote_currency: str
    settlement_currency: Optional[str]
    tick_size: float
    step_size: float
    minimum_quantity: float
    minimum_notional: Optional[float]
    contract_multiplier: float = 1.0
    tick_value: Optional[float] = None
    pip_size: Optional[float] = None
    lot_size: Optional[float] = None
    expiry_ms: Optional[int] = None
    source: str = ""
    as_of: Optional[int] = None
    version: str = VENUE_CAPABILITY_SCHEMA_VERSION


@dataclass(frozen=True)
class ExecutionCapabilities:
    """Section 17.11 -- information for TradePlan validation. Section 17
    never submits orders."""

    supported_order_types: Tuple[str, ...]
    supports_market: bool
    supports_limit: bool
    supports_stop: bool
    supports_stop_market: bool
    supports_post_only: bool
    supports_reduce_only: bool
    supports_partial_close: bool
    supports_native_oco: bool
    supports_hedge_mode: bool
    supports_one_way_mode: bool
    tick_size: float
    step_size: float
    minimum_quantity: float
    minimum_notional: Optional[float]
    contract_multiplier: float
    margin_modes: Tuple[str, ...]
    settlement_currency: Optional[str]
    supported_time_in_force: Tuple[str, ...] = ()
    venue_symbol: str = ""
    source: str = ""
    version: str = VENUE_CAPABILITY_SCHEMA_VERSION


@dataclass(frozen=True)
class SessionState:
    status: str  # SessionStatus
    source: str
    as_of: int
    reason_codes: Tuple[str, ...] = ()

    @property
    def tradable(self) -> bool:
        return self.status in (SessionStatus.ALWAYS_OPEN.value, SessionStatus.OPEN.value, SessionStatus.THIN.value)


# ---------------------------------------------------------------------------
# The observation
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class VenueEconomicObservation:
    """Everything one adapter observed for one instrument on one account at
    one decision instant. Side-independent: LONG and SHORT candidates of the
    same instrument share it; the cost model applies side."""

    observation_id: str
    observation_hash: str

    user_id: Optional[str]
    broker_account_id: Optional[str]
    bot_instance_id: Optional[str]
    run_id: Optional[str]
    cycle_id: Optional[str]

    broker: str
    venue: str
    environment: str  # VenueEnvironment
    instrument_key: InstrumentKey

    decision_time: int
    observed_at: int
    valid_until: int

    fee_observation: FeeObservation
    spread_observation: SpreadObservation
    slippage_observation: SlippageObservation
    funding_observation: FundingObservation
    financing_observation: FinancingObservation
    carry_observation: CarryObservation

    instrument_metadata: Optional[InstrumentMetadata]
    execution_capabilities: Optional[ExecutionCapabilities]
    session_state: SessionState

    adapter_id: str
    adapter_version: str
    adapter_status: str  # AdapterValidationStatus
    cost_policy_hash: str

    source_quality: str  # VALID | DEGRADED | INVALID
    feature_availability: Tuple[Tuple[str, bool], ...]
    reason_codes: Tuple[str, ...]

    schema_version: str = VENUE_ECONOMIC_OBSERVATION_SCHEMA_VERSION

    @property
    def fatal_reason_codes(self) -> Tuple[str, ...]:
        return tuple(r for r in self.reason_codes if r in FATAL_VENUE_REASONS)

    @property
    def tradable(self) -> bool:
        return self.session_state.tradable and not self.fatal_reason_codes

    @staticmethod
    def content_payload(**fields) -> dict:
        """The hashed content: every field except the id/hash themselves."""
        out = {}
        for key, value in fields.items():
            if key in ("observation_id", "observation_hash"):
                continue
            out[key] = asdict(value) if hasattr(value, "__dataclass_fields__") else value
        return out

    @classmethod
    def build(cls, **fields) -> "VenueEconomicObservation":
        digest = stable_hash(cls.content_payload(**fields))
        return cls(observation_id=short_id("vobs", digest), observation_hash=digest, **fields)


__all__ = [
    "AdapterValidationStatus", "VenueEnvironment", "FeeModel", "FeeSource", "SpreadSource", "SlippageSource",
    "FundingSource", "FinancingSource", "CarrySource", "SwapUnit", "SessionStatus", "VenueReasonCode",
    "FATAL_VENUE_REASONS", "FeeObservation", "SpreadObservation", "SlippageObservation", "FundingObservation",
    "FinancingObservation", "CarryObservation", "InstrumentMetadata", "ExecutionCapabilities", "SessionState",
    "VenueEconomicObservation",
]
