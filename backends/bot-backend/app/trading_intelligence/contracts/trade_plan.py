"""Section 18 contracts -- TradePlan, its policy, and the typed build /
pre-submission outcomes.

A TradePlan is an immutable, time-bounded execution INTENT for ONE
portfolio-selected, reservation-protected CATI opportunity. It is NOT an
order and NOT final sizing: it carries no order quantity, leverage, margin
or account risk amount. ``economic_size_assumption`` is only the notional the
Section 17 cost estimate assumed. The future hard-risk / sizing / slot /
execution path (Section 20) remains the authority and may still reject it;
CATI can never widen hard risk.

Any change to entry zone, invalidation, targets, costs, forecast, economics,
market state or portfolio selection is a NEW plan with a new id and hash --
a plan is never updated in place. Operational status (e.g. consumed,
expired) lives outside the analytical plan.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import CRYPTO, FUTURES, FX, InstrumentKey
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import TRADE_PLAN_POLICY_SCHEMA_VERSION, TRADE_PLAN_SCHEMA_VERSION


class TradePlanBuildStatus(str, Enum):
    PLAN_CREATED = "PLAN_CREATED"
    PLAN_NOT_CREATED = "PLAN_NOT_CREATED"
    INVALID_INPUT = "INVALID_INPUT"
    EXPIRED = "EXPIRED"
    RESERVATION_INVALID = "RESERVATION_INVALID"
    VETO_NOT_APPROVED = "VETO_NOT_APPROVED"
    RANKING_INCOMPLETE = "RANKING_INCOMPLETE"
    PORTFOLIO_NOT_SELECTED = "PORTFOLIO_NOT_SELECTED"
    ECONOMICS_STALE = "ECONOMICS_STALE"


class SubmissionValidity(str, Enum):
    VALID = "VALID"
    STALE = "STALE"
    EXPIRED = "EXPIRED"
    ENTRY_ZONE_VIOLATION = "ENTRY_ZONE_VIOLATION"
    BROKER_DEGRADED = "BROKER_DEGRADED"
    RESERVATION_LOST = "RESERVATION_LOST"
    UNSUPPORTED_EXECUTION_PREFERENCE = "UNSUPPORTED_EXECUTION_PREFERENCE"
    INVALID_INSTRUMENT_METADATA = "INVALID_INSTRUMENT_METADATA"


class OrderStyle(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    POST_ONLY_PREFERRED = "POST_ONLY_PREFERRED"
    PASSIVE_LIMIT = "PASSIVE_LIMIT"
    AGGRESSIVE_LIMIT = "AGGRESSIVE_LIMIT"


class Urgency(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"


class FillPolicy(str, Enum):
    ALLOW_PARTIAL_FILL = "ALLOW_PARTIAL_FILL"
    REQUIRE_FULL_FILL = "REQUIRE_FULL_FILL"


class TargetPurpose(str, Enum):
    PRIMARY = "PRIMARY"
    SECONDARY = "SECONDARY"
    STRUCTURAL = "STRUCTURAL"
    FORECAST_QUANTILE = "FORECAST_QUANTILE"


class ThesisCode(str, Enum):
    TREND_CONTINUATION_REMAINS_VALID = "TREND_CONTINUATION_REMAINS_VALID"
    BREAKOUT_HOLDS_ABOVE_BOUNDARY = "BREAKOUT_HOLDS_ABOVE_BOUNDARY"
    BREAKOUT_HOLDS_BELOW_BOUNDARY = "BREAKOUT_HOLDS_BELOW_BOUNDARY"
    RANGE_BOUNDARY_REMAINS_INTACT = "RANGE_BOUNDARY_REMAINS_INTACT"
    MOMENTUM_PARTICIPATION_PRESENT = "MOMENTUM_PARTICIPATION_PRESENT"
    HTF_ALIGNMENT_VALID = "HTF_ALIGNMENT_VALID"
    EVENT_CONTEXT_ACCEPTABLE = "EVENT_CONTEXT_ACCEPTABLE"
    LIQUIDITY_NOT_DEGRADED = "LIQUIDITY_NOT_DEGRADED"
    BROKER_HEALTH_ACCEPTABLE = "BROKER_HEALTH_ACCEPTABLE"
    ECONOMIC_EDGE_POSITIVE = "ECONOMIC_EDGE_POSITIVE"


class InvalidationCode(str, Enum):
    STRUCTURAL_LEVEL_BROKEN = "STRUCTURAL_LEVEL_BROKEN"
    ENTRY_ZONE_EXPIRED = "ENTRY_ZONE_EXPIRED"
    PLAN_EXPIRED = "PLAN_EXPIRED"
    REGIME_SHIFTED_TO_SHOCK = "REGIME_SHIFTED_TO_SHOCK"
    EVENT_VETO_BECAME_ACTIVE = "EVENT_VETO_BECAME_ACTIVE"
    BROKER_HEALTH_DEGRADED = "BROKER_HEALTH_DEGRADED"
    SPREAD_EXCEEDED_BUDGET = "SPREAD_EXCEEDED_BUDGET"
    ECONOMIC_EDGE_INVALIDATED = "ECONOMIC_EDGE_INVALIDATED"
    PORTFOLIO_RESERVATION_LOST = "PORTFOLIO_RESERVATION_LOST"


def _d(**kw):
    return field(default_factory=lambda: dict(kw))


def _plain(value):
    """Explicit JSON-able structure for hashing (dataclasses and nested tuples)."""
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    if isinstance(value, (tuple, list)):
        return [_plain(v) for v in value]
    return value


@dataclass(frozen=True)
class TradePlanPolicy:
    """RESEARCH DEFAULTS -- no constant lives in the builder."""

    schema_version: str = TRADE_PLAN_POLICY_SCHEMA_VERSION

    #: a plan never outlives this many bars after the decision (nor the candidate)
    max_plan_age_bars: int = 3
    #: how far price may move AGAINST the entry before the plan is stale
    max_entry_extension_R: float = 0.25
    #: how far price may move toward the invalidation before the thesis is suspect
    max_entry_improvement_R: float = 0.50
    default_max_slippage_bps: float = 10.0
    #: asset class -> ordered style preferences; the first the VENUE declares wins
    order_preferences_by_asset_class: Mapping[str, Tuple[str, ...]] = _d(**{
        CRYPTO: ("AGGRESSIVE_LIMIT", "LIMIT", "MARKET"), FX: ("LIMIT", "MARKET"), FUTURES: ("LIMIT", "MARKET")})
    time_in_force_by_asset_class: Mapping[str, Tuple[str, ...]] = _d(**{
        CRYPTO: ("GTD", "GTC"), FX: ("GTD", "GTC"), FUTURES: ("DAY", "GTC")})
    default_urgency: str = Urgency.NORMAL.value
    default_fill_policy: str = FillPolicy.ALLOW_PARTIAL_FILL.value
    require_portfolio_reservation: bool = True
    minimum_reservation_ttl_ms: int = 30_000
    target_zone_half_width_R: float = 0.10
    include_forecast_quantile_target: bool = True
    forecast_target_quantile: str = "p50"  # MFE quantile key
    min_forecast_target_R: float = 0.5
    max_spread_budget_bps: float = 15.0
    min_conservative_edge_R: float = 0.0
    max_market_reference_age_ms: int = 5_000

    @property
    def policy_hash(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True)
class TargetZone:
    zone_id: str
    price_low: float
    price_high: float
    reference_R: float
    purpose: str  # TargetPurpose


@dataclass(frozen=True)
class AllowedEntryZone:
    reference_price: float
    minimum_price: float
    maximum_price: float
    maximum_adverse_slippage_bps: float
    maximum_extension_R: float
    valid_until: int


@dataclass(frozen=True)
class ThesisCondition:
    code: str  # ThesisCode
    evidence: Tuple[Tuple[str, str], ...] = ()


@dataclass(frozen=True)
class InvalidationCondition:
    code: str  # InvalidationCode
    parameters: Tuple[Tuple[str, str], ...] = ()


@dataclass(frozen=True)
class ExecutionPreferences:
    """INTENT only. The executor validates venue support; CATI never assumes
    one venue supports what another does."""

    preferred_order_style: str  # OrderStyle
    fallback_order_styles: Tuple[str, ...]
    maximum_slippage_bps: float
    urgency: str  # Urgency
    fill_policy: str  # FillPolicy
    time_in_force: str
    max_spread_bps: float
    market_reference_max_age_ms: int


@dataclass(frozen=True)
class ExpectedCosts:
    cost_estimate_id: str
    venue_observation_id: Optional[str]
    fee_R: float
    spread_R: float
    slippage_R: float
    funding_R: float
    carry_R: float
    total_cost_R: float
    cost_uncertainty_R: float
    cost_scope: str
    adapter_status: Optional[str]
    source_quality: str


@dataclass(frozen=True)
class TradePlan:
    trade_plan_id: str
    trade_plan_hash: str

    # -- lineage -------------------------------------------------------------------
    snapshot_id: str
    market_state_id: str
    regime_distribution_id: str
    source_candidate_id: str
    forecast_id: str
    venue_observation_id: str
    cost_estimate_id: str
    economic_opportunity_id: str
    veto_decision_id: str
    ranking_batch_id: str
    ranked_opportunity_id: str
    portfolio_decision_id: str
    portfolio_reservation_id: str

    # -- tenant ---------------------------------------------------------------------
    user_id: Optional[str]
    broker_account_id: str
    bot_instance_id: str
    run_id: Optional[str]
    cycle_id: str

    # -- instrument -----------------------------------------------------------------
    instrument_key: InstrumentKey
    venue: str
    environment: str
    side: str
    setup_family: str
    setup_version: str

    # -- geometry / time --------------------------------------------------------------
    decision_time: int
    entry_reference: float
    allowed_entry_zone: AllowedEntryZone
    structural_invalidation_price: float
    initial_risk_distance: float
    target_zones: Tuple[TargetZone, ...]
    expected_holding_time_ms: Optional[int]
    plan_expiry_time: int

    # -- economics (copied from Sections 12-13 evidence, never recomputed) -------------
    expected_gross_R: float
    expected_net_R: float
    conservative_edge_R: float
    p_net_profitable: float
    credible_interval_low: float
    credible_interval_high: float
    raw_support: int
    ess: float
    backoff_level: int
    expected_costs: ExpectedCosts
    #: the notional Section 17 assumed for cost estimation -- NOT a quantity
    economic_size_assumption: Optional[float]

    execution_preferences: ExecutionPreferences
    thesis_conditions: Tuple[ThesisCondition, ...]
    invalidation_conditions: Tuple[InvalidationCondition, ...]
    reason_codes: Tuple[str, ...]

    # -- versions / hashes ----------------------------------------------------------------
    versions: Tuple[Tuple[str, str], ...]
    mode: str = "SHADOW"
    schema_version: str = TRADE_PLAN_SCHEMA_VERSION
    #: operational timestamp -- excluded from the analytical hash
    plan_created_at: int = 0

    #: fields that are NOT analytical content (never hashed)
    _NON_ANALYTICAL = ("trade_plan_id", "trade_plan_hash", "plan_created_at")

    @classmethod
    def content_hash(cls, fields: Mapping[str, object]) -> str:
        return stable_hash({k: _plain(v) for k, v in fields.items() if k not in cls._NON_ANALYTICAL})

    @classmethod
    def build(cls, **fields) -> "TradePlan":
        digest = cls.content_hash(fields)
        return cls(trade_plan_id=short_id("tplan", digest), trade_plan_hash=digest, **fields)

    @property
    def lineage(self) -> Mapping[str, str]:
        """The reconstructable chain snapshot -> ... -> trade_plan (Section 18.18)."""
        return {
            "snapshot": self.snapshot_id, "market_state": self.market_state_id,
            "regime_distribution": self.regime_distribution_id, "setup_candidate": self.source_candidate_id,
            "outcome_forecast": self.forecast_id, "venue_economic_observation": self.venue_observation_id,
            "cost_estimate": self.cost_estimate_id, "economic_opportunity": self.economic_opportunity_id,
            "veto_decision": self.veto_decision_id, "ranking_batch": self.ranking_batch_id,
            "ranked_opportunity": self.ranked_opportunity_id, "portfolio_decision": self.portfolio_decision_id,
            "portfolio_reservation": self.portfolio_reservation_id, "trade_plan": self.trade_plan_id,
        }


@dataclass(frozen=True)
class TradePlanResult:
    status: str  # TradePlanBuildStatus
    plan: Optional[TradePlan] = None
    reason_codes: Tuple[str, ...] = ()
    detail: str = ""

    @property
    def created(self) -> bool:
        return self.status == TradePlanBuildStatus.PLAN_CREATED.value and self.plan is not None


@dataclass(frozen=True)
class SubmissionValidationResult:
    status: str  # SubmissionValidity
    reason_codes: Tuple[str, ...]
    trade_plan_id: str
    checked_at: int

    @property
    def valid(self) -> bool:
        return self.status == SubmissionValidity.VALID.value


__all__ = [
    "TradePlanBuildStatus", "SubmissionValidity", "OrderStyle", "Urgency", "FillPolicy", "TargetPurpose", "ThesisCode",
    "InvalidationCode", "TradePlanPolicy", "TargetZone", "AllowedEntryZone", "ThesisCondition", "InvalidationCondition",
    "ExecutionPreferences", "ExpectedCosts", "TradePlan", "TradePlanResult", "SubmissionValidationResult",
]
