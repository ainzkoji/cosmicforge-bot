"""Section 19 contracts -- Position Intelligence & Adaptive Exit.

At entry CATI asked "is this trade worth opening?". Here it asks "given
everything that has happened since entry, is this trade still worth holding
NOW?" -- and it only ever answers with an INTENT. Nothing in this module (or
anything that produces these objects) places, cancels or replaces an order,
closes a broker position, changes quantity or leverage, widens a stop or
reserves margin. PositionManager, the executor and broker protection remain
the lifecycle authority.

R UNITS -- one basis, never mixed
---------------------------------
``original_R_reference`` is the TradePlan's ``initial_risk_distance`` (entry
to structural invalidation, in price units). It is kept for lineage and is
never silently redefined.

EVERY ``*_R`` quantity on PositionPathSnapshot / PositionForecast /
ExitDecision / ExitPolicy is expressed in ORIGINAL-R units
(``REMAINING_R_BASIS = "ORIGINAL_INITIAL_RISK"``): remaining gross/net EV,
remaining costs, penalties, the conservative remaining edge and every policy
floor. The CURRENT prospective risk (distance from the current price to the
current protective stop) is exposed separately as ``remaining_risk_distance``
and ``remaining_net_EV_per_remaining_risk`` for observability only -- no
decision rule compares a quantity in one basis against a floor in the other.

REMAINING EV is expected future PnL from NOW onward -- not the original EV
minus the current unrealized loss, and never "hold because this used to be a
good trade". Only FUTURE holding/exit costs are subtracted; costs already
paid are reported separately and never double counted.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import (
    EXIT_DECISION_SCHEMA_VERSION, EXIT_POLICY_SCHEMA_VERSION, POSITION_FORECAST_SCHEMA_VERSION,
    POSITION_PATH_SCHEMA_VERSION, REMAINING_COST_MODEL_VERSION,
)

#: The one R basis every remaining-economics quantity uses (see module docstring).
REMAINING_R_BASIS = "ORIGINAL_INITIAL_RISK"


class ThesisStatus(str, Enum):
    VALID = "VALID"
    WEAKENED = "WEAKENED"
    INVALIDATED = "INVALIDATED"
    UNKNOWN = "UNKNOWN"


class ConditionResult(str, Enum):
    PASS = "PASS"
    WEAKENED = "WEAKENED"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


class ConditionKind(str, Enum):
    #: the setup's own structural thesis (trend / breakout / range / momentum / HTF / regime)
    CORE = "CORE"
    #: event / liquidity / broker context -- a FAIL here is a mandatory de-risk, not a thesis break
    CONTEXT = "CONTEXT"
    #: the remaining economic edge -- judged by the edge floors, excluded from thesis aggregation
    ECONOMIC = "ECONOMIC"
    #: the plan's structural invalidation level itself
    STRUCTURAL = "STRUCTURAL"


class PositionConditionCode(str, Enum):
    """Position-time checks that are not TradePlan thesis codes."""

    STRUCTURAL_INVALIDATION_INTACT = "STRUCTURAL_INVALIDATION_INTACT"
    REGIME_SUPPORTS_THESIS = "REGIME_SUPPORTS_THESIS"
    NO_SHOCK_STATE = "NO_SHOCK_STATE"


class ProtectionState(str, Enum):
    PROTECTED = "PROTECTED"
    PARTIALLY_PROTECTED = "PARTIALLY_PROTECTED"
    UNPROTECTED = "UNPROTECTED"
    UNKNOWN = "UNKNOWN"


class ExitAction(str, Enum):
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    EXIT = "EXIT"
    TIGHTEN_PROTECTION = "TIGHTEN_PROTECTION"
    TAKE_PARTIAL = "TAKE_PARTIAL"
    NO_CHANGE_FALLBACK = "NO_CHANGE_FALLBACK"


#: Actions that would (in a FUTURE, explicitly enabled mode) need a broker action.
BROKER_ACTIONS = frozenset({ExitAction.REDUCE.value, ExitAction.EXIT.value, ExitAction.TIGHTEN_PROTECTION.value,
                            ExitAction.TAKE_PARTIAL.value})
FRACTION_ACTIONS = frozenset({ExitAction.REDUCE.value, ExitAction.TAKE_PARTIAL.value})


class DeRiskBehavior(str, Enum):
    EXIT = "EXIT"
    REDUCE = "REDUCE"
    NONE = "NONE"


class InsufficientSupportBehavior(str, Enum):
    #: existing mechanical protection continues untouched (default)
    NO_CHANGE_FALLBACK = "NO_CHANGE_FALLBACK"
    #: a WEAKENED thesis may still request a risk REDUCE without a forecast
    THESIS_BASED = "THESIS_BASED"


class PositionForecastStatus(str, Enum):
    VALID = "VALID"
    POSITION_FORECAST_SUPPORT_INSUFFICIENT = "POSITION_FORECAST_SUPPORT_INSUFFICIENT"
    OUTCOME_LIBRARY_UNAVAILABLE = "OUTCOME_LIBRARY_UNAVAILABLE"
    REMAINING_COST_UNAVAILABLE = "REMAINING_COST_UNAVAILABLE"
    INVALID_INPUT = "INVALID_INPUT"


class PositionReasonCode(str, Enum):
    POSITION_FORECAST_SUPPORT_INSUFFICIENT = "POSITION_FORECAST_SUPPORT_INSUFFICIENT"
    OUTCOME_LIBRARY_UNAVAILABLE = "OUTCOME_LIBRARY_UNAVAILABLE"
    LIBRARY_NOT_CALIBRATED = "LIBRARY_NOT_CALIBRATED"
    REMAINING_COST_UNAVAILABLE = "REMAINING_COST_UNAVAILABLE"
    MARKET_STATE_INVALID = "MARKET_STATE_INVALID"
    FORECAST_UNCERTAINTY_TOO_HIGH = "FORECAST_UNCERTAINTY_TOO_HIGH"
    OOD_TOO_HIGH = "OOD_TOO_HIGH"
    ESS_TOO_LOW = "ESS_TOO_LOW"
    PATH_CONDITIONED = "PATH_CONDITIONED"
    HIERARCHICAL_BACKOFF = "HIERARCHICAL_BACKOFF"
    THESIS_INVALIDATED = "THESIS_INVALIDATED"
    THESIS_WEAKENED = "THESIS_WEAKENED"
    THESIS_UNKNOWN = "THESIS_UNKNOWN"
    THESIS_CODE_NOT_EVALUABLE = "THESIS_CODE_NOT_EVALUABLE"
    CONTEXT_UNVERIFIED = "CONTEXT_UNVERIFIED"
    STRUCTURAL_INVALIDATION_BROKEN = "STRUCTURAL_INVALIDATION_BROKEN"
    EVENT_DE_RISK_REQUIRED = "EVENT_DE_RISK_REQUIRED"
    SHOCK_DE_RISK_REQUIRED = "SHOCK_DE_RISK_REQUIRED"
    LIQUIDITY_DE_RISK_REQUIRED = "LIQUIDITY_DE_RISK_REQUIRED"
    SYSTEM_DE_RISK_REQUIRED = "SYSTEM_DE_RISK_REQUIRED"
    REMAINING_EDGE_BELOW_EXIT_FLOOR = "REMAINING_EDGE_BELOW_EXIT_FLOOR"
    HOLDING_COSTS_DESTROY_EDGE = "HOLDING_COSTS_DESTROY_EDGE"
    REMAINING_EDGE_MARGINAL = "REMAINING_EDGE_MARGINAL"
    PROFIT_HARVEST = "PROFIT_HARVEST"
    TARGET_ZONE_REACHED = "TARGET_ZONE_REACHED"
    PROTECTION_TIGHTENED_STRUCTURAL = "PROTECTION_TIGHTENED_STRUCTURAL"
    PROTECTION_TIGHTENED_BREAK_EVEN = "PROTECTION_TIGHTENED_BREAK_EVEN"
    PROTECTION_WIDENING_REJECTED = "PROTECTION_WIDENING_REJECTED"
    PROTECTION_STATE_INVALID = "PROTECTION_STATE_INVALID"
    REMAINING_EDGE_ABOVE_HOLD_FLOOR = "REMAINING_EDGE_ABOVE_HOLD_FLOOR"
    NO_TRUSTWORTHY_INTENT = "NO_TRUSTWORTHY_INTENT"
    MAX_PARTIALS_REACHED = "MAX_PARTIALS_REACHED"
    COMPONENT_ERROR = "COMPONENT_ERROR"
    LEGACY_POSITION_NO_TRADE_PLAN = "LEGACY_POSITION_NO_TRADE_PLAN"
    LINEAGE_MISMATCH = "LINEAGE_MISMATCH"


def _plain(value):
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    if isinstance(value, (tuple, list)):
        return [_plain(v) for v in value]
    return value


def _hash_fields(fields: Mapping[str, object], non_analytical: Tuple[str, ...]) -> str:
    return stable_hash({k: _plain(v) for k, v in fields.items() if k not in non_analytical})


# ---------------------------------------------------------------------------
# 19.2 PositionPathSnapshot
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PositionPathSnapshot:
    """Immutable, causal view of one CATI-linked position's path up to
    ``current_time``. Built only from data timestamped at or before
    ``current_time`` (``position/path.py`` enforces this)."""

    position_path_id: str
    data_hash: str

    user_id: Optional[str]
    broker_account_id: str
    bot_instance_id: str

    position_id: str
    trade_plan_id: str

    instrument_key: object  # InstrumentKey
    venue: str
    environment: str
    side: str

    entry_time: int
    entry_price: float
    current_time: int
    current_price: float

    original_quantity: float
    current_quantity: float
    realized_quantity: float
    partial_exit_count: int

    realized_pnl: float
    unrealized_pnl: float

    mfe_price: float
    mae_price: float
    mfe_R: float  # >= 0, favorable excursion, original-R units
    mae_R: float  # >= 0, adverse excursion magnitude, original-R units
    current_R: float  # signed unrealized move per unit, original-R units

    elapsed_bars: int
    elapsed_seconds: int

    current_stop_price: Optional[float]
    current_target_prices: Tuple[float, ...]
    protection_state: str  # ProtectionState

    funding_paid_or_accrued: float
    financing_paid_or_accrued: float
    entry_fees_paid: float

    last_fill_time: Optional[int]

    original_R_reference: float
    remaining_risk_distance: Optional[float]
    timeframe: str
    contract_multiplier: float = 1.0
    snapshot_version: str = POSITION_PATH_SCHEMA_VERSION

    _NON_ANALYTICAL = ("position_path_id", "data_hash")

    @classmethod
    def build(cls, **fields) -> "PositionPathSnapshot":
        digest = _hash_fields(fields, cls._NON_ANALYTICAL)
        return cls(position_path_id=short_id("ppath", digest), data_hash=digest, **fields)

    @property
    def is_long(self) -> bool:
        return self.side == "LONG"


# ---------------------------------------------------------------------------
# 19.7 thesis condition result
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ThesisConditionResult:
    code: str
    result: str  # ConditionResult
    kind: str  # ConditionKind
    source: str  # "TRADE_PLAN" | "POSITION_POLICY"
    evidence: Tuple[Tuple[str, str], ...] = ()
    reason_codes: Tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# 19.9 remaining (from-NOW) holding + exit costs
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RemainingCostEstimate:
    """Section 17 venue economics evaluated from NOW for the REMAINING hold.

    ``expected_future_*`` are subtracted from remaining EV; ``costs_already_*``
    are sunk and reported only -- never subtracted again."""

    remaining_cost_id: str
    venue_observation_id: Optional[str]
    decision_time: int
    expected_remaining_holding_ms: int
    max_remaining_holding_ms: int

    exit_fee_R: float
    exit_spread_R: float
    exit_slippage_R: float
    future_funding_R: float
    future_financing_R: float
    future_carry_R: float
    expected_future_holding_and_exit_costs_R: float
    remaining_cost_uncertainty_R: float

    costs_already_realized_R: float
    entry_fees_paid_R: float
    funding_paid_R: float
    financing_paid_R: float

    source_quality: str  # VALID | DEGRADED | INVALID
    reason_codes: Tuple[str, ...] = ()
    cost_model_version: str = REMAINING_COST_MODEL_VERSION
    cost_policy_hash: str = ""

    @property
    def usable(self) -> bool:
        return self.source_quality != "INVALID"


# ---------------------------------------------------------------------------
# 19.4 PositionForecast
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PositionForecast:
    position_forecast_id: str
    forecast_hash: str

    position_id: str
    trade_plan_id: str
    market_state_id: Optional[str]
    regime_distribution_id: Optional[str]
    position_path_id: str

    user_id: Optional[str]
    broker_account_id: str
    bot_instance_id: str

    forecast_time: int
    status: str  # PositionForecastStatus
    thesis_status: str  # ThesisStatus

    remaining_horizon_bars: int
    remaining_horizon_ms: int

    p_positive_from_now: float
    p_target_from_now: float
    p_stop_from_now: float
    p_timeout_from_now: float

    remaining_gross_EV_R: float
    remaining_cost_R: float
    remaining_net_EV_R: float
    remaining_uncertainty_penalty_R: float
    remaining_ood_penalty_R: float
    remaining_execution_uncertainty_R: float
    conservative_remaining_edge_R: float

    remaining_MFE_R_quantiles: Mapping[str, float]
    remaining_MAE_R_quantiles: Mapping[str, float]
    time_to_target_remaining_quantiles: Mapping[str, float]
    time_to_stop_remaining_quantiles: Mapping[str, float]

    updated_support: int
    updated_ESS: float
    updated_backoff_level: int
    cohort_signature: str
    path_conditioning: Tuple[str, ...]

    current_state_uncertainty: float
    current_forecast_uncertainty: float
    current_OOD_score: float

    thesis_condition_results: Tuple[ThesisConditionResult, ...]
    remaining_costs: Optional[RemainingCostEstimate]
    costs_already_realized_R: float

    original_R_reference: float
    remaining_risk_distance: Optional[float]
    remaining_net_EV_per_remaining_risk: Optional[float]
    remaining_R_basis: str

    library_hash: str
    calibration_status: str
    reason_codes: Tuple[str, ...]

    forecast_policy_version: str
    forecast_policy_hash: str
    engine_version: str
    schema_version: str = POSITION_FORECAST_SCHEMA_VERSION
    #: SHADOW | REPLAY -- operational, excluded from analytical identity (replay parity)
    evaluation_mode: str = "SHADOW"

    _NON_ANALYTICAL = ("position_forecast_id", "forecast_hash", "evaluation_mode")

    @classmethod
    def build(cls, **fields) -> "PositionForecast":
        digest = _hash_fields(fields, cls._NON_ANALYTICAL)
        return cls(position_forecast_id=short_id("pfc", digest), forecast_hash=digest, **fields)

    @property
    def trustworthy(self) -> bool:
        return self.status == PositionForecastStatus.VALID.value


# ---------------------------------------------------------------------------
# 19.17 ExitPolicy
# ---------------------------------------------------------------------------
def _in_open_unit(x: float) -> bool:
    return 0.0 < float(x) < 1.0


@dataclass(frozen=True)
class ExitPolicy:
    """RESEARCH DEFAULTS, not replay-calibrated production thresholds. No
    decision constant lives outside this policy. Every *_R value is in
    ORIGINAL-R units (``REMAINING_R_BASIS``).

    P8: there is deliberately NO account-balance / "recover previous
    balance" input anywhere in this policy or the engine that reads it."""

    schema_version: str = EXIT_POLICY_SCHEMA_VERSION

    # -- remaining-edge floors -------------------------------------------------------
    hold_edge_floor: float = 0.10
    exit_edge_floor: float = -0.05
    partial_edge_band: float = 0.25

    # -- evidence quality -------------------------------------------------------------
    minimum_support: int = 15
    minimum_ESS: float = 10.0
    maximum_forecast_uncertainty: float = 0.65
    maximum_OOD: float = 0.60
    require_calibrated_library: bool = True
    conditioned_forecast_required_for_adaptive_actions: bool = True
    insufficient_support_behavior: str = InsufficientSupportBehavior.NO_CHANGE_FALLBACK.value
    prior_strength: float = 10.0
    credible_interval_level: float = 0.90
    uncertainty_penalty_coefficient: float = 1.0
    distribution_shift_penalty_coefficient: float = 1.0
    execution_uncertainty_penalty_coefficient: float = 1.0

    # -- path conditioning ---------------------------------------------------------------
    mfe_bucket_edges_R: Tuple[float, ...] = (0.5, 1.0, 2.0)
    mae_bucket_edges_R: Tuple[float, ...] = (0.25, 0.5, 0.75)
    max_holding_bars: int = 48

    # -- partials / reduce ------------------------------------------------------------------
    minimum_elapsed_bars_before_partial: int = 2
    partial_min_progress_R: float = 0.75
    maximum_partial_fraction: float = 0.50
    take_partial_fraction: float = 0.33
    default_reduce_fraction: float = 0.50
    max_partial_exits: int = 2

    # -- protection -------------------------------------------------------------------------
    tighten_protection_min_improvement_R: float = 0.10
    tighten_min_distance_from_price_R: float = 0.20
    break_even_trigger_R: float = 1.0

    # -- de-risk ------------------------------------------------------------------------------
    event_de_risk_behavior: str = DeRiskBehavior.REDUCE.value
    shock_de_risk_behavior: str = DeRiskBehavior.EXIT.value
    liquidity_de_risk_behavior: str = DeRiskBehavior.REDUCE.value
    event_lookahead_ms: int = 30 * 60_000
    #: a context condition that cannot be verified (e.g. a stale calendar) makes the thesis at most WEAKENED
    context_unknown_thesis_status: str = ThesisStatus.WEAKENED.value

    # -- thesis thresholds --------------------------------------------------------------------
    trend_min_strength: float = 0.35
    trend_reversal_strength: float = 0.50
    regime_against_weight: float = 0.50
    shock_regime_weight: float = 0.50
    liquidity_weak_spread_percentile: float = 0.70
    liquidity_degraded_spread_percentile: float = 0.90
    momentum_min_relative_volume: float = 0.80
    momentum_exhaustion_proxy_max: float = 0.80
    breakout_failure_tolerance_R: float = 0.25

    def __post_init__(self) -> None:
        for name in ("default_reduce_fraction", "take_partial_fraction", "maximum_partial_fraction"):
            if not _in_open_unit(getattr(self, name)):
                raise ValueError(f"ExitPolicy.{name} must satisfy 0 < fraction < 1")
        if self.take_partial_fraction > self.maximum_partial_fraction:
            raise ValueError("ExitPolicy.take_partial_fraction exceeds maximum_partial_fraction")
        if self.exit_edge_floor > self.hold_edge_floor:
            raise ValueError("ExitPolicy.exit_edge_floor must not exceed hold_edge_floor")
        if not self.conditioned_forecast_required_for_adaptive_actions:
            # V1 has no non-conditioned adaptive path: adaptive actions ALWAYS need the conditioned forecast
            raise ValueError("ExitPolicy V1 requires conditioned_forecast_required_for_adaptive_actions=True")
        if self.tighten_protection_min_improvement_R < 0:
            raise ValueError("ExitPolicy.tighten_protection_min_improvement_R must be >= 0")
        for name in ("event_de_risk_behavior", "shock_de_risk_behavior", "liquidity_de_risk_behavior"):
            if getattr(self, name) not in {b.value for b in DeRiskBehavior}:
                raise ValueError(f"ExitPolicy.{name} must be one of {[b.value for b in DeRiskBehavior]}")

    @property
    def policy_hash(self) -> str:
        return stable_hash(asdict(self))


# ---------------------------------------------------------------------------
# 19.10 ExitDecision
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ExitDecision:
    """ONE canonical advisory action per evaluation. An INTENT only."""

    exit_decision_id: str
    decision_hash: str

    position_forecast_id: str
    position_id: str
    trade_plan_id: str
    position_path_id: str

    user_id: Optional[str]
    broker_account_id: str
    bot_instance_id: str

    action: str  # ExitAction
    requested_fraction: Optional[float]
    suggested_protection_price: Optional[float]
    existing_protection_price: Optional[float]
    side: str

    decision_time: int
    conservative_remaining_edge_R: float
    thesis_status: str
    reason_codes: Tuple[str, ...]

    policy_version: str
    policy_hash: str
    engine_version: str
    lineage: Tuple[Tuple[str, str], ...] = ()
    schema_version: str = EXIT_DECISION_SCHEMA_VERSION
    evaluation_mode: str = "SHADOW"

    _NON_ANALYTICAL = ("exit_decision_id", "decision_hash", "evaluation_mode")

    def __post_init__(self) -> None:
        if self.action not in {a.value for a in ExitAction}:
            raise ValueError(f"unknown exit action {self.action!r}")
        if self.action in FRACTION_ACTIONS:
            if self.requested_fraction is None or not _in_open_unit(self.requested_fraction):
                raise ValueError(f"{self.action} requires 0 < requested_fraction < 1")
        elif self.requested_fraction is not None:
            raise ValueError(f"{self.action} carries no requested_fraction")
        if self.action == ExitAction.TIGHTEN_PROTECTION.value:
            if self.suggested_protection_price is None or self.existing_protection_price is None:
                raise ValueError("TIGHTEN_PROTECTION requires suggested and existing protection prices")
            if not protection_is_tighter(self.side, self.existing_protection_price, self.suggested_protection_price):
                raise ValueError("TIGHTEN_PROTECTION may never widen risk beyond existing protection")
        elif self.suggested_protection_price is not None:
            raise ValueError(f"{self.action} carries no suggested_protection_price")

    @classmethod
    def build(cls, **fields) -> "ExitDecision":
        digest = _hash_fields(fields, cls._NON_ANALYTICAL)
        return cls(exit_decision_id=short_id("exd", digest), decision_hash=digest, **fields)

    @property
    def requires_broker_action(self) -> bool:
        """True only for actions a FUTURE enabled mode would route through
        PositionManager/executor. HOLD / NO_CHANGE_FALLBACK never do."""
        return self.action in BROKER_ACTIONS


def protection_is_tighter(side: str, existing_stop: float, new_stop: float) -> bool:
    """P7 NO STOP WIDENING. LONG: new >= existing. SHORT: new <= existing.
    Strictly tighter is required for a TIGHTEN (an equal stop is no change)."""
    if side == "LONG":
        return float(new_stop) > float(existing_stop)
    if side == "SHORT":
        return float(new_stop) < float(existing_stop)
    return False


__all__ = [
    "REMAINING_R_BASIS", "ThesisStatus", "ConditionResult", "ConditionKind", "PositionConditionCode", "ProtectionState",
    "ExitAction", "BROKER_ACTIONS", "FRACTION_ACTIONS", "DeRiskBehavior", "InsufficientSupportBehavior",
    "PositionForecastStatus", "PositionReasonCode", "PositionPathSnapshot", "ThesisConditionResult",
    "RemainingCostEstimate", "PositionForecast", "ExitPolicy", "ExitDecision", "protection_is_tighter",
]
