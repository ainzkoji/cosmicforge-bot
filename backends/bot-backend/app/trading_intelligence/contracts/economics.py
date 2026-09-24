"""Section 13 contracts -- CostEstimate, AdmissionPolicy, AdmissionGateResult,
EconomicOpportunity.

Naming is deliberate: ``admission_status`` is never "TRADE_APPROVED".
``ECONOMICALLY_ADMISSIBLE`` answers one narrow question -- is this candidate
economically admissible given historical evidence, uncertainty, costs and
geometry -- not "submit this order". Veto (14), ranking (15), portfolio
selection (16), hard risk and execution all still sit downstream and are
NOT implemented here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import (
    ADMISSION_POLICY_SCHEMA_VERSION,
    COST_ESTIMATE_SCHEMA_VERSION,
    ECONOMIC_OPPORTUNITY_SCHEMA_VERSION,
)


class CostScope(str, Enum):
    REFERENCE_RESEARCH = "REFERENCE_RESEARCH"
    VENUE = "VENUE"
    ACCOUNT = "ACCOUNT"


class AdmissionStatus(str, Enum):
    ECONOMICALLY_ADMISSIBLE = "ECONOMICALLY_ADMISSIBLE"
    ECONOMICALLY_INADMISSIBLE = "ECONOMICALLY_INADMISSIBLE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class AdmissionGate(str, Enum):
    SUPPORT = "SUPPORT"
    PROBABILITY_UNCERTAINTY = "PROBABILITY_UNCERTAINTY"
    NET_EDGE = "NET_EDGE"
    CONSERVATIVE_EDGE = "CONSERVATIVE_EDGE"
    REWARD_GEOMETRY = "REWARD_GEOMETRY"
    COST_SHARE = "COST_SHARE"
    DISTRIBUTION_SHIFT = "DISTRIBUTION_SHIFT"
    DATA_QUALITY = "DATA_QUALITY"
    STATE_UNCERTAINTY = "STATE_UNCERTAINTY"
    FORECAST_UNCERTAINTY = "FORECAST_UNCERTAINTY"
    COST_QUALITY = "COST_QUALITY"


class EconomicsReasonCode(str, Enum):
    INSUFFICIENT_SUPPORT = "INSUFFICIENT_SUPPORT"
    INSUFFICIENT_ESS = "INSUFFICIENT_ESS"
    CREDIBLE_INTERVAL_TOO_WIDE = "CREDIBLE_INTERVAL_TOO_WIDE"
    NEGATIVE_GROSS_EV = "NEGATIVE_GROSS_EV"
    NET_EDGE_BELOW_FLOOR = "NET_EDGE_BELOW_FLOOR"
    CONSERVATIVE_EDGE_BELOW_FLOOR = "CONSERVATIVE_EDGE_BELOW_FLOOR"
    INVALID_GEOMETRY = "INVALID_GEOMETRY"
    INSUFFICIENT_TARGET_ROOM = "INSUFFICIENT_TARGET_ROOM"
    COST_SHARE_TOO_HIGH = "COST_SHARE_TOO_HIGH"
    COST_NOT_VIABLE = "COST_NOT_VIABLE"
    DISTRIBUTION_SHIFT_TOO_HIGH = "DISTRIBUTION_SHIFT_TOO_HIGH"
    DATA_QUALITY_UNACCEPTABLE = "DATA_QUALITY_UNACCEPTABLE"
    STATE_UNCERTAINTY_TOO_HIGH = "STATE_UNCERTAINTY_TOO_HIGH"
    FORECAST_UNCERTAINTY_TOO_HIGH = "FORECAST_UNCERTAINTY_TOO_HIGH"
    COST_UNBOUNDED = "COST_UNBOUNDED"
    FORECAST_UNAVAILABLE = "FORECAST_UNAVAILABLE"
    COST_ESTIMATE_UNAVAILABLE = "COST_ESTIMATE_UNAVAILABLE"


@dataclass(frozen=True)
class ComponentLineage:
    """Where one cost component came from (Section 17.16) -- kept so future
    calibration can compare planned against realized execution cost."""

    component: str  # FEE | SPREAD | SLIPPAGE | FUNDING | FINANCING | CARRY
    source: str  # e.g. OBSERVED_ACCOUNT_TIER, LIVE_TOP_OF_BOOK, DEPTH_WALK
    observed_at: Optional[int]
    quality: str  # VALID | FALLBACK | NOT_APPLICABLE | UNAVAILABLE
    fallback_level: int  # 0 = primary source; higher = further down the hierarchy
    version: str


@dataclass(frozen=True)
class CostUncertaintyBreakdown:
    """Section 17.15: execution uncertainty by driver, never one opaque number.
    ``adapter_uncertainty_R`` also carries broker-degradation uncertainty."""

    fee_uncertainty_R: float = 0.0
    spread_uncertainty_R: float = 0.0
    slippage_uncertainty_R: float = 0.0
    funding_uncertainty_R: float = 0.0
    carry_uncertainty_R: float = 0.0
    adapter_uncertainty_R: float = 0.0

    @property
    def total(self) -> float:
        return (self.fee_uncertainty_R + self.spread_uncertainty_R + self.slippage_uncertainty_R
                + self.funding_uncertainty_R + self.carry_uncertainty_R + self.adapter_uncertainty_R)


@dataclass(frozen=True)
class CostEstimate:
    cost_estimate_id: str
    instrument_key: InstrumentKey
    venue: str
    cost_scope: str  # CostScope

    fee_R: float
    spread_R: float
    slippage_R: float
    #: Perpetual-futures funding ONLY (Section 17.7).
    funding_R: float
    #: Holding carry: FX swap/rollover financing and dated-futures basis
    #: convergence (Sections 17.8-17.9). Native semantics stay in the
    #: VenueEconomicObservation -- never mapped into a fake funding rate.
    carry_R: float
    total_cost_R: float
    cost_uncertainty_R: float

    cost_model_version: str
    cost_policy_hash: str
    source_quality: str  # DataQualityLevel-style string
    reason_codes: Tuple[str, ...] = ()

    #: Only populated when cost_scope == ACCOUNT (Section 13.1 / 17.14).
    #: Market/reference-scoped estimates must never fabricate tenant identity.
    user_id: Optional[str] = None
    broker_account_id: Optional[str] = None
    bot_instance_id: Optional[str] = None
    run_id: Optional[str] = None
    cycle_id: Optional[str] = None

    # -- Section 17 venue evidence (all optional: the Section 13 reference
    #    estimate in economics/costs.py leaves them unset) -------------------
    environment: Optional[str] = None
    venue_observation_id: Optional[str] = None
    adapter_status: Optional[str] = None
    decision_time: Optional[int] = None
    expected_holding_ms: Optional[int] = None
    uncertainty_breakdown: Optional[CostUncertaintyBreakdown] = None
    component_lineage: Tuple[ComponentLineage, ...] = ()
    #: Raw/native amounts (quote currency, rates, counts) behind the R values.
    native_costs: Mapping[str, float] = field(default_factory=dict)
    #: The economic size the estimate assumed (quote-currency notional). An
    #: estimation assumption only -- never a final order quantity (17.6).
    reference_notional: Optional[float] = None
    #: (notional, total_cost_R) at increasing sizes; non-decreasing in cost.
    marginal_cost_curve: Tuple[Tuple[float, float], ...] = ()

    schema_version: str = COST_ESTIMATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.cost_scope == CostScope.ACCOUNT.value:
            if not (self.user_id and self.broker_account_id):
                raise ValueError("ACCOUNT-scoped CostEstimate requires user_id and broker_account_id")
        else:
            if self.user_id or self.broker_account_id or self.bot_instance_id or self.run_id or self.cycle_id:
                raise ValueError(f"{self.cost_scope} cost estimate must not carry tenant identity")

    @staticmethod
    def build_id(*, instrument_key: InstrumentKey, venue: str, cost_scope: str, cost_policy_hash: str, decision_time: int,
                 observation_hash: Optional[str] = None, broker_account_id: Optional[str] = None,
                 setup_candidate_id: Optional[str] = None, sizing_context_hash: Optional[str] = None) -> str:
        payload = {
            "canonical_symbol": instrument_key.canonical_symbol, "venue": venue,
            "cost_scope": cost_scope, "cost_policy_hash": cost_policy_hash, "decision_time": decision_time,
        }
        # Section 17 estimates are identified by their evidence and account too;
        # omitted when unset so every pre-Section-17 id is unchanged.
        if observation_hash is not None:
            payload["observation_hash"] = observation_hash
        if broker_account_id is not None:
            payload["broker_account_id"] = broker_account_id
        # a venue estimate is side/size/holding specific: one per candidate
        if setup_candidate_id is not None:
            payload["setup_candidate_id"] = setup_candidate_id
        if sizing_context_hash is not None:
            payload["sizing_context_hash"] = sizing_context_hash
        return short_id("cost", payload)


@dataclass(frozen=True)
class AdmissionGateResult:
    gate: str  # AdmissionGate
    passed: bool
    observed_value: Optional[float]
    required_value: Optional[float]
    reason_code: str


@dataclass(frozen=True)
class AdmissionPolicy:
    """RESEARCH-CALIBRATED configuration (Section 13.4) -- explicitly not
    yet validated by replay. Unrelated to V2's adaptive entry threshold:
    never derive one from the other (Section 13.16)."""

    schema_version: str = ADMISSION_POLICY_SCHEMA_VERSION

    minimum_raw_support: int = 15
    minimum_ess: float = 10.0
    maximum_credible_interval_width: float = 0.45

    minimum_ev_net_r: float = 0.0
    minimum_conservative_edge_r: float = -0.05

    minimum_room_to_target_r: float = 0.5
    maximum_cost_share: float = 0.5

    maximum_ood_score: float = 0.75
    maximum_state_uncertainty: float = 0.6
    maximum_forecast_uncertainty: float = 0.7

    allowed_data_quality: Tuple[str, ...] = ("VALID", "DEGRADED")

    #: Penalty coefficients (Section 13.3) -- every penalty is
    #: coefficient * driver, individually visible in EconomicOpportunity.
    #: Never inline constants scattered through economics/engine.py.
    uncertainty_penalty_coefficient: float = 1.0
    distribution_shift_penalty_coefficient: float = 1.0
    execution_uncertainty_penalty_coefficient: float = 1.0

    #: Per-setup-family overrides -- keys are setup_family, values are dicts
    #: of the same field names above, applied on top of these defaults.
    family_overrides: Mapping[str, Mapping[str, float]] = field(default_factory=dict)

    @property
    def policy_hash(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "minimum_raw_support": self.minimum_raw_support,
            "minimum_ess": self.minimum_ess,
            "maximum_credible_interval_width": self.maximum_credible_interval_width,
            "minimum_ev_net_r": self.minimum_ev_net_r,
            "minimum_conservative_edge_r": self.minimum_conservative_edge_r,
            "minimum_room_to_target_r": self.minimum_room_to_target_r,
            "maximum_cost_share": self.maximum_cost_share,
            "maximum_ood_score": self.maximum_ood_score,
            "maximum_state_uncertainty": self.maximum_state_uncertainty,
            "maximum_forecast_uncertainty": self.maximum_forecast_uncertainty,
            "allowed_data_quality": list(self.allowed_data_quality),
            "uncertainty_penalty_coefficient": self.uncertainty_penalty_coefficient,
            "distribution_shift_penalty_coefficient": self.distribution_shift_penalty_coefficient,
            "execution_uncertainty_penalty_coefficient": self.execution_uncertainty_penalty_coefficient,
            "family_overrides": {k: dict(v) for k, v in self.family_overrides.items()},
        }
        return stable_hash(payload)

    def for_family(self, setup_family: str) -> "AdmissionPolicy":
        overrides = self.family_overrides.get(setup_family)
        if not overrides:
            return self
        from dataclasses import replace

        return replace(self, **overrides)


@dataclass(frozen=True)
class EconomicOpportunity:
    economic_opportunity_id: str

    setup_candidate_id: str
    forecast_id: str
    cost_estimate_id: str

    instrument_key: InstrumentKey
    side: str
    decision_time: int
    setup_family: str

    p_net_profitable_mean: float
    credible_interval_low: float
    credible_interval_high: float

    p_target_before_stop: float
    p_stop_before_target: float
    p_timeout: float

    ev_gross_r: float

    fee_R: float
    spread_R: float
    slippage_R: float
    funding_R: float
    carry_R: float
    cost_r: float

    ev_net_r: float

    uncertainty_penalty_r: float
    distribution_shift_penalty_r: float
    execution_uncertainty_penalty_r: float

    conservative_edge_r: float

    room_to_target_r: Optional[float]
    cost_share: Optional[float]

    raw_support: int
    ess: float
    backoff_level: int

    state_uncertainty: float
    forecast_uncertainty: float
    ood_score: float

    gate_results: Tuple[AdmissionGateResult, ...]

    admission_status: str  # AdmissionStatus
    reason_codes: Tuple[str, ...]

    admission_policy_version: str
    admission_policy_hash: str

    #: NET lower-tail outcome: forecast gross lower quantile minus this
    #: opportunity's Cost_R (cost subtracted once; Section 15.8).
    lower_net_quantile_r: Optional[float] = None

    #: Only present when the feeding CostEstimate was ACCOUNT-scoped.
    user_id: Optional[str] = None
    broker_account_id: Optional[str] = None
    bot_instance_id: Optional[str] = None
    run_id: Optional[str] = None
    cycle_id: Optional[str] = None

    schema_version: str = ECONOMIC_OPPORTUNITY_SCHEMA_VERSION

    @staticmethod
    def build_id(*, setup_candidate_id: str, forecast_id: str, cost_estimate_id: str, admission_policy_hash: str) -> str:
        return short_id(
            "econ", {
                "setup_candidate_id": setup_candidate_id, "forecast_id": forecast_id,
                "cost_estimate_id": cost_estimate_id, "admission_policy_hash": admission_policy_hash,
            },
        )


__all__ = [
    "CostScope",
    "AdmissionStatus",
    "AdmissionGate",
    "EconomicsReasonCode",
    "ComponentLineage",
    "CostUncertaintyBreakdown",
    "CostEstimate",
    "AdmissionGateResult",
    "AdmissionPolicy",
    "EconomicOpportunity",
]
