"""Section 14 contracts -- VetoDecision, VetoCheckResult, VetoPolicy, and the
typed EventRiskContext / SystemHealthContext veto inputs.

Rejection is not an error. Veto is a first-class CATI authority: a candidate
can be economically admissible (Section 13) and still be vetoed because
current conditions make the evidence untrustworthy. Only
``APPROVE_FOR_RANKING`` may enter Section 15; WATCH and REJECT never do.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.events import (  # re-exported: typed veto inputs
    EventRiskContext, EventSourceState, MaintenanceContext, MaintenanceSourceState, MarketEvent, MarketEventType,
)
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.system_health import (  # re-exported
    BrokerHealthContext, BrokerHealthStatus, SystemHealthContext,
)
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import (
    VETO_DECISION_SCHEMA_VERSION,
    VETO_POLICY_SCHEMA_VERSION,
)


class VetoOutcome(str, Enum):
    APPROVE_FOR_RANKING = "APPROVE_FOR_RANKING"
    WATCH = "WATCH"
    REJECT = "REJECT"


class VetoCheckStatus(str, Enum):
    PASS = "PASS"
    WATCH = "WATCH"
    FAIL = "FAIL"
    NOT_EVALUATED = "NOT_EVALUATED"


class VetoFamily(str, Enum):
    STATE = "STATE"
    EVIDENCE = "EVIDENCE"
    OOD = "OOD"
    GEOMETRY = "GEOMETRY"
    COST = "COST"
    LIQUIDITY = "LIQUIDITY"
    CROWDING = "CROWDING"
    EVENT = "EVENT"
    PORTFOLIO = "PORTFOLIO"
    SYSTEM = "SYSTEM"


#: Deterministic evaluation/reporting order of families (never dict order).
FAMILY_ORDER: Tuple[str, ...] = tuple(f.value for f in VetoFamily)


class VetoStage(str, Enum):
    PRE_RANKING = "PRE_RANKING"
    PORTFOLIO_STAGE = "PORTFOLIO_STAGE"


class VetoReasonCode(str, Enum):
    # STATE
    TRANSITION_HIGH = "TRANSITION_HIGH"
    STATE_UNCERTAINTY_HIGH = "STATE_UNCERTAINTY_HIGH"
    SHOCK_STATE = "SHOCK_STATE"
    MARKET_STATE_INVALID = "MARKET_STATE_INVALID"
    # EVIDENCE
    INSUFFICIENT_SUPPORT = "INSUFFICIENT_SUPPORT"
    WIDE_CREDIBLE_INTERVAL = "WIDE_CREDIBLE_INTERVAL"
    UNCALIBRATED_PROBABILITY = "UNCALIBRATED_PROBABILITY"
    ECONOMICALLY_INADMISSIBLE = "ECONOMICALLY_INADMISSIBLE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    # OOD
    DISTRIBUTION_SHIFT_HIGH = "DISTRIBUTION_SHIFT_HIGH"
    UNSEEN_STATE_BUCKET = "UNSEEN_STATE_BUCKET"
    CAPABILITY_PATTERN_SHIFT = "CAPABILITY_PATTERN_SHIFT"
    SUPPORT_DOMAIN_SHIFT = "SUPPORT_DOMAIN_SHIFT"
    # GEOMETRY
    POOR_REWARD_GEOMETRY = "POOR_REWARD_GEOMETRY"
    LATE_ENTRY = "LATE_ENTRY"
    STRUCTURAL_INVALIDATION_TOO_WIDE = "STRUCTURAL_INVALIDATION_TOO_WIDE"
    # COST
    SPREAD_ANOMALY = "SPREAD_ANOMALY"
    SLIPPAGE_TOO_HIGH = "SLIPPAGE_TOO_HIGH"
    FUNDING_COST_EXCESSIVE = "FUNDING_COST_EXCESSIVE"
    COST_UNCERTAINTY_HIGH = "COST_UNCERTAINTY_HIGH"
    # LIQUIDITY
    DEPTH_INSUFFICIENT = "DEPTH_INSUFFICIENT"
    STALE_BOOK = "STALE_BOOK"
    LIQUIDITY_DETERIORATION = "LIQUIDITY_DETERIORATION"
    LIQUIDITY_UNVERIFIED = "LIQUIDITY_UNVERIFIED"
    # CROWDING
    FUNDING_EXTREME = "FUNDING_EXTREME"
    OI_CROWDING = "OI_CROWDING"
    CROWDING_UNVERIFIED = "CROWDING_UNVERIFIED"
    # EVENT
    EVENT_RISK_WINDOW = "EVENT_RISK_WINDOW"
    EXCHANGE_MAINTENANCE = "EXCHANGE_MAINTENANCE"
    EVENT_FEED_UNAVAILABLE = "EVENT_FEED_UNAVAILABLE"  # legacy alias of EVENT_SOURCE_UNAVAILABLE
    EVENT_SOURCE_UNAVAILABLE = "EVENT_SOURCE_UNAVAILABLE"
    EVENT_SOURCE_STALE = "EVENT_SOURCE_STALE"
    MAINTENANCE_SOURCE_UNAVAILABLE = "MAINTENANCE_SOURCE_UNAVAILABLE"
    MAINTENANCE_SOURCE_STALE = "MAINTENANCE_SOURCE_STALE"
    # PORTFOLIO (evaluated at PORTFOLIO_STAGE only)
    ACCOUNT_CORRELATION_CONFLICT = "ACCOUNT_CORRELATION_CONFLICT"
    COMMON_FACTOR_CONCENTRATION = "COMMON_FACTOR_CONCENTRATION"
    DUPLICATE_EXPOSURE = "DUPLICATE_EXPOSURE"
    SECTOR_CONCENTRATION = "SECTOR_CONCENTRATION"
    LIQUIDITY_CONCENTRATION = "LIQUIDITY_CONCENTRATION"
    ACCOUNT_RESERVATION_CONFLICT = "ACCOUNT_RESERVATION_CONFLICT"
    INSUFFICIENT_CORRELATION_HISTORY_FALLBACK = "INSUFFICIENT_CORRELATION_HISTORY_FALLBACK"
    PORTFOLIO_DATA_QUALITY_FAULT = "PORTFOLIO_DATA_QUALITY_FAULT"
    PORTFOLIO_CONTEXT_PENDING = "PORTFOLIO_CONTEXT_PENDING"
    # SYSTEM
    DATA_QUALITY_FAULT = "DATA_QUALITY_FAULT"
    BROKER_DEGRADED = "BROKER_DEGRADED"
    BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"
    BROKER_UNKNOWN = "BROKER_UNKNOWN"
    BROKER_HEALTH_STALE = "BROKER_HEALTH_STALE"
    BROKER_HEALTH_NOT_PROVIDED = "BROKER_HEALTH_NOT_PROVIDED"
    CATI_COMPONENT_ERROR = "CATI_COMPONENT_ERROR"
    SYSTEM_HEALTH_UNKNOWN = "SYSTEM_HEALTH_UNKNOWN"
    OK = "OK"


# Typed veto inputs (Sections 14.13, 14.14) live in contracts/events.py and
# contracts/system_health.py and are re-exported above.


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class VetoCheckResult:
    family: str
    check: str
    status: str  # VetoCheckStatus
    severity: str  # INFO | WATCH | HARD
    observed_value: Optional[float]
    policy_value: Optional[float]
    reason_code: str
    evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VetoDecision:
    veto_decision_id: str

    economic_opportunity_id: str
    setup_candidate_id: str
    forecast_id: str
    market_state_id: str

    instrument_key: InstrumentKey
    side: str
    decision_time: int

    outcome: str  # VetoOutcome
    stage: str  # VetoStage

    checks: Tuple[VetoCheckResult, ...]
    reason_codes: Tuple[str, ...]

    veto_policy_version: str
    veto_policy_hash: str
    data_quality: str

    created_at: int

    user_id: Optional[str] = None
    broker_account_id: Optional[str] = None
    bot_instance_id: Optional[str] = None
    run_id: Optional[str] = None
    cycle_id: Optional[str] = None

    schema_version: str = VETO_DECISION_SCHEMA_VERSION

    @property
    def approved_for_ranking(self) -> bool:
        return self.outcome == VetoOutcome.APPROVE_FOR_RANKING.value

    @staticmethod
    def build_id(*, economic_opportunity_id: str, veto_policy_hash: str, stage: str, portfolio_findings: Tuple[str, ...] = ()) -> str:
        return short_id(
            "veto", {
                "economic_opportunity_id": economic_opportunity_id, "veto_policy_hash": veto_policy_hash,
                "stage": stage, "portfolio_findings": list(portfolio_findings),
            },
        )


# ---------------------------------------------------------------------------
# Policy (Section 14.4) -- RESEARCH DEFAULTS, unrelated to any V2 threshold.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class VetoPolicy:
    schema_version: str = VETO_POLICY_SCHEMA_VERSION

    # STATE
    reject_on_shock_state: bool = True
    transition_uncertainty_watch: float = 0.55
    transition_uncertainty_reject: float = 0.80
    state_uncertainty_watch: float = 0.40
    state_uncertainty_reject: float = 0.60

    # EVIDENCE
    #: What an Section-13 INSUFFICIENT_EVIDENCE opportunity becomes: WATCH or REJECT.
    insufficient_evidence_outcome: str = "WATCH"
    credible_interval_width_watch: float = 0.35
    credible_interval_width_reject: float = 0.60
    #: Statuses whose probabilities may reach ranking. Production default is
    #: fail-closed: only CALIBRATED.
    calibration_statuses_allowed_for_approval: Tuple[str, ...] = ("CALIBRATED",)
    #: Outcome for a non-allowed calibration status: WATCH or REJECT.
    uncalibrated_outcome: str = "WATCH"

    # OOD
    ood_score_watch: float = 0.50
    ood_score_reject: float = 0.75
    unseen_bucket_outcome: str = "WATCH"  # WATCH | REJECT
    capability_shift_outcome: str = "WATCH"

    # GEOMETRY
    min_room_to_target_r: float = 0.5
    max_structural_risk_fraction: float = 0.08  # risk_distance / trigger price
    late_entry_extension_atr_watch: float = 3.0
    late_entry_extension_atr_reject: float = 4.5

    # COST
    max_spread_r: float = 0.15
    max_slippage_r: float = 0.15
    max_funding_r: float = 0.20
    cost_uncertainty_r_watch: float = 0.10

    # LIQUIDITY -- NOT_EVALUATED | WATCH | REJECT when book capability missing
    missing_liquidity_status: str = "NOT_EVALUATED"
    min_top_book_depth: float = 0.0  # 0 => depth check only flags missing depth as NOT_EVALUATED
    stale_book_outcome: str = "WATCH"
    spread_percentile_watch: float = 0.90
    spread_percentile_reject: float = 0.98

    # CROWDING
    funding_percentile_extreme: float = 0.97
    missing_crowding_status: str = "NOT_EVALUATED"

    # EVENT -- an absent/stale calendar is never an all-clear. WATCH keeps the
    # opportunity out of ranking; REJECT is stricter; NOT_EVALUATED permits it
    # (research shadow only) while still recording the gap.
    event_lookahead_ms: int = 4 * 3_600_000
    event_unavailable_status: str = "WATCH"  # NOT_EVALUATED | WATCH | REJECT
    event_stale_status: str = "WATCH"
    #: No maintenance feed exists for any current venue. Recorded explicitly
    #: (MAINTENANCE_SOURCE_UNAVAILABLE) -- never as "no maintenance scheduled".
    maintenance_unavailable_status: str = "NOT_EVALUATED"
    maintenance_stale_status: str = "WATCH"

    # SYSTEM -- broker health from the runtime's canonical source.
    broker_degraded_status: str = "WATCH"  # WATCH | REJECT
    #: Sub-reasons of DEGRADED that are severe enough to REJECT.
    broker_degraded_reject_reasons: Tuple[str, ...] = ("CIRCUIT_RECOVERY_AFTER_HALT",)
    unknown_broker_health_status: str = "WATCH"  # fail closed: never approvable
    missing_broker_health_status: str = "WATCH"  # caller did not wire health in
    broker_health_max_age_ms: int = 5 * 60_000

    #: family-specific override dicts keyed by setup_family (field -> value).
    family_overrides: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @property
    def policy_hash(self) -> str:
        payload = {k: (v if not isinstance(v, tuple) else list(v)) for k, v in self.__dict__.items() if k != "family_overrides"}
        payload["family_overrides"] = {k: dict(v) for k, v in self.family_overrides.items()}
        return stable_hash(payload)

    def for_family(self, setup_family: str) -> "VetoPolicy":
        overrides = self.family_overrides.get(setup_family)
        return replace(self, **overrides) if overrides else self


__all__ = [
    "VetoOutcome", "VetoCheckStatus", "VetoFamily", "FAMILY_ORDER", "VetoStage", "VetoReasonCode",
    "EventRiskContext", "EventSourceState", "MaintenanceContext", "MaintenanceSourceState", "MarketEvent",
    "MarketEventType", "BrokerHealthContext", "BrokerHealthStatus", "SystemHealthContext",
    "VetoCheckResult", "VetoDecision", "VetoPolicy",
]
