"""Section 20 contracts -- the CATI hard-risk / execution boundary.

``RiskDecision`` is EVIDENCE describing what the EXISTING hard-risk stack
(TradingOrchestrator Layers A/B/C + PolicyEngine sizing, and the executor's
capital / slot / margin gates) decided about one TradePlan. It is not, and
never becomes, a second risk engine: CATI cannot approve what that stack
rejects, and CATI opportunity quality never lowers a limit.

``ExecutionAttempt`` is evidence of one broker submission of an approved
plan, in broker-neutral terms. Its state history is append-only (a new row
per state). No credential is ever carried.

``ExecutionSupportStatus`` is distinct from Section 17's economic
``AdapterValidationStatus``: a venue whose economics work is NOT thereby
execution-supported.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import EXECUTION_ATTEMPT_SCHEMA_VERSION, RISK_DECISION_SCHEMA_VERSION


class RiskDecisionStatus(str, Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class RiskRejectionFamily(str, Enum):
    """Bounded families (21.12): alpha failure is never confused with a
    hard-risk rejection, and each hard-risk authority is distinguishable."""

    PREVALIDATION = "PREVALIDATION"
    DAILY_LOSS = "DAILY_LOSS"
    ADAPTIVE_RISK = "ADAPTIVE_RISK"
    DRAWDOWN = "DRAWDOWN"
    SLOT = "SLOT"
    MARGIN = "MARGIN"
    LEVERAGE = "LEVERAGE"
    SIZING = "SIZING"
    INSTRUMENT = "INSTRUMENT"
    OTHER = "OTHER"


class RiskStage(str, Enum):
    #: TradingOrchestrator.process_trade_plan (prevalidation, Layers A/B/C, PolicyEngine)
    PRE_EXECUTION = "PRE_EXECUTION"
    #: the executor's own atomic capital / slot / margin gates at submission time
    EXECUTOR_GATES = "EXECUTOR_GATES"


class ExecutionAttemptStatus(str, Enum):
    PENDING_SUBMIT = "PENDING_SUBMIT"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    NOT_FILLED = "NOT_FILLED"
    REJECTED = "REJECTED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    DUPLICATE_SUPPRESSED = "DUPLICATE_SUPPRESSED"
    PROTECTION_FAILED_ROLLED_BACK = "PROTECTION_FAILED_ROLLED_BACK"
    RECONCILED_POSITION_EXISTS = "RECONCILED_POSITION_EXISTS"
    RECONCILED_NO_POSITION = "RECONCILED_NO_POSITION"
    ERROR_PRE_SUBMIT = "ERROR_PRE_SUBMIT"


#: an authoritative position exists at the broker
POSITION_EXISTS = frozenset({ExecutionAttemptStatus.FILLED.value, ExecutionAttemptStatus.PARTIALLY_FILLED.value,
                             ExecutionAttemptStatus.RECONCILED_POSITION_EXISTS.value})
#: provably no position was created (reservation may be released)
NO_POSITION = frozenset({ExecutionAttemptStatus.NOT_FILLED.value, ExecutionAttemptStatus.REJECTED.value,
                         ExecutionAttemptStatus.ERROR_PRE_SUBMIT.value,
                         ExecutionAttemptStatus.PROTECTION_FAILED_ROLLED_BACK.value,
                         ExecutionAttemptStatus.RECONCILED_NO_POSITION.value})


class ExecutionSupportStatus(str, Enum):
    UNVALIDATED = "UNVALIDATED"
    CONTRACT_VALIDATED = "CONTRACT_VALIDATED"
    DEMO_VALIDATED = "DEMO_VALIDATED"
    PRODUCTION_VALIDATED = "PRODUCTION_VALIDATED"


SUPPORT_RANK = {ExecutionSupportStatus.UNVALIDATED.value: 0, ExecutionSupportStatus.CONTRACT_VALIDATED.value: 1,
                ExecutionSupportStatus.DEMO_VALIDATED.value: 2, ExecutionSupportStatus.PRODUCTION_VALIDATED.value: 3}


def _plain(v):
    if hasattr(v, "__dataclass_fields__"):
        return asdict(v)
    if isinstance(v, (tuple, list)):
        return [_plain(x) for x in v]
    return v


@dataclass(frozen=True)
class RiskDecision:
    risk_decision_id: str
    trade_plan_id: str
    trade_plan_hash: str

    user_id: Optional[str]
    broker_account_id: str
    bot_instance_id: str
    runtime_session_id: Optional[str]
    run_id: Optional[str]
    cycle_id: Optional[str]

    status: str  # RiskDecisionStatus
    stage: str  # RiskStage
    rejection_family: Optional[str]  # RiskRejectionFamily
    reason_codes: Tuple[str, ...]

    #: fixed_amount = per APPROVED TRADE allocation, never an aggregate bot budget (20.5)
    allocation_basis: Tuple[Tuple[str, str], ...]
    risk_budget: Optional[float]
    resolved_stop_price: Optional[float]
    resolved_stop_distance: Optional[float]
    stop_tightened_by_hard_risk: bool
    resolved_leverage: Optional[float]
    resolved_quantity: Optional[float]
    resolved_notional: Optional[float]
    resolved_take_profit: Optional[float]

    slot_reservation_id: Optional[str]
    margin_reservation_id: Optional[str]

    policy_versions: Tuple[Tuple[str, str], ...]
    source: str
    decision_time: int
    schema_version: str = RISK_DECISION_SCHEMA_VERSION

    @classmethod
    def build(cls, **fields) -> "RiskDecision":
        digest = stable_hash({k: _plain(v) for k, v in fields.items()})
        return cls(risk_decision_id=short_id("rdec", digest), **fields)

    @property
    def approved(self) -> bool:
        return self.status == RiskDecisionStatus.APPROVED.value


@dataclass(frozen=True)
class ExecutionAttempt:
    execution_attempt_id: str
    trade_plan_id: str
    risk_decision_id: Optional[str]

    user_id: Optional[str]
    broker_account_id: str
    bot_instance_id: str

    venue: str
    environment: str
    instrument_key: Any
    adapter_id: str
    execution_support_status: str

    side: str
    resolved_quantity: Optional[float]
    requested_quantity: Optional[float]
    submitted_quantity: Optional[float]
    filled_quantity: Optional[float]

    requested_order_type: str
    actual_order_type: Optional[str]
    requested_price: Optional[float]
    submitted_price: Optional[float]
    filled_price: Optional[float]
    max_slippage_budget_bps: float

    status: str  # ExecutionAttemptStatus
    broker_order_id: Optional[str]
    client_order_id: Optional[str]
    position_id: Optional[str]

    submitted_at: Optional[int]
    acknowledged_at: Optional[int]
    resolved_at: Optional[int]
    recorded_at: int

    planned_costs: Mapping[str, float]
    realized_costs: Mapping[str, float]
    protection: Mapping[str, Any]
    reason_codes: Tuple[str, ...]
    raw_executor_status: Optional[str] = None
    schema_version: str = EXECUTION_ATTEMPT_SCHEMA_VERSION

    @staticmethod
    def build_id(*, trade_plan_id: str, trade_plan_hash: str, broker_account_id: str) -> str:
        """Deterministic: ONE attempt identity per (plan, plan hash, account)."""
        return short_id("xatt", {"plan": trade_plan_id, "hash": trade_plan_hash, "acct": broker_account_id})


__all__ = [
    "RiskDecisionStatus", "RiskRejectionFamily", "RiskStage", "ExecutionAttemptStatus", "POSITION_EXISTS",
    "NO_POSITION", "ExecutionSupportStatus", "SUPPORT_RANK", "RiskDecision", "ExecutionAttempt",
]
