"""Canonical reason taxonomy for the active trading pipeline.

One vocabulary, shared by the runner, the decision engine, risk, execution
feasibility and the persisted evidence.  The point is that a later replay or
diagnostic layer can consume a *field*, not parse a sentence.

Responsibility boundaries encoded here:

  * ``Cycle``      — why a heartbeat produced no strategy evaluation at all.
  * ``Quality``    — why an opportunity did or did not clear entry quality.
                     This is the ONLY family that may speak about confidence.
  * ``Risk``       — whether the account can afford the trade, and at what size.
  * ``Execution``  — whether an approved, sized trade can be placed right now.
  * ``Protection`` — duplicate / idempotency barriers.
  * ``Lifecycle``  — what actually happened at the venue.
"""
from __future__ import annotations

from typing import Final


class CycleReason:
    """Management-heartbeat outcomes. Not strategy verdicts."""

    #: The heartbeat ran, but no new CLOSED strategy candle existed to evaluate.
    #: This is NOT a strategy HOLD — the strategy did not run.
    NO_NEW_CANDLE: Final = "NO_NEW_CANDLE"
    MANAGEMENT_ONLY: Final = "MANAGEMENT_ONLY"
    STOP_REQUESTED: Final = "STOP_REQUESTED"
    SYMBOL_LOCK_BUSY: Final = "SYMBOL_LOCK_BUSY"


class QualityReason:
    """Entry-quality verdicts. Owned solely by TradingDecisionEngine."""

    APPROVED_FOR_EXECUTION: Final = "APPROVED_FOR_EXECUTION"

    #: The strategy evaluated a new candle and found no directional candidate.
    #: Distinct from a confidence failure: there was nothing to be confident about.
    NO_OPPORTUNITY: Final = "NO_OPPORTUNITY"

    #: A direction exists but the strategies do not agree strongly enough.
    CONSENSUS_INSUFFICIENT: Final = "CONSENSUS_INSUFFICIENT"

    #: Strategies agree on direction, but the quality score is below threshold.
    ENTRY_CONFIDENCE_BELOW_THRESHOLD: Final = "ENTRY_CONFIDENCE_BELOW_THRESHOLD"

    HTF_NOT_ALIGNED: Final = "HTF_NOT_ALIGNED"
    REGIME_BLOCKED: Final = "REGIME_BLOCKED"
    SESSION_BLOCKED: Final = "SESSION_BLOCKED"
    EVENT_BLACKOUT: Final = "EVENT_BLACKOUT"

    #: An eligible expert failed to evaluate (an exception, or data it needs was
    #: unavailable). The candle fails closed: a failure is not a neutral vote,
    #: and the opportunity cannot be judged without evidence the regime asked for.
    EXPERT_EVALUATION_ERROR: Final = "EXPERT_EVALUATION_ERROR"

    #: An external signal could not be expressed as a TradingOpportunity (too
    #: little market data to classify the regime, for example), so it never
    #: reached the threshold authority and cannot execute.
    OPPORTUNITY_CONTRACT_UNSATISFIED: Final = "OPPORTUNITY_CONTRACT_UNSATISFIED"


class RiskReason:
    """Account/portfolio affordability and sizing. Never speaks about confidence."""

    APPROVED: Final = "RISK_APPROVED"
    CAPITAL_BUDGET_REQUIRED: Final = "RISK_CAPITAL_BUDGET_REQUIRED"
    MAX_DAILY_TRADES: Final = "RISK_MAX_DAILY_TRADES"
    MAX_OPEN_POSITIONS: Final = "RISK_MAX_OPEN_POSITIONS"
    DAILY_LOSS_LIMIT: Final = "RISK_DAILY_LOSS_LIMIT"
    WEEKLY_DRAWDOWN_LIMIT: Final = "RISK_WEEKLY_DRAWDOWN_LIMIT"
    MONTHLY_DRAWDOWN_LIMIT: Final = "RISK_MONTHLY_DRAWDOWN_LIMIT"
    CONSECUTIVE_LOSS_LIMIT: Final = "RISK_CONSECUTIVE_LOSS_LIMIT"
    LEVERAGE_LIMIT: Final = "RISK_LEVERAGE_LIMIT"
    CORRELATION_LIMIT: Final = "RISK_CORRELATION_LIMIT"
    EXPOSURE_LIMIT: Final = "RISK_EXPOSURE_LIMIT"
    MARGIN_INSUFFICIENT: Final = "RISK_MARGIN_INSUFFICIENT"
    STOP_INVALID: Final = "RISK_STOP_INVALID"
    RR_BELOW_MINIMUM: Final = "RISK_RR_BELOW_MINIMUM"
    ATR_NOISE_FLOOR: Final = "RISK_ATR_NOISE_FLOOR"
    INSUFFICIENT_CAPITAL: Final = "RISK_INSUFFICIENT_CAPITAL"
    #: The capital ledger could not be read or evaluated. Infrastructure failure
    #: is never authorisation: the entry is rejected in paper and live alike.
    CAPITAL_LEDGER_UNAVAILABLE: Final = "RISK_CAPITAL_LEDGER_UNAVAILABLE"


class ExecutionReason:
    """Can this already-approved, already-sized trade be placed right now?"""

    APPROVED: Final = "EXECUTION_APPROVED"
    DATA_STALE: Final = "EXECUTION_DATA_STALE"
    SPREAD_TOO_WIDE: Final = "EXECUTION_SPREAD_TOO_WIDE"
    LIQUIDITY_INSUFFICIENT: Final = "EXECUTION_LIQUIDITY_INSUFFICIENT"
    SYMBOL_UNAVAILABLE: Final = "EXECUTION_SYMBOL_UNAVAILABLE"
    CAPABILITY_UNAVAILABLE: Final = "EXECUTION_CAPABILITY_UNAVAILABLE"
    MARGIN_PREFLIGHT_FAILED: Final = "EXECUTION_MARGIN_PREFLIGHT_FAILED"
    MIN_NOTIONAL: Final = "EXECUTION_MIN_NOTIONAL"
    PROTECTION_UNAVAILABLE: Final = "EXECUTION_PROTECTION_UNAVAILABLE"


class ProtectionReason:
    """Final duplicate / idempotency barrier. Owned by EntryProtection."""

    ENTRY_DUPLICATE: Final = "ENTRY_DUPLICATE"
    POSITION_ALREADY_OPEN: Final = "POSITION_ALREADY_OPEN"
    SUBMISSION_UNCERTAIN: Final = "SUBMISSION_UNCERTAIN"


class LifecycleReason:
    """What actually happened at the venue or in the simulator."""

    PAPER_POSITION_OPENED: Final = "PAPER_POSITION_OPENED"
    PAPER_PARTIAL_CLOSE: Final = "PAPER_PARTIAL_CLOSE"
    PAPER_POSITION_CLOSED: Final = "PAPER_POSITION_CLOSED"
    BROKER_ORDER_SUBMITTED: Final = "BROKER_ORDER_SUBMITTED"
    BROKER_FILL_RECORDED: Final = "BROKER_FILL_RECORDED"
    KILL_SWITCH_ACTIVE: Final = "KILL_SWITCH_ACTIVE"
    DAILY_CLOSE: Final = "DAILY_CLOSE"
    RECONCILIATION_CLOSE: Final = "RECONCILIATION_CLOSE"


def _collect(*classes: type) -> frozenset[str]:
    return frozenset(
        value
        for cls in classes
        for name, value in vars(cls).items()
        if not name.startswith("_") and isinstance(value, str)
    )


#: Every reason code the active pipeline may emit.
ALL_REASON_CODES: Final = _collect(
    CycleReason, QualityReason, RiskReason, ExecutionReason, ProtectionReason, LifecycleReason
)

#: Reason codes that mean "the strategy never ran on a new candle".
NON_EVALUATION_REASONS: Final = frozenset({CycleReason.NO_NEW_CANDLE, CycleReason.MANAGEMENT_ONLY})

#: The three outcomes that must never be collapsed into one another.
DISTINCT_QUALITY_FAILURES: Final = frozenset({
    QualityReason.NO_OPPORTUNITY,
    QualityReason.CONSENSUS_INSUFFICIENT,
    QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD,
})


def is_known_reason(code: str | None) -> bool:
    return bool(code) and str(code) in ALL_REASON_CODES
