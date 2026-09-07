"""Map legacy engine reason codes onto the canonical Phase 8 taxonomy.

Compatibility strategy (§11): one active decision path, plus an adapter — never
two engines running side by side. PolicyEngine and SafetyEngine keep emitting
their historical ``ReasonCode`` values so existing routes and tests are
unaffected; this module is the single place that translates those into the
canonical vocabulary used for evidence and diagnostics.

The mapping also encodes the Phase 8 responsibility split, which is the whole
point of the exercise:

  * ``RISK_*``      — the account cannot afford this trade, or cannot size it.
  * ``EXECUTION_*`` — the trade is affordable but cannot be placed right now.

``LOW_CONFIDENCE`` deliberately has NO canonical equivalent below. After Phase 7
entry quality is decided once, upstream, by TradingDecisionEngine. A downstream
engine emitting LOW_CONFIDENCE is a duplicate authority, not a reason code, and
:func:`assert_no_duplicate_confidence_authority` exists to catch it.
"""
from __future__ import annotations

from app.decision.reasons import (
    ExecutionReason,
    LifecycleReason,
    ProtectionReason,
    QualityReason,
    RiskReason,
)

#: Legacy ReasonCode value -> canonical taxonomy value.
LEGACY_REASON_MAP: dict[str, str] = {
    # ── Risk: affordability, capacity, drawdown, sizing ──────────────────────
    "DAILY_LOSS_LIMIT": RiskReason.DAILY_LOSS_LIMIT,
    "WEEKLY_DRAWDOWN_LIMIT": RiskReason.WEEKLY_DRAWDOWN_LIMIT,
    "MONTHLY_DRAWDOWN_LIMIT": RiskReason.MONTHLY_DRAWDOWN_LIMIT,
    "DAILY_TRADE_LIMIT": RiskReason.MAX_DAILY_TRADES,
    "MAX_POSITIONS_REACHED": RiskReason.MAX_OPEN_POSITIONS,
    "MARGIN_INSUFFICIENT": RiskReason.MARGIN_INSUFFICIENT,
    "EXPOSURE_LIMIT": RiskReason.EXPOSURE_LIMIT,
    "COMPOUND_RISK_EXCEEDED": RiskReason.EXPOSURE_LIMIT,
    "LEVERAGE_TOO_HIGH": RiskReason.LEVERAGE_LIMIT,
    "CORRELATION_LIMIT": RiskReason.CORRELATION_LIMIT,
    "RISK_REWARD_TOO_LOW": RiskReason.RR_BELOW_MINIMUM,
    "INVALID_RISK": RiskReason.STOP_INVALID,
    "INVALID_REWARD": RiskReason.STOP_INVALID,
    "STOP_TOO_WIDE": RiskReason.STOP_INVALID,
    "ATR_INVALID": RiskReason.ATR_NOISE_FLOOR,
    "ATR_NOISE_FLOOR": RiskReason.ATR_NOISE_FLOOR,
    "SIZE_ZERO": RiskReason.INSUFFICIENT_CAPITAL,
    "CAPITAL_BUDGET_REQUIRED": RiskReason.CAPITAL_BUDGET_REQUIRED,
    "CONSECUTIVE_LOSS_LIMIT": RiskReason.CONSECUTIVE_LOSS_LIMIT,
    "SL_COOLDOWN_ACTIVE": RiskReason.CONSECUTIVE_LOSS_LIMIT,

    # ── Execution feasibility: can this be placed at this instant ────────────
    "MIN_NOTIONAL_NOT_MET": ExecutionReason.MIN_NOTIONAL,
    "PRICE_INVALID": ExecutionReason.DATA_STALE,
    "STALE_MARKET_DATA": ExecutionReason.DATA_STALE,
    "SPREAD_TOO_WIDE": ExecutionReason.SPREAD_TOO_WIDE,
    "MARKET_CLOSED": ExecutionReason.SYMBOL_UNAVAILABLE,
    "NOT_LIVE_SYMBOL": ExecutionReason.SYMBOL_UNAVAILABLE,
    "CIRCUIT_BREAKER_TRIPPED": ExecutionReason.CAPABILITY_UNAVAILABLE,

    # ── Entry quality: owned solely by TradingDecisionEngine ─────────────────
    "SIGNAL_HOLD": QualityReason.NO_OPPORTUNITY,
    "NO_OPPORTUNITY": QualityReason.NO_OPPORTUNITY,
    "HTF_OPPOSED": QualityReason.HTF_NOT_ALIGNED,
    "REGIME_BLOCKED": QualityReason.REGIME_BLOCKED,
    "VOLATILITY_SPIKE": QualityReason.REGIME_BLOCKED,
    "SESSION_BLOCKED": QualityReason.SESSION_BLOCKED,
    "EVENT_BLACKOUT": QualityReason.EVENT_BLACKOUT,
    "ENTRY_CONFIDENCE_BELOW_THRESHOLD": QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD,
    "CONSENSUS_INSUFFICIENT": QualityReason.CONSENSUS_INSUFFICIENT,

    # ── Protection / lifecycle ───────────────────────────────────────────────
    "WAITING_REENTRY_CONFIRMATION": ProtectionReason.ENTRY_DUPLICATE,
    "POSITION_ALREADY_OPEN": ProtectionReason.POSITION_ALREADY_OPEN,
    "SUBMISSION_UNCERTAIN": ProtectionReason.SUBMISSION_UNCERTAIN,
    "KILL_SWITCH_ACTIVE": LifecycleReason.KILL_SWITCH_ACTIVE,
    "DAILY_CLOSE": LifecycleReason.DAILY_CLOSE,
}

#: Legacy codes that must never be produced downstream of entry quality.
DUPLICATE_CONFIDENCE_CODES: frozenset[str] = frozenset({"LOW_CONFIDENCE"})


def to_canonical(reason_code: str | None) -> str | None:
    """Translate a legacy reason code, or return it unchanged if already canonical."""
    if reason_code is None:
        return None
    code = str(getattr(reason_code, "value", reason_code))
    return LEGACY_REASON_MAP.get(code, code)


def is_risk_reason(reason_code: str | None) -> bool:
    return str(to_canonical(reason_code) or "").startswith("RISK_")


def is_execution_reason(reason_code: str | None) -> bool:
    return str(to_canonical(reason_code) or "").startswith("EXECUTION_")


def assert_no_duplicate_confidence_authority(reason_code: str | None) -> None:
    """Raise if a post-quality stage tried to reject for confidence again.

    After TradingDecisionEngine approves entry quality, SafetyEngine and
    PolicyEngine are told ``confidence_already_approved=True``. If one of them
    still returns LOW_CONFIDENCE, the single-authority invariant has been
    broken and we want to know loudly rather than silently double-gating.
    """
    code = str(getattr(reason_code, "value", reason_code) or "")
    if code in DUPLICATE_CONFIDENCE_CODES:
        raise AssertionError(
            f"{code} was emitted after entry quality was already decided. "
            "Entry quality is owned solely by TradingDecisionEngine (Phase 7 §7.6)."
        )
