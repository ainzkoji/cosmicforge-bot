"""Phase 9 — the canonical ``TradingDecision`` recorder.

The invariant this module exists to enforce:

    one bot + one symbol + one evaluation event
        = exactly one FINALIZED TradingDecision

That includes every early return. A heartbeat that finds no new candle, a
symbol whose lock was busy, a regime block, a risk rejection, an executor
crash — all of them finalize a record. There are no abandoned traces.

Usage is deliberately hard to get wrong:

    with record_decision(db, bot_instance_id=..., symbol=...) as decision:
        decision.set_snapshot(snapshot)
        ...
        decision.reject(QualityReason.NO_OPPORTUNITY)

The context manager finalizes on exit no matter how the block is left,
including on an exception, which is recorded as EXECUTION_ERROR rather than
losing the evaluation entirely.
"""
from __future__ import annotations

import json
import logging
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence

from shared_lib.persistence.evidence_schema import (
    EVIDENCE_COMPLETE,
    PAPER_FORWARD,
)

logger = logging.getLogger(__name__)

#: A decision that never reaches finalize() is a bug. NONE is not a reason.
UNFINALIZED_REASON = "UNFINALIZED"
EXECUTION_ERROR = "EXECUTION_ERROR"


def new_decision_id() -> str:
    return f"dec_{uuid.uuid4().hex[:20]}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return json.dumps(str(value))


@dataclass
class TradingDecision:
    """A mutable builder that becomes an immutable row once finalized."""

    decision_id: str
    bot_instance_id: str
    symbol: str
    evaluated_at: str

    runtime_session_id: str | None = None
    user_id: str | None = None
    broker_account_id: str | None = None
    run_id: str | None = None
    cycle_id: str | None = None
    market_snapshot_id: str | None = None
    opportunity_id: str | None = None
    policy_hash: str | None = None

    market_type: str | None = None
    timeframe: str | None = None
    closed_candle_open_time: int | None = None
    closed_candle_close_time: int | None = None

    provenance: str = PAPER_FORWARD
    evidence_quality: str = EVIDENCE_COMPLETE
    execution_mode: str | None = None
    broker_environment: str | None = None

    regime: str | None = None
    regime_confidence: float | None = None
    component_signals: Any = None
    active_strategies: Sequence[str] | None = None
    supporting_strategies: Sequence[str] | None = None
    opposing_strategies: Sequence[str] | None = None
    component_metadata: Any = None

    buy_score: float | None = None
    sell_score: float | None = None
    consensus_observed: float | None = None
    consensus_required: float | None = None
    raw_confidence: float | None = None

    threshold_base: float | None = None
    threshold_dynamic: float | None = None
    threshold_adaptive_modifier: float | None = None
    threshold_regime_modifier: float | None = None
    effective_entry_threshold: float | None = None

    quality_result: str | None = None
    quality_reason: str | None = None
    hard_veto_result: str | None = None
    hard_veto_reason: str | None = None
    risk_result: str | None = None
    risk_reason: str | None = None

    requested_margin: float | None = None
    approved_margin: float | None = None
    requested_notional: float | None = None
    approved_notional: float | None = None
    quantity: float | None = None
    leverage: float | None = None
    risk_amount: float | None = None
    stop_price: float | None = None
    target_price: float | None = None
    stop_distance: float | None = None
    reward_distance: float | None = None
    resolved_rr: float | None = None

    execution_feasibility_result: str | None = None
    execution_feasibility_reason: str | None = None
    entry_protection_result: str | None = None
    entry_protection_reason: str | None = None

    execution_attempt_id: str | None = None
    order_id: str | None = None
    fill_ids: list[str] = field(default_factory=list)
    position_id: str | None = None

    final_action: str | None = None
    primary_reason: str | None = None
    secondary_reasons: list[str] = field(default_factory=list)
    legacy_reason: str | None = None

    complete: bool = False
    finalized_at: str | None = None

    # ── Fluent recording helpers ────────────────────────────────────────────

    def set_snapshot(self, snapshot: Any) -> "TradingDecision":
        """Bind the market view this decision was made against."""
        if snapshot is None:
            return self
        self.market_snapshot_id = getattr(snapshot, "market_snapshot_id", None)
        self.timeframe = self.timeframe or getattr(snapshot, "timeframe", None)
        self.closed_candle_close_time = getattr(snapshot, "latest_closed_candle_time", None)
        self.closed_candle_open_time = getattr(snapshot, "latest_closed_candle_open_time", None)
        return self

    def set_opportunity(self, opportunity: Any) -> "TradingDecision":
        """Preserve the full strategy evidence, not just a signal name.

        A future replay or AI layer must be able to reconstruct *why* the
        deterministic system decided what it did, so component breakdown,
        scores and per-strategy positions are all kept.
        """
        if opportunity is None:
            return self
        self.regime = getattr(opportunity, "regime", None)
        self.regime_confidence = getattr(opportunity, "regime_confidence", None)
        self.buy_score = getattr(opportunity, "buy_score", None)
        self.sell_score = getattr(opportunity, "sell_score", None)
        self.consensus_observed = getattr(opportunity, "consensus", None)
        self.active_strategies = getattr(opportunity, "active_strategies", None)
        self.component_metadata = getattr(opportunity, "component_breakdown", None)
        if getattr(opportunity, "is_opportunity", False):
            self.opportunity_id = opportunity.opportunity_id
            self.raw_confidence = opportunity.raw_confidence
            self.supporting_strategies = opportunity.supporting_strategies
            self.opposing_strategies = opportunity.opposing_strategies
            self.component_signals = list(getattr(opportunity, "strategy_reasons", ()) or ())
        return self

    def set_entry_quality(self, decision: Any) -> "TradingDecision":
        if decision is None:
            return self
        self.quality_result = "PASS" if decision.approved else "FAIL"
        self.quality_reason = decision.primary_reason
        self.effective_entry_threshold = decision.effective_entry_threshold
        self.threshold_base = (decision.threshold_inputs or {}).get("base_threshold")
        self.threshold_dynamic = (decision.threshold_inputs or {}).get("base_threshold")
        self.threshold_adaptive_modifier = (decision.threshold_inputs or {}).get("adaptive_gate")
        self.consensus_required = decision.consensus_required
        if self.raw_confidence is None:
            self.raw_confidence = decision.raw_confidence
        return self

    def set_risk(self, *, approved: bool, reason: str, **sizing: Any) -> "TradingDecision":
        self.risk_result = "PASS" if approved else "FAIL"
        self.risk_reason = reason
        for key, value in sizing.items():
            if hasattr(self, key):
                setattr(self, key, value)
        return self

    def set_execution_feasibility(self, *, approved: bool, reason: str) -> "TradingDecision":
        self.execution_feasibility_result = "PASS" if approved else "FAIL"
        self.execution_feasibility_reason = reason
        return self

    def set_entry_protection(self, *, approved: bool, reason: str) -> "TradingDecision":
        self.entry_protection_result = "PASS" if approved else "FAIL"
        self.entry_protection_reason = reason
        return self

    def reject(self, reason: str, *, secondary: Sequence[str] = (), legacy: str | None = None) -> "TradingDecision":
        """Terminate this evaluation with a machine-readable primary reason."""
        self.final_action = "REJECTED"
        self.primary_reason = reason
        self.secondary_reasons = list(secondary)
        self.legacy_reason = legacy
        return self

    def hold(self, reason: str, *, secondary: Sequence[str] = (), legacy: str | None = None) -> "TradingDecision":
        self.final_action = "HOLD"
        self.primary_reason = reason
        self.secondary_reasons = list(secondary)
        self.legacy_reason = legacy
        return self

    def approve(self, reason: str) -> "TradingDecision":
        self.final_action = "APPROVED"
        self.primary_reason = reason
        return self

    # ── Persistence ─────────────────────────────────────────────────────────

    def to_row(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "runtime_session_id": self.runtime_session_id,
            "bot_instance_id": self.bot_instance_id,
            "user_id": self.user_id,
            "broker_account_id": self.broker_account_id,
            "run_id": self.run_id,
            "cycle_id": self.cycle_id,
            "market_snapshot_id": self.market_snapshot_id,
            "opportunity_id": self.opportunity_id,
            "policy_hash": self.policy_hash,
            "symbol": self.symbol,
            "market_type": self.market_type,
            "timeframe": self.timeframe,
            "closed_candle_open_time": self.closed_candle_open_time,
            "closed_candle_close_time": self.closed_candle_close_time,
            "evaluated_at": self.evaluated_at,
            "provenance": self.provenance,
            "evidence_quality": self.evidence_quality,
            "execution_mode": self.execution_mode,
            "broker_environment": self.broker_environment,
            "regime": self.regime,
            "regime_confidence": self.regime_confidence,
            "component_signals_json": _json(self.component_signals),
            "active_strategies_json": _json(list(self.active_strategies or ())),
            "supporting_strategies_json": _json(list(self.supporting_strategies or ())),
            "opposing_strategies_json": _json(list(self.opposing_strategies or ())),
            "component_metadata_json": _json(self.component_metadata),
            "buy_score": self.buy_score,
            "sell_score": self.sell_score,
            "consensus_observed": self.consensus_observed,
            "consensus_required": self.consensus_required,
            "raw_confidence": self.raw_confidence,
            "threshold_base": self.threshold_base,
            "threshold_dynamic": self.threshold_dynamic,
            "threshold_adaptive_modifier": self.threshold_adaptive_modifier,
            "threshold_regime_modifier": self.threshold_regime_modifier,
            "effective_entry_threshold": self.effective_entry_threshold,
            "quality_result": self.quality_result,
            "quality_reason": self.quality_reason,
            "hard_veto_result": self.hard_veto_result,
            "hard_veto_reason": self.hard_veto_reason,
            "risk_result": self.risk_result,
            "risk_reason": self.risk_reason,
            "requested_margin": self.requested_margin,
            "approved_margin": self.approved_margin,
            "requested_notional": self.requested_notional,
            "approved_notional": self.approved_notional,
            "quantity": self.quantity,
            "leverage": self.leverage,
            "risk_amount": self.risk_amount,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "stop_distance": self.stop_distance,
            "reward_distance": self.reward_distance,
            "resolved_rr": self.resolved_rr,
            "execution_feasibility_result": self.execution_feasibility_result,
            "execution_feasibility_reason": self.execution_feasibility_reason,
            "entry_protection_result": self.entry_protection_result,
            "entry_protection_reason": self.entry_protection_reason,
            "execution_attempt_id": self.execution_attempt_id,
            "order_id": self.order_id,
            "fill_ids_json": _json(self.fill_ids),
            "position_id": self.position_id,
            "final_action": self.final_action,
            "primary_reason": self.primary_reason,
            "secondary_reasons_json": _json(self.secondary_reasons),
            "legacy_reason": self.legacy_reason,
            "complete": 1 if self.complete else 0,
            "finalized_at": self.finalized_at,
        }


def persist_decision(db: Any, decision: TradingDecision) -> None:
    row = decision.to_row()
    columns = ", ".join(row)
    placeholders = ", ".join("?" for _ in row)
    with db.connect() as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO trading_decisions ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )


@contextmanager
def record_decision(
    db: Any,
    *,
    bot_instance_id: str,
    symbol: str,
    **initial: Any,
) -> Iterator[TradingDecision]:
    """Open a decision that is guaranteed to finalize.

    Whatever happens inside the block — an early return, a raised exception, a
    completed trade — exactly one finalized row is written. A decision that
    reaches the end without a primary reason is recorded as UNFINALIZED rather
    than as a silent success, so the integrity checker can find it.
    """
    decision = TradingDecision(
        decision_id=initial.pop("decision_id", None) or new_decision_id(),
        bot_instance_id=bot_instance_id,
        symbol=str(symbol).upper(),
        evaluated_at=initial.pop("evaluated_at", None) or _now(),
        **initial,
    )
    try:
        yield decision
    except Exception as exc:
        # An exception must not cost us the evaluation record.
        decision.final_action = "ERROR"
        decision.primary_reason = EXECUTION_ERROR
        decision.legacy_reason = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if not decision.primary_reason:
            decision.primary_reason = UNFINALIZED_REASON
            decision.final_action = decision.final_action or "UNKNOWN"
            logger.error(
                "[DECISION] %s/%s finalized without a primary reason", bot_instance_id, symbol
            )
        decision.complete = True
        decision.finalized_at = _now()
        try:
            persist_decision(db, decision)
        except Exception as persist_exc:
            logger.error("[DECISION] failed to persist %s: %s", decision.decision_id, persist_exc)


# ── Lightweight heartbeat evidence (§59) ────────────────────────────────────


def record_no_new_candle(
    db: Any,
    *,
    bot_instance_id: str,
    symbol: str,
    timeframe: str,
    cycle_id: str | None = None,
    run_id: str | None = None,
    **extra: Any,
) -> TradingDecision:
    """Record a management-only heartbeat without a strategy payload.

    A NO_NEW_CANDLE tick happens every 10 seconds per symbol. It must prove the
    heartbeat ran and that no candle was available — nothing more. Storing the
    full component/candle blob here would be a write storm and would duplicate
    evidence that belongs to the actual new-candle evaluation.
    """
    from app.decision.reasons import CycleReason

    decision = TradingDecision(
        decision_id=new_decision_id(),
        bot_instance_id=bot_instance_id,
        symbol=str(symbol).upper(),
        evaluated_at=_now(),
        timeframe=timeframe,
        cycle_id=cycle_id,
        run_id=run_id,
        **extra,
    )
    decision.hold(CycleReason.NO_NEW_CANDLE)
    decision.complete = True
    decision.finalized_at = decision.evaluated_at
    try:
        persist_decision(db, decision)
    except Exception as exc:
        logger.error("[DECISION] failed to persist NO_NEW_CANDLE: %s", exc)
    return decision
