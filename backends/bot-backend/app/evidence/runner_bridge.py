"""Bridge the live runner onto the canonical evidence layer.

The runner's ``step_symbol`` has ~30 early-return branches spread over two
thousand lines. Rather than edit every one of them — which would be both risky
and easy to leave incomplete — this module wraps the single call boundary that
*all* of them pass through. Every symbol evaluation therefore finalizes exactly
one canonical decision, including the branches nobody remembered to update and
the ones that raise.

Two responsibilities:

* map the runner's loose result dicts onto the canonical reason taxonomy;
* pull the rich evidence (snapshot, opportunity, entry quality) the
  orchestrated path already produces into the canonical row.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping

from app.decision.reason_mapping import to_canonical
from app.decision.reasons import CycleReason, QualityReason
from app.evidence.decision_recorder import record_decision, record_no_new_candle
from shared_lib.persistence.evidence_schema import (
    LIVE_MAINNET,
    PAPER_FORWARD,
    TESTNET,
)

logger = logging.getLogger(__name__)

#: Runner-local strings that predate the canonical taxonomy.
_LEGACY_REASONS: dict[str, str] = {
    "SYMBOL_LOCK_BUSY": CycleReason.SYMBOL_LOCK_BUSY,
    "SYMBOL_LOCK_TIMEOUT": CycleReason.SYMBOL_LOCK_BUSY,
    "STOP_REQUESTED": CycleReason.STOP_REQUESTED,
    "NO_NEW_CANDLE": CycleReason.NO_NEW_CANDLE,
    "CONSECUTIVE_LOSS_COOLDOWN": "RISK_CONSECUTIVE_LOSS_LIMIT",
    "ERROR_STRATEGY_UNAVAILABLE": "EXECUTION_ERROR",
    "strategy_exception": "EXECUTION_ERROR",
    "daily_loss_limit_reached": "RISK_DAILY_LOSS_LIMIT",
    "POST_EVENT_VOLATILITY": QualityReason.EVENT_BLACKOUT,
    "EVENT_BLACKOUT_EXEC": QualityReason.EVENT_BLACKOUT,
}

#: Runner ``decision`` values that mean the trade was actually approved.
_APPROVED_DECISIONS = frozenset({"execute", "EXECUTE", "OPEN", "BUY", "SELL"})


def resolve_provenance(execution_mode: str | None, broker_environment: str | None) -> str:
    """Classify where this evidence came from.

    Paper stays PAPER_FORWARD; broker execution splits on the environment so a
    demo/testnet run is never mistaken for real-money evidence.
    """
    mode = str(execution_mode or "paper").lower()
    if mode != "broker":
        return PAPER_FORWARD
    env = str(broker_environment or "").lower()
    if env in {"live", "mainnet", "production"}:
        return LIVE_MAINNET
    return TESTNET


def canonical_reason(result: Mapping[str, Any]) -> str:
    """Best available machine-readable reason for one symbol result.

    Falls through explicit reason_code, then reason, then decision. Returns
    EXECUTION_ERROR for error payloads. Never returns None or an empty string —
    an unknown reason is a defect the integrity checker must be able to see.
    """
    for key in ("reason_code", "primary_reason", "reason"):
        raw = result.get(key)
        if raw:
            mapped = _LEGACY_REASONS.get(str(raw))
            if mapped:
                return mapped
            canonical = to_canonical(str(raw))
            if canonical:
                return canonical

    if result.get("error"):
        return "EXECUTION_ERROR"

    decision = str(result.get("decision") or "").upper()
    if decision in {"EXECUTE", "OPEN", "BUY", "SELL"}:
        return QualityReason.APPROVED_FOR_EXECUTION
    if decision == "ERROR":
        return "EXECUTION_ERROR"
    if decision:
        return decision
    return QualityReason.NO_OPPORTUNITY


def final_action(result: Mapping[str, Any]) -> str:
    if result.get("error"):
        return "ERROR"
    decision = str(result.get("decision") or "").upper()
    if decision in {d.upper() for d in _APPROVED_DECISIONS}:
        return "APPROVED"
    if decision.startswith("CLOSE"):
        return "CLOSE"
    if decision == "ERROR":
        return "ERROR"
    if result.get("skipped"):
        return "SKIPPED"
    return "HOLD"


def _strategy_meta(result: Mapping[str, Any]) -> dict[str, Any]:
    """Dig the ensemble meta out of the orchestrated result, if present."""
    details = result.get("details")
    if not isinstance(details, Mapping):
        return {}
    strategy_output = (details.get("details") or details).get("strategy_output")
    if isinstance(strategy_output, Mapping):
        meta = strategy_output.get("meta")
        if isinstance(meta, Mapping):
            return dict(meta)
    return {}


def apply_evidence(decision: Any, result: Mapping[str, Any], evidence: Mapping[str, Any]) -> None:
    """Copy runtime evidence onto the canonical decision builder."""
    snapshot = evidence.get("snapshot")
    if snapshot is not None:
        decision.set_snapshot(snapshot)

    opportunity = evidence.get("opportunity")
    if opportunity is not None:
        decision.set_opportunity(opportunity)

    quality = evidence.get("entry_quality")
    if quality is not None:
        decision.set_entry_quality(quality)

    # The ensemble also publishes a flattened meta dict; use it to fill in
    # anything the objects above did not supply (legacy/compat callers).
    meta = _strategy_meta(result)
    if meta:
        decision.regime = decision.regime or meta.get("regime")
        if decision.regime_confidence is None:
            decision.regime_confidence = meta.get("regime_confidence")
        if decision.buy_score is None:
            decision.buy_score = meta.get("buy_score")
        if decision.sell_score is None:
            decision.sell_score = meta.get("sell_score")
        if decision.effective_entry_threshold is None:
            decision.effective_entry_threshold = meta.get("threshold")
        if decision.opportunity_id is None:
            decision.opportunity_id = meta.get("opportunity_id")
        if decision.market_snapshot_id is None:
            decision.market_snapshot_id = meta.get("market_snapshot_id")
        entry_quality = meta.get("entry_quality")
        if isinstance(entry_quality, Mapping) and decision.quality_result is None:
            decision.quality_result = "PASS" if entry_quality.get("approved") else "FAIL"
            decision.quality_reason = entry_quality.get("primary_reason")
            if decision.raw_confidence is None:
                decision.raw_confidence = entry_quality.get("raw_confidence")

    for field in ("execution_attempt_id", "order_id", "position_id"):
        value = evidence.get(field)
        if value:
            setattr(decision, field, value)


def record_symbol_evaluation(
    runner: Any,
    symbol: str,
    *,
    evaluate,
):
    """Run one symbol evaluation and finalize exactly one canonical decision.

    ``evaluate`` is the runner's real ``step_symbol`` body. Whatever it returns
    — or raises — one finalized row is written.
    """
    context = getattr(runner, "context", None)
    if context is None:
        # A contextless (legacy/global) runner has no bot to attribute evidence
        # to. It never trades; do not manufacture a decision row for it.
        return evaluate(symbol)

    db = getattr(runner, "db", None)
    if db is None:
        return evaluate(symbol)

    execution_mode = runner._effective_execution_mode()
    provenance = resolve_provenance(execution_mode, getattr(context, "broker_environment", None))

    runner._symbol_evidence = getattr(runner, "_symbol_evidence", {})
    runner._symbol_evidence[symbol] = {}

    result: Mapping[str, Any] = {}
    try:
        with record_decision(
            db,
            bot_instance_id=context.bot_instance_id,
            symbol=symbol,
            user_id=getattr(context, "user_id", None),
            broker_account_id=getattr(context, "broker_account_id", None),
            run_id=getattr(runner, "run_id", None),
            cycle_id=getattr(runner, "cycle_id", None),
            runtime_session_id=getattr(runner, "runtime_session_id", None),
            policy_hash=getattr(context, "effective_policy_hash", None) or None,
            market_type=getattr(context, "market_type", None),
            timeframe=getattr(runner, "interval", None),
            provenance=provenance,
            execution_mode=execution_mode,
            broker_environment=getattr(context, "broker_environment", None),
        ) as decision:
            result = evaluate(symbol) or {}
            apply_evidence(decision, result, runner._symbol_evidence.get(symbol, {}))

            reason = canonical_reason(result)
            action = final_action(result)
            legacy = str(result.get("reason") or result.get("error") or "") or None

            if action == "APPROVED":
                decision.approve(reason)
            elif action in {"ERROR", "SKIPPED"}:
                decision.reject(reason, legacy=legacy)
            else:
                decision.hold(reason, legacy=legacy)
            decision.final_action = action
    except Exception:
        # record_decision already finalized an EXECUTION_ERROR row; let the
        # runner's own handling in run_cycle take it from here.
        raise

    return result


def maybe_record_no_new_candle(runner: Any, symbol: str) -> None:
    """Cheap heartbeat evidence for a symbol with no newly closed candle."""
    context = getattr(runner, "context", None)
    db = getattr(runner, "db", None)
    if context is None or db is None:
        return
    try:
        record_no_new_candle(
            db,
            bot_instance_id=context.bot_instance_id,
            symbol=symbol,
            timeframe=getattr(runner, "interval", None) or "",
            run_id=getattr(runner, "run_id", None),
            cycle_id=getattr(runner, "cycle_id", None),
            provenance=resolve_provenance(
                runner._effective_execution_mode(),
                getattr(context, "broker_environment", None),
            ),
        )
    except Exception as exc:
        logger.error("[EVIDENCE] %s: NO_NEW_CANDLE record failed: %s", symbol, exc)
