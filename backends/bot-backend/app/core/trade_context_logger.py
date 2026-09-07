"""
Trade Context Logger — E-1 Entry Decision Trace

Captures a comprehensive decision context at the moment a trade entry is
confirmed (order placed or paper-simulated) and persists it via the audit
system. This supplements the existing TraceRecorder with a flat, structured
record that is easy to query for performance analysis.

Every successful trade entry must call `log_entry_decision_context()`.
Every persistence failure must be logged at ERROR level and must not
prevent the trade from being treated as executed.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def log_entry_decision_context(
    *,
    audit: Any,                           # shared_lib.persistence.audit.Audit
    run_id: Optional[str],
    cycle_id: Optional[str],
    symbol: str,
    side: str,
    action: str,
    # Bot / user identity
    bot_instance_id: Optional[str] = None,
    user_id: Optional[str] = None,
    # Strategy
    strategy_name: Optional[str] = None,
    strategy_version: Optional[str] = None,
    confidence_score: Optional[float] = None,
    min_required_confidence: Optional[float] = None,
    # Market context
    market_regime: Optional[str] = None,
    market_regime_confidence: Optional[float] = None,
    atr_at_entry: Optional[float] = None,
    signal_interval: Optional[str] = None,
    higher_timeframe_interval: Optional[str] = None,
    # Trade structure
    intended_entry_price: Optional[float] = None,
    actual_entry_price: Optional[float] = None,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    take_profit_2: Optional[float] = None,
    position_size: Optional[float] = None,
    quantity: Optional[float] = None,
    notional: Optional[float] = None,
    leverage: Optional[float] = None,
    trade_amount_mode: Optional[str] = None,
    trade_amount_per_position: Optional[float] = None,
    # Risk / R:R
    gross_risk_reward: Optional[float] = None,
    min_required_risk_reward: Optional[float] = None,
    risk_amount: Optional[float] = None,
    reward_amount: Optional[float] = None,
    risk_per_trade_pct: Optional[float] = None,
    # Drawdown state
    daily_drawdown_pct: Optional[float] = None,
    weekly_drawdown_pct: Optional[float] = None,
    monthly_drawdown_pct: Optional[float] = None,
    consecutive_losses: Optional[int] = None,
    # Execution context
    broker_id: Optional[str] = None,
    environment: Optional[str] = None,
    trace_id: Optional[str] = None,
    position_id: Optional[str] = None,
    order_id: Optional[str] = None,
    # Execution quality
    estimated_fee: Optional[float] = None,
    estimated_slippage: Optional[float] = None,
    spread_pct: Optional[float] = None,
    # Policy
    policy_decision: Optional[str] = None,
    policy_reason: Optional[str] = None,
    approval_reason: Optional[str] = None,
    risk_checks_passed: Optional[list] = None,
    # Raw payloads (optional — stored in metadata_json)
    raw_strategy_payload: Optional[Dict] = None,
    raw_policy_payload: Optional[Dict] = None,
    raw_execution_response: Optional[Dict] = None,
) -> bool:
    """
    Write a comprehensive entry decision trace to the audit system.

    Returns True if the trace was written, False if it failed.
    Failures are logged at ERROR level and never suppress the trade.
    """
    logger.debug("DECISION_TRACE_WRITE_ATTEMPTED symbol=%s side=%s bot=%s", symbol, side, bot_instance_id)

    try:
        _missing: list[str] = []
        for name, val in [
            ("bot_instance_id", bot_instance_id),
            ("strategy_name", strategy_name),
            ("confidence_score", confidence_score),
            ("market_regime", market_regime),
            ("atr_at_entry", atr_at_entry),
            ("stop_loss", stop_loss),
            ("actual_entry_price", actual_entry_price),
        ]:
            if val is None:
                _missing.append(name)
        if _missing:
            logger.debug(
                "DECISION_TRACE_MISSING_OPTIONAL_FIELD symbol=%s fields=%s",
                symbol, _missing,
            )

        metadata = {
            "decision_trace_id": trace_id,
            "bot_instance_id": bot_instance_id,
            "user_id": user_id,
            "symbol": symbol,
            "side": side,
            "action": action,
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "confidence_score": confidence_score,
            "min_required_confidence": min_required_confidence,
            "market_regime": market_regime,
            "market_regime_confidence": market_regime_confidence,
            "atr_at_entry": atr_at_entry,
            "signal_interval": signal_interval,
            "higher_timeframe_interval": higher_timeframe_interval,
            "intended_entry_price": intended_entry_price,
            "actual_entry_price": actual_entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "take_profit_2": take_profit_2,
            "position_size": position_size,
            "quantity": quantity,
            "notional": notional,
            "leverage": leverage,
            "trade_amount_mode": trade_amount_mode,
            "trade_amount_per_position": trade_amount_per_position,
            "gross_risk_reward": gross_risk_reward,
            "min_required_risk_reward": min_required_risk_reward,
            "risk_amount": risk_amount,
            "reward_amount": reward_amount,
            "risk_per_trade_pct": risk_per_trade_pct,
            "daily_drawdown_pct": daily_drawdown_pct,
            "weekly_drawdown_pct": weekly_drawdown_pct,
            "monthly_drawdown_pct": monthly_drawdown_pct,
            "consecutive_losses": consecutive_losses,
            "broker_id": broker_id,
            "environment": environment,
            "position_id": position_id,
            "order_id": order_id,
            "estimated_fee": estimated_fee,
            "estimated_slippage": estimated_slippage,
            "spread_pct": spread_pct,
            "policy_decision": policy_decision,
            "policy_reason": policy_reason,
            "approval_reason": approval_reason,
            "risk_checks_passed": risk_checks_passed,
            "raw_strategy_payload": raw_strategy_payload,
            "raw_policy_payload": raw_policy_payload,
            "raw_execution_response": raw_execution_response,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        audit.event(
            event_type="TRADE_ENTRY_DECISION_CONTEXT",
            run_id=run_id,
            cycle_id=cycle_id,
            symbol=symbol,
            action=action,
            details=metadata,
            trace_id=trace_id,
        )

        logger.info(
            "DECISION_TRACE_WRITTEN symbol=%s side=%s bot=%s trace=%s order=%s "
            "strategy=%s confidence=%.3f regime=%s atr=%.4f sl=%.4f entry=%.4f rr=%s",
            symbol, side, bot_instance_id, trace_id, order_id,
            strategy_name, confidence_score or 0.0, market_regime,
            atr_at_entry or 0.0, stop_loss or 0.0, actual_entry_price or 0.0,
            f"{gross_risk_reward:.2f}" if gross_risk_reward else "n/a",
        )
        logger.debug("TRADE_EXECUTION_CONTEXT_CAPTURED symbol=%s bot=%s", symbol, bot_instance_id)
        return True

    except Exception as exc:
        logger.exception(
            "DECISION_TRACE_WRITE_FAILED symbol=%s side=%s bot=%s trace=%s error=%s",
            symbol, side, bot_instance_id, trace_id, exc,
        )
        return False
