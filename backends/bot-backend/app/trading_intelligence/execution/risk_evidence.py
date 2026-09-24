"""RiskDecision EVIDENCE built from what the existing hard-risk stack decided
(Section 20.4). This module decides nothing: it classifies the reason code
the existing authority returned into a bounded family and records the
resolved sizing/stop/leverage the authority approved.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.execution import (
    RiskDecision, RiskDecisionStatus as S, RiskRejectionFamily as F, RiskStage,
)
from app.trading_intelligence.contracts.trade_plan import TradePlan
from app.trading_intelligence.versions import (
    EXECUTION_BOUNDARY_VERSION, RISK_DECISION_SCHEMA_VERSION, TRADE_PLAN_SCHEMA_VERSION,
)

_FAMILY_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    # (Section 18 prevalidation failures pass rejection_family=PREVALIDATION explicitly)
    (F.PREVALIDATION.value, ("PREVALIDATION", "TRADE_PLAN_HASH")),
    (F.ADAPTIVE_RISK.value, ("DAILY_RISK_BUDGET", "DAILY_RISK_RESERVATION", "ADAPTIVE")),
    (F.DAILY_LOSS.value, ("DAILY_LOSS", "DAILY_HARD_EQUITY_CAP", "KILL_SWITCH")),
    (F.DRAWDOWN.value, ("DRAWDOWN", "CONSECUTIVE_LOSS")),
    (F.SLOT.value, ("MAX_POSITIONS", "MAX_OPEN_POSITIONS", "SLOT")),
    (F.MARGIN.value, ("MARGIN", "CAPITAL_LEDGER", "INSUFFICIENT")),
    (F.LEVERAGE.value, ("LEVERAGE", "COMPOUND_RISK")),
    (F.INSTRUMENT.value, ("SYMBOL_NOT_ALLOWED", "MARKET_CLOSED", "NOT_LIVE_SYMBOL", "INSTRUMENT")),
    (F.SIZING.value, ("SIZE", "NOTIONAL", "ATR", "STOP", "EXPOSURE", "RISK_REWARD", "INVALID_RISK",
                      "INVALID_REWARD", "PORTFOLIO_RISK_BUDGET", "QTY", "SYMBOL_CONCENTRATION")),
)


def classify_rejection(reason_codes: Iterable[str]) -> str:
    codes = [str(c).upper() for c in reason_codes if c]
    for family, needles in _FAMILY_RULES:
        if any(n in c for c in codes for n in needles):
            return family
    return F.OTHER.value


def allocation_basis(validated_config: Any, executor: Any = None) -> Tuple[Tuple[str, str], ...]:
    """The allocation the EXISTING sizing uses, recorded verbatim.
    fixed_amount / fixed_usdt means: up to this allocation PER APPROVED
    TRADE -- never an aggregate bot budget (Section 20.5)."""
    out = {"semantics": "PER_APPROVED_TRADE"}
    if executor is not None:
        out["allocation_type"] = str(getattr(executor, "_allocation_type", ""))
        out["allocation_value"] = repr(float(getattr(executor, "_allocation_value", 0.0) or 0.0))
    if validated_config is not None:
        out["use_fixed_size"] = str(bool(getattr(validated_config, "use_fixed_size", False)))
        out["fixed_size_usdt"] = repr(getattr(validated_config, "fixed_size_usdt", None))
        out["capital_allocation_pct"] = repr(getattr(validated_config, "capital_allocation_pct", None))
        out["risk_level"] = str(getattr(getattr(validated_config, "risk_level", None), "value",
                                        getattr(validated_config, "risk_level", None)))
    return tuple(sorted(out.items()))


def build_risk_decision(
    plan: TradePlan, *, approved: bool, stage: str, reason_codes: Iterable[str], decision_time: int,
    runtime_session_id: Optional[str] = None, trade_params: Optional[Mapping[str, Any]] = None,
    allocation: Tuple[Tuple[str, str], ...] = (), risk_budget: Optional[float] = None,
    stop_tightened: bool = False, slot_reservation_id: Optional[str] = None,
    margin_reservation_id: Optional[str] = None, source: str = "TradingOrchestrator.process_trade_plan",
    policy_versions: Tuple[Tuple[str, str], ...] = (), rejection_family: Optional[str] = None,
) -> RiskDecision:
    codes = tuple(dict.fromkeys(str(c) for c in reason_codes if c))
    tp = dict(trade_params or {})
    stop = tp.get("stop_loss")
    entry = tp.get("entry_price")
    versions = tuple(sorted(dict(policy_versions, risk_decision_schema=RISK_DECISION_SCHEMA_VERSION,
                                 trade_plan_schema=TRADE_PLAN_SCHEMA_VERSION,
                                 execution_boundary=EXECUTION_BOUNDARY_VERSION).items()))
    return RiskDecision.build(
        trade_plan_id=plan.trade_plan_id, trade_plan_hash=plan.trade_plan_hash, user_id=plan.user_id,
        broker_account_id=plan.broker_account_id, bot_instance_id=plan.bot_instance_id,
        runtime_session_id=runtime_session_id, run_id=plan.run_id, cycle_id=plan.cycle_id,
        status=(S.APPROVED if approved else S.REJECTED).value, stage=stage,
        rejection_family=None if approved else (rejection_family or classify_rejection(codes)),
        reason_codes=codes, allocation_basis=allocation, risk_budget=risk_budget,
        resolved_stop_price=float(stop) if stop is not None else None,
        resolved_stop_distance=abs(float(entry) - float(stop)) if stop is not None and entry is not None else None,
        stop_tightened_by_hard_risk=bool(stop_tightened),
        resolved_leverage=float(tp["leverage"]) if tp.get("leverage") is not None else None,
        resolved_quantity=float(tp["quantity"]) if tp.get("quantity") is not None else None,
        resolved_notional=(float(tp["quantity"]) * float(entry)) if tp.get("quantity") is not None and entry else None,
        resolved_take_profit=float(tp["take_profit"]) if tp.get("take_profit") is not None else None,
        slot_reservation_id=slot_reservation_id, margin_reservation_id=margin_reservation_id,
        policy_versions=versions, source=source, decision_time=int(decision_time),
    )


__all__ = ["classify_rejection", "allocation_basis", "build_risk_decision", "RiskStage"]
