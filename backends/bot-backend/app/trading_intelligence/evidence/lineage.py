"""Full CATI lineage reconstruction (Section 21.1).

    runtime_session -> bot_run -> cycle -> market_snapshot -> market_state
    -> regime_distribution -> setup_candidate -> outcome_forecast
    -> venue_economic_observation -> cost_estimate -> economic_opportunity
    -> veto_decision -> ranking_batch -> portfolio_decision
    -> portfolio_reservation -> trade_plan -> risk_decision
    -> execution_attempt -> position -> position_forecast -> exit_decision

Every downstream record points backward: the immutable TradePlan carries
the snapshot..reservation ids (Section 18), RiskDecision carries
trade_plan + runtime_session / bot_run / cycle, ExecutionAttempt carries
trade_plan + risk_decision and (once filled) position, PositionForecast
carries position + trade_plan + path, ExitDecision carries
position_forecast + position + trade_plan.

The reconstruction is TENANT-SCOPED: every row it reads must belong to the
requested broker account; a row from another tenant is reported as a
violation (and never followed). A referenced parent that is absent is
reported as missing -- never silently skipped.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

CHAIN: Tuple[str, ...] = (
    "runtime_session", "bot_run", "cycle", "market_snapshot", "market_state", "regime_distribution",
    "setup_candidate", "outcome_forecast", "venue_economic_observation", "cost_estimate", "economic_opportunity",
    "veto_decision", "ranking_batch", "portfolio_decision", "portfolio_reservation", "trade_plan", "risk_decision",
    "execution_attempt", "position", "position_forecast", "exit_decision",
)

_PLAN_KEYS = {
    "market_snapshot": "snapshot_id", "market_state": "market_state_id", "regime_distribution": "regime_distribution_id",
    "setup_candidate": "source_candidate_id", "outcome_forecast": "forecast_id",
    "venue_economic_observation": "venue_observation_id", "cost_estimate": "cost_estimate_id",
    "economic_opportunity": "economic_opportunity_id", "veto_decision": "veto_decision_id",
    "ranking_batch": "ranking_batch_id", "portfolio_decision": "portfolio_decision_id",
    "portfolio_reservation": "portfolio_reservation_id", "cycle": "cycle_id", "bot_run": "run_id",
}


@dataclass
class LineageReport:
    broker_account_id: str
    ids: Dict[str, Optional[str]] = field(default_factory=dict)
    missing: List[str] = field(default_factory=list)
    tenant_violations: List[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return not self.missing and not self.tenant_violations and all(self.ids.get(k) for k in CHAIN)

    def chain(self) -> List[Tuple[str, Optional[str]]]:
        return [(k, self.ids.get(k)) for k in CHAIN]


def _one(db: Any, sql: str, params: tuple) -> Optional[Dict[str, Any]]:
    with db.connect() as conn:
        row = conn.execute(sql, params).fetchone()
    return dict(row) if row is not None else None


def _all(db: Any, sql: str, params: tuple) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _own(report: LineageReport, name: str, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    if row.get("broker_account_id") != report.broker_account_id:
        report.tenant_violations.append(f"{name}:{row.get('broker_account_id')}")
        return None
    return row


def reconstruct_from_exit_decision(db: Any, broker_account_id: str, exit_decision_id: str) -> LineageReport:
    rep = LineageReport(broker_account_id)
    exd = _own(rep, "exit_decision", _one(db, "SELECT * FROM cati_exit_decisions WHERE exit_decision_id=?",
                                            (exit_decision_id,)))
    if exd is None:
        rep.missing.append("exit_decision")
        return rep
    rep.ids["exit_decision"] = exd["exit_decision_id"]
    pfc = _own(rep, "position_forecast", _one(db, "SELECT * FROM cati_position_forecasts WHERE position_forecast_id=?",
                                                (exd["position_forecast_id"],)))
    if pfc is None:
        rep.missing.append("position_forecast")
    else:
        rep.ids["position_forecast"] = pfc["position_forecast_id"]
        if pfc["trade_plan_id"] != exd["trade_plan_id"] or pfc["position_id"] != exd["position_id"]:
            rep.missing.append("position_forecast:LINEAGE_INCONSISTENT")
    rep.ids["position"] = exd["position_id"]
    _fill_from_plan(db, rep, exd["trade_plan_id"], position_id=exd["position_id"])
    return rep


def reconstruct_from_trade_plan(db: Any, broker_account_id: str, trade_plan_id: str) -> LineageReport:
    rep = LineageReport(broker_account_id)
    _fill_from_plan(db, rep, trade_plan_id)
    if rep.ids.get("position"):
        exd = _all(db, "SELECT * FROM cati_exit_decisions WHERE broker_account_id=? AND position_id=? "
                       "ORDER BY decision_time", (broker_account_id, rep.ids["position"]))
        pfc = _all(db, "SELECT * FROM cati_position_forecasts WHERE broker_account_id=? AND position_id=? "
                       "ORDER BY forecast_time", (broker_account_id, rep.ids["position"]))
        if pfc:
            rep.ids["position_forecast"] = pfc[-1]["position_forecast_id"]
        if exd:
            rep.ids["exit_decision"] = exd[-1]["exit_decision_id"]
    return rep


def _fill_from_plan(db: Any, rep: LineageReport, trade_plan_id: str, position_id: Optional[str] = None) -> None:
    plan = _own(rep, "trade_plan", _one(db, "SELECT * FROM cati_trade_plans WHERE trade_plan_id=?", (trade_plan_id,)))
    if plan is None:
        rep.missing.append("trade_plan")
        return
    rep.ids["trade_plan"] = plan["trade_plan_id"]
    payload = json.loads(plan["payload"])
    for name, key in _PLAN_KEYS.items():
        rep.ids[name] = payload.get(key) or None
        if not rep.ids[name] and name not in ("bot_run",):
            rep.missing.append(name)
    risks = _all(db, "SELECT * FROM cati_risk_decisions WHERE trade_plan_id=? ORDER BY decision_time", (trade_plan_id,))
    for r in risks:
        if r["broker_account_id"] != rep.broker_account_id:
            rep.tenant_violations.append(f"risk_decision:{r['broker_account_id']}")
    risks = [r for r in risks if r["broker_account_id"] == rep.broker_account_id]
    if risks:
        rep.ids["risk_decision"] = risks[0]["risk_decision_id"]
        rep.ids["runtime_session"] = risks[0]["runtime_session_id"]
        rep.ids["bot_run"] = rep.ids.get("bot_run") or risks[0]["run_id"]
    attempts = _all(db, "SELECT * FROM cati_execution_attempts WHERE trade_plan_id=? ORDER BY recorded_at, sequence",
                    (trade_plan_id,))
    for a in attempts:
        if a["broker_account_id"] != rep.broker_account_id:
            rep.tenant_violations.append(f"execution_attempt:{a['broker_account_id']}")
    attempts = [a for a in attempts if a["broker_account_id"] == rep.broker_account_id]
    if attempts:
        rep.ids["execution_attempt"] = attempts[-1]["execution_attempt_id"]
        if not risks:
            rep.missing.append("risk_decision")  # an attempt without its hard-risk verdict
        if attempts[-1]["risk_decision_id"] and rep.ids.get("risk_decision") and \
                attempts[-1]["risk_decision_id"] not in {r["risk_decision_id"] for r in risks}:
            rep.missing.append("risk_decision:REFERENCED_BY_ATTEMPT")
        pos = next((a["position_id"] for a in reversed(attempts) if a["position_id"]), None)
        if pos:
            rep.ids["position"] = pos
            if position_id and position_id != pos:
                rep.missing.append("position:LINEAGE_INCONSISTENT")


__all__ = ["CHAIN", "LineageReport", "reconstruct_from_exit_decision", "reconstruct_from_trade_plan"]
