"""Lineage-safe CATI research export (Sections 21.18-21.20).

One row per TradePlan (per tenant), with the three outcome families kept
STRICTLY separate:

* ``market_outcome``    -- what the MARKET did after the decision (a research
                           label joined later; ``UNLABELED`` until then). A
                           hard-risk rejection or an execution failure never
                           changes it: a rejected-by-risk candidate is still a
                           valid market observation.
* ``execution_outcome`` -- what the BROKER did (not submitted / filled /
                           partial / rejected / submit-unknown ...). Never
                           written into the market label.
* ``account_outcome``   -- realized account PnL, only where a position existed.

``labels.setup_failure`` derives from the market outcome ONLY -- never from
a risk rejection or a broker failure. Every row carries versions and hashes.
Rows are tenant-scoped (``broker_account_id`` is mandatory) and sanitized;
only opaque account ids are exported, never credentials or personal
account metadata.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional

from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.observability.sanitize import sanitize_payload
from app.trading_intelligence.versions import RESEARCH_EXPORT_SCHEMA_VERSION

MARKET_OUTCOME_LABELS = ("TARGET_BEFORE_STOP", "STOP_BEFORE_TARGET", "TIMEOUT", "UNLABELED")


def _rows(db: Any, sql: str, params: tuple) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        out = [dict(r) for r in conn.execute(sql, params).fetchall()]
    for r in out:
        if "payload" in r:
            r["payload"] = json.loads(r["payload"])
    return out


def _execution_outcome(risk: Optional[Dict[str, Any]], attempt: Optional[Dict[str, Any]]) -> str:
    if attempt is not None:
        return attempt["status"]
    if risk is None:
        return "NOT_SUBMITTED_NO_RISK_EVALUATION"
    if risk["status"] == "REJECTED":
        return "NOT_SUBMITTED_RISK_REJECTED"
    return "NOT_SUBMITTED"


def _forecast_section(p: Mapping[str, Any], upstream: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Forecast / OOD evidence RECONSTRUCTED BY ID from the canonical upstream
    decision evidence (the exact OutcomeForecast + EconomicOpportunity +
    VetoDecision the admission and veto used). Nothing is recomputed here; with
    no upstream evidence the OOD fields are explicitly UNAVAILABLE, never 0."""
    out: Dict[str, Any] = {
        "p_net_profitable": p.get("p_net_profitable"),
        "credible_interval": [p.get("credible_interval_low"), p.get("credible_interval_high")],
        "raw_support": p.get("raw_support"), "ess": p.get("ess"), "backoff_level": p.get("backoff_level"),
        "forecast_id": p.get("forecast_id"),
    }
    if upstream is None:
        out.update(ood_score=None, ood_evidence_status="UPSTREAM_EVIDENCE_UNAVAILABLE")
        return out
    payload = upstream["payload"]
    fc, opp, veto = payload["outcome_forecast"], payload["economic_opportunity"], payload["veto"]
    shift = fc.get("distribution_shift_assessment") or {}
    if fc.get("forecast_id") != p.get("forecast_id") or opp.get("economic_opportunity_id") != p.get(
            "economic_opportunity_id"):
        out.update(ood_score=None, ood_evidence_status="UPSTREAM_LINEAGE_MISMATCH")
        return out
    out.update(
        ood_evidence_status="UPSTREAM_EVIDENCE",
        ood_score=shift.get("ood_score"), ood_severity=shift.get("severity"),
        ood_reason_codes=list(shift.get("reason_codes") or ()),
        ood_unseen_categories=list(shift.get("unseen_categories") or ()),
        ood_support_shift=shift.get("support_shift"),
        admission_ood_score=opp.get("ood_score"),
        distribution_shift_penalty_r=opp.get("distribution_shift_penalty_r"),
        forecast_uncertainty=fc.get("forecast_uncertainty"), forecast_status=fc.get("status"),
        cohort_signature=fc.get("cohort_signature"), library_hash=fc.get("library_hash"),
        calibration_status=fc.get("calibration_status"),
        veto_ood_checks=[{k: c.get(k) for k in ("check", "status", "observed_value", "policy_value", "reason_code")}
                         for c in veto.get("ood_checks") or ()],
        decision_evidence_id=upstream["decision_evidence_id"],
    )
    return out


def export_research_rows(db: Any, broker_account_id: str, *,
                         market_outcomes: Optional[Mapping[str, Mapping[str, Any]]] = None,
                         account_outcomes: Optional[Mapping[str, Mapping[str, Any]]] = None) -> List[Dict[str, Any]]:
    if not broker_account_id:
        raise ValueError("research export is tenant-scoped: broker_account_id is required")
    market_outcomes = market_outcomes or {}
    account_outcomes = account_outcomes or {}
    from app.trading_intelligence.evidence.stores import DecisionEvidenceStore

    decisions = DecisionEvidenceStore(db)
    plans = _rows(db, "SELECT * FROM cati_trade_plans WHERE broker_account_id=? ORDER BY created_at, trade_plan_id",
                  (broker_account_id,))
    out: List[Dict[str, Any]] = []
    for row in plans:
        p = row["payload"]
        pid = p["trade_plan_id"]
        risks = _rows(db, "SELECT * FROM cati_risk_decisions WHERE broker_account_id=? AND trade_plan_id=? "
                          "ORDER BY decision_time, risk_decision_id", (broker_account_id, pid))
        attempts = _rows(db, "SELECT * FROM cati_execution_attempts WHERE broker_account_id=? AND trade_plan_id=? "
                             "ORDER BY recorded_at, sequence", (broker_account_id, pid))
        attempt = attempts[-1] if attempts else None
        # the binding verdict: an executor-gate rejection supersedes the pre-execution approval
        risk = next((r for r in risks if r["payload"].get("stage") == "EXECUTOR_GATES"), risks[-1] if risks else None)
        position_id = next((a["position_id"] for a in reversed(attempts) if a["position_id"]), None)
        pfcs = _rows(db, "SELECT * FROM cati_position_forecasts WHERE broker_account_id=? AND trade_plan_id=? "
                         "ORDER BY forecast_time", (broker_account_id, pid))
        exds = _rows(db, "SELECT * FROM cati_exit_decisions WHERE broker_account_id=? AND trade_plan_id=? "
                         "ORDER BY decision_time", (broker_account_id, pid))
        upstream = decisions.for_opportunity(broker_account_id, p.get("economic_opportunity_id") or "")
        mo = dict(market_outcomes.get(pid) or {})
        m_label = str(mo.get("terminal_outcome") or "UNLABELED")
        if m_label not in MARKET_OUTCOME_LABELS:
            m_label = "UNLABELED"
        ap = attempt["payload"] if attempt else {}
        last_path = (pfcs[-1]["payload"].get("position_path") or {}) if pfcs else {}
        costs = p.get("expected_costs") or {}
        record = {
            "schema_version": RESEARCH_EXPORT_SCHEMA_VERSION,
            "row_id": short_id("rrow", {"plan": pid, "acct": broker_account_id}),
            "tenant": {"broker_account_id": broker_account_id, "bot_instance_id": p.get("bot_instance_id")},
            "lineage": {k: p.get(k) for k in (
                "run_id", "cycle_id", "snapshot_id", "market_state_id", "regime_distribution_id", "source_candidate_id",
                "forecast_id", "venue_observation_id", "cost_estimate_id", "economic_opportunity_id",
                "veto_decision_id", "ranking_batch_id", "ranked_opportunity_id", "portfolio_decision_id",
                "portfolio_reservation_id", "trade_plan_id")} | {
                "risk_decision_id": risk["risk_decision_id"] if risk else None,
                "runtime_session_id": risk["runtime_session_id"] if risk else None,
                "execution_attempt_id": attempt["execution_attempt_id"] if attempt else None,
                "position_id": position_id,
                "position_forecast_id": pfcs[-1]["position_forecast_id"] if pfcs else None,
                "exit_decision_id": exds[-1]["exit_decision_id"] if exds else None},
            "market": {"instrument": (p.get("instrument_key") or {}).get("canonical_symbol"),
                       "asset_class": (p.get("instrument_key") or {}).get("asset_class"), "venue": p.get("venue"),
                       "environment": p.get("environment"), "setup_family": p.get("setup_family"), "side": p.get("side"),
                       "decision_time": p.get("decision_time")},
            "forecast": _forecast_section(p, upstream),
            "economics": {"expected_gross_R": p.get("expected_gross_R"), "expected_net_R": p.get("expected_net_R"),
                          "conservative_edge_R": p.get("conservative_edge_R"), "planned_costs_R": costs},
            "decision": {"veto_decision_id": p.get("veto_decision_id"), "ranked_opportunity_id": p.get("ranked_opportunity_id"),
                         "portfolio_decision_id": p.get("portfolio_decision_id")},
            "risk": ({"status": risk["status"], "stage": risk["payload"].get("stage"),
                      "rejection_family": risk["rejection_family"], "reason_codes": risk["payload"].get("reason_codes")}
                     if risk else {"status": "NOT_EVALUATED"}),
            "execution": {"status": attempt["status"] if attempt else None,
                          "requested_price": ap.get("requested_price"), "filled_price": ap.get("filled_price"),
                          "requested_quantity": ap.get("requested_quantity"), "filled_quantity": ap.get("filled_quantity"),
                          "planned_costs_R": ap.get("planned_costs"), "realized_costs": ap.get("realized_costs")},
            "position": {"mfe_R": last_path.get("mfe_R"), "mae_R": last_path.get("mae_R"),
                         "exit_intent": exds[-1]["action"] if exds else None,
                         "exit_reasons": (exds[-1]["payload"].get("reason_codes") if exds else None),
                         "thesis_status": exds[-1]["thesis_status"] if exds else None},
            # -- the three outcome families, never mixed ------------------------------------------------
            "market_outcome": {"terminal_outcome": m_label, "net_R": mo.get("net_R"),
                               "label_policy_version": mo.get("label_policy_version"),
                               "market_observation_valid": True},
            "execution_outcome": {"status": _execution_outcome(risk, attempt),
                                  "hard_risk_rejected": bool(risk and risk["status"] == "REJECTED"),
                                  "rejection_family": risk["rejection_family"] if risk else None},
            "account_outcome": ({"realized_pnl": (account_outcomes.get(pid) or {}).get("realized_pnl")}
                                if position_id else {"realized_pnl": None, "position_existed": False}),
            "labels": {"setup_failure": (m_label == "STOP_BEFORE_TARGET") if m_label != "UNLABELED" else None},
            "versions": {"trade_plan_hash": p.get("trade_plan_hash"), "plan_versions": p.get("versions"),
                         "trade_plan_schema": p.get("schema_version"),
                         "risk_policy_versions": risk["payload"].get("policy_versions") if risk else None},
        }
        out.append(sanitize_payload(record))
    return out


__all__ = ["MARKET_OUTCOME_LABELS", "export_research_rows"]
