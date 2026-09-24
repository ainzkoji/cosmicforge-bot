"""ForwardDemoCertificationTracker and operational-defect evidence (22.11.6, Gate I).

Forward demo = production code, real broker conditions, NO user capital
(DEMO / TESTNET environments only; REAL-environment evidence is excluded and
counted separately). It reads the Section 20-21 evidence tables; it never
writes, and it cannot manufacture time:

* elapsed days are measured between the FIRST and LAST recorded evidence
  timestamps -- a caller-supplied clock can only CLAMP (``now_ms`` in the
  future is refused), never extend the period;
* executions are counted only from recorded ``cati_execution_attempts``.

If 30 days pass without enough executed opportunities, the period is
extended -- the evidence floor is never lowered.
"""
from __future__ import annotations

import json
import time
from collections import Counter
from typing import Any, Dict, Optional, Sequence

from app.trading_intelligence.versions import FORWARD_DEMO_TRACKER_VERSION

DAY_MS = 86_400_000
NO_CAPITAL_ENVIRONMENTS = ("DEMO", "TESTNET")
EXECUTED_STATUSES = ("FILLED", "PARTIALLY_FILLED", "RECONCILED_POSITION_EXISTS")


def _rows(db: Any, sql: str, params: Sequence[Any] = ()) -> list:
    try:
        with db.connect() as conn:
            return [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    except Exception as exc:  # table absent: evidence unavailable, never "zero defects"
        raise EvidenceUnavailable(str(exc)) from exc


class EvidenceUnavailable(RuntimeError):
    pass


class ForwardDemoCertificationTracker:
    def __init__(self, db: Any, *, venue: str, symbols: Optional[Sequence[str]] = None,
                 min_days: int = 30, min_executed: Optional[int] = None,
                 environments: Sequence[str] = NO_CAPITAL_ENVIRONMENTS, broker_account_id: Optional[str] = None):
        self._db = db
        self.venue = str(venue).upper()
        self.symbols = tuple(sorted({s.upper() for s in symbols})) if symbols else None
        self.min_days = int(min_days)
        self.min_executed = min_executed
        self.environments = tuple(e.upper() for e in environments)
        #: tenant scope: evidence of one account never counts toward another's demo
        self.broker_account_id = broker_account_id
        if any(e == "REAL" for e in self.environments):
            raise ValueError("forward demo uses no user capital: REAL is not a forward-demo environment")

    def snapshot(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        wall = int(time.time() * 1000)
        if now_ms is not None and int(now_ms) > wall + 60_000:
            raise ValueError("forward-demo clock cannot be set in the future (elapsed days cannot be fabricated)")
        now = min(int(now_ms or wall), wall)
        base = {"tracker_version": FORWARD_DEMO_TRACKER_VERSION, "venue": self.venue, "environments": list(self.environments),
                "broker_account_id": self.broker_account_id,
                "min_days": self.min_days, "min_executed": self.min_executed}
        try:
            plans = _rows(self._db, "SELECT trade_plan_id, broker_account_id, canonical_symbol, venue, environment, "
                                    "created_at FROM cati_trade_plans ORDER BY created_at")
        except EvidenceUnavailable:
            return {**base, "status": "REQUIRED", "reason": "FORWARD_DEMO_EVIDENCE_UNAVAILABLE", "elapsed_days": 0.0,
                    "executed_count": 0}
        in_scope, excluded_real = [], 0
        for p in plans:
            if str(p["environment"]).upper() == "REAL":
                excluded_real += 1
                continue
            if str(p["environment"]).upper() not in self.environments or str(p["venue"]).upper() != self.venue:
                continue
            if self.broker_account_id is not None and p["broker_account_id"] != self.broker_account_id:
                continue
            if self.symbols and not any(s in str(p["canonical_symbol"]).upper().replace("/", "") for s in self.symbols):
                continue
            if int(p["created_at"]) <= now:
                in_scope.append(p)
        ids = {p["trade_plan_id"] for p in in_scope}
        attempts = [a for a in _safe(self._db, "SELECT * FROM cati_execution_attempts ORDER BY recorded_at, sequence")
                    if a.get("trade_plan_id") in ids and int(a["recorded_at"]) <= now]
        risks = [r for r in _safe(self._db, "SELECT trade_plan_id, status, decision_time FROM cati_risk_decisions")
                 if r.get("trade_plan_id") in ids]
        exits = [e for e in _safe(self._db, "SELECT trade_plan_id, action, decision_time FROM cati_exit_decisions")
                 if e.get("trade_plan_id") in ids]
        stamps = [int(p["created_at"]) for p in in_scope] + [int(a["recorded_at"]) for a in attempts]
        first, last = (min(stamps), max(stamps)) if stamps else (None, None)
        elapsed = ((last - first) / DAY_MS) if stamps else 0.0
        latest: Dict[str, Dict[str, Any]] = {}
        for a in attempts:
            latest[a["trade_plan_id"]] = a
        statuses = Counter(a["status"] for a in latest.values())
        executed = sum(statuses.get(s, 0) for s in EXECUTED_STATUSES)
        planned, realized = [], []
        for a in attempts:
            payload = json.loads(a["payload"]) if isinstance(a.get("payload"), str) else {}
            if payload.get("planned_costs") is not None and payload.get("realized_costs") is not None:
                planned.append(payload["planned_costs"])
                realized.append(payload["realized_costs"])
        snap = {**base, "evidence_start_ms": first, "evidence_last_ms": last, "elapsed_days": round(elapsed, 4),
                "trade_plans": len(in_scope), "risk_decisions": len(risks),
                "risk_approved": sum(1 for r in risks if r["status"] == "APPROVED"), "executions": len(attempts),
                "executed_count": executed, "fills": statuses.get("FILLED", 0) + statuses.get("PARTIALLY_FILLED", 0),
                "protection_failures": statuses.get("PROTECTION_FAILED_ROLLED_BACK", 0),
                "unresolved_submit_unknown": statuses.get("SUBMIT_UNKNOWN", 0),
                "reconciled": statuses.get("RECONCILED_POSITION_EXISTS", 0) + statuses.get("RECONCILED_NO_POSITION", 0),
                "exit_decisions": len(exits), "cost_pairs": len(planned),
                "planned_vs_realized_costs": {"planned": planned[-20:], "realized": realized[-20:]},
                "excluded_real_environment_plans": excluded_real}
        if not in_scope:
            snap.update(status="REQUIRED", reason="NO_FORWARD_DEMO_EVIDENCE")
        elif elapsed < self.min_days:
            snap.update(status="IN_PROGRESS", reason="FORWARD_DEMO_DAYS_INSUFFICIENT")
        elif self.min_executed is None:
            snap.update(status="IN_PROGRESS", reason="CERTIFICATION_POLICY_INCOMPLETE:min_forward_demo_executed_count")
        elif executed < self.min_executed:
            snap.update(status="IN_PROGRESS", reason="FORWARD_DEMO_EVIDENCE_INSUFFICIENT_EXTEND_PERIOD")
        else:
            snap.update(status="PASS", reason=None)
        return snap


def _safe(db: Any, sql: str) -> list:
    try:
        return _rows(db, sql)
    except EvidenceUnavailable:
        return []


def operational_defects(db: Any) -> Dict[str, Any]:
    """Unresolved reconciliation / protection / idempotency defects in the
    runtime evidence. Raises EvidenceUnavailable when the tables are absent
    (absence of evidence is never reported as zero defects)."""
    attempts = _rows(db, "SELECT trade_plan_id, execution_attempt_id, status, recorded_at, sequence "
                         "FROM cati_execution_attempts ORDER BY recorded_at, sequence")
    pending = _rows(db, "SELECT reservation_id FROM cati_portfolio_reservations WHERE status='RESOLUTION_PENDING'")
    latest: Dict[str, str] = {}
    filled_attempts: Dict[str, set] = {}
    for a in attempts:
        latest[a["trade_plan_id"]] = a["status"]
        if a["status"] == "FILLED":
            filled_attempts.setdefault(a["trade_plan_id"], set()).add(a["execution_attempt_id"])
    unresolved_unknown = sum(1 for s in latest.values() if s == "SUBMIT_UNKNOWN")
    # idempotency: one TradePlan must never produce two distinct filled entry attempts
    duplicate_fills = sum(1 for ids in filled_attempts.values() if len(ids) > 1)
    return {"unresolved_submit_outcomes": len(pending), "reconciliation_defects": unresolved_unknown,
            "protection_defects": 0, "idempotency_defects": duplicate_fills,
            "total": len(pending) + unresolved_unknown + duplicate_fills,
            "note": "protection failures roll back (PROTECTION_FAILED_ROLLED_BACK) and are counted by the tracker"}


__all__ = ["ForwardDemoCertificationTracker", "operational_defects", "EvidenceUnavailable", "NO_CAPITAL_ENVIRONMENTS"]
