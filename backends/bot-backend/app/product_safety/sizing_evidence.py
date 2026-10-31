"""Durable sizing-refusal evidence for the readiness gate and the operations alerts (E-03).

Until ``6e0b42c`` the legacy runner wrote a ``SIZING_FAILURE`` audit event when sizing refused a trade, and the
user-capital readiness gate counted those events. Since CATI became the sole runtime authority nothing writes that
event: a sizing refusal is recorded as a RiskDecision of status ``REJECTED`` and ``rejection_family = 'SIZING'`` in the
append-only ``cati_risk_decisions`` table (``CATIExecutionBoundary._append_risk`` -> ``RiskDecisionStore.append``),
both when the orchestrator's hard-risk sizing refuses (stage ``PRE_EXECUTION``) and when the executor's own sizing
gate refuses at submission (stage ``EXECUTOR_GATES``). That row is the canonical evidence read here.

* Each refusal counts ONCE: rows are keyed by the deterministic ``risk_decision_id`` (the primary key; re-appending
  the same decision is an idempotent no-op in the store), and ids are de-duplicated again here.
* Nothing is lost after a restart: only durable rows are read, never process memory.
* Attribution is the row's own ``bot_instance_id`` / ``broker_account_id``; the strategy family comes from the
  plan's recorded ``setup_family`` in ``cati_trade_plans`` where that plan was recorded (``None`` otherwise).
* Legacy ``SIZING_FAILURE`` events are still read, but only as a COMPATIBILITY source and reported separately: a
  database whose paper period began before ``6e0b42c`` holds real refusals only there, and dropping them would loosen
  the gate. Nothing emits that event any more, so the check does not depend on it.

This module only reads. It never writes a risk decision, never changes a limit, and certifies nothing.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

RISK_TABLE = "cati_risk_decisions"
PLAN_TABLE = "cati_trade_plans"
SIZING_FAMILY = "SIZING"
REJECTED = "REJECTED"
#: the dead legacy event name, read for compatibility only
LEGACY_EVENT = "SIZING_FAILURE"


@dataclass(frozen=True)
class SizingRefusal:
    risk_decision_id: str
    broker_account_id: str
    bot_instance_id: str
    trade_plan_id: str
    decision_time: int
    stage: Optional[str]
    reason_codes: Tuple[str, ...]
    setup_family: Optional[str]


@dataclass
class SizingRefusalSummary:
    #: False when the durable table does not exist: the evidence cannot be read (the readiness gate fails closed)
    evidence_available: bool
    refusals: List[SizingRefusal] = field(default_factory=list)
    legacy_event_count: int = 0

    @property
    def durable_count(self) -> int:
        return len(self.refusals)

    @property
    def total(self) -> int:
        return self.durable_count + self.legacy_event_count

    def by_account(self) -> Dict[str, int]:
        return _count(r.broker_account_id for r in self.refusals)

    def by_family(self) -> Dict[str, int]:
        return _count(r.setup_family or "UNRECORDED" for r in self.refusals)

    def by_reason(self) -> Dict[str, int]:
        return _count(code for r in self.refusals for code in (r.reason_codes or ("UNSPECIFIED",)))

    def by_stage(self) -> Dict[str, int]:
        return _count(r.stage or "UNKNOWN" for r in self.refusals)


def _count(values: Iterable[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for v in values:
        out[str(v)] = out.get(str(v), 0) + 1
    return dict(sorted(out.items()))


def _table_exists(conn: Any, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _payload(text: Any) -> Dict[str, Any]:
    try:
        out = json.loads(text) if text else {}
    except (TypeError, ValueError):
        return {}
    return out if isinstance(out, dict) else {}


def read_sizing_refusals(conn: Any, *, bot_instance_id: Optional[str] = None,
                         broker_account_id: Optional[str] = None, since_ms: Optional[int] = None,
                         until_ms: Optional[int] = None) -> Tuple[bool, List[SizingRefusal]]:
    """The durable SIZING refusals, optionally scoped to one bot and/or one account and a ``decision_time`` window
    (epoch ms, inclusive). Returns ``(table_exists, refusals)``, each refusal once, ordered by time and id."""
    if not _table_exists(conn, RISK_TABLE):
        return False, []
    where = ["r.status = ?", "r.rejection_family = ?"]
    params: List[Any] = [REJECTED, SIZING_FAMILY]
    if bot_instance_id is not None:
        where.append("r.bot_instance_id = ?")
        params.append(str(bot_instance_id))
    if broker_account_id is not None:
        where.append("r.broker_account_id = ?")
        params.append(str(broker_account_id))
    if since_ms is not None:
        where.append("r.decision_time >= ?")
        params.append(int(since_ms))
    if until_ms is not None:
        where.append("r.decision_time <= ?")
        params.append(int(until_ms))
    with_plans = _table_exists(conn, PLAN_TABLE)
    # the plan join is on the plan id AND the account, so a plan row can never lend another tenant's family
    plan_cols, plan_join = ("p.payload", f" LEFT JOIN {PLAN_TABLE} p ON p.trade_plan_id = r.trade_plan_id "
                                         "AND p.broker_account_id = r.broker_account_id") if with_plans else ("NULL", "")
    sql = (f"SELECT r.risk_decision_id, r.broker_account_id, r.bot_instance_id, r.trade_plan_id, r.decision_time, "
           f"r.payload, {plan_cols} FROM {RISK_TABLE} r{plan_join} WHERE {' AND '.join(where)} "
           "ORDER BY r.decision_time, r.risk_decision_id")
    seen: set = set()
    out: List[SizingRefusal] = []
    for rid, account, bot, plan_id, at, payload, plan_payload in conn.execute(sql, tuple(params)).fetchall():
        if rid in seen:
            continue
        seen.add(rid)
        body = _payload(payload)
        family = _payload(plan_payload).get("setup_family") if plan_payload else None
        out.append(SizingRefusal(
            risk_decision_id=str(rid), broker_account_id=str(account), bot_instance_id=str(bot),
            trade_plan_id=str(plan_id), decision_time=int(at), stage=body.get("stage"),
            reason_codes=tuple(str(c) for c in (body.get("reason_codes") or ())),
            setup_family=str(family) if family else None))
    return True, out


def legacy_sizing_failure_events(conn: Any, *, bot_instance_id: str, since_iso: Optional[str] = None,
                                 until_iso: Optional[str] = None) -> int:
    """COMPATIBILITY: ``SIZING_FAILURE`` audit events of this bot (written by the legacy runner before ``6e0b42c``).
    A database without the events table holds none."""
    where, params = ["event_type = ?"], [LEGACY_EVENT]
    if since_iso is not None:
        where.append("timestamp_utc >= ?")
        params.append(since_iso)
    if until_iso is not None:
        where.append("timestamp_utc <= ?")
        params.append(until_iso)
    try:
        rows = conn.execute(f"SELECT details_json FROM events WHERE {' AND '.join(where)}", tuple(params)).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return 0
        raise
    return sum(1 for (details,) in (tuple(r) for r in rows) if _payload(details).get("bot_instance_id") == bot_instance_id)


def sizing_refusal_summary(db: Any, *, bot_instance_id: str, since_ms: Optional[int] = None,
                           since_iso: Optional[str] = None) -> SizingRefusalSummary:
    """Everything the readiness gate needs about one bot's sizing refusals since ``since_ms`` (durable rows) /
    ``since_iso`` (legacy events); ``None`` means the bot's whole history."""
    with db.connect() as conn:
        available, refusals = read_sizing_refusals(conn, bot_instance_id=bot_instance_id, since_ms=since_ms)
        legacy = legacy_sizing_failure_events(conn, bot_instance_id=bot_instance_id, since_iso=since_iso)
    return SizingRefusalSummary(evidence_available=available, refusals=refusals, legacy_event_count=legacy)


__all__ = ["LEGACY_EVENT", "SIZING_FAMILY", "SizingRefusal", "SizingRefusalSummary", "legacy_sizing_failure_events",
           "read_sizing_refusals", "sizing_refusal_summary"]
