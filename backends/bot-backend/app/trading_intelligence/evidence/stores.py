"""Append-only CATI analytical evidence stores (Sections 21.2-21.3, 21.20).

Schema comes ONLY from the canonical migration
(``shared_lib.persistence.cati_schema``); nothing here issues DDL, and a
missing table fails closed with ``CATIEvidenceSchemaMissing``.

* Rows are written once, keyed by the record's deterministic id. Re-appending
  the identical record is an idempotent no-op; the same id with DIFFERENT
  content raises ``EvidenceConflict`` (old decisions are never rewritten to
  reflect new knowledge -- new knowledge is a new row). Database triggers
  additionally refuse UPDATE / DELETE.
* Every payload passes through ``sanitize_payload`` (no credentials) and
  carries only OPAQUE tenant ids.
* Every tenant read is scoped by ``broker_account_id`` (P3): account A's
  evidence can never be returned for account B.
"""
from __future__ import annotations

import dataclasses
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.observability.sanitize import sanitize_payload
from app.trading_intelligence.versions import CATI_EVIDENCE_TABLE_VERSION


class CATIEvidenceSchemaMissing(RuntimeError):
    """The canonical migration has not created a CATI evidence table."""


class EvidenceConflict(RuntimeError):
    """An existing append-only record id was offered with different content."""


def to_payload(obj: Any) -> Dict[str, Any]:
    raw = dataclasses.asdict(obj) if dataclasses.is_dataclass(obj) else dict(obj)
    return sanitize_payload(json.loads(json.dumps(raw, default=str, sort_keys=True)))


class _AppendOnlyStore:
    TABLE = ""
    ID = ""

    def __init__(self, db: Any) -> None:
        self._db = db
        with db.connect() as conn:
            row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.TABLE,)).fetchone()
        if row is None:
            raise CATIEvidenceSchemaMissing(f"{self.TABLE} is missing: run shared_lib.persistence.migrations.migrate()")

    def _insert(self, columns: Mapping[str, Any], payload: Mapping[str, Any], schema_version: str) -> bool:
        body = json.dumps(payload, sort_keys=True, default=str)
        digest = stable_hash(payload)
        cols = dict(columns)
        cols.update(schema_version=schema_version, table_version=CATI_EVIDENCE_TABLE_VERSION, payload=body,
                    payload_hash=digest)
        names = ", ".join(cols)
        marks = ", ".join("?" for _ in cols)
        with self._db.connect() as conn:
            cur = conn.execute(f"INSERT OR IGNORE INTO {self.TABLE} ({names}) VALUES ({marks})", tuple(cols.values()))
            if cur.rowcount == 1:
                return True
            existing = conn.execute(f"SELECT payload_hash FROM {self.TABLE} WHERE {self.ID}=?",
                                    (cols[self.ID],)).fetchone()
        if existing is not None and existing[0] != digest:
            raise EvidenceConflict(f"{self.TABLE}:{cols[self.ID]} already recorded with different content")
        return False

    def get(self, record_id: str) -> Optional[Dict[str, Any]]:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {self.TABLE} WHERE {self.ID}=?", (record_id,)).fetchone()
        return self._row(row) if row is not None else None

    @staticmethod
    def _row(row: Any) -> Dict[str, Any]:
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        return d

    def _select(self, where: str, params: Sequence[Any], order: str) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.TABLE} WHERE {where} ORDER BY {order}", tuple(params)).fetchall()
        return [self._row(r) for r in rows]


class PositionForecastStore(_AppendOnlyStore):
    TABLE, ID = "cati_position_forecasts", "position_forecast_id"

    def append(self, forecast: Any, path: Any = None) -> bool:
        payload = to_payload(forecast)
        if path is not None:
            payload["position_path"] = to_payload(path)
        return self._insert(dict(
            position_forecast_id=forecast.position_forecast_id, position_id=forecast.position_id,
            trade_plan_id=forecast.trade_plan_id, position_path_id=forecast.position_path_id,
            market_state_id=forecast.market_state_id, regime_distribution_id=forecast.regime_distribution_id,
            user_id=forecast.user_id, broker_account_id=forecast.broker_account_id,
            bot_instance_id=forecast.bot_instance_id, forecast_time=forecast.forecast_time, status=forecast.status,
            thesis_status=forecast.thesis_status,
            conservative_remaining_edge_r=forecast.conservative_remaining_edge_R,
            evaluation_mode=forecast.evaluation_mode), payload, forecast.schema_version)

    def for_account(self, broker_account_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=?", (broker_account_id,), "forecast_time, position_forecast_id")

    def for_position(self, broker_account_id: str, position_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=? AND position_id=?", (broker_account_id, position_id),
                            "forecast_time, position_forecast_id")


class ExitDecisionStore(_AppendOnlyStore):
    TABLE, ID = "cati_exit_decisions", "exit_decision_id"

    def append(self, decision: Any) -> bool:
        return self._insert(dict(
            exit_decision_id=decision.exit_decision_id, position_forecast_id=decision.position_forecast_id,
            position_id=decision.position_id, trade_plan_id=decision.trade_plan_id, user_id=decision.user_id,
            broker_account_id=decision.broker_account_id, bot_instance_id=decision.bot_instance_id,
            decision_time=decision.decision_time, action=decision.action,
            requested_fraction=decision.requested_fraction,
            suggested_protection_price=decision.suggested_protection_price, thesis_status=decision.thesis_status,
            evaluation_mode=decision.evaluation_mode), to_payload(decision), decision.schema_version)

    def for_account(self, broker_account_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=?", (broker_account_id,), "decision_time, exit_decision_id")

    def for_position(self, broker_account_id: str, position_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=? AND position_id=?", (broker_account_id, position_id),
                            "decision_time, exit_decision_id")


class RiskDecisionStore(_AppendOnlyStore):
    TABLE, ID = "cati_risk_decisions", "risk_decision_id"

    def append(self, risk: Any) -> bool:
        return self._insert(dict(
            risk_decision_id=risk.risk_decision_id, trade_plan_id=risk.trade_plan_id,
            trade_plan_hash=risk.trade_plan_hash, user_id=risk.user_id, broker_account_id=risk.broker_account_id,
            bot_instance_id=risk.bot_instance_id, runtime_session_id=risk.runtime_session_id, run_id=risk.run_id,
            cycle_id=risk.cycle_id, status=risk.status, rejection_family=risk.rejection_family,
            decision_time=risk.decision_time), to_payload(risk), risk.schema_version)

    def for_plan(self, broker_account_id: str, trade_plan_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=? AND trade_plan_id=?", (broker_account_id, trade_plan_id),
                            "decision_time, risk_decision_id")

    def for_account(self, broker_account_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=?", (broker_account_id,), "decision_time, risk_decision_id")


class ExecutionAttemptStore(_AppendOnlyStore):
    """One immutable row per attempt STATE (sequence 0, 1, ...). The latest
    row is the attempt's current evidence; earlier rows are never touched."""

    TABLE, ID = "cati_execution_attempts", "record_id"

    def append(self, attempt: Any, sequence: int) -> bool:
        payload = to_payload(attempt)
        return self._insert(dict(
            record_id=short_id("xrec", {"attempt": attempt.execution_attempt_id, "seq": int(sequence)}),
            execution_attempt_id=attempt.execution_attempt_id, sequence=int(sequence),
            trade_plan_id=attempt.trade_plan_id, risk_decision_id=attempt.risk_decision_id,
            user_id=attempt.user_id, broker_account_id=attempt.broker_account_id,
            bot_instance_id=attempt.bot_instance_id, position_id=attempt.position_id, status=attempt.status,
            broker_order_id=attempt.broker_order_id, recorded_at=int(attempt.recorded_at)),
            payload, attempt.schema_version)

    def history(self, broker_account_id: str, execution_attempt_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=? AND execution_attempt_id=?", (broker_account_id, execution_attempt_id),
                            "sequence")

    def latest(self, broker_account_id: str, execution_attempt_id: str) -> Optional[Dict[str, Any]]:
        rows = self.history(broker_account_id, execution_attempt_id)
        return rows[-1] if rows else None

    def next_sequence(self, broker_account_id: str, execution_attempt_id: str) -> int:
        rows = self.history(broker_account_id, execution_attempt_id)
        return (rows[-1]["sequence"] + 1) if rows else 0

    def for_plan(self, broker_account_id: str, trade_plan_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=? AND trade_plan_id=?", (broker_account_id, trade_plan_id),
                            "recorded_at, sequence")

    def for_account(self, broker_account_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=?", (broker_account_id,), "recorded_at, sequence")

    def by_position(self, broker_account_id: str, position_id: str) -> List[Dict[str, Any]]:
        return self._select("broker_account_id=? AND position_id=?", (broker_account_id, position_id),
                            "recorded_at, sequence")


class ComponentErrorStore(_AppendOnlyStore):
    TABLE, ID = "cati_component_errors", "error_id"

    def append(self, rec: Any) -> bool:
        payload = to_payload(rec)
        return self._insert(dict(
            error_id=short_id("cerr", payload), component=rec.component, stage=getattr(rec, "stage", None),
            exception_class=rec.exception_class, cycle_id=rec.cycle_id, user_id=getattr(rec, "user_id", None),
            broker_account_id=rec.broker_account_id, bot_instance_id=rec.bot_instance_id,
            observed_at=int(rec.observed_at)), payload, "1.0.0")

    def recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self._select("1=1", (), f"observed_at DESC LIMIT {int(limit)}")


__all__ = ["CATIEvidenceSchemaMissing", "EvidenceConflict", "to_payload", "PositionForecastStore", "ExitDecisionStore",
           "RiskDecisionStore", "ExecutionAttemptStore", "ComponentErrorStore"]
