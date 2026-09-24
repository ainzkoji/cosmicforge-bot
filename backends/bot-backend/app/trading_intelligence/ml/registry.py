"""Append-only CATI model registry + status history (Section 23.12).

A model row is written once (identity, hashes, window, commit); its status
is the LATEST row of ``cati_ml_model_events``. Transitions follow
``STATUS_TRANSITIONS`` (no skipping; any status may be REJECTED). PROMOTED
requires recorded evidence that the model promotion gate passed AND that
Section 25 governance authorized estimator use. Nothing is ever updated or
deleted; a retrained model is a NEW model that ``supersedes`` the old one.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.observability.sanitize import sanitize_payload

from .contracts import CATI_ML_CONTRACT_VERSION, STATUS_TRANSITIONS, ModelCard, ModelStatus


class ModelRegistryError(RuntimeError):
    pass


class ModelRegistry:
    MODELS, EVENTS = "cati_ml_models", "cati_ml_model_events"

    def __init__(self, db: Any):
        self._db = db

    def register(self, card: ModelCard, *, supersedes_model_id: Optional[str] = None,
                 now_ms: Optional[int] = None) -> str:
        if not card.artifact_hash:
            raise ModelRegistryError("register the saved artifact (artifact_hash required)")
        payload = sanitize_payload(json.loads(json.dumps(card.to_dict(), default=str)))
        ts = int(now_ms or time.time() * 1000)
        with self._db.connect() as conn:
            if conn.execute(f"SELECT 1 FROM {self.MODELS} WHERE model_id=?", (card.model_id,)).fetchone():
                return card.model_id  # identical identity: already registered (immutable)
            conn.execute(
                f"INSERT INTO {self.MODELS} (model_id, role, artifact_hash, training_dataset_hash, feature_schema_hash, "
                "label_schema_hash, code_commit, supersedes_model_id, created_at, schema_version, table_version, "
                "payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (card.model_id, card.role, card.artifact_hash, card.training_dataset_hash, card.feature_schema_hash,
                 card.label_schema_hash, card.code_commit, supersedes_model_id, ts, CATI_ML_CONTRACT_VERSION,
                 CATI_ML_CONTRACT_VERSION, json.dumps(payload, sort_keys=True), stable_hash(payload)))
        self._event(card.model_id, None, ModelStatus.RESEARCH.value, {"registered": True}, ts)
        return card.model_id

    def _event(self, model_id: str, frm: Optional[str], to: str, evidence: Mapping[str, Any], ts: int) -> None:
        payload = sanitize_payload({"model_id": model_id, "from": frm, "to": to,
                                    "evidence": json.loads(json.dumps(evidence, default=str))})
        with self._db.connect() as conn:
            n = conn.execute(f"SELECT COUNT(*) FROM {self.EVENTS} WHERE model_id=?", (model_id,)).fetchone()[0]
            conn.execute(
                f"INSERT INTO {self.EVENTS} (model_event_id, model_id, from_status, to_status, recorded_at, "
                "schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?)",
                (short_id("mev", {"m": model_id, "n": n, "to": to}), model_id, frm, to, ts, CATI_ML_CONTRACT_VERSION,
                 CATI_ML_CONTRACT_VERSION, json.dumps(payload, sort_keys=True), stable_hash(payload)))

    def status(self, model_id: str) -> Optional[str]:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT to_status FROM {self.EVENTS} WHERE model_id=? ORDER BY recorded_at DESC, "
                               "rowid DESC LIMIT 1", (model_id,)).fetchone()
        return row[0] if row else None

    def transition(self, model_id: str, to_status: str, *, evidence: Mapping[str, Any],
                   now_ms: Optional[int] = None) -> None:
        current = self.status(model_id)
        if current is None:
            raise ModelRegistryError(f"unknown model {model_id}")
        allowed = STATUS_TRANSITIONS.get(current, ()) + (ModelStatus.REJECTED.value,)
        if current in (ModelStatus.REJECTED.value, ModelStatus.RETIRED.value) or to_status not in allowed:
            raise ModelRegistryError(f"{current} -> {to_status} is not an allowed model transition")
        if to_status == ModelStatus.PROMOTED.value and not (evidence.get("promotion_gate_passed") is True
                                                            and evidence.get("governance_authorized") is True):
            raise ModelRegistryError("PROMOTED requires a passed model promotion gate AND Section 25 authorization")
        if to_status == ModelStatus.REJECTED.value and not evidence.get("reasons"):
            raise ModelRegistryError("a rejection must record its reasons")
        self._event(model_id, current, to_status, evidence, int(now_ms or time.time() * 1000))

    def history(self, model_id: str) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(f"SELECT * FROM {self.EVENTS} WHERE model_id=? "
                                                  "ORDER BY recorded_at, rowid", (model_id,))]
        return rows

    def models(self, role: Optional[str] = None) -> List[Dict[str, Any]]:
        sql, params = f"SELECT * FROM {self.MODELS}", []
        if role:
            sql += " WHERE role=?"
            params.append(role)
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(sql + " ORDER BY created_at, model_id", params)]
        for r in rows:
            r["status"] = self.status(r["model_id"])
        return rows

    def promoted(self, role: str) -> Sequence[Dict[str, Any]]:
        return [m for m in self.models(role) if m["status"] == ModelStatus.PROMOTED.value]


__all__ = ["ModelRegistry", "ModelRegistryError"]
