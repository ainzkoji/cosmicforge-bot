"""Append-only experiment and holdout registries (Sections 22.5, 22.9).

Selection-bias control: EVERY experiment is recorded, failures included.
Rows live in append-only tables (update/delete triggers abort), so a failed
experiment can never disappear because a later variant looked better, and an
existing record can never be rewritten with a different result.

A holdout is an event log (RESERVED -> OPENED -> BURNED). It opens once, for
one policy-freeze hash; after that it is burned and can never again be
presented as untouched -- not for a new policy, and not for the same one.
"""
from __future__ import annotations

import contextlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.observability.sanitize import sanitize_payload
from app.trading_intelligence.versions import EXPERIMENT_REGISTRY_SCHEMA_VERSION


class ExperimentStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    INCONCLUSIVE = "INCONCLUSIVE"


class HoldoutEvent(str, Enum):
    RESERVED = "RESERVED"
    OPENED = "OPENED"
    BURNED = "BURNED"


class RegistryError(RuntimeError):
    pass


class HoldoutBurned(RegistryError):
    """The holdout was already opened; it is no longer untouched."""


class SqliteResearchStore:
    """Minimal ``.connect()`` provider for a research database file (the CLI),
    with the canonical CATI schema ensured (no runtime DDL beyond it)."""

    def __init__(self, path: str):
        from shared_lib.persistence.cati_schema import ensure_cati_schema_on_connection

        self.path = str(path)
        with self.connect() as conn:
            ensure_cati_schema_on_connection(conn)

    @contextlib.contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class ExperimentRecord:
    dataset_hash: str
    policy_hash: str
    hypothesis: str
    changed_parameters: Mapping[str, Any]
    reason_for_change: str
    stages_run: Tuple[str, ...]
    results: Mapping[str, Any]
    status: str
    artifact_hashes: Mapping[str, str] = field(default_factory=dict)
    reason_codes: Tuple[str, ...] = ()
    notes: str = ""
    parent_experiment_id: Optional[str] = None
    schema_version: str = EXPERIMENT_REGISTRY_SCHEMA_VERSION
    #: operational metadata -- excluded from the id
    created_at: Optional[int] = None

    def __post_init__(self) -> None:
        if self.status not in {s.value for s in ExperimentStatus}:
            raise ValueError(f"unknown experiment status {self.status!r}")
        if self.parent_experiment_id and not self.changed_parameters:
            raise ValueError("a child experiment must declare what changed")

    @property
    def experiment_id(self) -> str:
        return short_id("exp", {"parent": self.parent_experiment_id, "dataset_hash": self.dataset_hash,
                                "policy_hash": self.policy_hash, "hypothesis": self.hypothesis,
                                "changed_parameters": dict(self.changed_parameters),
                                "stages_run": list(self.stages_run), "schema_version": self.schema_version})

    def payload(self) -> Dict[str, Any]:
        return sanitize_payload({
            "experiment_id": self.experiment_id, "parent_experiment_id": self.parent_experiment_id,
            "dataset_hash": self.dataset_hash, "policy_hash": self.policy_hash, "hypothesis": self.hypothesis,
            "changed_parameters": dict(self.changed_parameters), "reason_for_change": self.reason_for_change,
            "stages_run": list(self.stages_run), "results": json.loads(json.dumps(self.results, default=str)),
            "status": self.status, "artifact_hashes": dict(self.artifact_hashes),
            "reason_codes": list(self.reason_codes), "notes": self.notes, "schema_version": self.schema_version})


class ExperimentRegistry:
    TABLE = "cati_experiment_registry"

    def __init__(self, db: Any):
        self._db = db

    def record(self, rec: ExperimentRecord) -> str:
        payload = rec.payload()
        digest = stable_hash(payload)
        with self._db.connect() as conn:
            existing = conn.execute(f"SELECT payload_hash FROM {self.TABLE} WHERE experiment_id=?",
                                    (rec.experiment_id,)).fetchone()
            if existing is not None:
                if existing[0] != digest:
                    raise RegistryError(f"experiment {rec.experiment_id} already recorded with a different "
                                        "result; history is append-only (record a NEW experiment)")
                return rec.experiment_id
            conn.execute(
                f"INSERT INTO {self.TABLE} (experiment_id, parent_experiment_id, dataset_hash, policy_hash, status, "
                "created_at, schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (rec.experiment_id, rec.parent_experiment_id, rec.dataset_hash, rec.policy_hash, rec.status,
                 int(rec.created_at or _now_ms()), rec.schema_version, EXPERIMENT_REGISTRY_SCHEMA_VERSION,
                 json.dumps(payload, sort_keys=True), digest))
        return rec.experiment_id

    def all(self, *, dataset_hash: Optional[str] = None) -> List[Dict[str, Any]]:
        sql, params = f"SELECT * FROM {self.TABLE}", []
        if dataset_hash is not None:
            sql += " WHERE dataset_hash=?"
            params.append(dataset_hash)
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(sql + " ORDER BY created_at, experiment_id", params).fetchall()]
        for r in rows:
            r["payload"] = json.loads(r["payload"])
        return rows

    def trial_count(self, *, dataset_hash: Optional[str] = None) -> int:
        """Number of experiments actually run -- the multiple-testing N (DSR)."""
        return len(self.all(dataset_hash=dataset_hash))


def holdout_id_for(dataset_hash: str, start_ms: int, end_ms: int) -> str:
    return short_id("hold", {"dataset_hash": dataset_hash, "start_ms": int(start_ms), "end_ms": int(end_ms)})


class HoldoutRegistry:
    TABLE = "cati_holdout_registry"

    def __init__(self, db: Any):
        self._db = db

    def _events(self, holdout_id: str) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(
                f"SELECT * FROM {self.TABLE} WHERE holdout_id=? ORDER BY recorded_at, rowid", (holdout_id,)).fetchall()]
        for r in rows:
            r["payload"] = json.loads(r["payload"])
        return rows

    def _append(self, holdout_id: str, dataset_hash: str, event: str, freeze_hash: Optional[str],
                extra: Mapping[str, Any], now_ms: Optional[int]) -> None:
        payload = {"holdout_id": holdout_id, "dataset_hash": dataset_hash, "event": event,
                   "policy_freeze_hash": freeze_hash, **dict(extra)}
        event_id = short_id("hev", {"holdout_id": holdout_id, "event": event, "freeze": freeze_hash})
        with self._db.connect() as conn:
            conn.execute(
                f"INSERT INTO {self.TABLE} (holdout_event_id, holdout_id, dataset_hash, event, policy_freeze_hash, "
                "recorded_at, schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (event_id, holdout_id, dataset_hash, event, freeze_hash, int(now_ms or _now_ms()),
                 EXPERIMENT_REGISTRY_SCHEMA_VERSION, EXPERIMENT_REGISTRY_SCHEMA_VERSION,
                 json.dumps(payload, sort_keys=True), stable_hash(payload)))

    def status(self, holdout_id: str) -> Dict[str, Any]:
        events = self._events(holdout_id)
        if not events:
            return {"holdout_id": holdout_id, "status": "UNRESERVED", "first_opened_at": None,
                    "policy_freeze_hash": None}
        opened = next((e for e in events if e["event"] == HoldoutEvent.OPENED.value), None)
        first = events[0]["payload"]
        return {"holdout_id": holdout_id, "status": events[-1]["event"], "dataset_hash": events[0]["dataset_hash"],
                "start_ms": first.get("start_ms"), "end_ms": first.get("end_ms"),
                "first_opened_at": opened["recorded_at"] if opened else None,
                "policy_freeze_hash": opened["policy_freeze_hash"] if opened else None}

    def reserve(self, *, dataset_hash: str, start_ms: int, end_ms: int, now_ms: Optional[int] = None) -> str:
        """Reserve BEFORE any inspection. Idempotent for the same window."""
        hid = holdout_id_for(dataset_hash, start_ms, end_ms)
        if self.status(hid)["status"] == "UNRESERVED":
            self._append(hid, dataset_hash, HoldoutEvent.RESERVED.value, None,
                         {"start_ms": int(start_ms), "end_ms": int(end_ms)}, now_ms)
        return hid

    def open(self, holdout_id: str, *, policy_freeze_hash: str, now_ms: Optional[int] = None) -> None:
        st = self.status(holdout_id)
        if st["status"] == "UNRESERVED":
            raise RegistryError("a holdout must be reserved before it is opened")
        if st["status"] != HoldoutEvent.RESERVED.value:
            raise HoldoutBurned(f"holdout {holdout_id} is {st['status']} (opened for freeze "
                                f"{st['policy_freeze_hash']}); it is no longer untouched")
        self._append(holdout_id, st["dataset_hash"], HoldoutEvent.OPENED.value, policy_freeze_hash, {}, now_ms)

    def burn(self, holdout_id: str, *, policy_freeze_hash: str, result_hash: str, now_ms: Optional[int] = None) -> None:
        st = self.status(holdout_id)
        if st["status"] != HoldoutEvent.OPENED.value or st["policy_freeze_hash"] != policy_freeze_hash:
            raise RegistryError("only the freeze that opened a holdout may record its result")
        self._append(holdout_id, st["dataset_hash"], HoldoutEvent.BURNED.value, policy_freeze_hash,
                     {"result_hash": result_hash}, now_ms)


class CertificationRunStore:
    """Append-only persistence of CertificationRun + its stage result. The
    same analytical run (same id) is stored once; a different artifact under
    the same id is refused (determinism violation, never an overwrite)."""

    RUN_TABLE = "cati_certification_runs"
    STAGE_TABLE = "cati_certification_stage_results"

    def __init__(self, db: Any):
        self._db = db

    def append(self, run: Any, *, now_ms: Optional[int] = None) -> bool:
        from app.trading_intelligence.versions import CERTIFICATION_SCHEMA_VERSION

        payload = sanitize_payload(run.to_dict())
        payload.pop("metadata", None)  # operational timestamps never enter the stored analytical payload
        digest = stable_hash(payload)
        ts = int(now_ms or run.completed_at or _now_ms())
        with self._db.connect() as conn:
            existing = conn.execute(f"SELECT artifact_hash FROM {self.RUN_TABLE} WHERE certification_run_id=?",
                                    (run.certification_run_id,)).fetchone()
            if existing is not None:
                if existing[0] != run.artifact_hash:
                    raise RegistryError(f"certification run {run.certification_run_id} already stored with a "
                                        "different artifact hash (non-deterministic result)")
                return False
            conn.execute(
                f"INSERT INTO {self.RUN_TABLE} (certification_run_id, stage, status, scope_hash, dataset_hash, "
                "policy_freeze_hash, certification_policy_hash, artifact_hash, recorded_at, schema_version, "
                "table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (run.certification_run_id, run.stage, run.status, run.scope.scope_hash, run.dataset_hash,
                 run.policy_freeze_hash, run.certification_policy_hash, run.artifact_hash, ts,
                 CERTIFICATION_SCHEMA_VERSION, CERTIFICATION_SCHEMA_VERSION, json.dumps(payload, sort_keys=True),
                 digest))
            stage_payload = sanitize_payload(run.result.to_dict())
            conn.execute(
                f"INSERT INTO {self.STAGE_TABLE} (stage_result_id, certification_run_id, stage, status, recorded_at, "
                "schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?)",
                (short_id("cstg", {"run": run.certification_run_id, "result": run.result.result_hash}),
                 run.certification_run_id, run.stage, run.status, ts, CERTIFICATION_SCHEMA_VERSION,
                 CERTIFICATION_SCHEMA_VERSION, json.dumps(stage_payload, sort_keys=True), stable_hash(stage_payload)))
        return True

    def runs(self, *, dataset_hash: Optional[str] = None) -> List[Dict[str, Any]]:
        sql, params = f"SELECT * FROM {self.RUN_TABLE}", []
        if dataset_hash:
            sql += " WHERE dataset_hash=?"
            params.append(dataset_hash)
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(sql + " ORDER BY recorded_at, certification_run_id", params)]
        for r in rows:
            r["payload"] = json.loads(r["payload"])
        return rows


__all__ = ["ExperimentRecord", "ExperimentRegistry", "ExperimentStatus", "HoldoutRegistry", "HoldoutEvent",
           "HoldoutBurned", "RegistryError", "SqliteResearchStore", "CertificationRunStore", "holdout_id_for"]
