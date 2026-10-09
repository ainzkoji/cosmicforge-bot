"""The authoritative CATI research register: one committed, append-only, hash-chained file.

Why a file and not the research database: ``cati_experiment_registry`` lives in a gitignored SQLite file, is
filtered by dataset hash and held ZERO rows on 9 October 2026 although thirteen hypotheses had been evaluated
outside it -- so the multiple-testing count restarted at 1 for every new dataset and every new machine. A
register that must never forget has to travel with the repository.

Each line is one record::

    {"seq": n, "type": T, "recorded_at": ISO-8601 UTC, "prev_hash": H(n-1), "body": {...}, "record_hash": H(n)}

``record_hash`` is the canonical CATI ``stable_hash`` of the other five fields, so editing, reordering or
removing any earlier line breaks every later hash. Truncating the tail keeps a valid chain, which is why
callers pin an ANCHOR (``verify(anchor=...)``) and why every run artifact stores the head it was written
against. Status never mutates: a later ``HYPOTHESIS_STATUS`` record supersedes an earlier one by position.

Per-type rules are enforced at append time, under an exclusive lock:

* hypothesis numbers are 1, 2, 3 ... with no gap, no reuse and no second record for the same identity;
* a mandate is registered once, for an existing hypothesis; changes are numbered amendments, never a rewrite;
* a run needs a registered mandate; it starts once and ends once;
* a holdout goes RESERVED -> AUTHORIZED -> OPENED -> BURNED and cannot go back.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.hashing import stable_hash

REGISTER_SCHEMA_VERSION = "cati-research-register-v1"
GENESIS_HASH = "0" * 64
#: repository-relative location of the one authoritative register
REGISTER_RELATIVE_PATH = Path("docs") / "research" / "registry" / "research_register.jsonl"

HISTORICAL_IMPORT = "HISTORICAL_IMPORT"
HYPOTHESIS = "HYPOTHESIS"
HYPOTHESIS_STATUS = "HYPOTHESIS_STATUS"
MANDATE_REGISTERED = "MANDATE_REGISTERED"
MANDATE_AMENDMENT = "MANDATE_AMENDMENT"
DATASET_FROZEN = "DATASET_FROZEN"
COST_MODEL_REGISTERED = "COST_MODEL_REGISTERED"
GOVERNANCE_DECISION = "GOVERNANCE_DECISION"
RUN_STARTED = "RUN_STARTED"
RUN_COMPLETED = "RUN_COMPLETED"
RUN_FAILED = "RUN_FAILED"
HOLDOUT_RESERVED = "HOLDOUT_RESERVED"
HOLDOUT_AUTHORIZED = "HOLDOUT_AUTHORIZED"
HOLDOUT_OPENED = "HOLDOUT_OPENED"
HOLDOUT_BURNED = "HOLDOUT_BURNED"
RECORD_TYPES = (HISTORICAL_IMPORT, HYPOTHESIS, HYPOTHESIS_STATUS, MANDATE_REGISTERED, MANDATE_AMENDMENT,
                DATASET_FROZEN, COST_MODEL_REGISTERED, GOVERNANCE_DECISION, RUN_STARTED, RUN_COMPLETED, RUN_FAILED,
                HOLDOUT_RESERVED, HOLDOUT_AUTHORIZED, HOLDOUT_OPENED, HOLDOUT_BURNED)
_HOLDOUT_ORDER = (HOLDOUT_RESERVED, HOLDOUT_AUTHORIZED, HOLDOUT_OPENED, HOLDOUT_BURNED)
UNKNOWN = "UNKNOWN"

#: every field Section H 3.2 asks of a hypothesis record; an unsupported one is the literal UNKNOWN, never absent
HYPOTHESIS_FIELDS = ("hypothesis_id", "family_id", "mandate_id", "hypothesis_number", "strategy_description",
                     "created_at", "specification_hash", "source_commit", "data_manifest_hash",
                     "development_period", "holdout_period", "evaluation_id", "decision_date", "status",
                     "failure_reasons", "governance_stage", "evidence_locations")
MANDATE_FIELDS = ("mandate_id", "hypothesis_id", "family_id", "specification_version", "specification_path",
                  "specification_sha256", "rule_artifact_hash", "research_code_commit", "registry_version",
                  "registered_at", "registered_by", "approved_at", "approved_by", "authorization_reference")
RUN_FIELDS = ("run_id", "run_type", "mandate_id", "hypothesis_id", "strategy_hash", "dataset_hash", "code_commit",
              "parameter_hash", "cost_model_hash", "researcher", "started_at", "period", "authorization",
              "parent_run_id", "reason_for_rerun")


class RegisterError(RuntimeError):
    """A record the register refuses (duplicate, out of order, unregistered, malformed)."""


class RegisterTampered(RegisterError):
    """The file no longer matches its own hash chain or its pinned anchor."""


def repository_root() -> Path:
    return Path(__file__).resolve().parents[6]


def default_register_path() -> Path:
    return repository_root() / REGISTER_RELATIVE_PATH


def canonical_text_sha256(path: Path) -> str:
    """SHA-256 of a text file with CRLF folded to LF: the hash Git stores, identical on Windows (autocrlf)
    and on Linux. A frozen specification is pinned by this value."""
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _record_hash(rec: Mapping[str, Any]) -> str:
    return stable_hash({k: rec[k] for k in ("seq", "type", "recorded_at", "prev_hash", "body")})


def _plain(value: Any) -> Any:
    """JSON round trip: tuples become lists, keys become strings, non-finite floats are refused."""
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


class ResearchRegister:
    LOCK_TIMEOUT_S = 30.0

    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path) if path is not None else default_register_path()

    # ------------------------------------------------------------------ reading
    def records(self, *, anchor: Optional[Tuple[int, str]] = None) -> List[Dict[str, Any]]:
        """Every record, verified. ``anchor`` = ``(seq, record_hash)`` that must still be present."""
        if not self.path.exists():
            if anchor is not None:
                raise RegisterTampered(f"research register {self.path} is missing but an anchor is pinned")
            return []
        raw = self.path.read_bytes()
        if raw and not raw.endswith(b"\n"):
            raise RegisterTampered("research register ends in a partial record (interrupted write or edit)")
        out: List[Dict[str, Any]] = []
        prev = GENESIS_HASH
        for i, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
            if not line.strip():
                raise RegisterTampered(f"research register line {i} is blank")
            try:
                rec = json.loads(line)
            except ValueError as exc:
                raise RegisterTampered(f"research register line {i} is not JSON") from exc
            if rec.get("seq") != i or rec.get("prev_hash") != prev or rec.get("type") not in RECORD_TYPES:
                raise RegisterTampered(f"research register line {i} breaks the chain (seq/prev_hash/type)")
            if rec.get("record_hash") != _record_hash(rec):
                raise RegisterTampered(f"research register line {i} was altered (record hash mismatch)")
            prev = rec["record_hash"]
            out.append(rec)
        if anchor is not None:
            seq, digest = anchor
            if len(out) < seq or out[seq - 1]["record_hash"] != digest:
                raise RegisterTampered(f"research register lost or rewrote its anchored record {seq}")
        return out

    def verify(self, *, anchor: Optional[Tuple[int, str]] = None) -> Dict[str, Any]:
        recs = self.records(anchor=anchor)
        return {"path": str(self.path), "records": len(recs), "schema_version": REGISTER_SCHEMA_VERSION,
                "head_hash": recs[-1]["record_hash"] if recs else GENESIS_HASH,
                "hypotheses": sum(1 for r in recs if r["type"] == HYPOTHESIS)}

    def head_hash(self) -> str:
        return self.verify()["head_hash"]

    def of_type(self, *types: str) -> List[Dict[str, Any]]:
        return [r for r in self.records() if r["type"] in types]

    # ------------------------------------------------------------------ writing
    @contextlib.contextmanager
    def _lock(self) -> Iterator[None]:
        """Exclusive, cross-platform (O_EXCL). A stale lock is never broken automatically: it means a writer
        died mid-append and a person must look before anything else is recorded."""
        lock = self.path.with_name(self.path.name + ".lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.LOCK_TIMEOUT_S
        while True:
            try:
                fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except (FileExistsError, PermissionError):   # Windows reports a lock file mid-deletion as EACCES
                if time.monotonic() >= deadline:
                    raise RegisterError(f"research register is locked ({lock}); if no writer is running, inspect "
                                        "the register, then remove the lock file by hand") from None
                time.sleep(0.05)
        try:
            os.write(fd, f"{os.getpid()} {utc_now_iso()}".encode())
            os.close(fd)
            yield
        finally:
            for _ in range(200):                         # the holder always releases; Windows may need a moment
                try:
                    os.unlink(str(lock))
                    break
                except FileNotFoundError:
                    break
                except PermissionError:
                    time.sleep(0.01)

    def append(self, record_type: str, body: Mapping[str, Any], *, now: Optional[str] = None) -> Dict[str, Any]:
        if record_type not in RECORD_TYPES:
            raise RegisterError(f"unknown register record type {record_type!r}")
        body = _plain(dict(body))
        with self._lock():
            recs = self.records()
            _validate(record_type, body, recs)
            rec = {"seq": len(recs) + 1, "type": record_type, "recorded_at": now or utc_now_iso(),
                   "prev_hash": recs[-1]["record_hash"] if recs else GENESIS_HASH, "body": body}
            rec["record_hash"] = _record_hash(rec)
            line = json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
            with open(self.path, "ab") as fh:       # binary: LF on every platform
                fh.write(line.encode("utf-8"))
                fh.flush()
                os.fsync(fh.fileno())               # durable BEFORE the caller acts on it (holdout access)
        return rec

    # ------------------------------------------------------------------ views
    def hypotheses(self) -> List[Dict[str, Any]]:
        """Every hypothesis ever registered, in number order, with its CURRENT status (latest status record)."""
        recs = self.records()
        out = {r["body"]["hypothesis_id"]: {**r["body"], "registered_seq": r["seq"], "recorded_at": r["recorded_at"],
                                            "status_history": [{"seq": r["seq"], "status": r["body"]["status"],
                                                                "reason": "as registered"}]}
               for r in recs if r["type"] == HYPOTHESIS}
        for r in recs:
            if r["type"] == HYPOTHESIS_STATUS:
                h = out[r["body"]["hypothesis_id"]]
                h["status"] = r["body"]["status"]
                h["failure_reasons"] = r["body"].get("failure_reasons", h["failure_reasons"])
                h["status_history"].append({"seq": r["seq"], "status": r["body"]["status"],
                                            "reason": r["body"].get("reason"), "run_id": r["body"].get("run_id")})
        return sorted(out.values(), key=lambda h: h["hypothesis_number"])

    def hypothesis_count(self) -> int:
        """The multiple-testing N: every hypothesis ever registered, whatever became of it. It never shrinks."""
        return sum(1 for r in self.records() if r["type"] == HYPOTHESIS)

    def hypothesis(self, hypothesis_id: str) -> Dict[str, Any]:
        for h in self.hypotheses():
            if h["hypothesis_id"] == hypothesis_id:
                return h
        raise RegisterError(f"hypothesis {hypothesis_id!r} is not registered")

    def mandate(self, mandate_id: str) -> Dict[str, Any]:
        """The registration (immutable) plus its numbered amendments (append-only)."""
        recs = self.records()
        reg = next((r for r in recs if r["type"] == MANDATE_REGISTERED and r["body"]["mandate_id"] == mandate_id), None)
        if reg is None:
            raise RegisterError(f"mandate {mandate_id!r} is not registered")
        amendments = [{**r["body"], "seq": r["seq"], "recorded_at": r["recorded_at"]} for r in recs
                      if r["type"] == MANDATE_AMENDMENT and r["body"]["mandate_id"] == mandate_id]
        return {**reg["body"], "registered_seq": reg["seq"], "registration_record_hash": reg["record_hash"],
                "amendments": amendments}

    def runs(self, *, mandate_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Every run that was ever STARTED, with how it ended (or that it did not): failures stay visible."""
        recs = self.records()
        ends = {r["body"]["run_id"]: r for r in recs if r["type"] in (RUN_COMPLETED, RUN_FAILED)}
        out = []
        for r in recs:
            if r["type"] != RUN_STARTED or (mandate_id and r["body"]["mandate_id"] != mandate_id):
                continue
            end = ends.get(r["body"]["run_id"])
            out.append({**r["body"], "started_seq": r["seq"],
                        "state": end["type"] if end else "STARTED_NOT_FINISHED",
                        "end": ({**end["body"], "seq": end["seq"], "recorded_at": end["recorded_at"]} if end else None)})
        return out

    def holdout(self, holdout_id: str) -> Dict[str, Any]:
        events = [r for r in self.records() if r["type"] in _HOLDOUT_ORDER and r["body"]["holdout_id"] == holdout_id]
        if not events:
            return {"holdout_id": holdout_id, "status": "UNRESERVED", "events": []}
        by = {r["type"]: r for r in events}
        first = by[HOLDOUT_RESERVED]["body"]
        return {"holdout_id": holdout_id, "status": events[-1]["type"], "mandate_id": first["mandate_id"],
                "dataset_hash": first["dataset_hash"], "start": first["start"], "end": first["end"],
                "authorization": by[HOLDOUT_AUTHORIZED]["body"] if HOLDOUT_AUTHORIZED in by else None,
                "opened": by[HOLDOUT_OPENED]["body"] if HOLDOUT_OPENED in by else None,
                "burned": by[HOLDOUT_BURNED]["body"] if HOLDOUT_BURNED in by else None,
                "events": [{"seq": r["seq"], "type": r["type"], "recorded_at": r["recorded_at"]} for r in events]}

    def decision(self, decision_id: str) -> Optional[Dict[str, Any]]:
        """The LATEST recorded owner decision with this id (a later record supersedes an earlier one)."""
        hits = [r for r in self.records() if r["type"] == GOVERNANCE_DECISION and r["body"]["decision_id"] == decision_id]
        return {**hits[-1]["body"], "seq": hits[-1]["seq"], "recorded_at": hits[-1]["recorded_at"]} if hits else None


# ---------------------------------------------------------------------- per-type rules
def _require(body: Mapping[str, Any], fields: Sequence[str], what: str) -> None:
    missing = [f for f in fields if f not in body or body[f] in (None, "")]
    if missing:
        raise RegisterError(f"{what} record is missing {missing} (write the literal UNKNOWN for what cannot be "
                            "supported by evidence; never leave it out)")


def _validate(record_type: str, body: Mapping[str, Any], recs: Sequence[Mapping[str, Any]]) -> None:
    def bodies(*types: str) -> List[Mapping[str, Any]]:
        return [r["body"] for r in recs if r["type"] in types]

    if record_type == HYPOTHESIS:
        _require(body, HYPOTHESIS_FIELDS, "hypothesis")
        existing = bodies(HYPOTHESIS)
        if body["hypothesis_number"] != len(existing) + 1:
            raise RegisterError(f"hypothesis number {body['hypothesis_number']} refused: the next number is "
                                f"{len(existing) + 1} (numbers are never skipped, reused or reassigned)")
        if any(h["hypothesis_id"] == body["hypothesis_id"] for h in existing):
            raise RegisterError(f"hypothesis {body['hypothesis_id']!r} is already registered")
    elif record_type == HYPOTHESIS_STATUS:
        _require(body, ("hypothesis_id", "status", "reason"), "hypothesis status")
        if not any(h["hypothesis_id"] == body["hypothesis_id"] for h in bodies(HYPOTHESIS)):
            raise RegisterError(f"status refused: hypothesis {body['hypothesis_id']!r} is not registered")
    elif record_type == MANDATE_REGISTERED:
        _require(body, tuple(f for f in MANDATE_FIELDS if not f.startswith(("approved", "authorization"))), "mandate")
        for f in MANDATE_FIELDS:
            if f not in body:
                raise RegisterError(f"mandate record is missing {f!r} (use UNKNOWN / NOT_RECORDED explicitly)")
        if any(m["mandate_id"] == body["mandate_id"] for m in bodies(MANDATE_REGISTERED)):
            raise RegisterError(f"mandate {body['mandate_id']!r} is already registered; changes are amendments")
        if not any(h["hypothesis_id"] == body["hypothesis_id"] for h in bodies(HYPOTHESIS)):
            raise RegisterError("a mandate is registered for an existing hypothesis")
        if any(m["hypothesis_id"] == body["hypothesis_id"] for m in bodies(MANDATE_REGISTERED)):
            raise RegisterError(f"hypothesis {body['hypothesis_id']!r} already has a mandate")
        if len(str(body["specification_sha256"])) != 64:
            raise RegisterError("a mandate is pinned by the SHA-256 of its frozen specification")
    elif record_type == MANDATE_AMENDMENT:
        _require(body, ("mandate_id", "amendment_number", "kind", "summary", "artifact_path", "artifact_sha256",
                        "changes_strategy_rules", "recorded_by"), "amendment")
        if not any(m["mandate_id"] == body["mandate_id"] for m in bodies(MANDATE_REGISTERED)):
            raise RegisterError(f"amendment refused: mandate {body['mandate_id']!r} is not registered")
        prior = [a for a in bodies(MANDATE_AMENDMENT) if a["mandate_id"] == body["mandate_id"]]
        if body["amendment_number"] != len(prior) + 1:
            raise RegisterError(f"amendment number {body['amendment_number']} refused: next is {len(prior) + 1}")
    elif record_type == RUN_STARTED:
        _require(body, tuple(f for f in RUN_FIELDS if f not in ("parent_run_id", "reason_for_rerun", "authorization")),
                 "run")
        for f in RUN_FIELDS:
            if f not in body:
                raise RegisterError(f"run record is missing {f!r} (null is allowed, absence is not)")
        if not any(m["mandate_id"] == body["mandate_id"] for m in bodies(MANDATE_REGISTERED)):
            raise RegisterError(f"evaluation refused: mandate {body['mandate_id']!r} is not registered")
        if any(r["run_id"] == body["run_id"] for r in bodies(RUN_STARTED)):
            raise RegisterError(f"run {body['run_id']} was already started; a rerun is a new run with a parent")
        if body.get("parent_run_id") and not body.get("reason_for_rerun"):
            raise RegisterError("a rerun records why it was needed")
        if body.get("parent_run_id") and not any(r["run_id"] == body["parent_run_id"] for r in bodies(RUN_STARTED)):
            raise RegisterError("a rerun names a parent run that exists")
    elif record_type in (RUN_COMPLETED, RUN_FAILED):
        _require(body, ("run_id", "status", "completed_at"), "run end")
        if not any(r["run_id"] == body["run_id"] for r in bodies(RUN_STARTED)):
            raise RegisterError(f"run {body['run_id']} was never started")
        if any(r["run_id"] == body["run_id"] for r in bodies(RUN_COMPLETED, RUN_FAILED)):
            raise RegisterError(f"run {body['run_id']} already ended; its result cannot be replaced")
    elif record_type in _HOLDOUT_ORDER:
        _require(body, ("holdout_id",), "holdout")
        seen = [r["type"] for r in recs if r["type"] in _HOLDOUT_ORDER and r["body"]["holdout_id"] == body["holdout_id"]]
        expected = _HOLDOUT_ORDER[len(seen)] if len(seen) < len(_HOLDOUT_ORDER) else None
        if record_type != expected:
            raise RegisterError(f"holdout {body['holdout_id']}: {record_type} refused after {seen or ['nothing']} "
                                "(RESERVED -> AUTHORIZED -> OPENED -> BURNED, once each, never again)")
        if record_type == HOLDOUT_RESERVED:
            _require(body, ("mandate_id", "dataset_hash", "start", "end"), "holdout reservation")
        if record_type == HOLDOUT_AUTHORIZED:
            _require(body, ("authorized_by", "authorization_reference", "reason", "mandate_id",
                            "specification_sha256", "dataset_hash", "readiness_hash"), "holdout authorization")
        if record_type == HOLDOUT_OPENED:
            _require(body, ("run_id", "specification_sha256", "dataset_hash", "code_commit"), "holdout opening")
        if record_type == HOLDOUT_BURNED:
            _require(body, ("run_id", "result_hash"), "holdout result")
    elif record_type == GOVERNANCE_DECISION:
        _require(body, ("decision_id", "state", "decided_by", "authorization_reference", "reason"), "decision")
    elif record_type == DATASET_FROZEN:
        _require(body, ("dataset_id", "dataset_version", "manifest_path", "manifest_hash", "dataset_hash"), "dataset")
        if any(d["dataset_id"] == body["dataset_id"] and d["dataset_version"] == body["dataset_version"]
               for d in bodies(DATASET_FROZEN)):
            raise RegisterError("this dataset version is already frozen; a change is a new version")
    elif record_type == COST_MODEL_REGISTERED:
        _require(body, ("cost_model_id", "cost_model_version", "cost_model_hash", "basis"), "cost model")
        if any(c["cost_model_id"] == body["cost_model_id"] and c["cost_model_version"] == body["cost_model_version"]
               for c in bodies(COST_MODEL_REGISTERED)):
            raise RegisterError("this cost model version is already registered; calibration creates a new version")
    elif record_type == HISTORICAL_IMPORT:
        _require(body, ("source_path", "source_sha256", "counting_rule", "hypotheses_imported"), "historical import")
        if bodies(HISTORICAL_IMPORT):
            raise RegisterError("the historical record was already imported; it is never imported twice")


__all__ = ["ResearchRegister", "RegisterError", "RegisterTampered", "REGISTER_SCHEMA_VERSION", "RECORD_TYPES",
           "HYPOTHESIS_FIELDS", "MANDATE_FIELDS", "RUN_FIELDS", "UNKNOWN", "GENESIS_HASH", "canonical_text_sha256",
           "default_register_path", "repository_root", "utc_now_iso", "HISTORICAL_IMPORT", "HYPOTHESIS",
           "HYPOTHESIS_STATUS", "MANDATE_REGISTERED", "MANDATE_AMENDMENT", "DATASET_FROZEN",
           "COST_MODEL_REGISTERED", "GOVERNANCE_DECISION", "RUN_STARTED", "RUN_COMPLETED", "RUN_FAILED",
           "HOLDOUT_RESERVED", "HOLDOUT_AUTHORIZED", "HOLDOUT_OPENED", "HOLDOUT_BURNED"]
