"""Pre-holdout readiness and the holdout AUTHORIZATION barrier (Sections 22.3, 22.4, 22.10, 22.11).

Opening a holdout is irreversible (``HoldoutRegistry``: RESERVED -> OPENED -> BURNED). Before any run may open
one, BOTH must hold:

1. ``pre_holdout_readiness`` is READY -- deterministic, from recorded evidence only:
     manifest frozen (verified lineage manifest), dataset COMPLETE, quality acceptable, mapping (universe)
     frozen, policy hash frozen (a certifiable, committed-tree freeze of the CANONICAL policy), code identity
     recorded, real data only, pre-holdout run completed with its evidence produced. A dataset that is still
     acquiring is ``NOT_READY / DATA_ACQUISITION_IN_PROGRESS`` -- a state, not a failure.
2. an explicit, append-only ``HOLDOUT_AUTHORIZATION`` control event exists for THAT holdout AND THAT policy
   freeze (``cati_governance_controls``: append-only triggers). It can only be recorded while readiness is
   READY and the holdout is still untouched (RESERVED), by a named actor with a reason.

Nothing here opens a holdout, and nothing calls ``authorize_holdout`` automatically: no merge, deployment,
data completion, API call, UI toggle or background job can cross this boundary. A burned holdout can never be
re-authorized (no "run -> inspect -> retune -> rerun"): a changed threshold is a new policy freeze that needs a
NEW, never-opened holdout.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Mapping, Optional

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.observability.sanitize import sanitize_payload

PRE_HOLDOUT_VERSION = "pre-holdout-readiness-v1"
HOLDOUT_AUTHORIZATION = "HOLDOUT_AUTHORIZATION"
READY, NOT_READY = "READY", "NOT_READY"
DATA_ACQUISITION_IN_PROGRESS = "DATA_ACQUISITION_IN_PROGRESS"
_CONTROLS = "cati_governance_controls"

#: (requirement, reason when unmet) -- checked in this order; every unmet one is reported
REQUIREMENTS = (
    ("dataset_not_acquiring", DATA_ACQUISITION_IN_PROGRESS),
    ("manifest_frozen", "DATASET_MANIFEST_NOT_FROZEN"),
    ("dataset_complete", "DATASET_INCOMPLETE"),
    ("quality_acceptable", "DATASET_QUALITY_NOT_ACCEPTABLE"),
    ("mapping_frozen", "UNIVERSE_MAPPING_NOT_FROZEN"),
    ("real_data_only", "SYNTHETIC_OR_NON_CERTIFIABLE_DATA"),
    ("policy_hash_frozen", "POLICY_FREEZE_NOT_CERTIFIABLE"),
    ("canonical_policy", "POLICY_IS_NOT_THE_CANONICAL_SECTION22_POLICY"),
    ("code_identity_recorded", "CODE_IDENTITY_NOT_RECORDED"),
    ("pre_holdout_run_completed", "PRE_HOLDOUT_RUN_NOT_COMPLETED"),
    ("evidence_produced", "PRE_HOLDOUT_EVIDENCE_MISSING"),
)
_SYNTHETIC = ("synthetic", "fixture", "smoke", "test", "simulated", "generated")
_EXPECTED_CLOSURES = ("WEEKEND_CLOSED", "HOLIDAY_CLOSED", "SESSION_CLOSED", "LISTING_AGE")


def _manifest_facts(manifest: Optional[Mapping[str, Any]]) -> Dict[str, Optional[bool]]:
    if not manifest:
        return {"manifest_frozen": False, "dataset_complete": False, "quality_acceptable": False,
                "mapping_frozen": False, "real_data_only": False}
    try:
        from app.market_data.universe import verify_dataset_payload

        verify_dataset_payload(manifest)
        frozen = True
    except Exception:
        frozen = False
    parts = list(manifest.get("partitions") or ())
    complete = bool(parts) and all(p.get("status") == "COMPLETE" and p.get("rows") == p.get("expected_rows")
                                   for p in parts)
    quality = bool(parts) and all(
        p.get("invalid_rows") == 0 and p.get("duplicate_rows") == 0 and p.get("out_of_order") == 0
        and all(g.get("reason") in _EXPECTED_CLOSURES for g in p.get("missing_ranges") or ()) for p in parts)
    sources = {str(p.get("source") or "").lower() for p in parts}
    real = bool(sources) and not any(any(w in s for w in _SYNTHETIC) for s in sources)
    return {"manifest_frozen": frozen, "dataset_complete": complete, "quality_acceptable": quality,
            "mapping_frozen": bool(manifest.get("universe_hash")) and frozen, "real_data_only": real}


def pre_holdout_readiness(*, dataset_manifest: Optional[Mapping[str, Any]], acquisition_state: Optional[str],
                          freeze: Any = None, pre_holdout_run: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Pure and deterministic over recorded evidence (no wall clock, no holdout data)."""
    from app.trading_intelligence.research.certification.policy import canonical_certification_policy

    facts: Dict[str, Optional[bool]] = {"dataset_not_acquiring": str(acquisition_state or "").upper() not in (
        "ACQUIRING", "PARTIAL", "FAILED", "")}
    facts.update(_manifest_facts(dataset_manifest))
    facts["policy_hash_frozen"] = bool(freeze is not None and getattr(freeze, "certifiable", False))
    policy_hashes = dict(getattr(freeze, "policy_hashes", None) or {}) if freeze is not None else {}
    facts["canonical_policy"] = bool(freeze is not None and canonical_certification_policy().policy_hash
                                     in {str(v) for v in policy_hashes.values()})
    facts["code_identity_recorded"] = bool(freeze is not None and getattr(freeze, "source_commit", None))
    run = dict(pre_holdout_run or {})
    facts["pre_holdout_run_completed"] = run.get("status") == "PASS" and run.get("stage") in ("FULL", "MEDIUM")
    facts["evidence_produced"] = bool(run.get("artifact_hash"))
    reasons = [code for key, code in REQUIREMENTS if facts.get(key) is not True]
    status = READY if not reasons else NOT_READY
    body = {"version": PRE_HOLDOUT_VERSION, "status": status, "reason_codes": reasons,
            "facts": dict(sorted(facts.items())),
            "dataset_manifest_hash": (dataset_manifest or {}).get("manifest_hash"),
            "policy_freeze_hash": getattr(freeze, "freeze_hash", None) if freeze is not None else None}
    return {**body, "readiness_hash": stable_hash(body)}


def _events(db: Any, holdout_id: str) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute(f"SELECT state, payload, recorded_at FROM {_CONTROLS} WHERE control=? AND scope=? "
                            "ORDER BY recorded_at, rowid", (HOLDOUT_AUTHORIZATION, holdout_id)).fetchall()
    return [{"state": r[0], "payload": json.loads(r[1]), "recorded_at": r[2]} for r in rows]


def holdout_authorization(db: Any, holdout_id: str, policy_freeze_hash: str) -> Optional[Dict[str, Any]]:
    """The latest authorization event for this holdout, if it is ON and for THIS policy freeze."""
    ev = _events(db, holdout_id)
    if not ev or ev[-1]["state"] != "ON":
        return None
    return ev[-1] if ev[-1]["payload"].get("policy_freeze_hash") == policy_freeze_hash else None


def authorize_holdout(db: Any, *, holdout_id: str, readiness: Mapping[str, Any], policy_freeze_hash: str,
                      actor_ref: str, reason: str, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """The SEPARATE, deliberate operator action that allows one later run to open one holdout. Never called by
    this codebase automatically. Refused unless readiness is READY for this freeze and the holdout is untouched."""
    from app.trading_intelligence.research.certification.registry import HoldoutRegistry, RegistryError

    if readiness.get("status") != READY:
        raise RegistryError(f"holdout authorization refused: pre-holdout readiness is {readiness.get('status')} "
                            f"({','.join(readiness.get('reason_codes') or ())})")
    if readiness.get("policy_freeze_hash") != policy_freeze_hash:
        raise RegistryError("holdout authorization refused: readiness was computed for another policy freeze")
    if not (actor_ref and reason):
        raise RegistryError("holdout authorization needs a named actor and a reason")
    st = HoldoutRegistry(db).status(holdout_id)
    if st["status"] != "RESERVED":
        raise RegistryError(f"holdout {holdout_id} is {st['status']}: only an untouched, reserved holdout may be "
                            "authorized (a burned holdout can never be re-authorized)")
    ts = int(now_ms or time.time() * 1000)
    payload = sanitize_payload({"holdout_id": holdout_id, "dataset_hash": st.get("dataset_hash"),
                                "policy_freeze_hash": policy_freeze_hash, "readiness_hash": readiness["readiness_hash"],
                                "dataset_manifest_hash": readiness.get("dataset_manifest_hash"),
                                "actor_ref": actor_ref, "reason": reason})
    from app.trading_intelligence.governance.promotion import GOVERNANCE_VERSION

    with db.connect() as conn:
        conn.execute(
            f"INSERT INTO {_CONTROLS} (control_event_id, control, state, scope, recorded_at, schema_version, "
            "table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?)",
            (short_id("hauth", {"h": holdout_id, "f": policy_freeze_hash, "t": ts}), HOLDOUT_AUTHORIZATION, "ON",
             holdout_id, ts, GOVERNANCE_VERSION, GOVERNANCE_VERSION, json.dumps(payload, sort_keys=True),
             stable_hash(payload)))
    return {"holdout_id": holdout_id, "state": "ON", "payload": payload, "recorded_at": ts}


__all__ = ["DATA_ACQUISITION_IN_PROGRESS", "HOLDOUT_AUTHORIZATION", "NOT_READY", "PRE_HOLDOUT_VERSION", "READY",
           "REQUIREMENTS", "authorize_holdout", "holdout_authorization", "pre_holdout_readiness"]
