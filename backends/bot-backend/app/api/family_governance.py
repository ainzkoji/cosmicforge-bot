"""Administrative interface of CATI family governance (Section H, Step 3.11).

    GET  /api/v1/admin/governance/families                         the registry, as it stands now
    GET  /api/v1/admin/governance/families/{family_id}             one entry, its state and its whole history
    GET  /api/v1/admin/governance/history                          the governance log and its integrity
    GET  /api/v1/admin/governance/cycles                           the platform cycle driver's latest cycles
    GET  /api/v1/admin/governance/shadow/divergences               shadow-comparison alerts
    POST /api/v1/admin/governance/families/register                record the code-defined families (idempotent)
    POST /api/v1/admin/governance/families/{family_id}/evidence
    POST /api/v1/admin/governance/families/{family_id}/promotion-requests
    POST /api/v1/admin/governance/promotion-requests/{request_id}/approve
    POST /api/v1/admin/governance/promotion-requests/{request_id}/reject
    POST /api/v1/admin/governance/families/{family_id}/revoke
    POST /api/v1/admin/governance/families/{family_id}/revoke-certification
    POST /api/v1/admin/governance/families/{family_id}/kill-switch
    POST /api/v1/admin/governance/families/{family_id}/disable
    POST /api/v1/admin/governance/accounts/{account_id}/kill-switch

Authorisation is the emergency router's: ``require_admin_emergency``, the short-lived service token the
user-backend mints for a verified operator of the ``admins`` table. A plain ``role=admin`` access token is
refused. The operator id from that token is the reviewer recorded on every decision; there is no other way to
produce an ``OperatorAuthorization`` in the service.

What this interface cannot do, by construction:

* promote above M6, or move the global CATI phase (``PromotionGovernance.transition`` has no route);
* approve a promotion whose prerequisites the registry cannot verify *at the moment of approval* against the
  research register: the request body carries a reason, never a prerequisite;
* certify a family: certification is the research register's record and only the research process writes it;
* enable order submission: the submission flags are configuration and stay where they are.

The platform kill switch (all families, all accounts) and emergency flatten remain on the emergency router.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.api.emergency import get_db
from app.core.auth import require_admin_emergency
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/admin/governance", tags=["Admin Governance"])
CHANNEL, PERMISSION = "admin_api:/api/v1/admin/governance", "admin-emergency"


class Reason(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


class EvidenceRequest(BaseModel):
    key: str = Field(min_length=1, max_length=100)
    reference: str = Field(min_length=1, max_length=500)
    value: Any = True


class PromotionRequest(Reason):
    to_stage: str = Field(pattern=r"^M[0-9]$")
    evidence: Dict[str, Any] = Field(default_factory=dict)


class RevokeRequest(Reason):
    to_stage: str = Field(default="M0", pattern=r"^M[0-9]$")


class SwitchRequest(Reason):
    enabled: bool


def _operator(admin_id: str):
    from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization

    return OperatorAuthorization(operator_id=str(admin_id), channel=CHANNEL, permission=PERMISSION)


def _registry(db: DB):
    from app.trading_intelligence.pipeline.family_registry import FamilyRegistry

    return FamilyRegistry(db)


def _audit(db: Any, action: str, admin_id: str, details: Dict[str, Any]) -> None:
    record = {"actor": f"admin:{admin_id}", **details}
    logger.warning("[GOVERNANCE] %s %s", action, json.dumps(record, default=str, sort_keys=True))
    try:
        from shared_lib.persistence.audit import Audit

        Audit(db).event(event_type="GOVERNANCE", action=action, details=record)
    except Exception:
        logger.exception("[GOVERNANCE] audit event could not be persisted: %s", action)


def _refused(exc: Exception) -> JSONResponse:
    code = getattr(exc, "code", type(exc).__name__)
    status = 404 if code in ("FAMILY_NOT_REGISTERED", "REQUEST_NOT_FOUND") else 409
    return JSONResponse(status_code=status, content={"detail": str(exc), "reason": code})


def _act(db: DB, admin_id: str, action: str, details: Dict[str, Any], call) -> Any:
    """Audit the request, do it, audit the outcome. A governance refusal is an answer (409), not an error."""
    from app.trading_intelligence.pipeline.family_governance import FamilyGovernanceError

    _audit(db, f"{action}_REQUESTED", admin_id, details)
    try:
        event = call()
    except FamilyGovernanceError as exc:
        _audit(db, f"{action}_REFUSED", admin_id, {**details, "refusal": exc.code})
        return _refused(exc)
    _audit(db, f"{action}_RECORDED", admin_id, {**details, "event_id": event.get("event_id")})
    return {"event": event}


def _entry_view(registry: Any, entry: Any) -> Dict[str, Any]:
    state = registry.governance.state(entry.definition.family_id)
    view = entry.to_plain()
    view["pending_request"] = state.pending_request
    view["recorded_evidence"] = {k: v for k, v in state.evidence.items()}
    view["next_stage_prerequisites"] = None
    from app.trading_intelligence.governance.phases import next_phase, phase_index
    from app.trading_intelligence.pipeline.family_governance import MAX_OPERABLE_STAGE

    nxt = next_phase(state.stage)
    if state.registered and nxt and phase_index(nxt) <= phase_index(MAX_OPERABLE_STAGE):
        view["next_stage_prerequisites"] = {
            "stage": nxt, "missing": registry.governance.missing_prerequisites(
                state, nxt, prerequisites=registry.promotion_prerequisites(entry.definition.family_id))}
    return view


# ------------------------------------------------------------------------------------------------- reads
@router.get("/families")
def list_families(admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    registry = _registry(db)
    return {"families": [_entry_view(registry, e) for e in registry.entries()],
            "executable": [e.definition.family_id for e in registry.executable()],
            "log": registry.governance.verify_chain()}


@router.get("/families/{family_id}")
def get_family(family_id: str, admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    registry = _registry(db)
    entry = registry.entry(family_id)
    if entry is None:
        return JSONResponse(status_code=404, content={"detail": family_id, "reason": "FAMILY_UNKNOWN"})
    return {"family": _entry_view(registry, entry), "history": registry.governance.history(family_id)}


@router.get("/history")
def governance_history(admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return {"integrity": governance.verify_chain(), "events": governance.history()}


@router.get("/cycles")
def platform_cycles(limit: int = 50, admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    from shared_lib.persistence.cati_platform_schema import CYCLE_TABLE

    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            f"SELECT cycle_id, family_id, cycle_key, mode, status, attempt, started_at, completed_at, accounts_total, "
            f"accounts_done, accounts_failed FROM {CYCLE_TABLE} ORDER BY started_at DESC LIMIT ?", (max(1, min(int(limit), 500)),))]
    return {"cycles": rows}


@router.get("/shadow/divergences")
def shadow_divergences(limit: int = 100, admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    from shared_lib.persistence.cati_platform_schema import SHADOW_COMPARISON_TABLE

    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            f"SELECT * FROM {SHADOW_COMPARISON_TABLE} WHERE status != 'MATCH' ORDER BY recorded_at DESC LIMIT ?",
            (max(1, min(int(limit), 1000)),))]
        totals = {r[0]: r[1] for r in conn.execute(f"SELECT status, COUNT(*) FROM {SHADOW_COMPARISON_TABLE} GROUP BY status")}
    for r in rows:
        r["document"] = json.loads(r["document"])
    return {"totals": totals, "divergences": rows}


# ------------------------------------------------------------------------------------------------ writes
@router.post("/families/register")
def register_families(admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    registry = _registry(db)
    _audit(db, "FAMILIES_REGISTER_REQUESTED", admin_id, {})
    events = registry.register_all(actor_ref=_operator(admin_id).actor_ref)
    _audit(db, "FAMILIES_REGISTER_RECORDED", admin_id,
           {"registered": [e["family_id"] for e in events if not e.get("unchanged")]})
    return {"events": events}


@router.post("/families/{family_id}/evidence")
def record_evidence(family_id: str, body: EvidenceRequest, admin_id: str = Depends(require_admin_emergency),
                    db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return _act(db, admin_id, "EVIDENCE", {"family_id": family_id, "key": body.key, "reference": body.reference},
                lambda: governance.record_evidence(family_id, body.key, reference=body.reference, value=body.value,
                                                   actor_ref=_operator(admin_id).actor_ref))


@router.post("/families/{family_id}/promotion-requests")
def request_promotion(family_id: str, body: PromotionRequest, admin_id: str = Depends(require_admin_emergency),
                      db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return _act(db, admin_id, "PROMOTION_REQUEST", {"family_id": family_id, "to_stage": body.to_stage, "reason": body.reason},
                lambda: governance.request_promotion(family_id, body.to_stage, actor_ref=_operator(admin_id).actor_ref,
                                                     reason=body.reason, evidence=body.evidence))


@router.post("/promotion-requests/{request_id}/approve")
def approve_promotion(request_id: str, body: Reason, admin_id: str = Depends(require_admin_emergency),
                      db: DB = Depends(get_db)):
    registry = _registry(db)

    def approve():
        family_id = registry.governance.family_of_request(request_id)
        # verified here and now against the research register; nothing the caller sent is a prerequisite
        return registry.governance.approve_promotion(request_id, operator=_operator(admin_id), reason=body.reason,
                                                     prerequisites=registry.promotion_prerequisites(family_id))

    return _act(db, admin_id, "PROMOTION_APPROVAL", {"request_id": request_id, "reason": body.reason}, approve)


@router.post("/promotion-requests/{request_id}/reject")
def reject_promotion(request_id: str, body: Reason, admin_id: str = Depends(require_admin_emergency),
                     db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return _act(db, admin_id, "PROMOTION_REJECTION", {"request_id": request_id, "reason": body.reason},
                lambda: governance.reject_promotion(request_id, operator=_operator(admin_id), reason=body.reason))


@router.post("/families/{family_id}/revoke")
def revoke_promotion(family_id: str, body: RevokeRequest, admin_id: str = Depends(require_admin_emergency),
                     db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return _act(db, admin_id, "PROMOTION_REVOCATION", {"family_id": family_id, "to_stage": body.to_stage, "reason": body.reason},
                lambda: governance.revoke_promotion(family_id, operator=_operator(admin_id), reason=body.reason,
                                                    to_stage=body.to_stage))


@router.post("/families/{family_id}/revoke-certification")
def revoke_certification(family_id: str, body: Reason, admin_id: str = Depends(require_admin_emergency),
                         db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return _act(db, admin_id, "CERTIFICATION_REVOCATION", {"family_id": family_id, "reason": body.reason},
                lambda: governance.revoke_certification(family_id, operator=_operator(admin_id), reason=body.reason))


@router.post("/families/{family_id}/kill-switch")
def family_kill_switch(family_id: str, body: SwitchRequest, admin_id: str = Depends(require_admin_emergency),
                       db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return _act(db, admin_id, "FAMILY_KILL_SWITCH", {"family_id": family_id, "enabled": body.enabled, "reason": body.reason},
                lambda: governance.set_kill_switch(family_id, body.enabled, operator=_operator(admin_id), reason=body.reason))


@router.post("/families/{family_id}/disable")
def family_disable(family_id: str, body: SwitchRequest, admin_id: str = Depends(require_admin_emergency),
                   db: DB = Depends(get_db)):
    governance = _registry(db).governance
    return _act(db, admin_id, "FAMILY_DISABLE", {"family_id": family_id, "disabled": body.enabled, "reason": body.reason},
                lambda: governance.set_disabled(family_id, body.enabled, operator=_operator(admin_id), reason=body.reason))


@router.post("/accounts/{account_id}/kill-switch")
def account_kill_switch(account_id: str, body: SwitchRequest, admin_id: str = Depends(require_admin_emergency),
                        db: DB = Depends(get_db)):
    """No new entry for ONE account, from any family. Its positions keep their protection and their exits."""
    from app.trading_intelligence.governance.promotion import GLOBAL_SCOPE, PromotionGovernance

    if account_id == GLOBAL_SCOPE:
        return JSONResponse(status_code=400, content={"detail": "the platform kill switch is on the emergency router",
                                                      "reason": "SCOPE_INVALID"})
    with db.connect() as conn:
        known = conn.execute("SELECT 1 FROM broker_accounts WHERE id=?", (account_id,)).fetchone()
    if not known:
        return JSONResponse(status_code=404, content={"detail": account_id, "reason": "ACCOUNT_UNKNOWN"})
    governance = PromotionGovernance(db)
    details = {"account_id": account_id, "enabled": body.enabled, "reason": body.reason}
    _audit(db, "ACCOUNT_KILL_SWITCH_REQUESTED", admin_id, details)
    governance.set_kill_switch(body.enabled, reason=body.reason, actor_ref=_operator(admin_id).actor_ref, scope=account_id)
    status = governance.kill_switch_status(account_id)
    if status["enabled"] != body.enabled:
        _audit(db, "ACCOUNT_KILL_SWITCH_FAILED", admin_id, details)
        return JSONResponse(status_code=503, content={"detail": "the change could not be persisted",
                                                      "reason": "KILL_SWITCH_NOT_PERSISTED"})
    _audit(db, "ACCOUNT_KILL_SWITCH_SET", admin_id, details)
    return {"account_kill_switch": status}


__all__ = ["router", "get_db"]
