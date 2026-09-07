from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.core.auth import require_admin
from app.product_safety.approvals import (
    approve_readiness, confirm_sections_a_to_e, invalidate_readiness_approval,
)
from app.product_safety.readiness_gate import ReadinessStatus, evaluate_user_capital_readiness
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/api/v1/admin", tags=["Readiness Admin"])


def _current_policy_hash(db: DB, bot_id: str) -> str:
    from app.core.bot_instance_service import BotInstanceService
    from app.runner.effective_policy import resolve_effective_bot_policy
    service = BotInstanceService(db)
    instance = service.get_bot_instance(bot_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    with db.connect() as conn:
        row = conn.execute("SELECT environment FROM broker_accounts WHERE id=?", (instance.broker_account_id,)).fetchone()
    policy = resolve_effective_bot_policy(
        instance=instance,
        risk_params=service.get_risk_profile_preset(instance.risk_level),
        broker_environment=(row["environment"] if row else "unknown"),
    )
    return policy.policy_hash


class ApprovalRequest(BaseModel):
    policy_hash: str = Field(min_length=8)
    source_commit_sha: str | None = None
    notes: str | None = None


class RevocationRequest(BaseModel):
    reason: str = Field(min_length=3)


class SectionsConfirmationRequest(BaseModel):
    source_commit_sha: str | None = None
    release_ref: str | None = None
    evidence_reference: str = Field(min_length=3)


@router.post("/bots/{bot_id}/readiness/approve")
def approve_bot(bot_id: str, body: ApprovalRequest, admin_id: str = Depends(require_admin)):
    db = DB()
    current_hash = _current_policy_hash(db, bot_id)
    if body.policy_hash != current_hash:
        raise HTTPException(
            status_code=409,
            detail={"reason": "POLICY_HASH_MISMATCH", "current_policy_hash": current_hash},
        )
    report = evaluate_user_capital_readiness(db=db, bot_instance_id=bot_id)
    if report.readiness_status != ReadinessStatus.READY_FOR_CONTROLLED_BETA_REVIEW:
        raise HTTPException(status_code=409, detail={"reason": "BOT_NOT_READY_FOR_REVIEW", "report": report.to_dict()})
    approval = approve_readiness(
        db=db, bot_instance_id=bot_id, reviewer_admin_id=admin_id,
        policy_hash=current_hash, evidence_snapshot=report.to_dict(),
        source_commit_sha=body.source_commit_sha, notes=body.notes,
    )
    return {**approval, "readiness_status": ReadinessStatus.APPROVED_FOR_CONTROLLED_BETA.value}


@router.post("/bots/{bot_id}/readiness/revoke")
def revoke_bot(bot_id: str, body: RevocationRequest, admin_id: str = Depends(require_admin)):
    count = invalidate_readiness_approval(
        db=DB(), bot_instance_id=bot_id, reason=body.reason,
        current_policy_hash=None, revoked_by=admin_id,
    )
    return {"bot_instance_id": bot_id, "revoked": bool(count)}


@router.post("/readiness/sections-a-e/confirm")
def confirm_sections(body: SectionsConfirmationRequest, admin_id: str = Depends(require_admin)):
    return confirm_sections_a_to_e(
        db=DB(), confirmed_by=admin_id, source_commit_sha=body.source_commit_sha,
        release_ref=body.release_ref, evidence_reference=body.evidence_reference,
    )
