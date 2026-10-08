"""Operator plan grants (Step 1.5): /api/admin/billing/grants.

Administrators only (``require_admin``: an admin access token from the admins
table). A grant identifies its issuer, records user, plan, timestamps and
reason, has an explicit expiry, is distinguishable from Stripe (``provider =
'operator'``), is never overwritten by Stripe webhook processing while active,
and is revocable only here. Every call is also written to ``auth_audit_log``.
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.deps import require_admin
from shared_lib.billing import operator_grants
from shared_lib.billing.enforcement import billing_enforced
from shared_lib.persistence.db import DB, utc_now_iso

router = APIRouter(prefix="/admin/billing", tags=["Admin Billing"])


def _db() -> DB:
    return DB()


class GrantRequest(BaseModel):
    user_id: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    reason: str = Field(min_length=3, max_length=500)
    duration_days: int = Field(operator_grants.DEFAULT_DURATION_DAYS, ge=1, le=operator_grants.MAX_DURATION_DAYS)


class RevokeRequest(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


def _audit(db: DB, admin: Dict[str, Any], event_type: str, user_id: Optional[str], details: Dict[str, Any]) -> None:
    """Durable audit row; a failing audit store never changes the answer."""
    try:
        with db.connect() as conn:
            conn.execute(
                "INSERT INTO auth_audit_log (id, event_type, user_id, email, details, created_at) VALUES (?,?,?,?,?,?)",
                (str(uuid.uuid4()), event_type, user_id, admin.get("email"),
                 json.dumps({"admin_id": admin.get("id"), **details}, sort_keys=True, default=str), utc_now_iso()))
    except Exception:  # pragma: no cover - audit must not mask the outcome
        pass


@router.get("/grants")
def list_grants(user_id: Optional[str] = Query(None), limit: int = Query(100, ge=1, le=500),
                admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    db = _db()
    return {"billing_enforced": billing_enforced(), "grants": operator_grants.list_grants(db, user_id=user_id, limit=limit)}


@router.post("/grants", status_code=201)
def create_grant(body: GrantRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    db = _db()
    try:
        result = operator_grants.grant(db, user_id=body.user_id, plan_id=body.plan_id, issued_by=str(admin["id"]),
                                       reason=body.reason, duration_days=body.duration_days)
    except operator_grants.GrantError as exc:
        _audit(db, admin, "operator_plan_grant_refused", body.user_id, {"plan_id": body.plan_id, "error": exc.code})
        raise HTTPException(status_code=404 if exc.code == "USER_NOT_FOUND" else 400,
                            detail={"error_code": exc.code, "message": str(exc)})
    _audit(db, admin, "operator_plan_granted", body.user_id, result)
    return {**result, "billing_enforced": billing_enforced()}


@router.post("/grants/{grant_id}/revoke")
def revoke_grant(grant_id: str, body: RevokeRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    db = _db()
    try:
        result = operator_grants.revoke(db, grant_id=grant_id, revoked_by=str(admin["id"]), reason=body.reason)
    except operator_grants.GrantError as exc:
        _audit(db, admin, "operator_plan_revoke_refused", None, {"grant_id": grant_id, "error": exc.code})
        raise HTTPException(status_code=404 if exc.code == "GRANT_NOT_FOUND" else 409,
                            detail={"error_code": exc.code, "message": str(exc)})
    _audit(db, admin, "operator_plan_revoked", result["user_id"], result)
    return result


__all__ = ["router"]
