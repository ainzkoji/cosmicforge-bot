"""Authenticated owner-scoped views of CATI's local simulation ledger."""
from fastapi import APIRouter, Depends, HTTPException
from app.core.auth import get_current_active_user, require_permission
from app.trading_intelligence.integration.residual_simulation import Book
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/api/v1/cati/simulation", tags=["CATI simulated trading"])


def get_db():
    return DB()


@router.get("/status")
def status(user: dict = Depends(get_current_active_user), db=Depends(get_db),
           permission=Depends(require_permission("bot:read"))):
    result = Book(db).status()
    if result.get("owner_user_id") and result["owner_user_id"] != user["id"]:
        raise HTTPException(403, "CATI simulation belongs to another user")
    return result
