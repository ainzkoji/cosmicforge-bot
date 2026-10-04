"""Owner-scoped production broker observations, never a virtual ledger."""
from fastapi import APIRouter, Depends
from app.core.auth import get_current_active_user, require_permission
from app.trading_intelligence.integration.production_runtime import status
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/api/v1/cati/runtime", tags=["CATI production"])


@router.get("/status")
def runtime_status(user: dict = Depends(get_current_active_user),
                   permission=Depends(require_permission("bot:read"))):
    return status(DB(), user_id=user["id"])
