from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.users_repo import list_users_read_only


router = APIRouter(prefix="/api/admin", tags=["admin-users"])
logger = logging.getLogger(__name__)


@router.get("/users")
def list_users(
    _admin: dict = Depends(require_admin),
    status: str | None = None,
    limit: int = Query(50, ge=1, le=100),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = list_users_read_only(db, status=status, limit=limit)
    logger.info(
        "admin_users_endpoint endpoint=%s duration_ms=%s row_count=%s",
        "GET /api/admin/users",
        round((time.perf_counter() - started_at) * 1000, 2),
        payload.get("count", 0),
    )
    return payload
