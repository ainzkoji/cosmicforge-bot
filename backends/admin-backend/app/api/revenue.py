from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.revenue_repo import get_revenue_overview


router = APIRouter(prefix="/api/admin/revenue", tags=["admin-revenue"])
logger = logging.getLogger(__name__)


@router.get("/overview")
def revenue_overview(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = get_revenue_overview(db)
    logger.info(
        "admin_revenue_endpoint endpoint=%s duration_ms=%s row_count=%s",
        "GET /api/admin/revenue/overview",
        round((time.perf_counter() - started_at) * 1000, 2),
        len(payload.get("revenue_by_plan", [])),
    )
    return payload
