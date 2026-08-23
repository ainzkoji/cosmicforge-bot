from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.profitability_repo import get_profitability_report


router = APIRouter(prefix="/api/admin/profitability", tags=["admin-profitability"])
logger = logging.getLogger(__name__)


@router.get("/report")
def profitability_report(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = get_profitability_report(db)
    logger.info(
        "admin_profitability_endpoint endpoint=%s duration_ms=%s symbol_count=%s sizing_event_count=%s",
        "GET /api/admin/profitability/report",
        round((time.perf_counter() - started_at) * 1000, 2),
        len(payload.get("per_symbol", [])),
        len(payload.get("sizing_cap_events", [])),
    )
    return payload
