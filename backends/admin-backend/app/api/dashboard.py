from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.dashboard_repo import (
    get_dashboard_stats,
    get_revenue_overview,
    get_top_trading_pairs,
)


router = APIRouter(prefix="/api/admin/dashboard", tags=["admin-dashboard"])
logger = logging.getLogger(__name__)


def _log_timing(endpoint: str, started_at: float, *, row_count: int | None = None) -> None:
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    logger.info(
        "admin_dashboard_endpoint endpoint=%s duration_ms=%s row_count=%s",
        endpoint,
        duration_ms,
        row_count,
    )


@router.get("/stats")
def dashboard_stats(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = get_dashboard_stats(db)
    _log_timing("GET /api/admin/dashboard/stats", started_at, row_count=1)
    return payload


@router.get("/revenue-overview")
def revenue_overview(
    timeframe: str = Query("12m", pattern="^(30d|6m|12m)$"),
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = get_revenue_overview(db, timeframe=timeframe)
    _log_timing("GET /api/admin/dashboard/revenue-overview", started_at, row_count=len(payload.get("data", [])))
    return payload


@router.get("/top-trading-pairs")
def top_trading_pairs(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = get_top_trading_pairs(db, limit=5)
    _log_timing("GET /api/admin/dashboard/top-trading-pairs", started_at, row_count=len(payload.get("data", [])))
    return payload
