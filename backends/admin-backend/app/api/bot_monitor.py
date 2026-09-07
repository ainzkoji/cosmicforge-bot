from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.bot_monitor_repo import (
    get_bot_live_status,
    get_bot_overview,
    get_bot_run_details,
    list_bot_runs,
)


router = APIRouter(prefix="/api/admin/bot", tags=["admin-bot-monitor"])
logger = logging.getLogger(__name__)


def _log_timing(endpoint: str, started_at: float, *, row_count: int | None = None) -> None:
    logger.info(
        "admin_bot_monitor_endpoint endpoint=%s duration_ms=%s row_count=%s",
        endpoint,
        round((time.perf_counter() - started_at) * 1000, 2),
        row_count,
    )


@router.get("/overview")
def bot_overview(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    payload = get_bot_overview(db)
    _log_timing("GET /api/admin/bot/overview", started_at, row_count=1)
    return payload


@router.get("/runs")
def bot_runs(
    limit: int = Query(20, ge=1, le=100),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    payload = list_bot_runs(db, limit=limit)
    _log_timing("GET /api/admin/bot/runs", started_at, row_count=int(payload.get("count", 0)))
    return payload


@router.get("/runs/{run_id}")
def bot_run_details(
    run_id: str,
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    payload = get_bot_run_details(db, run_id)
    _log_timing("GET /api/admin/bot/runs/{run_id}", started_at, row_count=1 if payload else 0)
    if payload is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return payload


@router.get("/live")
def bot_live(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    payload = get_bot_live_status(db)
    _log_timing(
        "GET /api/admin/bot/live",
        started_at,
        row_count=len(payload.get("positions", [])) + len(payload.get("latest_decisions", [])) + len(payload.get("latest_events", [])),
    )
    return payload
