from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.tradingview_repo import (
    get_processor_heartbeat,
    list_alerts,
    list_decisions,
    list_external_signal_queue,
    list_processor_heartbeats,
    list_webhooks,
)


router = APIRouter(prefix="/api/admin/tradingview", tags=["admin-tradingview"])
logger = logging.getLogger(__name__)


def _log_timing(endpoint: str, started_at: float, *, row_count: int) -> None:
    logger.info(
        "admin_tradingview_endpoint endpoint=%s duration_ms=%s row_count=%s",
        endpoint,
        round((time.perf_counter() - started_at) * 1000, 2),
        row_count,
    )


@router.get("/webhooks")
def get_tradingview_webhooks(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, list[dict[str, Any]]]:
    del _admin
    started_at = time.perf_counter()
    items = list_webhooks(db, limit=limit)
    _log_timing("GET /api/admin/tradingview/webhooks", started_at, row_count=len(items))
    return {"items": items}


@router.get("/alerts")
def get_tradingview_alerts(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, list[dict[str, Any]]]:
    del _admin
    started_at = time.perf_counter()
    items = list_alerts(db, limit=limit)
    _log_timing("GET /api/admin/tradingview/alerts", started_at, row_count=len(items))
    return {"items": items}


@router.get("/decisions")
def get_tradingview_decisions(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, list[dict[str, Any]]]:
    del _admin
    started_at = time.perf_counter()
    items = list_decisions(db, limit=limit)
    _log_timing("GET /api/admin/tradingview/decisions", started_at, row_count=len(items))
    return {"items": items}


@router.get("/external-signals")
def get_tradingview_external_signals(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, list[dict[str, Any]]]:
    del _admin
    started_at = time.perf_counter()
    items = list_external_signal_queue(db, limit=limit)
    _log_timing("GET /api/admin/tradingview/external-signals", started_at, row_count=len(items))
    return {"items": items}


@router.get("/processor-status")
def get_tradingview_processor_status(
    bot_instance_id: str | None = Query(None, description="Filter by bot instance ID"),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    if bot_instance_id:
        heartbeat = get_processor_heartbeat(db, bot_instance_id)
        items = [] if heartbeat is None else [heartbeat]
        _log_timing("GET /api/admin/tradingview/processor-status", started_at, row_count=len(items))
        if heartbeat is None:
            return {"items": [], "note": f"No heartbeat found for bot_instance_id={bot_instance_id!r}"}
        return {"items": items}

    items = list_processor_heartbeats(db, limit=limit)
    _log_timing("GET /api/admin/tradingview/processor-status", started_at, row_count=len(items))
    return {"items": items}
