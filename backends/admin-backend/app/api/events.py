from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.events_repo import (
    count_active_blackouts,
    get_active_blackout_windows,
    get_event_snapshots,
    get_last_sync_utc,
    get_reactions_for_event,
    get_recent_reactions,
    get_single_reaction,
    get_upcoming_events,
)


router = APIRouter(prefix="/api/admin/events", tags=["admin-events"])
logger = logging.getLogger(__name__)

STALE_THRESHOLD_HOURS = 24


def _log_timing(endpoint: str, started_at: float, *, row_count: int | None = None) -> None:
    logger.info(
        "admin_events_endpoint endpoint=%s duration_ms=%s row_count=%s",
        endpoint,
        round((time.perf_counter() - started_at) * 1000, 2),
        row_count,
    )


@router.get("/upcoming")
def get_upcoming_events_endpoint(
    days: int = Query(default=7, ge=1, le=30),
    impact: str | None = Query(default=None),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    now = datetime.now(timezone.utc)
    to_utc = (now + timedelta(days=days)).isoformat()
    impact_levels = [i.strip().upper() for i in impact.split(",")] if impact else None
    rows = get_upcoming_events(
        db,
        from_utc=now.isoformat(),
        to_utc=to_utc,
        impact_levels=impact_levels,
    )
    _log_timing("GET /api/admin/events/upcoming", started_at, row_count=len(rows))
    return rows


@router.get("/active-blackouts")
def get_active_blackouts_endpoint(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = get_active_blackout_windows(db, at_utc=now_iso)
    _log_timing("GET /api/admin/events/active-blackouts", started_at, row_count=len(rows))
    return rows


@router.get("/feed-status")
def get_feed_status(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    last_sync = get_last_sync_utc(db)
    staleness_hours: float | None = None
    if last_sync:
        try:
            last_dt = datetime.fromisoformat(last_sync)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            staleness_hours = (
                datetime.now(timezone.utc) - last_dt
            ).total_seconds() / 3600.0
        except Exception:
            pass
    is_stale = staleness_hours is None or staleness_hours > STALE_THRESHOLD_HOURS
    now_iso = datetime.now(timezone.utc).isoformat()
    active_blackout_count = count_active_blackouts(db, at_utc=now_iso)
    result = {
        "last_sync_utc": last_sync,
        "staleness_hours": round(staleness_hours, 2) if staleness_hours is not None else None,
        "is_stale": is_stale,
        "stale_threshold_hours": STALE_THRESHOLD_HOURS,
        "active_blackout_count": active_blackout_count,
    }
    _log_timing("GET /api/admin/events/feed-status", started_at)
    return result


@router.get("/reactions/summary")
def get_reaction_summary_endpoint(
    days: int = Query(default=7, ge=1, le=30),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    rows = get_recent_reactions(db, days=days, limit=500)
    by_type: dict[str, int] = {}
    by_quality: dict[str, int] = {}
    latest: str | None = None
    for row in rows:
        rt = row.get("reaction_type", "NO_REACTION")
        dq = row.get("data_quality", "UNKNOWN")
        by_type[rt] = by_type.get(rt, 0) + 1
        by_quality[dq] = by_quality.get(dq, 0) + 1
        updated = row.get("updated_at") or row.get("created_at")
        if updated and (latest is None or updated > latest):
            latest = updated
    result = {
        "total_reactions": len(rows),
        "by_type": [{"reaction_type": k, "count": v} for k, v in sorted(by_type.items())],
        "by_quality": [{"data_quality": k, "count": v} for k, v in sorted(by_quality.items())],
        "latest_reaction_utc": latest,
    }
    _log_timing("GET /api/admin/events/reactions/summary", started_at)
    return result


@router.get("/reactions/recent")
def get_recent_reactions_endpoint(
    days: int = Query(default=7, ge=1, le=30),
    limit: int = Query(default=50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_recent_reactions(db, days=days, limit=limit)
    _log_timing("GET /api/admin/events/reactions/recent", started_at, row_count=len(rows))
    return rows


@router.get("/reactions/{event_id}")
def get_reactions_for_event_endpoint(
    event_id: str,
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_reactions_for_event(db, event_id=event_id)
    _log_timing(f"GET /api/admin/events/reactions/{event_id}", started_at, row_count=len(rows))
    return rows


@router.get("/reactions/{event_id}/{symbol}")
def get_single_reaction_endpoint(
    event_id: str,
    symbol: str,
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    row = get_single_reaction(db, event_id=event_id, symbol=symbol)
    _log_timing(f"GET /api/admin/events/reactions/{event_id}/{symbol}", started_at)
    if row is None:
        raise HTTPException(status_code=404, detail="Reaction not found")
    return row


@router.get("/snapshots/{event_id}/{symbol}")
def get_event_snapshots_endpoint(
    event_id: str,
    symbol: str,
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_event_snapshots(db, event_id=event_id, symbol=symbol)
    _log_timing(f"GET /api/admin/events/snapshots/{event_id}/{symbol}", started_at, row_count=len(rows))
    return rows
