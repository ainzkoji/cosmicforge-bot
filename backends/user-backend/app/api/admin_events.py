"""
Admin API for the Event Awareness system.

Endpoints:
  GET  /admin/events/upcoming          — events in the next N days
  GET  /admin/events/active-blackouts  — currently active blackout windows
  GET  /admin/events/feed-status       — staleness info + last sync timestamp
  POST /admin/events/seed              — manually seed an economic event
  DELETE /admin/events/{event_id}      — soft-delete (deactivate windows for) an event
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.deps import require_admin
from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import (
    insert_event,
    get_upcoming_events,
    get_active_blackout_windows,
    get_last_sync_utc,
    deactivate_past_windows,
)
from shared_lib.persistence.market_reactions import (
    get_reactions_for_event,
    get_recent_reactions,
    get_reaction,
    get_snapshots,
)


router = APIRouter(prefix="/admin/events", tags=["admin-events"])


def _get_db() -> DB:
    return DB()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class EventOut(BaseModel):
    id: int
    event_id: str
    title: str
    event_type: str
    country_currency: str
    impact_level: str
    scheduled_utc: str
    actual_val: Optional[float] = None
    forecast_val: Optional[float] = None
    previous_val: Optional[float] = None
    source: str
    created_at: str
    updated_at: str


class BlackoutWindowOut(BaseModel):
    id: int
    event_id: int
    start_utc: str
    end_utc: str
    affected_symbols: Optional[str] = None
    is_global: bool
    reason: str
    title: str
    event_type: str
    impact_level: str
    country_currency: str


class FeedStatusOut(BaseModel):
    last_sync_utc: Optional[str]
    staleness_hours: Optional[float]
    is_stale: bool
    stale_threshold_hours: int
    active_blackout_count: int


class SeedEventRequest(BaseModel):
    title: str = Field(..., min_length=2, max_length=200)
    event_type: str = Field(..., min_length=2, max_length=50)
    country_currency: str = Field(..., min_length=2, max_length=10)
    impact_level: str = Field(..., pattern="^(HIGH|MEDIUM|LOW)$")
    scheduled_utc: str = Field(..., description="ISO-8601 UTC datetime, e.g. 2026-05-01T12:30:00+00:00")
    forecast_val: Optional[float] = None
    previous_val: Optional[float] = None
    source: str = "manual"


class SeedEventResponse(BaseModel):
    event_id: str
    rowid: int
    message: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/upcoming", response_model=List[EventOut])
async def get_upcoming_events_endpoint(
    days: int = Query(default=7, ge=1, le=30),
    impact: Optional[str] = Query(default=None, description="Comma-separated: HIGH,MEDIUM,LOW"),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[EventOut]:
    now = datetime.now(timezone.utc)
    to_utc = (now + timedelta(days=days)).isoformat()
    impact_levels = [i.strip().upper() for i in impact.split(",")] if impact else None
    rows = get_upcoming_events(db, from_utc=now.isoformat(), to_utc=to_utc, impact_levels=impact_levels)
    return [EventOut(**r) for r in rows]


@router.get("/active-blackouts", response_model=List[BlackoutWindowOut])
async def get_active_blackouts_endpoint(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[BlackoutWindowOut]:
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = get_active_blackout_windows(db, at_utc=now_iso)
    return [
        BlackoutWindowOut(
            id=r["id"],
            event_id=r["event_id"],
            start_utc=r["start_utc"],
            end_utc=r["end_utc"],
            affected_symbols=r.get("affected_symbols"),
            is_global=bool(r.get("is_global", 1)),
            reason=r.get("reason", ""),
            title=r.get("title", ""),
            event_type=r.get("event_type", ""),
            impact_level=r.get("impact_level", ""),
            country_currency=r.get("country_currency", ""),
        )
        for r in rows
    ]


@router.get("/feed-status", response_model=FeedStatusOut)
async def get_feed_status(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> FeedStatusOut:
    from app.core.config import settings as bot_settings

    last_sync = get_last_sync_utc(db)
    staleness_hours: Optional[float] = None
    stale_threshold = 24  # default; bot settings may differ

    try:
        stale_threshold = getattr(bot_settings, "EVENT_FEED_STALE_HOURS", 24)
    except Exception:
        pass

    if last_sync:
        try:
            last_dt = datetime.fromisoformat(last_sync)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            staleness_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600.0
        except Exception:
            pass

    is_stale = staleness_hours is None or staleness_hours > stale_threshold

    now_iso = datetime.now(timezone.utc).isoformat()
    active_windows = get_active_blackout_windows(db, at_utc=now_iso)

    return FeedStatusOut(
        last_sync_utc=last_sync,
        staleness_hours=round(staleness_hours, 2) if staleness_hours is not None else None,
        is_stale=is_stale,
        stale_threshold_hours=stale_threshold,
        active_blackout_count=len(active_windows),
    )


@router.post("/seed", response_model=SeedEventResponse)
async def seed_event(
    body: SeedEventRequest,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> SeedEventResponse:
    # Validate scheduled_utc is parseable
    try:
        datetime.fromisoformat(body.scheduled_utc)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid scheduled_utc: {exc}") from exc

    event_id = str(uuid.uuid4())
    rowid = insert_event(
        db,
        title=body.title,
        event_type=body.event_type,
        country_currency=body.country_currency,
        impact_level=body.impact_level,
        scheduled_utc=body.scheduled_utc,
        forecast_val=body.forecast_val,
        previous_val=body.previous_val,
        source=body.source,
        event_id=event_id,
    )
    return SeedEventResponse(
        event_id=event_id,
        rowid=rowid or 0,
        message="Event seeded. Blackout windows will be generated on next calendar sync.",
    )


@router.post("/cleanup-expired")
async def cleanup_expired_windows(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    count = deactivate_past_windows(db, before_utc=now_iso)
    return {"deactivated": count, "message": f"Deactivated {count} expired blackout windows."}


# ---------------------------------------------------------------------------
# Phase 2 — Market Reaction Layer endpoints
# ---------------------------------------------------------------------------

class ReactionOut(BaseModel):
    id: Optional[int] = None
    event_id: str
    symbol: str
    exchange: str
    pre_window_start_utc: Optional[str] = None
    event_time_utc: str
    post_window_end_utc: Optional[str] = None
    price_before_event: Optional[float] = None
    price_at_event: Optional[float] = None
    price_after_5m: Optional[float] = None
    price_after_15m: Optional[float] = None
    price_after_30m: Optional[float] = None
    price_after_60m: Optional[float] = None
    max_move_pct: Optional[float] = None
    min_move_pct: Optional[float] = None
    net_move_pct: Optional[float] = None
    direction_after_event: Optional[str] = None
    continuation_or_reversal: Optional[str] = None
    atr_before: Optional[float] = None
    atr_after: Optional[float] = None
    volatility_expansion_ratio: Optional[float] = None
    candle_range_before: Optional[float] = None
    candle_range_during: Optional[float] = None
    realized_vol_before: Optional[float] = None
    realized_vol_after: Optional[float] = None
    average_volume_before: Optional[float] = None
    event_volume: Optional[float] = None
    volume_spike_ratio: Optional[float] = None
    abnormal_volume_score: Optional[float] = None
    spread_before: Optional[float] = None
    spread_during: Optional[float] = None
    spread_after: Optional[float] = None
    spread_widening_ratio: Optional[float] = None
    reaction_type: str
    confidence_score: Optional[float] = None
    data_quality: str
    created_at: str
    updated_at: str


class SnapshotOut(BaseModel):
    id: Optional[int] = None
    event_id: str
    symbol: str
    exchange: str
    timestamp_utc: str
    window_label: str
    price: Optional[float] = None
    volume: Optional[float] = None
    candle_open: Optional[float] = None
    candle_high: Optional[float] = None
    candle_low: Optional[float] = None
    candle_close: Optional[float] = None
    atr: Optional[float] = None
    spread: Optional[float] = None
    source: str
    created_at: str


@router.get("/reactions/summary")
async def get_reaction_summary_endpoint(
    days: int = Query(default=7, ge=1, le=30),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> Dict[str, Any]:
    rows = get_recent_reactions(db, days=days, limit=500)
    by_type: Dict[str, int] = {}
    by_quality: Dict[str, int] = {}
    latest: Optional[str] = None
    for row in rows:
        by_type[row.get("reaction_type", "NO_REACTION")] = by_type.get(row.get("reaction_type", "NO_REACTION"), 0) + 1
        by_quality[row.get("data_quality", "UNKNOWN")] = by_quality.get(row.get("data_quality", "UNKNOWN"), 0) + 1
        updated = row.get("updated_at") or row.get("created_at")
        if updated and (latest is None or updated > latest):
            latest = updated
    return {
        "total_reactions": len(rows),
        "by_type": [{"reaction_type": k, "count": v} for k, v in sorted(by_type.items())],
        "by_quality": [{"data_quality": k, "count": v} for k, v in sorted(by_quality.items())],
        "latest_reaction_utc": latest,
    }


@router.get("/reactions/recent", response_model=List[ReactionOut])
async def get_recent_reactions_endpoint(
    days: int = Query(default=7, ge=1, le=30),
    limit: int = Query(default=50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[ReactionOut]:
    rows = get_recent_reactions(db, days=days, limit=limit)
    return [ReactionOut(**r) for r in rows]


@router.get("/reactions/{event_id}", response_model=List[ReactionOut])
async def get_reactions_for_event_endpoint(
    event_id: str,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[ReactionOut]:
    rows = get_reactions_for_event(db, event_id=event_id)
    return [ReactionOut(**r) for r in rows]


@router.get("/reactions/{event_id}/{symbol}", response_model=ReactionOut)
async def get_single_reaction(
    event_id: str,
    symbol: str,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> ReactionOut:
    row = get_reaction(db, event_id=event_id, symbol=symbol)
    if not row:
        raise HTTPException(status_code=404, detail="Reaction not found")
    return ReactionOut(**row)


@router.get("/snapshots/{event_id}/{symbol}", response_model=List[SnapshotOut])
async def get_event_snapshots(
    event_id: str,
    symbol: str,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[SnapshotOut]:
    rows = get_snapshots(db, event_id=event_id, symbol=symbol)
    return [SnapshotOut(**r) for r in rows]
