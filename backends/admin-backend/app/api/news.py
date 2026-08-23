from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.news_repo import (
    get_active_narratives,
    get_active_signals,
    get_cluster_items,
    get_cluster_validation,
    get_clusters,
    get_conflict_clusters,
    get_data_quality_summary,
    get_duplicate_clusters,
    get_health,
    get_influence_decisions,
    get_influence_summary,
    get_manual_imports,
    get_market_confirmations,
    get_narrative_effectiveness,
    get_news_feed_status,
    get_provider_stats,
    get_recent_items,
    get_recent_validations,
    get_rt_feed,
    get_rt_provider_status,
    get_runtime_mode,
    get_recent_mode_decisions,
    get_signal_stats,
    get_sources,
    get_validation_summary,
)


router = APIRouter(prefix="/api/admin/news", tags=["admin-news"])
logger = logging.getLogger(__name__)


def _log_timing(endpoint: str, started_at: float, *, row_count: int | None = None) -> None:
    logger.info(
        "admin_news_endpoint endpoint=%s duration_ms=%s row_count=%s",
        endpoint,
        round((time.perf_counter() - started_at) * 1000, 2),
        row_count,
    )


def _since_hours(hours: int) -> str:
    from datetime import datetime, timezone, timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


@router.get("/feed-status")
def get_feed_status_endpoint(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    result = get_news_feed_status(db)
    _log_timing("GET /api/admin/news/feed-status", started_at)
    return result


@router.get("/runtime-mode")
def get_runtime_mode_endpoint(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    state = get_runtime_mode(db)
    decisions = get_recent_mode_decisions(db, limit=10)
    current = str(state.get("current_mode") or "SHADOW").upper()
    next_eligible: str | None = "ADVISORY" if current == "SHADOW" else None
    if current == "ADVISORY":
        next_eligible = "RISK_LITE"
    elif current == "RISK_LITE":
        next_eligible = "RISK_GUARD_LOCKED"
    result = {
        "state": state,
        "recent_decisions": decisions,
        "next_eligible_mode": next_eligible,
        "active_transition_limit": "DISABLED<->SHADOW<->ADVISORY<->RISK_LITE",
        "execution_impact": current == "RISK_LITE",
        "max_allowed_action": str(state.get("max_allowed_action") or "ANNOTATE_ONLY"),
    }
    _log_timing("GET /api/admin/news/runtime-mode", started_at)
    return result


@router.get("/influence-decisions")
def get_influence_decisions_endpoint(
    symbol: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_influence_decisions(db, symbol=symbol, limit=limit)
    _log_timing("GET /api/admin/news/influence-decisions", started_at, row_count=len(rows))
    return rows


@router.get("/influence-summary")
def get_influence_summary_endpoint(
    hours: int = Query(6, ge=1, le=168),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    result = get_influence_summary(db, hours=hours)
    _log_timing("GET /api/admin/news/influence-summary", started_at)
    return result


@router.get("/sources")
def get_sources_endpoint(
    trusted_only: bool = Query(False),
    blocked_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_sources(db, trusted_only=trusted_only, blocked_only=blocked_only, limit=limit)
    _log_timing("GET /api/admin/news/sources", started_at, row_count=len(rows))
    return rows


@router.get("/health")
def get_health_endpoint(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_health(db)
    _log_timing("GET /api/admin/news/health", started_at, row_count=len(rows))
    return rows


@router.get("/items")
def get_items_endpoint(
    hours: int = Query(24, ge=1, le=168),
    provider: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_recent_items(db, since_utc=_since_hours(hours), provider=provider, limit=limit)
    _log_timing("GET /api/admin/news/items", started_at, row_count=len(rows))
    return rows


@router.get("/clusters")
def get_clusters_endpoint(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_clusters(db, since_utc=_since_hours(hours), limit=limit)
    _log_timing("GET /api/admin/news/clusters", started_at, row_count=len(rows))
    return rows


@router.get("/clusters/{cluster_id}/items")
def get_cluster_items_endpoint(
    cluster_id: int,
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_cluster_items(db, cluster_id=cluster_id)
    _log_timing(f"GET /api/admin/news/clusters/{cluster_id}/items", started_at, row_count=len(rows))
    return rows


@router.get("/clusters/{cluster_id}/validation")
def get_cluster_validation_endpoint(
    cluster_id: int,
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_cluster_validation(db, cluster_id=cluster_id)
    _log_timing(f"GET /api/admin/news/clusters/{cluster_id}/validation", started_at, row_count=len(rows))
    return rows


@router.get("/narratives")
def get_narratives_endpoint(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=500),
    narrative_type: str | None = Query(None),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_active_narratives(
        db, since_utc=_since_hours(hours), narrative_type=narrative_type, limit=limit
    )
    _log_timing("GET /api/admin/news/narratives", started_at, row_count=len(rows))
    return rows


@router.get("/data-quality")
def get_data_quality_endpoint(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    result = get_data_quality_summary(db)
    _log_timing("GET /api/admin/news/data-quality", started_at)
    return result


@router.get("/signals")
def get_signals_endpoint(
    symbol: str | None = Query(None),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_active_signals(
        db, symbol=symbol, since_utc=_since_hours(hours), limit=limit
    )
    _log_timing("GET /api/admin/news/signals", started_at, row_count=len(rows))
    return rows


@router.get("/validations/summary")
def get_validations_summary_endpoint(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    result = get_validation_summary(db)
    _log_timing("GET /api/admin/news/validations/summary", started_at)
    return result


@router.get("/validations")
def get_validations_endpoint(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=500),
    symbol: str | None = Query(None),
    false_only: bool = Query(False),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_recent_validations(
        db,
        since_utc=_since_hours(hours),
        symbol=symbol,
        false_only=false_only,
        limit=limit,
    )
    _log_timing("GET /api/admin/news/validations", started_at, row_count=len(rows))
    return rows


@router.get("/narrative-effectiveness")
def get_narrative_effectiveness_endpoint(
    limit: int = Query(50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_narrative_effectiveness(db, limit=limit)
    _log_timing("GET /api/admin/news/narrative-effectiveness", started_at, row_count=len(rows))
    return rows


@router.get("/stats")
def get_stats_endpoint(
    hours: int = Query(24, ge=1, le=168),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    since = _since_hours(hours)
    result = {
        "providers": get_provider_stats(db, since_utc=since),
        "signals": get_signal_stats(db, since_utc=since),
        "data_quality": get_data_quality_summary(db),
        "validation": get_validation_summary(db),
    }
    _log_timing("GET /api/admin/news/stats", started_at)
    return result


@router.get("/provider-stats")
def get_provider_stats_endpoint(
    hours: int = Query(24, ge=1, le=168),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_provider_stats(db, since_utc=_since_hours(hours))
    _log_timing("GET /api/admin/news/provider-stats", started_at, row_count=len(rows))
    return rows


@router.get("/imports")
def get_imports_endpoint(
    limit: int = Query(50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_manual_imports(db, limit=limit)
    _log_timing("GET /api/admin/news/imports", started_at, row_count=len(rows))
    return rows


@router.get("/rt-provider-status")
def get_rt_provider_status_endpoint(
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_rt_provider_status(db)
    _log_timing("GET /api/admin/news/rt-provider-status", started_at, row_count=len(rows))
    return rows


@router.get("/rt-feed")
def get_rt_feed_endpoint(
    hours: int = Query(6, ge=1, le=72),
    provider: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_rt_feed(db, since_utc=_since_hours(hours), provider=provider, limit=limit)
    _log_timing("GET /api/admin/news/rt-feed", started_at, row_count=len(rows))
    return rows


@router.get("/duplicate-clusters")
def get_duplicate_clusters_endpoint(
    hours: int = Query(24, ge=1, le=168),
    min_sources: int = Query(2, ge=2, le=50),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_duplicate_clusters(
        db, since_utc=_since_hours(hours), min_sources=min_sources, limit=limit
    )
    _log_timing("GET /api/admin/news/duplicate-clusters", started_at, row_count=len(rows))
    return rows


@router.get("/conflicts")
def get_conflict_clusters_endpoint(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_conflict_clusters(db, since_utc=_since_hours(hours), limit=limit)
    _log_timing("GET /api/admin/news/conflicts", started_at, row_count=len(rows))
    return rows


@router.get("/market-confirmations")
def get_market_confirmations_endpoint(
    hours: int = Query(24, ge=1, le=168),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> list[dict[str, Any]]:
    del _admin
    started_at = time.perf_counter()
    rows = get_market_confirmations(db, since_utc=_since_hours(hours), status=status, limit=limit)
    _log_timing("GET /api/admin/news/market-confirmations", started_at, row_count=len(rows))
    return rows
