from fastapi import APIRouter, Depends, Query
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

from app.core.auth import get_current_active_user
from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import (
    get_recent_items, get_recent_clusters, get_provider_stats, get_items_for_cluster,
)
from shared_lib.persistence.news_intelligence import (
    get_active_narratives, get_active_signals, get_signal_stats,
    get_data_quality_summary,
    get_narratives_for_cluster, get_signals_for_cluster,
    get_market_reactions_for_cluster, get_recent_validations, get_validation_summary,
)
from app.news.news_narrative_tracker import get_all_narrative_effectiveness
from app.core.config import settings

router = APIRouter()


def get_db_path() -> str | None:
    url = settings.DATABASE_URL
    if url and url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "")
    return None


def get_db() -> DB:
    return DB(path=get_db_path())


def _default_since(hours: int = 24) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


# ── Raw feed ─────────────────────────────────────────────────────────────────

@router.get("/live", response_model=List[Dict[str, Any]])
def get_live_news(
    since_hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=1000),
    provider: Optional[str] = None,
    current_user: dict = Depends(get_current_active_user),
):
    db = get_db()
    return get_recent_items(db, since_utc=_default_since(since_hours), provider=provider, limit=limit)


# ── Clusters ─────────────────────────────────────────────────────────────────

@router.get("/clusters", response_model=List[Dict[str, Any]])
def get_news_clusters(
    since_hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=1000),
    include_manipulation: bool = Query(True),
    valid_only: bool = Query(False),
    data_quality_status: Optional[str] = None,
    current_user: dict = Depends(get_current_active_user),
):
    db = get_db()
    clusters = get_recent_clusters(
        db, since_utc=_default_since(since_hours), limit=limit,
        include_manipulation=include_manipulation,
    )
    if valid_only:
        clusters = [c for c in clusters if c.get("is_valid_signal")]
    if data_quality_status:
        clusters = [c for c in clusters if c.get("data_quality_status") == data_quality_status]
    for c in clusters:
        c["narratives"] = get_narratives_for_cluster(db, c["id"])
        c["signals"]    = get_signals_for_cluster(db, c["id"])
    return clusters


@router.get("/clusters/{cluster_id}/items", response_model=List[Dict[str, Any]])
def get_cluster_raw_items(
    cluster_id: int,
    current_user: dict = Depends(get_current_active_user),
):
    return get_items_for_cluster(get_db(), cluster_id=cluster_id)


@router.get("/clusters/{cluster_id}/validation", response_model=List[Dict[str, Any]])
def get_cluster_validation(
    cluster_id: int,
    current_user: dict = Depends(get_current_active_user),
):
    """All news_market_reactions rows for a specific cluster."""
    return get_market_reactions_for_cluster(get_db(), cluster_id)


# ── Narratives ───────────────────────────────────────────────────────────────

@router.get("/narratives", response_model=List[Dict[str, Any]])
def get_narratives(
    since_hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=500),
    narrative_type: Optional[str] = None,
    current_user: dict = Depends(get_current_active_user),
):
    db = get_db()
    return get_active_narratives(db, since_utc=_default_since(since_hours),
                                 narrative_type=narrative_type, limit=limit)


# ── Signals ──────────────────────────────────────────────────────────────────

@router.get("/signals", response_model=List[Dict[str, Any]])
def get_shadow_signals(
    since_hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=500),
    symbol: Optional[str] = None,
    valid_only: bool = Query(False),
    current_user: dict = Depends(get_current_active_user),
):
    db = get_db()
    signals = get_active_signals(db, symbol=symbol, since_utc=_default_since(since_hours), limit=limit)
    return [s for s in signals if s.get("is_valid_signal")] if valid_only else signals


# ── Sources ──────────────────────────────────────────────────────────────────

@router.get("/sources", response_model=List[Dict[str, Any]])
def get_source_trust(
    trusted_only: bool = Query(False),
    blocked_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    current_user: dict = Depends(get_current_active_user),
):
    from app.news.news_reliability_service import NewsReliabilityService
    return NewsReliabilityService(get_db()).list_sources(
        trusted_only=trusted_only, blocked_only=blocked_only, limit=limit)


# ── Data Quality ─────────────────────────────────────────────────────────────

@router.get("/data-quality", response_model=Dict[str, Any])
def get_data_quality(current_user: dict = Depends(get_current_active_user)):
    return get_data_quality_summary(get_db())


# ── Market Validation ────────────────────────────────────────────────────────

@router.get("/validations", response_model=List[Dict[str, Any]])
def get_validations(
    since_hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=500),
    symbol: Optional[str] = None,
    false_only: bool = Query(False),
    current_user: dict = Depends(get_current_active_user),
):
    """
    Validated news→market linkages joined with cluster headline + quality data.
    Shadow only — should_affect_trading is always 0.
    """
    return get_recent_validations(
        get_db(), since_utc=_default_since(since_hours),
        symbol=symbol, false_only=false_only, limit=limit,
    )


@router.get("/validations/summary", response_model=Dict[str, Any])
def get_validations_summary(current_user: dict = Depends(get_current_active_user)):
    """Accuracy %, false-signal %, impact distribution."""
    return get_validation_summary(get_db())


@router.get("/narrative-effectiveness", response_model=List[Dict[str, Any]])
def get_narrative_effectiveness(
    limit: int = Query(50, ge=1, le=200),
    current_user: dict = Depends(get_current_active_user),
):
    """Ranked narrative types by average market impact (EMA rolling)."""
    return get_all_narrative_effectiveness(get_db(), limit=limit)


# ── Stats ─────────────────────────────────────────────────────────────────────

@router.get("/stats", response_model=Dict[str, Any])
def get_intelligence_stats(
    since_hours: int = Query(24, ge=1, le=168),
    current_user: dict = Depends(get_current_active_user),
):
    db = get_db()
    since_utc = _default_since(since_hours)
    return {
        "providers":    get_provider_stats(db, since_utc),
        "signals":      get_signal_stats(db, since_utc),
        "data_quality": get_data_quality_summary(db),
        "validation":   get_validation_summary(db),
    }
