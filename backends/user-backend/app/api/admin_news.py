"""
Admin API — News Intelligence Layer (Phase 3 + Real-Time Hardening).

Endpoints:
  GET  /admin/news/feed-status          — live flow KPIs (today count, active sources)
  GET  /admin/news/sources              — all sources with health state
  GET  /admin/news/health               — latest health row per source
  GET  /admin/news/items                — recent raw_news_items
  GET  /admin/news/clusters             — recent news clusters
  GET  /admin/news/signals              — active shadow signals
  GET  /admin/news/provider-stats       — per-provider fetch stats
  POST /admin/news/import               — manual news import
  GET  /admin/news/imports              — list of manual imports
  --- Real-Time Hardening ---
  GET  /admin/news/rt-provider-status   — real_time_news_provider_status rows
  GET  /admin/news/rt-feed              — recent items from RT API providers
  GET  /admin/news/duplicate-clusters   — clusters with >1 item (dedup view)
  GET  /admin/news/conflicts            — clusters flagged conflict_flag=1
  GET  /admin/news/market-confirmations — clusters with market confirmation data
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.deps import require_admin
from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import (
    get_recent_items,
    get_recent_clusters,
    get_provider_stats,
    get_items_for_cluster,
    insert_raw_item,
    insert_cluster,
    link_item_to_cluster,
)
from shared_lib.persistence.news_intelligence import (
    get_active_signals,
    get_active_narratives,
    get_data_quality_summary,
    get_market_reactions_for_cluster,
    get_recent_validations,
    get_signal_stats,
    get_signals_for_cluster,
    get_narratives_for_cluster,
    get_validation_summary,
    upsert_asset_mapping,
    upsert_sentiment,
    upsert_narrative,
    insert_signal,
)
from shared_lib.persistence.event_news_mode import (
    get_event_news_runtime_mode,
    get_event_news_influence_summary,
    get_recent_event_news_influence_decisions,
    get_recent_event_news_mode_decisions,
)


router = APIRouter()


def _get_db() -> DB:
    return DB()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _since(hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _cluster_narratives(db: DB, cluster_id: int) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM news_narratives
            WHERE cluster_id = ?
            ORDER BY narrative_confidence DESC
            """,
            (cluster_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class FeedStatusOut(BaseModel):
    today_count: int
    active_sources: int
    failed_sources: int
    rss_enabled_sources: int = 0
    rt_enabled_providers: int = 0
    rt_healthy_providers: int = 0
    has_live_data: bool
    latest_title: Optional[str] = None
    latest_ingested_utc: Optional[str] = None
    ingestion_warning: Optional[str] = None
    shadow_only: bool = True
    signal_can_open_trades: bool = False
    signal_can_close_trades: bool = False
    signal_can_block_trades: bool = False


class SourceStatusOut(BaseModel):
    id: str
    source_name: str
    source_type: Optional[str] = None
    category: Optional[str] = None
    is_enabled: int = 1
    rss_url: Optional[str] = None
    fetch_interval_seconds: Optional[int] = None
    last_fetch_utc: Optional[str] = None
    last_success_utc: Optional[str] = None
    last_error: Optional[str] = None
    base_reliability_score: float = 0.5
    dynamic_reliability_score: float = 0.5
    is_trusted: int = 0
    is_blocked: int = 0
    health_status: Optional[str] = None
    items_fetched_last_run: Optional[int] = None
    duplicate_count_last_run: Optional[int] = None


class ManualImportIn(BaseModel):
    title: str = Field(..., min_length=5, max_length=500)
    body_snippet: str = ""
    source_name: str = "Manual"
    source_url: str = ""
    published_utc: str = ""
    affected_symbols: List[str] = []
    category: str = "CRYPTO"


class ManualImportOut(BaseModel):
    raw_news_item_id: Optional[int] = None
    cluster_id: Optional[int] = None
    is_new_cluster: bool = False
    symbols: List[str] = []
    top_narrative: Optional[str] = None
    signals_emitted: int = 0
    is_manipulation_suspect: bool = False
    error: Optional[str] = None


class RawItemOut(BaseModel):
    id: int
    provider: str
    source_name: Optional[str] = None
    source_domain: Optional[str] = None
    source_url: Optional[str] = None
    title: str
    body_snippet: Optional[str] = None
    published_utc: str
    ingested_utc: str
    latency_seconds: Optional[float] = None
    is_duplicate: int = 0
    language: str = "en"
    created_at: Optional[str] = None


class ClusterOut(BaseModel):
    id: int
    canonical_title: str
    summary: Optional[str] = None
    first_seen_utc: str
    last_seen_utc: str
    source_count: int
    provider_count: int = 0
    highest_reliability_score: float = 0.0
    is_manipulation_suspect: int = 0
    manipulation_reason: Optional[str] = None
    cluster_confidence: Optional[float] = None
    spam_score: float = 0.0
    latency_score: float = 0.0
    manipulation_flag: Optional[str] = None
    data_quality_status: Optional[str] = None
    is_valid_signal: Optional[int] = None
    narratives: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []


class SignalOut(BaseModel):
    id: int
    cluster_id: int
    symbol: str
    signal_type: str
    direction: Optional[str] = None
    confidence: Optional[float] = None
    confidence_score: Optional[float] = None
    sentiment_score: Optional[float] = None
    narrative_type: Optional[str] = None
    severity_level: Optional[str] = None
    reliability_score: Optional[float] = None
    spam_score: float = 0.0
    latency_score: float = 1.0
    is_valid_signal: int = 0
    manipulation_flag: Optional[str] = None
    data_quality_status: str = "LOW_CONFIDENCE"
    shadow_only: int = 1
    should_affect_trading: int = 0
    suppression_reason: Optional[str] = None
    created_at: str


class EventNewsRuntimeModeOut(BaseModel):
    state: Dict[str, Any]
    recent_decisions: List[Dict[str, Any]]
    next_eligible_mode: Optional[str] = None
    active_transition_limit: str
    execution_impact: bool = False
    max_allowed_action: str


class InfluenceDecisionOut(BaseModel):
    id: int
    trace_id: Optional[str] = None
    symbol: Optional[str] = None
    mode: str
    requested_action: str
    applied_action: str
    reason: Optional[str] = None
    confidence: Optional[float] = None
    reliability_score: Optional[float] = None
    fake_news_risk_score: Optional[float] = None
    conflict_flag: int = 0
    market_confirmation_status: Optional[str] = None
    size_multiplier: float = 1.0
    confidence_penalty: float = 0.0
    delay_seconds: int = 0
    expires_at: Optional[str] = None
    created_at: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/admin/news/feed-status", response_model=FeedStatusOut)
def get_feed_status(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    with db.connect() as conn:
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        today_count = int((conn.execute(
            """SELECT COUNT(*) FROM raw_news_items
               WHERE ingested_utc >= ? AND provider != 'manual'""",
            (today_start,),
        ).fetchone() or [0])[0])
        latest = conn.execute(
            """SELECT title, ingested_utc
               FROM raw_news_items
               WHERE provider != 'manual'
               ORDER BY ingested_utc DESC, id DESC LIMIT 1"""
        ).fetchone()
        health_rows = conn.execute(
            """SELECT source_id, status FROM news_provider_health nph
               WHERE nph.id = (
                   SELECT MAX(id) FROM news_provider_health
                   WHERE source_id = nph.source_id
               )"""
        ).fetchall()
        rss_enabled_sources = int((conn.execute(
            """SELECT COUNT(*) FROM news_sources
               WHERE source_type='RSS' AND is_enabled=1
                 AND rss_url IS NOT NULL AND rss_url != ''"""
        ).fetchone() or [0])[0])
        rt_rows = conn.execute(
            """SELECT provider, is_enabled, health_status
               FROM real_time_news_provider_status
               ORDER BY provider"""
        ).fetchall()
    active_sources = sum(1 for row in health_rows if row[1] in ("HEALTHY", "DEGRADED"))
    failed_sources = sum(1 for row in health_rows if row[1] == "FAILED")
    rt_enabled_providers = sum(1 for row in rt_rows if row[1])
    rt_healthy_providers = sum(1 for row in rt_rows if row[1] and row[2] == "HEALTHY")
    ingestion_warning: Optional[str] = None
    if rss_enabled_sources == 0:
        ingestion_warning = "No RSS sources are enabled. RSS is the primary live ingestion path."
    elif today_count == 0:
        ingestion_warning = "No live news has been ingested yet. RSS is primary; optional API providers may remain disabled."
    return FeedStatusOut(
        today_count=today_count,
        active_sources=active_sources,
        failed_sources=failed_sources,
        rss_enabled_sources=rss_enabled_sources,
        rt_enabled_providers=rt_enabled_providers,
        rt_healthy_providers=rt_healthy_providers,
        has_live_data=today_count > 0,
        latest_title=latest["title"] if latest else None,
        latest_ingested_utc=latest["ingested_utc"] if latest else None,
        ingestion_warning=ingestion_warning,
        shadow_only=True,
        signal_can_open_trades=False,
        signal_can_close_trades=False,
        signal_can_block_trades=False,
    )


@router.get("/admin/news/runtime-mode", response_model=EventNewsRuntimeModeOut)
def get_event_news_runtime_mode_endpoint(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> EventNewsRuntimeModeOut:
    state = get_event_news_runtime_mode(db)
    decisions = get_recent_event_news_mode_decisions(db, limit=10)
    current = str(state.get("current_mode") or "SHADOW").upper()
    next_eligible = "ADVISORY" if current == "SHADOW" else None
    if current == "ADVISORY":
        next_eligible = "RISK_LITE"
    elif current == "RISK_LITE":
        next_eligible = "RISK_GUARD_LOCKED"
    return EventNewsRuntimeModeOut(
        state=state,
        recent_decisions=decisions,
        next_eligible_mode=next_eligible,
        active_transition_limit="DISABLED<->SHADOW<->ADVISORY<->RISK_LITE",
        execution_impact=current == "RISK_LITE",
        max_allowed_action=str(state.get("max_allowed_action") or "ANNOTATE_ONLY"),
    )


@router.get("/admin/news/influence-decisions", response_model=List[InfluenceDecisionOut])
def get_influence_decisions(
    symbol: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[InfluenceDecisionOut]:
    rows = get_recent_event_news_influence_decisions(db, limit=limit, symbol=symbol)
    out: List[InfluenceDecisionOut] = []
    for row in rows:
        try:
            out.append(InfluenceDecisionOut(**{k: v for k, v in row.items() if k in InfluenceDecisionOut.model_fields}))
        except Exception:
            continue
    return out


@router.get("/admin/news/influence-summary")
def get_influence_summary(
    hours: int = Query(6, ge=1, le=168),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> Dict[str, Any]:
    summary = get_event_news_influence_summary(db, hours=hours)
    warnings: list[str] = []
    if summary.get("hard_block_count", 0) != 0:
        warnings.append("Influence engine emitted hard block; this should remain zero in RISK_LITE.")
    if summary.get("forbidden_action_count", 0) != 0:
        warnings.append("Forbidden influence action emitted; controller should demote/disable.")
    return {
        "summary": summary,
        "demotion_warnings": warnings,
        "locked_states": ["RISK_GUARD", "LIVE_SIGNAL_ELIGIBLE"],
        "news_execution_allowed": False,
    }


@router.get("/admin/news/sources", response_model=List[SourceStatusOut])
def get_sources(
    trusted_only: bool = Query(False),
    blocked_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    query = """
        SELECT ns.id, ns.source_name, ns.source_type, ns.category,
               ns.is_enabled, ns.rss_url, ns.fetch_interval_seconds,
               ns.last_fetch_utc, ns.last_success_utc, ns.last_error,
               ns.base_reliability_score, ns.dynamic_reliability_score,
               ns.is_trusted, ns.is_blocked,
               nph.status, nph.items_fetched_last_run,
               nph.duplicate_count_last_run, nph.last_checked_utc,
               nph.error_message
        FROM news_sources ns
        LEFT JOIN news_provider_health nph
          ON nph.source_id = ns.id
          AND nph.id = (
            SELECT MAX(id) FROM news_provider_health
            WHERE source_id = ns.id
          )
        WHERE 1 = 1
    """
    params: List[Any] = []
    if trusted_only:
        query += " AND ns.is_trusted = 1"
    if blocked_only:
        query += " AND ns.is_blocked = 1"
    query += " ORDER BY ns.source_type, ns.source_name LIMIT ?"
    params.append(limit)
    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    out = []
    for r in rows:
        out.append(SourceStatusOut(
            id=r["id"],
            source_name=r["source_name"],
            source_type=r.get("source_type"),
            category=r.get("category"),
            is_enabled=r.get("is_enabled", 1),
            rss_url=r.get("rss_url"),
            fetch_interval_seconds=r.get("fetch_interval_seconds"),
            last_fetch_utc=r.get("last_fetch_utc"),
            last_success_utc=r.get("last_success_utc"),
            base_reliability_score=r.get("base_reliability_score", 0.5),
            dynamic_reliability_score=r.get("dynamic_reliability_score", 0.5),
            is_trusted=r.get("is_trusted", 0),
            is_blocked=r.get("is_blocked", 0),
            health_status=r.get("status") or ("DISABLED" if r.get("is_enabled", 1) == 0 else "UNKNOWN"),
            items_fetched_last_run=r.get("items_fetched_last_run"),
            duplicate_count_last_run=r.get("duplicate_count_last_run"),
            last_error=r.get("last_error") or r.get("error_message"),
        ))
    return out


@router.get("/admin/news/health")
def get_health(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM news_provider_health nph
            WHERE nph.id = (
                SELECT MAX(id) FROM news_provider_health
                WHERE source_id = nph.source_id
            )
            ORDER BY source_id
            """
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/admin/news/items", response_model=List[RawItemOut])
def get_items(
    hours: int = Query(24, ge=1, le=168),
    provider: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    since = _since(hours)
    rows = get_recent_items(db, since_utc=since, provider=provider, limit=limit)
    return [RawItemOut(**{k: v for k, v in r.items() if k in RawItemOut.model_fields}) for r in rows]


@router.get("/admin/news/clusters", response_model=List[ClusterOut])
def get_clusters(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    since = _since(hours)
    with db.connect() as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT *
                FROM news_clusters
                WHERE first_seen_utc >= ?
                ORDER BY first_seen_utc DESC
                LIMIT ?
                """,
                (since, limit),
            ).fetchall()
        ]
    out = []
    for r in rows:
        try:
            out.append(ClusterOut(
                id=r["id"],
                canonical_title=r["canonical_title"],
                summary=r.get("summary"),
                first_seen_utc=r["first_seen_utc"],
                last_seen_utc=r["last_seen_utc"],
                source_count=r.get("source_count", 1),
                provider_count=r.get("provider_count", 0),
                highest_reliability_score=r.get("highest_reliability_score", 0.0),
                is_manipulation_suspect=r.get("is_manipulation_suspect", 0),
                manipulation_reason=r.get("manipulation_reason"),
                cluster_confidence=r.get("cluster_confidence"),
                spam_score=r.get("spam_score") or 0.0,
                latency_score=r.get("latency_score") or 0.0,
                manipulation_flag=r.get("manipulation_flag"),
                data_quality_status=r.get("data_quality_status"),
                is_valid_signal=r.get("is_valid_signal"),
                narratives=_cluster_narratives(db, r["id"]),
                signals=get_signals_for_cluster(db, r["id"]),
            ))
        except Exception:
            continue
    return out


@router.get("/admin/news/clusters/{cluster_id}/items", response_model=List[RawItemOut])
def get_cluster_items(
    cluster_id: int,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    rows = get_items_for_cluster(db, cluster_id=cluster_id)
    return [RawItemOut(**{k: v for k, v in r.items() if k in RawItemOut.model_fields}) for r in rows]


@router.get("/admin/news/clusters/{cluster_id}/validation")
def get_cluster_validation(
    cluster_id: int,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    return get_market_reactions_for_cluster(db, cluster_id)


@router.get("/admin/news/narratives")
def get_narratives(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=500),
    narrative_type: Optional[str] = Query(None),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    return get_active_narratives(
        db,
        since_utc=_since(hours),
        narrative_type=narrative_type,
        limit=limit,
    )


@router.get("/admin/news/data-quality")
def get_data_quality(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> Dict[str, Any]:
    return get_data_quality_summary(db)


@router.get("/admin/news/signals", response_model=List[SignalOut])
def get_signals(
    symbol: Optional[str] = Query(None),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    since = _since(hours)
    rows = get_active_signals(db, symbol=symbol, since_utc=since, limit=limit)
    out = []
    for r in rows:
        try:
            out.append(SignalOut(
                id=r["id"],
                cluster_id=r["cluster_id"],
                symbol=r["symbol"],
                signal_type=r.get("signal_type", "NEWS_SENTIMENT"),
                direction=r.get("direction"),
                confidence=r.get("confidence_score") or r.get("confidence"),
                confidence_score=r.get("confidence_score"),
                sentiment_score=r.get("sentiment_score"),
                narrative_type=r.get("narrative_type"),
                severity_level=r.get("severity_level"),
                reliability_score=r.get("reliability_score"),
                spam_score=r.get("spam_score") or 0.0,
                latency_score=r.get("latency_score") or 1.0,
                is_valid_signal=r.get("is_valid_signal", 0),
                manipulation_flag=r.get("manipulation_flag"),
                data_quality_status=r.get("data_quality_status") or "LOW_CONFIDENCE",
                shadow_only=r.get("shadow_only", 1),
                should_affect_trading=r.get("should_affect_trading", 0),
                suppression_reason=r.get("suppression_reason"),
                created_at=r["created_at"],
            ))
        except Exception:
            continue
    return out


@router.get("/admin/news/validations")
def get_validations(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=500),
    symbol: Optional[str] = Query(None),
    false_only: bool = Query(False),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    return get_recent_validations(
        db,
        since_utc=_since(hours),
        symbol=symbol,
        false_only=false_only,
        limit=limit,
    )


@router.get("/admin/news/validations/summary")
def get_validations_summary(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> Dict[str, Any]:
    return get_validation_summary(db)


@router.get("/admin/news/narrative-effectiveness")
def get_narrative_effectiveness(
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM narrative_effectiveness_scores
            ORDER BY avg_impact_score DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/admin/news/stats")
def get_intelligence_stats(
    hours: int = Query(24, ge=1, le=168),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> Dict[str, Any]:
    since_utc = _since(hours)
    return {
        "providers": get_provider_stats(db, since_utc=since_utc),
        "signals": get_signal_stats(db, since_utc=since_utc),
        "data_quality": get_data_quality_summary(db),
        "validation": get_validation_summary(db),
    }


@router.get("/admin/news/provider-stats")
def get_provider_stats_endpoint(
    hours: int = Query(24, ge=1, le=168),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    since = _since(hours)
    return get_provider_stats(db, since_utc=since)


@router.post("/admin/news/import", response_model=ManualImportOut)
def manual_import(
    body: ManualImportIn,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    if not body.published_utc:
        body.published_utc = _now_utc()

    raw_id = insert_raw_item(
        db,
        provider="manual",
        source_name=body.source_name,
        source_url=body.source_url,
        title=body.title,
        body_snippet=body.body_snippet,
        published_utc=body.published_utc,
        ingested_utc=_now_utc(),
        external_id=f"manual:{body.title}:{body.published_utc}",
        latency_seconds=0,
    )
    if raw_id is None:
        return ManualImportOut(error="Duplicate manual news item")

    cluster_id = insert_cluster(
        db,
        canonical_title=body.title,
        summary=body.body_snippet,
        first_seen_utc=body.published_utc,
        reliability_score=0.75,
        cluster_confidence=0.75,
    )
    link_item_to_cluster(db, cluster_id=cluster_id, raw_news_item_id=raw_id)
    symbols = body.affected_symbols or []
    for symbol in symbols:
        upsert_asset_mapping(
            db,
            cluster_id=cluster_id,
            symbol=symbol,
            asset="crypto",
            mapping_confidence=0.8,
            mapping_reason="manual_admin_import",
        )
        insert_signal(
            db,
            cluster_id=cluster_id,
            symbol=symbol,
            signal_type="NEWS_INTELLIGENCE",
            confidence_score=0.75,
            reliability_score=0.75,
            narrative_type="MANUAL_IMPORT",
            sentiment_score=0.0,
            data_quality_status="LOW_CONFIDENCE",
            source_validation_passed=True,
            market_validation_passed=False,
            suppression_reason="SHADOW_MODE_ENFORCED",
        )
    upsert_sentiment(
        db,
        cluster_id=cluster_id,
        sentiment_label="NEUTRAL",
        sentiment_score=0.0,
        confidence_score=0.5,
        model_version="manual-admin-1.0",
    )
    upsert_narrative(
        db,
        cluster_id=cluster_id,
        narrative_type="MANUAL_IMPORT",
        narrative_confidence=0.75,
        matched_keywords="manual",
    )
    return ManualImportOut(
        raw_news_item_id=raw_id,
        cluster_id=cluster_id,
        is_new_cluster=True,
        symbols=symbols,
        top_narrative="MANUAL_IMPORT",
        signals_emitted=len(symbols),
        is_manipulation_suspect=False,
    )


@router.get("/admin/news/imports")
def get_imports(
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM raw_news_items
            WHERE provider = 'manual'
            ORDER BY ingested_utc DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Real-Time Hardening endpoints
# ---------------------------------------------------------------------------

class RtProviderStatusOut(BaseModel):
    provider: str
    is_enabled: int = 0
    last_fetch_utc: Optional[str] = None
    last_success_utc: Optional[str] = None
    last_error: Optional[str] = None
    latency_avg_seconds: float = 0.0
    items_fetched_today: int = 0
    duplicate_rate: float = 0.0
    health_status: str = "UNKNOWN"
    updated_at: Optional[str] = None


class ConflictClusterOut(BaseModel):
    id: int
    canonical_title: str
    first_seen_utc: str
    source_count: int
    conflict_flag: int = 1
    fake_news_risk_score: Optional[float] = None
    market_confirmation_status: Optional[str] = None
    first_seen_provider: Optional[str] = None


class MarketConfirmationOut(BaseModel):
    id: int
    canonical_title: str
    first_seen_utc: str
    market_confirmation_status: Optional[str] = None
    fake_news_risk_score: Optional[float] = None
    confirmation_count: int = 0
    conflict_flag: int = 0


@router.get("/admin/news/rt-provider-status", response_model=List[RtProviderStatusOut])
def get_rt_provider_status(
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM real_time_news_provider_status ORDER BY provider"
        ).fetchall()
    return [RtProviderStatusOut(**dict(r)) for r in rows]


@router.get("/admin/news/rt-feed", response_model=List[RawItemOut])
def get_rt_feed(
    hours: int = Query(6, ge=1, le=72),
    provider: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    """Recent items from real-time API providers (not RSS)."""
    since = _since(hours)
    rt_providers = ("cryptopanic", "benzinga", "reuters", "generic")
    if provider:
        rt_providers = (provider,)

    rows: List[Dict[str, Any]] = []
    for prov in rt_providers:
        items = get_recent_items(db, since_utc=since, provider=prov, limit=limit)
        rows.extend(items)

    rows.sort(key=lambda r: r.get("ingested_utc", ""), reverse=True)
    rows = rows[:limit]
    return [RawItemOut(**{k: v for k, v in r.items() if k in RawItemOut.model_fields}) for r in rows]


@router.get("/admin/news/duplicate-clusters")
def get_duplicate_clusters(
    hours: int = Query(24, ge=1, le=168),
    min_sources: int = Query(2, ge=2, le=50),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> List[Dict[str, Any]]:
    """Clusters confirmed by multiple sources — shows cross-provider deduplication."""
    since = _since(hours)
    with db.connect() as conn:
        rows = conn.execute(
            """SELECT id, canonical_title, first_seen_utc, last_seen_utc,
                      source_count, confirmation_count,
                      first_seen_provider, conflict_flag,
                      fake_news_risk_score, market_confirmation_status
               FROM news_clusters
               WHERE first_seen_utc >= ?
                 AND source_count >= ?
               ORDER BY source_count DESC, first_seen_utc DESC
               LIMIT ?""",
            (since, min_sources, limit),
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/admin/news/conflicts", response_model=List[ConflictClusterOut])
def get_conflict_clusters(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    """Clusters flagged for conflicting reports."""
    since = _since(hours)
    with db.connect() as conn:
        rows = conn.execute(
            """SELECT id, canonical_title, first_seen_utc, source_count,
                      conflict_flag, fake_news_risk_score,
                      market_confirmation_status, first_seen_provider
               FROM news_clusters
               WHERE first_seen_utc >= ?
                 AND conflict_flag = 1
               ORDER BY first_seen_utc DESC
               LIMIT ?""",
            (since, limit),
        ).fetchall()
    out = []
    for r in rows:
        try:
            out.append(ConflictClusterOut(**dict(r)))
        except Exception:
            continue
    return out


@router.get("/admin/news/market-confirmations", response_model=List[MarketConfirmationOut])
def get_market_confirmations(
    hours: int = Query(24, ge=1, le=168),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    """Clusters with non-null market confirmation status."""
    since = _since(hours)
    where_status = "AND market_confirmation_status = ?" if status else ""
    params: list = [since]
    if status:
        params.append(status)
    params.append(limit)

    with db.connect() as conn:
        rows = conn.execute(
            f"""SELECT id, canonical_title, first_seen_utc,
                       market_confirmation_status, fake_news_risk_score,
                       confirmation_count, conflict_flag
                FROM news_clusters
                WHERE first_seen_utc >= ?
                  AND market_confirmation_status IS NOT NULL
                  {where_status}
                ORDER BY first_seen_utc DESC
                LIMIT ?""",
            params,
        ).fetchall()
    out = []
    for r in rows:
        try:
            out.append(MarketConfirmationOut(**dict(r)))
        except Exception:
            continue
    return out
