"""
News ingestion worker — registry-driven.

Polls all enabled RSS sources on per-source intervals, normalizes items,
and runs the full processing pipeline:
  insert raw → dedup → asset map → sentiment → narrative → signals

Shadow-only: never affects live execution.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import (
    insert_raw_item,
    mark_item_duplicate,
    get_cluster_by_id,
    purge_old_items,
)
from shared_lib.persistence.news_intelligence import purge_expired_signals

from app.news.news_normalizer import NormalizedNewsItem
from app.news.news_source_registry import NewsSourceRegistry
from app.news.news_provider_health import ProviderHealthService
from app.news.news_reliability_service import NewsReliabilityService
from app.news.news_deduplication_service import NewsDeduplicationService
from app.news.news_asset_mapper import NewsAssetMapper
from app.news.news_sentiment_service import NewsSentimentService
from app.news.news_narrative_classifier import NewsNarrativeClassifier
from app.news.manipulation_detector import ManipulationDetector
from app.news.market_confirmation_service import MarketConfirmationService
from app.news.news_intelligence_signal_service import NewsIntelligenceSignalService

logger = logging.getLogger(__name__)

_TICK_SECONDS = 60       # main loop heartbeat
_PURGE_EVERY  = 60       # purge every N ticks


class NewsIngestionWorker:
    def __init__(
        self,
        db: DB,
        registry: NewsSourceRegistry,
        health_svc: ProviderHealthService,
        dedup_similarity_threshold: float = 0.82,
        dedup_window_hours: int = 6,
        min_reliability_for_signal: float = 0.70,
        min_confidence_for_signal: float = 0.75,
        retention_hours: int = 168,
        enabled: bool = True,
    ) -> None:
        self._db = db
        self._registry = registry
        self._health_svc = health_svc
        self._enabled = enabled
        self._retention_hours = retention_hours
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self._reliability_svc = NewsReliabilityService(db)
        self._dedup_svc = NewsDeduplicationService(
            db,
            similarity_threshold=dedup_similarity_threshold,
            window_hours=dedup_window_hours,
        )
        self._asset_mapper = NewsAssetMapper(db)
        self._sentiment_svc = NewsSentimentService(db)
        self._narrative_clf = NewsNarrativeClassifier(db)
        self._manipulation_detector = ManipulationDetector(db)
        self._market_conf_svc = MarketConfirmationService(db)
        self._signal_svc = NewsIntelligenceSignalService(
            db,
            shadow_mode=True,
            min_reliability=min_reliability_for_signal,
            min_confidence=min_confidence_for_signal,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if not self._enabled:
            logger.info("[NewsIngestion] disabled — not starting")
            return
        rss_clients = self._registry.all_rss_clients()
        if not rss_clients:
            logger.info("[NewsIngestion] no enabled RSS sources — not starting")
            return
        self._running = True
        logger.info("[NewsIngestion] starting — %d RSS sources", len(rss_clients))
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[NewsIngestion] stopped")

    # ------------------------------------------------------------------
    # Main loop — 60s tick, per-source scheduling
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        purge_tick = 0
        while self._running:
            try:
                await asyncio.to_thread(self._poll_due_sources)
                purge_tick += 1
                if purge_tick >= _PURGE_EVERY:
                    await asyncio.to_thread(self._purge_old)
                    purge_tick = 0
            except Exception as exc:
                logger.exception("[NewsIngestion] unhandled loop error: %s", exc)
            await asyncio.sleep(_TICK_SECONDS)

    def _poll_due_sources(self) -> None:
        due = self._registry.get_due_rss_clients()
        if not due:
            return
        logger.debug("[NewsIngestion] %d sources due this tick", len(due))
        for client in due:
            self._poll_source(client)

    def _poll_source(self, client) -> None:
        now_utc = datetime.now(timezone.utc).isoformat()
        items, error = client.fetch()
        inserted = 0
        duplicates = 0

        for item in items:
            result = self._process_item(item)
            if result == "inserted":
                inserted += 1
            elif result == "duplicate":
                duplicates += 1

        status = self._health_svc.record(
            source_id=client.source_id,
            items_fetched=inserted,
            duplicate_count=duplicates,
            error_message=error,
            last_success_utc=now_utc if not error else None,
        )
        self._registry.mark_fetched(
            client.source_id, now_utc, error=error
        )
        if inserted or error:
            logger.info("[RSS:%s] status=%s inserted=%d dupes=%d error=%s",
                        client.source_id, status, inserted, duplicates, error or "—")

    # ------------------------------------------------------------------
    # Per-item processing pipeline
    # ------------------------------------------------------------------

    def _process_item(self, item: NormalizedNewsItem) -> str:
        reliability = self._reliability_svc.score(item.source_domain)

        raw_id = insert_raw_item(
            self._db,
            provider=item.provider,
            title=item.title,
            published_utc=item.published_utc,
            ingested_utc=item.ingested_utc,
            external_id=item.external_id,
            source_name=item.source_name,
            source_domain=item.source_domain,
            source_url=item.source_url,
            body_snippet=item.body_snippet,
            raw_payload_json=item.raw_payload_json,
            latency_seconds=item.latency_seconds,
            language=item.language,
        )
        if raw_id is None:
            return "duplicate"

        cluster_id, is_new_cluster = self._dedup_svc.assign_cluster(
            raw_news_item_id=raw_id,
            title=item.title,
            published_utc=item.published_utc,
            reliability_score=reliability,
            provider=item.provider,
        )

        if not is_new_cluster:
            mark_item_duplicate(self._db, raw_id)

        symbols = self._asset_mapper.map_and_store(
            cluster_id, title=item.title, body=item.body_snippet or ""
        )

        # Only run full pipeline on new clusters (avoid re-scoring)
        if not is_new_cluster:
            return "inserted"

        cluster_row = get_cluster_by_id(self._db, cluster_id) or {}

        sentiment_compound = self._sentiment_svc.score_and_store(
            cluster_id, title=item.title, body=item.body_snippet or ""
        )
        sentiment = {
            "label": "BULLISH" if sentiment_compound >= 0.05 else (
                "BEARISH" if sentiment_compound <= -0.05 else "NEUTRAL"
            ),
            "score": abs(sentiment_compound),
            "confidence": min(0.9, 0.4 + abs(sentiment_compound) * 0.8),
        }

        try:
            narratives = self._narrative_clf.classify_and_store(
                cluster_id=cluster_id,
                canonical_title=item.title,
                body_text=item.body_snippet or "",
                source_count=cluster_row.get("source_count", 1),
            )
        except Exception as exc:
            logger.exception(
                "[NewsIngestion] narrative classification failed cluster=%s raw_id=%s: %s",
                cluster_id,
                raw_id,
                exc,
            )
            narratives = []

        is_suspect, _ = self._manipulation_detector.check_and_flag(
            cluster_id=cluster_id,
            cluster_row=cluster_row,
            narratives=narratives,
            domain_reliability_map={item.source_domain: reliability},
        )

        self._signal_svc.maybe_emit_signal(
            cluster_id=cluster_id,
            cluster_row=cluster_row,
            symbols=symbols,
            sentiment=sentiment,
            narratives=narratives,
            is_manipulation_suspect=is_suspect,
        )

        top_narrative = narratives[0].get("narrative_type") if narratives else None
        try:
            self._market_conf_svc.run_due_windows(
                cluster_id=cluster_id,
                first_seen_utc=cluster_row.get("first_seen_utc", item.ingested_utc),
                sentiment_score=sentiment_compound,
                data_quality_score=min(1.0, reliability + 0.1),
                reliability_score=reliability,
                top_narrative=top_narrative,
            )
        except Exception as exc:
            logger.debug("[NewsIngestion] market conf skipped for cluster %d: %s", cluster_id, exc)

        return "inserted"

    def _purge_old(self) -> None:
        from datetime import timedelta
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=self._retention_hours)
        ).isoformat()
        n_items   = purge_old_items(self._db, cutoff)
        n_signals = purge_expired_signals(self._db)
        if n_items or n_signals:
            logger.info("[NewsIngestion] purged %d old items, %d expired signals",
                        n_items, n_signals)


# ------------------------------------------------------------------
# Factory
# ------------------------------------------------------------------

def build_news_ingestion_worker(db: DB) -> NewsIngestionWorker:
    from app.core.config import settings

    enabled = (
        settings.NEWS_INTELLIGENCE_ENABLED
        and settings.NEWS_REAL_SOURCE_INGESTION_ENABLED
    )

    registry = NewsSourceRegistry(
        db,
        rss_enabled=settings.NEWS_RSS_INGESTION_ENABLED and enabled,
        timeout=settings.NEWS_RSS_TIMEOUT_SECONDS,
        max_items_per_source=settings.NEWS_RSS_MAX_ITEMS_PER_SOURCE,
    )
    health_svc = ProviderHealthService(
        db, stale_minutes=settings.NEWS_SOURCE_STALE_MINUTES
    )

    return NewsIngestionWorker(
        db=db,
        registry=registry,
        health_svc=health_svc,
        dedup_similarity_threshold=settings.NEWS_DEDUP_SIMILARITY_THRESHOLD,
        dedup_window_hours=settings.NEWS_DEDUP_WINDOW_HOURS,
        min_reliability_for_signal=settings.NEWS_MIN_SOURCE_RELIABILITY_FOR_SIGNAL,
        min_confidence_for_signal=settings.NEWS_MIN_CONFIDENCE_FOR_SIGNAL,
        retention_hours=settings.NEWS_RETENTION_HOURS,
        enabled=enabled,
    )
