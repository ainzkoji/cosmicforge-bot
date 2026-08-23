"""
Real-Time News Ingestion Worker.

Polls CryptoPanic, Benzinga, Reuters (placeholder), and Generic API
on a 30-second tick with per-provider scheduling. Runs the full
hardening pipeline: dedup → conflict detection → fake news risk →
market confirmation → shadow signal.

Shadow-only invariants (NEVER violates):
  - should_affect_trading = 0
  - shadow_only = 1
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import (
    insert_raw_item,
    mark_item_duplicate,
    get_cluster_by_id,
)
from shared_lib.persistence.news_intelligence import purge_expired_signals

from app.news.news_normalizer import NormalizedNewsItem
from app.news.news_reliability_service import NewsReliabilityService
from app.news.news_deduplication_service import NewsDeduplicationService
from app.news.news_asset_mapper import NewsAssetMapper
from app.news.news_sentiment_service import NewsSentimentService
from app.news.news_narrative_classifier import NewsNarrativeClassifier
from app.news.manipulation_detector import ManipulationDetector
from app.news.news_conflict_detector import NewsConflictDetector
from app.news.fake_news_risk_service import FakeNewsRiskService
from app.news.market_confirmation_service import MarketConfirmationService
from app.news.news_latency_engine import compute_latency_score
from app.news.news_spam_detector import compute_spam_score
from app.news.news_intelligence_signal_service import NewsIntelligenceSignalService

logger = logging.getLogger(__name__)

_TICK_SECONDS = 30
_PURGE_EVERY  = 120   # purge every N ticks (~60 min)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_provider_status(
    db: DB,
    provider: str,
    *,
    is_enabled: bool,
    items_fetched: int,
    duplicates: int,
    error: Optional[str],
    latency: float,
    health: str,
) -> None:
    now = _now()
    total_seen = max(items_fetched + duplicates, 1)
    duplicate_rate = round(duplicates / total_seen, 4)
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO real_time_news_provider_status
               (provider, is_enabled, last_fetch_utc, last_success_utc, last_error,
                latency_avg_seconds, items_fetched_today, duplicate_rate,
                health_status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(provider) DO UPDATE SET
                 is_enabled=excluded.is_enabled,
                 last_fetch_utc=excluded.last_fetch_utc,
                 last_success_utc=CASE WHEN excluded.last_error IS NULL
                                       THEN excluded.last_fetch_utc
                                       ELSE last_success_utc END,
                 last_error=excluded.last_error,
                 latency_avg_seconds=ROUND(
                     (latency_avg_seconds * 0.8 + excluded.latency_avg_seconds * 0.2), 4),
                 items_fetched_today=items_fetched_today + excluded.items_fetched_today,
                 duplicate_rate=excluded.duplicate_rate,
                 health_status=excluded.health_status,
                 updated_at=excluded.updated_at""",
            (
                provider, int(is_enabled), now,
                now if not error else None, error,
                latency, items_fetched, duplicate_rate, health, now, now,
            ),
        )


def _seed_provider_status(
    db: DB,
    provider: str,
    *,
    is_enabled: bool,
    health: str,
    error: Optional[str],
) -> None:
    now = _now()
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO real_time_news_provider_status
               (provider, is_enabled, last_fetch_utc, last_success_utc, last_error,
                latency_avg_seconds, items_fetched_today, duplicate_rate,
                health_status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(provider) DO UPDATE SET
                 is_enabled=excluded.is_enabled,
                 last_error=excluded.last_error,
                 health_status=excluded.health_status,
                 updated_at=excluded.updated_at""",
            (
                provider,
                int(is_enabled),
                None,
                None,
                error,
                0.0,
                0,
                0.0,
                health,
                now,
                now,
            ),
        )


class RealTimeNewsWorker:
    """
    Polls real-time API providers and runs the full hardening pipeline.
    All outputs are shadow-only.
    """

    def __init__(
        self,
        db: DB,
        *,
        poll_interval_seconds: int = 30,
        dedup_similarity_threshold: float = 0.82,
        dedup_window_hours: int = 6,
        min_reliability_for_signal: float = 0.70,
        min_confidence_for_signal: float = 0.75,
        enabled: bool = True,
    ) -> None:
        self._db = db
        self._poll_interval = poll_interval_seconds
        self._enabled = enabled
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._purge_tick = 0

        self._providers: List = []  # populated in _init_providers()

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
        self._conflict_detector = NewsConflictDetector(db)
        self._fake_risk_svc = FakeNewsRiskService(db)
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

    def add_provider(self, client) -> None:
        """Register a provider client (CryptoPanic, Benzinga, etc.)."""
        self._providers.append(client)

    async def start(self) -> None:
        if not self._enabled:
            logger.info("[RT-News] disabled — not starting")
            return
        if not self._providers:
            logger.info("[RT-News] no providers configured — not starting")
            return
        enabled_count = sum(1 for p in self._providers if p.is_enabled())
        if enabled_count == 0:
            logger.info("[RT-News] no enabled real-time providers — baseline status rows only")
            return
        self._running = True
        logger.info("[RT-News] starting — %d providers (%d enabled)",
                    len(self._providers), enabled_count)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[RT-News] stopped")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.to_thread(self._poll_all_providers)
                self._purge_tick += 1
                if self._purge_tick >= _PURGE_EVERY:
                    await asyncio.to_thread(self._purge_old)
                    self._purge_tick = 0
            except Exception as exc:
                logger.exception("[RT-News] unhandled loop error: %s", exc)
            await asyncio.sleep(_TICK_SECONDS)

    def _poll_all_providers(self) -> None:
        for provider in self._providers:
            try:
                self._poll_provider(provider)
            except Exception as exc:
                logger.exception("[RT-News] poll crash for %s: %s", provider.provider, exc)

    def _poll_provider(self, provider) -> None:
        if not provider.is_enabled():
            return
        if not provider.is_due(self._poll_interval):
            return

        items, error, latency = provider.fetch()
        health = "HEALTHY" if not error and items else (
            "FAILED" if error else "DEGRADED"
        )

        inserted = 0
        duplicates = 0
        for item in items:
            result = self._process_item(item)
            if result == "inserted":
                inserted += 1
            elif result == "duplicate":
                duplicates += 1

        try:
            _update_provider_status(
                self._db,
                provider.provider,
                is_enabled=True,
                items_fetched=inserted,
                duplicates=duplicates,
                error=error,
                latency=latency,
                health=health,
            )
        except Exception as exc:
            logger.warning("[RT-News] status update failed for %s: %s", provider.provider, exc)

        logger.info(
            "[RT-News:%s] inserted=%d dupes=%d latency=%.2fs error=%s",
            provider.provider, inserted, duplicates, latency, error or "—",
        )

    # ------------------------------------------------------------------
    # Per-item hardening pipeline
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

        if not is_new_cluster:
            return "inserted"

        cluster_row = get_cluster_by_id(self._db, cluster_id) or {}
        first_seen_utc = cluster_row.get("first_seen_utc", item.ingested_utc)

        # Sentiment
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

        # Narrative
        narratives = self._narrative_clf.classify_and_store(
            cluster_id=cluster_id,
            canonical_title=item.title,
            body_text=item.body_snippet or "",
            source_count=cluster_row.get("source_count", 1),
        )

        # Manipulation
        is_suspect, _ = self._manipulation_detector.check_and_flag(
            cluster_id=cluster_id,
            cluster_row=cluster_row,
            narratives=narratives,
            domain_reliability_map={item.source_domain: reliability},
        )

        # Conflict detection (intra-cluster)
        conflict_flagged = self._conflict_detector.check_cluster(
            cluster_id=cluster_id,
            item_titles=[item.title],
            compound_scores=[sentiment_compound],
        )

        # Cross-cluster conflict
        since_utc = (
            datetime.now(timezone.utc) - timedelta(hours=6)
        ).isoformat()
        if not conflict_flagged:
            conflict_flagged = self._conflict_detector.check_cross_cluster(
                new_cluster_id=cluster_id,
                new_title=item.title,
                new_dominant_sentiment=sentiment_compound,
                since_utc=since_utc,
            )

        # Latency score
        latency_score, _ = compute_latency_score(first_seen_utc)

        # Spam score (single item — minimal info at this point)
        spam_score = compute_spam_score(
            titles=[item.title],
            source_reliabilities=[reliability],
            duplicate_count=0,
            total_count=1,
        )

        # Fake news risk
        self._fake_risk_svc.score(
            cluster_id=cluster_id,
            source_reliabilities=[reliability],
            spam_score=spam_score,
            latency_score=latency_score,
            conflict_flag=conflict_flagged,
            source_count=cluster_row.get("source_count", 1),
        )

        # Shadow intelligence record (always pending until market validation updates it).
        self._signal_svc.maybe_emit_signal(
            cluster_id=cluster_id,
            cluster_row=cluster_row,
            symbols=symbols,
            sentiment=sentiment,
            narratives=narratives,
            is_manipulation_suspect=is_suspect,
        )

        # Market confirmation (async-safe: runs quick check, longer windows
        # will produce status when actually due)
        top_narrative = narratives[0].get("narrative_type") if narratives else None
        try:
            self._market_conf_svc.run_due_windows(
                cluster_id=cluster_id,
                first_seen_utc=first_seen_utc,
                sentiment_score=sentiment_compound,
                data_quality_score=min(1.0, reliability + 0.1),
                reliability_score=reliability,
                top_narrative=top_narrative,
            )
        except Exception as exc:
            logger.debug("[RT-News] market conf skipped for cluster %d: %s", cluster_id, exc)

        return "inserted"

    def _purge_old(self) -> None:
        try:
            n_signals = purge_expired_signals(self._db)
            if n_signals:
                logger.info("[RT-News] purged %d expired signals", n_signals)
        except Exception as exc:
            logger.warning("[RT-News] purge error: %s", exc)


# ------------------------------------------------------------------
# Factory
# ------------------------------------------------------------------

def build_real_time_news_worker(db: DB) -> RealTimeNewsWorker:
    from app.core.config import settings

    enabled = (
        getattr(settings, "REAL_TIME_NEWS_ENABLED", False)
        and getattr(settings, "NEWS_INTELLIGENCE_ENABLED", False)
    )

    worker = RealTimeNewsWorker(
        db=db,
        poll_interval_seconds=getattr(settings, "REAL_TIME_NEWS_POLL_INTERVAL_SECONDS", 30),
        dedup_similarity_threshold=getattr(settings, "NEWS_DEDUP_SIMILARITY_THRESHOLD", 0.82),
        dedup_window_hours=getattr(settings, "NEWS_DEDUP_WINDOW_HOURS", 6),
        min_reliability_for_signal=getattr(settings, "REAL_TIME_NEWS_MIN_RELIABILITY_SCORE", 0.70),
        min_confidence_for_signal=getattr(settings, "REAL_TIME_NEWS_MIN_CLUSTER_CONFIDENCE", 0.75),
        enabled=enabled,
    )

    cp_enabled = bool(getattr(settings, "CRYPTOPANIC_ENABLED", False))
    cp_key = getattr(settings, "CRYPTOPANIC_API_KEY", "")
    bz_enabled = bool(getattr(settings, "BENZINGA_ENABLED", False))
    bz_key = getattr(settings, "BENZINGA_API_KEY", "")
    generic_enabled = bool(getattr(settings, "GENERIC_NEWS_API_ENABLED", False))
    generic_key = getattr(settings, "GENERIC_NEWS_API_KEY", "")
    generic_url = getattr(settings, "GENERIC_NEWS_API_URL", "")

    _seed_provider_status(
        db,
        "cryptopanic",
        is_enabled=enabled and cp_enabled and bool(cp_key),
        health="WAITING_CONFIG" if enabled and cp_enabled and not cp_key else ("DISABLED" if not cp_enabled or not enabled else "UNKNOWN"),
        error="API key not configured (optional)" if enabled and cp_enabled and not cp_key else ("Provider disabled" if not cp_enabled else None),
    )
    _seed_provider_status(
        db,
        "benzinga",
        is_enabled=enabled and bz_enabled and bool(bz_key),
        health="WAITING_CONFIG" if enabled and bz_enabled and not bz_key else ("DISABLED" if not bz_enabled or not enabled else "UNKNOWN"),
        error="API key not configured (optional)" if enabled and bz_enabled and not bz_key else ("Provider disabled" if not bz_enabled else None),
    )
    _seed_provider_status(
        db,
        "generic",
        is_enabled=enabled and generic_enabled and bool(generic_key and generic_url),
        health="WAITING_CONFIG" if enabled and generic_enabled and not (generic_key and generic_url) else ("DISABLED" if not generic_enabled or not enabled else "UNKNOWN"),
        error="API key or URL not configured (optional)" if enabled and generic_enabled and not (generic_key and generic_url) else ("Provider disabled" if not generic_enabled else None),
    )
    _seed_provider_status(
        db,
        "reuters",
        is_enabled=False,
        health="PLACEHOLDER",
        error="Provider placeholder only; no live ingestion implemented",
    )

    if not enabled:
        return worker

    timeout = getattr(settings, "REAL_TIME_NEWS_PROVIDER_TIMEOUT_SECONDS", 10)
    max_items = getattr(settings, "REAL_TIME_NEWS_MAX_ITEMS_PER_FETCH", 100)

    # CryptoPanic
    if cp_enabled:
        from app.news.providers.cryptopanic_client import CryptoPanicRealtimeClient
        if cp_key:
            worker.add_provider(CryptoPanicRealtimeClient(
                api_key=cp_key,
                timeout=timeout,
                max_items=max_items,
            ))

    # Benzinga
    if bz_enabled:
        from app.news.providers.benzinga_client import BenzingaRealtimeClient
        if bz_key:
            worker.add_provider(BenzingaRealtimeClient(
                api_key=bz_key,
                timeout=timeout,
                max_items=max_items,
            ))

    # Reuters (hard-disabled placeholder — always added but never fires)
    from app.news.providers.reuters_client import ReutersClient
    worker.add_provider(ReutersClient(
        api_key=getattr(settings, "REUTERS_API_KEY", ""),
        timeout=timeout,
        max_items=max_items,
    ))

    # Generic
    if generic_enabled:
        from app.news.providers.generic_news_api_client import GenericNewsApiClient
        if generic_key and generic_url:
            worker.add_provider(GenericNewsApiClient(
                api_key=generic_key,
                api_url=generic_url,
                timeout=timeout,
                max_items=max_items,
            ))

    return worker
