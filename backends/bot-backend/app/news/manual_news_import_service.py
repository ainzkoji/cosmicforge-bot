"""
Manual news import service.

Allows admin users to paste a headline + summary into the system.
The item enters raw_news_items and passes through the full pipeline
(dedup → clustering → sentiment → narrative → signals).
Never affects trading — shadow-only invariant is enforced downstream.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import insert_raw_item, get_cluster_by_id
from app.news.news_normalizer import normalize_manual_entry
from app.news.news_reliability_service import NewsReliabilityService
from app.news.news_deduplication_service import NewsDeduplicationService
from app.news.news_asset_mapper import NewsAssetMapper
from app.news.news_sentiment_service import NewsSentimentService
from app.news.news_narrative_classifier import NewsNarrativeClassifier
from app.news.manipulation_detector import ManipulationDetector
from app.news.market_confirmation_service import MarketConfirmationService
from app.news.news_intelligence_signal_service import NewsIntelligenceSignalService

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ManualNewsImportService:
    def __init__(self, db: DB) -> None:
        self._db = db
        self._reliability_svc = NewsReliabilityService(db)
        self._dedup_svc = NewsDeduplicationService(db)
        self._asset_mapper = NewsAssetMapper(db)
        self._sentiment_svc = NewsSentimentService(db)
        self._narrative_clf = NewsNarrativeClassifier(db)
        self._manipulation_detector = ManipulationDetector(db)
        self._market_conf_svc = MarketConfirmationService(db)
        self._signal_svc = NewsIntelligenceSignalService(db, shadow_mode=True)

    def import_item(
        self,
        title: str,
        source_name: str,
        source_url: str,
        published_utc_raw: str,
        body_snippet: str = "",
        affected_symbols: Optional[List[str]] = None,
        imported_by: str = "admin",
        category: str = "CRYPTO",
    ) -> Dict:
        """
        Process a manually-entered news item through the full pipeline.
        Returns a result dict with raw_news_item_id, cluster_id, signals emitted.
        """
        now = _now()
        item = normalize_manual_entry(
            title=title,
            source_name=source_name,
            source_url=source_url,
            published_utc_raw=published_utc_raw,
            body_snippet=body_snippet,
            category=category,
        )

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
            raw_payload_json="{}",
            language=item.language,
        )

        if raw_id is None:
            return {"error": "duplicate", "title": title}

        # Record in manual_news_imports table
        with self._db.connect() as conn:
            conn.execute(
                """INSERT INTO manual_news_imports
                   (title, body_snippet, source_name, source_url,
                    published_utc, affected_symbols, imported_by,
                    raw_news_item_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (title, body_snippet, source_name, source_url,
                 item.published_utc,
                 ",".join(affected_symbols) if affected_symbols else None,
                 imported_by, raw_id, now),
            )

        # Deduplication clustering
        cluster_id, is_new_cluster = self._dedup_svc.assign_cluster(
            raw_news_item_id=raw_id,
            title=item.title,
            published_utc=item.published_utc,
            reliability_score=reliability,
            provider=item.provider,
        )

        # Asset mapping — use provided symbols if given, else auto-detect
        if affected_symbols:
            from shared_lib.persistence.news_intelligence import upsert_asset_mapping
            for sym in affected_symbols:
                upsert_asset_mapping(
                    self._db,
                    cluster_id=cluster_id,
                    symbol=sym,
                    asset="crypto",
                    mapping_confidence=1.0,
                    mapping_reason="manual",
                )
            symbols = affected_symbols
        else:
            symbols = self._asset_mapper.map_and_store(
                cluster_id, title=item.title, body=item.body_snippet
            )

        cluster_row = get_cluster_by_id(self._db, cluster_id) or {}

        # Sentiment
        sentiment_compound = self._sentiment_svc.score_and_store(
            cluster_id, title=item.title, body=item.body_snippet
        )
        sentiment = {
            "label": "BULLISH" if sentiment_compound >= 0.05 else ("BEARISH" if sentiment_compound <= -0.05 else "NEUTRAL"),
            "score": abs(sentiment_compound),
            "confidence": min(0.9, 0.4 + abs(sentiment_compound) * 0.8),
        }

        # Narrative
        narratives = self._narrative_clf.classify_and_store(
            cluster_id=cluster_id,
            canonical_title=item.title,
            body_text=item.body_snippet,
        )

        # Manipulation check
        is_suspect, _ = self._manipulation_detector.check_and_flag(
            cluster_id=cluster_id,
            cluster_row=cluster_row,
            narratives=narratives,
            domain_reliability_map={item.source_domain: reliability},
        )

        # Shadow signal emission
        signals = self._signal_svc.maybe_emit_signal(
            cluster_id=cluster_id,
            cluster_row=cluster_row,
            symbols=symbols,
            sentiment=sentiment,
            narratives=narratives,
            is_manipulation_suspect=is_suspect,
        )

        try:
            self._market_conf_svc.run_due_windows(
                cluster_id=cluster_id,
                first_seen_utc=cluster_row.get("first_seen_utc", item.ingested_utc),
                sentiment_score=sentiment_compound,
                data_quality_score=min(1.0, reliability + 0.1),
                reliability_score=reliability,
                top_narrative=narratives[0]["narrative_type"] if narratives else None,
            )
        except Exception as exc:
            logger.debug("[ManualImport] market conf skipped for cluster %d: %s", cluster_id, exc)

        logger.info("[ManualImport] imported '%s' → raw_id=%d cluster_id=%d signals=%d",
                    title[:60], raw_id, cluster_id, len(signals))

        return {
            "raw_news_item_id": raw_id,
            "cluster_id": cluster_id,
            "is_new_cluster": is_new_cluster,
            "symbols": symbols,
            "sentiment": sentiment,
            "top_narrative": narratives[0]["narrative_type"] if narratives else None,
            "signals_emitted": len(signals),
            "is_manipulation_suspect": is_suspect,
        }

    def get_recent_imports(self, limit: int = 50) -> List[Dict]:
        with self._db.connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            rows = conn.execute(
                "SELECT * FROM manual_news_imports ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
