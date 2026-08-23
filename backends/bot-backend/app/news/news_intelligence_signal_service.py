"""
News intelligence signal service — Hardened Phase 3.

Orchestrates all quality engines and emits shadow-only signals.
Two hard invariants that can NEVER be violated:
  1. should_affect_trading = 0  (enforced here AND in persistence layer)
  2. shadow_only = 1            (enforced here AND in persistence layer)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_intelligence import (
    insert_signal,
    get_active_signals,
    update_cluster_quality,
)
from app.news.news_spam_detector import compute_spam_score, is_spam_cluster
from app.news.news_manipulation_detector import detect_manipulation
from app.news.news_latency_engine import compute_latency_score
from app.news.news_signal_validator import evaluate_signal_validity, DataQualityStatus
from app.news.news_reliability_service import NewsReliabilityService


class NewsIntelligenceSignalService:
    def __init__(
        self,
        db: DB,
        shadow_mode: bool = True,
        min_reliability: float = 0.45,
        min_confidence: float = 0.40,
        reliability_service: Optional[NewsReliabilityService] = None,
    ) -> None:
        self._db = db
        self._shadow_mode = True   # HARD LOCK — cannot be disabled
        self._min_reliability = min_reliability
        self._min_confidence = min_confidence
        self._rel_svc = reliability_service or NewsReliabilityService(db)

    def evaluate_and_emit(
        self,
        *,
        cluster_id: int,
        cluster_row: Dict,
        symbols: List[str],
        sentiment: Optional[Dict],
        narratives: List[Dict],
        # Optional enrichment for hardening engines
        titles: Optional[List[str]] = None,
        source_domains: Optional[List[str]] = None,
        source_reliabilities: Optional[List[float]] = None,
        ingestion_timestamps: Optional[List[str]] = None,
        duplicate_count: int = 0,
        total_item_count: int = 1,
    ) -> List[Dict]:
        """
        Full hardened pipeline:
          1. Spam detection
          2. Manipulation detection
          3. Latency scoring
          4. Source-quality evaluation
          5. Cluster quality update (persisted)
          6. Shadow intelligence emission (pending market validation)

        Returns list of signal dicts emitted (may be empty — even empty = good data).
        """
        now = datetime.now(timezone.utc)

        # ── Resolve source data ──────────────────────────────────────────────
        _domains = source_domains or []
        _reliabilities = source_reliabilities or [
            self._rel_svc.score(d) for d in _domains
        ]
        _titles = titles or [cluster_row.get("canonical_title", "")]
        _timestamps = ingestion_timestamps or []

        # ── 1. Spam Score ────────────────────────────────────────────────────
        spam_score = compute_spam_score(
            titles=_titles,
            source_reliabilities=_reliabilities,
            ingestion_timestamps=_timestamps,
            duplicate_count=duplicate_count,
            total_count=max(1, total_item_count),
        )

        # ── 2. Manipulation Detection ────────────────────────────────────────
        narrative_types = [n.get("narrative_type", "") for n in narratives]
        source_count = cluster_row.get("source_count", 1)
        provider_count = cluster_row.get("provider_count", 1)
        manipulation_flag = detect_manipulation(
            source_reliabilities=_reliabilities,
            source_domains=_domains,
            spam_score=spam_score,
            narrative_types=narrative_types,
            source_count=source_count,
            provider_count=provider_count,
            is_manipulation_suspect=bool(cluster_row.get("is_manipulation_suspect", 0)),
        )

        # ── 3. Latency Score ─────────────────────────────────────────────────
        first_seen = cluster_row.get("first_seen_utc", now.isoformat())
        latency_score, latency_flag = compute_latency_score(
            first_seen_utc=first_seen,
            now_utc=now,
        )

        # ── 4. Cluster Confidence ─────────────────────────────────────────────
        avg_similarity = spam_score * 0.7 if spam_score < 0.5 else 0.55
        cluster_confidence = self._rel_svc.compute_cluster_confidence(
            source_domains=_domains or [None],
            source_count=source_count,
            headline_similarity_score=avg_similarity,
        )

        reliability_score = cluster_row.get("highest_reliability_score", 0.0)
        if not reliability_score and _reliabilities:
            reliability_score = max(_reliabilities)

        # ── 5. Signal Validity ───────────────────────────────────────────────
        any_blocked = any(self._rel_svc.is_blocked(d) for d in _domains if d)
        source_quality_passed, data_quality_status = evaluate_signal_validity(
            cluster_confidence=cluster_confidence,
            reliability_score=reliability_score,
            spam_score=spam_score,
            latency_score=latency_score,
            manipulation_flag=manipulation_flag,
            latency_flag=latency_flag,
            sentiment_confidence=sentiment.get("confidence", 0.0) if sentiment else 0.0,
            is_blocked_source=any_blocked,
        )

        # ── 6. Persist quality scores back to cluster ────────────────────────
        update_cluster_quality(
            self._db,
            cluster_id,
            cluster_confidence=cluster_confidence,
            spam_score=spam_score,
            latency_score=latency_score,
            is_valid_signal=source_quality_passed,
            manipulation_flag=manipulation_flag,
            data_quality_status=data_quality_status,
            is_manipulation_suspect=manipulation_flag in (
                "POSSIBLE_MANIPULATION", "BOT_AMPLIFICATION"
            ),
            manipulation_reason=manipulation_flag,
        )

        # ── 7. Early exit: always surface in dashboard but only emit VALID ──
        if not symbols:
            return []
        if not sentiment:
            return []

        label = sentiment.get("label", "NEUTRAL")
        confidence = sentiment.get("confidence", 0.0)
        score = sentiment.get("score", 0.0)

        # Suppress NEUTRAL sentiment — keep intelligence observational only.
        if label == "NEUTRAL":
            return []

        signal_type = f"NEWS_SENTIMENT_{label}"
        top_narrative = narratives[0].get("narrative_type") if narratives else None

        suppression_reason: Optional[str] = None
        if not source_quality_passed:
            suppression_reason = f"SOURCE_QUALITY_FAILED: {data_quality_status}"
            if manipulation_flag:
                suppression_reason += f" / {manipulation_flag}"
        else:
            suppression_reason = "PENDING_MARKET_VALIDATION"

        emitted = []
        for symbol in symbols:
            signal_id = insert_signal(
                self._db,
                cluster_id=cluster_id,
                symbol=symbol,
                signal_type=signal_type,
                sentiment_label=label,
                confidence_score=confidence,
                reliability_score=reliability_score,
                narrative_type=top_narrative,
                sentiment_score=score,
                spam_score=spam_score,
                latency_score=latency_score,
                source_validation_passed=source_quality_passed,
                market_validation_passed=False,
                is_valid_signal=False,
                manipulation_flag=manipulation_flag,
                data_quality_status=data_quality_status,
                sentiment_accuracy=None,
                validation_status="PENDING_MARKET_VALIDATION",
                market_confirmation_status="PENDING_MARKET_VALIDATION",
                shadow_only=True,
                should_affect_trading=False,
                validated_at=None,
                suppression_reason=suppression_reason,
            )
            emitted.append({
                "signal_id": signal_id,
                "symbol": symbol,
                "signal_type": signal_type,
                "sentiment_label": label,
                "confidence_score": confidence,
                "reliability_score": reliability_score,
                "spam_score": spam_score,
                "latency_score": latency_score,
                "source_validation_passed": source_quality_passed,
                "market_validation_passed": False,
                "is_valid_signal": False,
                "manipulation_flag": manipulation_flag,
                "data_quality_status": data_quality_status,
                "validation_status": "PENDING_MARKET_VALIDATION",
                "shadow_only": True,
                "should_affect_trading": False,
            })

        return emitted

    # Backwards-compat alias
    def maybe_emit_signal(
        self,
        cluster_id: int,
        cluster_row: Dict,
        symbols: List[str],
        sentiment: Optional[Dict],
        narratives: List[Dict],
        is_manipulation_suspect: bool = False,
    ) -> List[Dict]:
        """Alias kept for existing callers."""
        if is_manipulation_suspect:
            cluster_row = {**cluster_row, "is_manipulation_suspect": 1}
        return self.evaluate_and_emit(
            cluster_id=cluster_id,
            cluster_row=cluster_row,
            symbols=symbols,
            sentiment=sentiment,
            narratives=narratives,
        )

    def get_active_signals_for_symbol(self, symbol: str) -> List[Dict]:
        return get_active_signals(self._db, symbol=symbol)

    def get_all_active_signals(self) -> List[Dict]:
        return get_active_signals(self._db)
