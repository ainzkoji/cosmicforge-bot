"""
Market Confirmation Service.

Wraps NewsMarketValidationService with 5-window structured scheduling:
  POST_1, POST_5, POST_15, POST_30, POST_60 (minutes after first_seen_utc).

Output statuses (stored in news_clusters.market_confirmation_status):
  MARKET_CONFIRMED          — price moved in direction consistent with sentiment
  NO_MARKET_REACTION        — no significant price movement in any window
  DELAYED_REACTION          — reaction only appeared in the 30/60-min windows
  CONFLICTING_MARKET_REACTION — reactions across windows contradict each other

Shadow-only: observation + measurement only, never opens/closes/blocks trades.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB
from app.news.news_market_validation_service import NewsMarketValidationService

logger = logging.getLogger(__name__)

# Minutes after first_seen_utc to attempt each confirmation window
_WINDOWS = [1, 5, 15, 30, 60]

_STATUS_CONFIRMED   = "MARKET_CONFIRMED"
_STATUS_NO_REACTION = "NO_MARKET_REACTION"
_STATUS_DELAYED     = "DELAYED_REACTION"
_STATUS_CONFLICT    = "CONFLICTING_MARKET_REACTION"
_STATUS_PENDING     = "PENDING_MARKET_VALIDATION"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(ts: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _set_confirmation_status(db: DB, cluster_id: int, status: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute(
            """UPDATE news_clusters
               SET market_confirmation_status=?,
                   updated_at=?
               WHERE id=? AND (
                   market_confirmation_status IS NULL
                   OR market_confirmation_status NOT IN (?,?,?,?)
               )""",
            (status, now, cluster_id,
             _STATUS_CONFIRMED, _STATUS_NO_REACTION, _STATUS_DELAYED, _STATUS_CONFLICT),
        )


class MarketConfirmationService:
    """
    Runs market validation across the 5 time windows for a cluster.

    Designed to be called once per cluster after ingestion; internally
    checks which windows are now due and runs them.
    """

    def __init__(
        self,
        db: DB,
        *,
        min_impact_threshold: float = 0.02,
        min_confidence_threshold: float = 0.70,
    ) -> None:
        self._db = db
        self._validator = NewsMarketValidationService(
            db,
            min_impact_threshold=min_impact_threshold,
            min_confidence_threshold=min_confidence_threshold,
            link_window_before_min=5,
            link_window_after_min=90,
        )

    def run_due_windows(
        self,
        cluster_id: int,
        first_seen_utc: str,
        sentiment_score: Optional[float] = None,
        data_quality_score: float = 0.5,
        reliability_score: float = 0.5,
        top_narrative: Optional[str] = None,
    ) -> str:
        """
        Check which confirmation windows are now due and run them.

        Returns the derived market_confirmation_status string.
        Persists the status to news_clusters.
        """
        first_seen = _parse_utc(first_seen_utc)
        if first_seen is None:
            logger.warning("[MarketConf] Cannot parse first_seen_utc=%s for cluster %d", first_seen_utc, cluster_id)
            return _STATUS_NO_REACTION

        now = _now_utc()
        due_windows = [
            w for w in _WINDOWS
            if now >= first_seen + timedelta(minutes=w)
        ]

        if not due_windows:
            try:
                _set_confirmation_status(self._db, cluster_id, _STATUS_PENDING)
            except Exception as exc:
                logger.debug("[MarketConf] pending status persist failed: %s", exc)
            return _STATUS_PENDING

        all_results: List[Dict] = []
        try:
            all_results = self._validator.validate_cluster(
                cluster_id,
                first_seen_utc=first_seen_utc,
                sentiment_score=sentiment_score,
                data_quality_score=data_quality_score,
                reliability_score=reliability_score,
                top_narrative=top_narrative,
            )
        except Exception as exc:
            logger.warning("[MarketConf] cluster=%d validation error: %s", cluster_id, exc)

        status = self._derive_status(all_results, due_windows)
        try:
            _set_confirmation_status(self._db, cluster_id, status)
        except Exception as exc:
            logger.warning("[MarketConf] status persist failed: %s", exc)

        logger.info(
            "[MarketConf] cluster=%d windows=%s status=%s",
            cluster_id, due_windows, status,
        )
        return status

    def _derive_status(self, results: List[Dict], due_windows: List[int]) -> str:
        """Classify the overall market reaction from multi-window results."""
        if not results:
            return _STATUS_NO_REACTION

        reaction_types = [r.get("reaction_latency_category", "NO_REACTION") for r in results]
        confirmed_results = [
            r for r in results
            if r.get("reaction_latency_category") not in ("NO_REACTION", None)
        ]

        # Conflicting if directions differ across results
        directions = [
            r.get("actual_direction") for r in results
            if r.get("actual_direction") is not None
        ]
        unique_dirs = set(d for d in directions if d and d not in ("NEUTRAL", None))
        conflicting = len(unique_dirs) >= 2

        if conflicting and confirmed_results:
            return _STATUS_CONFLICT

        if not confirmed_results:
            return _STATUS_NO_REACTION

        if all(r.get("reaction_latency_category") == "DELAYED" for r in confirmed_results):
            return _STATUS_DELAYED

        return _STATUS_CONFIRMED
