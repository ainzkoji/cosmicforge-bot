"""
Fake News Risk Service.

Aggregates signals from multiple quality engines into a single 0-1 risk score.

Formula:
    fake_news_risk = (1 - max_source_reliability) * 0.30
                   + spam_score                   * 0.20
                   + (1 - latency_score)          * 0.15
                   + single_source_penalty        * 0.20
                   + conflict_penalty             * 0.15

Flags:
  >= 0.60 → POSSIBLE_FAKE_NEWS  (stored in market_confirmation_status)
  >= 0.80 → HIGH_FAKE_NEWS_RISK
Score stored in news_clusters.fake_news_risk_score.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

_FLAG_POSSIBLE  = "POSSIBLE_FAKE_NEWS"
_FLAG_HIGH_RISK = "HIGH_FAKE_NEWS_RISK"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_cluster_risk(
    db: DB,
    cluster_id: int,
    risk_score: float,
    status_flag: Optional[str],
) -> None:
    now = _now()
    with db.connect() as conn:
        if status_flag:
            conn.execute(
                """UPDATE news_clusters
                   SET fake_news_risk_score=?,
                       market_confirmation_status=?,
                       updated_at=?
                   WHERE id=?""",
                (risk_score, status_flag, now, cluster_id),
            )
        else:
            conn.execute(
                """UPDATE news_clusters
                   SET fake_news_risk_score=?,
                       updated_at=?
                   WHERE id=?""",
                (risk_score, now, cluster_id),
            )


class FakeNewsRiskService:
    """
    Computes fake_news_risk_score for a cluster and persists it.

    All results are shadow-only — never affects live trading.
    """

    def __init__(self, db: DB) -> None:
        self._db = db

    def score(
        self,
        *,
        cluster_id: int,
        source_reliabilities: List[float],
        spam_score: float,
        latency_score: float,
        conflict_flag: bool = False,
        source_count: int = 1,
    ) -> float:
        """
        Compute and persist fake_news_risk_score.

        Parameters
        ----------
        cluster_id          : DB cluster id
        source_reliabilities: per-item reliability scores (list may be empty)
        spam_score          : 0-1 from compute_spam_score()
        latency_score       : 0-1 from compute_latency_score()
        conflict_flag       : True if conflict_detector flagged this cluster
        source_count        : distinct source domains reporting the story

        Returns
        -------
        float: fake_news_risk_score in [0.0, 1.0]
        """
        max_reliability = max(source_reliabilities) if source_reliabilities else 0.20

        # Single-source penalty: only one domain → higher risk
        single_source_penalty = 1.0 if source_count <= 1 else max(0.0, 1.0 - (source_count - 1) / 4.0)

        conflict_penalty = 1.0 if conflict_flag else 0.0

        risk = (
            (1.0 - max_reliability)   * 0.30
            + spam_score              * 0.20
            + (1.0 - latency_score)   * 0.15
            + single_source_penalty   * 0.20
            + conflict_penalty        * 0.15
        )
        risk = round(min(1.0, max(0.0, risk)), 4)

        status_flag: Optional[str] = None
        if risk >= 0.80:
            status_flag = _FLAG_HIGH_RISK
        elif risk >= 0.60:
            status_flag = _FLAG_POSSIBLE

        try:
            _update_cluster_risk(self._db, cluster_id, risk, status_flag)
        except Exception as exc:
            logger.warning("[FakeNewsRisk] DB update failed for cluster %d: %s", cluster_id, exc)

        if status_flag:
            logger.info(
                "[FakeNewsRisk] cluster=%d risk=%.3f flag=%s",
                cluster_id, risk, status_flag,
            )

        return risk

    @staticmethod
    def classify(risk_score: float) -> Optional[str]:
        """Return human-readable flag for a precomputed score."""
        if risk_score >= 0.80:
            return _FLAG_HIGH_RISK
        if risk_score >= 0.60:
            return _FLAG_POSSIBLE
        return None
