"""
News Conflict Detector.

Detects CONFLICTING_REPORTS within a cluster or across near-identical clusters.

Rules:
  1. Intra-cluster sentiment split — items in the same cluster have opposing
     compound sentiment scores (one > +0.2, another < -0.2).
  2. Keyword opposition — title contains both approval and denial keywords
     about the same subject (e.g. "approved" and "denied" in the same cluster).
  3. Cross-cluster conflict — two clusters with >0.85 Jaccard title similarity
     but opposite dominant sentiments → both flagged.

Result is stored in news_clusters.conflict_flag = 1.
Signals from conflicted clusters should be suppressed by the signal service.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Set, Tuple

from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

# Keyword pairs that indicate direct opposition on the same subject
_OPPOSITION_PAIRS: List[Tuple[Set[str], Set[str]]] = [
    ({"approved", "approval", "green light", "cleared", "confirmed"},
     {"denied", "rejected", "refused", "blocked", "no approval", "ban", "banned", "illegal", "prohibited", "crackdown"}),
    ({"launched", "live", "released", "announced"},
     {"cancelled", "postponed", "delayed", "abandoned", "shut down"}),
    ({"bullish", "surge", "rally", "gains", "all-time high"},
     {"bearish", "crash", "drop", "collapse", "all-time low"}),
    ({"partnership", "deal", "agreement", "acquisition"},
     {"deal fell through", "ended", "terminated", "dispute", "lawsuit"}),
    ({"no ban", "legal", "allowed", "permitted"},
     {"ban", "banned", "illegal", "prohibited", "crackdown"}),
]

_CONFLICT_STATUS = "CONFLICTING_REPORTS"


def _tokenize(text: str) -> Set[str]:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _has_keyword_conflict(titles: List[str]) -> bool:
    combined = " ".join(titles).lower()
    for pos_set, neg_set in _OPPOSITION_PAIRS:
        has_pos = any(kw in combined for kw in pos_set)
        has_neg = any(kw in combined for kw in neg_set)
        if has_pos and has_neg:
            return True
    return False


def _has_sentiment_split(compound_scores: List[float]) -> bool:
    """At least one item strongly positive AND one strongly negative."""
    has_pos = any(s >= 0.20 for s in compound_scores)
    has_neg = any(s <= -0.20 for s in compound_scores)
    return has_pos and has_neg


def _flag_cluster_conflict(db: DB, cluster_id: int) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute(
            """UPDATE news_clusters SET
               conflict_flag=1,
               market_confirmation_status=?,
               updated_at=?
               WHERE id=?""",
            (_CONFLICT_STATUS, now, cluster_id),
        )


class NewsConflictDetector:
    def __init__(self, db: DB, cross_cluster_threshold: float = 0.85) -> None:
        self._db = db
        self._threshold = cross_cluster_threshold

    def check_cluster(
        self,
        cluster_id: int,
        item_titles: List[str],
        compound_scores: Optional[List[float]] = None,
    ) -> bool:
        """
        Check a single cluster for internal conflict.
        Returns True if conflict detected and flagged.
        """
        if _has_keyword_conflict(item_titles):
            _flag_cluster_conflict(self._db, cluster_id)
            logger.info("[ConflictDetector] keyword conflict in cluster %d", cluster_id)
            return True

        if compound_scores and _has_sentiment_split(compound_scores):
            _flag_cluster_conflict(self._db, cluster_id)
            logger.info("[ConflictDetector] sentiment split in cluster %d", cluster_id)
            return True

        return False

    def check_cross_cluster(
        self,
        new_cluster_id: int,
        new_title: str,
        new_dominant_sentiment: float,
        since_utc: str,
    ) -> bool:
        """
        Compare a new cluster's title against recent clusters.
        If a near-identical cluster exists with opposite sentiment, flag both.
        Returns True if conflict detected.
        """
        new_tokens = _tokenize(new_title)
        with self._db.connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            rows = conn.execute(
                """SELECT id, canonical_title FROM news_clusters
                   WHERE first_seen_utc >= ?
                     AND id != ?
                     AND conflict_flag = 0
                   LIMIT 200""",
                (since_utc, new_cluster_id),
            ).fetchall()

        for row in rows:
            existing_tokens = _tokenize(row["canonical_title"])
            if _jaccard(new_tokens, existing_tokens) >= self._threshold:
                # Titles are near-identical — fetch dominant sentiment of existing
                existing_sentiment = self._get_cluster_dominant_sentiment(row["id"])
                if existing_sentiment is not None:
                    # Conflict if signs are opposite
                    if (new_dominant_sentiment > 0.10 and existing_sentiment < -0.10) or \
                       (new_dominant_sentiment < -0.10 and existing_sentiment > 0.10):
                        _flag_cluster_conflict(self._db, new_cluster_id)
                        _flag_cluster_conflict(self._db, row["id"])
                        logger.info(
                            "[ConflictDetector] cross-cluster conflict: %d vs %d",
                            new_cluster_id, row["id"],
                        )
                        return True
        return False

    def _get_cluster_dominant_sentiment(self, cluster_id: int) -> Optional[float]:
        with self._db.connect() as conn:
            row = conn.execute(
                """SELECT AVG(compound_raw) as avg_compound
                   FROM news_sentiment_scores
                   WHERE cluster_id = ?""",
                (cluster_id,),
            ).fetchone()
        if row and row[0] is not None:
            return float(row[0])
        return None
