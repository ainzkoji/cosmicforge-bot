"""
Jaccard-based news deduplication service.

Items with Jaccard similarity >= threshold are assigned to the same cluster.
No ML dependencies — pure token overlap.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import (
    get_recent_clusters,
    insert_cluster,
    update_cluster,
    link_item_to_cluster,
    get_items_for_cluster,
)


_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "will", "would", "could", "should", "may", "might",
    "this", "that", "these", "those", "it", "its", "as", "up", "out", "into",
    "over", "after", "before", "than", "so", "about", "says", "said", "new",
    "can", "now", "just", "also", "how", "what", "which", "who", "when",
})


def _tokenize(text: str) -> Set[str]:
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return {t for t in tokens if t not in _STOP_WORDS and len(t) > 1}


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union


class NewsDeduplicationService:
    def __init__(
        self,
        db: DB,
        similarity_threshold: float = 0.82,
        window_hours: int = 6,
    ) -> None:
        self._db = db
        self._threshold = similarity_threshold
        self._window_hours = window_hours

    def assign_cluster(
        self,
        raw_news_item_id: int,
        title: str,
        published_utc: str,
        reliability_score: float = 0.0,
        provider: str = "",
    ) -> Tuple[int, bool]:
        """
        Find or create a cluster for this item.
        Returns (cluster_id, is_new_cluster).
        """
        since = (
            datetime.now(timezone.utc) - timedelta(hours=self._window_hours)
        ).isoformat()
        recent_clusters = get_recent_clusters(self._db, since_utc=since, limit=200)

        item_tokens = _tokenize(title)
        best_cluster_id: Optional[int] = None
        best_score = 0.0

        for cluster in recent_clusters:
            cluster_tokens = _tokenize(cluster["canonical_title"])
            score = _jaccard(item_tokens, cluster_tokens)
            if score >= self._threshold and score > best_score:
                best_score = score
                best_cluster_id = cluster["id"]

        if best_cluster_id is not None:
            cluster_row = next(
                (c for c in recent_clusters if c["id"] == best_cluster_id), None
            )
            if cluster_row:
                new_source_count = cluster_row["source_count"] + 1
                new_provider_count = cluster_row.get("provider_count", 1)
                providers_in_cluster = {
                    item.get("provider", "")
                    for item in get_items_for_cluster(self._db, best_cluster_id)
                }
                if provider and provider not in providers_in_cluster:
                    new_provider_count += 1

                update_cluster(
                    self._db,
                    best_cluster_id,
                    last_seen_utc=published_utc,
                    source_count=new_source_count,
                    provider_count=new_provider_count,
                    highest_reliability_score=max(
                        cluster_row.get("highest_reliability_score", 0.0),
                        reliability_score,
                    ),
                    cluster_confidence=min(0.99, new_source_count / 5),
                )
            link_item_to_cluster(
                self._db, best_cluster_id, raw_news_item_id, similarity_score=best_score
            )
            return best_cluster_id, False
        else:
            cluster_id = insert_cluster(
                self._db,
                canonical_title=title,
                first_seen_utc=published_utc,
                reliability_score=reliability_score,
                cluster_confidence=0.2,
            )
            link_item_to_cluster(
                self._db, cluster_id, raw_news_item_id, similarity_score=1.0
            )
            return cluster_id, True
