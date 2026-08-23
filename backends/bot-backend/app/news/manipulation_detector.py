"""
Manipulation and coordinated-narrative detection for news clusters.

Rules:
  1. Domain flood  — same domain published N items within a short window
  2. Low-reliability flood — cluster has many low-reliability sources
  3. Rumor amplification — RUMOR narrative + rapid multi-source spread
  4. Coordinated timing — N items arrive within minutes of each other

Never produces a trading signal — only flags clusters in the DB.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import (
    get_items_for_cluster,
    update_cluster,
    get_recent_items,
)


_REASON_DOMAIN_FLOOD = "DOMAIN_FLOOD"
_REASON_LOW_RELIABILITY = "LOW_RELIABILITY_FLOOD"
_REASON_RUMOR_AMPLIFICATION = "RUMOR_AMPLIFICATION"
_REASON_COORDINATED_TIMING = "COORDINATED_TIMING"


class ManipulationDetector:
    def __init__(
        self,
        db: DB,
        domain_flood_threshold: int = 5,
        domain_flood_window_minutes: int = 10,
        low_reliability_threshold: float = 0.40,
        low_reliability_ratio: float = 0.70,
        coordinated_timing_items: int = 4,
        coordinated_timing_minutes: int = 3,
    ) -> None:
        self._db = db
        self._domain_flood_threshold = domain_flood_threshold
        self._domain_flood_window_minutes = domain_flood_window_minutes
        self._low_reliability_threshold = low_reliability_threshold
        self._low_reliability_ratio = low_reliability_ratio
        self._coordinated_timing_items = coordinated_timing_items
        self._coordinated_timing_minutes = coordinated_timing_minutes

    def check_and_flag(
        self,
        cluster_id: int,
        cluster_row: Dict,
        narratives: List[Dict],
        domain_reliability_map: Dict[str, float],
    ) -> Tuple[bool, Optional[str]]:
        """
        Returns (is_suspect, reason_code).
        If suspect, updates cluster row in DB.
        """
        items = get_items_for_cluster(self._db, cluster_id)
        if not items:
            return False, None

        reason = self._check_domain_flood(items)
        if not reason:
            reason = self._check_low_reliability_flood(items, domain_reliability_map)
        if not reason:
            reason = self._check_rumor_amplification(items, narratives)
        if not reason:
            reason = self._check_coordinated_timing(items)

        if reason:
            update_cluster(
                self._db,
                cluster_id,
                last_seen_utc=cluster_row.get("last_seen_utc", datetime.now(timezone.utc).isoformat()),
                source_count=cluster_row.get("source_count", len(items)),
                provider_count=cluster_row.get("provider_count", 1),
                highest_reliability_score=cluster_row.get("highest_reliability_score", 0.0),
                cluster_confidence=cluster_row.get("cluster_confidence", 0.0),
                canonical_title=None,
                is_manipulation_suspect=True,
                manipulation_reason=reason,
            )
            return True, reason

        return False, None

    def _check_domain_flood(self, items: List[Dict]) -> Optional[str]:
        """Flag if the same domain contributes >= threshold items."""
        domain_counts: Dict[str, int] = {}
        cutoff = (
            datetime.now(timezone.utc)
            - timedelta(minutes=self._domain_flood_window_minutes)
        ).isoformat()

        for item in items:
            domain = item.get("source_domain", "") or ""
            if not domain:
                continue
            published = item.get("published_utc", "") or ""
            if published >= cutoff:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1

        for domain, count in domain_counts.items():
            if count >= self._domain_flood_threshold:
                return f"{_REASON_DOMAIN_FLOOD}:{domain}"
        return None

    def _check_low_reliability_flood(
        self, items: List[Dict], reliability_map: Dict[str, float]
    ) -> Optional[str]:
        """Flag if majority of sources are low-reliability."""
        if len(items) < 3:
            return None
        low_count = sum(
            1
            for item in items
            if reliability_map.get(item.get("source_domain", "") or "", 0.5)
            < self._low_reliability_threshold
        )
        ratio = low_count / len(items)
        if ratio >= self._low_reliability_ratio:
            return _REASON_LOW_RELIABILITY
        return None

    def _check_rumor_amplification(
        self, items: List[Dict], narratives: List[Dict]
    ) -> Optional[str]:
        """Flag rumor narratives that spread across 3+ sources."""
        is_rumor = any(
            n.get("narrative_type") == "RUMOR_SPECULATION" and n.get("confidence", 0) > 0.5
            for n in narratives
        )
        if is_rumor and len(items) >= 3:
            return _REASON_RUMOR_AMPLIFICATION
        return None

    def _check_coordinated_timing(self, items: List[Dict]) -> Optional[str]:
        """Flag N+ items published within a tight time window."""
        if len(items) < self._coordinated_timing_items:
            return None
        try:
            times = sorted(
                datetime.fromisoformat(item["published_utc"].replace("Z", "+00:00"))
                for item in items
                if item.get("published_utc")
            )
        except Exception:
            return None

        window = timedelta(minutes=self._coordinated_timing_minutes)
        for i in range(len(times) - self._coordinated_timing_items + 1):
            if times[i + self._coordinated_timing_items - 1] - times[i] <= window:
                return _REASON_COORDINATED_TIMING
        return None
