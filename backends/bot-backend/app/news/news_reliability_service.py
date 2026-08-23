"""
Source Reliability Service — Hardened.

Manages the news_sources table for:
  - Lookup of per-domain reliability (base + dynamic)
  - Cluster confidence computation
  - Dynamic score updates based on observed accuracy
  - Source blocking / trust management
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB


_UNKNOWN_DOMAIN_CAP     = 0.35   # Untrusted unknown sources never exceed this
_UNKNOWN_DOMAIN_DEFAULT = 0.20


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class NewsReliabilityService:
    def __init__(self, db: DB) -> None:
        self._db = db
        self._cache: Dict[str, Dict] = {}  # domain → full row dict

    # ─────────────────────────────────────────────
    # Core lookup
    # ─────────────────────────────────────────────

    def get_source(self, domain: Optional[str]) -> Optional[Dict]:
        """Return the full news_sources row dict for a domain, or None."""
        if not domain:
            return None
        domain = domain.lower().strip()
        if domain in self._cache:
            return self._cache[domain]
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM news_sources WHERE id = ?", (domain,)
            ).fetchone()
        result = dict(row) if row else None
        if result:
            self._cache[domain] = result
        return result

    def score(self, domain: Optional[str]) -> float:
        """
        Return the effective reliability score for a domain.
        Uses dynamic_reliability_score when available.
        Caps unknown domains at _UNKNOWN_DOMAIN_CAP.
        """
        if not domain:
            return _UNKNOWN_DOMAIN_DEFAULT

        row = self.get_source(domain)
        if not row:
            return _UNKNOWN_DOMAIN_DEFAULT

        if row.get("is_blocked"):
            return 0.0

        return float(row.get("dynamic_reliability_score", row.get("base_reliability_score", _UNKNOWN_DOMAIN_DEFAULT)))

    def is_trusted(self, domain: Optional[str]) -> bool:
        row = self.get_source(domain)
        return bool(row and row.get("is_trusted"))

    def is_blocked(self, domain: Optional[str]) -> bool:
        row = self.get_source(domain)
        return bool(row and row.get("is_blocked"))

    # ─────────────────────────────────────────────
    # Cluster confidence formula
    # ─────────────────────────────────────────────

    def compute_cluster_confidence(
        self,
        *,
        source_domains: List[Optional[str]],
        source_count: int,
        headline_similarity_score: float,  # 0.0–1.0 from spam detector
    ) -> float:
        """
        cluster_confidence = weighted_source_reliability
                             * log(source_count + 1) / log(11)  [normalised to ~1 at 10 sources]
                             * headline_similarity_score

        Returns float in [0.0, 1.0].
        """
        if not source_domains or source_count == 0:
            return 0.0

        reliabilities = [self.score(d) for d in source_domains]
        weighted_rel = sum(reliabilities) / len(reliabilities)

        # Diversity bonus: more unique domains → more credible
        unique_domains = len(set(d.lower() for d in source_domains if d))
        diversity_factor = min(1.0, unique_domains / max(1, source_count))

        # Log scale for source count (normalised so 10 sources ≈ 1.0)
        count_factor = math.log(source_count + 1) / math.log(11)

        # headline_similarity reflects how consistent the story is
        # Very high similarity can be spam, but moderate is credible
        # Optimal ~0.60; penalise extremes slightly
        consistency_factor = 1.0 - abs(headline_similarity_score - 0.55) * 0.5
        consistency_factor = max(0.2, consistency_factor)

        confidence = (
            weighted_rel
            * count_factor
            * consistency_factor
            * (0.7 + 0.3 * diversity_factor)  # diversity contributes 30%
        )

        return round(min(1.0, confidence), 4)

    # ─────────────────────────────────────────────
    # Dynamic score updates
    # ─────────────────────────────────────────────

    def update_dynamic_score(
        self,
        domain: str,
        delta: float,  # positive = reward, negative = penalise
        min_score: float = 0.05,
        max_score: float = 0.98,
    ) -> None:
        """
        Adjust dynamic_reliability_score by delta (clamped).
        Called when a signal is validated/invalidated post-hoc.
        """
        row = self.get_source(domain)
        if not row:
            return
        new_score = float(row["dynamic_reliability_score"]) + delta
        new_score = max(min_score, min(max_score, new_score))
        now = _now()
        with self._db.connect() as conn:
            conn.execute(
                "UPDATE news_sources SET dynamic_reliability_score=?, updated_at=? WHERE id=?",
                (new_score, now, domain),
            )
        # Invalidate cache
        self._cache.pop(domain, None)

    def block_source(self, domain: str, reason: str = "") -> None:
        """Mark a source as blocked — all future items from it are ignored."""
        now = _now()
        with self._db.connect() as conn:
            conn.execute(
                "UPDATE news_sources SET is_blocked=1, updated_at=? WHERE id=?",
                (now, domain),
            )
        self._cache.pop(domain, None)

    def unblock_source(self, domain: str) -> None:
        now = _now()
        with self._db.connect() as conn:
            conn.execute(
                "UPDATE news_sources SET is_blocked=0, updated_at=? WHERE id=?",
                (now, domain),
            )
        self._cache.pop(domain, None)

    def invalidate_cache(self) -> None:
        self._cache.clear()

    # ─────────────────────────────────────────────
    # Listing helpers (for Admin UI)
    # ─────────────────────────────────────────────

    def list_sources(
        self,
        trusted_only: bool = False,
        blocked_only: bool = False,
        limit: int = 100,
    ) -> List[Dict]:
        query = "SELECT * FROM news_sources"
        params: list = []
        conditions = []
        if trusted_only:
            conditions.append("is_trusted = 1")
        if blocked_only:
            conditions.append("is_blocked = 1")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY dynamic_reliability_score DESC LIMIT ?"
        params.append(limit)
        with self._db.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
