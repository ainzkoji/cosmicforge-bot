"""
DB-backed registry of all news sources.

Loads from news_sources table and provides per-source RSSProviderClient instances.
Tracks last-fetch times in-memory (no DB write on every tick).
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB
from app.news.rss_provider_client import RSSProviderClient

logger = logging.getLogger(__name__)


class NewsSourceRegistry:
    def __init__(
        self,
        db: DB,
        rss_enabled: bool = True,
        timeout: int = 15,
        max_items_per_source: int = 50,
    ) -> None:
        self._db = db
        self._rss_enabled = rss_enabled
        self._timeout = timeout
        self._max_items = max_items_per_source
        self._clients: Dict[str, RSSProviderClient] = {}
        self._load()

    def _load(self) -> None:
        if not self._rss_enabled:
            return
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, source_name, source_type, category,
                       rss_url, fetch_interval_seconds, is_enabled
                FROM news_sources
                WHERE source_type = 'RSS'
                  AND is_enabled  = 1
                  AND rss_url IS NOT NULL
                  AND rss_url != ''
                """
            ).fetchall()

        for row in rows:
            source_id = row[0]
            self._clients[source_id] = RSSProviderClient(
                source_id=source_id,
                source_name=row[1],
                rss_url=row[4],
                category=row[3] or "CRYPTO",
                timeout=self._timeout,
                max_items=self._max_items,
                fetch_interval_seconds=row[5] or 300,
            )
        logger.info("[NewsRegistry] loaded %d RSS sources", len(self._clients))

    def reload(self) -> None:
        self._clients.clear()
        self._load()

    def get_due_rss_clients(self) -> List[RSSProviderClient]:
        return [c for c in self._clients.values() if c.is_due()]

    def all_rss_clients(self) -> List[RSSProviderClient]:
        return list(self._clients.values())

    def get_client(self, source_id: str) -> Optional[RSSProviderClient]:
        return self._clients.get(source_id)

    def mark_fetched(self, source_id: str, last_fetch_utc: str, error: Optional[str] = None) -> None:
        """Persist last_fetch_utc and last_error back to news_sources."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        try:
            with self._db.connect() as conn:
                if error:
                    conn.execute(
                        "UPDATE news_sources SET last_fetch_utc=?, last_error=?, updated_at=? WHERE id=?",
                        (last_fetch_utc, error, now, source_id),
                    )
                else:
                    conn.execute(
                        "UPDATE news_sources SET last_fetch_utc=?, last_success_utc=?, last_error=NULL, updated_at=? WHERE id=?",
                        (last_fetch_utc, last_fetch_utc, now, source_id),
                    )
        except Exception as exc:
            logger.warning("[NewsRegistry] failed to persist fetch state for %s: %s", source_id, exc)

    def get_all_sources_status(self) -> list:
        """Returns all sources with their health state for the admin API."""
        with self._db.connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            rows = conn.execute(
                """
                SELECT ns.id, ns.source_name, ns.source_type, ns.category,
                       ns.is_enabled, ns.rss_url, ns.fetch_interval_seconds,
                       ns.last_fetch_utc, ns.last_success_utc, ns.last_error,
                       ns.base_reliability_score, ns.dynamic_reliability_score,
                       nph.status, nph.items_fetched_last_run,
                       nph.duplicate_count_last_run, nph.last_checked_utc
                FROM news_sources ns
                LEFT JOIN news_provider_health nph
                  ON nph.source_id = ns.id
                  AND nph.id = (
                    SELECT MAX(id) FROM news_provider_health
                    WHERE source_id = ns.id
                  )
                ORDER BY ns.source_type, ns.source_name
                """
            ).fetchall()
        return [dict(r) for r in rows]
