"""
Provider health tracking service.

Writes one row to news_provider_health after each fetch attempt.
Status logic:
  HEALTHY  — fetched successfully, items > 0  OR  all items were already-seen duplicates
             (a fully-seeded feed that returns only known articles is still healthy)
  DEGRADED — fetched but 0 new items AND 0 duplicates (truly empty or silent feed)
  FAILED   — HTTP error or parse error
  STALE    — last success was > NEWS_SOURCE_STALE_MINUTES ago
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

_STATUS_HEALTHY  = "HEALTHY"
_STATUS_DEGRADED = "DEGRADED"
_STATUS_FAILED   = "FAILED"
_STATUS_STALE    = "STALE"
_STATUS_UNKNOWN  = "UNKNOWN"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProviderHealthService:
    def __init__(self, db: DB, stale_minutes: int = 60) -> None:
        self._db = db
        self._stale_minutes = stale_minutes

    def record(
        self,
        source_id: str,
        items_fetched: int,
        duplicate_count: int,
        error_message: Optional[str],
        last_success_utc: Optional[str] = None,
    ) -> str:
        now = _now()

        if error_message:
            status = _STATUS_FAILED
        elif items_fetched > 0:
            status = _STATUS_HEALTHY
        elif duplicate_count > 0:
            # Feed is responding and returning known content — still healthy.
            # All-duplicate runs are normal after initial ingestion.
            status = _STATUS_HEALTHY
        else:
            # Fetched OK but got 0 items and 0 duplicates — truly empty/silent feed.
            status = _STATUS_DEGRADED

        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO news_provider_health
                    (source_id, status, last_checked_utc, last_success_utc,
                     items_fetched_last_run, duplicate_count_last_run,
                     error_message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (source_id, status, now, last_success_utc,
                 items_fetched, duplicate_count, error_message, now),
            )
        return status

    def get_latest(self, source_id: str) -> Optional[Dict]:
        with self._db.connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            row = conn.execute(
                """SELECT * FROM news_provider_health
                   WHERE source_id = ?
                   ORDER BY id DESC LIMIT 1""",
                (source_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_all_latest(self) -> List[Dict]:
        with self._db.connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            rows = conn.execute(
                """
                SELECT * FROM news_provider_health nph
                WHERE nph.id = (
                    SELECT MAX(id) FROM news_provider_health
                    WHERE source_id = nph.source_id
                )
                ORDER BY source_id
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def compute_stale_status(self, last_success_utc: Optional[str]) -> str:
        """Return STALE if last success was too long ago."""
        if not last_success_utc:
            return _STATUS_UNKNOWN
        try:
            last = datetime.fromisoformat(last_success_utc.replace("Z", "+00:00"))
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=self._stale_minutes)
            return _STATUS_STALE if last < cutoff else _STATUS_HEALTHY
        except Exception:
            return _STATUS_UNKNOWN

    def get_feed_summary(self) -> Dict:
        """Aggregate stats for the admin LiveNewsFlowStatus panel."""
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        with self._db.connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            today_count_row = conn.execute(
                """SELECT COUNT(*) as c
                   FROM raw_news_items
                   WHERE ingested_utc >= ?
                     AND provider != 'manual'""",
                (today_start,),
            ).fetchone()
            today_count = int(today_count_row["c"]) if today_count_row else 0

            latest = (conn.execute(
                """SELECT title, ingested_utc
                   FROM raw_news_items
                   WHERE provider != 'manual'
                   ORDER BY id DESC LIMIT 1"""
            ).fetchone())

            health_rows = conn.execute(
                """
                SELECT nph.source_id, nph.status, nph.last_success_utc
                FROM news_provider_health nph
                WHERE nph.id = (
                    SELECT MAX(id) FROM news_provider_health WHERE source_id = nph.source_id
                )
                """
            ).fetchall()

        active = sum(1 for r in health_rows if r[1] in (_STATUS_HEALTHY, _STATUS_DEGRADED))
        failed = sum(1 for r in health_rows if r[1] == _STATUS_FAILED)

        return {
            "today_count": today_count,
            "active_sources": active,
            "failed_sources": failed,
            "has_live_data": today_count > 0,
            "latest_title": dict(latest)["title"] if latest else None,
            "latest_ingested_utc": dict(latest)["ingested_utc"] if latest else None,
        }
