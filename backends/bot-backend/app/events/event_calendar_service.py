"""
EventCalendarService — query layer for the event blackout system.

Thin wrapper over shared_lib economic_events helpers, with caching to avoid
hitting SQLite on every runner cycle (default TTL: 30 seconds).
"""
from __future__ import annotations

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import (
    get_active_blackout_windows,
    get_upcoming_events,
    get_last_sync_utc,
)

logger = logging.getLogger(__name__)

# Cache TTL in seconds — avoid SQLite on every 10-second runner cycle
_CACHE_TTL = 30.0


class EventCalendarService:
    """
    Provides cached access to economic event and blackout window data.

    Thread-safe: caching uses instance-level state; the runner creates one
    instance and shares it across symbol threads.
    """

    def __init__(self, db: DB):
        self._db = db
        self._cache_windows: List[Dict[str, Any]] = []
        self._cache_ts: float = 0.0

    def _refresh_if_stale(self) -> None:
        if time.monotonic() - self._cache_ts > _CACHE_TTL:
            now_iso = datetime.now(timezone.utc).isoformat()
            try:
                self._cache_windows = get_active_blackout_windows(self._db, now_iso)
            except Exception as exc:
                logger.warning("[EventCalendar] Failed to refresh blackout windows: %s", exc)
            self._cache_ts = time.monotonic()

    def get_active_windows(self) -> List[Dict[str, Any]]:
        """Return currently active blackout windows (cached, refreshed every 30s)."""
        self._refresh_if_stale()
        return self._cache_windows

    def invalidate_cache(self) -> None:
        """Force next call to re-query the DB (used after sync worker runs)."""
        self._cache_ts = 0.0

    def get_upcoming_events(
        self,
        hours_ahead: int = 168,
        impact_levels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        to_utc = (now + timedelta(hours=hours_ahead)).isoformat()
        return get_upcoming_events(
            self._db,
            from_utc=now.isoformat(),
            to_utc=to_utc,
            impact_levels=impact_levels,
        )

    def get_feed_staleness_hours(self) -> Optional[float]:
        """Return hours since last event sync, or None if no events exist."""
        last_sync = get_last_sync_utc(self._db)
        if not last_sync:
            return None
        try:
            last_dt = datetime.fromisoformat(last_sync)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - last_dt
            return delta.total_seconds() / 3600.0
        except Exception:
            return None
