"""
CalendarSyncWorker — background task that keeps event_blackout_windows up to date.

Responsibilities:
  1. Re-compute blackout windows for upcoming economic_events (every sync_interval_hours).
  2. Deactivate windows whose end_utc has passed.
  3. Hard-delete windows older than 7 days to keep the table lean.
  4. Invalidate the EventCalendarService cache after each sync.

For Phase 1, events may come from manual seeding or the EventIngestionWorker.
This worker remains source-agnostic and only maintains the derived
event_blackout_windows rows from already-stored economic_events.

The worker intentionally does NOT download events itself.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, TYPE_CHECKING

from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import (
    get_upcoming_events,
    upsert_blackout_window,
    deactivate_past_windows,
    delete_old_windows,
)
from app.events.event_symbol_mapper import get_affected_symbols

if TYPE_CHECKING:
    from app.events.event_calendar_service import EventCalendarService

logger = logging.getLogger(__name__)


class CalendarSyncWorker:
    """
    Background worker that maintains event_blackout_windows from economic_events.

    Usage (bot-backend main.py)::

        worker = CalendarSyncWorker(db, calendar_service, sync_interval_hours=1)
        asyncio.create_task(worker.start())
        ...
        worker.stop()
    """

    def __init__(
        self,
        db: DB,
        calendar_service: Optional["EventCalendarService"] = None,
        sync_interval_hours: int = 1,
        blackout_minutes_before_high: int = 30,
        blackout_minutes_after_high: int = 15,
        blackout_minutes_before_medium: int = 5,
        blackout_minutes_after_medium: int = 5,
    ):
        self._db = db
        self._calendar_service = calendar_service
        self._sync_interval = sync_interval_hours * 3600
        self._bef_high = blackout_minutes_before_high
        self._aft_high = blackout_minutes_after_high
        self._bef_med = blackout_minutes_before_medium
        self._aft_med = blackout_minutes_after_medium
        self.running = False

    async def start(self) -> None:
        self.running = True
        logger.info("[CalendarSyncWorker] Started (interval=%dh)", self._sync_interval // 3600)
        while self.running:
            try:
                await asyncio.to_thread(self._sync_once)
            except Exception as exc:
                logger.error("[CalendarSyncWorker] Sync failed: %s", exc, exc_info=True)
            await asyncio.sleep(self._sync_interval)

    def stop(self) -> None:
        self.running = False
        logger.info("[CalendarSyncWorker] Stopping.")

    def sync_once_now(self) -> None:
        """Public one-shot sync hook used after ingestion updates land."""
        self._sync_once()

    def _sync_once(self) -> None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        look_back_minutes = max(self._bef_high, self._bef_med, self._aft_high, self._aft_med) + 5
        look_back_iso = (now - timedelta(minutes=look_back_minutes)).isoformat()
        look_ahead_iso = (now + timedelta(days=7)).isoformat()

        # 1. Deactivate expired windows
        deactivated = deactivate_past_windows(self._db, now_iso)
        if deactivated:
            logger.debug("[CalendarSyncWorker] Deactivated %d expired windows", deactivated)

        # 2. Delete stale windows older than 7 days
        old_cutoff = (now - timedelta(days=7)).isoformat()
        deleted = delete_old_windows(self._db, old_cutoff)
        if deleted:
            logger.debug("[CalendarSyncWorker] Deleted %d old windows", deleted)

        # 3. Re-compute windows for upcoming HIGH and MEDIUM events
        upcoming = get_upcoming_events(
            self._db,
            from_utc=look_back_iso,
            to_utc=look_ahead_iso,
            impact_levels=["HIGH", "MEDIUM"],
        )

        for ev in upcoming:
            try:
                self._ensure_window(ev)
            except Exception as exc:
                logger.warning(
                    "[CalendarSyncWorker] Failed to create window for event %s: %s",
                    ev.get("event_id"),
                    exc,
                )

        # 4. Invalidate cache so runner picks up changes immediately
        if self._calendar_service is not None:
            self._calendar_service.invalidate_cache()

        logger.info(
            "[CalendarSyncWorker] Sync complete — %d upcoming events processed",
            len(upcoming),
        )

    def _ensure_window(self, ev: dict) -> None:
        """Create or re-create a blackout window for one event."""
        event_db_id: int = ev["id"]
        impact = (ev.get("impact_level") or "").upper()
        scheduled_iso: str = ev["scheduled_utc"]

        try:
            scheduled_dt = datetime.fromisoformat(scheduled_iso)
            if scheduled_dt.tzinfo is None:
                scheduled_dt = scheduled_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning("[CalendarSyncWorker] Unparseable scheduled_utc=%s", scheduled_iso)
            return

        affected_symbols = get_affected_symbols(
            str(ev.get("event_type", "")),
            str(ev.get("country_currency", "")),
        )

        if impact == "HIGH":
            bef = self._bef_high
            aft = self._aft_high
            if affected_symbols is None:
                is_global = True
            elif affected_symbols:
                is_global = False
            else:
                logger.debug(
                    "[CalendarSyncWorker] HIGH event %s has no reliable affected symbols; skipping hard blackout",
                    ev.get("event_id"),
                )
                return
        elif impact == "MEDIUM":
            bef = self._bef_med
            aft = self._aft_med
            is_global = False  # medium: symbol-specific or log-only
        else:
            return  # LOW events don't get windows

        start_dt = scheduled_dt - timedelta(minutes=bef)
        end_dt = scheduled_dt + timedelta(minutes=aft)
        start_iso = start_dt.isoformat()
        end_iso = end_dt.isoformat()

        reason = (
            f"{impact}_IMPACT_{ev.get('country_currency','').upper()}"
            f"_{ev.get('event_type','EVENT').upper()}"
        )
        upsert_blackout_window(
            self._db,
            event_db_id=event_db_id,
            start_utc=start_iso,
            end_utc=end_iso,
            reason=reason,
            affected_symbols=affected_symbols if affected_symbols else None,
            is_global=is_global,
        )
        logger.debug(
            "[CalendarSyncWorker] Created window event_id=%s %s → %s",
            ev.get("event_id"),
            start_iso,
            end_iso,
        )


def _iso_diff_minutes(a: str, b: str) -> float:
    try:
        da = datetime.fromisoformat(a)
        db = datetime.fromisoformat(b)
        return abs((da - db).total_seconds() / 60.0)
    except Exception:
        return 999.0


def build_calendar_sync_worker(db: DB, calendar_service: Optional["EventCalendarService"] = None) -> CalendarSyncWorker:
    """Factory that reads config and constructs the worker."""
    from app.core.config import settings

    return CalendarSyncWorker(
        db=db,
        calendar_service=calendar_service,
        sync_interval_hours=1,
        blackout_minutes_before_high=settings.HIGH_IMPACT_BLACKOUT_MINUTES_BEFORE,
        blackout_minutes_after_high=settings.HIGH_IMPACT_BLACKOUT_MINUTES_AFTER,
        blackout_minutes_before_medium=settings.MEDIUM_IMPACT_BLACKOUT_MINUTES_BEFORE,
        blackout_minutes_after_medium=settings.MEDIUM_IMPACT_BLACKOUT_MINUTES_AFTER,
    )
