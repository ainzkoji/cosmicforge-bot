"""
EventReactionWorker — background async task that drives the Market Reaction Layer.

Responsibilities:
  1. Every REACTION_SNAPSHOT_INTERVAL_SECONDS, scan for upcoming events that
     fall within the pre-event window.
  2. Schedule and collect market snapshots at each window label.
  3. After all post-event windows are collected, trigger the tracker to
     compute metrics and save the reaction row.
  4. Operate as an observer only — NEVER opens or closes trades.

Window schedule (relative to event_time):
  PRE_60  → event_time - 60 min (± collect window)
  PRE_30  → event_time - 30 min
  EVENT   → event_time ± 5 min
  POST_5  → event_time + 5 min
  POST_15 → event_time + 15 min
  POST_30 → event_time + 30 min
  POST_60 → event_time + 60 min
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import get_upcoming_events
from app.events.market_reaction_tracker import MarketReactionTracker
from app.events.event_symbol_mapper import get_affected_symbols
from app.services.market_data_snapshot_service import MarketDataSnapshotService

logger = logging.getLogger(__name__)

# (offset_minutes, window_label, collect_tolerance_minutes)
# The worker fires the snapshot if now is within [target - tolerance, target + tolerance]
_WINDOW_SCHEDULE: List[Tuple[int, str, int]] = [
    (-60,  "PRE_60",   3),
    (-30,  "PRE_30",   3),
    (0,    "EVENT",    5),
    (5,    "POST_5",   3),
    (15,   "POST_15",  3),
    (30,   "POST_30",  3),
    (60,   "POST_60",  3),
]

# Label that marks completion (all post-event data should be in by this point)
_FINAL_WINDOW = "POST_60"


class EventReactionWorker:
    """
    Background worker: drives snapshot collection and reaction computation.

    Usage::

        worker = EventReactionWorker(db, snapshot_svc, tracker, config)
        asyncio.create_task(worker.start())
        ...
        worker.stop()
    """

    def __init__(
        self,
        db: DB,
        snapshot_service: MarketDataSnapshotService,
        tracker: MarketReactionTracker,
        *,
        enabled: bool = False,
        shadow_mode: bool = True,
        pre_event_minutes: int = 60,
        post_event_minutes: int = 240,
        snapshot_interval_seconds: int = 60,
        active_symbols: Optional[List[str]] = None,
        vol_spike_threshold: float = 2.5,
        volume_spike_threshold: float = 3.0,
        spread_widening_threshold: float = 2.0,
    ):
        self._db = db
        self._snap_svc = snapshot_service
        self._tracker = tracker
        self._enabled = enabled
        self._shadow_mode = shadow_mode
        self._pre_min = pre_event_minutes
        self._post_min = post_event_minutes
        self._interval = snapshot_interval_seconds
        self._active_symbols = active_symbols or []
        self._vol_spike_thresh = vol_spike_threshold
        self._volume_spike_thresh = volume_spike_threshold
        self._spread_thresh = spread_widening_threshold

        # (event_id, symbol) → set of completed window_labels
        self._completed: Dict[Tuple[str, str], Set[str]] = {}
        # (event_id, symbol) → True when final reaction has been computed
        self._finalized: Set[Tuple[str, str]] = set()

        self.running = False

    async def start(self) -> None:
        if not self._enabled:
            logger.info("[EventReactionWorker] Disabled — not starting.")
            return
        self.running = True
        logger.info(
            "[EventReactionWorker] Started (interval=%ds shadow=%s)",
            self._interval, self._shadow_mode,
        )
        while self.running:
            try:
                await asyncio.to_thread(self._tick)
            except Exception as exc:
                logger.error("[EventReactionWorker] Tick error: %s", exc, exc_info=True)
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        self.running = False
        logger.info("[EventReactionWorker] Stopping.")

    def _tick(self) -> None:
        now = datetime.now(timezone.utc)
        # Look for events within pre_event_minutes (before) and post_event_minutes (after)
        from_utc = (now - timedelta(minutes=self._post_min)).isoformat()
        to_utc   = (now + timedelta(minutes=self._pre_min)).isoformat()

        events = get_upcoming_events(
            self._db,
            from_utc=from_utc,
            to_utc=to_utc,
            impact_levels=["HIGH", "MEDIUM"],
        )

        for ev in events:
            try:
                self._process_event(ev, now)
            except Exception as exc:
                logger.warning(
                    "[EventReactionWorker] Error processing event %s: %s",
                    ev.get("event_id"), exc,
                )

    def _process_event(self, ev: Dict[str, Any], now: datetime) -> None:
        event_id: str = ev["event_id"]
        scheduled_iso: str = ev["scheduled_utc"]

        try:
            event_dt = datetime.fromisoformat(scheduled_iso)
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return

        symbols = self._symbols_for_event(ev)
        if not symbols:
            return

        for symbol in symbols:
            self._process_symbol(event_id, symbol, ev, event_dt, now)

    def _process_symbol(
        self,
        event_id: str,
        symbol: str,
        ev: Dict[str, Any],
        event_dt: datetime,
        now: datetime,
    ) -> None:
        key = (event_id, symbol)
        if key in self._finalized:
            return

        completed = self._completed.setdefault(key, set())

        # Check each window
        for offset_min, label, tolerance_min in _WINDOW_SCHEDULE:
            if label in completed:
                continue
            target_dt = event_dt + timedelta(minutes=offset_min)
            delta_s = abs((now - target_dt).total_seconds())
            if delta_s <= tolerance_min * 60:
                ok = self._snap_svc.fetch_and_store(event_id, symbol, label)
                if ok:
                    completed.add(label)
                    logger.debug(
                        "[EventReactionWorker] Collected %s for event=%s symbol=%s",
                        label, event_id, symbol,
                    )

        # If the final window is done, compute the reaction
        if _FINAL_WINDOW in completed and key not in self._finalized:
            self._finalize(event_id, symbol, ev, event_dt)
            self._finalized.add(key)

    def _finalize(
        self,
        event_id: str,
        symbol: str,
        ev: Dict[str, Any],
        event_dt: datetime,
    ) -> None:
        exchange = self._snap_svc._exchange
        pre_start = (event_dt - timedelta(minutes=self._pre_min)).isoformat()
        post_end = (event_dt + timedelta(minutes=self._post_min)).isoformat()

        result = self._tracker.compute_and_save(
            event_id=event_id,
            symbol=symbol,
            exchange=exchange,
            event_time_utc=event_dt.isoformat(),
            pre_window_start_utc=pre_start,
            post_window_end_utc=post_end,
            shadow_mode=self._shadow_mode,
            vol_spike_threshold=self._vol_spike_thresh,
            volume_spike_threshold=self._volume_spike_thresh,
            spread_widening_threshold=self._spread_thresh,
        )
        if result:
            logger.info(
                "[EventReactionWorker] Reaction finalized event=%s symbol=%s "
                "type=%s quality=%s",
                event_id, symbol,
                result.get("reaction_type"), result.get("data_quality"),
            )

    def _symbols_for_event(self, ev: Dict[str, Any]) -> List[str]:
        """Resolve which symbols to track for this event."""
        event_type = ev.get("event_type", "")
        currency = ev.get("country_currency", "")
        affected = get_affected_symbols(event_type, currency, self._active_symbols)

        if affected is None:
            # Global event — track all configured active symbols
            return list(self._active_symbols)

        if not affected:
            # Log-only event — still track if we have active symbols for completeness
            return list(self._active_symbols) if self._active_symbols else []

        # Symbol-specific event — intersect with active symbols (if set)
        if self._active_symbols:
            return [s for s in affected if s in self._active_symbols]
        return list(affected)


def build_event_reaction_worker(
    db: DB,
    snapshot_service: MarketDataSnapshotService,
    tracker: MarketReactionTracker,
    active_symbols: Optional[List[str]] = None,
) -> EventReactionWorker:
    """Factory that wires worker to current settings."""
    from app.core.config import settings

    return EventReactionWorker(
        db=db,
        snapshot_service=snapshot_service,
        tracker=tracker,
        enabled=settings.MARKET_REACTION_LAYER_ENABLED and settings.REACTION_TRACKING_ENABLED,
        shadow_mode=settings.MARKET_REACTION_SHADOW_MODE,
        pre_event_minutes=settings.REACTION_PRE_EVENT_MINUTES,
        post_event_minutes=settings.REACTION_POST_EVENT_MINUTES,
        snapshot_interval_seconds=settings.REACTION_SNAPSHOT_INTERVAL_SECONDS,
        active_symbols=active_symbols or [],
        vol_spike_threshold=settings.REACTION_VOL_SPIKE_THRESHOLD,
        volume_spike_threshold=settings.REACTION_VOLUME_SPIKE_THRESHOLD,
        spread_widening_threshold=settings.REACTION_SPREAD_WIDENING_THRESHOLD,
    )
