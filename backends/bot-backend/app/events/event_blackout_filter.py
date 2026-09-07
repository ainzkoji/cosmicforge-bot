"""
EventBlackoutFilter — single decision point for the runner.

Called twice per cycle:
  1. Before strategy signal generation (fast return if globally blocked).
  2. Before policy engine evaluation (safety net).

Design invariants:
  - Returns (False, None, {}) when EVENT_FILTER_ENABLED=False (zero overhead).
  - Returns (True, reason, details) when any high-impact window is active.
  - When feed is stale AND FAILSAFE_ENABLED=True, blocks as a safety measure.
  - NEVER closes existing positions — only blocks NEW entries.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

from app.core.config import settings
from app.events.event_calendar_service import EventCalendarService
from app.events.event_symbol_mapper import is_global_block_currency

logger = logging.getLogger(__name__)


@dataclass
class BlockDecision:
    is_blocked: bool
    reason: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class EventBlackoutFilter:
    """
    Evaluates whether the runner should skip new entries for a given symbol.

    Parameters are read from the Settings object at construction time.
    To change behaviour at runtime, restart the bot (standard config reload path).

    Decision logic:
      EVENT_FILTER_ENABLED=False  → always allow (zero overhead)
      ALLOW_TRADING_DURING_NEWS=True → bypass filter (explicit admin override)
      Stale feed + FAILSAFE_ENABLED → block all
      Active HIGH-impact window → block
      Active MEDIUM or LOW window → log only, allow
    """

    def __init__(
        self,
        calendar_service: EventCalendarService,
        *,
        enabled: bool = False,
        allow_trading_during_news: bool = False,
        failsafe_enabled: bool = True,
        stale_hours: int = 24,
        stale_failsafe_warn_only: bool = False,
    ):
        self._svc = calendar_service
        self._enabled = enabled
        self._allow_during_news = allow_trading_during_news
        self._failsafe = failsafe_enabled
        self._stale_hours = stale_hours
        self._stale_warn_only = stale_failsafe_warn_only

    def check(self, symbol: Optional[str] = None) -> BlockDecision:
        """
        Return a BlockDecision for the given symbol (or global check if symbol=None).

        Callers should inspect .is_blocked and .reason.
        """
        if not self._enabled:
            return BlockDecision(is_blocked=False)

        # ALLOW_TRADING_DURING_NEWS=True is an explicit admin override — bypass all checks
        if self._allow_during_news:
            logger.debug("[EventFilter] ALLOW_TRADING_DURING_NEWS=True — bypassing filter")
            return BlockDecision(is_blocked=False)

        # --- Stale feed failsafe ---
        staleness = self._svc.get_feed_staleness_hours()
        if staleness is None:
            # No events in DB at all — treat as empty calendar, allow through
            # (prevents blocking on a fresh install with no events seeded yet)
            return BlockDecision(is_blocked=False)

        stale_warn_details: Dict[str, Any] | None = None
        if staleness > self._stale_hours:
            if self._stale_warn_only:
                logger.warning(
                    "[EventFilter] Feed stale %.1fh > threshold %dh — warn-only in testnet/demo",
                    staleness,
                    self._stale_hours,
                )
                stale_warn_details = {
                    "event_id": None,
                    "block_type": "FEED_STALE_WARN_ONLY",
                    "staleness_hours": round(staleness, 2),
                    "threshold_hours": self._stale_hours,
                }
            elif self._failsafe:
                logger.warning(
                    "[EventFilter] Feed stale %.1fh > threshold %dh — blocking (failsafe)",
                    staleness,
                    self._stale_hours,
                )
                return BlockDecision(
                    is_blocked=True,
                    reason="EVENT_FEED_STALE_FAILSAFE",
                    details={
                        "event_id": None,
                        "block_type": "FEED_STALE_FAILSAFE",
                        "staleness_hours": round(staleness, 2),
                        "threshold_hours": self._stale_hours,
                    },
                )
            else:
                logger.warning(
                    "[EventFilter] Feed stale %.1fh but failsafe disabled — allowing through",
                    staleness,
                )
                stale_warn_details = {
                    "event_id": None,
                    "block_type": "FEED_STALE_FAILSAFE_DISABLED",
                    "staleness_hours": round(staleness, 2),
                    "threshold_hours": self._stale_hours,
                }

        # --- Active blackout windows ---
        windows = self._svc.get_active_windows()
        if not windows:
            if stale_warn_details:
                return BlockDecision(
                    is_blocked=False,
                    reason="EVENT_FEED_STALE_WARN_ONLY",
                    details=stale_warn_details,
                )
            return BlockDecision(is_blocked=False)

        for w in windows:
            impact = (w.get("impact_level") or "").upper()
            if impact != "HIGH":
                continue  # medium/low never hard-block

            is_global = bool(w.get("is_global", 1))
            affected_raw = w.get("affected_symbols")

            if is_global:
                return self._make_block(w, symbol)

            # Symbol-specific check
            if symbol and affected_raw:
                try:
                    affected: List[str] = json.loads(affected_raw)
                    if symbol in affected:
                        return self._make_block(w, symbol)
                except (ValueError, TypeError):
                    pass

            # Fallback: if country_currency is a global-block currency, treat as global
            if is_global_block_currency(w.get("country_currency", "")):
                return self._make_block(w, symbol)

        return BlockDecision(is_blocked=False)

    @staticmethod
    def _make_block(window: Dict[str, Any], symbol: Optional[str]) -> BlockDecision:
        block_type = "GLOBAL_BLACKOUT" if bool(window.get("is_global", 1)) else "SYMBOL_BLACKOUT"
        reason = (
            f"HIGH_IMPACT_{window.get('country_currency','').upper()}"
            f"_{window.get('event_type','EVENT').upper()}_BLACKOUT"
        )
        details = {
            "event_id": window.get("event_id"),
            "block_type": block_type,
            "title": window.get("title"),
            "impact_level": window.get("impact_level"),
            "event_type": window.get("event_type"),
            "country_currency": window.get("country_currency"),
            "window_start_utc": window.get("start_utc"),
            "window_end_utc": window.get("end_utc"),
            "affected_symbols": window.get("affected_symbols"),
            "symbol_checked": symbol,
        }
        logger.info(
            "[EventFilter] BLOCKED symbol=%s reason=%s window_end=%s",
            symbol or "GLOBAL",
            reason,
            window.get("end_utc"),
        )
        return BlockDecision(is_blocked=True, reason=reason, details=details)


def build_event_blackout_filter(calendar_service: EventCalendarService) -> EventBlackoutFilter:
    """Factory that wires filter to current settings."""
    is_testnet_demo = (
        str(getattr(settings, "BINANCE_ENV", "") or "").lower() in {"testnet", "demo"}
        or "demo-fapi" in str(getattr(settings, "BINANCE_FAPI_BASE_URL", "") or "").lower()
    )
    # Manual calendar mode: EVENT_INGESTION_ENABLED=false means no live feed updates
    # are expected, so the staleness clock is meaningless — treat as warn-only.
    is_manual_calendar = not bool(getattr(settings, "EVENT_INGESTION_ENABLED", True))
    stale_warn_only = bool(
        (is_testnet_demo or is_manual_calendar)
        and getattr(settings, "EVENT_FILTER_STALE_FAILSAFE_TESTNET_WARN_ONLY", True)
    )
    return EventBlackoutFilter(
        calendar_service=calendar_service,
        enabled=settings.EVENT_FILTER_ENABLED,
        allow_trading_during_news=settings.ALLOW_TRADING_DURING_NEWS,
        failsafe_enabled=settings.EVENT_FILTER_FAILSAFE_ENABLED,
        stale_hours=settings.EVENT_FEED_STALE_HOURS,
        stale_failsafe_warn_only=stale_warn_only,
    )
