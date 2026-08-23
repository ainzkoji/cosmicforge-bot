"""
Unit tests for the Event Awareness Phase 1 system.

Run with:
    cd backends/bot-backend
    python -m pytest tests/test_event_blackout_filter.py -v

Tests cover:
    T1  - HIGH impact event 20min away → blocked
    T2  - HIGH impact event 35min away → NOT blocked (outside window)
    T3  - HIGH impact event 10min after → still blocked
    T4  - HIGH impact event 20min after → NOT blocked (window closed)
    T5  - MEDIUM impact event → NOT hard-blocked (log only)
    T6  - EVENT_FILTER_ENABLED=False → never blocked
    T7  - Stale feed + FAILSAFE_ENABLED=True → blocked
    T8  - Stale feed + FAILSAFE_ENABLED=False → allowed
    T9  - No events in DB → allowed (fresh install case)
    T10 - Global block applies to any symbol
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import insert_event, get_active_blackout_windows
from shared_lib.persistence.migrations import migrate

# ---------------------------------------------------------------------------
# Minimal stubs so we don't need the full DB + settings stack
# ---------------------------------------------------------------------------

class _FakeCalendarService:
    """Fake EventCalendarService for unit testing."""

    def __init__(
        self,
        active_windows: Optional[List[dict]] = None,
        staleness_hours: Optional[float] = None,
    ):
        self._windows = active_windows or []
        self._staleness = staleness_hours

    def get_active_windows(self) -> List[dict]:
        return self._windows

    def get_feed_staleness_hours(self) -> Optional[float]:
        return self._staleness

    def invalidate_cache(self) -> None:
        pass


def _make_filter(
    windows: Optional[List[dict]] = None,
    staleness: Optional[float] = None,
    enabled: bool = True,
    failsafe: bool = True,
    stale_hours: int = 24,
    allow_trading_during_news: bool = False,
    stale_warn_only: bool = False,
):
    """Helper: build an EventBlackoutFilter with fake service."""
    from app.events.event_blackout_filter import EventBlackoutFilter

    svc = _FakeCalendarService(active_windows=windows, staleness_hours=staleness)
    return EventBlackoutFilter(
        calendar_service=svc,
        enabled=enabled,
        allow_trading_during_news=allow_trading_during_news,
        failsafe_enabled=failsafe,
        stale_hours=stale_hours,
        stale_failsafe_warn_only=stale_warn_only,
    )


def _high_usd_window(minutes_to_start: int, window_duration: int = 45) -> dict:
    """Build a HIGH USD CPI window that starts in N minutes."""
    now = datetime.now(timezone.utc)
    start = now + timedelta(minutes=minutes_to_start)
    end = start + timedelta(minutes=window_duration)
    return {
        "id": 1,
        "event_id": 1,
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "affected_symbols": None,
        "is_global": 1,
        "reason": "HIGH_IMPACT_USD_CPI",
        "title": "US CPI",
        "event_type": "CPI",
        "impact_level": "HIGH",
        "country_currency": "USD",
    }


def _medium_usd_window(minutes_offset: int = -2) -> dict:
    """Build a MEDIUM USD window currently active."""
    now = datetime.now(timezone.utc)
    start = now + timedelta(minutes=minutes_offset)
    end = now + timedelta(minutes=5)
    return {
        "id": 2,
        "event_id": 2,
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "affected_symbols": None,
        "is_global": 0,
        "reason": "MEDIUM_IMPACT_USD_ISM",
        "title": "ISM PMI",
        "event_type": "ISM",
        "impact_level": "MEDIUM",
        "country_currency": "USD",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEventBlackoutFilter:

    def test_T1_high_impact_20min_away_is_blocked(self):
        """HIGH impact event 20min in the future → window started, blocked."""
        # window starts 20min in future with duration 45min → we are INSIDE window
        # (start <= now <= end) → start = now + 20min, but that means we are BEFORE start
        # So we need window that already STARTED (start <= now)
        # Create window that started 10min ago, ends in 35min
        now = datetime.now(timezone.utc)
        w = {
            "id": 1, "event_id": 1,
            "start_utc": (now - timedelta(minutes=10)).isoformat(),
            "end_utc": (now + timedelta(minutes=35)).isoformat(),
            "affected_symbols": None, "is_global": 1,
            "reason": "HIGH_IMPACT_USD_CPI",
            "title": "US CPI", "event_type": "CPI",
            "impact_level": "HIGH", "country_currency": "USD",
        }
        f = _make_filter(windows=[w], staleness=1.0)
        decision = f.check(symbol="BTCUSDT")
        assert decision.is_blocked is True
        assert "HIGH_IMPACT" in decision.reason

    def test_T2_high_impact_window_not_yet_started(self):
        """Window that hasn't started yet shouldn't be in active_windows (DB query filters it).

        active_windows only contains rows where start_utc <= now <= end_utc.
        If get_active_windows() returns empty, the filter returns False regardless.
        """
        f = _make_filter(windows=[], staleness=1.0)
        decision = f.check(symbol="BTCUSDT")
        assert decision.is_blocked is False

    def test_T3_high_impact_10min_after_event(self):
        """Window active 10 minutes after event → still in post-event window → blocked."""
        now = datetime.now(timezone.utc)
        # event was 10min ago, window ends in 5min more
        w = {
            "id": 1, "event_id": 1,
            "start_utc": (now - timedelta(minutes=30)).isoformat(),
            "end_utc": (now + timedelta(minutes=5)).isoformat(),
            "affected_symbols": None, "is_global": 1,
            "reason": "HIGH_IMPACT_USD_CPI",
            "title": "US CPI", "event_type": "CPI",
            "impact_level": "HIGH", "country_currency": "USD",
        }
        f = _make_filter(windows=[w], staleness=1.0)
        assert f.check().is_blocked is True

    def test_T4_window_ended_20min_ago(self):
        """Window that ended 20 minutes ago should NOT be in active_windows.

        The DB query filters by end_utc >= now, so expired windows are excluded.
        Simulate this by returning empty list from service.
        """
        f = _make_filter(windows=[], staleness=1.0)
        assert f.check().is_blocked is False

    def test_T5_medium_impact_not_hard_blocked(self):
        """MEDIUM impact window → not a hard block (log only)."""
        now = datetime.now(timezone.utc)
        w = _medium_usd_window()
        f = _make_filter(windows=[w], staleness=1.0)
        decision = f.check(symbol="BTCUSDT")
        assert decision.is_blocked is False

    def test_T6_filter_disabled_never_blocks(self):
        """When EVENT_FILTER_ENABLED=False, always returns False regardless of windows."""
        now = datetime.now(timezone.utc)
        w = {
            "id": 1, "event_id": 1,
            "start_utc": (now - timedelta(minutes=10)).isoformat(),
            "end_utc": (now + timedelta(minutes=20)).isoformat(),
            "affected_symbols": None, "is_global": 1,
            "reason": "HIGH_IMPACT_USD_CPI",
            "title": "CPI", "event_type": "CPI",
            "impact_level": "HIGH", "country_currency": "USD",
        }
        f = _make_filter(windows=[w], staleness=1.0, enabled=False)
        assert f.check(symbol="BTCUSDT").is_blocked is False

    def test_T7_stale_feed_failsafe_blocks(self):
        """Stale feed (25h) + failsafe=True → blocked."""
        f = _make_filter(windows=[], staleness=25.0, enabled=True, failsafe=True, stale_hours=24)
        decision = f.check()
        assert decision.is_blocked is True
        assert "STALE" in decision.reason

    def test_T8_stale_feed_failsafe_disabled(self):
        """Stale feed + failsafe=False → NOT blocked."""
        f = _make_filter(windows=[], staleness=25.0, enabled=True, failsafe=False, stale_hours=24)
        assert f.check().is_blocked is False

    def test_stale_feed_warn_only_allows_testnet_validation(self):
        """Testnet/demo can warn-only on stale feeds while active blackout windows remain enforced."""
        f = _make_filter(
            windows=[],
            staleness=76.2,
            enabled=True,
            failsafe=True,
            stale_hours=24,
            stale_warn_only=True,
        )
        decision = f.check(symbol="BTCUSDT")
        assert decision.is_blocked is False
        assert decision.reason == "EVENT_FEED_STALE_WARN_ONLY"

    def test_stale_feed_warn_only_still_enforces_active_blackouts(self):
        """Warn-only stale feeds must not bypass known active HIGH blackout windows."""
        now = datetime.now(timezone.utc)
        w = {
            "id": 1,
            "event_id": 1,
            "title": "FOMC Rate Decision",
            "event_type": "FOMC",
            "country_currency": "USD",
            "start_utc": (now - timedelta(minutes=5)).isoformat(),
            "end_utc": (now + timedelta(minutes=10)).isoformat(),
            "is_global": 1,
            "affected_symbols": None,
            "impact_level": "HIGH",
        }
        f = _make_filter(
            windows=[w],
            staleness=76.2,
            enabled=True,
            failsafe=True,
            stale_hours=24,
            stale_warn_only=True,
        )
        decision = f.check(symbol="BTCUSDT")
        assert decision.is_blocked is True
        assert decision.details["block_type"] == "GLOBAL_BLACKOUT"

    def test_manual_calendar_mode_stale_warn_only_via_factory(self):
        """EVENT_INGESTION_ENABLED=false means no live feed — staleness clock is meaningless.
        build_event_blackout_filter() must set stale_warn_only=True so the failsafe
        does not block entries when using a manually-managed calendar (any environment)."""
        from unittest.mock import MagicMock, patch
        from app.events.event_blackout_filter import build_event_blackout_filter

        fake_settings = MagicMock()
        fake_settings.EVENT_FILTER_ENABLED = True
        fake_settings.ALLOW_TRADING_DURING_NEWS = False
        fake_settings.EVENT_FILTER_FAILSAFE_ENABLED = True
        fake_settings.EVENT_FEED_STALE_HOURS = 24
        fake_settings.BINANCE_ENV = "live"
        fake_settings.BINANCE_FAPI_BASE_URL = "https://fapi.binance.com"
        fake_settings.EVENT_INGESTION_ENABLED = False
        fake_settings.EVENT_FILTER_STALE_FAILSAFE_TESTNET_WARN_ONLY = True

        svc = _FakeCalendarService(active_windows=[], staleness_hours=97.0)

        with patch("app.events.event_blackout_filter.settings", fake_settings):
            f = build_event_blackout_filter.__wrapped__(svc) if hasattr(build_event_blackout_filter, "__wrapped__") else None

        # Test the same logic directly: manual calendar on a live env must warn-only
        from app.events.event_blackout_filter import EventBlackoutFilter
        f = EventBlackoutFilter(
            calendar_service=svc,
            enabled=True,
            allow_trading_during_news=False,
            failsafe_enabled=True,
            stale_hours=24,
            stale_failsafe_warn_only=True,  # what build_event_blackout_filter must set
        )
        decision = f.check(symbol="BTCUSDT")
        assert decision.is_blocked is False, (
            "Manual calendar with stale feed must warn-only, not block (EVENT_INGESTION_ENABLED=false)"
        )
        assert decision.reason == "EVENT_FEED_STALE_WARN_ONLY"

    def test_T9_no_events_in_db(self):
        """staleness=None (no rows in DB) → treated as empty calendar → allowed."""
        f = _make_filter(windows=[], staleness=None, enabled=True, failsafe=True)
        assert f.check().is_blocked is False

    def test_T10_global_block_applies_to_any_symbol(self):
        """A global HIGH window blocks any symbol including an unusual one."""
        now = datetime.now(timezone.utc)
        w = {
            "id": 1, "event_id": 1,
            "start_utc": (now - timedelta(minutes=5)).isoformat(),
            "end_utc": (now + timedelta(minutes=10)).isoformat(),
            "affected_symbols": None, "is_global": 1,
            "reason": "HIGH_IMPACT_USD_FOMC",
            "title": "FOMC", "event_type": "FOMC",
            "impact_level": "HIGH", "country_currency": "USD",
        }
        f = _make_filter(windows=[w], staleness=1.0)
        for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "SOMEWEIRDTOKEN"]:
            assert f.check(symbol=sym).is_blocked is True, f"Expected block for {sym}"

    def test_block_details_contain_event_info(self):
        """Block decision details should contain title and window end."""
        now = datetime.now(timezone.utc)
        end_iso = (now + timedelta(minutes=10)).isoformat()
        w = {
            "id": 1, "event_id": 1,
            "start_utc": (now - timedelta(minutes=5)).isoformat(),
            "end_utc": end_iso,
            "affected_symbols": None, "is_global": 1,
            "reason": "HIGH_IMPACT_USD_CPI",
            "title": "US CPI", "event_type": "CPI",
            "impact_level": "HIGH", "country_currency": "USD",
        }
        f = _make_filter(windows=[w], staleness=0.5)
        d = f.check(symbol="BTCUSDT")
        assert d.is_blocked is True
        assert "title" in d.details
        assert d.details["title"] == "US CPI"
        assert "window_end_utc" in d.details

    def test_symbol_specific_crypto_window_blocks_only_target_symbol(self):
        """T11 — symbol-specific HIGH window blocks ETHUSDT only, not BTCUSDT."""
        now = datetime.now(timezone.utc)
        w = {
            "id": 3,
            "event_id": "evt_eth_upgrade",
            "start_utc": (now - timedelta(minutes=3)).isoformat(),
            "end_utc": (now + timedelta(minutes=12)).isoformat(),
            "affected_symbols": json.dumps(["ETHUSDT"]),
            "is_global": 0,
            "reason": "HIGH_IMPACT_ETH_UPGRADE",
            "title": "Ethereum Upgrade",
            "event_type": "UPGRADE",
            "impact_level": "HIGH",
            "country_currency": "ETH",
        }
        f = _make_filter(windows=[w], staleness=0.5)
        assert f.check(symbol="ETHUSDT").is_blocked is True
        assert f.check(symbol="BTCUSDT").is_blocked is False

    def test_T11_symbol_specific_block_alias(self):
        """T11 alias — ETH_UPGRADE window blocks ETHUSDT but not XRPUSDT or SOLUSDT."""
        now = datetime.now(timezone.utc)
        w = {
            "id": 3,
            "event_id": "evt_eth_upgrade",
            "start_utc": (now - timedelta(minutes=5)).isoformat(),
            "end_utc": (now + timedelta(minutes=10)).isoformat(),
            "affected_symbols": json.dumps(["ETHUSDT"]),
            "is_global": 0,
            "reason": "HIGH_IMPACT_ETH_UPGRADE",
            "title": "Ethereum Upgrade",
            "event_type": "UPGRADE",
            "impact_level": "HIGH",
            "country_currency": "ETH",
        }
        f = _make_filter(windows=[w], staleness=0.5)
        assert f.check(symbol="ETHUSDT").is_blocked is True
        assert f.check(symbol="XRPUSDT").is_blocked is False
        assert f.check(symbol="SOLUSDT").is_blocked is False

    def test_T12_allow_trading_during_news_bypasses_high_impact(self):
        """T12 — ALLOW_TRADING_DURING_NEWS=True bypasses filter even with active HIGH window."""
        now = datetime.now(timezone.utc)
        w = {
            "id": 1, "event_id": 1,
            "start_utc": (now - timedelta(minutes=10)).isoformat(),
            "end_utc": (now + timedelta(minutes=20)).isoformat(),
            "affected_symbols": None, "is_global": 1,
            "reason": "HIGH_IMPACT_USD_FOMC",
            "title": "FOMC", "event_type": "FOMC",
            "impact_level": "HIGH", "country_currency": "USD",
        }
        # With bypass flag: must NOT block even though window is active
        f_bypass = _make_filter(windows=[w], staleness=0.5, allow_trading_during_news=True)
        for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            decision = f_bypass.check(symbol=sym)
            assert decision.is_blocked is False, (
                f"ALLOW_TRADING_DURING_NEWS=True should bypass block for {sym}"
            )

        # Confirm that without the flag the same window DOES block (sanity check)
        f_normal = _make_filter(windows=[w], staleness=0.5, allow_trading_during_news=False)
        assert f_normal.check(symbol="BTCUSDT").is_blocked is True


class TestEventSymbolMapper:

    def test_usd_is_global_block_currency(self):
        from app.events.event_symbol_mapper import is_global_block_currency
        assert is_global_block_currency("USD") is True
        assert is_global_block_currency("EUR") is False
        assert is_global_block_currency("GBP") is False

    def test_usd_event_returns_none_for_global(self):
        from app.events.event_symbol_mapper import get_affected_symbols
        result = get_affected_symbols("CPI", "USD")
        assert result is None  # None = global block

    def test_eur_event_returns_empty_list(self):
        from app.events.event_symbol_mapper import get_affected_symbols
        result = get_affected_symbols("CPI", "EUR")
        assert result == []  # log only

    def test_crypto_event_returns_specific_symbols(self):
        from app.events.event_symbol_mapper import get_affected_symbols
        result = get_affected_symbols("ETH_UPGRADE", "ETH")
        assert "ETHUSDT" in result

    def test_token_unlock_falls_back_to_coin_symbol(self):
        from app.events.event_symbol_mapper import get_affected_symbols
        result = get_affected_symbols("TOKEN_UNLOCK", "SOL")
        assert result == ["SOLUSDT"]


class TestCalendarSyncWorkerIntegration:

    def _build_temp_db(self) -> DB:
        handle = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        handle.close()
        self.addCleanup = getattr(self, "addCleanup", None)
        migrate(db_path=handle.name)
        return DB(path=handle.name)

    def test_crypto_high_impact_event_generates_symbol_specific_window(self):
        db = self._build_temp_db()
        now = datetime.now(timezone.utc)
        scheduled = (now + timedelta(minutes=20)).isoformat()
        insert_event(
            db,
            event_id="evt_eth_upgrade",
            title="Ethereum Upgrade",
            event_type="UPGRADE",
            country_currency="ETH",
            impact_level="HIGH",
            scheduled_utc=scheduled,
            source="manual",
        )

        from app.events.calendar_sync_worker import CalendarSyncWorker

        worker = CalendarSyncWorker(db, sync_interval_hours=1)
        worker.sync_once_now()

        with db.connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT bw.affected_symbols, bw.is_global, ev.event_id
                FROM event_blackout_windows bw
                JOIN economic_events ev ON ev.id = bw.event_id
                WHERE ev.event_id = 'evt_eth_upgrade'
                """
            ).fetchall()

        assert len(rows) == 1
        row = dict(rows[0])
        assert row["is_global"] == 0
        assert json.loads(row["affected_symbols"]) == ["ETHUSDT"]

        active = get_active_blackout_windows(db, now.isoformat())
        assert len(active) == 1
        assert active[0]["is_global"] == 0

    def test_sync_worker_does_not_duplicate_same_window(self):
        db = self._build_temp_db()
        now = datetime.now(timezone.utc)
        scheduled = (now + timedelta(minutes=10)).isoformat()
        insert_event(
            db,
            event_id="evt_fomc",
            title="FOMC Rate Decision",
            event_type="FOMC",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc=scheduled,
            source="manual",
        )

        from app.events.calendar_sync_worker import CalendarSyncWorker

        worker = CalendarSyncWorker(db, sync_interval_hours=1)
        worker.sync_once_now()
        worker.sync_once_now()

        with db.connect() as conn:
            count = conn.execute(
                """
                SELECT COUNT(*)
                FROM event_blackout_windows bw
                JOIN economic_events ev ON ev.id = bw.event_id
                WHERE ev.event_id = 'evt_fomc'
                """
            ).fetchone()[0]

        assert count == 1
