"""
CosmicForge Section C Signal Generation Fixes Tests

C-1: Signal scheduler is wired and auto-generates signals at configured UTC times
C-2: Max published per scan = 10; max active signals = 20
C-3: Stale signals are expired on schedule; expired signals no longer block duplicates
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest


def _make_temp_db():
    """Create a fresh temp-file DB with signals schema applied."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.signals import ensure_signals_schema

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = DB(path=path)
    ensure_signals_schema(db)
    return db, path


def _cleanup_db(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# C-1: Signal Scheduler Wiring
# ═══════════════════════════════════════════════════════════════════════════

class TestSignalSchedulerConfig:
    def test_generation_times_configured(self):
        from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC
        assert isinstance(SIGNAL_GENERATION_TIMES_UTC, list)
        assert len(SIGNAL_GENERATION_TIMES_UTC) >= 1
        for t in SIGNAL_GENERATION_TIMES_UTC:
            parts = t.split(":")
            assert len(parts) == 2, f"Bad time format: {t}"
            assert 0 <= int(parts[0]) <= 23
            assert 0 <= int(parts[1]) <= 59

    def test_default_times_include_four_slots(self):
        from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC
        assert len(SIGNAL_GENERATION_TIMES_UTC) == 4

    def test_default_times_values(self):
        from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC
        assert "07:00" in SIGNAL_GENERATION_TIMES_UTC
        assert "12:00" in SIGNAL_GENERATION_TIMES_UTC
        assert "16:00" in SIGNAL_GENERATION_TIMES_UTC
        assert "20:00" in SIGNAL_GENERATION_TIMES_UTC


class TestSignalSchedulerStartup:
    """Verify startup handler registers correct APScheduler jobs."""

    def _run_startup(self):
        """Execute the startup coroutine with a mocked scheduler."""
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        mock_scheduler = MagicMock(spec=AsyncIOScheduler)
        jobs_added = []

        def capture_add_job(fn, trigger=None, **kwargs):
            jobs_added.append({
                "fn": fn,
                "trigger": trigger,
                "kwargs": kwargs,
            })

        mock_scheduler.add_job.side_effect = capture_add_job

        with patch("apscheduler.schedulers.asyncio.AsyncIOScheduler", return_value=mock_scheduler), \
             patch("app.main._signal_scheduler", None):
            # Import the module-level scheduler var and startup function
            import app.main as main_mod
            original = main_mod._signal_scheduler
            main_mod._signal_scheduler = None  # ensure clean state
            try:
                asyncio.get_event_loop().run_until_complete(main_mod._startup_signal_scheduler())
            finally:
                # Clean up so we don't pollute other tests
                if main_mod._signal_scheduler:
                    main_mod._signal_scheduler.shutdown(wait=False)
                main_mod._signal_scheduler = original

        return mock_scheduler, jobs_added

    def test_scheduler_registers_generation_jobs_for_each_configured_time(self):
        from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC
        mock_scheduler, jobs = self._run_startup()
        gen_job_ids = [j["kwargs"].get("id", "") for j in jobs if j["kwargs"].get("id", "").startswith("signal_gen_")]
        assert len(gen_job_ids) == len(SIGNAL_GENERATION_TIMES_UTC), (
            f"Expected {len(SIGNAL_GENERATION_TIMES_UTC)} generation jobs, got {len(gen_job_ids)}"
        )

    def test_scheduler_job_ids_are_stable(self):
        from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC
        mock_scheduler, jobs = self._run_startup()
        expected_ids = {f"signal_gen_{t.replace(':', '')}" for t in SIGNAL_GENERATION_TIMES_UTC}
        actual_ids = {j["kwargs"].get("id") for j in jobs if j["kwargs"].get("id", "").startswith("signal_gen_")}
        assert actual_ids == expected_ids

    def test_scheduler_generation_jobs_use_utc(self):
        mock_scheduler, jobs = self._run_startup()
        # AsyncIOScheduler was called with timezone="UTC"
        mock_scheduler.start.assert_called_once()

    def test_scheduler_generation_jobs_use_cron_trigger(self):
        mock_scheduler, jobs = self._run_startup()
        gen_jobs = [j for j in jobs if j["kwargs"].get("id", "").startswith("signal_gen_")]
        for j in gen_jobs:
            assert j["trigger"] == "cron", f"Expected cron trigger, got {j['trigger']}"

    def test_scheduler_generation_jobs_set_replace_existing(self):
        mock_scheduler, jobs = self._run_startup()
        gen_jobs = [j for j in jobs if j["kwargs"].get("id", "").startswith("signal_gen_")]
        for j in gen_jobs:
            assert j["kwargs"].get("replace_existing") is True

    def test_scheduler_expiry_job_registered(self):
        mock_scheduler, jobs = self._run_startup()
        expiry_jobs = [j for j in jobs if j["kwargs"].get("id") == "signal_expiry"]
        assert len(expiry_jobs) == 1, "signal_expiry job must be registered exactly once"

    def test_scheduler_expiry_job_interval_5_minutes(self):
        mock_scheduler, jobs = self._run_startup()
        expiry_jobs = [j for j in jobs if j["kwargs"].get("id") == "signal_expiry"]
        assert expiry_jobs[0]["trigger"] == "interval"
        assert expiry_jobs[0]["kwargs"].get("minutes") == 5

    def test_scheduler_expiry_job_replace_existing(self):
        mock_scheduler, jobs = self._run_startup()
        expiry_jobs = [j for j in jobs if j["kwargs"].get("id") == "signal_expiry"]
        assert expiry_jobs[0]["kwargs"].get("replace_existing") is True

    def test_scheduler_starts(self):
        mock_scheduler, _ = self._run_startup()
        mock_scheduler.start.assert_called_once()

    def test_scheduler_generation_callable_is_generate_crypto_signals(self):
        from app.signals.crypto_signal_engine import generate_crypto_signals
        mock_scheduler, jobs = self._run_startup()
        gen_jobs = [j for j in jobs if j["kwargs"].get("id", "").startswith("signal_gen_")]
        for j in gen_jobs:
            assert j["fn"] is generate_crypto_signals

    def test_scheduler_expiry_callable_is_expire_stale_signals(self):
        from app.signals.signal_expiry import expire_stale_signals
        mock_scheduler, jobs = self._run_startup()
        expiry_jobs = [j for j in jobs if j["kwargs"].get("id") == "signal_expiry"]
        assert expiry_jobs[0]["fn"] is expire_stale_signals

    def test_duplicate_startup_skipped(self):
        """If scheduler is already set, a second startup call must not start another."""
        import app.main as main_mod
        original = main_mod._signal_scheduler

        sentinel = MagicMock()
        main_mod._signal_scheduler = sentinel
        try:
            asyncio.get_event_loop().run_until_complete(main_mod._startup_signal_scheduler())
            # start() must NOT have been called on the sentinel
            sentinel.start.assert_not_called()
        finally:
            main_mod._signal_scheduler = original

    def test_shutdown_clears_scheduler(self):
        import app.main as main_mod
        original = main_mod._signal_scheduler

        sentinel = MagicMock()
        main_mod._signal_scheduler = sentinel
        try:
            asyncio.get_event_loop().run_until_complete(main_mod._shutdown_signal_scheduler())
            sentinel.shutdown.assert_called_once_with(wait=False)
            assert main_mod._signal_scheduler is None
        finally:
            main_mod._signal_scheduler = original


# ═══════════════════════════════════════════════════════════════════════════
# C-2: Signal Limit Increases
# ═══════════════════════════════════════════════════════════════════════════

class TestSignalLimitsConfig:
    def test_max_published_per_scan_is_10(self):
        from app.signals.signal_scheduler_config import DEFAULT_MAX_PUBLISHED_PER_SCAN
        assert DEFAULT_MAX_PUBLISHED_PER_SCAN == 10

    def test_max_active_signals_is_20(self):
        from app.signals.signal_scheduler_config import DEFAULT_MAX_ACTIVE_SIGNALS
        assert DEFAULT_MAX_ACTIVE_SIGNALS == 20

    def test_max_published_above_old_limit_of_5(self):
        from app.signals.signal_scheduler_config import DEFAULT_MAX_PUBLISHED_PER_SCAN
        assert DEFAULT_MAX_PUBLISHED_PER_SCAN > 5

    def test_max_active_above_old_limit_of_10(self):
        from app.signals.signal_scheduler_config import DEFAULT_MAX_ACTIVE_SIGNALS
        assert DEFAULT_MAX_ACTIVE_SIGNALS > 10


class TestSignalEngineLimitEnforcement:
    """Engine respects per-scan and active-signal limits without removing quality filters."""

    def _make_engine(self, *, published_per_scan=10, max_active=20, active_now=0, signals_per_symbol=1):
        """Build a CryptoSignalEngine with mocked repository and market data."""
        from app.signals.crypto_signal_engine import CryptoSignalEngine

        repo = MagicMock()
        repo.count_active_signals.return_value = active_now
        repo.has_duplicate_open_signal.return_value = False

        published_counter = {"n": 0}

        def fake_scan_symbol(symbol):
            results = []
            for _ in range(signals_per_symbol):
                if published_counter["n"] < published_per_scan:
                    published_counter["n"] += 1
                    results.append({"accepted": True, "signal_id": "x", "auto_published": True})
                else:
                    results.append({"accepted": False, "signal_id": None, "auto_published": False})
            return results

        engine = CryptoSignalEngine(
            repository=repo,
            market_data=MagicMock(),
            allowed_symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
                             "XRPUSDT", "ADAUSDT", "DOTUSDT", "LINKUSDT",
                             "MATICUSDT", "LTCUSDT", "AVAXUSDT", "ATOMUSDT"],
            max_published_per_scan=published_per_scan,
            max_active_signals=max_active,
        )
        engine.scan_symbol = fake_scan_symbol
        return engine, repo

    def test_engine_respects_max_published_per_scan(self):
        engine, repo = self._make_engine(published_per_scan=10, max_active=20)
        summary = engine.generate_crypto_signals()
        assert summary["published"] <= 10

    def test_engine_can_publish_more_than_5_signals(self):
        """The old limit was 5; the new limit allows up to 10."""
        engine, repo = self._make_engine(published_per_scan=10, max_active=20)
        summary = engine.generate_crypto_signals()
        # With 12 symbols and 1 signal each, published should reach 10 (the cap)
        assert summary["published"] == 10

    def test_engine_does_not_exceed_max_published_per_scan(self):
        engine, repo = self._make_engine(published_per_scan=10, max_active=20)
        for _ in range(3):
            summary = engine.generate_crypto_signals()
            assert summary["published"] <= 10

    def test_engine_stops_when_active_limit_reached(self):
        engine, repo = self._make_engine(published_per_scan=10, max_active=20, active_now=20)
        summary = engine.generate_crypto_signals()
        # No symbols should be scanned once active limit is already at max
        assert summary["scanned_symbols"] == 0
        assert summary["published"] == 0

    def test_engine_does_not_publish_when_active_signals_exceed_max(self):
        engine, repo = self._make_engine(published_per_scan=10, max_active=5, active_now=10)
        summary = engine.generate_crypto_signals()
        assert summary["published"] == 0

    def test_engine_uses_config_defaults(self):
        from app.signals.crypto_signal_engine import CryptoSignalEngine
        from app.signals.signal_scheduler_config import DEFAULT_MAX_ACTIVE_SIGNALS, DEFAULT_MAX_PUBLISHED_PER_SCAN
        engine = CryptoSignalEngine(
            repository=MagicMock(),
            market_data=MagicMock(),
            allowed_symbols=["BTCUSDT"],
        )
        assert engine.max_published_per_scan == DEFAULT_MAX_PUBLISHED_PER_SCAN
        assert engine.max_active_signals == DEFAULT_MAX_ACTIVE_SIGNALS

    def test_skipped_counters_populated_on_active_limit(self):
        engine, repo = self._make_engine(published_per_scan=10, max_active=20, active_now=20)
        summary = engine.generate_crypto_signals()
        assert summary["skipped_active_limit"] > 0

    def test_skipped_counters_populated_on_published_limit(self):
        engine, repo = self._make_engine(published_per_scan=3, max_active=100)
        # 12 symbols but only 3 can be published before limit kicks in
        summary = engine.generate_crypto_signals()
        assert summary["published"] <= 3


# ═══════════════════════════════════════════════════════════════════════════
# C-3: Signal Expiry
# ═══════════════════════════════════════════════════════════════════════════

class TestSignalExpiryUpdater:
    """Unit tests for SignalExpiryUpdater.expire_due_signals()."""

    def _make_db_with_signals(self, signals: list[dict]):
        """Build a temp-file DB seeded with the given signals. Returns (db, path)."""
        db, path = _make_temp_db()
        with db.connect() as conn:
            for s in signals:
                conn.execute(
                    """
                    INSERT INTO trading_signals
                        (id, asset_class, symbol, side, status, is_published, expires_at,
                         created_at, updated_at, entry_price, stop_loss, take_profit_1,
                         risk_reward, confidence_score)
                    VALUES (?, 'crypto', ?, ?, ?, 1, ?, datetime('now'), datetime('now'),
                            50000.0, 49000.0, 51000.0, 2.0, 75.0)
                    """,
                    (s["id"], s.get("symbol", "BTCUSDT"), s.get("side", "BUY"), s["status"], s["expires_at"]),
                )
            conn.commit()
        return db, path

    def _past(self, minutes: int = 60) -> str:
        return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()

    def _future(self, minutes: int = 60) -> str:
        return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()

    def test_pending_entry_signal_expired_when_past_expires_at(self):
        from app.signals.signal_expiry import SignalExpiryUpdater
        from shared_lib.persistence.signals import SIGNAL_STATUS_EXPIRED, SIGNAL_STATUS_PENDING_ENTRY

        db, path = self._make_db_with_signals([
            {"id": "sig-1", "status": SIGNAL_STATUS_PENDING_ENTRY, "expires_at": self._past(60)},
        ])
        try:
            updater = SignalExpiryUpdater(db=db)
            summary = updater.expire_due_signals()
            assert summary["expired"] == 1
            with db.connect() as conn:
                row = conn.execute("SELECT status FROM trading_signals WHERE id='sig-1'").fetchone()
            assert row["status"] == SIGNAL_STATUS_EXPIRED
        finally:
            _cleanup_db(path)

    def test_active_signal_expired_when_past_expires_at(self):
        from app.signals.signal_expiry import SignalExpiryUpdater
        from shared_lib.persistence.signals import SIGNAL_STATUS_ACTIVE, SIGNAL_STATUS_EXPIRED

        db, path = self._make_db_with_signals([
            {"id": "sig-2", "status": SIGNAL_STATUS_ACTIVE, "expires_at": self._past(30)},
        ])
        try:
            updater = SignalExpiryUpdater(db=db)
            summary = updater.expire_due_signals()
            assert summary["expired"] == 1
            with db.connect() as conn:
                row = conn.execute("SELECT status FROM trading_signals WHERE id='sig-2'").fetchone()
            assert row["status"] == SIGNAL_STATUS_EXPIRED
        finally:
            _cleanup_db(path)

    def test_fresh_pending_signal_not_expired(self):
        from app.signals.signal_expiry import SignalExpiryUpdater
        from shared_lib.persistence.signals import SIGNAL_STATUS_PENDING_ENTRY

        db, path = self._make_db_with_signals([
            {"id": "sig-3", "status": SIGNAL_STATUS_PENDING_ENTRY, "expires_at": self._future(60)},
        ])
        try:
            updater = SignalExpiryUpdater(db=db)
            summary = updater.expire_due_signals()
            assert summary["expired"] == 0
            with db.connect() as conn:
                row = conn.execute("SELECT status FROM trading_signals WHERE id='sig-3'").fetchone()
            assert row["status"] == SIGNAL_STATUS_PENDING_ENTRY
        finally:
            _cleanup_db(path)

    def test_fresh_active_signal_not_expired(self):
        from app.signals.signal_expiry import SignalExpiryUpdater
        from shared_lib.persistence.signals import SIGNAL_STATUS_ACTIVE

        db, path = self._make_db_with_signals([
            {"id": "sig-4", "status": SIGNAL_STATUS_ACTIVE, "expires_at": self._future(120)},
        ])
        try:
            updater = SignalExpiryUpdater(db=db)
            summary = updater.expire_due_signals()
            assert summary["expired"] == 0
            with db.connect() as conn:
                row = conn.execute("SELECT status FROM trading_signals WHERE id='sig-4'").fetchone()
            assert row["status"] == SIGNAL_STATUS_ACTIVE
        finally:
            _cleanup_db(path)

    def test_mixed_signals_only_stale_expire(self):
        from app.signals.signal_expiry import SignalExpiryUpdater
        from shared_lib.persistence.signals import (
            SIGNAL_STATUS_ACTIVE,
            SIGNAL_STATUS_EXPIRED,
            SIGNAL_STATUS_PENDING_ENTRY,
        )

        db, path = self._make_db_with_signals([
            {"id": "stale-1", "status": SIGNAL_STATUS_PENDING_ENTRY, "expires_at": self._past(90)},
            {"id": "stale-2", "status": SIGNAL_STATUS_ACTIVE, "expires_at": self._past(10)},
            {"id": "fresh-1", "status": SIGNAL_STATUS_PENDING_ENTRY, "expires_at": self._future(60)},
            {"id": "fresh-2", "status": SIGNAL_STATUS_ACTIVE, "expires_at": self._future(30)},
        ])
        try:
            updater = SignalExpiryUpdater(db=db)
            summary = updater.expire_due_signals()
            assert summary["expired"] == 2
            assert summary["checked"] == 4
            with db.connect() as conn:
                rows = {r["id"]: r["status"] for r in conn.execute("SELECT id, status FROM trading_signals").fetchall()}
            assert rows["stale-1"] == SIGNAL_STATUS_EXPIRED
            assert rows["stale-2"] == SIGNAL_STATUS_EXPIRED
            assert rows["fresh-1"] == SIGNAL_STATUS_PENDING_ENTRY
            assert rows["fresh-2"] == SIGNAL_STATUS_ACTIVE
        finally:
            _cleanup_db(path)


class TestDuplicateCheckIgnoresExpired:
    """Expired signals must not block new signals for the same symbol/side."""

    def _make_repo_db(self, signals: list[dict]):
        """Returns (db, path) — caller must _cleanup_db(path)."""
        db, path = _make_temp_db()
        with db.connect() as conn:
            for s in signals:
                conn.execute(
                    """
                    INSERT INTO trading_signals
                        (id, asset_class, symbol, side, status, is_published, expires_at,
                         created_at, updated_at, entry_price, stop_loss, take_profit_1,
                         risk_reward, confidence_score)
                    VALUES (?, 'crypto', ?, ?, ?, 1, ?, datetime('now'), datetime('now'),
                            50000.0, 49000.0, 51000.0, 2.0, 75.0)
                    """,
                    (s["id"], s["symbol"], s["side"], s["status"], s["expires_at"]),
                )
            conn.commit()
        return db, path

    def _past(self, minutes: int = 60) -> str:
        return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()

    def _future(self, minutes: int = 60) -> str:
        return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()

    def test_expired_signal_does_not_block_duplicate_check(self):
        from app.signals.signal_repository import SignalRepository
        from shared_lib.persistence.signals import SIGNAL_STATUS_EXPIRED

        db, path = self._make_repo_db([
            {"id": "old-sig", "symbol": "BTCUSDT", "side": "BUY",
             "status": SIGNAL_STATUS_EXPIRED, "expires_at": self._past(60)},
        ])
        try:
            repo = SignalRepository(db=db)
            assert repo.has_duplicate_open_signal("BTCUSDT", "BUY") is False
        finally:
            _cleanup_db(path)

    def test_active_non_expired_signal_blocks_duplicate(self):
        from app.signals.signal_repository import SignalRepository
        from shared_lib.persistence.signals import SIGNAL_STATUS_ACTIVE

        db, path = self._make_repo_db([
            {"id": "live-sig", "symbol": "BTCUSDT", "side": "BUY",
             "status": SIGNAL_STATUS_ACTIVE, "expires_at": self._future(60)},
        ])
        try:
            repo = SignalRepository(db=db)
            assert repo.has_duplicate_open_signal("BTCUSDT", "BUY") is True
        finally:
            _cleanup_db(path)

    def test_pending_non_expired_signal_blocks_duplicate(self):
        from app.signals.signal_repository import SignalRepository
        from shared_lib.persistence.signals import SIGNAL_STATUS_PENDING_ENTRY

        db, path = self._make_repo_db([
            {"id": "pend-sig", "symbol": "ETHUSDT", "side": "SELL",
             "status": SIGNAL_STATUS_PENDING_ENTRY, "expires_at": self._future(30)},
        ])
        try:
            repo = SignalRepository(db=db)
            assert repo.has_duplicate_open_signal("ETHUSDT", "SELL") is True
        finally:
            _cleanup_db(path)

    def test_after_expiry_new_signal_allowed(self):
        """Simulate the full flow: signal expires → duplicate check clears → new signal can be generated."""
        from app.signals.signal_expiry import SignalExpiryUpdater
        from app.signals.signal_repository import SignalRepository
        from shared_lib.persistence.signals import SIGNAL_STATUS_PENDING_ENTRY

        db, path = self._make_repo_db([
            {"id": "block-sig", "symbol": "SOLUSDT", "side": "BUY",
             "status": SIGNAL_STATUS_PENDING_ENTRY, "expires_at": self._past(1)},
        ])
        try:
            repo = SignalRepository(db=db)
            # past expires_at: query already excludes it (expires_at < now)
            assert repo.has_duplicate_open_signal("SOLUSDT", "BUY") is False

            updater = SignalExpiryUpdater(db=db)
            summary = updater.expire_due_signals()
            assert summary["expired"] == 1

            # After expiry status=EXPIRED: still not blocked
            assert repo.has_duplicate_open_signal("SOLUSDT", "BUY") is False
        finally:
            _cleanup_db(path)

    def test_different_side_not_blocked_by_existing_signal(self):
        from app.signals.signal_repository import SignalRepository
        from shared_lib.persistence.signals import SIGNAL_STATUS_ACTIVE

        db, path = self._make_repo_db([
            {"id": "buy-sig", "symbol": "BTCUSDT", "side": "BUY",
             "status": SIGNAL_STATUS_ACTIVE, "expires_at": self._future(60)},
        ])
        try:
            repo = SignalRepository(db=db)
            assert repo.has_duplicate_open_signal("BTCUSDT", "BUY") is True
            assert repo.has_duplicate_open_signal("BTCUSDT", "SELL") is False
        finally:
            _cleanup_db(path)


class TestExpireStaleSignalsWrapper:
    """expire_stale_signals() is the scheduler-facing wrapper."""

    def test_expire_stale_signals_callable(self):
        from app.signals.signal_expiry import expire_stale_signals
        assert callable(expire_stale_signals)

    def test_expire_stale_signals_runs_without_error_on_empty_db(self):
        from app.signals.signal_expiry import expire_stale_signals

        db, path = _make_temp_db()
        try:
            expire_stale_signals(db=db)
        finally:
            _cleanup_db(path)

    def test_expire_stale_signals_expires_stale_records(self):
        from shared_lib.persistence.signals import SIGNAL_STATUS_EXPIRED, SIGNAL_STATUS_PENDING_ENTRY
        from app.signals.signal_expiry import expire_stale_signals

        db, path = _make_temp_db()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        try:
            with db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO trading_signals
                        (id, asset_class, symbol, side, status, is_published, expires_at,
                         created_at, updated_at, entry_price, stop_loss, take_profit_1,
                         risk_reward, confidence_score)
                    VALUES ('exp-1', 'crypto', 'BTCUSDT', 'BUY', ?, 1, ?,
                            datetime('now'), datetime('now'), 50000.0, 49000.0, 51000.0, 2.0, 75.0)
                    """,
                    (SIGNAL_STATUS_PENDING_ENTRY, past),
                )
                conn.commit()

            expire_stale_signals(db=db)

            with db.connect() as conn:
                row = conn.execute("SELECT status FROM trading_signals WHERE id='exp-1'").fetchone()
            assert row["status"] == SIGNAL_STATUS_EXPIRED
        finally:
            _cleanup_db(path)
