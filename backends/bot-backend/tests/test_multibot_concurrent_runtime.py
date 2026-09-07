"""
CosmicForge Bot Backend — Multi-bot concurrent runtime validation (SQLite)

Goal: prove multi-tenant isolation holds under concurrent load with 10+ bots
sharing the same SQLite DB file.

This test is intentionally "runtime-style": it performs concurrent reads/writes
to the shared durable tables used by AdaptiveEngine, StateStore snapshots, and
decision logging, and asserts:
  - no cross-bot contamination
  - no cross-bot overwrites
  - no unexpected SQLite lock failures (WAL + busy_timeout expected)
"""

from __future__ import annotations

import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone, date

import pytest


def _make_temp_db():
    """Return (DB, path) backed by a temp file with full schema applied."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    migrate(db_path=path)
    return DB(path=path), path


def _cleanup(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TestMultiBotConcurrentRuntime:
    def setup_method(self):
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        _cleanup(self.path)

    def test_10_bots_concurrent_writes_and_isolation(self):
        from app.adaptive.engine import AdaptiveEngine
        from app.risk.circuit import CircuitBreakerRegistry
        from app.risk.state import PeriodSnapshot
        from shared_lib.persistence.state_store import StateStore

        # ── WAL / busy_timeout sanity (recommendation requirement) ───────────
        with self.db.connect() as conn:
            journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            busy_timeout = conn.execute("PRAGMA busy_timeout;").fetchone()[0]
        assert str(journal_mode).lower() == "wal"
        assert int(busy_timeout) >= 5000

        bot_ids = [f"bot{i:02d}" for i in range(10)]
        run_ids = {b: f"run-{b}-{uuid.uuid4().hex[:8]}" for b in bot_ids}

        # Scenario 2 — Circuit breaker isolation (in-memory registry)
        registry = CircuitBreakerRegistry()
        registry.reset_all()
        key_a = f"{bot_ids[0]}:broker-uuid-001"
        key_b = f"{bot_ids[1]}:broker-uuid-001"
        breaker_a = registry.get_breaker(key_a, error_limit=3)
        for _ in range(5):
            breaker_a.record_error()
        assert registry.is_tripped(key_a) is True
        assert registry.is_tripped(key_b) is False

        # Scenario 3 — Snapshot isolation (same period dates across bots)
        week = date(2024, 1, 1)
        month = date(2024, 1, 1)
        for i, bot_id in enumerate(bot_ids):
            store = StateStore(self.db, bot_instance_id=bot_id)
            base = 1000.0 + i * 100.0
            store.save_weekly_snapshot(PeriodSnapshot(week, base, base + 10.0, base - 5.0))
            store.save_monthly_snapshot(PeriodSnapshot(month, base, base + 20.0, base - 10.0))
        for i, bot_id in enumerate(bot_ids):
            store = StateStore(self.db, bot_instance_id=bot_id)
            snap_w = store.load_weekly_snapshot(week)
            snap_m = store.load_monthly_snapshot(month)
            assert snap_w is not None and snap_m is not None
            assert snap_w.start_equity == 1000.0 + i * 100.0
            assert snap_m.start_equity == 1000.0 + i * 100.0

        # Scenario 5 — Decision log isolation via exec failure rate
        # Insert Bot00 blocked decisions only; others should remain unaffected.
        now = _utc_now_iso()
        with self.db.connect() as conn:
            for _ in range(10):
                conn.execute(
                    """
                    INSERT INTO decision_logs (id, config_id, run_id, symbol, final_action, created_at)
                    VALUES (?, ?, ?, 'BTCUSDT', 'blocked', ?)
                    """,
                    (str(uuid.uuid4()), bot_ids[0], run_ids[bot_ids[0]], now),
                )
            for b in bot_ids[1:]:
                for _ in range(10):
                    conn.execute(
                        """
                        INSERT INTO decision_logs (id, config_id, run_id, symbol, final_action, created_at)
                        VALUES (?, ?, ?, 'BTCUSDT', 'execute', ?)
                        """,
                        (str(uuid.uuid4()), b, run_ids[b], now),
                    )

        rate_a = AdaptiveEngine(self.db, bot_instance_id=bot_ids[0])._get_exec_failure_rate("BTCUSDT", lookback=10)
        assert rate_a == 1.0
        for b in bot_ids[1:]:
            rate = AdaptiveEngine(self.db, bot_instance_id=b)._get_exec_failure_rate("BTCUSDT", lookback=10)
            assert rate == 0.0

        # Scenario 1 + 4 — Loss streak isolation (trade_fills scoped by bot_instance_id)
        # Insert 3 losing CLOSE fills for Bot00 only.
        ts0 = datetime.now(timezone.utc)
        with self.db.connect() as conn:
            for i in range(3):
                conn.execute(
                    """
                    INSERT INTO trade_fills
                        (run_id, symbol, side, action, qty, price, fee, realized_pnl, timestamp_utc, bot_instance_id)
                    VALUES
                        (?, 'BTCUSDT', 'LONG', 'CLOSE', 1.0, 100.0, 0.0, ?, ?, ?)
                    """,
                    (
                        run_ids[bot_ids[0]],
                        -10.0,
                        (ts0 + timedelta(seconds=i)).isoformat(),
                        bot_ids[0],
                    ),
                )

        streak_a = AdaptiveEngine(self.db, bot_instance_id=bot_ids[0])._get_loss_streak_from_db("BTCUSDT")
        assert streak_a == 3
        for b in bot_ids[1:]:
            streak = AdaptiveEngine(self.db, bot_instance_id=b)._get_loss_streak_from_db("BTCUSDT")
            assert streak == 0

        # Scenario 6 — Concurrent writes to trade_fills/decision_logs/snapshots
        errors: list[str] = []

        def worker(bot_id: str) -> None:
            store = StateStore(self.db, bot_instance_id=bot_id)
            base = 5000.0 + int(bot_id[-2:]) * 10.0
            for n in range(25):
                ts = (datetime.now(timezone.utc) + timedelta(milliseconds=n)).isoformat()
                with self.db.connect() as conn:
                    conn.execute(
                        """
                        INSERT INTO trade_fills
                            (run_id, symbol, side, action, qty, price, fee, realized_pnl, timestamp_utc, bot_instance_id)
                        VALUES
                            (?, 'ETHUSDT', 'LONG', 'CLOSE', 1.0, 200.0, 0.0, ?, ?, ?)
                        """,
                        (run_ids[bot_id], -1.0 if (n % 2 == 0) else 1.0, ts, bot_id),
                    )
                    conn.execute(
                        """
                        INSERT INTO decision_logs (id, config_id, run_id, symbol, final_action, created_at)
                        VALUES (?, ?, ?, 'ETHUSDT', ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            bot_id,
                            run_ids[bot_id],
                            "blocked" if (n % 5 == 0) else "execute",
                            ts,
                        ),
                    )

                # Snapshots: update same periods repeatedly (INSERT OR REPLACE)
                store.save_weekly_snapshot(PeriodSnapshot(week, base, base + n, base - n))
                store.save_monthly_snapshot(PeriodSnapshot(month, base, base + n, base - n))

        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = [ex.submit(worker, b) for b in bot_ids]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as exc:  # pragma: no cover
                    errors.append(str(exc))

        assert errors == []

        # Post-conditions: no cross-bot overwrites and rows remain scoped.
        with self.db.connect() as conn:
            for b in bot_ids:
                tf = conn.execute(
                    "SELECT COUNT(*) FROM trade_fills WHERE bot_instance_id = ?",
                    (b,),
                ).fetchone()[0]
                dl = conn.execute(
                    "SELECT COUNT(*) FROM decision_logs WHERE config_id = ?",
                    (b,),
                ).fetchone()[0]
                assert tf >= 25  # plus potential earlier inserts for bot00
                assert dl >= 25  # plus potential earlier inserts

