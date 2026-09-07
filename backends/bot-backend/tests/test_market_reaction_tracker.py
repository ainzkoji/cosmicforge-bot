"""
Integration tests for the Market Reaction Tracker and EventReactionWorker.

Run with:
    cd backends/bot-backend
    python -m pytest tests/test_market_reaction_tracker.py -v

These tests use an in-memory SQLite DB (no external services required).
"""
from __future__ import annotations

import sqlite3
import tempfile
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate
from shared_lib.persistence.market_reactions import (
    get_snapshots,
    get_reaction,
    insert_snapshot,
)
from app.events.market_reaction_tracker import MarketReactionTracker
from app.services.market_data_snapshot_service import MarketDataSnapshotService
from app.workers.event_reaction_worker import EventReactionWorker
from app.risk.event_reaction_risk_gate import EventReactionRiskGate


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = DB(path=path)
    migrate(db_path=path)
    yield db
    os.unlink(path)


def _make_kline(open_=100.0, high=102.0, low=98.0, close=101.0, vol=500.0):
    return [
        int(datetime.now(timezone.utc).timestamp() * 1000),
        str(open_), str(high), str(low), str(close), str(vol),
        int(datetime.now(timezone.utc).timestamp() * 1000) + 60000,
        "0", "10", "0", "0", "0"
    ]


def _fake_client(n_klines=16, fail=False):
    client = MagicMock()
    if fail:
        client.klines.side_effect = Exception("Exchange error")
    else:
        client.klines.return_value = [_make_kline() for _ in range(n_klines)]
    return client


# ---------------------------------------------------------------------------
# T1 — Snapshot service stores pre and post snapshots
# ---------------------------------------------------------------------------

class TestSnapshotService:
    def test_stores_snapshot(self, tmp_db):
        client = _fake_client()
        svc = MarketDataSnapshotService(tmp_db, client, exchange_name="binance")

        ok = svc.fetch_and_store("evt-001", "BTCUSDT", "PRE_60")
        assert ok is True

        rows = get_snapshots(tmp_db, "evt-001", "BTCUSDT")
        assert len(rows) == 1
        assert rows[0]["window_label"] == "PRE_60"
        assert rows[0]["price"] is not None

    def test_skips_duplicate_by_default(self, tmp_db):
        client = _fake_client()
        svc = MarketDataSnapshotService(tmp_db, client, exchange_name="binance")

        svc.fetch_and_store("evt-001", "BTCUSDT", "PRE_60")
        svc.fetch_and_store("evt-001", "BTCUSDT", "PRE_60")

        rows = get_snapshots(tmp_db, "evt-001", "BTCUSDT")
        assert len(rows) == 1  # duplicate not inserted

    def test_exchange_error_returns_false(self, tmp_db):
        client = _fake_client(fail=True)
        svc = MarketDataSnapshotService(tmp_db, client, exchange_name="binance")

        ok = svc.fetch_and_store("evt-001", "BTCUSDT", "PRE_60")
        assert ok is False

        rows = get_snapshots(tmp_db, "evt-001", "BTCUSDT")
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# T2 — Tracker computes reaction from snapshots
# ---------------------------------------------------------------------------

class TestReactionTracker:
    def _seed_snapshots(self, db, event_id="evt-001", symbol="BTCUSDT"):
        now = datetime.now(timezone.utc)
        windows = ["PRE_60", "PRE_30", "EVENT", "POST_5", "POST_15", "POST_30", "POST_60"]
        prices = [100.0, 100.5, 102.0, 102.5, 103.0, 103.5, 104.0]
        for w, p in zip(windows, prices):
            insert_snapshot(
                db,
                event_id=event_id,
                symbol=symbol,
                exchange="binance",
                timestamp_utc=now.isoformat(),
                window_label=w,
                price=p,
                volume=500.0 if w == "EVENT" else 200.0,
                candle_open=p - 0.5,
                candle_high=p + 0.5,
                candle_low=p - 0.5,
                candle_close=p,
                atr=0.5,
            )

    def test_computes_and_saves_reaction(self, tmp_db):
        self._seed_snapshots(tmp_db)
        tracker = MarketReactionTracker(tmp_db)
        now = datetime.now(timezone.utc)

        result = tracker.compute_and_save(
            event_id="evt-001",
            symbol="BTCUSDT",
            exchange="binance",
            event_time_utc=now.isoformat(),
            pre_window_start_utc=(now - timedelta(hours=1)).isoformat(),
            post_window_end_utc=(now + timedelta(hours=4)).isoformat(),
            shadow_mode=True,
        )
        assert result is not None
        assert result["price_before_event"] is not None
        assert result["price_after_event"] == pytest.approx(104.0)
        assert result["reaction_type"] is not None
        assert result["data_quality"] is not None

        # Row should be persisted in DB
        row = get_reaction(tmp_db, "evt-001", "BTCUSDT")
        assert row is not None
        assert row["reaction_type"] == result["reaction_type"]

    def test_missing_data_gets_degraded_quality(self, tmp_db):
        # Only seed EVENT snapshot, no PRE or POST data
        now = datetime.now(timezone.utc)
        insert_snapshot(
            tmp_db,
            event_id="evt-002",
            symbol="ETHUSDT",
            exchange="binance",
            timestamp_utc=now.isoformat(),
            window_label="EVENT",
            price=2000.0,
            volume=1000.0,
        )

        tracker = MarketReactionTracker(tmp_db)
        result = tracker.compute_and_save(
            event_id="evt-002",
            symbol="ETHUSDT",
            exchange="binance",
            event_time_utc=now.isoformat(),
            pre_window_start_utc=(now - timedelta(hours=1)).isoformat(),
            post_window_end_utc=(now + timedelta(hours=1)).isoformat(),
            shadow_mode=True,
        )
        assert result is not None
        assert result["data_quality"] != "COMPLETE"
        assert result["confidence_score"] < 1.0

    def test_no_snapshots_marks_exchange_error(self, tmp_db):
        now = datetime.now(timezone.utc)
        tracker = MarketReactionTracker(tmp_db)
        result = tracker.compute_and_save(
            event_id="evt-003",
            symbol="XRPUSDT",
            exchange="binance",
            event_time_utc=now.isoformat(),
            pre_window_start_utc=(now - timedelta(hours=1)).isoformat(),
            post_window_end_utc=(now + timedelta(hours=1)).isoformat(),
            shadow_mode=True,
        )
        assert result is not None
        assert result["data_quality"] == "EXCHANGE_DATA_ERROR"


class TestEventReactionWorkerWindowContract:
    def test_finalizes_after_post_60_window(self, tmp_db):
        snapshot_service = MagicMock()
        tracker = MagicMock()
        worker = EventReactionWorker(
            tmp_db,
            snapshot_service=snapshot_service,
            tracker=tracker,
            enabled=True,
            shadow_mode=True,
            pre_event_minutes=60,
            post_event_minutes=60,
            snapshot_interval_seconds=60,
            active_symbols=["BTCUSDT"],
        )

        now = datetime.now(timezone.utc)
        event_dt = now - timedelta(minutes=60)
        ev = {
            "event_id": "evt-060",
            "event_type": "CPI",
            "country_currency": "USD",
            "scheduled_utc": event_dt.isoformat(),
        }

        snapshot_service.fetch_and_store.return_value = True
        worker._process_event(ev, now)

        assert tracker.compute_and_save.called is True
        kwargs = tracker.compute_and_save.call_args.kwargs
        assert kwargs["symbol"] == "BTCUSDT"
        assert kwargs["shadow_mode"] is True


# ---------------------------------------------------------------------------
# T3 — Reaction layer does NOT open trades
# ---------------------------------------------------------------------------

class TestNoTradesOpened:
    def test_tracker_never_calls_order_placement(self, tmp_db):
        now = datetime.now(timezone.utc)
        tracker = MarketReactionTracker(tmp_db)

        result = tracker.compute_and_save(
            event_id="evt-004",
            symbol="BTCUSDT",
            exchange="binance",
            event_time_utc=now.isoformat(),
            pre_window_start_utc=(now - timedelta(hours=1)).isoformat(),
            post_window_end_utc=(now + timedelta(hours=1)).isoformat(),
            shadow_mode=True,
        )
        assert result is not None


# ---------------------------------------------------------------------------
# T4 — Shadow mode: risk gate returns False when influence disabled
# ---------------------------------------------------------------------------

class TestShadowMode:
    def test_shadow_gate_never_blocks(self, tmp_db):
        # Insert a WHIPSAW reaction
        now = datetime.now(timezone.utc)
        from shared_lib.persistence.market_reactions import upsert_reaction
        upsert_reaction(
            tmp_db,
            event_id="evt-001",
            symbol="BTCUSDT",
            exchange="binance",
            event_time_utc=now.isoformat(),
            reaction_type="WHIPSAW",
            volatility_expansion_ratio=3.5,
            data_quality="COMPLETE",
        )

        gate = EventReactionRiskGate(
            tmp_db,
            enabled=False,  # shadow — never influences risk
            extend_on_vol_spike=True,
            vol_spike_threshold=2.5,
        )
        decision = gate.check("BTCUSDT")
        assert decision.is_blocked is False


# ---------------------------------------------------------------------------
# T5 — Risk gate blocks when enabled and vol spike detected
# ---------------------------------------------------------------------------

class TestReactionRiskGateBlocking:
    def test_blocks_on_vol_spike_within_window(self, tmp_db):
        now = datetime.now(timezone.utc)
        from shared_lib.persistence.market_reactions import upsert_reaction
        upsert_reaction(
            tmp_db,
            event_id="evt-001",
            symbol="BTCUSDT",
            exchange="binance",
            event_time_utc=now.isoformat(),   # event just happened
            reaction_type="VOL_SPIKE",
            volatility_expansion_ratio=3.5,
            data_quality="COMPLETE",
        )

        gate = EventReactionRiskGate(
            tmp_db,
            enabled=True,
            extend_on_vol_spike=True,
            vol_spike_threshold=2.5,
            extension_minutes=30,
        )
        decision = gate.check("BTCUSDT")
        assert decision.is_blocked is True
        assert decision.reason is not None

    def test_does_not_block_outside_extension_window(self, tmp_db):
        old_event_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        from shared_lib.persistence.market_reactions import upsert_reaction
        upsert_reaction(
            tmp_db,
            event_id="evt-002",
            symbol="ETHUSDT",
            exchange="binance",
            event_time_utc=old_event_time,
            reaction_type="VOL_SPIKE",
            volatility_expansion_ratio=4.0,
            data_quality="COMPLETE",
        )

        gate = EventReactionRiskGate(
            tmp_db,
            enabled=True,
            extend_on_vol_spike=True,
            vol_spike_threshold=2.5,
            extension_minutes=30,  # 30 min extension but event was 2h ago
        )
        decision = gate.check("ETHUSDT")
        assert decision.is_blocked is False
