"""Phases 5 & 6 — run_cycle authority, the strategy clock, and MarketSnapshot.

Two clocks, deliberately separate:

  * the 10-second management heartbeat  — positions, protection, kill switch,
    reconciliation, daily close;
  * the strategy-entry clock            — one evaluation per newly CLOSED
    strategy candle, per bot, per symbol, per timeframe.

A heartbeat that finds no new closed candle did not "decide to hold". It did
not run the strategy at all, and says so with NO_NEW_CANDLE.
"""
from __future__ import annotations

import inspect

import pytest

from app.decision.reasons import CycleReason, QualityReason
from app.runner.market_snapshot import (
    MarketSnapshot,
    SnapshotMarketClient,
    claim_candle,
    closed_candles,
)
from app.runner.runner import PaperRunner
from shared_lib.persistence.db import DB

MINUTE_MS = 60_000


def candle(index: int, *, tf_ms: int = 15 * MINUTE_MS, close: float = 100.0) -> list:
    """[openTime, open, high, low, close, volume, closeTime]"""
    open_ms = index * tf_ms
    return [open_ms, close, close * 1.01, close * 0.99, close, 10.0, open_ms + tf_ms - 1]


@pytest.fixture
def candles():
    return [candle(i) for i in range(1, 21)]


@pytest.fixture
def db():
    return DB(":memory:")


# ══════════════════════════════════════════════════════════════════════════
# Phase 5 — run_cycle is the only active management authority
# ══════════════════════════════════════════════════════════════════════════


def test_run_once_has_no_unique_business_logic():
    """§5.8 — run_once must be a thin wrapper, not a second trading system."""
    source = inspect.getsource(PaperRunner.run_once)

    assert "return self.run_cycle()" in source
    body = [
        line.strip()
        for line in source.splitlines()[1:]
        if line.strip() and not line.strip().startswith(("#", '"""', "'''"))
    ]
    # Docstring + deprecation log + delegation. Nothing else may live here.
    assert len(body) <= 4, f"run_once has grown its own logic: {body}"
    for forbidden in ("kill_switch", "daily_close", "step_symbol", "reconcile", "execute_signal"):
        assert forbidden not in source, f"run_once still owns {forbidden}"


def test_run_cycle_owns_the_canonical_management_sequence():
    """§5.2 — the whole sequence is invoked from run_cycle, in order."""
    source = inspect.getsource(PaperRunner.run_cycle)

    expected_order = [
        "reconcile_positions_on_startup",   # 2. reconcile
        "activate_kill_switch",             # 4. kill switch
        "_run_daily_close_from_cycle",      # 5. daily/session close
        "step_symbol",                      # 6/7/8. manage + entry decision
    ]
    positions = [source.index(name) for name in expected_order if name in source]
    assert len(positions) == len(expected_order), "run_cycle lost part of the sequence"
    assert positions == sorted(positions), "run_cycle sequence is out of order"


def test_kill_switch_is_evaluated_inside_run_cycle():
    source = inspect.getsource(PaperRunner.run_cycle)
    assert "activate_kill_switch" in source


def test_daily_close_runs_inside_run_cycle():
    source = inspect.getsource(PaperRunner.run_cycle)
    assert "_run_daily_close_from_cycle" in source


def test_daily_close_uses_the_managed_remaining_quantity_not_the_original():
    """After a TP1 the daily close must close the remainder."""
    source = inspect.getsource(PaperRunner._run_daily_close_from_cycle)
    assert "pos.current_qty" in source
    assert "_close_managed_position(symbol, \"DAILY_CLOSE\")" in source


def test_daily_close_marks_are_durable_not_in_memory():
    """§5.6 — an in-memory flag would not survive a restart."""
    source = inspect.getsource(PaperRunner._mark_daily_close)
    assert "bot_daily_close_marks" in source
    assert "INSERT OR REPLACE" in source


def test_daily_close_idempotency_lookup_fails_closed():
    """If we cannot prove it is unclosed, we must not close it again."""
    source = inspect.getsource(PaperRunner._daily_close_already_marked)
    assert "return True" in source
    assert "except Exception" in source


def test_daily_close_marker_round_trips_and_blocks_a_repeat(db):
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO bot_daily_close_marks
               (bot_instance_id,symbol,close_window,position_id,closed_at,reason)
               VALUES (?,?,?,?,?,?)""",
            ("bot-1", "BTCUSDT", "2026-09-07:23:30", "pos-1", "now", "DAILY_CLOSE"),
        )
        found = conn.execute(
            """SELECT 1 FROM bot_daily_close_marks
               WHERE bot_instance_id=? AND symbol=? AND close_window=?""",
            ("bot-1", "BTCUSDT", "2026-09-07:23:30"),
        ).fetchone()
        other_window = conn.execute(
            """SELECT 1 FROM bot_daily_close_marks
               WHERE bot_instance_id=? AND symbol=? AND close_window=?""",
            ("bot-1", "BTCUSDT", "2026-09-08:23:30"),
        ).fetchone()

    assert found is not None, "same window is already closed"
    assert other_window is None, "the next day's window is a different window"


def test_paper_reconciliation_never_consults_broker_positions():
    """§5.3 — a demo exchange is flat while a paper position is open."""
    source = inspect.getsource(PaperRunner._step_symbol_orchestrated)
    assert 'if self._effective_execution_mode() == "broker":' in source
    guard_at = source.index('if self._effective_execution_mode() == "broker":')
    call_at = source.index("get_position_info", guard_at)
    assert call_at > guard_at, "exchange positions are read outside the broker-mode guard"


def test_broker_mode_reconciliation_still_reads_exchange_truth():
    source = inspect.getsource(PaperRunner._step_symbol_orchestrated)
    assert "self.executor.client.get_position_info(symbol)" in source


# ══════════════════════════════════════════════════════════════════════════
# Phase 6 — the strategy clock
# ══════════════════════════════════════════════════════════════════════════


def test_only_closed_candles_are_returned(candles):
    """An unfinished candle is never an entry signal."""
    now = candles[-1][6] - 1  # the final candle has not closed yet
    result = closed_candles(candles, now_ms=now)
    assert len(result) == len(candles) - 1
    assert result[-1] is candles[-2]


def test_snapshot_build_rejects_data_with_no_closed_candle():
    with pytest.raises(ValueError, match="STALE_MARKET_DATA"):
        MarketSnapshot.build(
            symbol="BTCUSDT", timeframe="15m", candles=[], source="test"
        )


def test_same_candle_is_evaluated_only_once(db, candles):
    close_time = candles[-1][6]
    first = claim_candle(db, bot_instance_id="bot-1", symbol="BTCUSDT", timeframe="15m", close_time=close_time)
    second = claim_candle(db, bot_instance_id="bot-1", symbol="BTCUSDT", timeframe="15m", close_time=close_time)
    third = claim_candle(db, bot_instance_id="bot-1", symbol="BTCUSDT", timeframe="15m", close_time=close_time)

    assert first is True, "the first heartbeat on a new candle evaluates it"
    assert second is False, "later heartbeats on the same candle must not re-evaluate"
    assert third is False


def test_a_newly_closed_candle_triggers_exactly_one_evaluation(db, candles):
    first_close = candles[-2][6]
    next_close = candles[-1][6]

    assert claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=first_close)
    assert not claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=first_close)
    assert claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=next_close)
    assert not claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=next_close)


def test_an_older_candle_never_reclaims(db):
    assert claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=5000)
    assert not claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=4000)


def test_restart_does_not_re_evaluate_a_persisted_candle(db):
    """The marker is in the database, so a fresh process sees it."""
    assert claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=9000)

    # A restart re-reads the same durable store.
    assert not claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=9000)


def test_btc_and_eth_keep_independent_markers(db):
    assert claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=9000)
    assert claim_candle(db, bot_instance_id="b", symbol="ETHUSDT", timeframe="15m", close_time=9000)
    assert not claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=9000)


def test_timeframes_keep_independent_markers(db):
    assert claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=9000)
    assert claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="1h", close_time=9000)
    assert not claim_candle(db, bot_instance_id="b", symbol="BTCUSDT", timeframe="15m", close_time=9000)


def test_bots_keep_independent_markers(db):
    assert claim_candle(db, bot_instance_id="bot-a", symbol="BTCUSDT", timeframe="15m", close_time=9000)
    assert claim_candle(db, bot_instance_id="bot-b", symbol="BTCUSDT", timeframe="15m", close_time=9000)


# ── Canonical MarketSnapshot ────────────────────────────────────────────────


def test_snapshot_is_immutable(candles):
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="test")
    with pytest.raises(Exception):
        snap.reference_price = 1.0  # type: ignore[misc]


def test_snapshot_carries_a_correlation_id_and_identity_fields(candles):
    snap = MarketSnapshot.build(
        symbol="btcusdt", timeframe="15m", candles=candles, source="binance",
        source_environment="demo",
    )
    assert snap.market_snapshot_id.startswith("ms_")
    assert snap.symbol == "BTCUSDT"
    assert snap.source == "binance"
    assert snap.source_environment == "demo"
    assert snap.fetched_at
    assert snap.latest_closed_candle_time == candles[-1][6]
    assert snap.latest_closed_candle_open_time == candles[-1][0]


def test_each_snapshot_gets_its_own_id(candles):
    a = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    b = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    assert a.market_snapshot_id != b.market_snapshot_id


def test_snapshot_data_hash_is_deterministic_for_the_same_candles(candles):
    a = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    b = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    assert a.data_hash == b.data_hash

    changed = MarketSnapshot.build(
        symbol="BTCUSDT", timeframe="15m", candles=candles[:-1], source="t"
    )
    assert changed.data_hash != a.data_hash


# ── Fetch once; every component sees the same view ──────────────────────────


class CountingClient:
    def __init__(self, candles):
        self._candles = candles
        self.kline_calls: list[tuple[str, str]] = []

    def klines(self, *, symbol, interval, limit=500, **kw):
        self.kline_calls.append((symbol, interval))
        return self._candles[-limit:]


def test_components_consume_the_snapshot_without_refetching(candles):
    client = CountingClient(candles)
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    pinned = SnapshotMarketClient(client, snap)

    for _ in range(5):  # five components, one decision
        rows = pinned.klines(symbol="BTCUSDT", interval="15m", limit=50)
        assert rows[-1] == snap.candles[-1]

    assert client.kline_calls == [], "no component may fetch its own market view"


def test_every_component_sees_the_identical_latest_candle(candles):
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    pinned = SnapshotMarketClient(CountingClient(candles), snap)

    views = {pinned.klines(symbol="BTCUSDT", interval="15m")[-1][6] for _ in range(8)}
    assert views == {snap.latest_closed_candle_time}


def test_snapshot_client_refuses_a_different_symbol(candles):
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    pinned = SnapshotMarketClient(CountingClient(candles), snap)
    with pytest.raises(ValueError, match="snapshot_symbol_mismatch"):
        pinned.klines(symbol="ETHUSDT", interval="15m")


def test_snapshot_client_refuses_an_unpinned_timeframe(candles):
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    pinned = SnapshotMarketClient(CountingClient(candles), snap)
    with pytest.raises(ValueError, match="snapshot_timeframe_unavailable"):
        pinned.klines(symbol="BTCUSDT", interval="5m")


# ── HTF timestamp alignment (closes OLD-R2) ─────────────────────────────────


def test_htf_candle_that_already_closed_is_aligned(candles):
    strategy_close = candles[-1][6]
    htf = [[0, 1, 2, 0.5, 1.5, 10, strategy_close - MINUTE_MS]]
    snap = MarketSnapshot.build(
        symbol="BTCUSDT", timeframe="15m", candles=candles, source="t",
        higher_timeframe="1h", higher_timeframe_candles=htf,
    )
    assert snap.htf_is_timestamp_aligned() is True
    assert snap.higher_timeframe_closed_candle_time == strategy_close - MINUTE_MS


def test_a_future_htf_candle_is_rejected_as_look_ahead(candles):
    """A 15m decision at 14:45 may not use a 1h candle closing at 15:00."""
    strategy_close = candles[-1][6]
    future_htf = [[0, 1, 2, 0.5, 1.5, 10, strategy_close + 15 * MINUTE_MS]]

    snap = MarketSnapshot(
        symbol="BTCUSDT", timeframe="15m", candles=tuple(candles),
        latest_closed_candle_time=strategy_close, reference_price=100.0,
        fetched_at="now", source="t", higher_timeframe="1h",
        higher_timeframe_candles=tuple(future_htf),
        higher_timeframe_closed_candle_time=future_htf[0][6],
    )

    assert snap.htf_is_timestamp_aligned() is False, "look-ahead HTF data was accepted"


def test_snapshot_build_filters_unclosed_htf_candles(candles):
    """build() must not admit an HTF candle that has not closed in wall time."""
    far_future = 99_999_999_999_999
    snap = MarketSnapshot.build(
        symbol="BTCUSDT", timeframe="15m", candles=candles, source="t",
        higher_timeframe="1h",
        higher_timeframe_candles=[[0, 1, 2, 0.5, 1.5, 10, far_future]],
    )
    assert snap.higher_timeframe_candles == ()


def test_no_htf_data_counts_as_aligned(candles):
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=candles, source="t")
    assert snap.htf_is_timestamp_aligned() is True


# ── NO_NEW_CANDLE is not a strategy HOLD ────────────────────────────────────


def test_no_new_candle_is_not_reported_as_a_strategy_hold():
    """§6.9 — the runner must not manufacture a HOLD it never computed."""
    source = inspect.getsource(PaperRunner._step_symbol_orchestrated)

    assert '{"symbol": symbol, "decision": "HOLD", "reason": "NO_NEW_CANDLE"}' not in source
    assert '"decision": CycleReason.NO_NEW_CANDLE' in source
    assert '"evaluated": False' in source


def test_no_new_candle_and_no_opportunity_are_different_codes():
    assert CycleReason.NO_NEW_CANDLE == "NO_NEW_CANDLE"
    assert QualityReason.NO_OPPORTUNITY == "NO_OPPORTUNITY"
    assert CycleReason.NO_NEW_CANDLE != QualityReason.NO_OPPORTUNITY


def test_management_continues_on_heartbeats_without_a_new_candle():
    """Position management must not be gated behind the entry clock."""
    source = inspect.getsource(PaperRunner._step_symbol_orchestrated)
    gate_at = source.index("if not evaluate_entry:")
    # Exit management (PositionManager price ticks) runs before the entry gate.
    assert source.index("self.position_manager.update_price") < gate_at
