from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from app.core.config import settings
from scripts.validation.run_paper_cycle_diagnostic import (
    BLOCK_REASONS,
    block_reason_counts,
    classify_block_reason,
    minute_in_windows,
    paper_order_created,
    run_diagnostic,
)


def _create_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE decision_traces (
            trace_id TEXT, run_id TEXT, cycle_id TEXT, bot_instance_id TEXT,
            symbol TEXT, timeframe TEXT, ts TEXT, last_price REAL,
            regime_state TEXT, signal TEXT, confidence REAL, reason_codes TEXT,
            gate_allowed INTEGER, gate_reason TEXT, intended_action TEXT,
            execution_status TEXT, execution_error TEXT, submit_attempted INTEGER,
            fill_recorded INTEGER, position_opened INTEGER, rejection_reason TEXT,
            kill_switch_state TEXT, open_positions_count INTEGER,
            event_block_reason TEXT
        );
        CREATE TABLE bot_instances (
            id TEXT, status TEXT, last_run_at TEXT, last_error TEXT
        );
        CREATE TABLE runs (
            run_id TEXT, started_at TEXT, stopped_at TEXT, mode TEXT,
            interval_seconds INTEGER, max_symbols INTEGER, config_json TEXT,
            status TEXT
        );
        CREATE TABLE bot_daily_state (
            bot_instance_id TEXT, day TEXT, realized_pnl REAL, kill INTEGER,
            trade_count INTEGER, last_updated_at TEXT, consecutive_losses INTEGER,
            consec_loss_cooldown_until_ms INTEGER
        );
        CREATE TABLE trade_fills (
            id INTEGER, timestamp_utc TEXT
        );
        CREATE TABLE events (
            timestamp_utc TEXT, symbol TEXT, event_type TEXT, action TEXT,
            details_json TEXT
        );
        """
    )
    now = datetime.now(timezone.utc).isoformat()
    run_config = {
        "EXECUTION_MODE": "paper",
        "ML_ENABLED": False,
        "IOFS_GATE_ENABLED": True,
        "IOFS_GATE_MODE": "shadow",
        "TRADE_SYMBOLS": "BTCUSDT,ETHUSDT",
        "MAX_TRADES_DAILY": 3,
        "MAX_OPEN_POSITIONS": 3,
        "DAILY_MAX_LOSS_USDT": 50.0,
        "ENSEMBLE_SESSION_WINDOWS_UTC": "06:00-19:00",
    }
    conn.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?)",
        ("run-1", now, None, "paper", 60, 2, json.dumps(run_config), "running"),
    )
    conn.execute(
        "INSERT INTO bot_instances VALUES (?,?,?,?)",
        ("bot-1", "active", now, None),
    )
    conn.execute(
        "INSERT INTO bot_daily_state VALUES (?,?,?,?,?,?,?,?)",
        ("bot-1", now[:10], 0.0, 0, 0, now, 0, 0),
    )
    for cycle in range(3):
        for symbol in ("BTCUSDT", "ETHUSDT"):
            conn.execute(
                "INSERT INTO decision_traces VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"trace-{cycle}-{symbol}",
                    "run-1",
                    f"cycle-{cycle}",
                    "bot-1",
                    symbol,
                    "15m",
                    now,
                    100.0,
                    "WEAK_TREND",
                    "hold",
                    0.0,
                    "MASTER_ENSEMBLE_V2",
                    0,
                    "MASTER_ENSEMBLE_V2",
                    "HOLD",
                    "None",
                    None,
                    0,
                    0,
                    0,
                    "MASTER_ENSEMBLE_V2",
                    "NORMAL",
                    0,
                    None,
                ),
            )
    for symbol in ("BTCUSDT", "ETHUSDT"):
        conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?)",
            (
                now,
                symbol,
                "IOFS_GATE",
                "EVALUATED",
                json.dumps(
                    {
                        "symbol": symbol,
                        "mode": "shadow",
                        "reason": "TREND_NOT_ALIGNED",
                        "blocked_trade": False,
                    }
                ),
            ),
        )
    conn.commit()
    conn.close()


def _healthy_candles(symbol, interval, limit, **kwargs):
    del symbol, kwargs
    interval_ms = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}[interval]
    now_ms = int(time.time() * 1000)
    end = now_ms - 1_000
    rows = []
    for index in range(limit):
        close_time = end - ((limit - index - 1) * interval_ms)
        rows.append(
            [
                close_time - interval_ms + 1,
                "100",
                "102",
                "99",
                "101",
                "10",
                close_time,
            ]
        )
    return rows


def test_runtime_session_window_allows_0600_to_1900_and_replay_does_not_override():
    runtime = settings.ENSEMBLE_SESSION_WINDOWS_UTC
    replay = settings.IOFS_SESSION_WINDOWS_UTC

    assert runtime == "06:00-19:00"
    assert replay == "07:00-10:00,13:00-16:00"
    assert minute_in_windows(runtime, datetime(2026, 6, 14, 12, 0, tzinfo=timezone.utc))
    assert not minute_in_windows(replay, datetime(2026, 6, 14, 12, 0, tzinfo=timezone.utc))


def test_block_reason_is_visible_when_no_trade_is_attempted():
    trace = {
        "signal": "hold",
        "intended_action": "HOLD",
        "gate_reason": "SESSION_BLOCKED",
        "submit_attempted": 0,
    }

    assert classify_block_reason(trace) == "session_blocked"
    counts = block_reason_counts([trace])
    assert set(counts) == set(BLOCK_REASONS)
    assert counts["session_blocked"] == 1


def test_paper_position_opened_status_counts_as_created_order():
    assert paper_order_created(
        {
            "execution_status": "PAPER_POSITION_OPENED",
            "fill_recorded": 0,
            "position_opened": 0,
        }
    )


def test_diagnostic_command_produces_json_markdown_and_safety_evidence(tmp_path):
    db_path = tmp_path / "diagnostic.db"
    output = tmp_path / "reports"
    _create_db(db_path)

    payload = run_diagnostic(
        symbols=["BTCUSDT", "ETHUSDT"],
        cycles=3,
        db_path=db_path,
        output_dir=output,
        market_fetcher=_healthy_candles,
    )

    expected = {
        "paper_cycle_diagnostic.json",
        "paper_cycle_diagnostic.md",
        "trading_block_reason_summary.json",
        "trading_block_reason_summary.md",
        "trading_activity_diagnostic.md",
    }
    assert expected == {path.name for path in output.iterdir()}
    assert payload["safety"]["executor_imported_or_called"] is False
    assert payload["safety"]["orders_placed"] is False
    assert payload["findings"]["runner_loop_alive"] is True
    assert payload["findings"]["market_data_loading"] is True
    assert payload["findings"]["session_filter_blocking_all_cycles"] is False
    assert payload["findings"]["circuit_or_daily_limit_stuck"] is False
    assert "circuit_states" in payload["runtime"]
    assert payload["runtime"]["iofs_summary"]["blocked_trade_true"] == 0
    assert payload["block_summary"]["counts"]["strategy_no_signal"] == 6
    assert all(
        entry["block_reason"] == "strategy_no_signal"
        for cycle in payload["cycles"]
        for entry in cycle["symbols"]
    )


def test_daily_counter_zero_after_restart_is_not_reported_stuck(tmp_path):
    db_path = tmp_path / "diagnostic.db"
    _create_db(db_path)

    payload = run_diagnostic(
        symbols=["BTCUSDT", "ETHUSDT"],
        cycles=1,
        db_path=db_path,
        output_dir=tmp_path / "reports",
        market_fetcher=_healthy_candles,
    )

    assert payload["runtime"]["current_daily"]["trade_count"] == 0
    assert payload["runtime"]["current_daily"]["kill"] == 0
    assert payload["findings"]["circuit_or_daily_limit_stuck"] is False
