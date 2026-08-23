from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.validation.daily_paper_validation_monitor import (
    run_daily_paper_validation_monitor,
)


NOW = datetime(2026, 6, 16, 6, 0, tzinfo=timezone.utc)
CLEAN_START = "2026-06-15T04:46:39.294507Z"


def _fixture(tmp_path: Path):
    db = tmp_path / "monitor.db"
    with sqlite3.connect(db) as connection:
        connection.executescript(
            """
            CREATE TABLE decision_traces (
                trace_id TEXT, cycle_id TEXT, bot_instance_id TEXT, symbol TEXT,
                ts TEXT, regime_state TEXT, signal TEXT, intended_action TEXT,
                reason_codes TEXT, gate_reason TEXT, execution_status TEXT,
                execution_error TEXT, submit_attempted INTEGER, rejection_reason TEXT,
                event_block_reason TEXT, open_positions_count INTEGER
            );
            CREATE TABLE trade_fills (
                id INTEGER PRIMARY KEY, bot_instance_id TEXT, symbol TEXT,
                action TEXT, timestamp_utc TEXT
            );
            CREATE TABLE position_lifecycle_state (
                bot_instance_id TEXT, phase TEXT, exchange_position_active INTEGER
            );
            CREATE TABLE bot_instances (
                id TEXT, status TEXT, mode TEXT, last_run_at TEXT, last_error TEXT,
                active_positions INTEGER
            );
            CREATE TABLE bot_daily_state (
                bot_instance_id TEXT, day TEXT, realized_pnl REAL, kill INTEGER,
                trade_count INTEGER, last_updated_at TEXT
            );
            CREATE TABLE events (
                timestamp_utc TEXT, event_type TEXT, action TEXT
            );
            INSERT INTO decision_traces VALUES
              ('t1','c1','bot1','BTCUSDT','2026-06-16T05:59:00+00:00','STRONG_TREND',
               'HOLD','HOLD','REGIME_BLOCKED_STRONG_TREND','REGIME_BLOCKED_STRONG_TREND',
               'None',NULL,0,NULL,NULL,0),
              ('t2','c1','bot1','ETHUSDT','2026-06-16T05:58:00+00:00','WEAK_TREND',
               'HOLD','HOLD','NO_PATTERN','MASTER_ENSEMBLE_V2',
               'None',NULL,0,NULL,NULL,0);
            INSERT INTO bot_instances VALUES
              ('bot1','active','paper','2026-06-16T05:59:30+00:00',NULL,0);
            INSERT INTO bot_daily_state VALUES
              ('bot1','2026-06-16',0,0,0,'2026-06-16T00:00:00+00:00');
            """
        )
    env = tmp_path / ".env"
    env.write_text(
        "EXECUTION_MODE=paper\nML_ENABLED=False\nIOFS_GATE_MODE=shadow\n",
        encoding="utf-8",
    )
    experiment = tmp_path / ".env.paper_strong_trend_experiment"
    experiment.write_text(
        "STRONG_TREND_EXPERIMENT_START_TIME=2026-06-15T17:00:00Z\n",
        encoding="utf-8",
    )
    section4 = tmp_path / "iofs_paper_validation_status.md"
    section4.write_text(
        "# IOFS Paper Validation Status\n\n"
        f"- start_timestamp_utc: {CLEAN_START}\n"
        "- number_of_closed_paper_trades: 0\n"
        "- status: In Progress\n",
        encoding="utf-8",
    )
    production = tmp_path / "production"
    production.mkdir()
    (production / "README.md").write_text("unchanged", encoding="utf-8")
    component = tmp_path / "components.json"
    component.write_text(
        json.dumps({"summary": {"top_failed_conditions": {"fresh_breakout": 12}}}),
        encoding="utf-8",
    )
    return {
        "db_path": db,
        "active_env": env,
        "experiment_config": experiment,
        "section4_status": section4,
        "production_dir": production,
        "component_report": component,
        "output_json": tmp_path / "daily.json",
        "output_md": tmp_path / "daily.md",
    }


def _health(_url):
    return {
        "status": "ok",
        "execution_mode": "paper",
        "binance_env": "testnet",
        "ml_enabled": False,
        "iofs_gate_mode": "shadow",
        "trade_symbols": "BTCUSDT,ETHUSDT",
        "live_symbols_count": 0,
    }


def _strong(_db, start):
    return {
        "experiment_start_time": start,
        "strong_trend_cycles": 4,
        "strong_trend_signals": 0,
        "strong_trend_order_attempts": 0,
        "strong_trend_paper_orders_created": 0,
        "strong_trend_paper_orders": 0,
        "strong_trend_order_errors": 0,
        "strong_trend_fills": 0,
        "strong_trend_closed_trades": 0,
        "order_consistency_note": "",
        "order_count_diagnosis_summary": {
            "total_reported_paper_orders": 0,
            "valid_post_experiment_strong_trend_orders": 0,
            "failed_attempts": 0,
        },
        "strong_trend_metrics": {
            "win_rate": None,
            "profit_factor": None,
            "expectancy_R": None,
            "max_drawdown_R": 0,
        },
        "stop_recommended": False,
        "stop_reason": "",
    }


def _readiness(**_kwargs):
    return {
        "organic_rows": 326,
        "iofs_organic_rows": 0,
        "closed_iofs_paper_trades": 0,
        "ready_to_retry_5a": False,
        "ready_for_5b": False,
        "blocking_reasons": ["insufficient data"],
    }


def _run(tmp_path: Path, **overrides):
    paths = _fixture(tmp_path)
    result = run_daily_paper_validation_monitor(
        **paths,
        now=overrides.pop("now", NOW),
        health_fetcher=overrides.pop("health_fetcher", _health),
        strong_report_builder=overrides.pop("strong_report_builder", _strong),
        readiness_evaluator=overrides.pop("readiness_evaluator", _readiness),
        **overrides,
    )
    return result, paths


def test_monitor_writes_json_and_markdown_reports(tmp_path):
    result, paths = _run(tmp_path)
    assert result["health_ok"] is True
    assert paths["output_json"].exists()
    assert paths["output_md"].exists()


def test_monitor_handles_health_endpoint_unavailable(tmp_path):
    def unavailable(_url):
        raise OSError("connection refused")

    result, _ = _run(tmp_path, health_fetcher=unavailable)
    assert result["health_ok"] is False
    assert result["bot_process_running"] == "unknown"
    assert "BOT_NOT_RUNNING" in [alert["alert_name"] for alert in result["alerts"]]


def test_monitor_detects_no_trades_after_threshold(tmp_path):
    result, _ = _run(tmp_path)
    names = [alert["alert_name"] for alert in result["alerts"]]
    assert "NO_TRADES_AFTER_24H" in names


def test_monitor_includes_strong_trend_and_section5_status(tmp_path):
    result, _ = _run(tmp_path)
    assert result["strong_trend_experiment_active"] is True
    assert result["strong_trend_cycles"] == 4
    assert result["strong_trend_order_attempts"] == 0
    assert result["strong_trend_paper_orders_created"] == 0
    assert result["strong_trend_order_errors"] == 0
    assert result["organic_rows"] == 326
    assert result["ready_to_retry_5a"] is False


def test_monitor_updates_section4_without_marking_passed(tmp_path):
    _, paths = _run(tmp_path)
    text = paths["section4_status"].read_text(encoding="utf-8")
    assert "## Latest Daily Monitor Snapshot" in text
    assert "section4_status: In Progress" in text
    assert "status: Passed" not in text


def test_monitor_preserves_env_production_and_experiment_config(tmp_path):
    _, paths = _run(tmp_path)
    assert "ML_ENABLED=False" in paths["active_env"].read_text(encoding="utf-8")
    assert sorted(item.name for item in paths["production_dir"].iterdir()) == ["README.md"]
    assert "STRONG_TREND_EXPERIMENT_START_TIME" in paths[
        "experiment_config"
    ].read_text(encoding="utf-8")


def test_daily_monitor_clean_start_filtering_for_closed_trades(tmp_path):
    paths = _fixture(tmp_path)
    with sqlite3.connect(paths["db_path"]) as connection:
        connection.executemany(
            """
            INSERT INTO trade_fills
              (bot_instance_id, symbol, action, timestamp_utc)
            VALUES (?,?,?,?)
            """,
            [
                ("bot1", "BTCUSDT", "CLOSE", "2026-06-15T04:00:00+00:00"),
                ("bot1", "BTCUSDT", "CLOSE", "2026-06-15T05:00:00+00:00"),
            ],
        )

    result = run_daily_paper_validation_monitor(
        **paths,
        now=NOW,
        health_fetcher=_health,
        strong_report_builder=_strong,
        readiness_evaluator=_readiness,
    )

    assert result["closed_paper_trades_total"] == 2
    assert result["closed_paper_trades_since_clean_start"] == 1


def test_daily_monitor_filters_counts_to_active_bot_instance(tmp_path):
    paths = _fixture(tmp_path)
    with sqlite3.connect(paths["db_path"]) as connection:
        connection.execute(
            """
            INSERT INTO bot_instances VALUES
              ('bot_old','stopped','paper','2026-06-15T00:00:00+00:00',NULL,0)
            """
        )
        connection.executemany(
            """
            INSERT INTO decision_traces VALUES
              (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    "active_attempt",
                    "c2",
                    "bot1",
                    "BTCUSDT",
                    "2026-06-16T05:55:00+00:00",
                    "STRONG_TREND",
                    "buy",
                    "EXECUTE",
                    "",
                    "PROTECTIVE_ORDERS_VALIDATED",
                    "PAPER_ONLY",
                    "Paper mode (paper) - no execution",
                    1,
                    "BROKER_REJECT",
                    None,
                    0,
                ),
                (
                    "old_bot_attempt",
                    "c3",
                    "bot_old",
                    "BTCUSDT",
                    "2026-06-16T05:56:00+00:00",
                    "STRONG_TREND",
                    "buy",
                    "EXECUTE",
                    "",
                    "PROTECTIVE_ORDERS_VALIDATED",
                    "PAPER_ONLY",
                    "Paper mode (paper) - no execution",
                    1,
                    "BROKER_REJECT",
                    None,
                    0,
                ),
            ],
        )
        connection.executemany(
            """
            INSERT INTO trade_fills
              (bot_instance_id, symbol, action, timestamp_utc)
            VALUES (?,?,?,?)
            """,
            [
                ("bot1", "BTCUSDT", "CLOSE", "2026-06-16T05:00:00+00:00"),
                ("bot_old", "BTCUSDT", "CLOSE", "2026-06-16T05:00:00+00:00"),
            ],
        )
        connection.execute(
            """
            INSERT INTO bot_daily_state VALUES
              ('bot_old','2026-06-16',0,0,99,'2026-06-16T05:00:00+00:00')
            """
        )

    result = run_daily_paper_validation_monitor(
        **paths,
        now=NOW,
        health_fetcher=_health,
        strong_report_builder=_strong,
        readiness_evaluator=_readiness,
    )

    assert result["active_bot"]["id"] == "bot1"
    assert result["paper_orders_today"] == 1
    assert result["closed_paper_trades_since_clean_start"] == 1
    assert result["max_daily_trades_status"]["trade_count"] == 0


def test_daily_monitor_excludes_isolated_paper_smoke_rows_from_section4(tmp_path):
    paths = _fixture(tmp_path)
    with sqlite3.connect(paths["db_path"]) as connection:
        connection.execute("UPDATE bot_instances SET status='stopped' WHERE id='bot1'")
        connection.executemany(
            """
            INSERT INTO decision_traces VALUES
              (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    "real_attempt",
                    "c2",
                    "bot1",
                    "BTCUSDT",
                    "2026-06-16T05:57:00+00:00",
                    "WEAK_TREND",
                    "buy",
                    "EXECUTE",
                    "",
                    "PROTECTIVE_ORDERS_VALIDATED",
                    "PAPER_POSITION_OPENED",
                    None,
                    1,
                    None,
                    None,
                    0,
                ),
                (
                    "smoke_attempt",
                    "paper_smoke_cycle",
                    "paper_smoke",
                    "BTCUSDT",
                    "2026-06-16T05:58:00+00:00",
                    "SMOKE_TEST",
                    "buy",
                    "BUY",
                    "PAPER_EXECUTION_SMOKE",
                    "PAPER_EXECUTION_SMOKE",
                    "PAPER_POSITION_OPENED",
                    None,
                    1,
                    None,
                    None,
                    0,
                ),
            ],
        )
        connection.executemany(
            """
            INSERT INTO trade_fills
              (bot_instance_id, symbol, action, timestamp_utc)
            VALUES (?,?,?,?)
            """,
            [
                ("bot1", "BTCUSDT", "OPEN", "2026-06-16T05:57:00+00:00"),
                ("paper_smoke", "BTCUSDT", "OPEN", "2026-06-16T05:58:00+00:00"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO position_lifecycle_state
              (bot_instance_id, phase, exchange_position_active)
            VALUES (?,?,?)
            """,
            [
                ("bot1", "SEEKING_TP1", 1),
                ("paper_smoke", "SEEKING_TP1", 1),
            ],
        )

    result = run_daily_paper_validation_monitor(
        **paths,
        now=NOW,
        health_fetcher=_health,
        strong_report_builder=_strong,
        readiness_evaluator=_readiness,
    )

    assert result["active_bot"] == {}
    assert result["paper_orders_today"] == 1
    assert result["paper_fills_today"] == 1
    assert result["active_positions"] == 1
    assert "SMOKE_TEST" not in result["latest_regime_distribution"]


def test_daily_monitor_strong_trend_metrics_are_internally_consistent(tmp_path):
    def strong_with_attempt_note(_db, start):
        payload = _strong(_db, start)
        payload.update(
            {
                "strong_trend_signals": 0,
                "strong_trend_order_attempts": 2,
                "strong_trend_paper_orders_created": 0,
                "strong_trend_paper_orders": 0,
                "strong_trend_order_errors": 2,
                "order_consistency_note": "failed attempts are not created orders",
                "stop_recommended": True,
                "stop_reason": "strong_trend_order_errors_since_experiment_start >= 2",
            }
        )
        return payload

    result, _ = _run(tmp_path, strong_report_builder=strong_with_attempt_note)

    assert result["strong_trend_signals"] == 0
    assert result["strong_trend_order_attempts"] == 2
    assert result["strong_trend_paper_orders_created"] == 0
    assert result["strong_trend_order_errors"] == 2
    assert "failed attempts are not created orders" in result[
        "strong_trend_order_consistency_note"
    ]


def test_monitor_failure_does_not_raise_and_writes_failed_report(tmp_path):
    paths = _fixture(tmp_path)
    result = run_daily_paper_validation_monitor(
        **paths,
        now=NOW,
        health_fetcher=_health,
        strong_report_builder=MagicMock(side_effect=RuntimeError("experiment failed")),
        readiness_evaluator=_readiness,
    )
    assert result["pipeline_error"] == "experiment failed"
    assert paths["output_json"].exists()
    assert "DAILY_MONITOR_FAILED" in [alert["alert_name"] for alert in result["alerts"]]


def test_missing_section4_status_does_not_escape_scheduler_boundary(tmp_path):
    paths = _fixture(tmp_path)
    paths["section4_status"].unlink()
    result = run_daily_paper_validation_monitor(
        **paths,
        now=NOW,
        health_fetcher=_health,
        strong_report_builder=_strong,
        readiness_evaluator=_readiness,
    )
    assert result["pipeline_error"]
    assert paths["output_json"].exists()
    assert "DAILY_MONITOR_FAILED" in [alert["alert_name"] for alert in result["alerts"]]


def test_scheduler_registers_daily_monitor_job():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    import app.main as main_mod

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    scheduler = MagicMock(spec=AsyncIOScheduler)
    jobs = []
    scheduler.add_job.side_effect = lambda fn, trigger=None, **kwargs: jobs.append(
        {"fn": fn, "trigger": trigger, "kwargs": kwargs}
    )
    original = main_mod._signal_scheduler
    main_mod._signal_scheduler = None
    try:
        with patch("apscheduler.schedulers.asyncio.AsyncIOScheduler", return_value=scheduler):
            loop.run_until_complete(main_mod._startup_signal_scheduler())
    finally:
        main_mod._signal_scheduler = original
        loop.close()
        asyncio.set_event_loop(None)
    job = next(
        item
        for item in jobs
        if item["kwargs"].get("id") == "daily_paper_validation_monitor"
    )
    assert job["trigger"] == "cron"
    assert job["kwargs"]["hour"] == 23
    assert job["kwargs"]["minute"] == 30
