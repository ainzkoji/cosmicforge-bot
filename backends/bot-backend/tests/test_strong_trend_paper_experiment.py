from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

from app.core.strong_trend_guard import evaluate_strong_trend_guard
from app.core.config import settings
from app.symbols.universe import parse_symbols
from scripts.validation.monitor_strong_trend_experiment import (
    build_order_count_diagnosis,
    build_report,
    calculate_metrics,
    run_monitor,
    stop_recommendation,
)


def config(**overrides):
    values = {
        "EXECUTION_MODE": "paper",
        "ML_ENABLED": False,
        "LIVE_SYMBOLS": "",
        "MAX_SYMBOLS": 2,
        "ENSEMBLE_BLOCKED_REGIMES": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_strong_trend_unblocked_allowed_only_in_safe_paper_mode():
    status = evaluate_strong_trend_guard(config())
    assert status.effective_unblocked is True
    assert status.allowed_only_in_paper is True


def test_active_experiment_safety_settings_remain_restricted():
    assert settings.EXECUTION_MODE == "paper"
    assert settings.ML_ENABLED is False
    assert settings.IOFS_GATE_MODE == "shadow"
    assert parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS) == [
        "BTCUSDT",
        "ETHUSDT",
    ]
    assert parse_symbols(settings.LIVE_SYMBOLS, settings.MAX_SYMBOLS) == []


def test_strong_trend_unblocked_is_forced_blocked_outside_paper():
    for unsafe in (
        config(EXECUTION_MODE="live"),
        config(ML_ENABLED=True),
        config(LIVE_SYMBOLS="BTCUSDT"),
    ):
        status = evaluate_strong_trend_guard(unsafe)
        assert status.forced_blocked is True
        assert "STRONG_TREND" in status.effective_blocked_regimes


def test_runner_execution_mode_override_cannot_bypass_guard():
    status = evaluate_strong_trend_guard(config(), execution_mode="live")
    assert status.forced_blocked is True
    assert status.effective_unblocked is False


def test_stop_recommendation_after_three_consecutive_losses():
    metrics = calculate_metrics([{"r_multiple": -1, "exit_reason": "SL"}] * 3)
    stop, reasons = stop_recommendation(metrics, 0)
    assert stop is True
    assert "three consecutive STRONG_TREND losses" in reasons


def test_stop_recommendation_when_pf_below_one_after_five_closes():
    metrics = calculate_metrics(
        [{"r_multiple": value, "exit_reason": "SL"} for value in (0.5, -1, -1, 0.25, -1)]
    )
    stop, reasons = stop_recommendation(metrics, 0)
    assert stop is True
    assert any("profit_factor < 1.0" in reason for reason in reasons)


def test_stop_recommendation_when_drawdown_reaches_three_r():
    metrics = calculate_metrics([{"r_multiple": -1, "exit_reason": "SL"}] * 3)
    stop, reasons = stop_recommendation(metrics, 0)
    assert stop is True
    assert "max_drawdown_R >= 3.0" in reasons


def test_monitor_generates_report_without_touching_production_models(tmp_path, monkeypatch):
    db_path = tmp_path / "experiment.db"
    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE decision_traces (
                trace_id TEXT, cycle_id TEXT, symbol TEXT, ts TEXT, regime_state TEXT,
                intended_action TEXT, signal TEXT, submit_attempted INTEGER,
                execution_error TEXT, rejection_reason TEXT, execution_status TEXT
            );
            CREATE TABLE trade_fills (
                id INTEGER PRIMARY KEY, position_id TEXT, trace_id TEXT, symbol TEXT,
                action TEXT, timestamp_utc TEXT, r_multiple REAL, exit_reason TEXT,
                exit_regime TEXT
            );
            INSERT INTO decision_traces VALUES
                ('t1','c1','BTCUSDT','2026-06-15T18:00:01+00:00','STRONG_TREND','SELL','sell',0,NULL,NULL,'None');
            """
        )
    experiment = tmp_path / ".env.paper_strong_trend_experiment"
    experiment.write_text(
        "STRONG_TREND_EXPERIMENT_START_TIME=2026-06-15T18:00:00+00:00\n",
        encoding="utf-8",
    )
    active_env = tmp_path / ".env"
    active_env.write_text(
        "EXECUTION_MODE=paper\nML_ENABLED=False\nLIVE_SYMBOLS=\nENSEMBLE_BLOCKED_REGIMES=\n",
        encoding="utf-8",
    )
    production = tmp_path / "models/production"
    production.mkdir(parents=True)
    marker = production / "README.md"
    marker.write_text("unchanged", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    report = run_monitor(
        db_path=db_path,
        experiment_config=experiment,
        active_env=active_env,
        output_md=tmp_path / "status.md",
        output_json=tmp_path / "status.json",
        output_diagnosis_md=tmp_path / "diagnosis.md",
        output_diagnosis_json=tmp_path / "diagnosis.json",
    )

    assert report["strong_trend_cycles"] == 1
    assert (tmp_path / "status.md").exists()
    assert (tmp_path / "status.json").exists()
    assert (tmp_path / "diagnosis.md").exists()
    assert (tmp_path / "diagnosis.json").exists()
    assert marker.read_text(encoding="utf-8") == "unchanged"


def _experiment_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "experiment.db"
    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE bot_instances (
                id TEXT, status TEXT, mode TEXT, last_run_at TEXT, last_error TEXT,
                active_positions INTEGER
            );
            CREATE TABLE decision_traces (
                trace_id TEXT, run_id TEXT, cycle_id TEXT, bot_instance_id TEXT,
                symbol TEXT, ts TEXT, regime_state TEXT, intended_action TEXT,
                signal TEXT, submit_attempted INTEGER, execution_error TEXT,
                rejection_reason TEXT, execution_status TEXT, order_id TEXT,
                fill_recorded INTEGER, position_opened INTEGER
            );
            CREATE TABLE trade_fills (
                id INTEGER PRIMARY KEY, trace_id TEXT, bot_instance_id TEXT,
                symbol TEXT, action TEXT, timestamp_utc TEXT, r_multiple REAL,
                exit_reason TEXT, exit_regime TEXT
            );
            INSERT INTO bot_instances VALUES
              ('bot_active','active','paper','2026-06-16T00:00:00+00:00',NULL,0),
              ('bot_old','stopped','paper','2026-06-15T00:00:00+00:00',NULL,0);
            """
        )
    return db_path


def _insert_trace(
    db_path: Path,
    trace_id: str,
    *,
    bot_instance_id: str = "bot_active",
    ts: str = "2026-06-16T12:00:00+00:00",
    symbol: str = "BTCUSDT",
    regime: str = "STRONG_TREND",
    signal: str = "buy",
    intended_action: str = "EXECUTE",
    submit_attempted: int = 1,
    execution_status: str = "PAPER_ONLY",
    execution_error: str | None = "Paper mode (paper) - no execution",
    rejection_reason: str | None = "BROKER_REJECT: Paper mode (paper) - no execution",
    order_id: str | None = None,
    fill_recorded: int = 0,
    position_opened: int = 0,
) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO decision_traces VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                trace_id,
                bot_instance_id,
                f"cycle_{trace_id}",
                bot_instance_id,
                symbol,
                ts,
                regime,
                intended_action,
                signal,
                submit_attempted,
                execution_error,
                rejection_reason,
                execution_status,
                order_id,
                fill_recorded,
                position_opened,
            ),
        )


def test_old_pre_experiment_orders_are_not_counted_as_experiment_orders(tmp_path):
    db_path = _experiment_db(tmp_path)
    _insert_trace(db_path, "old_attempt", ts="2026-06-15T10:00:00+00:00")

    report = build_report(db_path, "2026-06-15T17:21:40.112591Z")
    diagnosis = build_order_count_diagnosis(
        db_path,
        "2026-06-15T17:21:40.112591Z",
        clean_start_time="2026-06-15T04:46:39.294507Z",
    )

    assert report["strong_trend_order_attempts"] == 0
    assert report["strong_trend_paper_orders_created"] == 0
    assert diagnosis["summary"]["old_orders_before_experiment"] == 1


def test_failed_attempts_are_separate_from_created_paper_orders_and_stop_rule(tmp_path):
    db_path = _experiment_db(tmp_path)
    _insert_trace(db_path, "failed_1")
    _insert_trace(db_path, "failed_2", signal="sell")

    report = build_report(db_path, "2026-06-15T17:21:40.112591Z")

    assert report["strong_trend_signals"] == 2
    assert report["strong_trend_order_attempts"] == 2
    assert report["strong_trend_paper_orders_created"] == 0
    assert report["strong_trend_order_errors"] == 2
    assert report["stop_recommended"] is True
    assert report["stop_reason"] == "strong_trend_order_errors_since_experiment_start >= 2"


def test_successful_paper_orders_are_counted_as_created_not_errors(tmp_path):
    db_path = _experiment_db(tmp_path)
    _insert_trace(
        db_path,
        "created_1",
        execution_status="PAPER_POSITION_OPENED",
        execution_error=None,
        rejection_reason=None,
        order_id="paper_order_1",
        fill_recorded=1,
        position_opened=1,
    )

    report = build_report(db_path, "2026-06-15T17:21:40.112591Z")

    assert report["strong_trend_order_attempts"] == 1
    assert report["strong_trend_paper_orders_created"] == 1
    assert report["strong_trend_order_errors"] == 0
    assert report["stop_recommended"] is False


def test_zero_signals_and_zero_attempts_do_not_trigger_order_error_stop(tmp_path):
    db_path = _experiment_db(tmp_path)
    _insert_trace(
        db_path,
        "hold_1",
        signal="HOLD",
        intended_action="HOLD",
        submit_attempted=0,
        execution_status="None",
        execution_error=None,
        rejection_reason=None,
    )

    report = build_report(db_path, "2026-06-15T17:21:40.112591Z")

    assert report["strong_trend_signals"] == 0
    assert report["strong_trend_order_attempts"] == 0
    assert report["strong_trend_order_errors"] == 0
    assert report["stop_recommended"] is False


def test_strong_monitor_filters_to_active_bot_instance_where_available(tmp_path):
    db_path = _experiment_db(tmp_path)
    _insert_trace(db_path, "active_attempt")
    _insert_trace(db_path, "old_bot_attempt", bot_instance_id="bot_old")

    report = build_report(db_path, "2026-06-15T17:21:40.112591Z")
    diagnosis = build_order_count_diagnosis(
        db_path,
        "2026-06-15T17:21:40.112591Z",
        clean_start_time="2026-06-15T04:46:39.294507Z",
    )

    assert report["filtering"]["active_bot_instance_id"] == "bot_active"
    assert report["strong_trend_order_attempts"] == 1
    assert diagnosis["summary"]["wrong_bot_instance_orders"] == 1


def test_monitor_default_does_not_modify_active_env_when_stop_is_recommended(tmp_path):
    db_path = _experiment_db(tmp_path)
    _insert_trace(db_path, "failed_1")
    _insert_trace(db_path, "failed_2", signal="sell")
    experiment = tmp_path / ".env.paper_strong_trend_experiment"
    experiment.write_text(
        "STRONG_TREND_EXPERIMENT_START_TIME=2026-06-15T17:21:40.112591Z\n",
        encoding="utf-8",
    )
    active_env = tmp_path / ".env"
    before = "EXECUTION_MODE=paper\nML_ENABLED=False\nLIVE_SYMBOLS=\nENSEMBLE_BLOCKED_REGIMES=\n"
    active_env.write_text(before, encoding="utf-8")
    section4 = tmp_path / "iofs_paper_validation_status.md"
    section4.write_text(
        "- start_timestamp_utc: 2026-06-15T04:46:39.294507Z\n",
        encoding="utf-8",
    )

    report = run_monitor(
        db_path=db_path,
        experiment_config=experiment,
        active_env=active_env,
        output_md=tmp_path / "status.md",
        output_json=tmp_path / "status.json",
        output_diagnosis_md=tmp_path / "diagnosis.md",
        output_diagnosis_json=tmp_path / "diagnosis.json",
        section4_status=section4,
    )

    assert report["stop_recommended"] is True
    assert report["auto_restore_performed"] is False
    assert active_env.read_text(encoding="utf-8") == before
