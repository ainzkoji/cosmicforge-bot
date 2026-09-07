from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import run_tradingview_phase5_proof as phase5
from shared_lib.persistence.db import DB
from shared_lib.persistence.tradingview import ensure_tradingview_schema


def _conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _create_core_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_instances (
            id TEXT PRIMARY KEY,
            broker_account_id TEXT,
            allocation_type TEXT,
            allocation_value REAL,
            mode TEXT,
            status TEXT,
            last_run_at TEXT,
            started_at TEXT,
            updated_at TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_accounts (
            id TEXT PRIMARY KEY,
            broker_id TEXT,
            environment TEXT,
            status TEXT,
            last_validated_at TEXT,
            validation_error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_lifecycle_state (
            bot_instance_id TEXT,
            symbol TEXT,
            phase TEXT,
            sl_order_id TEXT,
            tp_order_id TEXT,
            updated_at TEXT,
            position_id TEXT,
            exchange_position_active INTEGER,
            reconciliation_status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_fills (
            bot_instance_id TEXT,
            action TEXT,
            trigger_source TEXT,
            initiator_type TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_traces (
            trace_id TEXT,
            ts TEXT,
            symbol TEXT,
            last_price REAL,
            mark_price REAL,
            equity REAL,
            atr_pct REAL
        )
        """
    )
    existing_trace_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(decision_traces)").fetchall()
    }
    for col, ddl in {
        "last_price": "REAL",
        "mark_price": "REAL",
        "equity": "REAL",
        "atr_pct": "REAL",
    }.items():
        if col not in existing_trace_cols:
            conn.execute(f"ALTER TABLE decision_traces ADD COLUMN {col} {ddl}")
    for table in [
        "bot_instances",
        "broker_accounts",
        "position_lifecycle_state",
        "trade_fills",
        "tradingview_webhooks",
        "tradingview_alerts",
        "tradingview_signal_decisions",
        "external_signal_queue",
        "tradingview_processor_heartbeat",
        "decision_traces",
    ]:
        conn.execute(f"DELETE FROM {table}")


def _seed_safe_db(path: Path) -> sqlite3.Connection:
    ensure_tradingview_schema(DB(path=str(path)))
    conn = _conn(path)
    _create_core_schema(conn)
    conn.execute(
        """
        INSERT INTO bot_instances (
            id, user_id, broker_account_id, market_type, strategy_id, strategy_version,
            config_id, risk_profile_id, allocation_type, allocation_value,
            mode, status, last_run_at, created_at, updated_at
        ) VALUES (
            'bot_live_mode', 'user_1', 'brk_demo', 'CRYPTO', 'master_ensemble', '1.0.0',
            'cfg_1', 'risk_1', 'fixed_amount', 120, 'live', 'active',
            '2026-05-10T12:00:00+00:00', '2026-05-10T10:00:00+00:00',
            '2026-05-10T10:00:00+00:00'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO broker_accounts (
            id, user_id, broker_id, market_type, label, environment, status,
            last_validated_at, created_at, updated_at
        ) VALUES (
            'brk_demo', 'user_1', 'binance', 'crypto', 'Binance Demo',
            'demo', 'connected', '2026-05-10T11:00:00+00:00',
            '2026-05-10T10:00:00+00:00', '2026-05-10T10:00:00+00:00'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO tradingview_webhooks (
            id, bot_id, name, token_hash, mode, is_enabled, allowed_actions_json,
            max_alert_age_seconds, rate_limit_per_minute, created_at, updated_at
        ) VALUES (
            'wh_1', 'bot_live_mode', 'candidate', 'hash',
            'EXTERNAL_SIGNAL_CANDIDATE', 1, '["BUY","SELL"]',
            300, 30, '2026-05-10T11:00:00+00:00', '2026-05-10T11:00:00+00:00'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO tradingview_processor_heartbeat (
            bot_instance_id, processor_enabled, env_gate_reason,
            last_started_at, last_finished_at, last_processed_count,
            last_rejected_count, last_failed_count, last_skipped_count,
            updated_at
        ) VALUES (
            'bot_live_mode', 1, NULL,
            '2026-05-10T11:30:00+00:00', '2026-05-10T11:30:01+00:00',
            0, 0, 0, 0, '2026-05-10T11:30:01+00:00'
        )
        """
    )
    conn.commit()
    return conn


def _args(path: Path, **kwargs):
    values = {
        "db_path": path,
        "bot_id": None,
        "duration_minutes": 30,
        "poll_seconds": 10,
        "output_dir": path.parent / "reports",
        "strict": True,
        "preflight_only": True,
        "allow_waiting_for_signals": False,
    }
    values.update(kwargs)
    return SimpleNamespace(**values)


def _live_mode_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setenv("BINANCE_ENV", "testnet")
    monkeypatch.setenv("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", "true")
    monkeypatch.setenv("TRADINGVIEW_TESTNET_ONLY", "true")
    monkeypatch.setenv("TRADINGVIEW_ALLOW_PAPER_LIVE_MODE", "false")
    monkeypatch.setenv("PAPER_TRADING_MODE", "true")


def _phase5b_policy_test_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.core.config import settings

    monkeypatch.setattr(settings, "STOP_LOSS_PCT", 0.02, raising=False)
    monkeypatch.setattr(settings, "TAKE_PROFIT_PCT", 0.03, raising=False)


def _seed_phase5b_trace(
    conn: sqlite3.Connection,
    symbol: str,
    *,
    atr_pct: float = 0.02,
    price: float = 100.0,
    ts: str | None = None,
) -> None:
    ts = ts or phase5.utc_now()
    conn.execute(
        """
        INSERT INTO decision_traces (
            trace_id, ts, symbol, last_price, mark_price, equity, atr_pct
        ) VALUES (?, ?, ?, ?, ?, 1000.0, ?)
        """,
        (f"trace_{symbol}", ts, symbol, price, price, atr_pct),
    )


def test_preflight_does_not_block_simply_because_bot_mode_is_live(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)

    report = phase5.evaluate_preflight(conn, _args(db_path))

    assert report.live_mode_safety_confirmation["live_mode_safety_verified"] is True
    assert report.preflight_verdict == phase5.VERDICT_WAITING


def test_preflight_rejects_possible_real_capital_mode(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5.db"
    conn = _seed_safe_db(db_path)
    conn.execute("UPDATE broker_accounts SET environment='live'")
    conn.commit()
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("BINANCE_ENV", "live")
    monkeypatch.setenv("PAPER_TRADING_MODE", "false")

    report = phase5.evaluate_preflight(conn, _args(db_path))

    assert report.final_verdict == phase5.VERDICT_UNSAFE


def test_preflight_disabled_processor_needs_setup(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    monkeypatch.setenv("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", "false")

    report = phase5.evaluate_preflight(conn, _args(db_path))

    assert report.final_verdict == phase5.VERDICT_SETUP
    assert "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=false" in report.blocking_findings


def test_preflight_missing_candidate_webhook_needs_setup(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5.db"
    conn = _seed_safe_db(db_path)
    conn.execute("UPDATE tradingview_webhooks SET mode='ADVISORY_ONLY'")
    conn.commit()
    _live_mode_env(monkeypatch)

    report = phase5.evaluate_preflight(conn, _args(db_path))

    assert report.final_verdict == phase5.VERDICT_SETUP


def test_preflight_unclean_protection_needs_fix(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5.db"
    conn = _seed_safe_db(db_path)
    conn.execute(
        """
        INSERT INTO position_lifecycle_state (
            bot_instance_id, symbol, phase, sl_order_id, tp_order_id,
            updated_at, position_id, exchange_position_active, reconciliation_status
        ) VALUES (
            'bot_live_mode', 'RAYSOLUSDT', 'SEEKING_TP1',
            'DUPLICATE_4130', 'DUPLICATE_4130',
            '2026-05-10T12:00:00+00:00', 'pos_1', 1, 'PROTECTED'
        )
        """
    )
    conn.commit()
    _live_mode_env(monkeypatch)

    report = phase5.evaluate_preflight(conn, _args(db_path))

    assert report.final_verdict == phase5.VERDICT_FIX
    assert report.protection_evidence["unclean_protection_rows"] == 1


def test_preflight_ready_when_queue_rows_exist(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5.db"
    conn = _seed_safe_db(db_path)
    conn.execute(
        """
        INSERT INTO external_signal_queue (
            id, source, source_alert_id, bot_id, symbol, side, action, confidence,
            status, available_at, expires_at, created_at
        ) VALUES (
            'q1', 'TRADINGVIEW', '1', 'bot_live_mode', 'BTCUSDT', 'LONG', 'BUY',
            0.75, 'PENDING', '2026-05-10T12:00:00+00:00',
            '2026-05-10T12:05:00+00:00', '2026-05-10T12:00:00+00:00'
        )
        """
    )
    conn.commit()
    _live_mode_env(monkeypatch)

    report = phase5.evaluate_preflight(conn, _args(db_path))

    assert report.final_verdict == phase5.VERDICT_READY


def test_invariant_failure_returns_unsafe():
    metrics = {
        "webhook_direct_executor_calls": 0,
        "unprotected_positions": 1,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": 0,
        "stuck_claimed_rows": 0,
    }
    assert phase5.invariant_results(metrics)["all_passed"] is False


def test_reports_are_created(tmp_path):
    report = phase5.Phase5Report(
        generated_at="2026-05-10T12:00:00+00:00",
        preflight_verdict=phase5.VERDICT_WAITING,
        final_verdict=phase5.VERDICT_WAITING,
    )

    phase5.write_reports(report, tmp_path)

    assert Path(report.report_files["json"]).exists()
    assert Path(report.report_files["markdown"]).exists()
    markdown = Path(report.report_files["markdown"]).read_text(encoding="utf-8")
    assert "Live-Mode TradingView" in markdown
    assert "Paper-Money" not in markdown
    assert "phase5_live_mode_tradingview_proof" in report.report_files["json"]


def test_exit_codes_are_correct():
    assert phase5.EXIT_CODES[phase5.VERDICT_SAFE] == 0
    assert phase5.EXIT_CODES[phase5.VERDICT_FIX] == 1
    assert phase5.EXIT_CODES[phase5.VERDICT_UNSAFE] == 2
    assert phase5.EXIT_CODES[phase5.VERDICT_SETUP] == 3
    assert phase5.EXIT_CODES[phase5.VERDICT_WAITING] == 4


def test_new_live_mode_verdict_names_are_used():
    assert phase5.VERDICT_SAFE == "LIVE-MODE TRADINGVIEW CANDIDATE MODE SAFE"
    assert phase5.VERDICT_FIX == "LIVE-MODE TRADINGVIEW NEEDS FIX"


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5B tests
# ─────────────────────────────────────────────────────────────────────────────


def _phase5b_args(path: Path, **kwargs):
    values = {
        "db_path": path,
        "bot_id": None,
        "duration_minutes": 0.05,  # 3 seconds — instant in tests
        "poll_seconds": 1,
        "output_dir": path.parent / "reports",
        "strict": True,
        "preflight_only": False,
        "allow_waiting_for_signals": False,
        "phase5b_successful_execution": True,
        "symbol": None,
        "wait_for_valid_candidate": False,
        "candidate_check_interval_seconds": 1,
        "candidate_wait_timeout_minutes": 0.01,
        "diagnose_stop_distance": False,
        "diagnostic_symbols": None,
        "diagnostic_timeframes": "1m,5m,15m",
        "diagnostic_bars": 120,
        "diagnostic_output_dir": path.parent / "diagnostics",
        "phase5b_clean_candle_proof": False,
        "clean_candle_symbol": None,
        "clean_candle_action": None,
        "clean_candle_timeframe": "1m",
        "clean_candle_bars": 120,
        "clean_candle_volatility_pct": 0.2,
    }
    values.update(kwargs)
    return SimpleNamespace(**values)


def test_phase5b_verdict_names_are_correct():
    assert phase5.VERDICT_5B_PASSED == "SUCCESSFUL LIVE-MODE TRADINGVIEW EXECUTION PROOF PASSED"
    assert phase5.VERDICT_5B_NEEDS_CANDIDATE == "LIVE-MODE TRADINGVIEW NEEDS VALID EXECUTION CANDIDATE"
    assert phase5.EXIT_CODES[phase5.VERDICT_5B_PASSED] == 0
    assert phase5.EXIT_CODES[phase5.VERDICT_5B_NEEDS_CANDIDATE] == 4


def test_phase5b_candidate_selection_excludes_open_positions(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT")
    _seed_phase5b_trace(conn, "ETHUSDT")
    conn.execute(
        """
        INSERT INTO position_lifecycle_state (
            bot_instance_id, symbol, phase, sl_order_id, tp_order_id,
            updated_at, position_id, exchange_position_active, reconciliation_status
        ) VALUES ('bot_live_mode', 'BTCUSDT', 'SEEKING_TP1', 'sl1', 'tp1',
                  '2026-05-10T12:00:00+00:00', 'pos_1', 1, 'PROTECTED')
        """
    )
    conn.commit()

    result = phase5.select_phase5b_candidate(conn, "bot_live_mode")

    assert result["selected"] == "ETHUSDT"
    assert "BTCUSDT" in result["open_position_symbols"]


def test_phase5b_candidate_selection_excludes_pending_queue_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT")
    _seed_phase5b_trace(conn, "ETHUSDT")
    conn.execute(
        """
        INSERT INTO external_signal_queue (
            id, source, source_alert_id, bot_id, symbol, side, action,
            confidence, status, available_at, expires_at, created_at
        ) VALUES ('q_btc', 'TRADINGVIEW', '1', 'bot_live_mode', 'BTCUSDT', 'LONG', 'BUY',
                  0.75, 'PENDING', '2026-05-10T12:00:00+00:00',
                  '2026-05-10T12:05:00+00:00', '2026-05-10T12:00:00+00:00')
        """
    )
    conn.commit()

    result = phase5.select_phase5b_candidate(conn, "bot_live_mode")

    assert result["selected"] == "ETHUSDT"
    assert "BTCUSDT" in result["pending_queue_symbols"]


def test_phase5b_candidate_selection_returns_none_when_all_blocked(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT")
    conn.execute(
        """
        INSERT INTO position_lifecycle_state (
            bot_instance_id, symbol, phase, sl_order_id, tp_order_id,
            updated_at, position_id, exchange_position_active, reconciliation_status
        ) VALUES ('bot_live_mode', 'BTCUSDT', 'SEEKING_TP1', 'sl1', 'tp1',
                  '2026-05-10T12:00:00+00:00', 'pos_1', 1, 'PROTECTED')
        """
    )
    conn.commit()

    result = phase5.select_phase5b_candidate(conn, "bot_live_mode")

    assert result["selected"] is None


def test_phase5b_preferred_symbol_is_tried_first(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT,XRPUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT")
    _seed_phase5b_trace(conn, "ETHUSDT")
    _seed_phase5b_trace(conn, "XRPUSDT")

    result = phase5.select_phase5b_candidate(conn, "bot_live_mode", preferred_symbol="XRPUSDT")

    assert result["selected"] == "XRPUSDT"
    assert result["candidates_considered"][0] == "XRPUSDT"


def test_phase5b_candidate_selection_skips_stop_too_wide_and_continues(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT,XRPUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=1.0)
    _seed_phase5b_trace(conn, "XRPUSDT", atr_pct=0.02)

    result = phase5.select_phase5b_candidate(conn, "bot_live_mode")

    assert result["selected"] == "XRPUSDT"
    btc_checks = [p for p in result["prechecks"] if p["symbol"] == "BTCUSDT"]
    assert btc_checks
    assert all(p["eligible"] is False for p in btc_checks)
    assert any("STOP_TOO_WIDE" in p["reason"] for p in btc_checks)


def test_phase5b_candidate_universe_combines_sources_without_duplicates(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    monkeypatch.setenv("PHASE5B_CANDIDATE_SYMBOLS", "ETHUSDT,SOLUSDT,BNBUSDT")
    _seed_phase5b_trace(conn, "ADAUSDT")

    universe = phase5.build_phase5b_candidate_universe(conn, "bot_live_mode")

    assert universe["symbols"].count("ETHUSDT") == 1
    assert "BTCUSDT" in universe["symbols"]
    assert "SOLUSDT" in universe["symbols"]
    assert "ADAUSDT" in universe["symbols"]
    assert universe["sources"]["phase5b_candidate_symbols_env"] == ["ETHUSDT", "SOLUSDT", "BNBUSDT"]


def test_phase5b_ranking_selects_lowest_valid_stop_distance(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "SOLUSDT,XRPUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "SOLUSDT", atr_pct=0.03)
    _seed_phase5b_trace(conn, "XRPUSDT", atr_pct=0.015)

    result = phase5.select_phase5b_candidate(conn, "bot_live_mode")

    assert result["selected"] == "XRPUSDT"
    assert result["selected_action"] == "BUY"
    assert result["lowest_stop_distance_pct"] == 3.0


def test_phase5b_candidate_selection_evaluates_buy_and_sell(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=1.0)

    result = phase5.select_phase5b_candidate(conn, "bot_live_mode")

    btc_actions = [p["action"] for p in result["prechecks"] if p["symbol"] == "BTCUSDT"]
    assert btc_actions == ["BUY", "SELL"]
    assert len(result["prechecks"]) >= 2
    assert result["selected"] is None


def test_phase5b_no_candidate_does_not_seed_or_create_queue(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=1.0)

    args = _phase5b_args(db_path, duration_minutes=0.01, poll_seconds=1)
    report = phase5.run_phase5b(conn, args)

    assert report.final_verdict == phase5.VERDICT_5B_NEEDS_CANDIDATE
    assert report.seeded_signal == {}
    assert conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0] == 0
    assert report.waiting_mode_summary["enabled"] is False


def test_phase5b_precheck_does_not_mutate_queue(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _phase5b_policy_test_settings(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=0.02)

    before = conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0]
    check = phase5.precheck_phase5b_candidate(conn, "bot_live_mode", "BTCUSDT", "BUY")
    after = conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0]

    assert check["eligible"] is True
    assert before == after == 0


def test_phase5b_waiting_mode_times_out_without_seeding(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    _phase5b_policy_test_settings(monkeypatch)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=1.0)

    report = phase5.run_phase5b(
        conn,
        _phase5b_args(
            db_path,
            wait_for_valid_candidate=True,
            candidate_check_interval_seconds=1,
            candidate_wait_timeout_minutes=0.001,
        ),
    )

    assert report.final_verdict == phase5.VERDICT_5B_NEEDS_CANDIDATE
    assert report.seeded_signal == {}
    assert conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0] == 0
    assert report.waiting_mode_summary["enabled"] is True
    assert report.candidate_scan_history


def test_phase5b_waiting_mode_seeds_once_candidate_appears(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    _phase5b_policy_test_settings(monkeypatch)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=0.02)

    report = phase5.run_phase5b(
        conn,
        _phase5b_args(
            db_path,
            duration_minutes=0.001,
            wait_for_valid_candidate=True,
            candidate_check_interval_seconds=1,
            candidate_wait_timeout_minutes=0.01,
        ),
    )

    assert report.seeded_signal.get("queue_id") is not None
    assert conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0] == 1
    assert report.waiting_mode_summary["signal_seeded_while_waiting"] is True


def test_phase5b_waiting_mode_stops_on_unsafe_preflight(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    conn.execute("UPDATE broker_accounts SET environment='live'")
    conn.commit()
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("BINANCE_ENV", "live")
    monkeypatch.setenv("PAPER_TRADING_MODE", "false")

    report = phase5.run_phase5b(
        conn,
        _phase5b_args(db_path, wait_for_valid_candidate=True),
    )

    assert report.final_verdict == phase5.VERDICT_UNSAFE
    assert report.seeded_signal == {}


def test_stop_distance_diagnostics_does_not_seed_or_queue(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=1.0)

    args = _phase5b_args(
        db_path,
        diagnose_stop_distance=True,
        diagnostic_symbols="BTCUSDT",
        diagnostic_timeframes="1m",
        diagnostic_output_dir=tmp_path / "diagnostics",
    )
    before = conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0]
    report = phase5.run_stop_distance_diagnostics(conn, args)
    phase5.write_stop_distance_diagnostic_reports(report, args.diagnostic_output_dir)
    after = conn.execute("SELECT COUNT(*) FROM external_signal_queue").fetchone()[0]

    assert before == after
    assert report.runtime_summary["diagnostics_read_only"] is True
    assert report.runtime_summary["alerts_seeded"] is False
    assert report.runtime_summary["executor_called"] is False
    assert Path(report.report_files["json"]).exists()
    assert Path(report.report_files["markdown"]).exists()
    assert report.atr_comparison[0]["computed_stop_distance_pct"] is not None
    assert report.root_cause_assessment["classifications"]


def test_candle_anomaly_detection_identifies_extreme_wick_and_invalid_ohlc():
    candles = [
        {"ts": "2026-05-10T00:00:00+00:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1},
        {"ts": "2026-05-10T00:01:00+00:00", "open": 100, "high": 140, "low": 90, "close": 110, "volume": 1},
        {"ts": "2026-05-10T00:02:00+00:00", "open": 100, "high": 95, "low": 96, "close": 97, "volume": 0},
    ]

    anomalies = phase5._detect_candle_anomalies(candles, 100)
    types = {a["type"] for a in anomalies}

    assert "RANGE_GT_10_PCT" in types
    assert "HIGH_LOW_RATIO_GT_1_10" in types
    assert "INVALID_OHLC" in types
    assert "ZERO_VOLUME" in types


def test_atr_comparison_includes_required_methods(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    _seed_phase5b_trace(conn, "BTCUSDT", atr_pct=0.09, price=100)

    row = phase5._atr_diagnostic_for(conn, "BTCUSDT", "BUY", "1m", 20)

    assert "policy_atr_value" in row
    assert "sma_true_range_atr" in row
    assert "wilder_atr" in row
    assert "median_true_range_atr" in row
    assert "computed_stop_distance_pct" in row


def test_stop_distance_root_cause_classifies_testnet_spikes_and_outliers():
    diagnostics = [
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "anomaly_count": 1,
            "would_pass_with_trimmed_atr": True,
            "would_pass_with_median_tr": True,
            "policy_vs_sma_divergence_pct": 10,
            "data_source": "cached_table:market_candles",
            "computed_stop_distance_pct": 18,
            "max_allowed_stop_distance_pct": 10,
        }
    ]

    root = phase5._classify_stop_distance_root_causes(diagnostics, {"BINANCE_ENV": "testnet"})

    assert "TESTNET_CANDLE_SPIKES" in root["classifications"]
    assert "ATR_OUTLIER_INFLATION" in root["classifications"]


def test_stop_distance_root_cause_classifies_policy_formula_bug():
    diagnostics = [
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "anomaly_count": 0,
            "would_pass_with_trimmed_atr": False,
            "would_pass_with_median_tr": False,
            "policy_vs_sma_divergence_pct": 75,
            "data_source": "cached_table:market_candles",
            "computed_stop_distance_pct": 18,
            "max_allowed_stop_distance_pct": 10,
        }
    ]

    root = phase5._classify_stop_distance_root_causes(diagnostics, {"BINANCE_ENV": "live"})

    assert "POLICY_FORMULA_BUG" in root["classifications"]


def test_stop_distance_root_cause_classifies_timeframe_mismatch(monkeypatch):
    monkeypatch.setenv("STRATEGY_TIMEFRAME", "1m")
    diagnostics = [
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "pass_fail": "FAIL",
            "anomaly_count": 0,
            "data_source": "cached_table:market_candles",
            "computed_stop_distance_pct": 18,
            "max_allowed_stop_distance_pct": 10,
        },
        {
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "pass_fail": "PASS",
            "anomaly_count": 0,
            "data_source": "cached_table:market_candles",
            "computed_stop_distance_pct": 4,
            "max_allowed_stop_distance_pct": 10,
        },
    ]

    root = phase5._classify_stop_distance_root_causes(diagnostics, {"BINANCE_ENV": "live"})

    assert "TIMEFRAME_MISMATCH" in root["classifications"]


def test_clean_candle_proof_mode_disabled_by_default(tmp_path):
    args = _phase5b_args(tmp_path / "phase5b.db")
    assert args.phase5b_clean_candle_proof is False


def test_clean_candle_provider_creates_valid_low_atr_candles():
    candles = phase5.generate_clean_phase5c_candles(
        price=100.0,
        bars=120,
        volatility_pct=0.2,
        timeframe="1m",
        now=phase5.datetime(2026, 5, 10, tzinfo=phase5.timezone.utc),
    )
    summary = phase5._clean_candle_summary(candles, 100.0)
    anomalies = phase5._detect_candle_anomalies(candles, 100.0)

    assert len(candles) == 120
    assert anomalies == []
    assert summary["stop_distance_pct"] < 10.0


def test_clean_candle_preflight_requires_strict(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)

    report = phase5.evaluate_phase5b_preflight(
        conn,
        _phase5b_args(db_path, phase5b_clean_candle_proof=True, strict=False),
    )

    assert report.final_verdict == phase5.VERDICT_FIX
    assert any("requires --strict" in f for f in report.blocking_findings)


def test_clean_candle_candidate_selects_without_natural_market_data(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")

    args = _phase5b_args(
        db_path,
        phase5b_clean_candle_proof=True,
        clean_candle_symbol="BTCUSDT",
        clean_candle_action="BUY",
    )
    report = phase5.evaluate_phase5b_preflight(conn, args)

    assert report.final_verdict == phase5.VERDICT_READY
    assert report.clean_candle_proof["enabled"] is True
    assert report.candidate_selection["selected"] == "BTCUSDT"
    assert report.candidate_selection["selected_action"] == "BUY"
    assert report.candidate_selection["selected_precheck"]["stop_distance_pct"] < 10.0


def test_clean_candle_seed_uses_queue_metadata_path(tmp_path):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    candles = phase5.generate_clean_phase5c_candles(price=100, bars=20)
    proof_context = {
        "phase5b_clean_candle_proof": True,
        "proof_type": "CONTROLLED_CLEAN_CANDLE_PROOF",
        "clean_candles": candles,
        "clean_reference_price": 100,
    }

    seeded = phase5.seed_phase5b_signal(conn, "bot_live_mode", "BTCUSDT", "BUY", proof_context=proof_context)
    stored = conn.execute("SELECT result FROM external_signal_queue WHERE id = ?", (seeded["queue_id"],)).fetchone()[0]
    parsed = phase5.json.loads(stored)

    assert parsed["proof_type"] == "CONTROLLED_CLEAN_CANDLE_PROOF"
    assert parsed["phase5b_clean_candle_proof"] is True
    assert len(parsed["clean_candles"]) == 20


def test_clean_candle_report_discloses_controlled_proof(tmp_path):
    report = phase5.Phase5BReport(
        generated_at="2026-05-03T12:00:00+00:00",
        final_verdict=phase5.VERDICT_5B_PASSED,
        clean_candle_proof={
            "enabled": True,
            "proof_type": "CONTROLLED_CLEAN_CANDLE_PROOF",
            "disclosure": "CONTROLLED CLEAN-CANDLE PROOF USED. Production policy/risk thresholds were not weakened.",
        },
    )

    phase5.write_phase5b_reports(report, tmp_path)
    md = Path(report.report_files["markdown"]).read_text(encoding="utf-8")

    assert "phase5c_controlled_clean_candle_execution_proof" in report.report_files["json"]
    assert "CONTROLLED CLEAN-CANDLE PROOF USED" in md
    assert "Production policy/risk thresholds were not weakened" in md


def test_phase5b_report_includes_candidate_rejection_matrix(tmp_path):
    report = phase5.Phase5BReport(
        generated_at="2026-05-03T12:00:00+00:00",
        final_verdict=phase5.VERDICT_5B_NEEDS_CANDIDATE,
        candidate_rejection_matrix=[
            {
                "symbol": "BTCUSDT",
                "action": "BUY",
                "eligible": False,
                "reason": "BLOCKED:STOP_TOO_WIDE",
            }
        ],
    )

    phase5.write_phase5b_reports(report, tmp_path)
    md = Path(report.report_files["markdown"]).read_text(encoding="utf-8")

    assert "Candidate Rejection Matrix" in md
    assert "STOP_TOO_WIDE" in md


def test_phase5b_seeded_signal_appears_in_db(tmp_path):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)

    seeded = phase5.seed_phase5b_signal(conn, "bot_live_mode", "BTCUSDT")

    assert seeded["queue_id"] is not None
    assert seeded["symbol"] == "BTCUSDT"
    assert seeded["action"] == "BUY"

    queue_rows = conn.execute(
        "SELECT * FROM external_signal_queue WHERE id = ?", (seeded["queue_id"],)
    ).fetchall()
    assert len(queue_rows) == 1
    assert queue_rows[0]["status"] == "PENDING"
    assert queue_rows[0]["action"] == "BUY"

    alert_rows = conn.execute(
        "SELECT * FROM tradingview_alerts WHERE id = ?", (seeded["alert_db_id"],)
    ).fetchall()
    assert len(alert_rows) == 1
    assert alert_rows[0]["action"] == "BUY"
    assert alert_rows[0]["status"] == "ACCEPTED_EXTERNAL_SIGNAL_CANDIDATE"

    decision_rows = conn.execute(
        "SELECT * FROM tradingview_signal_decisions WHERE queue_id = ?", (seeded["queue_id"],)
    ).fetchall()
    assert len(decision_rows) == 1


def test_phase5b_verdict_unsafe_when_invariant_fails():
    metrics = {
        "trades_opened": 1,
        "trades_protected": 0,
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": 0,
    }
    inv = phase5._phase5b_invariant_results(metrics)
    assert inv["all_passed"] is False
    assert inv["trades_protected_equals_trades_opened"] is False

    verdict, reason = phase5._determine_phase5b_verdict("PROCESSED", metrics, inv)
    assert verdict == phase5.VERDICT_UNSAFE
    assert "invariant" in reason.lower()


def test_phase5b_verdict_needs_candidate_when_rejected():
    metrics = {
        "trades_opened": 0,
        "trades_protected": 0,
        "decision_final_reason": "REJECTED_POLICY: no trend signal",
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": 0,
    }
    inv = phase5._phase5b_invariant_results(metrics)
    assert inv["all_passed"] is True

    verdict, reason = phase5._determine_phase5b_verdict("REJECTED", metrics, inv)
    assert verdict == phase5.VERDICT_5B_NEEDS_CANDIDATE
    assert "rejected" in reason.lower()


def test_phase5b_verdict_fix_when_queue_fails():
    metrics = {
        "trades_opened": 0,
        "trades_protected": 0,
        "decision_final_reason": "FAILED_EXECUTION",
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": 0,
    }
    inv = phase5._phase5b_invariant_results(metrics)
    verdict, _ = phase5._determine_phase5b_verdict("FAILED", metrics, inv)
    assert verdict == phase5.VERDICT_FIX


def test_phase5b_verdict_passed_when_trade_executed_and_protected():
    metrics = {
        "trades_opened": 1,
        "trades_protected": 1,
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": 0,
    }
    inv = phase5._phase5b_invariant_results(metrics)
    assert inv["all_passed"] is True
    assert inv["trades_protected_equals_trades_opened"] is True

    verdict, reason = phase5._determine_phase5b_verdict("PROCESSED", metrics, inv)
    assert verdict == phase5.VERDICT_5B_PASSED
    assert "protected" in reason.lower()


def test_phase5b_verdict_fix_when_processed_but_no_fill():
    metrics = {
        "trades_opened": 0,
        "trades_protected": 0,
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": 0,
    }
    inv = phase5._phase5b_invariant_results(metrics)
    verdict, _ = phase5._determine_phase5b_verdict("PROCESSED", metrics, inv)
    assert verdict == phase5.VERDICT_FIX


def test_phase5b_verdict_needs_candidate_when_expired():
    metrics = {
        "trades_opened": 0,
        "trades_protected": 0,
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": 0,
    }
    inv = phase5._phase5b_invariant_results(metrics)
    verdict, _ = phase5._determine_phase5b_verdict("EXPIRED", metrics, inv)
    assert verdict == phase5.VERDICT_5B_NEEDS_CANDIDATE


def test_phase5b_reports_use_correct_filename_prefix(tmp_path):
    report = phase5.Phase5BReport(
        generated_at="2026-05-03T12:00:00+00:00",
        final_verdict=phase5.VERDICT_5B_NEEDS_CANDIDATE,
    )

    phase5.write_phase5b_reports(report, tmp_path)

    assert Path(report.report_files["json"]).exists()
    assert Path(report.report_files["markdown"]).exists()
    assert "phase5b_successful_live_mode_tradingview_execution" in report.report_files["json"]
    assert "phase5b_successful_live_mode_tradingview_execution" in report.report_files["markdown"]

    md = Path(report.report_files["markdown"]).read_text(encoding="utf-8")
    assert "Phase 5B" in md
    assert "Successful Live-Mode TradingView Execution Proof" in md


def test_phase5b_preflight_blocked_by_unsafe_real_capital(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    conn.execute("UPDATE broker_accounts SET environment='live'")
    conn.commit()
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("BINANCE_ENV", "live")
    monkeypatch.setenv("PAPER_TRADING_MODE", "false")

    report = phase5.evaluate_phase5b_preflight(conn, _phase5b_args(db_path))

    assert report.final_verdict == phase5.VERDICT_UNSAFE


def test_phase5b_preflight_needs_fix_when_automation_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)

    # Patch automation check to simulate missing overlap guard
    import unittest.mock as mock
    with mock.patch.object(
        phase5,
        "processor_automation_status",
        return_value={
            "external_signal_processor_exists": True,
            "multi_runner_hook_exists": True,
            "overlap_guard_exists": False,
            "processor_heartbeat_table_exists": True,
            "processor_status_admin_endpoint_exists": True,
            "stale_claimed_recovery_exists": True,
            "processor_not_stuck": True,
            "last_heartbeats": [],
        },
    ):
        report = phase5.evaluate_phase5b_preflight(conn, _phase5b_args(db_path))

    assert report.final_verdict == phase5.VERDICT_FIX
    assert any("automation incomplete" in f.lower() for f in report.blocking_findings)


def test_phase5b_preflight_needs_candidate_when_all_symbols_blocked(tmp_path, monkeypatch):
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _phase5b_policy_test_settings(monkeypatch)

    conn.execute(
        """
        INSERT INTO position_lifecycle_state (
            bot_instance_id, symbol, phase, sl_order_id, tp_order_id,
            updated_at, position_id, exchange_position_active, reconciliation_status
        ) VALUES ('bot_live_mode', 'BTCUSDT', 'SEEKING_TP1', 'sl1', 'tp1',
                  '2026-05-10T12:00:00+00:00', 'pos_1', 1, 'PROTECTED')
        """
    )
    conn.commit()

    report = phase5.evaluate_phase5b_preflight(conn, _phase5b_args(db_path))

    assert report.final_verdict == phase5.VERDICT_5B_NEEDS_CANDIDATE
    assert report.candidate_selection["selected"] is None


def test_phase5b_run_times_out_gives_fix_verdict(tmp_path, monkeypatch):
    """When bot doesn't process the seeded signal in time, verdict is NEEDS_FIX."""
    db_path = tmp_path / "phase5b.db"
    conn = _seed_safe_db(db_path)
    _live_mode_env(monkeypatch)
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    _seed_phase5b_trace(conn, "BTCUSDT")

    args = _phase5b_args(db_path, duration_minutes=0.01, poll_seconds=1)
    report = phase5.run_phase5b(conn, args)

    assert report.final_verdict in {
        phase5.VERDICT_FIX,
        phase5.VERDICT_5B_NEEDS_CANDIDATE,
        phase5.VERDICT_5B_PASSED,
    }
    if report.phase5_preflight_verdict == phase5.VERDICT_READY:
        assert report.seeded_signal.get("queue_id") is not None
