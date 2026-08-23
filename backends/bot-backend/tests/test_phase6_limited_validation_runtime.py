import json
import sqlite3
from pathlib import Path

import scripts.run_tradingview_phase6_limited_validation as validation
import scripts.run_tradingview_daily_monitoring_report as daily_report
import scripts.run_tradingview_phase6f_operational_observation as phase6f
import scripts.run_tradingview_phase6g_active_observation as phase6g


def _make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE external_signal_queue (
            id TEXT,
            bot_id TEXT,
            status TEXT
        );
        CREATE TABLE position_lifecycle_state (
            bot_instance_id TEXT,
            exchange_position_active INTEGER,
            sl_order_id TEXT,
            tp_order_id TEXT
        );
        CREATE TABLE tradingview_processor_heartbeat (
            bot_instance_id TEXT,
            updated_at TEXT
        );
        CREATE TABLE tradingview_safety_lockouts (
            bot_instance_id TEXT,
            is_locked INTEGER,
            reason TEXT
        );
        INSERT INTO tradingview_processor_heartbeat VALUES ('bot_test', '2026-05-21T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()


def _make_proof_dir(path: Path) -> None:
    path.mkdir()
    (path / "phase5c_controlled_clean_candle_execution_proof_20260521_000000.json").write_text(
        json.dumps({"final_verdict": "SUCCESSFUL LIVE-MODE TRADINGVIEW EXECUTION PROOF PASSED"}),
        encoding="utf-8",
    )


def _set_enabled_env(monkeypatch) -> None:
    values = {
        "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": "true",
        "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED": "true",
        "TRADINGVIEW_ALLOWED_ACTIONS": "BUY,SELL",
        "TRADINGVIEW_ALLOWED_SYMBOLS": "BTCUSDT,ETHUSDT,BNBUSDT",
        "TRADINGVIEW_MAX_TRADE_USDT_CAP": "20",
        "TRADINGVIEW_MAX_SIGNALS_PER_HOUR": "3",
        "TRADINGVIEW_MAX_SIGNALS_PER_DAY": "10",
        "TRADINGVIEW_MAX_EXECUTIONS_PER_DAY": "1",
        "TRADINGVIEW_MAX_QUEUE_PER_CYCLE": "1",
        "TRADINGVIEW_REQUIRE_SLTP_PROTECTION": "true",
        "TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL": "true",
        "TRADINGVIEW_ALLOW_CLOSE": "false",
        "TRADINGVIEW_ALLOW_REVERSE": "false",
        "TRADINGVIEW_ALLOW_REDUCE": "false",
        "TRADINGVIEW_ALLOW_CANCEL": "false",
        "TRADINGVIEW_ALLOW_EXTERNAL_SLTP": "false",
        "TRADINGVIEW_ALLOW_EXTERNAL_SIZE": "false",
        "TRADINGVIEW_ALLOW_RISK_OVERRIDE": "false",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def _fresh_fingerprint(**overrides):
    base = {
        "reachable": True,
        "fingerprint_present": True,
        "phase6_gate_available": True,
        "phase6_gate_code_version": "phase6_limited_gate_v1_2026-05-21",
        "pid": 1234,
        "port_owner_pid": 1234,
        "working_directory": "C:/repo/backends/bot-backend",
        "python_executable": "C:/repo/backends/venv/Scripts/python.exe",
        "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": True,
        "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED": True,
        "TRADINGVIEW_ALLOWED_SYMBOLS": ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        "TRADINGVIEW_ALLOWED_ACTIONS": ["BUY", "SELL"],
        "TRADINGVIEW_MAX_QUEUE_PER_CYCLE": 1,
        "TRADINGVIEW_MAX_EXECUTIONS_PER_DAY": 1,
        "TRADINGVIEW_MAX_SIGNALS_PER_HOUR": 3,
        "TRADINGVIEW_MAX_SIGNALS_PER_DAY": 10,
        "TRADINGVIEW_MAX_TRADE_USDT_CAP": 20.0,
        "TRADINGVIEW_ALLOW_CLOSE": False,
        "TRADINGVIEW_ALLOW_REVERSE": False,
        "TRADINGVIEW_ALLOW_REDUCE": False,
        "TRADINGVIEW_ALLOW_CANCEL": False,
        "TRADINGVIEW_ALLOW_EXTERNAL_SLTP": False,
        "TRADINGVIEW_ALLOW_EXTERNAL_SIZE": False,
        "TRADINGVIEW_ALLOW_RISK_OVERRIDE": False,
        "TRADINGVIEW_REQUIRE_SLTP_PROTECTION": True,
        "TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL": True,
        "active_safety_lockout": False,
    }
    base.update(overrides)
    return base


def test_fresh_runtime_disabled_config_is_needs_fix_not_stale(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setenv("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", "false")
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: _fresh_fingerprint(TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=False),
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_NEEDS_FIX
    assert report.runtime_stale_findings == []
    assert "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED is false" in report.config_findings


def test_fresh_runtime_active_lockout_is_needs_fix_not_stale(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: _fresh_fingerprint(active_safety_lockout=True),
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_NEEDS_FIX
    assert report.runtime_stale_findings == []
    assert "TradingView safety lockout is active in live runtime" in report.lockout_findings


def test_missing_fingerprint_is_runtime_stale(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: {"reachable": True, "fingerprint_present": False},
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_RUNTIME_STALE
    assert "Runtime endpoint does not expose Phase 6 fingerprint" in report.runtime_stale_findings


def test_pid_mismatch_is_runtime_stale(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: _fresh_fingerprint(pid=1234, port_owner_pid=9999),
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_RUNTIME_STALE
    assert any("does not match port owner" in item for item in report.runtime_stale_findings)


def test_runtime_child_pid_of_port_owner_is_not_stale(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setattr(validation, "_pid_matches_or_child", lambda pid, owner_pid: True)
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: _fresh_fingerprint(pid=44452, port_owner_pid=46960),
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_READY
    assert report.runtime_stale_findings == []


def test_phase6d_twenty_symbol_rollout_is_allowed(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    phase6d_symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "LINKUSDT",
        "AVAXUSDT",
        "LTCUSDT",
        "APEUSDT",
        "SUIUSDT",
        "INJUSDT",
        "AAVEUSDT",
        "ZECUSDT",
        "HYPEUSDT",
        "ENAUSDT",
        "LDOUSDT",
        "MASKUSDT",
        "TAOUSDT",
    ]
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setenv("TRADINGVIEW_ALLOWED_SYMBOLS", ",".join(phase6d_symbols))
    monkeypatch.setenv("TRADINGVIEW_MAX_TRADE_USDT_CAP", "200")
    monkeypatch.setenv("TRADINGVIEW_MAX_SIGNALS_PER_HOUR", "15")
    monkeypatch.setenv("TRADINGVIEW_MAX_SIGNALS_PER_DAY", "40")
    monkeypatch.setenv("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY", "3")
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: _fresh_fingerprint(
            TRADINGVIEW_ALLOWED_SYMBOLS=phase6d_symbols,
            TRADINGVIEW_MAX_TRADE_USDT_CAP=200.0,
            TRADINGVIEW_MAX_SIGNALS_PER_HOUR=15,
            TRADINGVIEW_MAX_SIGNALS_PER_DAY=40,
            TRADINGVIEW_MAX_EXECUTIONS_PER_DAY=3,
        ),
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_READY
    assert report.config_findings == []


def test_phase6_rejects_more_than_twenty_allowed_symbols(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    symbols = [f"SYM{i}USDT" for i in range(21)]
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setenv("TRADINGVIEW_ALLOWED_SYMBOLS", ",".join(symbols))
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: _fresh_fingerprint(TRADINGVIEW_ALLOWED_SYMBOLS=symbols),
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_NEEDS_FIX
    assert "Allowed symbols must be restricted to 1-20 symbols for Phase 6" in report.config_findings


def test_phase6e_four_hundred_trade_cap_is_allowed(tmp_path, monkeypatch):
    db_path = tmp_path / "phase6.db"
    proof_dir = tmp_path / "proof"
    _make_db(db_path)
    _make_proof_dir(proof_dir)
    _set_enabled_env(monkeypatch)
    monkeypatch.setenv("TRADINGVIEW_MAX_TRADE_USDT_CAP", "400")
    monkeypatch.setenv("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY", "3")
    monkeypatch.setattr(
        validation,
        "fetch_runtime_fingerprint",
        lambda *_args, **_kwargs: _fresh_fingerprint(
            TRADINGVIEW_MAX_TRADE_USDT_CAP=400.0,
            TRADINGVIEW_MAX_EXECUTIONS_PER_DAY=3,
        ),
    )

    report = validation.evaluate(db_path, proof_dir, runtime_url="http://runtime", require_runtime=True)

    assert report.verdict == validation.VERDICT_READY
    assert report.config["TRADINGVIEW_MAX_TRADE_USDT_CAP"] == 400.0


def _phase6f_report(**overrides):
    base = {
        "generated_at": "2026-05-22T00:00:00+00:00",
        "final_verdict": phase6f.VERDICT_NEEDS_FIX,
        "runtime_process_verification": {"pid_matches_port_owner": True},
        "runtime_fingerprint": {"phase6_gate_available": True},
        "phase6f_config": {"max_trade_usdt_cap": 400},
        "validation_result": "PHASE 6 LIMITED MODE READY",
        "observation_window": {"sample_count": 1},
        "alerts_summary": {"alerts_received": 0, "alerts_accepted": 0, "alerts_rejected": 0},
        "queue_summary": {
            "queue_rows_created": 0,
            "queue_rows_processed": 0,
            "queue_rows_rejected": 0,
            "queue_rows_failed": 0,
            "queue_rows_expired": 0,
            "stuck_claimed_rows": 0,
        },
        "decision_summary": {},
        "execution_summary": {"execution_attempts": 0, "orders_placed": 0, "trades_opened": 0},
        "sltp_protection_summary": {"trades_protected": 0, "unprotected_positions": 0},
        "lifecycle_reconciliation_summary": {"unprotected_positions": 0},
        "gate_rejection_summary": {},
        "rate_limit_cap_summary": {},
        "admin_visibility_summary": {
            "limited_status_reachable": True,
            "processor_status_reachable": True,
            "secrets_exposed": False,
        },
        "safety_lockout_summary": {"active": False},
        "safety_invariant_results": {
            "webhook_direct_executor_calls": 0,
            "queue_direct_execution_calls": 0,
            "unsupported_actions_executed": 0,
            "close_reverse_reduce_executed": 0,
            "cancel_executed": 0,
            "sltp_update_executed_from_tradingview": 0,
            "external_size_used": 0,
            "risk_override_used": 0,
            "duplicate_processed_queue_rows": 0,
            "stuck_claimed_rows": 0,
            "unprotected_positions": 0,
        },
    }
    base.update(overrides)
    return phase6f.Phase6FReport(**base)


def test_phase6f_no_activity_is_safe_when_invariants_pass():
    report = _phase6f_report()

    assert phase6f.determine_verdict(report) == phase6f.VERDICT_PASSED_NO_ACTIVITY


def test_phase6f_activity_is_operational_pass_when_invariants_pass():
    report = _phase6f_report(
        alerts_summary={"alerts_received": 1, "alerts_accepted": 1, "alerts_rejected": 0},
        queue_summary={
            "queue_rows_created": 1,
            "queue_rows_processed": 1,
            "queue_rows_rejected": 0,
            "queue_rows_failed": 0,
            "queue_rows_expired": 0,
            "stuck_claimed_rows": 0,
        },
    )

    assert phase6f.determine_verdict(report) == phase6f.VERDICT_PASSED_ACTIVITY


def test_phase6f_stuck_claimed_row_is_unsafe():
    report = _phase6f_report(
        safety_invariant_results={
            "webhook_direct_executor_calls": 0,
            "queue_direct_execution_calls": 0,
            "unsupported_actions_executed": 0,
            "close_reverse_reduce_executed": 0,
            "cancel_executed": 0,
            "sltp_update_executed_from_tradingview": 0,
            "external_size_used": 0,
            "risk_override_used": 0,
            "duplicate_processed_queue_rows": 0,
            "stuck_claimed_rows": 1,
            "unprotected_positions": 0,
        }
    )

    assert phase6f.determine_verdict(report) == phase6f.VERDICT_UNSAFE


def test_phase6f_missing_admin_visibility_needs_fix():
    report = _phase6f_report(
        admin_visibility_summary={
            "limited_status_reachable": False,
            "processor_status_reachable": True,
            "secrets_exposed": False,
        }
    )

    assert phase6f.determine_verdict(report) == phase6f.VERDICT_NEEDS_FIX


def _phase6g_report(**overrides):
    base = {
        "generated_at": "2026-05-22T00:00:00+00:00",
        "final_verdict": phase6g.VERDICT_NEEDS_FIX,
        "runtime_process_verification": {"pid_matches_port_owner": True},
        "runtime_fingerprint": {"phase6_gate_available": True},
        "phase6g_config": {"max_trade_usdt_cap": 400},
        "validation_result": "PHASE 6 LIMITED MODE READY",
        "reset_evidence": None,
        "observation_window": {"sample_count": 1},
        "controlled_alert_plan": {"send_controlled_alerts": True},
        "controlled_alert_results": [],
        "alerts_summary": {"alerts_received": 3, "alerts_accepted": 3, "alerts_rejected": 0},
        "queue_summary": {
            "queue_rows_created": 3,
            "queue_rows_processed": 0,
            "queue_rows_rejected": 3,
            "queue_rows_failed": 0,
            "queue_rows_expired": 0,
            "stuck_claimed_rows": 0,
        },
        "decision_summary": {},
        "execution_summary": {"execution_attempts": 0, "orders_placed": 0, "trades_opened": 0},
        "sltp_protection_summary": {"trades_protected": 0, "unprotected_positions": 0},
        "lifecycle_reconciliation_summary": {"unprotected_positions": 0},
        "gate_rejection_summary": {},
        "rate_limit_cap_summary": {},
        "negative_safety_checks": {
            "outside_symbol_rejected": True,
            "forbidden_action_rejected": True,
            "duplicate_blocked": True,
        },
        "admin_visibility_summary": {
            "limited_status_reachable": True,
            "processor_status_reachable": True,
            "secrets_exposed": False,
        },
        "safety_lockout_summary": {"active": False},
        "safety_invariant_results": {
            "webhook_direct_executor_calls": 0,
            "queue_direct_execution_calls": 0,
            "unsupported_actions_executed": 0,
            "close_reverse_reduce_executed": 0,
            "cancel_executed": 0,
            "sltp_update_executed_from_tradingview": 0,
            "external_size_used": 0,
            "risk_override_used": 0,
            "duplicate_processed_queue_rows": 0,
            "stuck_claimed_rows": 0,
            "unprotected_positions": 0,
        },
    }
    base.update(overrides)
    return phase6g.Phase6GReport(**base)


def test_phase6g_safe_rejections_pass():
    report = _phase6g_report()

    assert phase6g.determine_verdict(report) == phase6g.VERDICT_PASSED_SAFE_REJECTIONS


def test_phase6g_execution_activity_passes_when_protected():
    report = _phase6g_report(
        execution_summary={"execution_attempts": 1, "orders_placed": 1, "trades_opened": 1},
        sltp_protection_summary={"trades_protected": 1, "unprotected_positions": 0},
    )

    assert phase6g.determine_verdict(report) == phase6g.VERDICT_PASSED_ACTIVITY


def test_phase6g_negative_failure_needs_fix():
    report = _phase6g_report(
        negative_safety_checks={
            "outside_symbol_rejected": False,
            "forbidden_action_rejected": True,
            "duplicate_blocked": True,
        }
    )

    assert phase6g.determine_verdict(report) == phase6g.VERDICT_NEEDS_FIX


def test_phase6g_failed_execution_needs_fix():
    report = _phase6g_report(
        queue_summary={
            "queue_rows_created": 1,
            "queue_rows_processed": 0,
            "queue_rows_rejected": 0,
            "queue_rows_failed": 1,
            "queue_rows_expired": 0,
            "stuck_claimed_rows": 0,
        },
        controlled_alert_results=[
            {
                "symbol": "ETHUSDT",
                "action": "BUY",
                "final_status": "FAILED_EXECUTION",
                "final_reason": "Failed to place Stop-Loss. Entry was closed immediately.",
            }
        ],
    )

    assert phase6g.determine_verdict(report) == phase6g.VERDICT_NEEDS_FIX


def test_daily_report_flags_controlled_mode_expansion():
    runtime = _fresh_fingerprint(
        TRADINGVIEW_ALLOWED_SYMBOLS=[f"S{i}USDT" for i in range(21)],
        TRADINGVIEW_MAX_TRADE_USDT_CAP=401,
    )
    snapshot = {
        "stuck_claimed_rows": 0,
        "unprotected_positions": 0,
        "queue_rows_failed": 0,
    }
    attempts = {
        "external_size_attempts": 0,
        "external_sltp_attempts": 0,
        "risk_override_attempts": 0,
    }

    incidents = daily_report.operational_incidents(
        runtime=runtime,
        admin={"limited_status_reachable": True, "processor_status_reachable": True, "secrets_exposed": False},
        snapshot=snapshot,
        attempts=attempts,
    )

    assert any("20-symbol" in item for item in incidents)
    assert any("400 USDT" in item for item in incidents)


def test_daily_report_markdown_contains_operational_checklist():
    report = daily_report.DailyTradingViewReport(
        generated_at="2026-05-22T00:00:00+00:00",
        bot_id="bot_test",
        reporting_window={"since": "2026-05-21T00:00:00+00:00", "until": "2026-05-22T00:00:00+00:00"},
        controlled_mode_confirmation=daily_report.CONTROLLED_MODE_CONFIRMATION,
        runtime_fingerprint={},
        phase6_config={},
        alerts_summary={},
        queue_summary={},
        execution_summary={},
        protection_summary={},
        forbidden_external_attempts={},
        rate_limit_cap_summary={},
        safety_lockout_status={},
        processor_heartbeat={},
        admin_endpoint_health={},
        incidents_anomalies=[],
    )

    markdown = daily_report.render_markdown(report)

    assert "Operational Checklist" in markdown
    assert "BUY/SELL only" in markdown
    assert "400 USDT max trade cap" in markdown
