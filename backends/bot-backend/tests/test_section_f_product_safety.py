from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture()
def tmp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    try:
        os.unlink(path)
    except Exception:
        pass


def _make_db(tmp_db_path: str):
    from shared_lib.persistence.db import DB

    db = DB(path=tmp_db_path)

    def _add_col(conn, table: str, col: str, col_type: str) -> None:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if col not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

    with db.connect() as conn:
        # Ensure the events table supports Audit.event(...) (trace_id was a later migration)
        _add_col(conn, "events", "trace_id", "TEXT")

        # Ensure trade_fills supports shared_lib.persistence.trade_fills.record_fill(...)
        for col, typ in [
            ("strategy", "TEXT"),
            ("strategy_version", "TEXT"),
            ("broker_id", "TEXT"),
            ("account_id", "TEXT"),
            ("asset_class", "TEXT"),
            ("timeframe", "TEXT"),
            ("confidence", "REAL"),
            ("user_id", "TEXT"),
            ("bot_instance_id", "TEXT"),
            ("broker_account_id", "TEXT"),
            ("quote_currency", "TEXT"),
            ("base_currency", "TEXT"),
            ("order_id", "TEXT"),
            ("initiator_type", "TEXT"),
            ("trigger_source", "TEXT"),
            ("position_phase", "TEXT"),
            ("time_in_trade_sec", "REAL"),
            ("sl_at_exit", "REAL"),
            ("tp_at_exit", "REAL"),
            ("market_price_used", "REAL"),
            ("price_source", "TEXT"),
            ("opposite_signal_detected", "INTEGER"),
            ("ensemble_decision", "TEXT"),
            ("risk_force_close", "INTEGER"),
            ("sync_state_before", "TEXT"),
            ("sync_state_after", "TEXT"),
            ("close_order_type", "TEXT"),
            ("broker_response", "TEXT"),
            ("expected_close", "INTEGER"),
            ("exit_reason", "TEXT"),
            ("mfe_pct", "REAL"),
            ("mae_pct", "REAL"),
            ("exit_regime", "TEXT"),
            ("exit_regime_confidence", "REAL"),
            ("gross_pnl", "REAL"),
            ("total_fees", "REAL"),
            ("funding_fees", "REAL"),
            ("net_pnl", "REAL"),
            ("net_pnl_percent", "REAL"),
            ("entry_fee", "REAL"),
            ("exit_fee", "REAL"),
            ("fees_estimated", "INTEGER"),
            ("slippage_estimated", "INTEGER"),
        ]:
            _add_col(conn, "trade_fills", col, typ)

        # Section F bot health columns on bot_instances
        for col, typ in [
            ("bot_health_status", "TEXT DEFAULT 'UNKNOWN'"),
            ("bot_health_message", "TEXT"),
            ("bot_health_reason_code", "TEXT"),
            ("bot_health_recommended_action", "TEXT"),
            ("bot_health_updated_at", "TEXT"),
            ("last_warning", "TEXT"),
        ]:
            _add_col(conn, "bot_instances", col, typ)

        # Section F-3 validation report storage
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_close_validation_reports (
                id TEXT PRIMARY KEY,
                bot_instance_id TEXT,
                run_id TEXT,
                environment TEXT,
                daily_close_enabled INTEGER,
                window_start TEXT,
                window_end TEXT,
                position_opened_at TEXT,
                position_symbol TEXT,
                position_side TEXT,
                entry_price REAL,
                close_trigger_time TEXT,
                close_price REAL,
                exit_reason TEXT,
                close_fill_id INTEGER,
                gross_pnl REAL,
                fees REAL,
                slippage REAL,
                net_pnl REAL,
                audit_event_written INTEGER,
                state_reset_confirmed INTEGER,
                validation_status TEXT,
                errors_json TEXT,
                validated_at TEXT
            );
            """
        )
    return db


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _seed_paper_success(
    *,
    db,
    bot_instance_id: str,
    run_id: str,
    start: datetime,
    days: int = 21,
    closed_trades: int = 60,
    include_daily_close: bool = True,
    include_signal_scheduler: bool = True,
    include_decision_traces: bool = True,
    trade_spacing_hours: int = 6,
):
    from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC

    with db.connect() as conn:
        conn.execute(
            "INSERT INTO runs (run_id, started_at, mode, interval_seconds, max_symbols) VALUES (?,?,?,?,?)",
            (run_id, _iso(start), "paper", 60, 10),
        )

        # Equity snapshots: mostly increasing, tiny dip (<8% drawdown)
        base = 1000.0
        peak = base
        for i in range(days):
            ts = start + timedelta(days=i)
            eq = base + (i * 2.0)
            if i == int(days / 2):
                eq = peak * 0.96
            peak = max(peak, eq)
            conn.execute(
                """
                INSERT INTO equity_snapshots (
                    user_id, bot_instance_id, broker_account_id, broker_id,
                    timestamp_utc, equity, source, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    "u1",
                    bot_instance_id,
                    "acct1",
                    "binance_futures",
                    _iso(ts),
                    float(eq),
                    "pytest",
                    _iso(ts),
                    _iso(ts),
                ),
            )

        # Daily close evidence (one per day)
        if include_daily_close:
            for i in range(days):
                ts = start + timedelta(days=i, hours=1)
                conn.execute(
                    """
                    INSERT INTO events (timestamp_utc, ts, run_id, cycle_id, symbol, event_type, action, details_json, trace_id)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        _iso(ts),
                        _iso(ts),
                        run_id,
                        "daily_close",
                        "BTCUSDT",
                        "DAILY_PROFIT_CLOSE_TRIGGERED",
                        None,
                        json.dumps({"bot_instance_id": bot_instance_id}),
                        None,
                    ),
                )

        # Signal generation evidence: completed for each day+slot
        if include_signal_scheduler:
            for i in range(days):
                d = (start + timedelta(days=i)).date().isoformat()
                for slot in SIGNAL_GENERATION_TIMES_UTC:
                    ts = datetime.fromisoformat(f"{d}T{slot}:00+00:00")
                    conn.execute(
                        """
                        INSERT INTO events (timestamp_utc, ts, run_id, cycle_id, symbol, event_type, action, details_json, trace_id)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            _iso(ts),
                            _iso(ts),
                            run_id,
                            "signal_scheduler",
                            "SYSTEM",
                            "SIGNAL_GENERATION_COMPLETED",
                            None,
                            json.dumps({"scheduled_time_utc": slot, "source": "pytest"}),
                            None,
                        ),
                    )

        # Closed trade fills over the period
        wins = int(closed_trades * 0.67)
        losses = closed_trades - wins
        all_pnls = ([10.0] * wins) + ([-5.0] * losses)  # PF=4.0, positive expectancy

        for i, pnl in enumerate(all_pnls):
            ts = start + timedelta(hours=i * trade_spacing_hours)
            trace_id = f"tr_{bot_instance_id}_{i}" if include_decision_traces else None
            if include_decision_traces:
                conn.execute(
                    "INSERT OR IGNORE INTO decision_traces (trace_id, run_id, cycle_id, ts) VALUES (?,?,?,?)",
                    (trace_id, run_id, f"c{i}", _iso(ts)),
                )
            conn.execute(
                """
                INSERT INTO trade_fills (
                    run_id, cycle_id, trace_id, symbol, side, action, qty, price,
                    fee, realized_pnl, total_fees, net_pnl, timestamp_utc, bot_instance_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    f"c{i}",
                    trace_id,
                    "BTCUSDT",
                    "LONG",
                    "CLOSE",
                    1.0,
                    100.0,
                    0.1,
                    pnl,
                    0.1,
                    pnl - 0.1,
                    _iso(ts),
                    bot_instance_id,
                ),
            )

        conn.commit()


class TestUserCapitalReadinessGate:
    def test_rejected_if_sections_not_confirmed(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", False, raising=False)
        db = _make_db(tmp_db_path)
        _seed_paper_success(db=db, bot_instance_id="bot-a", run_id="run-a", start=datetime(2026, 1, 5, tzinfo=timezone.utc))

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert report.readiness_status.value in {"PAPER_VALIDATION_RUNNING", "NOT_READY"}
        assert RejectionReason.SECTIONS_A_TO_E_NOT_CONFIRMED.value in report.blocking_reasons

    def test_rejected_if_paper_period_too_short(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        _seed_paper_success(
            db=db,
            bot_instance_id="bot-a",
            run_id="run-a",
            start=datetime(2026, 2, 3, tzinfo=timezone.utc),
            days=7,
            closed_trades=60,
            trade_spacing_hours=1,
        )

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.PAPER_TRADING_PERIOD_TOO_SHORT.value in report.blocking_reasons

    def test_rejected_if_closed_trades_below_60(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        _seed_paper_success(
            db=db,
            bot_instance_id="bot-a",
            run_id="run-a",
            start=datetime(2026, 1, 5, tzinfo=timezone.utc),
            days=21,
            closed_trades=10,
        )

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.INSUFFICIENT_CLOSED_TRADES.value in report.blocking_reasons

    def test_rejected_if_missing_decision_traces(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        _seed_paper_success(
            db=db,
            bot_instance_id="bot-a",
            run_id="run-a",
            start=datetime(2026, 1, 5, tzinfo=timezone.utc),
            include_decision_traces=False,
        )

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.MISSING_DECISION_TRACES.value in report.blocking_reasons

    def test_rejected_if_negative_expectancy(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        start = datetime(2026, 1, 5, tzinfo=timezone.utc)

        with db.connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, started_at, mode, interval_seconds, max_symbols) VALUES (?,?,?,?,?)",
                ("run-a", _iso(start), "paper", 60, 10),
            )
            for i in range(21):
                ts = start + timedelta(days=i)
                conn.execute(
                    "INSERT INTO equity_snapshots (user_id, bot_instance_id, broker_account_id, broker_id, timestamp_utc, equity, source, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    ("u1", "bot-a", "acct1", "binance_futures", _iso(ts), 1000.0 + i, "pytest", _iso(ts), _iso(ts)),
                )
                conn.execute(
                    "INSERT INTO events (timestamp_utc, ts, run_id, cycle_id, symbol, event_type, action, details_json, trace_id) VALUES (?,?,?,?,?,?,?,?,?)",
                    (_iso(ts), _iso(ts), "run-a", "daily_close", "BTCUSDT", "DAILY_PROFIT_CLOSE_TRIGGERED", None, json.dumps({"bot_instance_id": "bot-a"}), None),
                )
            conn.commit()

        # 60 losing trades => negative expectancy
        with db.connect() as conn:
            for i in range(60):
                ts = start + timedelta(hours=i * 6)
                trace_id = f"tr_bot-a_{i}"
                conn.execute(
                    "INSERT OR IGNORE INTO decision_traces (trace_id, run_id, cycle_id, ts) VALUES (?,?,?,?)",
                    (trace_id, "run-a", f"c{i}", _iso(ts)),
                )
                conn.execute(
                    """
                    INSERT INTO trade_fills (
                        run_id, cycle_id, trace_id, symbol, side, action, qty, price,
                        fee, realized_pnl, total_fees, net_pnl, timestamp_utc, bot_instance_id
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    ("run-a", f"c{i}", trace_id, "BTCUSDT", "LONG", "CLOSE", 1.0, 100.0, 0.1, -5.0, 0.1, -5.1, _iso(ts), "bot-a"),
                )
            conn.commit()

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.NEGATIVE_EXPECTANCY.value in report.blocking_reasons

    def test_rejected_if_max_drawdown_above_8pct(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        start = datetime(2026, 1, 5, tzinfo=timezone.utc)
        _seed_paper_success(db=db, bot_instance_id="bot-a", run_id="run-a", start=start)

        # Force big drawdown within paper period
        with db.connect() as conn:
            conn.execute(
                "UPDATE equity_snapshots SET equity = ? WHERE bot_instance_id = ?",
                (1000.0, "bot-a"),
            )
            conn.execute(
                "UPDATE equity_snapshots SET equity = ? WHERE bot_instance_id = ? AND id = (SELECT MIN(id) FROM equity_snapshots WHERE bot_instance_id = ?)",
                (2000.0, "bot-a", "bot-a"),
            )
            conn.execute(
                "UPDATE equity_snapshots SET equity = ? WHERE bot_instance_id = ? AND id = (SELECT MAX(id) FROM equity_snapshots WHERE bot_instance_id = ?)",
                (1500.0, "bot-a", "bot-a"),
            )
            conn.commit()

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.MAX_DRAWDOWN_TOO_HIGH.value in report.blocking_reasons

    def test_rejected_if_profit_factor_below_1_3(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        start = datetime(2026, 1, 5, tzinfo=timezone.utc)

        _seed_paper_success(db=db, bot_instance_id="bot-a", run_id="run-a", start=start)
        # Make profits roughly equal to losses => PF ~1
        with db.connect() as conn:
            conn.execute(
                "UPDATE trade_fills SET realized_pnl = -5.0, net_pnl = -5.1 WHERE bot_instance_id = ? AND id IN (SELECT id FROM trade_fills WHERE bot_instance_id = ? LIMIT 40)",
                ("bot-a", "bot-a"),
            )
            conn.commit()

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.PROFIT_FACTOR_TOO_LOW.value in report.blocking_reasons

    def test_rejected_if_sizing_failures_detected(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        start = datetime(2026, 1, 5, tzinfo=timezone.utc)
        _seed_paper_success(db=db, bot_instance_id="bot-a", run_id="run-a", start=start)

        with db.connect() as conn:
            conn.execute(
                "INSERT INTO events (timestamp_utc, ts, run_id, cycle_id, symbol, event_type, action, details_json, trace_id) VALUES (?,?,?,?,?,?,?,?,?)",
                (_iso(start + timedelta(days=1)), _iso(start + timedelta(days=1)), "run-a", "cycle", "BTCUSDT", "SIZING_FAILURE", None, json.dumps({"bot_instance_id": "bot-a"}), None),
            )
            conn.commit()

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.SIZING_FAILURES_DETECTED.value in report.blocking_reasons

    def test_rejected_if_daily_close_evidence_missing(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        _seed_paper_success(
            db=db,
            bot_instance_id="bot-a",
            run_id="run-a",
            start=datetime(2026, 1, 5, tzinfo=timezone.utc),
            include_daily_close=False,
        )

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.DAILY_CLOSE_NOT_VALIDATED.value in report.blocking_reasons

    def test_rejected_if_signal_scheduler_evidence_missing(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, RejectionReason

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        _seed_paper_success(
            db=db,
            bot_instance_id="bot-a",
            run_id="run-a",
            start=datetime(2026, 1, 5, tzinfo=timezone.utc),
            include_signal_scheduler=False,
        )

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert RejectionReason.SIGNAL_SCHEDULER_NOT_VALIDATED.value in report.blocking_reasons

    def test_passing_metrics_is_only_eligible_for_review(self, tmp_db_path, monkeypatch):
        from app.product_safety.readiness_gate import evaluate_user_capital_readiness, ReadinessStatus

        monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)
        db = _make_db(tmp_db_path)
        # Offset the start time so the inferred paper_end falls after the last signal slot on the final day.
        _seed_paper_success(db=db, bot_instance_id="bot-a", run_id="run-a", start=datetime(2026, 1, 5, 5, tzinfo=timezone.utc))

        report = evaluate_user_capital_readiness(db=db, bot_instance_id="bot-a")
        assert report.readiness_status == ReadinessStatus.READY_FOR_CONTROLLED_BETA_REVIEW, report.blocking_reasons


class TestBotHealthStatus:
    def test_update_bot_health_persists_fields(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            now = _iso(datetime(2026, 1, 1, tzinfo=timezone.utc))
            conn.execute(
                """
                INSERT INTO bot_instances (
                    id, user_id, broker_account_id, market_type,
                    strategy_id, strategy_version, config_id, risk_profile_id,
                    symbols, timeframes,
                    allocation_type, allocation_value,
                    mode, status,
                    created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "bot-a",
                    "u1",
                    "acct1",
                    "crypto",
                    "strat1",
                    "0",
                    None,
                    None,
                    "[]",
                    "[]",
                    "fixed_amount",
                    100.0,
                    "paper",
                    "active",
                    now,
                    now,
                ),
            )
            conn.commit()

        from app.core.bot_instance_service import BotInstanceService

        svc = BotInstanceService(db=db)
        svc.update_bot_health(
            "bot-a",
            bot_health_status="ERROR_SIZING_FAILURE",
            bot_health_message="Trade amount too small for exchange minimum.",
            bot_health_reason_code="TRADE_AMOUNT_TOO_SMALL_MINIMUM_50_USDT",
            bot_health_recommended_action="Increase trade amount per position.",
            last_warning="sizing_warning",
        )

        with db.connect() as conn:
            row = conn.execute(
                """
                SELECT bot_health_status, bot_health_message, bot_health_reason_code,
                       bot_health_recommended_action, bot_health_updated_at, last_warning
                FROM bot_instances WHERE id = ?
                """,
                ("bot-a",),
            ).fetchone()
        assert row["bot_health_status"] == "ERROR_SIZING_FAILURE"
        assert "exchange minimum" in (row["bot_health_message"] or "")
        assert row["bot_health_reason_code"] == "TRADE_AMOUNT_TOO_SMALL_MINIMUM_50_USDT"
        assert row["bot_health_recommended_action"]
        assert row["bot_health_updated_at"]
        assert row["last_warning"] == "sizing_warning"


class TestDailyCloseEndToEndValidation:
    def test_validate_daily_close_paper_records_fill_and_events_and_report(self, tmp_db_path, monkeypatch):
        from app.product_safety.daily_close_validation import validate_daily_close_paper

        # Ensure daily close path is enabled for this test
        monkeypatch.setattr("app.core.config.settings.DAILY_CLOSE_ENABLED", True, raising=False)

        db = _make_db(tmp_db_path)
        report = validate_daily_close_paper(
            db=db,
            bot_instance_id="bot-a",
            run_id="run-a",
            symbol="BTCUSDT",
            side="LONG",
            entry_price=100.0,
            close_price=101.0,
            quantity=1.0,
            estimated_fees=0.1,
            estimated_slippage_pct=0.0,
        )

        assert report.validation_status == "PASSED"
        assert report.close_fill_id is not None

        with db.connect() as conn:
            fill = conn.execute(
                "SELECT exit_reason, action FROM trade_fills WHERE id = ?",
                (report.close_fill_id,),
            ).fetchone()
            assert fill is not None
            assert fill["action"] == "CLOSE"
            assert fill["exit_reason"] in {"DAILY_CLOSE", "EXIT_DAILY_CLOSE"}

            ev = conn.execute(
                "SELECT COUNT(*) as c FROM events WHERE event_type = 'DAILY_PROFIT_CLOSE_TRIGGERED'",
            ).fetchone()["c"]
            assert int(ev) >= 1

            rep = conn.execute(
                "SELECT COUNT(*) as c FROM daily_close_validation_reports WHERE bot_instance_id = ?",
                ("bot-a",),
            ).fetchone()["c"]
            assert int(rep) == 1
