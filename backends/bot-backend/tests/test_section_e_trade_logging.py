"""
CosmicForge Section E Trade Logging and Explainability Tests

E-1: Entry decision trace — full context captured after every successful open
E-2: Close fill — exit reason, gross/net PnL, fees, slippage, R multiple
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call
from typing import Optional


# ═══════════════════════════════════════════════════════════════════════════
# E-1: Entry Decision Trace
# ═══════════════════════════════════════════════════════════════════════════

class TestEntryDecisionTrace:

    def _make_audit(self):
        return MagicMock()

    def test_successful_entry_calls_log_entry_decision_context(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        result = log_entry_decision_context(
            audit=audit,
            run_id="run1",
            cycle_id="cycle1",
            symbol="BTCUSDT",
            side="LONG",
            action="OPEN",
            bot_instance_id="bot1",
            user_id="user1",
            strategy_name="master_ensemble",
            confidence_score=0.80,
            actual_entry_price=50000.0,
        )
        assert result is True
        audit.event.assert_called_once()

    def test_trace_includes_bot_instance_id(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit,
            run_id="run1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            bot_instance_id="bot-abc",
        )
        call_kwargs = audit.event.call_args
        details = call_kwargs.kwargs.get("details") or (call_kwargs.args[0] if call_kwargs.args else {})
        if isinstance(details, dict):
            assert details.get("bot_instance_id") == "bot-abc"
        else:
            assert "bot-abc" in str(call_kwargs)

    def test_trace_includes_strategy_name(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            strategy_name="master_ensemble",
        )
        call_str = str(audit.event.call_args)
        assert "master_ensemble" in call_str

    def test_trace_includes_confidence_score(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            confidence_score=0.82,
        )
        call_str = str(audit.event.call_args)
        assert "0.82" in call_str

    def test_trace_includes_market_regime(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            market_regime="TRENDING",
        )
        assert "TRENDING" in str(audit.event.call_args)

    def test_trace_includes_atr(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            atr_at_entry=523.5,
        )
        assert "523.5" in str(audit.event.call_args)

    def test_trace_includes_sl_tp(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            stop_loss=49000.0, take_profit=51500.0,
        )
        call_str = str(audit.event.call_args)
        assert "49000" in call_str
        assert "51500" in call_str

    def test_trace_includes_position_size_and_leverage(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            quantity=0.001, leverage=3.0,
        )
        call_str = str(audit.event.call_args)
        assert "0.001" in call_str
        assert "3.0" in call_str

    def test_trace_includes_expected_rr(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            gross_risk_reward=2.1,
        )
        assert "2.1" in str(audit.event.call_args)

    def test_trace_includes_approval_reason(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
            approval_reason="POLICY_PASSED",
        )
        assert "POLICY_PASSED" in str(audit.event.call_args)

    def test_trace_persistence_failure_is_logged(self):
        """If audit.event raises, the failure must be logged."""
        from app.core.trade_context_logger import log_entry_decision_context
        audit = MagicMock()
        audit.event.side_effect = RuntimeError("db error")

        with patch("app.core.trade_context_logger.logger") as mock_log:
            result = log_entry_decision_context(
                audit=audit, run_id="r1", cycle_id="c1",
                symbol="BTCUSDT", side="LONG", action="OPEN",
            )

        assert result is False
        error_calls = [str(c) for c in mock_log.exception.call_args_list]
        assert any("DECISION_TRACE_WRITE_FAILED" in c for c in error_calls)

    def test_trace_event_type_is_correct(self):
        from app.core.trade_context_logger import log_entry_decision_context
        audit = self._make_audit()
        log_entry_decision_context(
            audit=audit, run_id="r1", cycle_id="c1",
            symbol="BTCUSDT", side="LONG", action="OPEN",
        )
        # event_type must be TRADE_ENTRY_DECISION_CONTEXT
        call_str = str(audit.event.call_args)
        assert "TRADE_ENTRY_DECISION_CONTEXT" in call_str


# ═══════════════════════════════════════════════════════════════════════════
# E-2: Standardised Exit Reasons
# ═══════════════════════════════════════════════════════════════════════════

class TestStandardisedExitReasons:

    def test_exit_tp1_maps_to_tp1(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert ExitReason.EXIT_TP1 == "TP1"

    def test_exit_tp2_maps_to_tp2(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert ExitReason.EXIT_TP2 == "TP2"

    def test_exit_stop_loss_maps_to_sl(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert ExitReason.EXIT_STOP_LOSS == "SL"

    def test_exit_trailing_stop(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert ExitReason.EXIT_TRAILING_STOP == "TRAILING_SL"

    def test_exit_break_even(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert ExitReason.EXIT_BREAK_EVEN == "BREAK_EVEN"

    def test_exit_daily_close_in_all(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert "DAILY_CLOSE" in ExitReason.ALL

    def test_exit_kill_switch_maps_to_kill_switch(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert ExitReason.EXIT_KILL_SWITCH == "KILL_SWITCH"

    def test_exit_time_based_maps_to_time_exit(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert ExitReason.EXIT_TIME_BASED == "TIME_EXIT"

    def test_exit_event_blackout_in_all(self):
        from shared_lib.persistence.trade_fills import ExitReason
        assert "EVENT_BLACKOUT" in ExitReason.ALL


# ═══════════════════════════════════════════════════════════════════════════
# E-2: PnL Calculator
# ═══════════════════════════════════════════════════════════════════════════

class TestPnLCalculator:

    def test_long_gross_pnl_winner(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="LONG", entry_price=50000.0, exit_price=51000.0,
            quantity=0.001,
        )
        assert r["gross_pnl"] == pytest.approx(1.0, rel=1e-4)

    def test_long_gross_pnl_loser(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="LONG", entry_price=50000.0, exit_price=49000.0,
            quantity=0.001,
        )
        assert r["gross_pnl"] == pytest.approx(-1.0, rel=1e-4)

    def test_short_gross_pnl_winner(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="SHORT", entry_price=50000.0, exit_price=49000.0,
            quantity=0.001,
        )
        assert r["gross_pnl"] == pytest.approx(1.0, rel=1e-4)

    def test_fees_reduce_net_pnl(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="LONG", entry_price=50000.0, exit_price=51000.0,
            quantity=0.001, entry_fee=0.10, exit_fee=0.10,
        )
        assert r["net_pnl"] == pytest.approx(0.80, rel=1e-3)
        assert r["total_fees"] == pytest.approx(0.20, rel=1e-3)

    def test_slippage_reduces_net_pnl(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="LONG", entry_price=50000.0, exit_price=51000.0,
            quantity=0.001, slippage_cost=0.05,
        )
        assert r["net_pnl"] == pytest.approx(0.95, rel=1e-3)

    def test_funding_fees_included_in_total_fees(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="LONG", entry_price=50000.0, exit_price=51000.0,
            quantity=0.001, exit_fee=0.10, funding_fees=0.05,
        )
        assert r["total_fees"] == pytest.approx(0.15, rel=1e-3)
        assert r["funding_fees"] == pytest.approx(0.05, rel=1e-3)

    def test_r_multiple_calculated(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="LONG", entry_price=50000.0, exit_price=51000.0,
            quantity=0.001, initial_risk=0.5,
        )
        assert r["r_multiple"] is not None
        assert r["r_multiple"] == pytest.approx(2.0, rel=1e-2)

    def test_r_multiple_none_when_no_initial_risk(self):
        from shared_lib.persistence.trade_fills import calculate_close_pnl
        r = calculate_close_pnl(
            side="LONG", entry_price=50000.0, exit_price=51000.0,
            quantity=0.001,
        )
        assert r["r_multiple"] is None


# ═══════════════════════════════════════════════════════════════════════════
# E-2: record_fill() extended fields
# ═══════════════════════════════════════════════════════════════════════════

class TestRecordFillExtended:

    def _mock_db(self):
        db = MagicMock()
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value = MagicMock(fetchone=lambda: None, lastrowid=1)
        db.connect.return_value = conn
        return db, conn

    def test_record_fill_accepts_exit_reason(self):
        from shared_lib.persistence.trade_fills import record_fill, ExitReason
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, exit_reason=ExitReason.EXIT_STOP_LOSS)
        conn.execute.assert_called()

    def test_record_fill_accepts_gross_pnl(self):
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, gross_pnl=1.0)
        conn.execute.assert_called()

    def test_record_fill_accepts_total_fees(self):
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, total_fees=0.20)
        conn.execute.assert_called()

    def test_record_fill_accepts_net_pnl(self):
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, net_pnl=0.80)
        conn.execute.assert_called()

    def test_record_fill_accepts_funding_fees(self):
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, funding_fees=0.05)
        conn.execute.assert_called()

    def test_record_fill_accepts_bot_instance_id(self):
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, bot_instance_id="bot1",
                    account_id="external")  # external avoids lineage check
        conn.execute.assert_called()

    def test_record_fill_exit_reason_in_insert(self):
        """The exit reason must appear in the INSERT VALUES."""
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, exit_reason="TP1")
        # Check the VALUES tuple contains the exit reason
        insert_args = conn.execute.call_args_list[-1][0]
        sql, vals = insert_args[0], insert_args[1]
        assert "TP1" in vals

    def test_sl_close_exit_reason_is_sl(self):
        from shared_lib.persistence.trade_fills import record_fill, ExitReason
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=49000.0, exit_reason=ExitReason.EXIT_STOP_LOSS)
        insert_args = conn.execute.call_args_list[-1][0]
        vals = insert_args[1]
        assert ExitReason.EXIT_STOP_LOSS in vals  # "SL"

    def test_daily_close_exit_reason_accepted(self):
        from shared_lib.persistence.trade_fills import record_fill, ExitReason
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=50500.0, exit_reason=ExitReason.EXIT_DAILY_CLOSE)
        assert conn.execute.called

    def test_kill_switch_exit_reason_accepted(self):
        from shared_lib.persistence.trade_fills import record_fill, ExitReason
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=49500.0, exit_reason=ExitReason.EXIT_KILL_SWITCH)
        assert conn.execute.called

    def test_time_exit_reason_accepted(self):
        from shared_lib.persistence.trade_fills import record_fill, ExitReason
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=50000.0, exit_reason=ExitReason.EXIT_TIME_BASED)
        assert conn.execute.called

    def test_gross_pnl_in_insert_values(self):
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, gross_pnl=1.25)
        insert_args = conn.execute.call_args_list[-1][0]
        vals = insert_args[1]
        assert 1.25 in vals

    def test_net_pnl_used_as_realized_when_no_realized(self):
        """net_pnl fills in realized_pnl when not provided."""
        from shared_lib.persistence.trade_fills import record_fill
        db, conn = self._mock_db()
        record_fill(db, symbol="BTCUSDT", side="LONG", action="CLOSE",
                    qty=0.001, price=51000.0, net_pnl=0.80)
        insert_args = conn.execute.call_args_list[-1][0]
        vals = insert_args[1]
        # realized_pnl should be 0.80 (derived from net_pnl)
        assert 0.80 in vals


# ═══════════════════════════════════════════════════════════════════════════
# E-2: Migration columns
# ═══════════════════════════════════════════════════════════════════════════

class TestMigrationColumns:

    def _create_test_db(self, db_path: str):
        """Create a minimal but migration-compatible DB."""
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        # Include all columns referenced by migration indexes
        conn.execute(
            """CREATE TABLE trade_fills (
                id INTEGER PRIMARY KEY,
                symbol TEXT, timestamp_utc TEXT,
                action TEXT, side TEXT, qty REAL, price REAL,
                fee REAL, realized_pnl REAL, strategy TEXT,
                bot_instance_id TEXT, position_id TEXT,
                run_id TEXT, cycle_id TEXT, trace_id TEXT
            )"""
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS decision_traces "
            "(id INTEGER PRIMARY KEY, trace_id TEXT, run_id TEXT, "
            " cycle_id TEXT, symbol TEXT)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bot_symbol_state "
            "(id INTEGER PRIMARY KEY, symbol TEXT)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS daily_state "
            "(id INTEGER PRIMARY KEY, day TEXT)"
        )
        conn.commit()
        conn.close()

    def test_migration_adds_gross_pnl_column(self):
        """E-2 columns must be added by _add_column_if_missing helper."""
        import sqlite3, tempfile, os
        from shared_lib.persistence.migrations import _add_column_if_missing

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute(
                "CREATE TABLE trade_fills "
                "(id INTEGER PRIMARY KEY, symbol TEXT, timestamp_utc TEXT)"
            )
            conn.commit()

            # Apply just the E-2 column additions
            for col, col_type in [
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
                _add_column_if_missing(conn, "trade_fills", col, col_type)
            conn.commit()

            cols = {r["name"] for r in conn.execute("PRAGMA table_info(trade_fills)").fetchall()}
            conn.close()

            for col in ("gross_pnl", "total_fees", "funding_fees", "net_pnl", "net_pnl_percent"):
                assert col in cols, f"Column {col!r} missing after migration"
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass

    def test_migration_is_idempotent(self):
        """Running _add_column_if_missing twice must not crash."""
        import sqlite3, tempfile, os
        from shared_lib.persistence.migrations import _add_column_if_missing

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute(
                "CREATE TABLE trade_fills "
                "(id INTEGER PRIMARY KEY, symbol TEXT, timestamp_utc TEXT)"
            )
            conn.commit()

            # Run twice — must not raise
            for _ in range(2):
                for col, col_type in [("gross_pnl", "REAL"), ("net_pnl", "REAL")]:
                    _add_column_if_missing(conn, "trade_fills", col, col_type)
            conn.commit()
            conn.close()
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass
