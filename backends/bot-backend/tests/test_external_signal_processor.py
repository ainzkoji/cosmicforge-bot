"""
Phase 4 TradingView Runner Adapter — comprehensive unit tests.

Coverage:
  - Environment gate (disabled flag, testnet-only gate)
  - Queue claiming (atomic CAS, race condition)
  - Expiry handling
  - Action validation (BUY/SELL pass; CLOSE/etc. reject)
  - All safety gates: event blackout, kill switch, max positions, duplicate entry
  - Sizing validation
  - Execution paths: PAPER_ONLY, FILLED, STALE_DATA_DETECTED, exception, unknown failure
  - DB audit trail: queue status + decision table updated on every path
  - Regression: env gate blocks before any DB access; no mutation on race-condition skip
"""
from __future__ import annotations

import sqlite3
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, call

# ---------------------------------------------------------------------------
# Minimal stubs so the module imports without the full app tree
# ---------------------------------------------------------------------------

def _make_settings(**overrides):
    s = MagicMock()
    s.TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED = overrides.get("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", True)
    s.TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED = overrides.get("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED", True)
    s.TRADINGVIEW_ALLOWED_ACTIONS = overrides.get("TRADINGVIEW_ALLOWED_ACTIONS", "BUY,SELL")
    s.TRADINGVIEW_ALLOWED_SYMBOLS = overrides.get("TRADINGVIEW_ALLOWED_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,XRPUSDT,ADAUSDT")
    s.TRADINGVIEW_MAX_SIGNALS_PER_HOUR = overrides.get("TRADINGVIEW_MAX_SIGNALS_PER_HOUR", 99)
    s.TRADINGVIEW_MAX_SIGNALS_PER_DAY = overrides.get("TRADINGVIEW_MAX_SIGNALS_PER_DAY", 999)
    s.TRADINGVIEW_MAX_EXECUTIONS_PER_DAY = overrides.get("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY", 99)
    s.TRADINGVIEW_MAX_QUEUE_PER_CYCLE = overrides.get("TRADINGVIEW_MAX_QUEUE_PER_CYCLE", 3)
    s.TRADINGVIEW_MAX_TRADE_USDT_CAP = overrides.get("TRADINGVIEW_MAX_TRADE_USDT_CAP", 150.0)
    s.TRADINGVIEW_REQUIRE_SLTP_PROTECTION = overrides.get("TRADINGVIEW_REQUIRE_SLTP_PROTECTION", True)
    s.TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL = overrides.get("TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL", True)
    s.TRADINGVIEW_SAFETY_LOCKOUT_ENABLED = overrides.get("TRADINGVIEW_SAFETY_LOCKOUT_ENABLED", False)
    s.TRADINGVIEW_SAFETY_LOCKOUT_REASON = overrides.get("TRADINGVIEW_SAFETY_LOCKOUT_REASON", "")
    s.TRADINGVIEW_ALLOW_CLOSE = overrides.get("TRADINGVIEW_ALLOW_CLOSE", False)
    s.TRADINGVIEW_ALLOW_REVERSE = overrides.get("TRADINGVIEW_ALLOW_REVERSE", False)
    s.TRADINGVIEW_ALLOW_REDUCE = overrides.get("TRADINGVIEW_ALLOW_REDUCE", False)
    s.TRADINGVIEW_ALLOW_CANCEL = overrides.get("TRADINGVIEW_ALLOW_CANCEL", False)
    s.TRADINGVIEW_ALLOW_EXTERNAL_SLTP = overrides.get("TRADINGVIEW_ALLOW_EXTERNAL_SLTP", False)
    s.TRADINGVIEW_ALLOW_EXTERNAL_SIZE = overrides.get("TRADINGVIEW_ALLOW_EXTERNAL_SIZE", False)
    s.TRADINGVIEW_ALLOW_RISK_OVERRIDE = overrides.get("TRADINGVIEW_ALLOW_RISK_OVERRIDE", False)
    s.TRADINGVIEW_TESTNET_ONLY = overrides.get("TRADINGVIEW_TESTNET_ONLY", True)
    s.TRADINGVIEW_QUEUE_MAX_PER_CYCLE = overrides.get("TRADINGVIEW_QUEUE_MAX_PER_CYCLE", 3)
    s.BINANCE_ENV = overrides.get("BINANCE_ENV", "testnet")
    s.TRADINGVIEW_LIVE_MODE_PROOF_ENABLED = overrides.get("TRADINGVIEW_LIVE_MODE_PROOF_ENABLED", False)
    s.TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED = overrides.get("TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED", False)
    s.TRADINGVIEW_ALLOW_PAPER_LIVE_MODE = overrides.get("TRADINGVIEW_ALLOW_PAPER_LIVE_MODE", False)
    s.PAPER_TRADING_MODE = overrides.get("PAPER_TRADING_MODE", False)
    s.MAX_OPEN_POSITIONS = overrides.get("MAX_OPEN_POSITIONS", 3)
    s.TRADE_USDT_PER_ORDER = overrides.get("TRADE_USDT_PER_ORDER", 10.0)
    return s


def _utc_iso(delta_seconds: int = 0) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=delta_seconds)
    return dt.isoformat()


# ---------------------------------------------------------------------------
# In-memory DB helper (mirrors the real schema the processor uses)
# ---------------------------------------------------------------------------

def _make_in_memory_db():
    """
    Returns a minimal DB-like object backed by an in-memory SQLite connection
    with the external_signal_queue and tradingview_signal_decisions tables.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE external_signal_queue (
            id TEXT PRIMARY KEY,
            source TEXT,
            source_alert_id TEXT,
            bot_id TEXT,
            symbol TEXT,
            side TEXT,
            action TEXT,
            confidence REAL,
            status TEXT DEFAULT 'PENDING',
            available_at TEXT,
            expires_at TEXT,
            claimed_at TEXT,
            processed_at TEXT,
            result TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE tradingview_signal_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER,
            bot_id TEXT,
            symbol TEXT,
            action TEXT,
            mode TEXT,
            normalized_signal_json TEXT,
            event_filter_result TEXT,
            policy_result TEXT,
            sizing_result TEXT,
            execution_result TEXT,
            decision_trace_id TEXT,
            final_status TEXT,
            final_reason TEXT,
            queue_id TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE tradingview_safety_lockouts (
            bot_instance_id TEXT PRIMARY KEY,
            is_locked INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE position_lifecycle_state (
            bot_instance_id TEXT,
            symbol TEXT,
            exchange_position_active INTEGER,
            sl_order_id TEXT,
            tp_order_id TEXT
        )
    """)
    conn.commit()

    class _DB:
        def connect(self):
            return _Ctx(conn)

    class _Ctx:
        def __init__(self, c):
            self._c = c

        def __enter__(self):
            return self._c

        def __exit__(self, *_):
            self._c.commit()

    return _DB(), conn


def _insert_queue_row(
    conn,
    *,
    queue_id: str = "extsig_test001",
    bot_id: str = "bot_1",
    symbol: str = "BTCUSDT",
    action: str = "BUY",
    status: str = "PENDING",
    expires_at: str | None = None,
    created_at: str | None = None,
    source_alert_id: str | None = None,
):
    if expires_at is None:
        expires_at = _utc_iso(+600)  # 10 min from now
    if created_at is None:
        created_at = _utc_iso(-5)
    if source_alert_id is None:
        source_alert_id = f"alert_{queue_id}"
    conn.execute(
        """
        INSERT INTO external_signal_queue
            (id, source, source_alert_id, bot_id, symbol, side, action,
             confidence, status, available_at, expires_at, claimed_at,
             processed_at, result, created_at)
        VALUES
            (?, 'TRADINGVIEW', ?, ?, ?, NULL, ?, 0.8, ?,
             ?, ?, NULL, NULL, NULL, ?)
        """,
        (queue_id, source_alert_id, bot_id, symbol, action, status, created_at, expires_at, created_at),
    )
    conn.execute(
        """
        INSERT INTO tradingview_signal_decisions
            (alert_id, bot_id, symbol, action, mode, normalized_signal_json,
             event_filter_result, policy_result, sizing_result, execution_result,
             decision_trace_id, final_status, final_reason, queue_id, created_at)
        VALUES
            (1, ?, ?, ?, 'EXTERNAL_SIGNAL_CANDIDATE', '{}',
             NULL, NULL, NULL, 'NOT_APPLICABLE',
             NULL, 'QUEUED_EXTERNAL_SIGNAL', 'Queued by phase 3', ?, ?)
        """,
        (bot_id, symbol, action, queue_id, created_at),
    )
    conn.commit()


def _queue_row(conn, queue_id: str) -> dict:
    row = conn.execute(
        "SELECT * FROM external_signal_queue WHERE id=?", (queue_id,)
    ).fetchone()
    return dict(row) if row else {}


def _decision_row(conn, queue_id: str) -> dict:
    row = conn.execute(
        "SELECT * FROM tradingview_signal_decisions WHERE queue_id=?", (queue_id,)
    ).fetchone()
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Runner mock helpers
# ---------------------------------------------------------------------------

def _make_runner(
    *,
    kill=False,
    positions: dict[str, str] | None = None,  # symbol → position label
    trade_usdt: float = 10.0,
    event_blocked: bool = False,
    event_reason: str = "TEST_BLACKOUT",
    exec_status: str = "PAPER_ONLY",
    exec_success: bool = False,
    exec_order_id: str | None = None,
    exec_avg_price: float | None = None,
    exec_error: str | None = None,
    exec_raises: Exception | None = None,
):
    from unittest.mock import MagicMock

    # BlockDecision-like object
    blackout = MagicMock()
    blackout.is_blocked = event_blocked
    blackout.reason = event_reason if event_blocked else None

    event_filter = MagicMock()
    event_filter.check = MagicMock(return_value=blackout)

    # Daily state
    daily = MagicMock()
    daily.kill = kill

    # SymbolState
    positions = positions or {}

    def _state_for(sym):
        s = MagicMock()
        s.position = positions.get(sym, "FLAT")
        return s

    state = {sym: _state_for(sym) for sym in positions}

    # ExecResult
    exec_result = MagicMock()
    exec_result.status = exec_status
    exec_result.success = exec_success
    exec_result.order_id = exec_order_id
    exec_result.avg_price = exec_avg_price
    exec_result.error = exec_error

    executor = MagicMock()
    if exec_raises:
        executor.execute_signal = MagicMock(side_effect=exec_raises)
    else:
        executor.execute_signal = MagicMock(return_value=exec_result)

    runner = MagicMock()
    runner.event_blackout_filter = event_filter
    runner.daily = daily
    runner.state = state
    runner.trade_usdt = trade_usdt
    runner.executor = executor

    def _runner_adapter(candidate: dict[str, Any]):
        symbol = str(candidate.get("symbol") or "").upper()
        action = str(candidate.get("action") or "").upper()
        if kill:
            return {
                "queue_status": "REJECTED",
                "final_status": "REJECTED_KILL_SWITCH",
                "final_reason": "Kill switch is active",
                "event_filter_result": "PASS",
                "policy_result": "BLOCKED:KILL_SWITCH",
                "sizing_result": None,
                "execution_result": "NOT_CALLED:POLICY_RISK",
                "decision_trace_id": "trace_extsig",
            }
        open_count = sum(
            1
            for s in state.values()
            if getattr(s, "position", "FLAT") in ("LONG", "SHORT")
        )
        if open_count >= 3 and symbol not in positions:
            return {
                "queue_status": "REJECTED",
                "final_status": "REJECTED_MAX_POSITIONS",
                "final_reason": "Max open positions reached",
                "event_filter_result": "PASS",
                "policy_result": "BLOCKED:MAX_OPEN_POSITIONS",
                "sizing_result": None,
                "execution_result": "NOT_CALLED:POLICY_RISK",
                "decision_trace_id": "trace_extsig",
            }
        existing = positions.get(symbol)
        if (action == "BUY" and existing == "LONG") or (
            action == "SELL" and existing == "SHORT"
        ):
            return {
                "queue_status": "REJECTED",
                "final_status": "REJECTED_DUPLICATE_POSITION",
                "final_reason": "Duplicate position blocked",
                "event_filter_result": "PASS",
                "policy_result": "BLOCKED:DUPLICATE_POSITION",
                "sizing_result": None,
                "execution_result": "NOT_CALLED:DUPLICATE_POSITION",
                "decision_trace_id": "trace_extsig",
            }
        if trade_usdt <= 0:
            return {
                "queue_status": "REJECTED",
                "final_status": "REJECTED_SIZING",
                "final_reason": "Policy sizing produced zero/negative risk_usdt",
                "event_filter_result": "PASS",
                "policy_result": "PASS",
                "sizing_result": "risk_usdt<=0",
                "execution_result": "NOT_CALLED:SIZING",
                "decision_trace_id": "trace_extsig",
            }
        if exec_raises:
            raise exec_raises
        if exec_status == "STALE_DATA_DETECTED":
            return {
                "queue_status": "REJECTED",
                "final_status": "REJECTED_STALE_MARKET_DATA",
                "final_reason": "STALE_DATA_DETECTED",
                "event_filter_result": "PASS",
                "policy_result": "PASS",
                "sizing_result": f"PASS:{trade_usdt}",
                "execution_result": "STALE_DATA_DETECTED",
                "decision_trace_id": "trace_extsig",
            }
        if exec_status in {"PAPER_ONLY", "FILLED", "ORDER_PLACED"}:
            return {
                "queue_status": "PROCESSED",
                "final_status": "PROCESSED_EXECUTED",
                "final_reason": exec_status,
                "event_filter_result": "PASS",
                "policy_result": "PASS",
                "sizing_result": f"PASS:{trade_usdt}",
                "execution_result": f"{exec_status}:bot_computed_size={trade_usdt}",
                "decision_trace_id": "trace_extsig",
                "bot_computed_size": trade_usdt,
            }
        return {
            "queue_status": "FAILED",
            "final_status": "FAILED_EXECUTION",
            "final_reason": exec_error or exec_status,
            "event_filter_result": "PASS",
            "policy_result": "PASS",
            "sizing_result": f"PASS:{trade_usdt}",
            "execution_result": f"FAILED:{exec_status}",
            "decision_trace_id": "trace_extsig",
        }

    runner.process_external_signal_candidate = MagicMock(side_effect=_runner_adapter)
    return runner


# ---------------------------------------------------------------------------
# Actual test cases
# ---------------------------------------------------------------------------

class TestEnvGate(unittest.TestCase):
    """Env gate blocks all processing before any DB access."""

    def _run_processor(self, settings_kwargs: dict, pending_rows: int = 1):
        db, conn = _make_in_memory_db()
        if pending_rows:
            _insert_queue_row(conn, bot_id="bot_1")
        settings_obj = _make_settings(**settings_kwargs)
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        return result, conn

    def test_disabled_flag_returns_empty(self):
        result, conn = self._run_processor({"TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": False})
        self.assertEqual(result, [])
        # Queue row must NOT have been mutated
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "PENDING")

    def test_phase6_limited_mode_disabled_returns_empty(self):
        result, conn = self._run_processor({"TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED": False})
        self.assertEqual(result, [])
        self.assertEqual(_queue_row(conn, "extsig_test001")["status"], "PENDING")

    def test_testnet_only_mainnet_returns_empty(self):
        result, conn = self._run_processor(
            {"TRADINGVIEW_TESTNET_ONLY": True, "BINANCE_ENV": "mainnet"}
        )
        self.assertEqual(result, [])
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "PENDING")

    def test_testnet_only_testnet_passes_gate(self):
        """testnet env passes the testnet-only gate."""
        result, conn = self._run_processor(
            {"TRADINGVIEW_TESTNET_ONLY": True, "BINANCE_ENV": "testnet"}
        )
        # Should have processed (paper mode)
        self.assertTrue(len(result) > 0)

    def test_testnet_only_demo_passes_gate(self):
        result, conn = self._run_processor(
            {"TRADINGVIEW_TESTNET_ONLY": True, "BINANCE_ENV": "demo"}
        )
        self.assertTrue(len(result) > 0)

    def test_testnet_only_false_live_env_still_requires_explicit_live_acknowledgement(self):
        result, conn = self._run_processor(
            {"TRADINGVIEW_TESTNET_ONLY": False, "BINANCE_ENV": "mainnet"}
        )
        self.assertEqual(result, [])
        self.assertEqual(_queue_row(conn, "extsig_test001")["status"], "PENDING")


class TestExpiry(unittest.TestCase):
    def _processor_with_row(self, expires_delta_s: int):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, expires_at=_utc_iso(expires_delta_s))
        settings_obj = _make_settings()
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        return result, conn

    def test_expired_signal_marked_expired(self):
        result, conn = self._processor_with_row(-60)  # 1 min ago
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["outcome"], "EXPIRED")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "EXPIRED")

    def test_expired_decision_row_updated(self):
        _,  conn = self._processor_with_row(-60)
        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "EXPIRED")
        self.assertIn("expired", dec["final_reason"].lower())

    def test_future_signal_not_expired(self):
        result, conn = self._processor_with_row(+600)
        self.assertNotEqual(result[0]["outcome"], "EXPIRED")


class TestActionValidation(unittest.TestCase):
    def _run_action(self, action: str):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, action=action)
        settings_obj = _make_settings()
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        return result, conn

    def test_buy_passes(self):
        result, _ = self._run_action("BUY")
        self.assertNotEqual(result[0]["outcome"], "REJECTED_UNSUPPORTED_ACTION")

    def test_sell_passes(self):
        result, _ = self._run_action("SELL")
        self.assertNotEqual(result[0]["outcome"], "REJECTED_UNSUPPORTED_ACTION")

    def test_close_rejected(self):
        result, conn = self._run_action("CLOSE")
        self.assertEqual(result[0]["outcome"], "REJECTED_UNSUPPORTED_ACTION")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "REJECTED")

    def test_reduce_rejected(self):
        result, _ = self._run_action("REDUCE")
        self.assertEqual(result[0]["outcome"], "REJECTED_UNSUPPORTED_ACTION")

    def test_reverse_rejected(self):
        result, _ = self._run_action("REVERSE")
        self.assertEqual(result[0]["outcome"], "REJECTED_UNSUPPORTED_ACTION")

    def test_rejected_action_decision_row_updated(self):
        _, conn = self._run_action("CLOSE")
        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "REJECTED_TV_ACTION_NOT_ALLOWED")


class TestPhase6LimitedGate(unittest.TestCase):
    def _run(self, *, settings_kwargs: dict | None = None, symbol: str = "BTCUSDT", action: str = "BUY"):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, symbol=symbol, action=action)
        settings_obj = _make_settings(**(settings_kwargs or {}))
        runner = _make_runner()
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor

            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)
        return result, conn, runner

    def test_symbol_not_allowed_rejected_before_runner(self):
        result, conn, runner = self._run(
            settings_kwargs={"TRADINGVIEW_ALLOWED_SYMBOLS": "ETHUSDT"},
            symbol="BTCUSDT",
        )
        self.assertEqual(result[0]["outcome"], "REJECTED_TV_SYMBOL_NOT_ALLOWED")
        self.assertEqual(_queue_row(conn, "extsig_test001")["status"], "REJECTED")
        runner.process_external_signal_candidate.assert_not_called()

    def test_allowed_buy_reaches_runner(self):
        result, _conn, runner = self._run(
            settings_kwargs={"TRADINGVIEW_ALLOWED_SYMBOLS": "BTCUSDT"},
            action="BUY",
        )
        self.assertEqual(result[0]["outcome"], "PROCESSED_EXECUTED")
        runner.process_external_signal_candidate.assert_called_once()

    def test_external_sltp_flag_blocks_limited_mode(self):
        result, conn, runner = self._run(
            settings_kwargs={"TRADINGVIEW_ALLOW_EXTERNAL_SLTP": True},
        )
        self.assertEqual(result[0]["outcome"], "REJECTED_TV_LIMITED_MODE_DISABLED")
        self.assertEqual(_decision_row(conn, "extsig_test001")["final_status"], "REJECTED_TV_LIMITED_MODE_DISABLED")
        runner.process_external_signal_candidate.assert_not_called()

    def test_daily_execution_cap_rejected(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        conn.execute(
            """
            INSERT INTO tradingview_signal_decisions (
                alert_id, bot_id, symbol, action, mode, normalized_signal_json,
                final_status, final_reason, queue_id, created_at
            ) VALUES (99, 'bot_1', 'BTCUSDT', 'BUY', 'EXTERNAL_SIGNAL_CANDIDATE',
                      '{}', 'PROCESSED_EXECUTED', 'old execution', 'old_queue', ?)
            """,
            (_utc_iso(-60),),
        )
        conn.commit()
        settings_obj = _make_settings(TRADINGVIEW_MAX_EXECUTIONS_PER_DAY=1)
        runner = _make_runner()
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor

            result = ExternalSignalProcessor(db).process_pending_for_bot("bot_1", runner)
        self.assertEqual(result[0]["outcome"], "REJECTED_TV_DAILY_EXECUTION_CAP")
        runner.process_external_signal_candidate.assert_not_called()

    def test_unprotected_position_triggers_safety_lockout(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        conn.execute(
            """
            INSERT INTO position_lifecycle_state (
                bot_instance_id, symbol, exchange_position_active, sl_order_id, tp_order_id
            ) VALUES ('bot_1', 'BTCUSDT', 1, NULL, NULL)
            """
        )
        conn.commit()
        settings_obj = _make_settings()
        runner = _make_runner()
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor

            result = ExternalSignalProcessor(db).process_pending_for_bot("bot_1", runner)
        self.assertEqual(result[0]["outcome"], "REJECTED_TV_SAFETY_LOCKOUT")
        lockout = conn.execute("SELECT * FROM tradingview_safety_lockouts WHERE bot_instance_id='bot_1'").fetchone()
        self.assertEqual(lockout["is_locked"], 1)
        runner.process_external_signal_candidate.assert_not_called()


class TestAtomicClaim(unittest.TestCase):
    """Simulate a race condition where status changes between fetch and claim."""

    def test_already_claimed_row_skipped(self):
        db, conn = _make_in_memory_db()
        # Insert as CLAIMED to simulate another processor won the race
        _insert_queue_row(conn, status="CLAIMED")
        settings_obj = _make_settings()
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            # Inject row manually so fetch returns it despite CLAIMED status:
            # We monkey-patch _fetch_pending to return the row regardless
            proc._fetch_pending = lambda bot_id, max_rows: [
                {
                    "id": "extsig_test001",
                    "symbol": "BTCUSDT",
                    "action": "BUY",
                    "expires_at": _utc_iso(+600),
                    "created_at": _utc_iso(-5),
                }
            ]
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "SKIPPED_ALREADY_CLAIMED")
        # Status must remain CLAIMED (not mutated further)
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "CLAIMED")


class TestEventBlackout(unittest.TestCase):
    def test_blocked_by_event(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings()
        runner = _make_runner(event_blocked=True, event_reason="HIGH_IMPACT_USD_NFP_BLACKOUT")

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "REJECTED_EVENT_BLACKOUT")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "REJECTED")

    def test_event_filter_result_written(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings()
        runner = _make_runner(event_blocked=True, event_reason="HIGH_IMPACT_USD_NFP_BLACKOUT")

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        dec = _decision_row(conn, "extsig_test001")
        self.assertIn("BLOCKED", dec["event_filter_result"])
        self.assertEqual(dec["final_status"], "REJECTED_EVENT_BLACKOUT")


class TestKillSwitch(unittest.TestCase):
    def test_kill_switch_active_rejects(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings()
        runner = _make_runner(kill=True)

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "REJECTED_KILL_SWITCH")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "REJECTED")

    def test_kill_switch_decision_row_updated(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings()
        runner = _make_runner(kill=True)

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "REJECTED_KILL_SWITCH")
        self.assertEqual(dec["event_filter_result"], "PASS")  # gate passed before kill switch


class TestMaxPositions(unittest.TestCase):
    def test_at_max_rejects(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, symbol="XRPUSDT")
        settings_obj = _make_settings(MAX_OPEN_POSITIONS=3)
        # 3 open positions already
        runner = _make_runner(
            positions={"BTCUSDT": "LONG", "ETHUSDT": "LONG", "ADAUSDT": "LONG"}
        )

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "REJECTED_MAX_POSITIONS")

    def test_under_max_proceeds(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, symbol="XRPUSDT")
        settings_obj = _make_settings(MAX_OPEN_POSITIONS=3)
        runner = _make_runner(positions={"BTCUSDT": "LONG"})

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertNotEqual(result[0]["outcome"], "REJECTED_MAX_POSITIONS")


class TestDuplicateEntry(unittest.TestCase):
    def test_symbol_already_long_rejects(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, symbol="BTCUSDT")
        settings_obj = _make_settings()
        runner = _make_runner(positions={"BTCUSDT": "LONG"})

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "REJECTED_DUPLICATE_POSITION")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "REJECTED")

    def test_symbol_already_short_rejects(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, symbol="ETHUSDT", action="SELL")
        settings_obj = _make_settings()
        runner = _make_runner(positions={"ETHUSDT": "SHORT"})

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "REJECTED_DUPLICATE_POSITION")

    def test_symbol_flat_proceeds(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, symbol="BTCUSDT")
        settings_obj = _make_settings()
        runner = _make_runner(positions={"BTCUSDT": "FLAT"})

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertNotEqual(result[0]["outcome"], "REJECTED_DUPLICATE_POSITION")

    def test_symbol_unknown_to_runner_proceeds(self):
        """Symbol not in runner.state at all (new symbol) should proceed."""
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, symbol="SOLUSDT")
        settings_obj = _make_settings()
        runner = _make_runner(positions={})  # SOLUSDT not tracked

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertNotEqual(result[0]["outcome"], "REJECTED_DUPLICATE_POSITION")


class TestSizing(unittest.TestCase):
    def test_zero_usdt_rejected(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(TRADE_USDT_PER_ORDER=0.0)
        runner = _make_runner(trade_usdt=0.0)

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "REJECTED_SIZING")

    def test_positive_usdt_proceeds(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(TRADE_USDT_PER_ORDER=10.0)
        runner = _make_runner(trade_usdt=10.0)

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertNotEqual(result[0]["outcome"], "REJECTED_SIZING")


class TestExecution(unittest.TestCase):
    """Execution path variants — all run through all safety gates."""

    def _run(self, exec_status="PAPER_ONLY", exec_success=False,
             exec_order_id=None, exec_error=None, exec_raises=None, action="BUY"):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, action=action)
        settings_obj = _make_settings()
        runner = _make_runner(
            exec_status=exec_status,
            exec_success=exec_success,
            exec_order_id=exec_order_id,
            exec_error=exec_error,
            exec_raises=exec_raises,
        )

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        return result, conn

    def test_paper_only_executed_paper(self):
        result, conn = self._run(exec_status="PAPER_ONLY")
        self.assertEqual(result[0]["outcome"], "PROCESSED_EXECUTED")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "PROCESSED")

    def test_paper_only_decision_row(self):
        _, conn = self._run(exec_status="PAPER_ONLY")
        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "PROCESSED_EXECUTED")
        self.assertIn("PAPER", dec["execution_result"])

    def test_filled_executed(self):
        result, conn = self._run(exec_status="FILLED", exec_success=True, exec_order_id="ORD123")
        self.assertEqual(result[0]["outcome"], "PROCESSED_EXECUTED")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "PROCESSED")

    def test_filled_decision_row(self):
        _, conn = self._run(exec_status="FILLED", exec_success=True, exec_order_id="ORD123")
        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "PROCESSED_EXECUTED")
        self.assertIn("FILLED", dec["execution_result"])

    def test_stale_data_rejected(self):
        result, conn = self._run(exec_status="STALE_DATA_DETECTED")
        self.assertEqual(result[0]["outcome"], "REJECTED_STALE_MARKET_DATA")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "REJECTED")

    def test_stale_data_decision_row(self):
        _, conn = self._run(exec_status="STALE_DATA_DETECTED")
        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "REJECTED_STALE_MARKET_DATA")

    def test_executor_exception_failed(self):
        result, conn = self._run(exec_raises=RuntimeError("connection lost"))
        self.assertEqual(result[0]["outcome"], "FAILED_EXECUTION")
        self.assertIn("connection lost", result[0]["final_reason"])
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "FAILED")

    def test_executor_exception_decision_row(self):
        _, conn = self._run(exec_raises=RuntimeError("connection lost"))
        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "FAILED_EXECUTION")
        self.assertIn("connection lost", dec["execution_result"])

    def test_unknown_failure_status(self):
        result, conn = self._run(exec_status="UNKNOWN_ERROR", exec_success=False)
        self.assertEqual(result[0]["outcome"], "FAILED_EXECUTION")
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "FAILED")

    def test_processor_delegates_to_runner_adapter_with_candidate_metadata(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, action="SELL")
        settings_obj = _make_settings()
        runner = _make_runner(exec_status="PAPER_ONLY")

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        # Processor must not call executor directly; it delegates to the runner.
        runner.executor.execute_signal.assert_not_called()
        runner.process_external_signal_candidate.assert_called_once()
        candidate = runner.process_external_signal_candidate.call_args[0][0]
        self.assertEqual(candidate["source"], "TRADINGVIEW")
        self.assertEqual(candidate["bot_id"], "bot_1")
        self.assertEqual(candidate["symbol"], "BTCUSDT")
        self.assertEqual(candidate["action"], "SELL")


class TestMaxPerCycle(unittest.TestCase):
    """max_per_cycle limits rows processed."""

    def test_only_max_rows_processed(self):
        db, conn = _make_in_memory_db()
        for i in range(5):
            _insert_queue_row(
                conn,
                queue_id=f"extsig_test{i:03d}",
                created_at=_utc_iso(-100 + i),
            )
        settings_obj = _make_settings(TRADINGVIEW_QUEUE_MAX_PER_CYCLE=2)
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(len(result), 2)

    def test_no_pending_returns_empty(self):
        db, conn = _make_in_memory_db()
        settings_obj = _make_settings()
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result, [])


class TestAuditTrail(unittest.TestCase):
    """Verify that every outcome path writes coherent audit data to both tables."""

    def _verify_audit(self, outcome_expected: str, queue_status: str, **runner_kwargs):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings()
        runner = _make_runner(**runner_kwargs)

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], outcome_expected)
        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], queue_status, f"Queue status mismatch for {outcome_expected}")
        dec = _decision_row(conn, "extsig_test001")
        self.assertIsNotNone(dec.get("final_status"), f"Decision row final_status is None for {outcome_expected}")
        self.assertIsNotNone(dec.get("final_reason"), f"Decision row final_reason is None for {outcome_expected}")
        return row, dec

    def test_audit_paper(self):
        row, dec = self._verify_audit("PROCESSED_EXECUTED", "PROCESSED", exec_status="PAPER_ONLY")
        self.assertEqual(dec["event_filter_result"], "PASS")
        self.assertEqual(dec["policy_result"], "PASS")
        self.assertIn("PASS", dec["sizing_result"])

    def test_audit_kill_switch(self):
        row, dec = self._verify_audit("REJECTED_KILL_SWITCH", "REJECTED", kill=True)
        self.assertEqual(dec["event_filter_result"], "PASS")
        self.assertIn("KILL_SWITCH", dec["policy_result"])

    def test_audit_event_blackout(self):
        row, dec = self._verify_audit(
            "REJECTED_EVENT_BLACKOUT", "REJECTED",
            event_blocked=True, event_reason="HIGH_IMPACT_USD_NFP_BLACKOUT"
        )
        self.assertIn("BLOCKED", dec["event_filter_result"])
        self.assertIsNone(dec["policy_result"])

    def test_audit_max_positions(self):
        row, dec = self._verify_audit(
            "REJECTED_MAX_POSITIONS", "REJECTED",
            positions={"ETHUSDT": "LONG", "XRPUSDT": "LONG", "ADAUSDT": "LONG"},
        )
        self.assertIn("MAX_OPEN_POSITIONS", dec["policy_result"])


class TestRegression(unittest.TestCase):
    """Regression: invariants that must never break."""

    def test_env_gate_no_db_mutation(self):
        """When gate blocks, zero DB mutations."""
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=False)
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        row = _queue_row(conn, "extsig_test001")
        self.assertEqual(row["status"], "PENDING")  # untouched
        dec = _decision_row(conn, "extsig_test001")
        self.assertEqual(dec["final_status"], "QUEUED_EXTERNAL_SIGNAL")  # untouched

    def test_executor_not_called_when_gate_blocks(self):
        """Executor.execute_signal must never be invoked when env gate blocks."""
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=False)
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        runner.executor.execute_signal.assert_not_called()

    def test_executor_not_called_when_kill_switch(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings()
        runner = _make_runner(kill=True)

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        runner.executor.execute_signal.assert_not_called()

    def test_multiple_signals_independent(self):
        """Each signal is processed independently; one failure does not stop others."""
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, queue_id="extsig_a", symbol="BTCUSDT", created_at=_utc_iso(-10))
        # Second signal: expired
        _insert_queue_row(conn, queue_id="extsig_b", symbol="ETHUSDT",
                          expires_at=_utc_iso(-60), created_at=_utc_iso(-5))
        settings_obj = _make_settings()
        runner = _make_runner(exec_status="PAPER_ONLY")

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(len(result), 2)
        outcomes = {r["symbol"]: r["outcome"] for r in result}
        self.assertEqual(outcomes["BTCUSDT"], "PROCESSED_EXECUTED")
        self.assertEqual(outcomes["ETHUSDT"], "EXPIRED")


# ===========================================================================
# NEW TESTS — Phase 4 Automation hardening (added in automation pass)
# ===========================================================================


class TestLiveEnvGuard(unittest.TestCase):
    """3-factor unlock required for live/mainnet environments."""

    def _run_gate(self, **settings_kwargs):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(**settings_kwargs)
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        return result, conn

    def test_mainnet_blocked_by_default(self):
        """live env with testnet_only=True (default) must be blocked."""
        result, conn = self._run_gate(BINANCE_ENV="mainnet", TRADINGVIEW_TESTNET_ONLY=True)
        self.assertEqual(result, [])
        self.assertEqual(_queue_row(conn, "extsig_test001")["status"], "PENDING")

    def test_live_blocked_by_default(self):
        result, conn = self._run_gate(BINANCE_ENV="live", TRADINGVIEW_TESTNET_ONLY=True)
        self.assertEqual(result, [])

    def test_prod_blocked_by_default(self):
        result, conn = self._run_gate(BINANCE_ENV="prod", TRADINGVIEW_TESTNET_ONLY=True)
        self.assertEqual(result, [])

    def test_mainnet_testnet_only_false_missing_live_mode_acknowledgement(self):
        """factor 1 set (testnet_only=false) but factor 2 missing."""
        result, conn = self._run_gate(
            BINANCE_ENV="mainnet",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_ALLOW_PAPER_LIVE_MODE=False,
            PAPER_TRADING_MODE=False,
        )
        self.assertEqual(result, [])
        self.assertEqual(_queue_row(conn, "extsig_test001")["status"], "PENDING")

    def test_mainnet_missing_second_live_mode_acknowledgement_flag(self):
        """One live-mode acknowledgement flag is not enough."""
        result, conn = self._run_gate(
            BINANCE_ENV="mainnet",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_LIVE_MODE_PROOF_ENABLED=True,
            TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED=False,
        )
        self.assertEqual(result, [])
        self.assertEqual(_queue_row(conn, "extsig_test001")["status"], "PENDING")

    def test_mainnet_explicit_live_mode_acknowledgement_allows_processing(self):
        """All 3 factors set → processing allowed on mainnet."""
        result, conn = self._run_gate(
            BINANCE_ENV="mainnet",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_LIVE_MODE_PROOF_ENABLED=True,
            TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED=True,
        )
        # Should have processed; executor status is mocked.
        self.assertTrue(len(result) > 0)
        self.assertNotEqual(_queue_row(conn, "extsig_test001")["status"], "PENDING")

    def test_live_all_three_factors_allows_processing(self):
        result, conn = self._run_gate(
            BINANCE_ENV="live",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_ALLOW_PAPER_LIVE_MODE=True,
            PAPER_TRADING_MODE=True,
        )
        self.assertTrue(len(result) > 0)

    def test_unknown_env_blocked(self):
        """Unrecognised BINANCE_ENV is always blocked (conservative default)."""
        result, conn = self._run_gate(BINANCE_ENV="staging")
        self.assertEqual(result, [])

    def test_unknown_env_no_db_mutation(self):
        _, conn = self._run_gate(BINANCE_ENV="staging")
        self.assertEqual(_queue_row(conn, "extsig_test001")["status"], "PENDING")

    def test_no_claim_when_live_env_blocked(self):
        """No queue row should be CLAIMED when env gate blocks."""
        _, conn = self._run_gate(BINANCE_ENV="mainnet", TRADINGVIEW_TESTNET_ONLY=True)
        self.assertNotEqual(_queue_row(conn, "extsig_test001")["status"], "CLAIMED")

    def test_env_gate_reason_live_testnet_only(self):
        """env_gate_reason returns informative string for live+testnet_only."""
        from app.queue.external_signal_processor import ExternalSignalProcessor
        db, _ = _make_in_memory_db()
        settings_obj = _make_settings(BINANCE_ENV="mainnet", TRADINGVIEW_TESTNET_ONLY=True)
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            reason = proc._env_gate_reason()
        self.assertIsNotNone(reason)
        self.assertIn("TESTNET_ONLY_GATE", reason)

    def test_env_gate_reason_live_missing_live_mode_acknowledgement(self):
        from app.queue.external_signal_processor import ExternalSignalProcessor
        db, _ = _make_in_memory_db()
        settings_obj = _make_settings(
            BINANCE_ENV="mainnet",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_ALLOW_PAPER_LIVE_MODE=False,
        )
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            reason = proc._env_gate_reason()
        self.assertIsNotNone(reason)
        self.assertIn("LIVE_ENV_BLOCKED", reason)
        self.assertIn("TRADINGVIEW_LIVE_MODE_PROOF_ENABLED", reason)

    def test_env_gate_reason_live_missing_acknowledged_flag(self):
        from app.queue.external_signal_processor import ExternalSignalProcessor
        db, _ = _make_in_memory_db()
        settings_obj = _make_settings(
            BINANCE_ENV="mainnet",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_LIVE_MODE_PROOF_ENABLED=True,
            TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED=False,
        )
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            reason = proc._env_gate_reason()
        self.assertIsNotNone(reason)
        self.assertIn("TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED", reason)

    def test_env_gate_reason_none_for_safe_env(self):
        from app.queue.external_signal_processor import ExternalSignalProcessor
        db, _ = _make_in_memory_db()
        settings_obj = _make_settings(BINANCE_ENV="testnet")
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            reason = proc._env_gate_reason()
        self.assertIsNone(reason)

    def test_env_gate_reason_none_with_live_mode_acknowledgement(self):
        from app.queue.external_signal_processor import ExternalSignalProcessor
        db, _ = _make_in_memory_db()
        settings_obj = _make_settings(
            BINANCE_ENV="mainnet",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_LIVE_MODE_PROOF_ENABLED=True,
            TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED=True,
        )
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            reason = proc._env_gate_reason()
        self.assertIsNone(reason)


class TestStaleClaimed(unittest.TestCase):
    """Stale CLAIMED row recovery."""

    def _make_claimed_row(self, conn, age_minutes: int, queue_id: str = "extsig_stale001"):
        claimed_at = (
            datetime.now(timezone.utc) - timedelta(minutes=age_minutes)
        ).isoformat()
        conn.execute(
            """
            INSERT INTO external_signal_queue
                (id, source, source_alert_id, bot_id, symbol, side, action,
                 confidence, status, available_at, expires_at, claimed_at,
                 processed_at, result, created_at)
            VALUES
                (?, 'TRADINGVIEW', 'alert_stale', 'bot_1', 'BTCUSDT', NULL, 'BUY', 0.8,
                 'CLAIMED', ?, ?, ?, NULL, NULL, ?)
            """,
            (
                queue_id,
                claimed_at,
                _utc_iso(+3600),
                claimed_at,
                claimed_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO tradingview_signal_decisions
                (alert_id, bot_id, symbol, action, mode, normalized_signal_json,
                 event_filter_result, policy_result, sizing_result, execution_result,
                 decision_trace_id, final_status, final_reason, queue_id, created_at)
            VALUES (1, 'bot_1', 'BTCUSDT', 'BUY', 'EXTERNAL_SIGNAL_CANDIDATE', '{}',
                    NULL, NULL, NULL, 'NOT_APPLICABLE', NULL,
                    'QUEUED_EXTERNAL_SIGNAL', 'Queued', ?, ?)
            """,
            (queue_id, claimed_at),
        )
        conn.commit()

    def test_stale_claimed_marked_failed(self):
        db, conn = _make_in_memory_db()
        self._make_claimed_row(conn, age_minutes=15)  # older than 10-min timeout

        from app.queue.external_signal_processor import ExternalSignalProcessor
        settings_obj = _make_settings()
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            count = proc.recover_stale_claimed("bot_1", timeout_minutes=10)

        self.assertEqual(count, 1)
        row = _queue_row(conn, "extsig_stale001")
        self.assertEqual(row["status"], "FAILED")
        self.assertIn("STALE_CLAIM_TIMEOUT", row["result"])

    def test_fresh_claimed_not_touched(self):
        db, conn = _make_in_memory_db()
        self._make_claimed_row(conn, age_minutes=2)  # 2 min old, under 10-min timeout

        from app.queue.external_signal_processor import ExternalSignalProcessor
        settings_obj = _make_settings()
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            count = proc.recover_stale_claimed("bot_1", timeout_minutes=10)

        self.assertEqual(count, 0)
        row = _queue_row(conn, "extsig_stale001")
        self.assertEqual(row["status"], "CLAIMED")  # untouched

    def test_stale_claimed_decision_row_updated(self):
        db, conn = _make_in_memory_db()
        self._make_claimed_row(conn, age_minutes=20)

        from app.queue.external_signal_processor import ExternalSignalProcessor
        settings_obj = _make_settings()
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            proc.recover_stale_claimed("bot_1", timeout_minutes=10)

        dec = _decision_row(conn, "extsig_stale001")
        self.assertEqual(dec["final_status"], "FAILED")
        self.assertIn("timeout", dec["final_reason"].lower())

    def test_stale_claimed_not_reprocessed(self):
        """After recovery, stale rows are FAILED — must not be picked up as PENDING."""
        db, conn = _make_in_memory_db()
        self._make_claimed_row(conn, age_minutes=20)
        settings_obj = _make_settings()
        runner = _make_runner(exec_status="PAPER_ONLY")

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            # Run process_pending_for_bot — stale CLAIMED gets recovered to FAILED,
            # and since it's not PENDING, it's never queued for execution.
            result = proc.process_pending_for_bot("bot_1", runner)

        # No new signals to process (there were no PENDING rows)
        self.assertEqual(result, [])
        # Confirmed FAILED after recovery
        row = _queue_row(conn, "extsig_stale001")
        self.assertEqual(row["status"], "FAILED")
        # Executor must NOT have been called on the stale row
        runner.executor.execute_signal.assert_not_called()

    def test_recovery_only_touches_own_bot(self):
        """Recovery must not affect CLAIMED rows belonging to other bots."""
        db, conn = _make_in_memory_db()
        # Stale CLAIMED row for another bot
        conn.execute(
            """
            INSERT INTO external_signal_queue
                (id, source, source_alert_id, bot_id, symbol, side, action,
                 confidence, status, available_at, expires_at, claimed_at,
                 processed_at, result, created_at)
            VALUES ('other_bot_row', 'TRADINGVIEW', 'alert_other', 'bot_other',
                    'ETHUSDT', NULL, 'BUY', 0.8, 'CLAIMED',
                    ?, ?, ?, NULL, NULL, ?)
            """,
            (
                _utc_iso(-900),  # claimed 15 min ago
                _utc_iso(+3600),
                _utc_iso(-900),
                _utc_iso(-900),
            ),
        )
        conn.commit()

        from app.queue.external_signal_processor import ExternalSignalProcessor
        settings_obj = _make_settings()
        with patch("app.queue.external_signal_processor.settings", settings_obj):
            proc = ExternalSignalProcessor(db)
            count = proc.recover_stale_claimed("bot_1", timeout_minutes=10)

        self.assertEqual(count, 0)  # bot_1 has no stale rows — other bot's row untouched
        row = conn.execute(
            "SELECT status FROM external_signal_queue WHERE id='other_bot_row'"
        ).fetchone()
        self.assertEqual(row["status"], "CLAIMED")


class TestHeartbeat(unittest.TestCase):
    """tradingview_processor_heartbeat table upsert and read."""

    def test_upsert_and_read_heartbeat(self):
        db, conn = _make_in_memory_db()
        # Add heartbeat table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tradingview_processor_heartbeat (
                bot_instance_id TEXT PRIMARY KEY,
                processor_enabled INTEGER NOT NULL DEFAULT 0,
                env_gate_reason TEXT,
                last_started_at TEXT,
                last_finished_at TEXT,
                last_processed_count INTEGER NOT NULL DEFAULT 0,
                last_rejected_count INTEGER NOT NULL DEFAULT 0,
                last_failed_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_reason TEXT,
                last_result_json TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        conn.commit()

        from shared_lib.persistence.tradingview import (
            get_processor_heartbeat,
            list_processor_heartbeats,
            upsert_processor_heartbeat,
        )

        upsert_processor_heartbeat(
            db,
            bot_instance_id="bot_1",
            processor_enabled=True,
            last_started_at=_utc_iso(-5),
            last_finished_at=_utc_iso(-1),
            last_processed_count=2,
            last_rejected_count=1,
            last_failed_count=0,
            last_skipped_count=0,
            last_result_json='[{"BTCUSDT": "EXECUTED_PAPER"}]',
        )

        hb = get_processor_heartbeat(db, "bot_1")
        self.assertIsNotNone(hb)
        self.assertEqual(hb["bot_instance_id"], "bot_1")
        self.assertEqual(hb["processor_enabled"], 1)
        self.assertEqual(hb["last_processed_count"], 2)
        self.assertEqual(hb["last_rejected_count"], 1)

    def test_upsert_idempotent(self):
        """Second upsert overwrites the first."""
        db, conn = _make_in_memory_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tradingview_processor_heartbeat (
                bot_instance_id TEXT PRIMARY KEY,
                processor_enabled INTEGER NOT NULL DEFAULT 0,
                env_gate_reason TEXT,
                last_started_at TEXT,
                last_finished_at TEXT,
                last_processed_count INTEGER NOT NULL DEFAULT 0,
                last_rejected_count INTEGER NOT NULL DEFAULT 0,
                last_failed_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_reason TEXT,
                last_result_json TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        conn.commit()

        from shared_lib.persistence.tradingview import (
            get_processor_heartbeat,
            upsert_processor_heartbeat,
        )

        upsert_processor_heartbeat(db, bot_instance_id="bot_1", processor_enabled=True,
                                   last_processed_count=1)
        upsert_processor_heartbeat(db, bot_instance_id="bot_1", processor_enabled=True,
                                   last_processed_count=5)

        hb = get_processor_heartbeat(db, "bot_1")
        self.assertEqual(hb["last_processed_count"], 5)

    def test_get_nonexistent_returns_none(self):
        db, conn = _make_in_memory_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tradingview_processor_heartbeat (
                bot_instance_id TEXT PRIMARY KEY,
                processor_enabled INTEGER NOT NULL DEFAULT 0,
                env_gate_reason TEXT,
                last_started_at TEXT,
                last_finished_at TEXT,
                last_processed_count INTEGER NOT NULL DEFAULT 0,
                last_rejected_count INTEGER NOT NULL DEFAULT 0,
                last_failed_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_reason TEXT,
                last_result_json TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        conn.commit()

        from shared_lib.persistence.tradingview import get_processor_heartbeat
        hb = get_processor_heartbeat(db, "nonexistent_bot")
        self.assertIsNone(hb)


class TestAutomationInvariantsRegression(unittest.TestCase):
    """Regression tests for the automation hardening requirements."""

    def test_executor_not_called_mainnet_no_live_acknowledgement_flags(self):
        """Live env without explicit live-mode acknowledgement: executor never called."""
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(BINANCE_ENV="mainnet", TRADINGVIEW_TESTNET_ONLY=True)
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        runner.executor.execute_signal.assert_not_called()

    def test_executor_not_called_unknown_env(self):
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(BINANCE_ENV="unknown_exchange")
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        runner.executor.execute_signal.assert_not_called()

    def test_webhook_cannot_bypass_gate_by_setting_action(self):
        """CLOSE action still rejected even with all env flags enabled."""
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn, action="CLOSE")
        settings_obj = _make_settings(
            BINANCE_ENV="mainnet",
            TRADINGVIEW_TESTNET_ONLY=False,
            TRADINGVIEW_ALLOW_PAPER_LIVE_MODE=True,
            PAPER_TRADING_MODE=True,
        )
        runner = _make_runner()

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertEqual(result[0]["outcome"], "REJECTED_UNSUPPORTED_ACTION")
        runner.executor.execute_signal.assert_not_called()

    def test_tradingview_cannot_set_size_via_action(self):
        """Processor delegates only intent; runner computes size internally."""
        db, conn = _make_in_memory_db()
        _insert_queue_row(conn)
        settings_obj = _make_settings(TRADE_USDT_PER_ORDER=10.0)
        runner = _make_runner(exec_status="PAPER_ONLY", trade_usdt=10.0)

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            proc.process_pending_for_bot("bot_1", runner)

        runner.executor.execute_signal.assert_not_called()
        runner.process_external_signal_candidate.assert_called_once()
        self.assertEqual(
            runner.process_external_signal_candidate.call_args[0][0]["action"],
            "BUY",
        )

    def test_max_per_cycle_from_settings(self):
        """TRADINGVIEW_QUEUE_MAX_PER_CYCLE setting is respected."""
        db, conn = _make_in_memory_db()
        for i in range(10):
            _insert_queue_row(conn, queue_id=f"extsig_{i:03d}", created_at=_utc_iso(-100 + i))
        settings_obj = _make_settings(TRADINGVIEW_QUEUE_MAX_PER_CYCLE=2)
        runner = _make_runner(exec_status="PAPER_ONLY")

        with patch("app.queue.external_signal_processor.settings", settings_obj):
            from app.queue.external_signal_processor import ExternalSignalProcessor
            proc = ExternalSignalProcessor(db)
            result = proc.process_pending_for_bot("bot_1", runner)

        self.assertLessEqual(len(result), 2)

    def test_all_existing_safe_envs_still_pass(self):
        """Existing safe envs (testnet, demo, paper, sandbox) all still work."""
        for env in ("testnet", "demo", "paper", "sandbox"):
            with self.subTest(env=env):
                db, conn = _make_in_memory_db()
                _insert_queue_row(conn)
                settings_obj = _make_settings(BINANCE_ENV=env)
                runner = _make_runner(exec_status="PAPER_ONLY")

                with patch("app.queue.external_signal_processor.settings", settings_obj):
                    from app.queue.external_signal_processor import ExternalSignalProcessor
                    proc = ExternalSignalProcessor(db)
                    result = proc.process_pending_for_bot("bot_1", runner)

                self.assertTrue(len(result) > 0, f"env={env} should allow processing")


if __name__ == "__main__":
    unittest.main()
