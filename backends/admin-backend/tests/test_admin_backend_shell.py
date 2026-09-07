from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jose import jwt


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "admin-backend"))
sys.path.insert(0, str(ROOT / "backends" / "shared"))

from app.api import health as health_api  # noqa: E402
from app.core.config import settings  # noqa: E402
from app.core import deps as deps_api  # noqa: E402
from app.main import app  # noqa: E402
from app.persistence.db import AdminDB  # noqa: E402
from shared_lib.persistence.admin_analytics import ensure_admin_analytics_snapshot_tables  # noqa: E402


def _create_admin_db(tmp_path: Path) -> AdminDB:
    db_path = tmp_path / "admin_backend_shell.db"
    today = datetime.now(timezone.utc).date().isoformat()
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            full_name TEXT,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        INSERT INTO admins (id, email, full_name, role, is_superuser, is_active)
        VALUES ('admin-test', 'admin@example.com', 'Admin Test', 'admin', 1, 1)
        """
    )
    conn.execute(
        """
        CREATE TABLE users (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            status TEXT,
            role TEXT,
            created_at TEXT,
            last_login_at TEXT,
            total_trades INTEGER DEFAULT 0,
            total_commission REAL DEFAULT 0,
            is_verified INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        INSERT INTO users
            (id, email, status, role, created_at, last_login_at, total_trades, total_commission, is_verified)
        VALUES
            ('user-1', 'one@example.com', 'active', 'user', '2026-05-01T00:00:00+00:00', NULL, 12, 3.5, 1),
            ('user-2', 'two@example.com', 'suspended', 'user', '2026-05-02T00:00:00+00:00', NULL, 0, 0, 0)
        """
    )
    conn.execute("CREATE TABLE subscriptions (user_id TEXT PRIMARY KEY, status TEXT, plan_id TEXT)")
    conn.execute("INSERT INTO subscriptions (user_id, status, plan_id) VALUES ('user-1', 'active', 'pro')")
    conn.execute("CREATE TABLE invoices (id TEXT PRIMARY KEY, user_id TEXT, amount REAL, status TEXT)")
    conn.execute("INSERT INTO invoices (id, user_id, amount, status) VALUES ('inv-1', 'user-1', 99.0, 'paid')")
    conn.execute("CREATE TABLE commission_ledger (id TEXT PRIMARY KEY, commission_amount REAL)")
    conn.execute("INSERT INTO commission_ledger (id, commission_amount) VALUES ('comm-1', 12.5)")
    conn.execute(
        """
        CREATE TABLE revenue_snapshots (
            date TEXT PRIMARY KEY,
            subscription_revenue REAL,
            commission_revenue REAL,
            total_revenue REAL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO revenue_snapshots (date, subscription_revenue, commission_revenue, total_revenue)
        VALUES ('2026-05-01', 100, 25, 125)
        """
    )
    conn.execute(
        """
        CREATE TABLE trade_fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            account_id TEXT,
            initiator_type TEXT,
            timestamp_utc TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO trade_fills (symbol, account_id, initiator_type, timestamp_utc) VALUES (?, ?, ?, ?)",
        [
            ("BTCUSDT", None, None, "2026-05-01T00:00:00+00:00"),
            ("BTCUSDT", None, None, "2026-05-01T00:01:00+00:00"),
            ("ETHUSDT", None, None, "2026-05-01T00:02:00+00:00"),
            ("DOGEUSDT", "backfill", None, "2026-05-01T00:03:00+00:00"),
            ("SOLUSDT", None, "SHADOW", "2026-05-01T00:04:00+00:00"),
        ],
    )
    conn.execute(
        """
        CREATE TABLE admin_profitability_daily_summary (
            date TEXT NOT NULL,
            account_scope TEXT NOT NULL,
            fills_count INTEGER NOT NULL DEFAULT 0,
            closed_trades INTEGER NOT NULL DEFAULT 0,
            winning_trades INTEGER NOT NULL DEFAULT 0,
            losing_trades INTEGER NOT NULL DEFAULT 0,
            total_realized_pnl REAL NOT NULL DEFAULT 0,
            avg_pnl REAL,
            win_rate REAL,
            profit_factor REAL,
            avg_r_multiple REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date, account_scope)
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO admin_profitability_daily_summary (
            date, account_scope, fills_count, closed_trades, winning_trades, losing_trades,
            total_realized_pnl, avg_pnl, win_rate, profit_factor, avg_r_multiple,
            created_at, updated_at
        ) VALUES (?, 'live', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (today, 3, 2, 1, 1, 42.5, 21.25, 50.0, 2.1, 0.65, today, today),
            ((datetime.now(timezone.utc).date() - timedelta(days=5)).isoformat(), 1, 1, 1, 0, 15.0, 15.0, 100.0, None, 0.4, today, today),
        ],
    )
    conn.execute(
        """
        CREATE TABLE admin_profitability_symbol_summary (
            symbol TEXT NOT NULL,
            account_scope TEXT NOT NULL,
            fills_count INTEGER NOT NULL DEFAULT 0,
            closed_trades INTEGER NOT NULL DEFAULT 0,
            total_realized_pnl REAL NOT NULL DEFAULT 0,
            avg_pnl REAL,
            win_rate REAL,
            avg_r_multiple REAL,
            sl_count INTEGER NOT NULL DEFAULT 0,
            tp_count INTEGER NOT NULL DEFAULT 0,
            time_exit_count INTEGER NOT NULL DEFAULT 0,
            other_exit_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol, account_scope)
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO admin_profitability_symbol_summary (
            symbol, account_scope, fills_count, closed_trades, total_realized_pnl,
            avg_pnl, win_rate, avg_r_multiple, sl_count, tp_count, time_exit_count,
            other_exit_count, created_at, updated_at
        ) VALUES (?, 'live', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("BTCUSDT", 3, 2, 42.5, 21.25, 50.0, 0.65, 1, 1, 0, 0, today, today),
            ("ETHUSDT", 1, 1, 15.0, 15.0, 100.0, 0.4, 0, 1, 0, 0, today, today),
        ],
    )
    conn.execute(
        """
        CREATE TABLE admin_profitability_sizing_events (
            id TEXT PRIMARY KEY,
            trace_id TEXT,
            symbol TEXT,
            ts TEXT,
            run_id TEXT,
            cycle_id TEXT,
            sizing_method TEXT,
            configured_margin REAL,
            final_margin REAL,
            base_notional REAL,
            final_notional REAL,
            leverage REAL,
            cap_applied INTEGER NOT NULL DEFAULT 0,
            risk_cap_pct REAL,
            atr_stop_distance_pct REAL,
            explanation TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO admin_profitability_sizing_events (
            id, trace_id, symbol, ts, run_id, cycle_id, sizing_method, configured_margin,
            final_margin, base_notional, final_notional, leverage, cap_applied,
            risk_cap_pct, atr_stop_distance_pct, explanation, created_at
        ) VALUES (
            'size-1', 'trace-size-1', 'BTCUSDT', ?, 'run-1', 'cycle-1',
            'fixed_margin', 100, 80, 1000, 800, 10, 1, 1.5, 0.8,
            'ATR cap applied', ?
        )
        """,
        (today, today),
    )
    conn.execute(
        """
        CREATE TABLE tradingview_webhooks (
            id TEXT PRIMARY KEY,
            bot_id TEXT NOT NULL,
            name TEXT NOT NULL,
            token_hash TEXT NOT NULL,
            secret_hash TEXT,
            mode TEXT NOT NULL DEFAULT 'ADVISORY_ONLY',
            is_enabled INTEGER NOT NULL DEFAULT 1,
            allowed_symbols_json TEXT,
            allowed_actions_json TEXT NOT NULL DEFAULT '["BUY","SELL"]',
            max_alert_age_seconds INTEGER NOT NULL DEFAULT 300,
            rate_limit_per_minute INTEGER NOT NULL DEFAULT 30,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_used_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO tradingview_webhooks (
            id, bot_id, name, token_hash, mode, is_enabled, allowed_symbols_json,
            allowed_actions_json, max_alert_age_seconds, rate_limit_per_minute,
            created_at, updated_at, last_used_at
        )
        VALUES (
            'tvwh_test', 'bot-1', 'Test Webhook', 'redacted',
            'ADVISORY_ONLY', 1, '["BTCUSDT"]', '["BUY","SELL"]',
            300, 30, '2026-05-01T00:00:00+00:00',
            '2026-05-01T00:01:00+00:00', NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE tradingview_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            webhook_id TEXT,
            bot_id TEXT,
            alert_id TEXT,
            symbol_raw TEXT,
            symbol_normalized TEXT,
            action TEXT,
            side TEXT,
            timeframe TEXT,
            strategy_name TEXT,
            price REAL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            received_at TEXT NOT NULL,
            alert_timestamp TEXT,
            status TEXT NOT NULL,
            reject_reason TEXT,
            idempotency_key TEXT,
            source_ip TEXT,
            signature_valid INTEGER,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO tradingview_alerts (
            webhook_id, bot_id, alert_id, symbol_raw, symbol_normalized,
            action, side, timeframe, strategy_name, price, payload_json,
            received_at, alert_timestamp, status, reject_reason, idempotency_key,
            source_ip, signature_valid, created_at
        )
        VALUES (
            'tvwh_test', 'bot-1', 'alert-1', 'BINANCE:BTCUSDT', 'BTCUSDT',
            'BUY', 'LONG', '15m', 'Breakout', 65000.5, '{"symbol":"BTCUSDT"}',
            '2026-05-01T00:02:00+00:00', '2026-05-01T00:01:30+00:00',
            'ACCEPTED_ADVISORY', NULL, 'idem-1', '127.0.0.1', 1,
            '2026-05-01T00:02:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE tradingview_signal_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER NOT NULL,
            bot_id TEXT,
            symbol TEXT,
            action TEXT,
            mode TEXT NOT NULL,
            normalized_signal_json TEXT NOT NULL DEFAULT '{}',
            event_filter_result TEXT,
            policy_result TEXT,
            sizing_result TEXT,
            execution_result TEXT,
            decision_trace_id TEXT,
            final_status TEXT NOT NULL,
            final_reason TEXT NOT NULL,
            queue_id TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO tradingview_signal_decisions (
            alert_id, bot_id, symbol, action, mode, normalized_signal_json,
            event_filter_result, policy_result, sizing_result, execution_result,
            decision_trace_id, final_status, final_reason, queue_id, created_at
        )
        VALUES (
            1, 'bot-1', 'BTCUSDT', 'BUY', 'ADVISORY_ONLY',
            '{"symbol":"BTCUSDT","action":"BUY"}', NULL, NULL, NULL, NULL,
            NULL, 'ACCEPTED_ADVISORY_ONLY',
            'Alert accepted; advisory-only mode; no queue row created.',
            NULL, '2026-05-01T00:03:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE external_signal_queue (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            source_alert_id TEXT NOT NULL,
            bot_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT,
            action TEXT NOT NULL,
            confidence REAL,
            status TEXT NOT NULL,
            available_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            claimed_at TEXT,
            processed_at TEXT,
            result TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO external_signal_queue (
            id, source, source_alert_id, bot_id, symbol, side, action,
            confidence, status, available_at, expires_at, claimed_at,
            processed_at, result, created_at
        )
        VALUES (
            'extsig_test', 'TRADINGVIEW', 'alert-1', 'bot-1', 'BTCUSDT',
            'LONG', 'BUY', 0.75, 'PENDING',
            '2026-05-01T00:04:00+00:00',
            '2026-05-01T01:04:00+00:00',
            NULL, NULL, '{"queued":true}', '2026-05-01T00:04:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE tradingview_processor_heartbeat (
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
        """
    )
    conn.execute(
        """
        INSERT INTO tradingview_processor_heartbeat (
            bot_instance_id, processor_enabled, env_gate_reason,
            last_started_at, last_finished_at, last_processed_count,
            last_rejected_count, last_failed_count, last_skipped_count,
            last_skipped_reason, last_result_json, last_error, updated_at
        )
        VALUES (
            'bot-1', 0, 'DISABLED_BY_ENV', NULL, NULL, 0, 0, 0, 0,
            NULL, NULL, NULL, '2026-05-01T00:05:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            stopped_at TEXT,
            mode TEXT,
            status TEXT,
            config_json TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO runs (run_id, started_at, stopped_at, mode, status, config_json)
        VALUES ('run-1', '2026-05-01T00:00:00+00:00', NULL, 'paper', 'running', '{"symbols":["BTCUSDT"]}')
        """
    )
    conn.execute(
        """
        CREATE TABLE run_summary (
            run_id TEXT PRIMARY KEY,
            cycles INTEGER,
            trades INTEGER,
            realized_pnl REAL,
            win_trades INTEGER,
            loss_trades INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO run_summary (run_id, cycles, trades, realized_pnl, win_trades, loss_trades)
        VALUES ('run-1', 12, 3, 42.5, 2, 1)
        """
    )
    conn.execute("CREATE TABLE daily_state (day TEXT PRIMARY KEY, realized_pnl REAL, trade_count INTEGER)")
    conn.execute(
        "INSERT INTO daily_state (day, realized_pnl, trade_count) VALUES (?, 42.5, 3)",
        (today,),
    )
    conn.execute(
        """
        CREATE TABLE symbol_state (
            symbol TEXT PRIMARY KEY,
            position TEXT,
            entry_price REAL,
            entry_qty REAL,
            last_signal TEXT,
            last_action TEXT,
            updated_at TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO symbol_state (
            symbol, position, entry_price, entry_qty, last_signal, last_action, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("BTCUSDT", "LONG", 65000.0, 0.01, "BUY", "OPEN", "2026-05-01T00:10:00+00:00"),
            ("ETHUSDT", "NONE", None, None, "HOLD", "SKIP", "2026-05-01T00:09:00+00:00"),
        ],
    )
    conn.execute(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            event_type TEXT,
            symbol TEXT,
            action TEXT,
            timestamp_utc TEXT,
            details_json TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO events (run_id, event_type, symbol, action, timestamp_utc, details_json)
        VALUES ('run-1', 'cycle_completed', 'BTCUSDT', 'HOLD', datetime('now'), '{"ok":true}')
        """
    )
    conn.execute(
        """
        CREATE TABLE decision_traces (
            trace_id TEXT PRIMARY KEY,
            run_id TEXT,
            symbol TEXT,
            signal TEXT,
            confidence REAL,
            intended_action TEXT,
            execution_status TEXT,
            final_position TEXT,
            ts TEXT,
            sizing_json TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO decision_traces (
            trace_id, run_id, symbol, signal, confidence, intended_action,
            execution_status, final_position, ts, sizing_json
        )
        VALUES (
            'trace-1', 'run-1', 'BTCUSDT', 'BUY', 0.82, 'OPEN',
            'SKIPPED_PAPER', 'LONG', '2026-05-01T00:11:00+00:00',
            '{"cap_applied":true,"base_margin_usdt":100,"final_margin_usdt":50,"risk_level":"standard","account_risk_pct":1,"stop_distance_pct":2}'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE signal_candidates (
            id TEXT PRIMARY KEY,
            asset_class TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            timeframe TEXT,
            strategy_name TEXT,
            entry_price REAL,
            entry_zone_low REAL,
            entry_zone_high REAL,
            stop_loss REAL,
            take_profit_1 REAL,
            take_profit_2 REAL,
            take_profit_3 REAL,
            risk_reward REAL,
            confidence_score REAL,
            signal_reason TEXT,
            rejection_reason TEXT,
            source TEXT,
            status TEXT NOT NULL,
            dev_mode INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO signal_candidates (
            id, asset_class, symbol, side, timeframe, strategy_name, entry_price,
            entry_zone_low, entry_zone_high, stop_loss, take_profit_1,
            take_profit_2, take_profit_3, risk_reward, confidence_score,
            signal_reason, rejection_reason, source, status, dev_mode,
            created_at, updated_at
        )
        VALUES (
            'sigcand-1', 'crypto', 'BTCUSDT', 'BUY', '1h', 'Breakout',
            65000, 64900, 65100, 64000, 67000, 68000, NULL, 2.0, 82,
            'Momentum breakout', NULL, 'internal_signal_engine', 'CANDIDATE',
            0, '2026-05-01T00:20:00+00:00', '2026-05-01T00:20:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE trading_signals (
            id TEXT PRIMARY KEY,
            candidate_id TEXT,
            asset_class TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            timeframe TEXT,
            strategy_name TEXT,
            entry_price REAL NOT NULL,
            entry_zone_low REAL,
            entry_zone_high REAL,
            stop_loss REAL NOT NULL,
            take_profit_1 REAL NOT NULL,
            take_profit_2 REAL,
            take_profit_3 REAL,
            risk_reward REAL NOT NULL,
            confidence_score REAL NOT NULL,
            signal_reason TEXT,
            status TEXT NOT NULL,
            is_published INTEGER DEFAULT 0,
            source TEXT,
            dev_mode INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            published_at TEXT,
            expires_at TEXT NOT NULL,
            invalidated_at TEXT,
            tp1_hit_at TEXT,
            tp2_hit_at TEXT,
            tp3_hit_at TEXT,
            sl_hit_at TEXT,
            cancelled_at TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO trading_signals (
            id, candidate_id, asset_class, symbol, side, timeframe,
            strategy_name, entry_price, entry_zone_low, entry_zone_high,
            stop_loss, take_profit_1, take_profit_2, take_profit_3,
            risk_reward, confidence_score, signal_reason, status,
            is_published, source, dev_mode, created_at, published_at,
            expires_at, invalidated_at, tp1_hit_at, tp2_hit_at, tp3_hit_at,
            sl_hit_at, cancelled_at, updated_at
        )
        VALUES (
            'sig-1', 'sigcand-1', 'crypto', 'BTCUSDT', 'BUY', '1h',
            'Breakout', 65000, 64900, 65100, 64000, 67000, 68000,
            NULL, 2.0, 82, 'Momentum breakout', 'PENDING_ENTRY',
            1, 'internal_signal_engine', 0, '2026-05-01T00:21:00+00:00',
            '2026-05-01T00:22:00+00:00', '2026-05-02T00:21:00+00:00',
            NULL, NULL, NULL, NULL, NULL, NULL, '2026-05-01T00:22:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE signal_pair_universe (
            symbol TEXT PRIMARY KEY,
            exchange TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            quote_asset TEXT,
            contract_type TEXT,
            tier TEXT,
            enabled INTEGER DEFAULT 1,
            whitelisted INTEGER DEFAULT 0,
            blacklisted INTEGER DEFAULT 0,
            blacklist_reason TEXT,
            discovered_at TEXT,
            last_seen_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO signal_pair_universe (
            symbol, exchange, asset_class, quote_asset, contract_type, tier,
            enabled, whitelisted, blacklisted, blacklist_reason, discovered_at,
            last_seen_at, created_at, updated_at
        )
        VALUES (
            'BTCUSDT', 'binance_futures', 'crypto', 'USDT', 'PERPETUAL',
            'TIER_1', 1, 1, 0, NULL, '2026-05-01T00:00:00+00:00',
            '2026-05-01T00:30:00+00:00', '2026-05-01T00:00:00+00:00',
            '2026-05-01T00:30:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE signal_pair_metrics (
            symbol TEXT PRIMARY KEY,
            exchange TEXT NOT NULL,
            quote_volume_24h REAL,
            spread_percent REAL,
            bid_price REAL,
            ask_price REAL,
            candle_count INTEGER,
            atr_percent REAL,
            volatility_score REAL,
            liquidity_score REAL,
            spread_score REAL,
            reliability_score REAL,
            is_safe INTEGER DEFAULT 0,
            unsafe_reason TEXT,
            last_updated TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO signal_pair_metrics (
            symbol, exchange, quote_volume_24h, spread_percent, bid_price,
            ask_price, candle_count, atr_percent, volatility_score,
            liquidity_score, spread_score, reliability_score, is_safe,
            unsafe_reason, last_updated
        )
        VALUES (
            'BTCUSDT', 'binance_futures', 100000000, 0.04, 65000, 65001,
            500, 1.2, 88, 95, 99, 94, 1, NULL,
            '2026-05-01T00:31:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE signal_scan_runs (
            id TEXT PRIMARY KEY,
            scan_type TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_seconds REAL,
            symbols_discovered INTEGER DEFAULT 0,
            symbols_eligible INTEGER DEFAULT 0,
            symbols_scanned INTEGER DEFAULT 0,
            candidates_created INTEGER DEFAULT 0,
            signals_published INTEGER DEFAULT 0,
            errors TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO signal_scan_runs (
            id, scan_type, started_at, ended_at, duration_seconds,
            symbols_discovered, symbols_eligible, symbols_scanned,
            candidates_created, signals_published, errors, status,
            created_at, updated_at
        )
        VALUES (
            'scan-1', 'DISCOVERY', '2026-05-01T00:40:00+00:00',
            '2026-05-01T00:41:00+00:00', 60, 10, 5, 5, 1, 1,
            NULL, 'COMPLETED', '2026-05-01T00:40:00+00:00',
            '2026-05-01T00:41:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE signal_scan_results (
            id TEXT PRIMARY KEY,
            scan_run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            was_scanned INTEGER DEFAULT 0,
            was_skipped INTEGER DEFAULT 0,
            skip_reason TEXT,
            candidate_count INTEGER DEFAULT 0,
            accepted_count INTEGER DEFAULT 0,
            rejected_count INTEGER DEFAULT 0,
            published_count INTEGER DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO signal_scan_results (
            id, scan_run_id, symbol, was_scanned, was_skipped, skip_reason,
            candidate_count, accepted_count, rejected_count, published_count,
            error, created_at
        )
        VALUES (
            'scanres-1', 'scan-1', 'BTCUSDT', 1, 0, NULL, 1, 1, 0, 1,
            NULL, '2026-05-01T00:41:00+00:00'
        )
        """
    )
    # ── Events / blackouts / reactions / snapshots ──────────────────────────
    conn.execute(
        """
        CREATE TABLE economic_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            event_type TEXT,
            country_currency TEXT,
            impact_level TEXT,
            scheduled_utc TEXT,
            actual_val REAL,
            forecast_val REAL,
            previous_val REAL,
            source TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO economic_events (
            event_id, title, event_type, country_currency, impact_level,
            scheduled_utc, forecast_val, previous_val, source, created_at, updated_at
        )
        VALUES (
            'ev-1', 'US CPI', 'INFLATION', 'USD', 'HIGH',
            '2026-05-25T12:30:00+00:00', 0.3, 0.2, 'forex_factory',
            '2026-05-01T00:00:00+00:00', '2026-05-01T00:00:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE event_blackout_windows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            affected_symbols TEXT,
            is_global INTEGER DEFAULT 0,
            reason TEXT,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE market_event_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            exchange TEXT,
            pre_window_start_utc TEXT,
            event_time_utc TEXT,
            post_window_end_utc TEXT,
            price_before_event REAL,
            price_at_event REAL,
            price_after_5m REAL,
            price_after_15m REAL,
            price_after_30m REAL,
            price_after_60m REAL,
            max_move_pct REAL,
            min_move_pct REAL,
            net_move_pct REAL,
            direction_after_event TEXT,
            continuation_or_reversal TEXT,
            atr_before REAL,
            atr_after REAL,
            volatility_expansion_ratio REAL,
            candle_range_before REAL,
            candle_range_during REAL,
            realized_vol_before REAL,
            realized_vol_after REAL,
            average_volume_before REAL,
            event_volume REAL,
            volume_spike_ratio REAL,
            abnormal_volume_score REAL,
            spread_before REAL,
            spread_during REAL,
            spread_after REAL,
            spread_widening_ratio REAL,
            reaction_type TEXT NOT NULL DEFAULT 'NO_REACTION',
            confidence_score REAL,
            data_quality TEXT NOT NULL DEFAULT 'UNKNOWN',
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO market_event_reactions (
            event_id, symbol, exchange, event_time_utc,
            reaction_type, data_quality, created_at, updated_at
        )
        VALUES (
            'ev-1', 'BTCUSDT', 'binance_futures', '2026-05-24T12:30:00+00:00',
            'TREND_CONTINUATION', 'COMPLETE',
            '2026-05-24T12:30:00+00:00', '2026-05-24T12:30:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE event_market_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            exchange TEXT,
            timestamp_utc TEXT,
            window_label TEXT,
            price REAL,
            volume REAL,
            candle_open REAL,
            candle_high REAL,
            candle_low REAL,
            candle_close REAL,
            atr REAL,
            spread REAL,
            source TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO event_market_snapshots (
            event_id, symbol, exchange, timestamp_utc, window_label,
            price, source, created_at
        )
        VALUES (
            'ev-1', 'BTCUSDT', 'binance_futures',
            '2026-05-24T12:25:00+00:00', 'PRE_5M',
            65000, 'internal', '2026-05-24T12:25:00+00:00'
        )
        """
    )
    # ── News intelligence tables ─────────────────────────────────────────────
    conn.execute(
        """
        CREATE TABLE raw_news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            source_name TEXT,
            source_domain TEXT,
            source_url TEXT,
            external_id TEXT,
            author TEXT,
            title TEXT,
            body_snippet TEXT,
            published_utc TEXT,
            ingested_utc TEXT,
            latency_seconds REAL,
            language TEXT,
            is_duplicate INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO raw_news_items (
            provider, source_name, title, published_utc, ingested_utc,
            latency_seconds, is_duplicate, created_at
        )
        VALUES (
            'rss', 'CoinDesk', 'BTC hits 65k',
            '2026-05-24T10:00:00+00:00', '2026-05-24T10:00:05+00:00',
            5.0, 0, '2026-05-24T10:00:05+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE news_clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_title TEXT,
            summary TEXT,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            source_count INTEGER DEFAULT 1,
            provider_count INTEGER DEFAULT 1,
            highest_reliability_score REAL DEFAULT 0.5,
            cluster_confidence REAL DEFAULT 0.5,
            spam_score REAL DEFAULT 0.0,
            latency_score REAL DEFAULT 0.0,
            is_valid_signal INTEGER DEFAULT 0,
            manipulation_flag TEXT,
            data_quality_status TEXT DEFAULT 'MEDIUM_CONFIDENCE',
            is_manipulation_suspect INTEGER DEFAULT 0,
            manipulation_reason TEXT,
            confirmation_count INTEGER DEFAULT 0,
            conflict_flag INTEGER DEFAULT 0,
            fake_news_risk_score REAL,
            market_confirmation_status TEXT,
            first_seen_provider TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO news_clusters (
            canonical_title, summary, first_seen_utc, last_seen_utc,
            source_count, provider_count, spam_score, is_valid_signal,
            data_quality_status, created_at, updated_at
        )
        VALUES (
            'BTC hits 65k', 'Bitcoin reached 65000',
            '2026-05-24T10:00:00+00:00', '2026-05-24T10:00:05+00:00',
            1, 1, 0.1, 1, 'HIGH_CONFIDENCE',
            '2026-05-24T10:00:00+00:00', '2026-05-24T10:00:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE news_cluster_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id INTEGER NOT NULL,
            raw_news_item_id INTEGER NOT NULL,
            similarity_score REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO news_cluster_items (cluster_id, raw_news_item_id, similarity_score) VALUES (1, 1, 0.95)"
    )
    conn.execute(
        """
        CREATE TABLE news_narratives (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id INTEGER NOT NULL,
            narrative_type TEXT,
            narrative_confidence REAL,
            severity_level TEXT,
            matched_keywords TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO news_narratives (cluster_id, narrative_type, narrative_confidence, severity_level, created_at)
        VALUES (1, 'BULLISH', 0.8, 'MEDIUM', '2026-05-24T10:00:00+00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE news_intelligence_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id INTEGER,
            symbol TEXT,
            signal_type TEXT,
            sentiment_score REAL,
            narrative_type TEXT,
            severity_level TEXT,
            reliability_score REAL,
            confidence_score REAL,
            spam_score REAL DEFAULT 0.0,
            latency_score REAL DEFAULT 0.0,
            is_valid_signal INTEGER DEFAULT 0,
            manipulation_flag TEXT,
            data_quality_status TEXT,
            should_affect_trading INTEGER DEFAULT 0,
            shadow_only INTEGER DEFAULT 1,
            suppression_reason TEXT,
            validation_status TEXT DEFAULT 'PENDING_MARKET_VALIDATION',
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO news_intelligence_signals (
            cluster_id, symbol, signal_type, confidence_score, spam_score,
            is_valid_signal, data_quality_status,
            should_affect_trading, shadow_only, created_at
        )
        VALUES (
            1, 'BTCUSDT', 'BULLISH', 0.8, 0.1, 1, 'HIGH_CONFIDENCE',
            0, 1, '2026-05-24T10:01:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE news_market_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id INTEGER,
            symbol TEXT,
            sentiment_accuracy TEXT,
            is_false_signal INTEGER DEFAULT 0,
            false_signal_reason TEXT,
            impact_score REAL,
            signal_effectiveness_score REAL,
            reaction_latency_category TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO news_market_reactions (
            cluster_id, symbol, sentiment_accuracy, is_false_signal,
            impact_score, signal_effectiveness_score, reaction_latency_category, created_at
        )
        VALUES (1, 'BTCUSDT', 'CORRECT', 0, 0.7, 0.8, 'FAST', '2026-05-24T11:00:00+00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE news_sources (
            id TEXT PRIMARY KEY,
            source_name TEXT,
            source_type TEXT,
            category TEXT,
            is_enabled INTEGER DEFAULT 1,
            rss_url TEXT,
            fetch_interval_seconds INTEGER,
            last_fetch_utc TEXT,
            last_success_utc TEXT,
            last_error TEXT,
            base_reliability_score REAL DEFAULT 0.5,
            dynamic_reliability_score REAL DEFAULT 0.5,
            is_trusted INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        INSERT INTO news_sources (
            id, source_name, source_type, is_enabled, rss_url,
            base_reliability_score, dynamic_reliability_score
        )
        VALUES ('coindesk', 'CoinDesk', 'RSS', 1, 'https://coindesk.com/rss', 0.9, 0.88)
        """
    )
    conn.execute(
        """
        CREATE TABLE news_provider_health (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT,
            status TEXT,
            items_fetched_last_run INTEGER,
            duplicate_count_last_run INTEGER,
            last_checked_utc TEXT,
            error_message TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO news_provider_health (source_id, status, items_fetched_last_run, last_checked_utc)
        VALUES ('coindesk', 'HEALTHY', 5, '2026-05-24T10:00:00+00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE real_time_news_provider_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            is_enabled INTEGER DEFAULT 0,
            last_fetch_utc TEXT,
            last_success_utc TEXT,
            last_error TEXT,
            latency_avg_seconds REAL DEFAULT 0.0,
            items_fetched_today INTEGER DEFAULT 0,
            duplicate_rate REAL DEFAULT 0.0,
            health_status TEXT DEFAULT 'DISABLED',
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO real_time_news_provider_status (
            provider, is_enabled, latency_avg_seconds,
            items_fetched_today, health_status, updated_at
        )
        VALUES ('cryptopanic', 0, 0.0, 0, 'DISABLED', '2026-05-24T10:00:00+00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE narrative_effectiveness_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            narrative_type TEXT,
            avg_impact_score REAL,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO narrative_effectiveness_scores (narrative_type, avg_impact_score, created_at)
        VALUES ('BULLISH', 0.72, '2026-05-24T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE event_news_runtime_mode (
            id INTEGER PRIMARY KEY,
            current_mode TEXT,
            previous_mode TEXT,
            max_allowed_action TEXT,
            readiness_score REAL,
            safety_status TEXT,
            failed_criteria_json TEXT,
            passed_criteria_json TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO event_news_runtime_mode (
            id, current_mode, previous_mode, max_allowed_action,
            readiness_score, safety_status, failed_criteria_json, passed_criteria_json
        )
        VALUES (1, 'SHADOW', NULL, 'ANNOTATE_ONLY', 0.0, 'UNKNOWN', '[]', '[]')
        """
    )
    conn.execute(
        """
        CREATE TABLE event_news_mode_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_mode TEXT,
            to_mode TEXT,
            reason TEXT,
            evidence_json TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE event_news_influence_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            applied_action TEXT,
            size_multiplier REAL,
            confidence_penalty REAL,
            delay_seconds REAL,
            source_context_json TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO event_news_influence_decisions (
            symbol, applied_action, size_multiplier, confidence_penalty,
            delay_seconds, source_context_json, created_at
        )
        VALUES ('BTCUSDT', 'ANNOTATE_ONLY', 1.0, 0.0, 0.0, '{}', '2026-05-24T10:00:00+00:00')
        """
    )
    _seed_ml_snapshots(conn, today)
    conn.commit()
    conn.close()
    return AdminDB(path=str(db_path))


def _seed_ml_snapshots(conn: sqlite3.Connection, today: str) -> None:
    ensure_admin_analytics_snapshot_tables(conn)
    generated_at = f"{today}T00:00:00+00:00"
    overview = {
        "ml_enabled": True,
        "ml_mode": "shadow",
        "current_model_version": "entry_quality_v1.1_test",
        "current_threshold": 0.4,
        "current_hard_block_floor": 0.15,
        "model_artifact_path": "models/artifacts/test.pkl",
        "encoder_path": "models/artifacts/test_encoders.pkl",
        "metadata_path": "models/artifacts/test_meta.json",
        "last_model_load_time": generated_at,
        "last_bot_restart_time": generated_at,
        "current_ml_status": "collecting_data",
    }
    training_gate = {
        "total_linked_completed_trades": 178,
        "required_trades": 200,
        "wins": 42,
        "required_wins": 50,
        "losses": 80,
        "breakeven_trades": 56,
        "excluded_open_positions": 3,
        "trades_with_full_feature_coverage": 152,
        "trades_missing_critical_features": 26,
        "current_win_rate": 34.4262,
        "feature_coverage_pct": 85.3933,
        "linkage_healthy": False,
        "label_distribution_single_class": False,
        "training_ready": False,
        "status": "blocked",
        "linkage_warnings": {
            "unlinked_completed_trades": 53,
            "unlinked_reason_counts": {
                "missing_run_cycle_metadata|no_matching_decision_trace_position_id|old_tracing_gap": 53
            },
        },
    }
    feature_completeness = {
        "recent_window_size": 178,
        "recent_window_basis": "last_500_linked_completed_trades",
        "recent_completeness_pct": 93.49,
        "lifetime_completeness_pct": 93.49,
        "features": [
            {
                "feature_name": "ml_score",
                "null_count_recent": 0,
                "null_pct_recent": 0.0,
                "null_count_lifetime": 0,
                "null_pct_lifetime": 0.0,
                "last_seen_populated_at": generated_at,
                "status": "healthy",
            }
        ],
        "broken_feature_count": 0,
        "partially_missing_feature_count": 1,
    }
    linkage = {
        "post_fix_start": generated_at,
        "total_post_fix_fills": 500,
        "fills_with_non_null_run_id": 400,
        "fills_with_non_null_cycle_id": 400,
        "fills_with_non_null_position_id": 490,
        "fully_linked_completed_trades": 178,
        "fully_linked_completed_trades_pct": 77.0563,
        "orphan_open_fills": 3,
        "unmatched_close_fills": 0,
        "linkage_healthy": False,
        "unlinked_completed_trades": 53,
        "unlinked_reason_counts": {
            "missing_run_cycle_metadata|no_matching_decision_trace_position_id|old_tracing_gap": 53
        },
    }
    activity = {
        "window_days": 30,
        "page": 1,
        "page_size": 50,
        "total_recent_rows": 2,
        "total_ml_scored_entries": 2,
        "allow_count": 1,
        "shadow_count": 1,
        "block_count": 0,
        "skip_count": 0,
        "average_ml_score": 0.55,
        "current_threshold": 0.4,
        "current_hard_floor": 0.15,
        "score_distribution": [{"bucket": "0.5-0.6", "count": 2}],
        "per_symbol_actions": [{"key": "BTCUSDT", "allow_count": 1, "shadow_count": 1, "block_count": 0, "skip_count": 0}],
        "per_regime_actions": [],
        "per_session_actions": [],
        "recent_activity_rows": [
            {
                "timestamp": generated_at,
                "symbol": "BTCUSDT",
                "side": "LONG",
                "ml_score": 0.61,
                "ml_action": "ALLOW",
                "ml_model_version": "entry_quality_v1.1_test",
                "threshold": 0.4,
                "regime": "WEAK_TREND",
                "session": "london",
                "linkage_status": "fully_linked_completed",
            }
        ],
    }
    shadow = {
        "window_days": 90,
        "total_linked_completed_trades_with_ml_attribution": 18,
        "decision_groups": {
            "ALLOW": {"count": 16, "wins": 4, "losses": 12, "breakevens": 0, "total_pnl": -12.0, "average_pnl": -0.75},
            "SHADOW": {"count": 2, "wins": 1, "losses": 1, "breakevens": 0, "total_pnl": 1.0, "average_pnl": 0.5},
            "BLOCK": {"count": 0, "wins": 0, "losses": 0, "breakevens": 0, "total_pnl": 0.0, "average_pnl": 0.0},
        },
        "good_allows": 4,
        "bad_allows": 12,
        "good_blocks": 1,
        "bad_blocks": 1,
        "classification_logic": "snapshot test",
    }
    validation = {
        "items": [
            {
                "model_version": "entry_quality_v1.1_test",
                "training_date": generated_at,
                "dataset_used": "dataset.parquet",
                "train_rows": 100,
                "test_rows": 25,
                "train_auc": 0.7,
                "test_auc": 0.66,
                "validation_method": "walk_forward",
                "notes": "ok",
                "verdict": "accepted",
                "deployed_mode": "shadow",
            }
        ],
        "source_note": "snapshot test",
    }
    dataset_status = {
        "dataset_source_date_range": {"start": generated_at, "end": generated_at},
        "linked_trade_count": 178,
        "fully_usable_rows": 152,
        "dropped_rows": 26,
        "dropped_row_reasons": [{"reason": "feature_null", "count": 26}],
        "feature_completeness_status": "partially_missing",
        "label_distribution": {"wins": 42, "losses": 80, "breakevens": 56, "single_class": False},
        "last_dataset_build_time": generated_at,
        "last_dataset_path": "dataset.parquet",
        "rebuild_dataset_allowed": True,
        "source_note": "snapshot test",
    }
    alerts = {
        "generated_at": generated_at,
        "items": [
            {
                "code": "unlinked_completed_trades",
                "level": "warning",
                "title": "Completed trades are missing trace linkage",
                "body": "53 completed trades are unlinked.",
            }
        ],
    }
    drift = {
        "window_days": 30,
        "live_win_rate": 31.0,
        "historical_win_rate": 34.0,
        "win_rate_delta": -3.0,
        "live_score_distribution": [{"bucket": "0.5-0.6", "count": 2}],
        "training_score_distribution": [{"bucket": "0.5-0.6", "count": 4}],
        "symbol_distribution": [{"key": "BTCUSDT", "count": 2, "pct": 100.0, "average_pnl": -1.0}],
        "regime_distribution": [],
        "session_distribution": [],
        "average_pnl_by_regime": [],
        "average_pnl_by_symbol": [{"key": "BTCUSDT", "average_pnl": -1.0, "count": 2}],
        "average_pnl_by_score_band": [{"bucket": "0.5-0.6", "count": 2, "average_pnl": -1.0}],
        "source_note": "snapshot test",
    }
    dashboard_summary = {
        "ml_mode": overview["ml_mode"],
        "current_model_version": overview["current_model_version"],
        "total_linked_completed_trades": training_gate["total_linked_completed_trades"],
        "wins": training_gate["wins"],
        "feature_coverage_pct": training_gate["feature_coverage_pct"],
        "linkage_healthy": training_gate["linkage_healthy"],
        "training_ready": training_gate["training_ready"],
        "status": training_gate["status"],
    }
    control = {
        "readiness_status": "blocked",
        "training_allowed_right_now": False,
        "current_dataset_path": "dataset.parquet",
        "target_output_model_version": "entry_quality_v1.1_test",
        "last_training_run_status": None,
        "last_training_run_logs": [],
        "last_dataset_rebuild_status": None,
        "last_validation_run_status": None,
        "actions": [
            {
                "action_key": "run_training",
                "label": "Run Training",
                "supported": True,
                "allowed": False,
                "blocked_reason": "ML actions remain on user-backend.",
                "dangerous": True,
                "requires_confirmation": True,
                "confirmation_phrase": "RUN TRAINING",
                "dataset_path": "dataset.parquet",
                "target_model_version": "entry_quality_v1.1_test",
                "log_path": None,
            }
        ],
        "recent_action_runs": [],
    }
    dashboard = {
        "overview": overview,
        "training_gate": training_gate,
        "feature_completeness": {
            "recent_completeness_pct": feature_completeness["recent_completeness_pct"],
            "lifetime_completeness_pct": feature_completeness["lifetime_completeness_pct"],
            "broken_feature_count": feature_completeness["broken_feature_count"],
            "partially_missing_feature_count": feature_completeness["partially_missing_feature_count"],
        },
        "linkage_health": linkage,
        "activity_summary": {
            "window_days": activity["window_days"],
            "total_ml_scored_entries": activity["total_ml_scored_entries"],
            "allow_count": activity["allow_count"],
            "shadow_count": activity["shadow_count"],
            "block_count": activity["block_count"],
            "skip_count": activity["skip_count"],
            "average_ml_score": activity["average_ml_score"],
            "current_threshold": activity["current_threshold"],
            "current_hard_floor": activity["current_hard_floor"],
            "recent_activity_rows": activity["recent_activity_rows"],
        },
        "shadow_performance": {
            "window_days": shadow["window_days"],
            "total_linked_completed_trades_with_ml_attribution": shadow["total_linked_completed_trades_with_ml_attribution"],
            "decision_groups": shadow["decision_groups"],
            "good_allows": shadow["good_allows"],
            "bad_allows": shadow["bad_allows"],
            "good_blocks": shadow["good_blocks"],
            "bad_blocks": shadow["bad_blocks"],
        },
        "validation_history": {
            "total_models": 1,
            "latest_model": validation["items"][0],
            "source_note": validation["source_note"],
        },
        "dataset_builder_status": dataset_status,
        "alerts": alerts,
        "control_panel": control,
        "drift_monitoring": {
            "window_days": drift["window_days"],
            "live_win_rate": drift["live_win_rate"],
            "historical_win_rate": drift["historical_win_rate"],
            "win_rate_delta": drift["win_rate_delta"],
            "symbol_distribution": drift["symbol_distribution"],
            "regime_distribution": drift["regime_distribution"],
            "session_distribution": drift["session_distribution"],
            "average_pnl_by_score_band": drift["average_pnl_by_score_band"],
            "source_note": drift["source_note"],
        },
    }
    conn.execute(
        """
        INSERT INTO admin_ml_dashboard_snapshot (
            snapshot_key, generated_at, overview_json, training_gate_json, feature_completeness_json,
            activity_json, linkage_json, shadow_performance_json, validation_history_json,
            alerts_json, dataset_status_json, drift_json, dashboard_summary_json, dashboard_json,
            control_panel_json, metadata_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "latest",
            generated_at,
            json.dumps(overview),
            json.dumps(training_gate),
            json.dumps(feature_completeness),
            json.dumps(activity),
            json.dumps(linkage),
            json.dumps(shadow),
            json.dumps(validation),
            json.dumps(alerts),
            json.dumps(dataset_status),
            json.dumps(drift),
            json.dumps(dashboard_summary),
            json.dumps(dashboard),
            json.dumps(control),
            json.dumps({"generated_at": generated_at, "source_tables": ["admin_ml_*"], "warnings": alerts["items"]}),
            generated_at,
            generated_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO admin_ml_validation_history_snapshot (
            model_version, training_date, dataset_used, train_rows, test_rows, train_auc,
            test_auc, validation_method, notes, verdict, deployed_mode, generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "entry_quality_v1.1_test",
            generated_at,
            "dataset.parquet",
            100,
            25,
            0.7,
            0.66,
            "walk_forward",
            "ok",
            "accepted",
            "shadow",
            generated_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO admin_ml_control_visibility_snapshot (
            snapshot_key, generated_at, control_panel_json, recent_action_runs_json,
            source_note, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("latest", generated_at, json.dumps(control), "[]", "snapshot test", generated_at, generated_at),
    )
    conn.execute(
        """
        INSERT INTO admin_ml_linked_trade_snapshot (
            id, symbol, position_id, run_id, cycle_id, open_trace_id, close_trace_id,
            open_ts, close_ts, side, realized_pnl, r_multiple, ml_score, ml_action,
            ml_model_version, regime, confidence, threshold, features_json, linkage_branch,
            unlinked_reason, trace_match_count, source_trace_match_basis, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "ml-linked-1",
            "BTCUSDT",
            "pos-1",
            "run-1",
            "cycle-1",
            "trace-ml-1",
            None,
            generated_at,
            generated_at,
            "LONG",
            12.5,
            1.2,
            0.61,
            "ALLOW",
            "entry_quality_v1.1_test",
            "WEAK_TREND",
            0.8,
            0.4,
            json.dumps({"ml_score": 0.61}),
            "trace_id",
            None,
            1,
            "trade_fills.trace_id",
            generated_at,
            generated_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO admin_ml_feature_completeness_snapshot (
            id, scope, feature_name, total_rows, non_null_rows, null_rows,
            completeness_pct, recent_total_rows, recent_non_null_rows,
            recent_completeness_pct, last_seen_populated_at, frontend_status,
            recent_window_basis, recent_window_limit, generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("lifetime:ml_score", "lifetime", "ml_score", 1, 1, 0, 100.0, 1, 1, 100.0, generated_at, "healthy", "last_500_linked_completed_trades", 500, generated_at),
    )
    conn.execute(
        """
        INSERT INTO admin_ml_drift_snapshot (
            id, scope, symbol, regime, score_band, sample_count, win_rate,
            avg_pnl, avg_r_multiple, generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("recent_30d:symbol:BTCUSDT", "recent_30d", "BTCUSDT", None, None, 2, 50.0, -1.0, 0.2, generated_at),
    )


def _admin_token(admin_id: str = "admin-test") -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {
            "exp": now + timedelta(minutes=15),
            "nbf": now,
            "iat": now,
            "iss": "cosmicforge-admin-backend",
            "aud": "admin-portal",
            "sub": admin_id,
            "type": "admin_access",
            "role": "admin",
        },
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


@pytest.fixture
def client(tmp_path: Path):
    db = _create_admin_db(tmp_path)
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_returns_admin_backend_status(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "admin-backend"
    assert payload["database_reachable"] is True
    assert payload["user_backend_url"] == settings.USER_BACKEND_URL
    assert payload["bot_backend_url"] == settings.BOT_BACKEND_URL


def test_admin_auth_check_rejects_missing_token(client: TestClient):
    response = client.get("/health/admin-auth-check")
    assert response.status_code == 401


def test_admin_auth_check_rejects_malformed_token(client: TestClient):
    response = client.get("/health/admin-auth-check", headers={"Authorization": "Bearer not-a-token"})
    assert response.status_code == 401


def test_admin_auth_check_accepts_existing_admin_token(client: TestClient):
    response = client.get("/health/admin-auth-check", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "admin-backend"
    assert payload["admin"]["id"] == "admin-test"
    assert payload["admin"]["email"] == "admin@example.com"


def test_dashboard_stats_requires_admin(client: TestClient):
    response = client.get("/api/admin/dashboard/stats")
    assert response.status_code == 401


def test_dashboard_stats_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/dashboard/stats", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "total_users": 2,
        "active_subscriptions": 1,
        "total_revenue": 125.0,
        "platform_trades": 12,
    }


def test_revenue_overview_returns_data_array(client: TestClient):
    response = client.get(
        "/api/admin/dashboard/revenue-overview",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert list(payload.keys()) == ["data"]
    assert payload["data"][0]["total_revenue"] == 125


def test_top_trading_pairs_excludes_backfill_and_shadow(client: TestClient):
    response = client.get(
        "/api/admin/dashboard/top-trading-pairs",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["symbol"] == "BTCUSDT"
    assert payload["data"][0]["trade_count"] == 2
    assert {item["symbol"] for item in payload["data"]} == {"BTCUSDT", "ETHUSDT"}


def test_users_read_only_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/users", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert len(payload["users"]) == 2
    assert payload["users"][0]["email"] == "two@example.com"
    assert payload["users"][1]["verification_status"] == "verified"


def test_revenue_overview_requires_admin(client: TestClient):
    response = client.get("/api/admin/revenue/overview")
    assert response.status_code == 401


def test_revenue_overview_returns_real_stored_totals(client: TestClient):
    response = client.get("/api/admin/revenue/overview", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["subscription_revenue"] == 99.0
    assert payload["commission_revenue"] == 12.5
    assert payload["total_revenue"] == 111.5
    assert payload["revenue_by_plan"][0]["plan"] == "pro"
    assert payload["by_plan"] == {"pro": 99.0}


def test_revenue_overview_handles_missing_optional_tables(tmp_path: Path):
    db_path = tmp_path / "empty_revenue.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()
    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        response = TestClient(app).get("/api/admin/revenue/overview", headers={"Authorization": f"Bearer {_admin_token()}"})
    finally:
        app.dependency_overrides.clear()
        assert response.status_code == 200
        assert response.json() == {
            "total_revenue": 0.0,
            "subscription_revenue": 0.0,
            "commission_revenue": 0.0,
            "revenue_by_plan": [],
            "by_plan": {},
        }


def test_profitability_report_requires_admin(client: TestClient):
    response = client.get("/api/admin/profitability/report")
    assert response.status_code == 401


def test_profitability_report_returns_snapshot_backed_shape(client: TestClient):
    response = client.get(
        "/api/admin/profitability/report",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["scope"].startswith("snapshot: admin_profitability_")
    assert payload["snapshot_metadata"]["snapshot_source"] == "admin_profitability_*"
    assert payload["overall"]["total_fills"] == 4
    assert payload["overall"]["closed_trades"] == 3
    assert payload["overall"]["total_realized_pnl"] == 57.5
    assert payload["overall"]["best_trade"] is None
    assert payload["overall"]["worst_trade"] is None
    assert payload["risk_execution_quality"]["duplicate_order_id_action_symbol_groups"] == 0
    assert payload["recent"]["last_7d"]["closed_trades"] >= 3
    assert payload["per_symbol"][0]["symbol"] == "BTCUSDT"
    assert payload["per_symbol"][0]["trades"] == 2
    assert payload["sizing_cap_events"][0]["trace_id"] == "trace-size-1"
    assert payload["sizing_cap_events"][0]["cap_applied"] is True


def test_profitability_report_empty_snapshots_return_stable_response(tmp_path: Path):
    db_path = tmp_path / "empty_profitability_snapshots.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            full_name TEXT,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        INSERT INTO admins (id, email, full_name, role, is_superuser, is_active)
        VALUES ('admin-test', 'admin@example.com', 'Admin Test', 'admin', 1, 1)
        """
    )
    conn.execute(
        """
        CREATE TABLE admin_profitability_daily_summary (
            date TEXT NOT NULL,
            account_scope TEXT NOT NULL,
            fills_count INTEGER NOT NULL DEFAULT 0,
            closed_trades INTEGER NOT NULL DEFAULT 0,
            winning_trades INTEGER NOT NULL DEFAULT 0,
            losing_trades INTEGER NOT NULL DEFAULT 0,
            total_realized_pnl REAL NOT NULL DEFAULT 0,
            avg_pnl REAL,
            win_rate REAL,
            profit_factor REAL,
            avg_r_multiple REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date, account_scope)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE admin_profitability_symbol_summary (
            symbol TEXT NOT NULL,
            account_scope TEXT NOT NULL,
            fills_count INTEGER NOT NULL DEFAULT 0,
            closed_trades INTEGER NOT NULL DEFAULT 0,
            total_realized_pnl REAL NOT NULL DEFAULT 0,
            avg_pnl REAL,
            win_rate REAL,
            avg_r_multiple REAL,
            sl_count INTEGER NOT NULL DEFAULT 0,
            tp_count INTEGER NOT NULL DEFAULT 0,
            time_exit_count INTEGER NOT NULL DEFAULT 0,
            other_exit_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol, account_scope)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE admin_profitability_sizing_events (
            id TEXT PRIMARY KEY,
            trace_id TEXT,
            symbol TEXT,
            ts TEXT,
            run_id TEXT,
            cycle_id TEXT,
            sizing_method TEXT,
            configured_margin REAL,
            final_margin REAL,
            base_notional REAL,
            final_notional REAL,
            leverage REAL,
            cap_applied INTEGER NOT NULL DEFAULT 0,
            risk_cap_pct REAL,
            atr_stop_distance_pct REAL,
            explanation TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        response = TestClient(app).get(
            "/api/admin/profitability/report",
            headers={"Authorization": f"Bearer {_admin_token()}"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["overall"]["total_fills"] == 0
    assert payload["per_symbol"] == []
    assert payload["sizing_cap_events"] == []
    assert payload["snapshot_metadata"]["snapshot_stale"] is True
    assert payload["snapshot_metadata"]["snapshot_warning"] == "Profitability snapshots have not been generated yet."


def test_profitability_report_uses_snapshot_tables_only(tmp_path: Path):
    db = _create_admin_db(tmp_path)
    statements: list[str] = []

    class TraceDB(AdminDB):
        @contextmanager
        def connect(self):
            conn = sqlite3.connect(str(db.path), timeout=10)
            conn.row_factory = sqlite3.Row
            conn.set_trace_callback(statements.append)
            try:
                yield conn
            finally:
                conn.close()

    trace_db = TraceDB(path=str(db.path))
    app.dependency_overrides[health_api.get_db] = lambda: trace_db
    app.dependency_overrides[deps_api.get_db] = lambda: trace_db
    try:
        response = TestClient(app).get(
            "/api/admin/profitability/report",
            headers={"Authorization": f"Bearer {_admin_token()}"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    sql = "\n".join(statements).lower()
    forbidden_writes = ("insert ", "update ", "delete ", "create ", "alter ", "drop ")
    assert not any(token in sql for token in forbidden_writes)
    assert "admin_profitability_daily_summary" in sql
    assert "admin_profitability_symbol_summary" in sql
    assert "admin_profitability_sizing_events" in sql
    assert " from trade_fills" not in sql
    assert " from decision_traces" not in sql


ML_GET_ENDPOINTS = (
    "/api/admin/ml/dashboard-summary",
    "/api/admin/ml/overview",
    "/api/admin/ml/training-gate",
    "/api/admin/ml/feature-completeness",
    "/api/admin/ml/linkage-health",
    "/api/admin/ml/activity",
    "/api/admin/ml/shadow-performance",
    "/api/admin/ml/validation-history",
    "/api/admin/ml/dataset-builder-status",
    "/api/admin/ml/alerts",
    "/api/admin/ml/control-panel",
    "/api/admin/ml/drift-monitoring",
    "/api/admin/ml/dashboard",
)


def test_ml_snapshot_endpoints_require_admin(client: TestClient):
    for endpoint in ML_GET_ENDPOINTS:
        response = client.get(endpoint)
        assert response.status_code == 401, endpoint


def test_ml_snapshot_endpoints_return_stable_json(client: TestClient):
    headers = {"Authorization": f"Bearer {_admin_token()}"}
    for endpoint in ML_GET_ENDPOINTS:
        response = client.get(endpoint, headers=headers)
        assert response.status_code == 200, endpoint
        assert isinstance(response.json(), dict), endpoint


def test_ml_snapshot_dashboard_uses_contract_payload(client: TestClient):
    response = client.get("/api/admin/ml/dashboard", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["overview"]["ml_mode"] == "shadow"
    assert payload["training_gate"]["linkage_warnings"]["unlinked_completed_trades"] == 53
    assert payload["alerts"]["items"][0]["code"] == "unlinked_completed_trades"
    assert payload["snapshot_source"] == "admin_ml_dashboard_snapshot.dashboard_json"


def test_ml_validation_history_is_snapshot_read_only(client: TestClient):
    response = client.get(
        "/api/admin/ml/validation-history?limit=1",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["items"][0]["model_version"] == "entry_quality_v1.1_test"
    assert "snapshot" in payload["source_note"]


def test_ml_control_panel_is_visibility_only(client: TestClient):
    response = client.get("/api/admin/ml/control-panel", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["training_allowed_right_now"] is False
    assert payload["actions"][0]["allowed"] is False
    assert "user-backend" in payload["actions"][0]["blocked_reason"]


def test_ml_missing_snapshots_return_safe_defaults(tmp_path: Path):
    db_path = tmp_path / "missing_ml_snapshots.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 1,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute("INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)")
    conn.commit()
    conn.close()

    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        response = TestClient(app).get(
            "/api/admin/ml/dashboard-summary",
            headers={"Authorization": f"Bearer {_admin_token()}"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["snapshot_missing"] is True
    assert payload["status"] == "not_ready"


def test_ml_snapshot_endpoints_do_not_write_or_scan_raw_heavy_tables(tmp_path: Path):
    db = _create_admin_db(tmp_path)
    statements: list[str] = []

    class TraceDB(AdminDB):
        @contextmanager
        def connect(self):
            conn = sqlite3.connect(str(db.path), timeout=10)
            conn.row_factory = sqlite3.Row
            conn.set_trace_callback(statements.append)
            try:
                yield conn
            finally:
                conn.close()

    trace_db = TraceDB(path=str(db.path))
    app.dependency_overrides[health_api.get_db] = lambda: trace_db
    app.dependency_overrides[deps_api.get_db] = lambda: trace_db
    try:
        client = TestClient(app)
        headers = {"Authorization": f"Bearer {_admin_token()}"}
        assert client.get("/api/admin/ml/validation-history", headers=headers).status_code == 200
        assert client.get("/api/admin/ml/control-panel", headers=headers).status_code == 200
        assert client.get("/api/admin/ml/dashboard", headers=headers).status_code == 200
    finally:
        app.dependency_overrides.clear()

    sql = "\n".join(statements).lower()
    forbidden_writes = ("insert ", "update ", "delete ", "create ", "alter ", "drop ")
    assert not any(token in sql for token in forbidden_writes)
    assert "decision_traces" not in sql
    assert "trade_fills" not in sql


def test_admin_backend_does_not_expose_ml_action_post(client: TestClient):
    response = client.post(
        "/api/admin/ml/actions/run_training",
        headers={"Authorization": f"Bearer {_admin_token()}"},
        json={"confirmation_phrase": "RUN TRAINING"},
    )
    assert response.status_code in {404, 405}


def test_tradingview_webhooks_requires_admin(client: TestClient):
    response = client.get("/api/admin/tradingview/webhooks")
    assert response.status_code == 401


def test_tradingview_webhooks_returns_compatible_items(client: TestClient):
    response = client.get("/api/admin/tradingview/webhooks", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert list(payload.keys()) == ["items"]
    assert payload["items"][0]["id"] == "tvwh_test"
    assert payload["items"][0]["allowed_symbols"] == ["BTCUSDT"]
    assert payload["items"][0]["allowed_actions"] == ["BUY", "SELL"]
    assert "token_hash" not in payload["items"][0]


def test_tradingview_alerts_returns_compatible_items(client: TestClient):
    response = client.get("/api/admin/tradingview/alerts", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert list(payload.keys()) == ["items"]
    assert payload["items"][0]["status"] == "ACCEPTED_ADVISORY"
    assert payload["items"][0]["symbol_normalized"] == "BTCUSDT"
    assert payload["items"][0]["signature_valid"] == 1


def test_tradingview_decisions_returns_compatible_items(client: TestClient):
    response = client.get("/api/admin/tradingview/decisions", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert list(payload.keys()) == ["items"]
    assert payload["items"][0]["mode"] == "ADVISORY_ONLY"
    assert payload["items"][0]["normalized_signal"] == {"symbol": "BTCUSDT", "action": "BUY"}
    assert payload["items"][0]["final_status"] == "ACCEPTED_ADVISORY_ONLY"


def test_tradingview_external_signal_queue_returns_compatible_items(client: TestClient):
    response = client.get(
        "/api/admin/tradingview/external-signals",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert list(payload.keys()) == ["items"]
    assert payload["items"][0]["id"] == "extsig_test"
    assert payload["items"][0]["status"] == "PENDING"
    assert payload["items"][0]["result_json"] == {"queued": True}


def test_tradingview_processor_status_returns_compatible_items(client: TestClient):
    response = client.get(
        "/api/admin/tradingview/processor-status",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert list(payload.keys()) == ["items"]
    assert payload["items"][0]["bot_instance_id"] == "bot-1"
    assert payload["items"][0]["processor_enabled"] == 0
    assert payload["items"][0]["env_gate_reason"] == "DISABLED_BY_ENV"


def test_tradingview_processor_status_filter_missing_bot(client: TestClient):
    response = client.get(
        "/api/admin/tradingview/processor-status",
        params={"bot_instance_id": "missing-bot"},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    assert response.json() == {"items": [], "note": "No heartbeat found for bot_instance_id='missing-bot'"}


def test_tradingview_endpoints_handle_missing_tables(tmp_path: Path):
    db_path = tmp_path / "empty_tradingview.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()
    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        test_client = TestClient(app)
        for path in (
            "/api/admin/tradingview/webhooks",
            "/api/admin/tradingview/alerts",
            "/api/admin/tradingview/decisions",
            "/api/admin/tradingview/external-signals",
            "/api/admin/tradingview/processor-status",
        ):
            response = test_client.get(path, headers={"Authorization": f"Bearer {_admin_token()}"})
            assert response.status_code == 200
            assert response.json() == {"items": []}
    finally:
        app.dependency_overrides.clear()


def test_bot_monitor_overview_requires_admin(client: TestClient):
    response = client.get("/api/admin/bot/overview")
    assert response.status_code == 401


def test_bot_monitor_overview_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/bot/overview", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "running"
    assert payload["active_run_id"] == "run-1"
    assert payload["active_positions"] == 1
    assert payload["daily_pnl"] == 42.5
    assert payload["daily_trades"] == 3
    assert payload["recent_events_1h"] == 1
    assert isinstance(payload["uptime_seconds"], int)


def test_bot_monitor_runs_returns_compatible_shape(client: TestClient):
    response = client.get(
        "/api/admin/bot/runs",
        params={"limit": 10},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["runs"][0]["run_id"] == "run-1"
    assert payload["runs"][0]["started_at"] == "2026-05-01T00:00:00+00:00"
    assert payload["runs"][0]["realized_pnl"] == 42.5
    assert payload["runs"][0]["trades"] == 3


def test_bot_monitor_run_detail_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/bot/runs/run-1", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["run_id"] == "run-1"
    assert payload["summary"]["cycles"] == 12
    assert payload["events"][0]["event_type"] == "cycle_completed"
    assert payload["traces"][0]["trace_id"] == "trace-1"
    assert payload["traces"][0]["execution_status"] == "SKIPPED_PAPER"


def test_bot_monitor_run_detail_missing_run_returns_404(client: TestClient):
    response = client.get("/api/admin/bot/runs/missing-run", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 404
    assert response.json()["detail"] == "Run not found"


def test_bot_monitor_live_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/bot/live", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["positions"][0]["symbol"] == "BTCUSDT"
    assert payload["latest_decisions"][0]["trace_id"] == "trace-1"
    assert payload["latest_decisions"][0]["sizing_cap_event"]["cap_applied"] is True
    assert "reduced to 50.00 USDT" in payload["latest_decisions"][0]["sizing_cap_event"]["admin_message"]
    assert payload["latest_events"][0]["event_type"] == "cycle_completed"


def test_bot_monitor_endpoints_handle_missing_tables(tmp_path: Path):
    db_path = tmp_path / "empty_bot_monitor.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()
    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        test_client = TestClient(app)
        headers = {"Authorization": f"Bearer {_admin_token()}"}
        assert test_client.get("/api/admin/bot/overview", headers=headers).json() == {
            "status": "stopped",
            "uptime_seconds": 0,
            "active_run_id": None,
            "active_positions": 0,
            "daily_pnl": 0,
            "daily_trades": 0,
            "recent_events_1h": 0,
        }
        assert test_client.get("/api/admin/bot/runs", headers=headers).json() == {"runs": [], "count": 0}
        assert test_client.get("/api/admin/bot/live", headers=headers).json() == {
            "positions": [],
            "latest_decisions": [],
            "latest_events": [],
        }
        assert test_client.get("/api/admin/bot/runs/missing-run", headers=headers).status_code == 404
    finally:
        app.dependency_overrides.clear()


def test_admin_signals_requires_admin(client: TestClient):
    response = client.get("/api/admin/signals")
    assert response.status_code == 401


def test_admin_signals_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/signals", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["limit"] == 50
    assert payload["offset"] == 0
    assert payload["items"][0]["id"] == "sig-1"
    assert payload["items"][0]["symbol"] == "BTCUSDT"
    assert payload["items"][0]["is_published"] == 1


def test_admin_signal_candidates_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/signals/candidates", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["id"] == "sigcand-1"
    assert payload["items"][0]["status"] == "CANDIDATE"


def test_admin_signal_pairs_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/signals/pairs", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["symbol"] == "BTCUSDT"
    assert payload["items"][0]["enabled"] == 1
    assert payload["items"][0]["tier"] == "TIER_1"


def test_admin_signal_pair_metrics_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/signals/pairs/metrics", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["symbol"] == "BTCUSDT"
    assert payload["items"][0]["is_safe"] == 1
    assert payload["items"][0]["enabled"] == 1
    assert payload["items"][0]["whitelisted"] == 1


def test_admin_signal_scan_runs_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/signals/scan-runs", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["id"] == "scan-1"
    assert payload["items"][0]["status"] == "COMPLETED"


def test_admin_signal_scan_run_detail_returns_compatible_shape(client: TestClient):
    response = client.get("/api/admin/signals/scan-runs/scan-1", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["scan_run"]["id"] == "scan-1"
    assert payload["results"][0]["id"] == "scanres-1"
    assert payload["results"][0]["published_count"] == 1


def test_admin_signal_scan_run_detail_missing_returns_404(client: TestClient):
    response = client.get("/api/admin/signals/scan-runs/missing-scan", headers={"Authorization": f"Bearer {_admin_token()}"})
    assert response.status_code == 404
    assert response.json()["detail"] == "SCAN_RUN_NOT_FOUND"


# ---------------------------------------------------------------------------
# Phase 3F: Events read-only endpoints
# ---------------------------------------------------------------------------

def test_events_upcoming_requires_admin(client: TestClient):
    response = client.get("/api/admin/events/upcoming")
    assert response.status_code == 401


def test_events_upcoming_returns_scheduled_events(client: TestClient):
    response = client.get(
        "/api/admin/events/upcoming",
        params={"days": 30},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert payload[0]["event_id"] == "ev-1"
    assert payload[0]["impact_level"] == "HIGH"


def test_events_active_blackouts_returns_empty_list(client: TestClient):
    response = client.get(
        "/api/admin/events/active-blackouts",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    assert response.json() == []


def test_events_feed_status_returns_compatible_shape(client: TestClient):
    response = client.get(
        "/api/admin/events/feed-status",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert "last_sync_utc" in payload
    assert "is_stale" in payload
    assert "active_blackout_count" in payload
    assert "stale_threshold_hours" in payload


def test_events_reactions_summary_returns_compatible_shape(client: TestClient):
    response = client.get(
        "/api/admin/events/reactions/summary",
        params={"days": 30},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_reactions"] == 1
    assert payload["by_type"][0]["reaction_type"] == "TREND_CONTINUATION"
    assert isinstance(payload["by_quality"], list)


def test_events_reactions_recent_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/events/reactions/recent",
        params={"days": 30},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["event_id"] == "ev-1"
    assert payload[0]["symbol"] == "BTCUSDT"


def test_events_reactions_for_event_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/events/reactions/ev-1",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["symbol"] == "BTCUSDT"


def test_events_single_reaction_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/events/reactions/ev-1/BTCUSDT",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["event_id"] == "ev-1"
    assert payload["reaction_type"] == "TREND_CONTINUATION"


def test_events_single_reaction_missing_returns_404(client: TestClient):
    response = client.get(
        "/api/admin/events/reactions/ev-1/MISSING",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 404


def test_events_snapshots_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/events/snapshots/ev-1/BTCUSDT",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["window_label"] == "PRE_5M"


def test_events_endpoints_handle_missing_tables(tmp_path: Path):
    db_path = tmp_path / "empty_events.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()
    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        test_client = TestClient(app)
        headers = {"Authorization": f"Bearer {_admin_token()}"}
        assert test_client.get("/api/admin/events/upcoming", headers=headers).json() == []
        assert test_client.get("/api/admin/events/active-blackouts", headers=headers).json() == []
        assert test_client.get("/api/admin/events/reactions/recent", headers=headers).json() == []
        assert test_client.get("/api/admin/events/reactions/ev-1", headers=headers).json() == []
        assert test_client.get("/api/admin/events/snapshots/ev-1/BTCUSDT", headers=headers).json() == []
        assert test_client.get("/api/admin/events/reactions/ev-1/BTCUSDT", headers=headers).status_code == 404
        summary = test_client.get("/api/admin/events/reactions/summary", headers=headers).json()
        assert summary["total_reactions"] == 0
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Phase 3F: News read-only endpoints
# ---------------------------------------------------------------------------

def test_news_feed_status_requires_admin(client: TestClient):
    response = client.get("/api/admin/news/feed-status")
    assert response.status_code == 401


def test_news_feed_status_returns_compatible_shape(client: TestClient):
    response = client.get(
        "/api/admin/news/feed-status",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert "today_count" in payload
    assert "active_sources" in payload
    assert payload["shadow_only"] is True
    assert payload["signal_can_open_trades"] is False


def test_news_runtime_mode_returns_shadow_mode(client: TestClient):
    response = client.get(
        "/api/admin/news/runtime-mode",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["state"]["current_mode"] == "SHADOW"
    assert payload["max_allowed_action"] == "ANNOTATE_ONLY"
    assert payload["next_eligible_mode"] == "ADVISORY"
    assert payload["execution_impact"] is False


def test_news_runtime_mode_missing_table_returns_safe_default(tmp_path: Path):
    db_path = tmp_path / "no_mode.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()
    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        response = TestClient(app).get(
            "/api/admin/news/runtime-mode",
            headers={"Authorization": f"Bearer {_admin_token()}"},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    payload = response.json()
    assert payload["state"]["current_mode"] == "SHADOW"
    assert payload["max_allowed_action"] == "ANNOTATE_ONLY"


def test_news_influence_decisions_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/news/influence-decisions",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["symbol"] == "BTCUSDT"
    assert payload[0]["applied_action"] == "ANNOTATE_ONLY"


def test_news_sources_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/news/sources",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["id"] == "coindesk"
    assert "health_status" in payload[0]


def test_news_clusters_returns_data_with_narratives_and_signals(client: TestClient):
    response = client.get(
        "/api/admin/news/clusters",
        params={"hours": 168},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["canonical_title"] == "BTC hits 65k"
    assert len(payload[0]["narratives"]) == 1
    assert payload[0]["narratives"][0]["narrative_type"] == "BULLISH"
    assert len(payload[0]["signals"]) == 1


def test_news_signals_returns_shadow_only(client: TestClient):
    response = client.get(
        "/api/admin/news/signals",
        params={"hours": 168},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["should_affect_trading"] == 0
    assert payload[0]["shadow_only"] == 1


def test_news_validations_summary_returns_compatible_shape(client: TestClient):
    response = client.get(
        "/api/admin/news/validations/summary",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_validations"] == 1
    assert payload["correct_count"] == 1
    assert payload["correct_pct"] == 100.0
    assert "by_latency_category" in payload


def test_news_validations_summary_route_distinct_from_list(client: TestClient):
    r_summary = client.get(
        "/api/admin/news/validations/summary",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    r_list = client.get(
        "/api/admin/news/validations",
        params={"hours": 168},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert r_summary.status_code == 200
    assert r_list.status_code == 200
    assert isinstance(r_summary.json(), dict)
    assert isinstance(r_list.json(), list)


def test_news_items_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/news/items",
        params={"hours": 168},
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["title"] == "BTC hits 65k"


def test_news_data_quality_returns_compatible_shape(client: TestClient):
    response = client.get(
        "/api/admin/news/data-quality",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_clusters"] == 1
    assert payload["valid_clusters"] == 1
    assert "by_status" in payload


def test_news_narrative_effectiveness_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/news/narrative-effectiveness",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["narrative_type"] == "BULLISH"


def test_news_rt_provider_status_returns_data(client: TestClient):
    response = client.get(
        "/api/admin/news/rt-provider-status",
        headers={"Authorization": f"Bearer {_admin_token()}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["provider"] == "cryptopanic"
    assert payload[0]["health_status"] == "DISABLED"


def test_news_endpoints_handle_missing_tables(tmp_path: Path):
    db_path = tmp_path / "empty_news.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()
    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        test_client = TestClient(app)
        headers = {"Authorization": f"Bearer {_admin_token()}"}
        for path in (
            "/api/admin/news/sources",
            "/api/admin/news/health",
            "/api/admin/news/items",
            "/api/admin/news/clusters",
            "/api/admin/news/narratives",
            "/api/admin/news/signals",
            "/api/admin/news/validations",
            "/api/admin/news/narrative-effectiveness",
            "/api/admin/news/provider-stats",
            "/api/admin/news/imports",
            "/api/admin/news/rt-provider-status",
            "/api/admin/news/rt-feed",
            "/api/admin/news/duplicate-clusters",
            "/api/admin/news/conflicts",
            "/api/admin/news/influence-decisions",
        ):
            response = test_client.get(path, headers=headers)
            assert response.status_code == 200, f"{path} returned {response.status_code}"
            assert isinstance(response.json(), list), f"{path} did not return a list"
        for path, expected_keys in (
            ("/api/admin/news/feed-status", ["today_count", "shadow_only"]),
            ("/api/admin/news/data-quality", ["total_clusters", "valid_clusters"]),
            ("/api/admin/news/validations/summary", ["total_validations", "correct_count"]),
        ):
            response = test_client.get(path, headers=headers)
            assert response.status_code == 200, f"{path} returned {response.status_code}"
            data = response.json()
            for k in expected_keys:
                assert k in data, f"{path} missing key {k}"
    finally:
        app.dependency_overrides.clear()


def test_admin_signal_endpoints_handle_missing_tables(tmp_path: Path):
    db_path = tmp_path / "empty_signals.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()
    db = AdminDB(path=str(db_path))
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    try:
        test_client = TestClient(app)
        headers = {"Authorization": f"Bearer {_admin_token()}"}
        for path, expected in (
            ("/api/admin/signals", {"items": [], "count": 0, "limit": 50, "offset": 0}),
            ("/api/admin/signals/candidates", {"items": [], "count": 0, "limit": 50, "offset": 0}),
            ("/api/admin/signals/pairs", {"items": [], "count": 0, "limit": 100, "offset": 0}),
            ("/api/admin/signals/pairs/metrics", {"items": [], "count": 0, "limit": 100, "offset": 0}),
            ("/api/admin/signals/scan-runs", {"items": [], "count": 0, "limit": 50, "offset": 0}),
        ):
            response = test_client.get(path, headers=headers)
            assert response.status_code == 200
            assert response.json() == expected
        assert test_client.get("/api/admin/signals/scan-runs/missing-scan", headers=headers).status_code == 404
    finally:
        app.dependency_overrides.clear()
