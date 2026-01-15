from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator


# =========================
# Time helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =========================
# Database class
# =========================
class DB:
    """
    Single source of truth for SQLite access.
    Default path: data/bot.db
    """

    def __init__(self, path: str = "data/bot.db"):
        self.path = path

        folder = os.path.dirname(self.path)
        if folder:
            os.makedirs(folder, exist_ok=True)

        self._init()

    # -------------------------
    # Connection manager
    # -------------------------
    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        # ✅ keep same timeout + thread setting, just add pragmas to reduce "database is locked"
        conn = sqlite3.connect(self.path, timeout=1, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # ✅ ADD: improve concurrent read/write + wait on locks instead of failing
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=1000;")  # 1s wait on locks (fast shutdown)
        except Exception:
            # pragma failures shouldn't crash app; continue with defaults
            pass

        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # -------------------------
    # Init / migrations
    # -------------------------
    def _init(self) -> None:
        conn = sqlite3.connect(self.path, timeout=1)
        try:
            # ✅ ADD: same hardening for init connection as well
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA busy_timeout=1000;")
            except Exception:
                pass

            # =========================
            # Runs (one per bot run)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    stopped_at TEXT,
                    mode TEXT NOT NULL,
                    interval_seconds INTEGER NOT NULL,
                    max_symbols INTEGER NOT NULL,
                    config_json TEXT,
                    status TEXT
                )
                """
            )

            # =========================
            # Events (audit log)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_utc TEXT NOT NULL,
                    ts TEXT, -- ✅ Added for consistency
                    run_id TEXT,
                    cycle_id TEXT,
                    symbol TEXT,
                    event_type TEXT NOT NULL,
                    action TEXT,
                    details_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
                """
            )

            # =========================
            # Daily risk state
            # =========================
            # Daily/Risk state
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_state (
                    day TEXT PRIMARY KEY,
                    realized_pnl REAL DEFAULT 0.0,
                    kill INTEGER DEFAULT 0,
                    trade_count INTEGER DEFAULT 0,
                    last_updated_at TEXT
                )
            """)

            # Weekly equity snapshots (for drawdown)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS weekly_snapshots (
                    week_start_date TEXT PRIMARY KEY, -- Monday date
                    start_equity REAL,
                    peak_equity REAL,
                    low_equity REAL,
                    updated_at TEXT
                )
            """)

            # Monthly equity snapshots (for drawdown)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS monthly_snapshots (
                    month_start_date TEXT PRIMARY KEY, -- 1st of month
                    start_equity REAL,
                    peak_equity REAL,
                    low_equity REAL,
                    updated_at TEXT
                )
            """)

            # =========================
            # Per-symbol trading state
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_state (
                    symbol TEXT PRIMARY KEY,
                    position TEXT NOT NULL,
                    entry_price REAL,
                    last_signal TEXT NOT NULL,
                    last_action TEXT NOT NULL,
                    last_checked_ms INTEGER NOT NULL,
                    adds INTEGER NOT NULL,
                    last_trade_ms INTEGER NOT NULL,
                    pending_open TEXT NOT NULL,
                    entry_qty REAL NOT NULL,
                    last_user_trade_id INTEGER NOT NULL DEFAULT 0,
                    last_stop_ms INTEGER NOT NULL DEFAULT 0,
                    reentry_confirm_signal TEXT NOT NULL DEFAULT 'NONE',
                    reentry_confirm_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )

            # =========================
            # Run summary (derived metrics)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_summary (
                    run_id TEXT PRIMARY KEY,
                    cycles INTEGER NOT NULL DEFAULT 0,
                    trades INTEGER NOT NULL DEFAULT 0,
                    opens INTEGER NOT NULL DEFAULT 0,
                    closes INTEGER NOT NULL DEFAULT 0,
                    errors INTEGER NOT NULL DEFAULT 0,
                    realized_pnl REAL,
                    win_trades INTEGER,
                    loss_trades INTEGER,
                    last_event_at TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
                """
            )

            # =========================
            # Trade fills (for true PnL + win rate)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    cycle_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,                 -- LONG/SHORT
                    action TEXT NOT NULL,               -- OPEN/CLOSE
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    fee REAL,
                    realized_pnl REAL,                  -- only for CLOSE
                    timestamp_utc TEXT NOT NULL
                )
                """
            )

            # =========================
            # Indexes
            # =========================
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_time ON events(timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)"
            )

            # =========================
            # Migrations (Auto-patch)
            # =========================
            try:
                # 1) config_json in runs
                c = conn.execute("PRAGMA table_info(runs)")
                cols = [r[1] for r in c.fetchall()]
                if "config_json" not in cols:
                    conn.execute("ALTER TABLE runs ADD COLUMN config_json TEXT")

                # 2) ts in events
                c = conn.execute("PRAGMA table_info(events)")
                cols = [r[1] for r in c.fetchall()]
                if "ts" not in cols:
                    conn.execute("ALTER TABLE events ADD COLUMN ts TEXT")

                # 3) status in runs
                c = conn.execute("PRAGMA table_info(runs)")
                cols = [r[1] for r in c.fetchall()]
                if "status" not in cols:
                    conn.execute("ALTER TABLE runs ADD COLUMN status TEXT")

                # 4) trades in run_summary (was orders before)
                c = conn.execute("PRAGMA table_info(run_summary)")
                cols = [r[1] for r in c.fetchall()]
                # If we have orders but not trades, we could rename, but simpler to just add trades
                if "trades" not in cols:
                     conn.execute("ALTER TABLE run_summary ADD COLUMN trades INTEGER DEFAULT 0")

                if "last_event_at" not in cols:
                     conn.execute("ALTER TABLE run_summary ADD COLUMN last_event_at TEXT")

                # 5) trace_id in events (for monitoring)
                c = conn.execute("PRAGMA table_info(events)")
                cols = [r[1] for r in c.fetchall()]
                if "trace_id" not in cols:
                    conn.execute("ALTER TABLE events ADD COLUMN trace_id TEXT")

            except Exception:
                pass

            # =========================
            # Decision Traces (Monitoring Phase 1)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS decision_traces (
                    trace_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    cycle_id TEXT,
                    account_id TEXT,
                    environment TEXT,
                    symbol TEXT,
                    timeframe TEXT,
                    ts TEXT,
                    
                    -- Market snapshot
                    last_price REAL,
                    mark_price REAL,
                    
                    -- Risk snapshot
                    equity REAL,
                    margin_used REAL,
                    margin_level REAL,
                    drawdown_pct REAL,
                    open_positions_count INTEGER,
                    
                    -- Strategy outputs (JSON array)
                    strategy_signals_json TEXT,
                    chosen_strategy TEXT,
                    signal TEXT,
                    confidence REAL,
                    
                    -- Gate decisions
                    gate_allowed INTEGER,
                    gate_reason TEXT,
                    gate_details_json TEXT,
                    
                    -- Action plan
                    intended_action TEXT,
                    sizing_json TEXT,
                    sl_plan REAL,
                    tp_plan REAL,
                    
                    -- Execution result
                    order_id TEXT,
                    execution_status TEXT,
                    execution_error TEXT,
                    fill_price REAL,
                    fill_qty REAL,
                    
                    -- Final outcome
                    final_state_change TEXT,
                    final_position TEXT
                )
                """
            )

            # =========================
            # Invariant Violations (Monitoring Phase 3)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS invariant_violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    trace_id TEXT,
                    run_id TEXT,
                    symbol TEXT,
                    violation_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    details_json TEXT,
                    auto_action_taken TEXT,
                    acknowledged INTEGER DEFAULT 0
                )
                """
            )

            # =========================
            # Alerts (Monitoring Phase 5)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    trace_id TEXT,
                    symbol TEXT,
                    message TEXT,
                    details_json TEXT,
                    acknowledged INTEGER DEFAULT 0,
                    acknowledged_at TEXT,
                    acknowledged_by TEXT
                )
                """
            )

            # =========================
            # Broker Connections (Page 4)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS broker_accounts (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    broker_id TEXT NOT NULL,        -- binance, bybit, etc.
                    market_type TEXT NOT NULL,      -- crypto, forex, stocks
                    label TEXT,
                    status TEXT NOT NULL,           -- draft, validating, connected, restricted, disconnected, disabled
                    environment TEXT DEFAULT 'live',-- live, demo
                    account_type TEXT,              -- spot, futures
                    capabilities JSON,              -- read, trade, margin, etc.
                    masked_key TEXT,                -- first4..last4
                    last_validated_at TEXT,
                    last_error_code TEXT,
                    last_error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS broker_credentials (
                    account_id TEXT PRIMARY KEY,
                    encrypted_blob TEXT NOT NULL,
                    key_metadata TEXT,              -- version, algo, etc.
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS broker_audit_log (
                    id TEXT PRIMARY KEY,
                    broker_account_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,       -- connected, credentials_updated, validation_failed
                    details_json TEXT,
                    ip_address TEXT,
                    timestamp_utc TEXT NOT NULL,
                    FOREIGN KEY(broker_account_id) REFERENCES broker_accounts(id)
                )
                """
            )

            # =========================
            # Subscription & Billing (Page 5)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id TEXT PRIMARY KEY,
                    plan_id TEXT NOT NULL,
                    status TEXT NOT NULL,           -- trialing, active, past_due, canceled, incomplete
                    provider_sub_id TEXT,
                    current_period_end TEXT,
                    cancel_at_period_end INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS invoices (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    amount REAL,
                    currency TEXT,
                    status TEXT NOT NULL,           -- paid, open, void, uncollectible
                    period_start TEXT,
                    period_end TEXT,
                    hosted_invoice_url TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_intents (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    plan_id TEXT NOT NULL,
                    session_id TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )

            # =========================
            # Billing Events (Audit)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS billing_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT,                  -- provider event id
                    event_type TEXT NOT NULL,       -- checkouts.session.completed, etc
                    provider TEXT DEFAULT 'stripe',
                    payload_json TEXT,
                    processed_at TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )

            # =========================
            # Revenue / Commissions
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS commission_config (
                    id TEXT PRIMARY KEY,            -- 'global' or plan_id
                    percentage REAL NOT NULL,       -- 0.1 = 10%
                    cap_amount REAL,                -- max per trade?
                    currency TEXT DEFAULT 'USD',
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS commission_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    plan_id TEXT,
                    
                    trade_pnl REAL,
                    commission_amount REAL,
                    currency TEXT,
                    
                    status TEXT NOT NULL,           -- pending, realized, invoiced
                    invoice_id TEXT,
                    
                    created_at TEXT NOT NULL
                )
                """
            )

            # =========================
            # Onboarding Wizard (Page 6)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS onboarding_profiles (
                    user_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,           -- not_started, in_progress, completed, skipped
                    current_step TEXT,
                    data_json TEXT,                 -- answers
                    recommended_defaults TEXT,      -- generated configs
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                )
                """
            )

            # =========================
            # Strategy System (Page 7)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategies (
                    id TEXT PRIMARY KEY,
                    owner_id TEXT,                  -- NULL for official/system strategies
                    visibility TEXT NOT NULL,       -- official, community, private, premium
                    status TEXT NOT NULL,           -- draft, active, deprecated, removed, under_review, rejected
                    name TEXT NOT NULL,
                    description TEXT,
                    market_types JSON,              -- ["crypto", "forex"]
                    timeframes JSON,                -- ["1m", "1h"]
                    tags JSON,                      -- ["trend", "scalping"]
                    entitlement_tier TEXT DEFAULT 'free', -- plan requirement
                    recommended_risk_style TEXT,    -- conservative, aggressive
                    constraints_json TEXT,          -- {"required": ["LEVERAGE"], "min_capital": 1000}
                    metrics_json TEXT,              -- Cached stats: {"win_rate": 65.4, "roi": 12.0}
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_versions (
                    id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    version_number INTEGER NOT NULL,
                    spec_json TEXT NOT NULL,        -- The DSL execution graph
                    param_schema_json TEXT,         -- Inputs for the user UI
                    changelog TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(strategy_id) REFERENCES strategies(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_strategies (
                    user_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    is_favorite INTEGER DEFAULT 0,
                    last_used_at TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(user_id, strategy_id),
                    FOREIGN KEY(strategy_id) REFERENCES strategies(id)
                )
                """
            )
            
            # --- Migrations for Strategy System ---
            try:
                c = conn.execute("PRAGMA table_info(strategies)")
                cols = [r[1] for r in c.fetchall()]
                
                if "recommended_risk_style" not in cols:
                    conn.execute("ALTER TABLE strategies ADD COLUMN recommended_risk_style TEXT")
                if "constraints_json" not in cols:
                    conn.execute("ALTER TABLE strategies ADD COLUMN constraints_json TEXT") 
                if "metrics_json" not in cols:
                    conn.execute("ALTER TABLE strategies ADD COLUMN metrics_json TEXT")
            except Exception:
                pass

            conn.commit()

        finally:
            conn.close()

    # =========================
    # Utility helpers
    # =========================
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
