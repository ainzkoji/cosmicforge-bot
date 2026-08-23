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

    def __init__(self, path: str = None):
        if path is None:
            # ── CANONICAL PATH RESOLUTION ──────────────────────────────────────
            # Priority 1: DATABASE_URL environment variable (set in .env)
            # This is the authoritative runtime DB — shared by runner, API, and
            # all persistence services.  Strip the sqlite:/// scheme prefix and
            # resolve relative to the current working directory.
            import dotenv
            import logging
            dotenv.load_dotenv()
            _db_url = os.environ.get("DATABASE_URL", "")
            if _db_url.startswith("sqlite:///"):
                _rel = _db_url[len("sqlite:///"):]
                if not os.path.isabs(_rel):
                    # This file is: backends/shared/shared_lib/persistence/db.py
                    # We want to resolve relative to 'backends' root.
                    # 'backends' is 3 levels up from this file's directory.
                    base_dir = os.path.dirname(os.path.abspath(__file__))
                    backends_dir = os.path.abspath(os.path.join(base_dir, "../../.."))
                    # The URL in .env is typically relative to the component (bot-backend)
                    # Resolve it as if we were in bot-backend.
                    path = os.path.abspath(os.path.join(backends_dir, "bot-backend", _rel))
                else:
                    path = _rel
                logging.getLogger(__name__).info(f"[DB INIT] Resolved DATABASE_URL: {path}")
            else:
                # Priority 2: legacy fallback — 4-level root traversal
                base_dir = os.path.dirname(os.path.abspath(__file__))
                root_dir = os.path.abspath(os.path.join(base_dir, "../../../../"))
                if os.path.exists(os.path.join(root_dir, "data")):
                     path = os.path.join(root_dir, "data", "bot.db")
                else:
                     path = "../../data/bot.db"
                logging.getLogger(__name__).warning(f"[DB INIT] DATABASE_URL missing or invalid. Falling back to legacy path: {path}")
        
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
        """
        Get a database connection as a context manager.
        
        Usage:
            with db.connect() as conn:
                cursor = conn.execute("SELECT * FROM table")
                ...
        """
        # Increased timeout to 10s for Windows/OneDrive stability
        conn = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # ✅ ADD: improve concurrent read/write + wait on locks instead of failing
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=10000;")  # 10s wait on locks
        except Exception:
            # pragma failures shouldn't crash app; continue with defaults
            pass

        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
    
    def get_connection(self) -> Iterator[sqlite3.Connection]:
        """
        Alias for connect() for backward compatibility.
        Many analytics services use db.get_connection() instead of db.connect().
        """
        return self.connect()

    # -------------------------
    # Init / migrations
    # -------------------------
    def _init(self) -> None:
        # Increased timeout to 10s for Windows/OneDrive stability
        conn = sqlite3.connect(self.path, timeout=10)
        try:
            # ✅ ADD: same hardening for init connection as well
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA busy_timeout=10000;")
            except Exception:
                pass

            # =========================
            # Admins (Decoupled Identity)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admins (
                    id TEXT PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    hashed_password TEXT NOT NULL,
                    full_name TEXT,
                    role TEXT NOT NULL DEFAULT 'admin',
                    is_active INTEGER DEFAULT 1,
                    is_superuser INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_login_at TEXT
                )
                """
            )

            # =========================
            # Admin Sessions
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_sessions (
                    id TEXT PRIMARY KEY,
                    admin_id TEXT NOT NULL,
                    refresh_token_hash TEXT NOT NULL,
                    device TEXT,
                    ip TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT,
                    rotated_from TEXT,
                    FOREIGN KEY (admin_id) REFERENCES admins(id)
                )
                """
            )

            # =========================
            # Users (Auth)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    hashed_password TEXT NOT NULL,
                    full_name TEXT,
                    is_active INTEGER DEFAULT 1,
                    is_superuser INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending_verification',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            # =========================
            # Admin Roles
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_roles (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'admin',
                    granted_by TEXT,
                    granted_at TEXT NOT NULL,
                    revoked_at TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS revenue_snapshots (
                    date TEXT PRIMARY KEY,
                    subscription_revenue REAL DEFAULT 0,
                    commission_revenue REAL DEFAULT 0,
                    total_revenue REAL DEFAULT 0,
                    created_at TEXT
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_audit_log (
                    id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    user_id TEXT,
                    email TEXT,
                    ip TEXT,
                    details TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kyc_submissions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    document_type TEXT,
                    risk_level TEXT,
                    status TEXT DEFAULT 'pending',
                    submitted_at TEXT NOT NULL,
                    reviewed_by TEXT,
                    reviewed_at TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS aml_alerts (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    alert_type TEXT,
                    severity TEXT,
                    description TEXT,
                    status TEXT DEFAULT 'open',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS commission_tiers (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    min_volume REAL,
                    max_volume REAL,
                    rate REAL,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )

            # =========================
            # System Settings (Admin)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS system_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT,
                    updated_at TEXT NOT NULL,
                    updated_by TEXT
                )
                """
            )

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

            # ✅ ADD: Per-bot daily state (for multi-bot support)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_daily_state (
                    bot_instance_id TEXT NOT NULL,
                    day TEXT NOT NULL,
                    realized_pnl REAL DEFAULT 0.0,
                    kill INTEGER DEFAULT 0,
                    trade_count INTEGER DEFAULT 0,
                    last_updated_at TEXT,
                    PRIMARY KEY (bot_instance_id, day)
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
            # Bot Instances
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_instances (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    broker_account_id TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    strategy_version TEXT,
                    config_id TEXT,
                    risk_profile_id TEXT,
                    
                    symbols JSON,
                    timeframes JSON,
                    
                    allocation_type TEXT,
                    allocation_value REAL,
                    
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    stopped_at TEXT,
                    
                    last_run_at TEXT,
                    last_run_id TEXT,
                    last_error TEXT,
                    
                    FOREIGN KEY(user_id) REFERENCES users(id),
                    FOREIGN KEY(broker_account_id) REFERENCES broker_accounts(id)
                )
                """
            )

            # =========================
            # Bot Instance Symbol State (Multi-User)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_symbol_state (
                    bot_instance_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
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
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (bot_instance_id, symbol),
                    FOREIGN KEY(bot_instance_id) REFERENCES bot_instances(id)
                )
                """
            )

            # =========================
            # Position Lifecycle State (SEV-1: Protection Invariant)
            # =========================
            # Stores per-position PM state so restarts don't rebuild from ATR defaults.
            # Required by state_store.save_lifecycle_state / load_lifecycle_state.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS position_lifecycle_state (
                    bot_instance_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    position_id TEXT,
                    phase TEXT NOT NULL DEFAULT 'SEEKING_TP1',
                    original_stop REAL,
                    current_stop REAL,
                    original_tp1 REAL,
                    original_tp2 REAL,
                    is_break_even INTEGER DEFAULT 0,
                    tp1_hit INTEGER DEFAULT 0,
                    trailing_active INTEGER DEFAULT 0,
                    highest_since_entry REAL,
                    lowest_since_entry REAL,
                    entry_qty_remaining REAL,
                    sl_order_id TEXT,
                    tp_order_id TEXT,
                    exchange_position_active INTEGER,
                    reconciliation_status TEXT,
                    reconciliation_reason TEXT,
                    last_reconciled_at TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (bot_instance_id, symbol),
                    FOREIGN KEY(bot_instance_id) REFERENCES bot_instances(id)
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
                    trace_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,                 -- LONG/SHORT
                    action TEXT NOT NULL,               -- OPEN/CLOSE
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    fee REAL,
                    realized_pnl REAL,                  -- only for CLOSE
                    timestamp_utc TEXT NOT NULL,
                    slippage_pct REAL,
                    entry_price_expected REAL,
                    stop_loss_price REAL,
                    position_id TEXT,
                    r_multiple REAL
                )
                """
            )

            # =========================
            # Equity Snapshots (Broker Truth for Reporting)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    bot_instance_id TEXT,
                    broker_account_id TEXT NOT NULL,
                    broker_id TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    wallet_balance REAL,
                    equity REAL,
                    available_balance REAL,
                    unrealized_pnl REAL,
                    margin_used REAL,
                    currency TEXT DEFAULT 'USDT',
                    source TEXT NOT NULL,
                    metadata_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            # =========================
            # Broker Transfers Cache (Deposits/Withdrawals)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS broker_transfers_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    broker_account_id TEXT NOT NULL,
                    broker_id TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    type TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT,
                    fee REAL,
                    network TEXT,
                    txid TEXT,
                    raw_id TEXT,
                    raw_json TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(broker_account_id, type, raw_id)
                )
                """
            )
            
            # =========================
            # Migrations for equity_snapshots
            # =========================
            # Add missing columns to equity_snapshots if they don't exist
            try:
                c = conn.execute("PRAGMA table_info(equity_snapshots)")
                cols = [r[1] for r in c.fetchall()]
                if "user_id" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN user_id TEXT NOT NULL DEFAULT ''")
                if "broker_account_id" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN broker_account_id TEXT NOT NULL DEFAULT ''")
                if "broker_id" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN broker_id TEXT NOT NULL DEFAULT ''")
                if "wallet_balance" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN wallet_balance REAL")
                if "equity" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN equity REAL")
                if "available_balance" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN available_balance REAL")
                if "unrealized_pnl" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN unrealized_pnl REAL")
                if "margin_used" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN margin_used REAL")
                if "currency" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN currency TEXT DEFAULT 'USDT'")
                if "source" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN source TEXT NOT NULL DEFAULT 'migration'")
                if "metadata_json" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN metadata_json TEXT")
                if "updated_at" not in cols:
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass

            # =========================
            # Indexes
            # ========================
            
            # Migration: Ensure trade_fills has user_id/bot_instance_id BEFORE indexing
            try:
                c = conn.execute("PRAGMA table_info(trade_fills)")
                cols = [r[1] for r in c.fetchall()]
                if "user_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN user_id TEXT")
                if "bot_instance_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN bot_instance_id TEXT")
                if "broker_account_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN broker_account_id TEXT")
                if "quote_currency" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN quote_currency TEXT DEFAULT 'USDT'")
                if "base_currency" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN base_currency TEXT")
                if "order_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN order_id TEXT")
                
                # Analytics Hardening columns
                if "slippage_pct" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN slippage_pct REAL")
                if "entry_price_expected" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN entry_price_expected REAL")
                if "stop_loss_price" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN stop_loss_price REAL")
                if "position_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN position_id TEXT")
                if "trace_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN trace_id TEXT")
                if "r_multiple" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN r_multiple REAL")
            except Exception:
                pass

            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_time ON events(timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)"
            )
            
            # Trade fills indexes (for reporting)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fills_user_time ON trade_fills(user_id, timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fills_bot_time ON trade_fills(bot_instance_id, timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fills_broker_time ON trade_fills(broker_account_id, timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fills_symbol ON trade_fills(symbol, timestamp_utc)"
            )
            
            # Equity snapshots indexes
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_equity_user_time ON equity_snapshots(user_id, timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_equity_bot_time ON equity_snapshots(bot_instance_id, timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_equity_broker_time ON equity_snapshots(broker_account_id, timestamp_utc)"
            )
            
            # Transfers indexes
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_broker_time ON broker_transfers_cache(broker_account_id, ts_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_user_time ON broker_transfers_cache(user_id, ts_utc)"
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

                # 6) Missing columns in users (Fix for Auth)
                c = conn.execute("PRAGMA table_info(users)")
                cols = [r[1] for r in c.fetchall()]
                
                # Core auth
                if "last_login_at" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN last_login_at TEXT")
                if "is_verified" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN is_verified INTEGER DEFAULT 0")
                if "terms_accepted_at" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN terms_accepted_at TEXT")
                if "risk_disclaimer_accepted_at" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN risk_disclaimer_accepted_at TEXT")
                
                # Profile / Marketing
                if "locale" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN locale TEXT")
                if "country" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN country TEXT")
                if "timezone" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN timezone TEXT")
                if "marketing_session_id" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN marketing_session_id TEXT")
                if "selected_plan_id" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN selected_plan_id TEXT")
                if "total_trades" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN total_trades INTEGER DEFAULT 0")
                if "total_commission" not in cols:
                    conn.execute("ALTER TABLE users ADD COLUMN total_commission REAL DEFAULT 0")

                # 7) Reporting enrichment - trade_fills multi-user + FOREX columns
                c = conn.execute("PRAGMA table_info(trade_fills)")
                cols = [r[1] for r in c.fetchall()]
                if "user_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN user_id TEXT")
                if "bot_instance_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN bot_instance_id TEXT")
                if "broker_account_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN broker_account_id TEXT")
                if "quote_currency" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN quote_currency TEXT DEFAULT 'USDT'")
                if "base_currency" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN base_currency TEXT")
                if "order_id" not in cols:
                    conn.execute("ALTER TABLE trade_fills ADD COLUMN order_id TEXT")



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
                    position_id TEXT,
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
                    
                    -- Always-On Risk (Added)
                    regime_state TEXT DEFAULT 'UNKNOWN',
                    regime_confidence REAL DEFAULT 0.0,
                    exposure_freeze INTEGER DEFAULT 0,
                    kill_switch_state TEXT DEFAULT 'NORMAL',
                    portfolio_risk_budget REAL DEFAULT 0.0,
                    portfolio_risk_used REAL DEFAULT 0.0,
                    
                    -- Strategy outputs (JSON array)
                    strategy_signals_json TEXT,
                    chosen_strategy TEXT,
                    signal TEXT,
                    confidence REAL,
                    reason_codes TEXT DEFAULT 'NONE',
                    
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
                    final_position TEXT,

                    -- Event awareness
                    event_blocked INTEGER DEFAULT 0,
                    event_block_reason TEXT,
                    event_block_event_id TEXT,
                    event_block_type TEXT,
                    event_block_details TEXT
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
                    bot_instance_id TEXT,
                    symbol TEXT,
                    violation_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    details_json TEXT,
                    auto_action_taken TEXT,
                    acknowledged INTEGER DEFAULT 0
                )
                """
            )
            # Migration: add missing columns to existing invariant_violations table
            try:
                c = conn.execute("PRAGMA table_info(invariant_violations)")
                cols = [r[1] for r in c.fetchall()]
                if "bot_instance_id" not in cols:
                    conn.execute("ALTER TABLE invariant_violations ADD COLUMN bot_instance_id TEXT")
                # Helpful indexes for scoped lookups (idempotent)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_invariant_violations_bot_ts "
                    "ON invariant_violations(bot_instance_id, ts)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_invariant_violations_run_ts "
                    "ON invariant_violations(run_id, ts)"
                )
            except Exception:
                pass

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
                    acknowledged_by TEXT,
                    -- Added fields (auto-migrated below)
                    user_id TEXT,
                    bot_instance_id TEXT,
                    broker_account_id TEXT
                )
                """
            )
            
            # Migration: Add missing columns to existing alerts table
            try:
                c = conn.execute("PRAGMA table_info(alerts)")
                cols = [r[1] for r in c.fetchall()]
                if "user_id" not in cols:
                    conn.execute("ALTER TABLE alerts ADD COLUMN user_id TEXT")
                if "bot_instance_id" not in cols:
                    conn.execute("ALTER TABLE alerts ADD COLUMN bot_instance_id TEXT")
                if "broker_account_id" not in cols:
                    conn.execute("ALTER TABLE alerts ADD COLUMN broker_account_id TEXT")
            except Exception:
                pass



            # =========================
            # Benchmarking (Phase 5)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS benchmark_prices (
                    symbol TEXT NOT NULL,
                    date TEXT NOT NULL,
                    close_price REAL NOT NULL,
                    quote_currency TEXT DEFAULT 'USD',
                    asset_class TEXT,
                    provider TEXT,
                    updated_at TEXT,
                    PRIMARY KEY (symbol, date)
                )
                """
            )

            # =========================
            # Analytics Cache (Phase 7)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_cache (
                    cache_key TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    bot_instance_id TEXT,
                    broker_account_id TEXT,
                    timeframe TEXT,
                    computed_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    metrics_json TEXT NOT NULL
                )
                """
            )
            # Add index for faster invalidation by user
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_analytics_cache_user ON analytics_cache(user_id)"
            )

            # =========================
            # Notifications System (Full-Scale)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_preferences (
                    user_id TEXT NOT NULL,
                    channel TEXT NOT NULL,      -- email, telegram, push, in_app
                    category TEXT NOT NULL,     -- trade, risk, system, marketing
                    is_enabled INTEGER DEFAULT 1,
                    min_severity TEXT DEFAULT 'INFO',
                    updated_at TEXT,
                    PRIMARY KEY (user_id, channel, category)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_endpoints (
                    user_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    verified_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, channel)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_jobs (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    channel TEXT NOT NULL,
                    recipient TEXT,
                    template_id TEXT,
                    subject TEXT,
                    body_text TEXT,
                    body_html TEXT,
                    metadata_json TEXT,
                    
                    dedupe_key TEXT,
                    
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    next_retry_at TEXT,
                    
                    processing_token TEXT,
                    processing_started_at TEXT,
                    
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            # Indexes for Job Queue
            conn.execute("CREATE INDEX IF NOT EXISTS idx_notify_jobs_status ON notification_jobs(status, next_retry_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_notify_jobs_dedupe ON notification_jobs(dedupe_key)")

            # Telegram Linking
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_link_codes (
                    code TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
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
            # Marketing / Value Tracking (Added for public.py)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS marketing_sessions (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    ip TEXT,
                    user_agent TEXT,
                    landing_page TEXT,
                    utm_source TEXT,
                    utm_medium TEXT,
                    utm_campaign TEXT,
                    utm_content TEXT,
                    utm_term TEXT,
                    ref_code TEXT,
                    aff_broker TEXT
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS marketing_events (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    page TEXT,
                    metadata_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(session_id) REFERENCES marketing_sessions(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS public_content_blocks (
                    key TEXT NOT NULL,
                    locale TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    updated_at TEXT,
                    PRIMARY KEY(key, locale)
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
            
            # =========================
            # User Strategy Configuration (Added for Orchestrator)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_strategy_configs (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    broker_account_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    activated_at TEXT
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS risk_parameters (
                    config_id TEXT PRIMARY KEY,
                    risk_profile TEXT,
                    portfolio_risk_pct REAL,
                    per_trade_risk_pct REAL,
                    max_margin_usage_pct REAL,
                    max_drawdown_pct REAL,
                    daily_loss_limit_pct REAL,
                    position_sizing_method TEXT,
                    base_position_slots INTEGER,
                    max_position_slots INTEGER,
                    stop_loss_multiplier REAL,
                    take_profit_multiplier REAL,
                    parameters_json TEXT,
                    FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_parameters (
                    config_id TEXT PRIMARY KEY,
                    overrides_json TEXT,
                    FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS protection_state (
                    config_id TEXT PRIMARY KEY,
                    is_protected INTEGER DEFAULT 0,
                    daily_loss_today REAL DEFAULT 0.0,
                    consecutive_losses INTEGER DEFAULT 0,
                    updated_at TEXT,
                    FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
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

            # ✅ SAFE MIGRATION: Ensure decision_traces has new columns
            # This runs AFTER table creation, so table definitely exists or was just created
            try:
                c = conn.execute("PRAGMA table_info(decision_traces)")
                cols = [r[1] for r in c.fetchall()]
                if cols and "regime_state" not in cols:
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN regime_state TEXT DEFAULT 'UNKNOWN'")
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN regime_confidence REAL DEFAULT 0.0")
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN exposure_freeze INTEGER DEFAULT 0")
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN kill_switch_state TEXT DEFAULT 'NORMAL'")
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN portfolio_risk_budget REAL DEFAULT 0.0")
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN portfolio_risk_used REAL DEFAULT 0.0")
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN reason_codes TEXT DEFAULT 'NONE'")           
                if cols and "position_id" not in cols:
                    conn.execute("ALTER TABLE decision_traces ADD COLUMN position_id TEXT")
            except Exception:
                pass

            # =========================
            # Affiliate & Broker Revenue
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS affiliate_settings (
                    id TEXT PRIMARY KEY DEFAULT 'global',
                    referral_commission_rate REAL NOT NULL DEFAULT 15.0,
                    cookie_duration_days INTEGER NOT NULL DEFAULT 30,
                    minimum_payout_amount REAL NOT NULL DEFAULT 100.0,
                    minimum_payout_currency TEXT NOT NULL DEFAULT 'USD',
                    tiered_referrals_enabled INTEGER NOT NULL DEFAULT 0,
                    attribution_model TEXT NOT NULL DEFAULT 'last_click',
                    is_enabled INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.commit()

        finally:
            conn.close()

    # =========================
    # Utility helpers
    # =========================
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
