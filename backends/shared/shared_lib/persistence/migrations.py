from shared_lib.persistence.db import DB
from shared_lib.persistence.event_news_mode import ensure_event_news_mode_schema
from shared_lib.persistence.signals import ensure_signals_schema
from shared_lib.persistence.tradingview import ensure_tradingview_schema


def _add_column_if_missing(conn, table: str, col: str, col_type: str):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {r["name"] for r in rows}
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def _normalize_news_narratives_schema(conn) -> None:
    """
    news_narratives must allow multiple narrative types per cluster.

    Older databases created a unique index on cluster_id only, which crashes
    ingestion whenever a cluster legitimately matches more than one narrative.
    The durable uniqueness rule is (cluster_id, narrative_type).
    """
    rows = conn.execute("PRAGMA table_info(news_narratives)").fetchall()
    if not rows:
        return

    existing_cols = {r["name"] for r in rows}
    if "updated_at" not in existing_cols:
        conn.execute("ALTER TABLE news_narratives ADD COLUMN updated_at TEXT")

    index_rows = conn.execute("PRAGMA index_list(news_narratives)").fetchall()
    for idx in index_rows:
        idx_name = idx["name"]
        is_unique = bool(idx["unique"])
        idx_cols = [
            r["name"]
            for r in conn.execute(f"PRAGMA index_info({idx_name})").fetchall()
        ]
        if is_unique and idx_cols == ["cluster_id"]:
            if idx_name.startswith("sqlite_autoindex"):
                _rebuild_news_narratives_table(conn)
                break
            conn.execute(f"DROP INDEX IF EXISTS {idx_name}")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nn_cluster "
        "ON news_narratives(cluster_id)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_nn_cluster_type "
        "ON news_narratives(cluster_id, narrative_type)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nn_type "
        "ON news_narratives(narrative_type)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nn_severity "
        "ON news_narratives(severity_level)"
    )


def _rebuild_news_narratives_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_narratives_new (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id           INTEGER NOT NULL REFERENCES news_clusters(id),
            narrative_type       TEXT NOT NULL,
            narrative_confidence REAL NOT NULL,
            severity_level       TEXT NOT NULL DEFAULT 'LOW',
            matched_keywords     TEXT,
            created_at           TEXT NOT NULL,
            updated_at           TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO news_narratives_new
            (id, cluster_id, narrative_type, narrative_confidence,
             severity_level, matched_keywords, created_at, updated_at)
        SELECT
            MIN(id) AS id,
            cluster_id,
            narrative_type,
            MAX(narrative_confidence) AS narrative_confidence,
            COALESCE(MAX(severity_level), 'LOW') AS severity_level,
            MAX(matched_keywords) AS matched_keywords,
            MIN(created_at) AS created_at,
            MAX(COALESCE(updated_at, created_at)) AS updated_at
        FROM news_narratives
        GROUP BY cluster_id, narrative_type
        """
    )
    conn.execute("DROP TABLE news_narratives")
    conn.execute("ALTER TABLE news_narratives_new RENAME TO news_narratives")


def migrate(db_path: str | DB = None):
    db = db_path if isinstance(db_path, DB) else DB(path=db_path)
    ensure_tradingview_schema(db)
    ensure_signals_schema(db)
    with db.connect() as conn:
        # 1) Ensure table exists (pure SQL only)
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS strategy_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            broker_id TEXT NOT NULL,
            account_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            trades INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            net_pnl REAL NOT NULL DEFAULT 0,
            gross_pnl REAL NOT NULL DEFAULT 0,
            fees REAL NOT NULL DEFAULT 0,
            avg_slippage REAL NOT NULL DEFAULT 0,
            avg_r REAL NOT NULL DEFAULT 0,
            max_drawdown REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        """
        )

        # 2) Apply ALTER TABLE upgrades (Python calls happen OUTSIDE SQL strings)
        _add_column_if_missing(conn, "trade_fills", "strategy", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "strategy_version", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "timeframe", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "confidence", "REAL")
        _add_column_if_missing(conn, "trade_fills", "broker_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "account_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "asset_class", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "exit_reason", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "mfe_pct", "REAL")
        _add_column_if_missing(conn, "trade_fills", "mae_pct", "REAL")
        _add_column_if_missing(conn, "trade_fills", "exit_regime", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "exit_regime_confidence", "REAL")
        # 2a-ext) Extended execution quality + observability columns for trade_fills
        _add_column_if_missing(conn, "trade_fills", "user_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "bot_instance_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "broker_account_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "quote_currency", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "base_currency", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "order_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "initiator_type", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "trigger_source", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "position_phase", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "time_in_trade_sec", "REAL")
        _add_column_if_missing(conn, "trade_fills", "sl_at_exit", "REAL")
        _add_column_if_missing(conn, "trade_fills", "tp_at_exit", "REAL")
        _add_column_if_missing(conn, "trade_fills", "market_price_used", "REAL")
        _add_column_if_missing(conn, "trade_fills", "price_source", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "opposite_signal_detected", "INTEGER")
        _add_column_if_missing(conn, "trade_fills", "ensemble_decision", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "risk_force_close", "INTEGER")
        _add_column_if_missing(conn, "trade_fills", "sync_state_before", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "sync_state_after", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "close_order_type", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "broker_response", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "expected_close", "INTEGER")

        # 2b) decision_traces frozen indicator snapshot columns (ML feature capture)
        _add_column_if_missing(conn, "decision_traces", "adx", "REAL")
        _add_column_if_missing(conn, "decision_traces", "atr_pct", "REAL")
        _add_column_if_missing(conn, "decision_traces", "ma_slope", "REAL")
        _add_column_if_missing(conn, "decision_traces", "compression_ratio", "REAL")
        _add_column_if_missing(conn, "decision_traces", "breakout_pressure", "REAL")
        _add_column_if_missing(conn, "decision_traces", "buy_score", "REAL")
        _add_column_if_missing(conn, "decision_traces", "sell_score", "REAL")
        _add_column_if_missing(conn, "decision_traces", "threshold", "REAL")
        _add_column_if_missing(conn, "decision_traces", "active_strategy_count", "INTEGER")
        _add_column_if_missing(conn, "decision_traces", "htf_opposed", "INTEGER")
        _add_column_if_missing(conn, "decision_traces", "aggressiveness_score", "REAL")
        _add_column_if_missing(conn, "decision_traces", "confidence_gate_modifier", "REAL")
        _add_column_if_missing(conn, "decision_traces", "size_multiplier", "REAL")
        _add_column_if_missing(conn, "decision_traces", "rolling_win_rate", "REAL")
        _add_column_if_missing(conn, "decision_traces", "rolling_expectancy", "REAL")
        _add_column_if_missing(conn, "decision_traces", "loss_streak", "INTEGER")

        # 2c) ML inference columns (Step 5D-2) — shadow + live scoring
        _add_column_if_missing(conn, "decision_traces", "ml_score", "REAL")
        _add_column_if_missing(conn, "decision_traces", "ml_action", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "ml_model_version", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "ml_threshold", "REAL")

        # 2d) Phase 4 Fix 1 — position_id persistence in bot_symbol_state
        # Enables OPEN→CLOSE fill linkage to survive restarts.
        _add_column_if_missing(conn, "bot_symbol_state", "position_id", "TEXT")

        # 2e) Observability Fix 5 — bot_instance_id on decision_traces
        # Enables per-instance auditing and multi-user scope verification.
        _add_column_if_missing(conn, "decision_traces", "bot_instance_id", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "position_id", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "allocation_mode", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "base_size", "REAL")
        _add_column_if_missing(conn, "decision_traces", "final_size", "REAL")
        _add_column_if_missing(conn, "decision_traces", "final_qty", "REAL")
        _add_column_if_missing(conn, "decision_traces", "min_qty", "REAL")
        _add_column_if_missing(conn, "decision_traces", "min_notional", "REAL")
        _add_column_if_missing(conn, "decision_traces", "submit_attempted", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "decision_traces", "broker_response", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "fill_recorded", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "decision_traces", "position_opened", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "decision_traces", "rejection_reason", "TEXT")

        # 3) Ensure signal_outcomes exists
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            broker_id TEXT NOT NULL,
            account_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            confidence REAL NOT NULL,
            outcome INTEGER NOT NULL, -- 1 win / 0 loss
            pnl REAL NOT NULL,
            r_multiple REAL,
            opened_at TEXT,
            closed_at TEXT,
            created_at TEXT NOT NULL
        );
        """
        )
        
        # 4) Add trade_count to daily_state
        _add_column_if_missing(conn, "daily_state", "trade_count", "INTEGER DEFAULT 0")
        # F-9: persist consecutive loss state so daily counter survives restarts
        _add_column_if_missing(conn, "daily_state", "consecutive_losses", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "daily_state", "consec_loss_cooldown_until_ms", "INTEGER DEFAULT 0")
        
        # 5) Create weekly_snapshots table
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS weekly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start_date TEXT NOT NULL UNIQUE,
            start_equity REAL NOT NULL,
            peak_equity REAL NOT NULL,
            low_equity REAL NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
        )
        
        # 6) Create monthly_snapshots table
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS monthly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_start_date TEXT NOT NULL UNIQUE,
            start_equity REAL NOT NULL,
            peak_equity REAL NOT NULL,
            low_equity REAL NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
        )

        # 6b) Create equity_snapshots table (Missing dependency for analytics_service)
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS equity_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT NOT NULL,
            equity REAL NOT NULL,
            bot_instance_id TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
        )
        # Add index for timestamp_utc
        conn.execute("CREATE INDEX IF NOT EXISTS idx_equity_snapshots_time ON equity_snapshots(timestamp_utc)")
        
        # Migration: Rename total_equity to equity if it exists
        try:
            c = conn.execute("PRAGMA table_info(equity_snapshots)")
            cols = [r[1] for r in c.fetchall()]
            if "total_equity" in cols and "equity" not in cols:
                # SQLite doesn't support ALTER COLUMN, so we need to recreate the table
                # But first, check if there's data
                row_count = conn.execute("SELECT COUNT(*) FROM equity_snapshots").fetchone()[0]
                if row_count > 0:
                    # Copy total_equity data to equity column (add equity column first)
                    conn.execute("ALTER TABLE equity_snapshots ADD COLUMN equity REAL")
                    conn.execute("UPDATE equity_snapshots SET equity = total_equity WHERE equity IS NULL")
                    # Make equity NOT NULL by setting default for any remaining nulls
                    conn.execute("UPDATE equity_snapshots SET equity = 0.0 WHERE equity IS NULL")
        except Exception:
            pass

        # E-2 Section E migration: Extended close performance fields for trade_fills
        _add_column_if_missing(conn, "trade_fills", "gross_pnl", "REAL")
        _add_column_if_missing(conn, "trade_fills", "total_fees", "REAL")
        _add_column_if_missing(conn, "trade_fills", "funding_fees", "REAL")
        _add_column_if_missing(conn, "trade_fills", "net_pnl", "REAL")
        _add_column_if_missing(conn, "trade_fills", "net_pnl_percent", "REAL")
        _add_column_if_missing(conn, "trade_fills", "entry_fee", "REAL")
        _add_column_if_missing(conn, "trade_fills", "exit_fee", "REAL")
        _add_column_if_missing(conn, "trade_fills", "fees_estimated", "INTEGER")
        _add_column_if_missing(conn, "trade_fills", "slippage_estimated", "INTEGER")

        # E-2 indexes for analytics and monitoring queries (idempotent — skip if column missing)
        for _idx_sql in [
            ("CREATE INDEX IF NOT EXISTS idx_trade_fills_bot_symbol_time "
             "ON trade_fills(bot_instance_id, symbol, timestamp_utc)"),
            ("CREATE INDEX IF NOT EXISTS idx_trade_fills_bot_action_time "
             "ON trade_fills(bot_instance_id, action, timestamp_utc)"),
            ("CREATE INDEX IF NOT EXISTS idx_trade_fills_decision_trace "
             "ON trade_fills(trace_id)"),
        ]:
            try:
                conn.execute(_idx_sql)
            except Exception:
                pass  # Column may not exist in minimal test schema



        
        # 7) Ensure events table has all required columns for audit.py
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT NOT NULL,
            run_id TEXT,
            cycle_id TEXT,
            symbol TEXT,
            event_type TEXT,
            action TEXT,
            details_json TEXT
        );
        """
        )
        # Add columns used by audit.py (backward compat)
        _add_column_if_missing(conn, "events", "timestamp_utc", "TEXT")
        _add_column_if_missing(conn, "events", "details_json", "TEXT")
        _add_column_if_missing(conn, "events", "action", "TEXT")
        _add_column_if_missing(conn, "events", "event_type", "TEXT")
        _add_column_if_missing(conn, "events", "cycle_id", "TEXT")
        _add_column_if_missing(conn, "events", "symbol", "TEXT")
        _add_column_if_missing(conn, "events", "run_id", "TEXT")

        # --- AUTH & IDENTITY TABLES (Merged from migrate_identity_v2 and migrate_auth_security) ---

        # 7b) Admins Table (Identity Decoupling)
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
        
        # 7c) Admin Sessions
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_sessions_admin_id ON admin_sessions(admin_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_sessions_refresh_hash ON admin_sessions(refresh_token_hash)")

        # 8) Users Table Enhancements
        _add_column_if_missing(conn, "users", "status", "TEXT DEFAULT 'pending_verification'")
        _add_column_if_missing(conn, "users", "role", "TEXT DEFAULT 'user'")
        _add_column_if_missing(conn, "users", "totp_secret", "TEXT")
        _add_column_if_missing(conn, "users", "is_2fa_enabled", "BOOLEAN DEFAULT 0")
        _add_column_if_missing(conn, "users", "name", "TEXT")

        # Ensure existing users are active/user by default if null
        conn.execute("UPDATE users SET status = 'active' WHERE status IS NULL")
        conn.execute("UPDATE users SET role = 'user' WHERE role IS NULL")

        # 9) Email Verifications
        conn.execute("""
            CREATE TABLE IF NOT EXISTS email_verifications (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                code_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                used_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_email_verifications_user_id ON email_verifications(user_id)")

        # 10) Auth Sessions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                refresh_token_hash TEXT NOT NULL,
                device TEXT,
                ip TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT,
                rotated_from TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_id ON auth_sessions(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_refresh_hash ON auth_sessions(refresh_token_hash)")

        # 11) Password Resets
        conn.execute("""
            CREATE TABLE IF NOT EXISTS password_resets (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                code_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                used_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_password_resets_user_id ON password_resets(user_id)")

        # 12) Login Attempts (Rate Limiting)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                ip TEXT,
                success INTEGER DEFAULT 0,
                attempted_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_login_attempts_email ON login_attempts(email)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_login_attempts_time ON login_attempts(attempted_at)")

        # 13) Auth Audit Log
        conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_audit_log (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                user_id TEXT,
                email TEXT,
                ip TEXT,
                details TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_audit_event_type ON auth_audit_log(event_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_audit_user_id ON auth_audit_log(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_audit_created_at ON auth_audit_log(created_at)")

        # 14) Broker Connection Tables (Page 4)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS broker_accounts (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                broker_id TEXT NOT NULL,
                market_type TEXT NOT NULL,
                label TEXT,
                status TEXT NOT NULL,
                environment TEXT DEFAULT 'live',
                account_type TEXT,
                capabilities JSON,
                masked_key TEXT,
                last_validated_at TEXT,
                last_error_code TEXT,
                last_error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_broker_accounts_user_id ON broker_accounts(user_id)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS broker_credentials (
                account_id TEXT PRIMARY KEY,
                encrypted_blob TEXT NOT NULL,
                key_metadata TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS broker_audit_log (
                id TEXT PRIMARY KEY,
                broker_account_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                details_json TEXT,
                ip_address TEXT,
                timestamp_utc TEXT NOT NULL,
                FOREIGN KEY(broker_account_id) REFERENCES broker_accounts(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_broker_audit_account_id ON broker_audit_log(broker_account_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_broker_audit_account_id ON broker_audit_log(broker_account_id)")

        # 15) Subscription & Billing (Page 5)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                status TEXT NOT NULL,
                provider_sub_id TEXT,
                current_period_end TEXT,
                cancel_at_period_end INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount REAL,
                currency TEXT,
                status TEXT NOT NULL,
                period_start TEXT,
                period_end TEXT,
                hosted_invoice_url TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_user_id ON invoices(user_id)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS pricing_intents (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                plan_id TEXT NOT NULL,
                session_id TEXT,
                created_at TEXT NOT NULL
            )
        """)
        _add_column_if_missing(conn, "pricing_intents", "session_id", "TEXT")

        # 16) Onboarding Wizard (Page 6)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS onboarding_profiles (
                user_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                current_step TEXT,
                data_json TEXT,
                recommended_defaults TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            )
        """)
        # 17) Strategy Management Tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategies (
                id TEXT PRIMARY KEY,
                owner_id TEXT,
                visibility TEXT NOT NULL,
                status TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                market_types TEXT, -- JSON
                timeframes TEXT, -- JSON
                tags TEXT, -- JSON
                entitlement_tier TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategies_owner_id ON strategies(owner_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategies_visibility ON strategies(visibility)")

        _add_column_if_missing(conn, "strategies", "recommended_risk_style", "TEXT DEFAULT 'balanced'")
        _add_column_if_missing(conn, "strategies", "constraints_json", "TEXT DEFAULT '{}'")
        _add_column_if_missing(conn, "strategies", "metrics_json", "TEXT DEFAULT '{}'")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_versions (
                id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                version_number INTEGER NOT NULL,
                spec_json TEXT,
                param_schema_json TEXT,
                changelog TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (strategy_id) REFERENCES strategies(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy_id ON strategy_versions(strategy_id)")

        # 18) User Strategy Configurations (User-specific strategy instances)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_strategy_configs (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                broker_account_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL,  -- draft, active, paused, archived
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                activated_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(broker_account_id) REFERENCES broker_accounts(id),
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_strategy_configs_user_id ON user_strategy_configs(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_strategy_configs_broker_account ON user_strategy_configs(broker_account_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_strategy_configs_status ON user_strategy_configs(status)")

        # 19) Risk Parameters (per configuration)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS risk_parameters (
                config_id TEXT PRIMARY KEY,
                risk_profile TEXT NOT NULL,  -- conservative, balanced, aggressive, custom
                portfolio_risk_pct REAL NOT NULL,
                per_trade_risk_pct REAL NOT NULL,
                max_margin_usage_pct REAL NOT NULL,
                max_drawdown_pct REAL NOT NULL,
                daily_loss_limit_pct REAL NOT NULL,
                position_sizing_method TEXT NOT NULL,  -- fixed, risk_based, kelly, atr
                base_position_slots INTEGER NOT NULL,
                max_position_slots INTEGER NOT NULL,
                stop_loss_multiplier REAL,
                take_profit_multiplier REAL,
                parameters_json TEXT,  -- Additional custom parameters
                FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
            )
        """)

        # 20) Strategy Parameters (strategy-specific overrides per configuration)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_parameters (
                config_id TEXT PRIMARY KEY,
                overrides_json TEXT NOT NULL,  -- Strategy-specific parameter overrides
                FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
            )
        """)

        # 21) Protection State (account protection tracking)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS protection_state (
                config_id TEXT PRIMARY KEY,
                is_protected INTEGER DEFAULT 0,
                protection_reason TEXT,
                daily_loss_today REAL DEFAULT 0.0,
                peak_equity REAL,
                current_drawdown_pct REAL DEFAULT 0.0,
                consecutive_losses INTEGER DEFAULT 0,
                last_loss_at TEXT,
                cool_down_until TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
            )
        """)
        
        # 22) Decision Logs (Comprehensive audit trail of the decision pipeline)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_logs (
                id TEXT PRIMARY KEY,
                config_id TEXT NOT NULL,
                run_id TEXT,
                symbol TEXT NOT NULL,
                strategy_signal_json TEXT,  -- Raw strategy output
                risk_gate_decision_json TEXT, -- Pre-trade gate result
                sizing_decision_json TEXT, -- Sizing engine calculation
                protection_decision_json TEXT, -- Protective order validation
                final_action TEXT, -- EXECUTE, BLOCK, SKIP
                execution_result_json TEXT, -- If executed, the result
                created_at TEXT NOT NULL,
                FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_logs_config_id ON decision_logs(config_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_logs_created_at ON decision_logs(created_at)")

        # Updates to User Strategy Configs
        _add_column_if_missing(conn, "user_strategy_configs", "symbols_json", "TEXT") # specific symbols for this config
        _add_column_if_missing(conn, "user_strategy_configs", "timeframes_json", "TEXT")
        _add_column_if_missing(conn, "user_strategy_configs", "allocation_type", "TEXT DEFAULT 'percent_balance'") # percent_balance or fixed_amount
        _add_column_if_missing(conn, "user_strategy_configs", "allocation_value", "REAL DEFAULT 1.0") # 1.0 = 100% or fixed USDT amount

        # 23) Bot Instances (Multi-user execution contexts)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_instances (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                broker_account_id TEXT NOT NULL,
                market_type TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                config_id TEXT NOT NULL,
                risk_profile_id TEXT NOT NULL,
                symbols_json TEXT NOT NULL,
                timeframes_json TEXT NOT NULL,
                allocation_type TEXT NOT NULL,
                allocation_value REAL NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                stopped_at TEXT,
                last_run_at TEXT,
                last_error TEXT,
                total_trades INTEGER DEFAULT 0,
                active_positions INTEGER DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(broker_account_id) REFERENCES broker_accounts(id),
                FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_instances_user_id ON bot_instances(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_instances_status ON bot_instances(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_instances_broker_account ON bot_instances(broker_account_id)")

        # Section F-3 — Daily close validation reports (paper/testnet only).
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_close_validation_reports (
                id TEXT PRIMARY KEY,
                bot_instance_id TEXT NOT NULL,
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
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dcv_bot_time ON daily_close_validation_reports(bot_instance_id, validated_at)"
        )

        # Auto Pilot migration: Add risk_level column for internal risk management
        _add_column_if_missing(conn, "bot_instances", "risk_level", "TEXT DEFAULT 'balanced'")
        
        # Auto Pilot migration: Make config_id and risk_profile_id optional (allow NULL)
        # Note: SQLite doesn't support ALTER COLUMN, so these changes apply to new rows
        # Existing rows will keep their values
        _add_column_if_missing(conn, "bot_instances", "capital_allocation", "REAL")
        _add_column_if_missing(conn, "bot_instances", "capital_allocation_type", "TEXT DEFAULT 'fixed_amount'")
        _add_column_if_missing(conn, "bot_instances", "symbols_json", "TEXT DEFAULT '[]'")
        _add_column_if_missing(conn, "bot_instances", "timeframes_json", "TEXT DEFAULT '[\"15m\"]'")
        _add_column_if_missing(conn, "bot_instances", "allocation_type", "TEXT DEFAULT 'fixed_amount'")
        _add_column_if_missing(conn, "bot_instances", "allocation_value", "REAL DEFAULT 0.0")
        _add_column_if_missing(conn, "bot_instances", "mode", "TEXT DEFAULT 'paper'")
        _add_column_if_missing(conn, "bot_instances", "total_trades", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "bot_instances", "active_positions", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "bot_instances", "last_run_at", "TEXT")
        _add_column_if_missing(conn, "bot_instances", "last_error", "TEXT")
        _add_column_if_missing(conn, "bot_instances", "started_at", "TEXT")
        _add_column_if_missing(conn, "bot_instances", "stopped_at", "TEXT")

        # Section F — Product safety: user-facing bot health status (idempotent).
        _add_column_if_missing(conn, "bot_instances", "bot_health_status", "TEXT DEFAULT 'UNKNOWN'")
        _add_column_if_missing(conn, "bot_instances", "bot_health_message", "TEXT")
        _add_column_if_missing(conn, "bot_instances", "bot_health_reason_code", "TEXT")
        _add_column_if_missing(conn, "bot_instances", "bot_health_recommended_action", "TEXT")
        _add_column_if_missing(conn, "bot_instances", "bot_health_updated_at", "TEXT")
        _add_column_if_missing(conn, "bot_instances", "last_warning", "TEXT")

        # 23b) Bot Daily/Symbol State (Multi-user persistence)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_daily_state (
                bot_instance_id TEXT NOT NULL,
                day TEXT NOT NULL,
                realized_pnl REAL DEFAULT 0.0,
                kill INTEGER DEFAULT 0,
                trade_count INTEGER DEFAULT 0,
                consecutive_losses INTEGER DEFAULT 0,
                consec_loss_cooldown_until_ms INTEGER DEFAULT 0,
                last_updated_at TEXT NOT NULL,
                PRIMARY KEY (bot_instance_id, day),
                FOREIGN KEY(bot_instance_id) REFERENCES bot_instances(id)
            )
        """)
        # F-9: add consecutive loss columns to existing bot_daily_state rows
        _add_column_if_missing(conn, "bot_daily_state", "consecutive_losses", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "bot_daily_state", "consec_loss_cooldown_until_ms", "INTEGER DEFAULT 0")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_symbol_state (
                bot_instance_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                position TEXT DEFAULT 'NONE',
                entry_price REAL,
                last_signal TEXT,
                last_action TEXT,
                last_checked_ms INTEGER DEFAULT 0,
                adds INTEGER DEFAULT 0,
                last_trade_ms INTEGER DEFAULT 0,
                last_stop_ms INTEGER DEFAULT 0,
                pending_open TEXT DEFAULT 'NONE',
                entry_qty REAL DEFAULT 0.0,
                last_user_trade_id INTEGER DEFAULT 0,
                reentry_confirm_signal TEXT DEFAULT 'NONE',
                reentry_confirm_count INTEGER DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (bot_instance_id, symbol),
                FOREIGN KEY(bot_instance_id) REFERENCES bot_instances(id)
            )
        """)

        # 24) Official Strategies Cache (Catalog metadata cache)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS official_strategies_cache (
                strategy_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                description TEXT,
                strategy_type TEXT,
                market_types_json TEXT,
                indicators_json TEXT,
                params_schema_json TEXT,
                risk_compatibility_json TEXT,
                default_risk_profiles_json TEXT,
                performance_hints_json TEXT,
                updated_at TEXT NOT NULL
            )
        """)

        # =====================================================================
        # BACKTESTING INFRASTRUCTURE (FOREX-READY, CRYPTO-FIRST)
        # =====================================================================

        # 25) Backtest Runs - Metadata for each backtest execution
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                
                -- Strategy configuration
                strategy_id TEXT NOT NULL,
                strategy_version TEXT,
                symbols_json TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                
                -- Multi-asset support (forex-ready)
                market_type TEXT NOT NULL DEFAULT 'crypto',
                base_currency TEXT,
                quote_currency TEXT NOT NULL DEFAULT 'USDT',
                
                -- Capital & risk settings
                initial_capital REAL NOT NULL,
                leverage INTEGER,
                strategy_params_json TEXT,
                risk_params_json TEXT,
                slippage_bps REAL DEFAULT 10.0,
                fee_bps REAL DEFAULT 6.0,
                
                -- Data source tracking (reproducibility)
                data_source TEXT NOT NULL,
                data_version TEXT,
                seed INTEGER,
                
                -- Results
                status TEXT NOT NULL DEFAULT 'pending',
                progress_pct REAL DEFAULT 0.0,
                total_trades INTEGER DEFAULT 0,
                win_rate REAL,
                net_pnl REAL,
                gross_pnl REAL,
                total_fees REAL,
                max_drawdown REAL,
                sharpe_ratio REAL,
                sortino_ratio REAL,
                calmar_ratio REAL,
                profit_factor REAL,
                error_message TEXT,
                
                -- Timestamps
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_user_created ON backtest_runs(user_id, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_status ON backtest_runs(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy ON backtest_runs(strategy_id)")

        # 26) Backtest Jobs - Worker queue for async backtest execution
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_jobs (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                run_id TEXT NOT NULL,
                
                -- Job configuration
                status TEXT NOT NULL DEFAULT 'pending',
                priority INTEGER DEFAULT 5,
                config_json TEXT NOT NULL,
                
                -- Token claiming (SQLite-safe concurrency)
                processing_token TEXT,
                processing_started_at TEXT,
                attempts INTEGER DEFAULT 0,
                next_retry_at TEXT,
                last_error TEXT,
                
                -- Progress tracking
                progress_pct REAL DEFAULT 0.0,
                current_candle INTEGER,
                total_candles INTEGER,
                
                -- Results
                result_json TEXT,
                
                -- Timestamps
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(run_id) REFERENCES backtest_runs(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_jobs_status ON backtest_jobs(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_jobs_priority ON backtest_jobs(priority, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_jobs_run_id ON backtest_jobs(run_id)")

        # 27) Backtest Fills - Simulated trade executions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                
                -- Trade details
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                fill_price REAL NOT NULL,
                fee_usdt REAL DEFAULT 0.0,
                slippage_usdt REAL DEFAULT 0.0,
                pnl REAL,
                
                -- Context
                strategy TEXT,
                confidence REAL,
                position_state TEXT,
                trade_type TEXT,
                
                -- Multi-currency support
                quote_currency TEXT DEFAULT 'USDT',
                
                FOREIGN KEY(run_id) REFERENCES backtest_runs(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_fills_run_id ON backtest_fills(run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_fills_symbol ON backtest_fills(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_fills_timestamp ON backtest_fills(timestamp_utc)")

        # 28) Backtest Equity Curve - Equity snapshots during backtest
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_equity_curve (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                
                -- Equity metrics
                equity REAL NOT NULL,
                balance REAL NOT NULL,
                unrealized_pnl REAL DEFAULT 0.0,
                realized_pnl REAL DEFAULT 0.0,
                
                -- Risk metrics
                drawdown_pct REAL,
                drawdown_usdt REAL,
                peak_equity REAL,
                
                -- Multi-currency
                quote_currency TEXT DEFAULT 'USDT',
                
                FOREIGN KEY(run_id) REFERENCES backtest_runs(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_equity_run_id ON backtest_equity_curve(run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_equity_timestamp ON backtest_equity_curve(timestamp_utc)")

        # 29) Backtest Decision Logs - Full decision pipeline audit trail
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_decision_logs (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                
                -- Decision pipeline
                strategy_signal_json TEXT,
                risk_gate_decision_json TEXT,
                sizing_decision_json TEXT,
                protection_decision_json TEXT,
                final_action TEXT,
                execution_result_json TEXT,
                
                -- Context
                candle_index INTEGER,
                
                FOREIGN KEY(run_id) REFERENCES backtest_runs(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_decision_logs_run_id ON backtest_decision_logs(run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_decision_logs_symbol ON backtest_decision_logs(symbol)")

        # 30) Historical Candles Cache - Optional performance optimization
        conn.execute("""
            CREATE TABLE IF NOT EXISTS historical_candles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Identifier
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                
                -- OHLCV data
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                quote_volume REAL,
                trades INTEGER,
                
                -- Multi-asset support
                market_type TEXT NOT NULL DEFAULT 'crypto',
                base_currency TEXT,
                quote_currency TEXT NOT NULL DEFAULT 'USDT',
                
                -- Data provenance
                data_source TEXT NOT NULL,
                data_version TEXT,
                fetched_at TEXT,
                
                UNIQUE(symbol, interval, open_time, data_source, market_type)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_candles_symbol_interval ON historical_candles(symbol, interval, open_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_candles_market_type ON historical_candles(market_type, symbol)")

        # 31) Backtest Performance Summary - Aggregated metrics per symbol
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                
                -- Trade statistics
                total_trades INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                win_rate REAL,
                
                -- PnL metrics
                gross_pnl REAL,
                net_pnl REAL,
                total_fees REAL,
                avg_win REAL,
                avg_loss REAL,
                largest_win REAL,
                largest_loss REAL,
                
                -- Risk metrics
                max_drawdown REAL,
                avg_trade_duration_minutes REAL,
                sharpe_ratio REAL,
                profit_factor REAL,
                expectancy REAL,
                
                -- Multi-currency
                quote_currency TEXT DEFAULT 'USDT',
                
                FOREIGN KEY(run_id) REFERENCES backtest_runs(id),
                UNIQUE(run_id, symbol)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_performance_run_id ON backtest_performance(run_id)")

        # 32) Entry Protection — fail-safe duplicate open prevention
        # Persists entry-intent lifecycle so protection survives restarts.
        # UNIQUE(bot_id, symbol, side) is the hard atomic lock.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_entries (
                id                  TEXT PRIMARY KEY,
                bot_id              TEXT NOT NULL,
                symbol              TEXT NOT NULL,
                side                TEXT NOT NULL,
                state               TEXT NOT NULL DEFAULT 'PENDING_OPEN',
                submit_state        TEXT NOT NULL DEFAULT 'NOT_SUBMITTED',
                client_order_id     TEXT NOT NULL,
                broker_order_id     TEXT,
                intent_key          TEXT,
                intended_notional   REAL NOT NULL DEFAULT 0.0,
                sized_notional      REAL,
                sized_qty           REAL,
                submitted_notional  REAL,
                submitted_qty       REAL,
                filled_notional     REAL,
                filled_qty          REAL,
                reference_price     REAL,
                submitted_at_ms     INTEGER NOT NULL DEFAULT 0,
                confirmed_at_ms     INTEGER,
                last_reconcile_at_ms INTEGER,
                flat_confirmations  INTEGER NOT NULL DEFAULT 0,
                cycle_id            TEXT,
                error_reason        TEXT,
                created_at          TEXT DEFAULT (datetime('now')),
                updated_at          TEXT DEFAULT (datetime('now')),

                UNIQUE(bot_id, symbol, side)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pe_bot_symbol "
            "ON pending_entries(bot_id, symbol)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entry_protection_events (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms              INTEGER NOT NULL,
                event_type         TEXT NOT NULL,
                bot_id             TEXT NOT NULL,
                symbol             TEXT NOT NULL,
                side               TEXT NOT NULL,
                state              TEXT,
                submit_state       TEXT,
                client_order_id    TEXT,
                intent_key         TEXT,
                intended_notional  REAL,
                sized_notional     REAL,
                submitted_notional REAL,
                filled_notional    REAL,
                reference_price    REAL,
                protected_exposure REAL,
                max_exposure_limit REAL,
                reason             TEXT,
                cycle_id           TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_epe_bot_symbol_ts "
            "ON entry_protection_events(bot_id, symbol, ts_ms)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_epe_event_ts "
            "ON entry_protection_events(event_type, ts_ms)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_epe_client_ts "
            "ON entry_protection_events(client_order_id, ts_ms)"
        )

        # =====================================================================
        # 31b) ML admin readiness indexes
        #
        # These power the admin ML training gate joins:
        # - trade_fills OPEN/CLOSE scans by action / timestamp
        # - position lifecycle pairing by position_id
        # - decision trace linkage by run_id + cycle_id + symbol
        # =====================================================================
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_fills_action_time "
            "ON trade_fills(action, timestamp_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_fills_position_action "
            "ON trade_fills(position_id, action, timestamp_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_fills_run_cycle_symbol_action "
            "ON trade_fills(run_id, cycle_id, symbol, action)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_decision_traces_run_cycle_symbol "
            "ON decision_traces(run_id, cycle_id, symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_decision_traces_ts "
            "ON decision_traces(ts)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_decision_traces_ml_action "
            "ON decision_traces(ml_action, ts)"
        )

        # =====================================================================
        # 31c) Admin ML action / validation persistence
        # =====================================================================
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_validation_history (
                model_version TEXT PRIMARY KEY,
                training_date TEXT,
                dataset_used TEXT,
                train_rows INTEGER,
                test_rows INTEGER,
                train_auc REAL,
                test_auc REAL,
                validation_method TEXT,
                notes TEXT,
                verdict TEXT NOT NULL,
                deployed_mode TEXT NOT NULL DEFAULT 'not_deployed',
                metadata_path TEXT,
                validation_path TEXT,
                source_payload_json TEXT,
                synced_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_action_runs (
                id TEXT PRIMARY KEY,
                action_key TEXT NOT NULL,
                requested_by_admin_id TEXT,
                requested_by_email TEXT,
                confirmation_phrase TEXT,
                note TEXT,
                status TEXT NOT NULL,
                reason TEXT,
                supported INTEGER NOT NULL DEFAULT 0,
                readiness_snapshot_json TEXT,
                request_payload_json TEXT,
                dataset_path TEXT,
                target_model_version TEXT,
                log_path TEXT,
                command_json TEXT,
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                result_json TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_validation_history_training_date "
            "ON ml_validation_history(training_date DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_action_runs_action_created "
            "ON ml_action_runs(action_key, created_at DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_runtime_status (
                scope_key TEXT PRIMARY KEY,
                bot_instance_id TEXT,
                enabled INTEGER NOT NULL DEFAULT 0,
                shadow_mode INTEGER NOT NULL DEFAULT 0,
                loaded INTEGER NOT NULL DEFAULT 0,
                model_version TEXT,
                model_path TEXT,
                metadata_path TEXT,
                encoders_path TEXT,
                threshold REAL,
                hard_block_floor REAL,
                contract_version TEXT,
                schema_hash TEXT,
                last_score_timestamp TEXT,
                last_update_timestamp TEXT NOT NULL,
                load_error TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_runtime_status_updated "
            "ON ml_runtime_status(last_update_timestamp DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_action_runs_status_created "
            "ON ml_action_runs(status, created_at DESC)"
        )

        # Additional columns and indexes for backtest_fills
        _add_column_if_missing(conn, "backtest_fills", "metadata_json", "TEXT")
        
        # Combined index for time-range queries (as per spec)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_fills_run_time ON backtest_fills(run_id, timestamp_utc)")

        # Additional columns and indexes for backtest_equity_curve
        _add_column_if_missing(conn, "backtest_equity_curve", "metadata_json", "TEXT")
        
        # Combined index for time-range queries (as per spec)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_equity_run_time ON backtest_equity_curve(run_id, timestamp_utc)")

        # =====================================================================
        # 32) Position Lifecycle State — persist PositionManager advanced exit phases
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_lifecycle_state (
                bot_instance_id     TEXT NOT NULL,
                symbol              TEXT NOT NULL,
                position_id         TEXT,
                phase               TEXT NOT NULL DEFAULT 'SEEKING_TP1',
                original_stop       REAL,
                current_stop        REAL,
                original_tp1        REAL,
                original_tp2        REAL,
                is_break_even       INTEGER NOT NULL DEFAULT 0,
                tp1_hit             INTEGER NOT NULL DEFAULT 0,
                trailing_active     INTEGER NOT NULL DEFAULT 0,
                highest_since_entry REAL,
                lowest_since_entry  REAL,
                entry_qty_remaining REAL,
                sl_order_id         TEXT,
                tp_order_id         TEXT,
                exchange_position_active INTEGER,
                reconciliation_status TEXT,
                reconciliation_reason TEXT,
                last_reconciled_at  TEXT,
                updated_at          TEXT NOT NULL,
                PRIMARY KEY (bot_instance_id, symbol)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pls_bot_symbol "
            "ON position_lifecycle_state(bot_instance_id, symbol)"
        )
        _add_column_if_missing(conn, "position_lifecycle_state", "position_id", "TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pls_position_id "
            "ON position_lifecycle_state(position_id)"
        )
        _add_column_if_missing(conn, "position_lifecycle_state", "exchange_position_active", "INTEGER")
        _add_column_if_missing(conn, "position_lifecycle_state", "reconciliation_status", "TEXT")
        _add_column_if_missing(conn, "position_lifecycle_state", "reconciliation_reason", "TEXT")
        _add_column_if_missing(conn, "position_lifecycle_state", "last_reconciled_at", "TEXT")
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_preserve_lifecycle_protection_ids
            AFTER UPDATE OF sl_order_id, tp_order_id ON position_lifecycle_state
            WHEN OLD.sl_order_id IS NOT NULL
             AND OLD.tp_order_id IS NOT NULL
             AND (NEW.sl_order_id IS NULL OR NEW.tp_order_id IS NULL)
            BEGIN
                UPDATE position_lifecycle_state
                   SET sl_order_id = COALESCE(NEW.sl_order_id, OLD.sl_order_id),
                       tp_order_id = COALESCE(NEW.tp_order_id, OLD.tp_order_id),
                       reconciliation_status = CASE
                           WHEN OLD.reconciliation_status = 'PROTECTED' THEN OLD.reconciliation_status
                           ELSE NEW.reconciliation_status
                       END,
                       reconciliation_reason = CASE
                           WHEN OLD.reconciliation_status = 'PROTECTED' THEN OLD.reconciliation_reason
                           ELSE NEW.reconciliation_reason
                       END,
                       last_reconciled_at = COALESCE(NEW.last_reconciled_at, OLD.last_reconciled_at)
                 WHERE bot_instance_id = NEW.bot_instance_id
                   AND symbol = NEW.symbol;
            END
        """)

        # Backfill legacy symbol_state table with lifecycle columns
        # (used by default / PaperRunner path where bot_instance_id = 'default')
        _add_column_if_missing(conn, "symbol_state", "lifecycle_phase",       "TEXT DEFAULT 'SEEKING_TP1'")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_current_stop", "REAL")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_original_stop","REAL")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_tp1",          "REAL")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_tp2",          "REAL")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_is_be",        "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_tp1_hit",      "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_trailing",     "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_highest",      "REAL")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_lowest",       "REAL")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_qty_remaining","REAL")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_sl_order_id",  "TEXT")
        _add_column_if_missing(conn, "symbol_state", "lifecycle_tp_order_id",  "TEXT")

        # Same for multi-bot bot_symbol_state table
        _add_column_if_missing(conn, "bot_symbol_state", "lifecycle_phase",       "TEXT DEFAULT 'SEEKING_TP1'")
        _add_column_if_missing(conn, "bot_symbol_state", "lifecycle_current_stop", "REAL")
        _add_column_if_missing(conn, "bot_symbol_state", "lifecycle_original_stop","REAL")
        _add_column_if_missing(conn, "bot_symbol_state", "lifecycle_is_be",        "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "bot_symbol_state", "lifecycle_tp1_hit",      "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "bot_symbol_state", "lifecycle_sl_order_id",  "TEXT")
        _add_column_if_missing(conn, "bot_symbol_state", "lifecycle_tp_order_id",  "TEXT")

        # =====================================================================
        # SEV-1 FIX: Broker credential versioning + events.trace_id
        # =====================================================================

        # 33a) If broker_credentials_v2 already exists from an older migration that
        # omitted the `id` autoincrement column, recreate it with the correct schema.
        # We detect this by checking the table_info pragma.  If `id` is missing, we
        # rename the old table, create the correct one, migrate data, then drop the old.
        _v2_cols = {r["name"] for r in conn.execute("PRAGMA table_info(broker_credentials_v2)").fetchall()}
        if _v2_cols and "id" not in _v2_cols:
            # Old table exists but lacks `id` — recreate with correct schema.
            # Build INSERT column list from only the cols that exist in the old table.
            _known_new_cols = [
                "account_id", "version", "status", "encrypted_blob", "key_metadata",
                "key_fingerprint", "created_at", "updated_at",
                "superseded_at", "last_validated_at", "validation_error",
            ]
            _copy_cols = [c for c in _known_new_cols if c in _v2_cols]
            _copy_sql = ", ".join(_copy_cols)
            # For cols that exist in new schema but not old, provide defaults
            _select_parts = []
            for c in _copy_cols:
                if c in ("version",):
                    _select_parts.append(f"COALESCE({c}, 1)")
                elif c in ("status",):
                    _select_parts.append(f"COALESCE({c}, 'active')")
                elif c in ("created_at", "updated_at"):
                    _select_parts.append(f"COALESCE({c}, datetime('now'))")
                else:
                    _select_parts.append(c)
            _select_sql = ", ".join(_select_parts)

            conn.execute("ALTER TABLE broker_credentials_v2 RENAME TO broker_credentials_v2_old")
            conn.execute("""
                CREATE TABLE broker_credentials_v2 (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id          TEXT NOT NULL,
                    version             INTEGER NOT NULL DEFAULT 1,
                    status              TEXT NOT NULL DEFAULT 'active',
                    encrypted_blob      TEXT NOT NULL,
                    key_metadata        TEXT,
                    key_fingerprint     TEXT,
                    created_at          TEXT NOT NULL,
                    updated_at          TEXT NOT NULL,
                    superseded_at       TEXT,
                    last_validated_at   TEXT,
                    validation_error    TEXT,
                    UNIQUE (account_id, version)
                )
            """)
            conn.execute(
                f"INSERT INTO broker_credentials_v2 ({_copy_sql}) "
                f"SELECT {_select_sql} FROM broker_credentials_v2_old"
            )
            conn.execute("DROP TABLE broker_credentials_v2_old")

        # 33b) broker_credentials versioning (Option A: stable account_id FK, versioned creds)
        #
        # The legacy table used account_id as PRIMARY KEY (one row per account).
        # The new table adds version + status and a UNIQUE(account_id, version) guard.
        # We keep the old table alive and CREATE IF NOT EXISTS the new one so that
        # already-upgraded databases are idempotent.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS broker_credentials_v2 (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id          TEXT NOT NULL,
                version             INTEGER NOT NULL DEFAULT 1,
                status              TEXT NOT NULL DEFAULT 'active',
                encrypted_blob      TEXT NOT NULL,
                key_metadata        TEXT,
                key_fingerprint     TEXT,
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL,
                superseded_at       TEXT,
                last_validated_at   TEXT,
                validation_error    TEXT,
                UNIQUE (account_id, version),
                FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_brcred_v2_account "
            "ON broker_credentials_v2(account_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_brcred_v2_account_status "
            "ON broker_credentials_v2(account_id, status)"
        )

        # Backfill: seed broker_credentials_v2 from legacy broker_credentials for any
        # account_id that does not yet have a row in v2.
        # We insert version=1, status='active', preserving encrypted_blob and key_metadata.
        conn.execute("""
            INSERT OR IGNORE INTO broker_credentials_v2
                (account_id, version, status, encrypted_blob, key_metadata,
                 created_at, updated_at)
            SELECT
                bc.account_id,
                1          AS version,
                'active'   AS status,
                bc.encrypted_blob,
                bc.key_metadata,
                COALESCE(bc.updated_at, datetime('now')) AS created_at,
                COALESCE(bc.updated_at, datetime('now')) AS updated_at
            FROM broker_credentials bc
            WHERE NOT EXISTS (
                SELECT 1 FROM broker_credentials_v2 v2
                WHERE v2.account_id = bc.account_id
            )
        """)

        # 34) broker_accounts: add active_credential_version pointer + metadata columns
        _add_column_if_missing(conn, "broker_accounts", "active_credential_version", "INTEGER")
        _add_column_if_missing(conn, "broker_accounts", "validation_error",          "TEXT")
        _add_column_if_missing(conn, "broker_accounts", "superseded_at",             "TEXT")

        # Backfill: point active_credential_version=1 for accounts that have a v2 row
        # but no pointer yet (i.e. all legacy accounts just seeded above).
        conn.execute("""
            UPDATE broker_accounts
            SET    active_credential_version = 1
            WHERE  active_credential_version IS NULL
              AND  id IN (SELECT DISTINCT account_id FROM broker_credentials_v2)
        """)

        # 35) events table: add trace_id column (missed in the original migration)
        #
        # audit.py uses trace_id in every INSERT; the events table was created without
        # it, causing OperationalError on every event write.  Adding it here with a
        # NULL default is safe — existing rows stay intact, new writes succeed.
        _add_column_if_missing(conn, "events", "trace_id", "TEXT")

        # Index for trace lookups (audit queries, debugging)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_trace_id "
            "ON events(trace_id) WHERE trace_id IS NOT NULL"
        )

        # =====================================================================
        # 36) Bot broker health quarantine columns
        #
        # Tracks whether a bot's broker is in a blocked/invalid state so that
        # the runner can separate "active and tradable" from "active but blocked".
        #
        # broker_health_status values:
        #   'ok'              — broker resolved successfully, bot may trade
        #   'broker_blocked'  — broker resolution failed with a non-transient error;
        #                       bot is excluded from the execution pool until fixed
        #
        # broker_error_code  — last BrokerResolverError.reason_code
        # broker_blocked_at  — ISO timestamp when the bot was quarantined
        # =====================================================================
        _add_column_if_missing(conn, "bot_instances", "broker_health_status", "TEXT DEFAULT 'ok'")
        _add_column_if_missing(conn, "bot_instances", "broker_error_code",    "TEXT")
        _add_column_if_missing(conn, "bot_instances", "broker_blocked_at",    "TEXT")
        
        # New observability columns representing explicit block state
        _add_column_if_missing(conn, "bot_instances", "block_category",       "TEXT")
        _add_column_if_missing(conn, "bot_instances", "block_reason_code",    "TEXT")
        _add_column_if_missing(conn, "bot_instances", "block_reason_detail",  "TEXT")
        _add_column_if_missing(conn, "bot_instances", "blocked_since",        "TEXT")
        _add_column_if_missing(conn, "bot_instances", "last_validated_at",    "TEXT")
        _add_column_if_missing(conn, "bot_instances", "last_validation_error","TEXT")

        # Backfill explicitly requested fields from legacy broker_* columns 
        try:
            conn.execute("""
                UPDATE bot_instances
                SET block_category = 'legacy_block_state_unknown',
                    block_reason_code = broker_error_code,
                    block_reason_detail = last_error,
                    blocked_since = broker_blocked_at
                WHERE broker_health_status = 'broker_blocked'
                  AND block_reason_code IS NULL
            """)
        except Exception:
            pass

        # Backfill: any existing bot already in status='error' with a broker-related
        # last_error stays in 'ok' broker_health_status — the new code will set it
        # correctly on the next cycle.  No retroactive quarantine applied here.

        # Index for fast "give me tradable bots" query
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bot_instances_broker_health "
            "ON bot_instances(status, broker_health_status)"
        )

        # =====================================================================
        # 37) Shadow Trading System — Research & Evaluation Layer
        #
        # These two tables capture rejected/non-executed trade opportunities
        # and their hypothetical outcomes for research and threshold tuning.
        #
        # SAFETY NOTE:
        #   - shadow_trades NEVER appears in trade_fills
        #   - shadow_trade_outcomes NEVER drives any execution
        #   - Both tables are purely for research analytics
        # =====================================================================

        conn.execute("""
            CREATE TABLE IF NOT EXISTS shadow_trades (
                id                  TEXT PRIMARY KEY,
                bot_instance_id     TEXT NOT NULL,
                trace_id            TEXT NOT NULL,         -- FK to decision_traces.trace_id
                symbol              TEXT NOT NULL,
                side                TEXT,                   -- BUY / SELL
                regime              TEXT,
                strategy            TEXT,
                confidence          REAL,
                threshold           REAL,
                confidence_gap      REAL,                   -- confidence - threshold
                ml_score            REAL,
                ml_action           TEXT,
                gate_reason         TEXT,
                rejection_stage     TEXT NOT NULL,          -- ML_BLOCKED | THRESHOLD_BLOCKED | ...
                rejection_reason    TEXT,
                entry_time          TEXT NOT NULL,          -- ISO timestamp at decision
                entry_price         REAL,                   -- mark price at decision time
                stop_loss           REAL,
                take_profit         REAL,
                expiry_time         TEXT,                   -- ISO timestamp (entry + N bars)
                status              TEXT NOT NULL DEFAULT 'PENDING',
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL,
                UNIQUE(trace_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_shadow_trades_status "
            "ON shadow_trades(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_shadow_trades_trace "
            "ON shadow_trades(trace_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_shadow_trades_bot "
            "ON shadow_trades(bot_instance_id, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_shadow_trades_stage "
            "ON shadow_trades(rejection_stage, created_at)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS shadow_trade_outcomes (
                id                  TEXT PRIMARY KEY,
                shadow_trade_id     TEXT NOT NULL,
                outcome             TEXT NOT NULL,  -- TP_HIT | SL_HIT | EXPIRED | DATA_MISSING
                exit_time           TEXT,
                exit_price          REAL,
                pnl_abs             REAL,           -- hypothetical gross PnL (1 unit normalized)
                pnl_pct             REAL,           -- PnL as % of entry_price
                pnl_net             REAL,           -- after assumed fee deduction
                mfe                 REAL,           -- max favorable excursion (abs price delta)
                mae                 REAL,           -- max adverse excursion (abs price delta)
                bars_elapsed        INTEGER,
                minutes_elapsed     REAL,
                evaluation_notes    TEXT,           -- JSON: methodology notes, ambiguity flag, etc.
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL,
                FOREIGN KEY(shadow_trade_id) REFERENCES shadow_trades(id)
            )
        """)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_outcomes_shadow_id "
            "ON shadow_trade_outcomes(shadow_trade_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_shadow_outcomes_outcome "
            "ON shadow_trade_outcomes(outcome)"
        )

        # =====================================================================
        # 38) Event Awareness — economic event calendar + blackout windows
        #
        # economic_events: canonical store for scheduled macro/crypto events.
        # event_blackout_windows: computed pre/post windows per event.
        # decision_traces: 3 new columns to log event-blocked cycles.
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS economic_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id      TEXT NOT NULL UNIQUE,
                title         TEXT NOT NULL,
                event_type    TEXT NOT NULL,
                country_currency TEXT NOT NULL,
                impact_level  TEXT NOT NULL,
                scheduled_utc TEXT NOT NULL,
                actual_val    REAL,
                forecast_val  REAL,
                previous_val  REAL,
                source        TEXT NOT NULL DEFAULT 'manual',
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_eco_events_scheduled "
            "ON economic_events(scheduled_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_eco_events_impact "
            "ON economic_events(impact_level, scheduled_utc)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS event_blackout_windows (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id         INTEGER NOT NULL REFERENCES economic_events(id),
                start_utc        TEXT NOT NULL,
                end_utc          TEXT NOT NULL,
                affected_symbols TEXT,
                is_global        INTEGER NOT NULL DEFAULT 1,
                is_active        INTEGER NOT NULL DEFAULT 1,
                reason           TEXT NOT NULL,
                created_at       TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ebw_active_range "
            "ON event_blackout_windows(is_active, start_utc, end_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ebw_event_id "
            "ON event_blackout_windows(event_id)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_ebw_event_range_unique "
            "ON event_blackout_windows(event_id, start_utc, end_utc)"
        )

        # Add event-block observability columns to decision_traces
        _add_column_if_missing(conn, "decision_traces", "event_blocked",      "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "decision_traces", "event_block_reason", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "event_block_details","TEXT")
        _add_column_if_missing(conn, "decision_traces", "event_block_event_id", "TEXT")
        _add_column_if_missing(conn, "decision_traces", "event_block_type", "TEXT")

        # =====================================================================
        # 39) Market Reaction Layer (Phase 2)
        #
        # event_market_snapshots: raw periodic OHLCV+ATR snapshots collected
        #   during pre/event/post windows around each economic event.
        # market_event_reactions: computed reaction summary per event+symbol,
        #   including price, volatility, volume metrics and a rule-based
        #   reaction_type classification.
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS event_market_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id      TEXT NOT NULL,
                symbol        TEXT NOT NULL,
                exchange      TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                window_label  TEXT NOT NULL,
                price         REAL,
                volume        REAL,
                candle_open   REAL,
                candle_high   REAL,
                candle_low    REAL,
                candle_close  REAL,
                atr           REAL,
                spread        REAL,
                bid_depth     REAL,
                ask_depth     REAL,
                source        TEXT NOT NULL DEFAULT 'binance',
                created_at    TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ems_event_symbol "
            "ON event_market_snapshots(event_id, symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ems_timestamp "
            "ON event_market_snapshots(timestamp_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ems_window "
            "ON event_market_snapshots(event_id, symbol, window_label)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_event_reactions (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id                 TEXT NOT NULL,
                symbol                   TEXT NOT NULL,
                exchange                 TEXT NOT NULL,
                pre_window_start_utc     TEXT,
                event_time_utc           TEXT NOT NULL,
                post_window_end_utc      TEXT,

                price_before_event       REAL,
                price_at_event           REAL,
                price_after_event        REAL,
                price_after_5m           REAL,
                price_after_15m          REAL,
                price_after_30m          REAL,
                price_after_60m          REAL,
                max_move_pct             REAL,
                min_move_pct             REAL,
                net_move_pct             REAL,
                direction_after_event    TEXT,
                continuation_or_reversal TEXT,

                atr_before               REAL,
                atr_after                REAL,
                volatility_expansion_ratio REAL,
                candle_range_before      REAL,
                candle_range_during      REAL,
                realized_vol_before      REAL,
                realized_vol_after       REAL,

                average_volume_before    REAL,
                event_volume             REAL,
                volume_spike_ratio       REAL,
                abnormal_volume_score    REAL,

                spread_before            REAL,
                spread_during            REAL,
                spread_after             REAL,
                spread_widening_ratio    REAL,
                slippage_estimate        REAL,
                order_book_depth_change  REAL,

                reaction_type            TEXT NOT NULL DEFAULT 'NO_REACTION',
                confidence_score         REAL,
                data_quality             TEXT NOT NULL DEFAULT 'COMPLETE',

                created_at               TEXT NOT NULL,
                updated_at               TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mer_event_symbol "
            "ON market_event_reactions(event_id, symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mer_reaction_type "
            "ON market_event_reactions(reaction_type)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mer_event_time "
            "ON market_event_reactions(event_time_utc)"
        )
        _add_column_if_missing(conn, "market_event_reactions", "price_after_event", "REAL")

        # =====================================================================
        # 40) News Intelligence Layer (Phase 3 — Hardened)
        #
        # news_sources            : source trust registry with dynamic scores
        # raw_news_items          : one row per article/post from any provider
        # news_clusters           : deduplicated story groups + quality scores
        # news_cluster_items      : many-to-one mapping of items → clusters
        # news_asset_mappings     : cluster → affected symbols/assets
        # news_sentiment_scores   : VADER sentiment per cluster
        # news_narratives         : rule-based narrative type per cluster
        # news_intelligence_signals : shadow-only signals (never affect trading)
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_sources (
                id                        TEXT PRIMARY KEY,
                source_name               TEXT NOT NULL,
                base_reliability_score    REAL NOT NULL DEFAULT 0.5,
                dynamic_reliability_score REAL NOT NULL DEFAULT 0.5,
                is_trusted                INTEGER NOT NULL DEFAULT 0,
                is_blocked                INTEGER NOT NULL DEFAULT 0,
                created_at                TEXT NOT NULL,
                updated_at                TEXT NOT NULL
            )
        """)

        # Seed known sources (INSERT OR IGNORE so re-runs are safe)
        _now_iso = __import__("datetime").datetime.utcnow().isoformat()
        _sources = [
            # (id/domain, source_name, base_reliability, dynamic_reliability, is_trusted, is_blocked)
            ("reuters.com",       "Reuters",              0.95, 0.95, 1, 0),
            ("bloomberg.com",     "Bloomberg",            0.95, 0.95, 1, 0),
            ("ft.com",            "Financial Times",      0.90, 0.90, 1, 0),
            ("wsj.com",           "Wall Street Journal",  0.90, 0.90, 1, 0),
            ("apnews.com",        "AP News",              0.90, 0.90, 1, 0),
            ("coindesk.com",      "CoinDesk",             0.80, 0.80, 1, 0),
            ("cointelegraph.com", "CoinTelegraph",        0.75, 0.75, 1, 0),
            ("benzinga.com",      "Benzinga",             0.75, 0.75, 1, 0),
            ("decrypt.co",        "Decrypt",              0.70, 0.70, 1, 0),
            ("theblock.co",       "The Block",            0.75, 0.75, 1, 0),
            ("cryptopanic.com",   "CryptoPanic",          0.50, 0.50, 0, 0),
            ("twitter.com",       "Twitter/X",            0.25, 0.25, 0, 0),
            ("x.com",             "Twitter/X",            0.25, 0.25, 0, 0),
            ("reddit.com",        "Reddit",               0.20, 0.20, 0, 0),
            ("t.me",              "Telegram",             0.15, 0.15, 0, 0),
            ("unknown",           "Unknown Source",       0.10, 0.10, 0, 0),
        ]
        for domain, name, base_r, dyn_r, trusted, blocked in _sources:
            conn.execute(
                """INSERT OR IGNORE INTO news_sources
                   (id, source_name, base_reliability_score, dynamic_reliability_score,
                    is_trusted, is_blocked, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (domain, name, base_r, dyn_r, trusted, blocked, _now_iso, _now_iso),
            )

        # Migrate old news_source_reliability → news_sources if old table exists
        try:
            old_rows = conn.execute(
                "SELECT domain, provider_name, reliability_score FROM news_source_reliability"
            ).fetchall()
            for row in old_rows:
                conn.execute(
                    """INSERT OR IGNORE INTO news_sources
                       (id, source_name, base_reliability_score, dynamic_reliability_score,
                        is_trusted, is_blocked, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row[0], row[1], row[2], row[2], 0, 0, _now_iso, _now_iso),
                )
        except Exception:
            pass  # old table may not exist on fresh DBs

        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_news_items (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                provider         TEXT NOT NULL,
                source_name      TEXT,
                source_domain    TEXT,
                source_url       TEXT,
                external_id      TEXT,
                author           TEXT,
                title            TEXT NOT NULL,
                body_snippet     TEXT,
                raw_payload_json TEXT,
                published_utc    TEXT NOT NULL,
                ingested_utc     TEXT NOT NULL,
                latency_seconds  REAL,
                language         TEXT NOT NULL DEFAULT 'en',
                is_duplicate     INTEGER NOT NULL DEFAULT 0,
                created_at       TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_rni_provider_extid "
            "ON raw_news_items(provider, external_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rni_published "
            "ON raw_news_items(published_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rni_domain "
            "ON raw_news_items(source_domain)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rni_ingested "
            "ON raw_news_items(ingested_utc)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_clusters (
                id                        INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_title           TEXT NOT NULL,
                summary                   TEXT,
                first_seen_utc            TEXT NOT NULL,
                last_seen_utc             TEXT NOT NULL,
                source_count              INTEGER NOT NULL DEFAULT 1,
                provider_count            INTEGER NOT NULL DEFAULT 1,
                highest_reliability_score REAL NOT NULL DEFAULT 0.0,
                cluster_confidence        REAL NOT NULL DEFAULT 0.0,
                spam_score                REAL NOT NULL DEFAULT 0.0,
                latency_score             REAL NOT NULL DEFAULT 0.0,
                is_valid_signal           INTEGER NOT NULL DEFAULT 0,
                manipulation_flag         TEXT,
                data_quality_status       TEXT NOT NULL DEFAULT 'LOW_CONFIDENCE',
                is_manipulation_suspect   INTEGER NOT NULL DEFAULT 0,
                manipulation_reason       TEXT,
                created_at                TEXT NOT NULL,
                updated_at                TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_first_seen "
            "ON news_clusters(first_seen_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_last_seen "
            "ON news_clusters(last_seen_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_manip "
            "ON news_clusters(is_manipulation_suspect)"
        )

        # Migrate: add new columns to existing news_clusters
        try:
            _nc_cols = {r[1] for r in conn.execute("PRAGMA table_info(news_clusters)").fetchall()}
            for _col, _def in [
                ("spam_score",          "REAL NOT NULL DEFAULT 0.0"),
                ("latency_score",        "REAL NOT NULL DEFAULT 0.0"),
                ("is_valid_signal",      "INTEGER NOT NULL DEFAULT 0"),
                ("manipulation_flag",    "TEXT"),
                ("data_quality_status",  "TEXT NOT NULL DEFAULT 'LOW_CONFIDENCE'"),
            ]:
                if _col not in _nc_cols:
                    conn.execute(f"ALTER TABLE news_clusters ADD COLUMN {_col} {_def}")
        except Exception:
            pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_dq_status "
            "ON news_clusters(data_quality_status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_valid "
            "ON news_clusters(is_valid_signal)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_cluster_items (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id       INTEGER NOT NULL REFERENCES news_clusters(id),
                raw_news_item_id INTEGER NOT NULL REFERENCES raw_news_items(id),
                similarity_score REAL NOT NULL DEFAULT 1.0,
                created_at       TEXT NOT NULL,
                UNIQUE(cluster_id, raw_news_item_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nci_cluster "
            "ON news_cluster_items(cluster_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nci_item "
            "ON news_cluster_items(raw_news_item_id)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_asset_mappings (
                id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id             INTEGER NOT NULL REFERENCES news_clusters(id),
                symbol                 TEXT,
                asset                  TEXT,
                mapping_reason         TEXT NOT NULL,
                mapping_confidence     REAL NOT NULL DEFAULT 0.5,
                is_global_market_event INTEGER NOT NULL DEFAULT 0,
                created_at             TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nam_cluster "
            "ON news_asset_mappings(cluster_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nam_symbol "
            "ON news_asset_mappings(symbol)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_sentiment_scores (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id       INTEGER NOT NULL REFERENCES news_clusters(id),
                sentiment_score  REAL NOT NULL,
                sentiment_label  TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                model_name       TEXT NOT NULL DEFAULT 'vader',
                model_version    TEXT,
                compound_raw     REAL,
                pos_raw          REAL,
                neg_raw          REAL,
                neu_raw          REAL,
                created_at       TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_nss_cluster "
            "ON news_sentiment_scores(cluster_id)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_narratives (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id           INTEGER NOT NULL REFERENCES news_clusters(id),
                narrative_type       TEXT NOT NULL,
                narrative_confidence REAL NOT NULL,
                severity_level       TEXT NOT NULL DEFAULT 'LOW',
                matched_keywords     TEXT,
                created_at           TEXT NOT NULL,
                updated_at           TEXT
            )
        """)
        _normalize_news_narratives_schema(conn)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_intelligence_signals (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id            INTEGER NOT NULL REFERENCES news_clusters(id),
                symbol                TEXT,
                signal_type           TEXT NOT NULL,
                sentiment_label       TEXT,
                sentiment_score       REAL,
                narrative_type        TEXT,
                severity_level        TEXT NOT NULL DEFAULT 'LOW',
                reliability_score     REAL,
                confidence_score      REAL,
                spam_score            REAL NOT NULL DEFAULT 0.0,
                latency_score         REAL NOT NULL DEFAULT 0.0,
                source_validation_passed INTEGER NOT NULL DEFAULT 0,
                market_validation_passed INTEGER NOT NULL DEFAULT 0,
                is_valid_signal       INTEGER NOT NULL DEFAULT 0,
                manipulation_flag     TEXT,
                data_quality_status   TEXT NOT NULL DEFAULT 'LOW_CONFIDENCE',
                sentiment_accuracy    TEXT,
                validation_status     TEXT NOT NULL DEFAULT 'PENDING_MARKET_VALIDATION',
                should_affect_trading INTEGER NOT NULL DEFAULT 0,
                shadow_only           INTEGER NOT NULL DEFAULT 1,
                validated_at          TEXT,
                suppression_reason    TEXT,
                created_at            TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_cluster "
            "ON news_intelligence_signals(cluster_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_symbol "
            "ON news_intelligence_signals(symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_created "
            "ON news_intelligence_signals(created_at)"
        )
        # Migrate: add new columns to existing news_intelligence_signals
        try:
            _nis_cols = {r[1] for r in conn.execute("PRAGMA table_info(news_intelligence_signals)").fetchall()}
            for _col, _def in [
                ("sentiment_label",      "TEXT"),
                ("spam_score",          "REAL NOT NULL DEFAULT 0.0"),
                ("latency_score",        "REAL NOT NULL DEFAULT 0.0"),
                ("source_validation_passed", "INTEGER NOT NULL DEFAULT 0"),
                ("market_validation_passed", "INTEGER NOT NULL DEFAULT 0"),
                ("is_valid_signal",      "INTEGER NOT NULL DEFAULT 0"),
                ("manipulation_flag",    "TEXT"),
                ("data_quality_status",  "TEXT NOT NULL DEFAULT 'LOW_CONFIDENCE'"),
                ("sentiment_accuracy",   "TEXT"),
                ("validation_status",    "TEXT NOT NULL DEFAULT 'PENDING_MARKET_VALIDATION'"),
                ("validated_at",         "TEXT"),
            ]:
                if _col not in _nis_cols:
                    conn.execute(f"ALTER TABLE news_intelligence_signals ADD COLUMN {_col} {_def}")
        except Exception:
            pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_dq_status "
            "ON news_intelligence_signals(data_quality_status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_valid "
            "ON news_intelligence_signals(is_valid_signal)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_validation_status "
            "ON news_intelligence_signals(validation_status)"
        )
        # ================================================================
        # 41) News Market Validation Layer (Phase 3 Extension)
        # ================================================================
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_market_reactions (
                id                         INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id                 INTEGER NOT NULL
                                               REFERENCES news_clusters(id),
                symbol                     TEXT NOT NULL,
                event_reaction_id          INTEGER,
                sentiment_score            REAL,
                sentiment_direction        TEXT,
                actual_direction           TEXT,
                sentiment_accuracy         TEXT NOT NULL DEFAULT 'NEUTRAL',
                sentiment_accuracy_score   REAL NOT NULL DEFAULT 0.0,
                impact_score               REAL NOT NULL DEFAULT 0.0,
                max_price_move_pct         REAL,
                volatility_expansion       REAL,
                volume_spike               REAL,
                reaction_type              TEXT NOT NULL DEFAULT 'NO_REACTION',
                reaction_latency_minutes   REAL,
                reaction_latency_category  TEXT NOT NULL DEFAULT 'NO_REACTION',
                signal_effectiveness_score REAL NOT NULL DEFAULT 0.0,
                is_false_signal            INTEGER NOT NULL DEFAULT 0,
                false_signal_reason        TEXT,
                data_quality_score         REAL NOT NULL DEFAULT 0.0,
                reliability_score          REAL NOT NULL DEFAULT 0.0,
                created_at                 TEXT NOT NULL,
                updated_at                 TEXT NOT NULL
            )
            """
        )
        for _idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_nmr_cluster  "
            "ON news_market_reactions(cluster_id)",
            "CREATE INDEX IF NOT EXISTS idx_nmr_symbol   "
            "ON news_market_reactions(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_nmr_accuracy "
            "ON news_market_reactions(sentiment_accuracy)",
            "CREATE INDEX IF NOT EXISTS idx_nmr_false    "
            "ON news_market_reactions(is_false_signal)",
            "CREATE INDEX IF NOT EXISTS idx_nmr_created  "
            "ON news_market_reactions(created_at)",
        ]:
            conn.execute(_idx_sql)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS narrative_effectiveness_scores (
                narrative_type          TEXT PRIMARY KEY,
                sample_count            INTEGER NOT NULL DEFAULT 0,
                avg_impact_score        REAL NOT NULL DEFAULT 0.0,
                avg_price_move_pct      REAL NOT NULL DEFAULT 0.0,
                correct_sentiment_ratio REAL NOT NULL DEFAULT 0.0,
                false_signal_ratio      REAL NOT NULL DEFAULT 0.0,
                avg_effectiveness_score REAL NOT NULL DEFAULT 0.0,
                last_updated            TEXT NOT NULL
            )
            """
        )

        # ================================================================
        # 42) Real News Source Connection Layer (Phase 3 — RSS feeds)
        # ================================================================
        # Extend news_sources with provider/scheduling columns
        for _col, _def in [
            ("source_type",            "TEXT    NOT NULL DEFAULT 'API'"),
            ("category",               "TEXT    NOT NULL DEFAULT 'CRYPTO'"),
            ("is_enabled",             "INTEGER NOT NULL DEFAULT 1"),
            ("fetch_interval_seconds", "INTEGER NOT NULL DEFAULT 300"),
            ("rss_url",                "TEXT"),
            ("last_fetch_utc",         "TEXT"),
            ("last_success_utc",       "TEXT"),
            ("last_error",             "TEXT"),
        ]:
            _add_column_if_missing(conn, "news_sources", _col, _def)

        # Update / seed the 8 RSS sources with their URLs and type
        _now_iso2 = __import__("datetime").datetime.utcnow().isoformat()
        _rss_sources = [
            # (id/domain, source_name, base_r, dyn_r, is_trusted, source_type, category, rss_url, interval)
            ("coindesk.com",      "CoinDesk",        0.80, 0.80, 1, "RSS", "CRYPTO",
             "https://www.coindesk.com/arc/outboundfeeds/rss/", 300),
            ("cointelegraph.com", "CoinTelegraph",   0.75, 0.75, 1, "RSS", "CRYPTO",
             "https://cointelegraph.com/rss", 300),
            ("decrypt.co",        "Decrypt",         0.70, 0.70, 1, "RSS", "CRYPTO",
             "https://decrypt.co/feed", 300),
            ("theblock.co",       "The Block",       0.75, 0.75, 1, "RSS", "CRYPTO",
             "https://www.theblock.co/rss.xml", 300),
            ("bitcoinmagazine.com", "Bitcoin Magazine", 0.72, 0.72, 1, "RSS", "CRYPTO",
             "https://bitcoinmagazine.com/.rss/full/", 600),
            ("forexlive.com",     "ForexLive",       0.75, 0.75, 1, "RSS", "MACRO",
             "https://www.forexlive.com/feed/news", 300),
            ("fxstreet.com",      "FXStreet",        0.70, 0.70, 1, "RSS", "MACRO",
             "https://www.fxstreet.com/rss/news", 300),
            ("investing.com",     "Investing.com",   0.70, 0.70, 1, "RSS", "MARKET",
             "https://www.investing.com/rss/news_301.rss", 600),
        ]
        for (_d, _n, _br, _dr, _tr, _st, _cat, _rss, _intv) in _rss_sources:
            conn.execute(
                """INSERT INTO news_sources
                   (id, source_name, base_reliability_score, dynamic_reliability_score,
                    is_trusted, is_blocked, source_type, category, is_enabled,
                    fetch_interval_seconds, rss_url, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     source_type=excluded.source_type,
                     category=excluded.category,
                     is_enabled=excluded.is_enabled,
                     fetch_interval_seconds=excluded.fetch_interval_seconds,
                     rss_url=excluded.rss_url,
                     updated_at=excluded.updated_at""",
                (_d, _n, _br, _dr, _tr, _st, _cat, _intv, _rss, _now_iso2, _now_iso2),
            )

        # Add bitcoinmagazine.com as a new source if it doesn't exist yet
        conn.execute(
            """INSERT OR IGNORE INTO news_sources
               (id, source_name, base_reliability_score, dynamic_reliability_score,
                is_trusted, is_blocked, created_at, updated_at)
               VALUES ('bitcoinmagazine.com', 'Bitcoin Magazine', 0.72, 0.72, 1, 0, ?, ?)""",
            (_now_iso2, _now_iso2),
        )
        conn.execute(
            """INSERT OR IGNORE INTO news_sources
               (id, source_name, base_reliability_score, dynamic_reliability_score,
                is_trusted, is_blocked, created_at, updated_at)
               VALUES ('forexlive.com', 'ForexLive', 0.75, 0.75, 1, 0, ?, ?)""",
            (_now_iso2, _now_iso2),
        )
        conn.execute(
            """INSERT OR IGNORE INTO news_sources
               (id, source_name, base_reliability_score, dynamic_reliability_score,
                is_trusted, is_blocked, created_at, updated_at)
               VALUES ('fxstreet.com', 'FXStreet', 0.70, 0.70, 1, 0, ?, ?)""",
            (_now_iso2, _now_iso2),
        )
        conn.execute(
            """INSERT OR IGNORE INTO news_sources
               (id, source_name, base_reliability_score, dynamic_reliability_score,
                is_trusted, is_blocked, created_at, updated_at)
               VALUES ('investing.com', 'Investing.com', 0.70, 0.70, 1, 0, ?, ?)""",
            (_now_iso2, _now_iso2),
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_provider_health (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id                TEXT NOT NULL,
                status                   TEXT NOT NULL DEFAULT 'UNKNOWN',
                last_checked_utc         TEXT NOT NULL,
                last_success_utc         TEXT,
                items_fetched_last_run   INTEGER NOT NULL DEFAULT 0,
                duplicate_count_last_run INTEGER NOT NULL DEFAULT 0,
                error_message            TEXT,
                created_at               TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nph_source   "
            "ON news_provider_health(source_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nph_status   "
            "ON news_provider_health(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nph_checked  "
            "ON news_provider_health(last_checked_utc)"
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_news_imports (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                title            TEXT NOT NULL,
                body_snippet     TEXT,
                source_name      TEXT,
                source_url       TEXT,
                published_utc    TEXT NOT NULL,
                affected_symbols TEXT,
                imported_by      TEXT,
                raw_news_item_id INTEGER,
                created_at       TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mni_created  "
            "ON manual_news_imports(created_at)"
        )

        # ================================================================
        # 43) Real-Time News Ingestion Hardening Layer
        # ================================================================

        # Per-provider operational status table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS real_time_news_provider_status (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                provider            TEXT NOT NULL UNIQUE,
                is_enabled          INTEGER NOT NULL DEFAULT 0,
                last_fetch_utc      TEXT,
                last_success_utc    TEXT,
                last_error          TEXT,
                latency_avg_seconds REAL NOT NULL DEFAULT 0.0,
                items_fetched_today INTEGER NOT NULL DEFAULT 0,
                duplicate_rate      REAL NOT NULL DEFAULT 0.0,
                health_status       TEXT NOT NULL DEFAULT 'UNKNOWN',
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rtnps_provider "
            "ON real_time_news_provider_status(provider)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rtnps_status   "
            "ON real_time_news_provider_status(health_status)"
        )

        # Extend news_clusters with real-time intelligence columns
        _nc43_cols = {
            r[1] for r in conn.execute(
                "PRAGMA table_info(news_clusters)"
            ).fetchall()
        }
        for _col43, _def43 in [
            ("first_seen_provider",        "TEXT"),
            ("latency_category",           "TEXT    NOT NULL DEFAULT 'UNKNOWN'"),
            ("confirmation_count",         "INTEGER NOT NULL DEFAULT 0"),
            ("conflict_flag",              "INTEGER NOT NULL DEFAULT 0"),
            ("fake_news_risk_score",       "REAL    NOT NULL DEFAULT 0.0"),
            ("market_confirmation_status", "TEXT    NOT NULL DEFAULT 'PENDING'"),
        ]:
            if _col43 not in _nc43_cols:
                conn.execute(
                    f"ALTER TABLE news_clusters ADD COLUMN {_col43} {_def43}"
                )

        for _idx43 in [
            "CREATE INDEX IF NOT EXISTS idx_nc_latency_cat  "
            "ON news_clusters(latency_category)",
            "CREATE INDEX IF NOT EXISTS idx_nc_conflict      "
            "ON news_clusters(conflict_flag)",
            "CREATE INDEX IF NOT EXISTS idx_nc_fake_risk     "
            "ON news_clusters(fake_news_risk_score)",
            "CREATE INDEX IF NOT EXISTS idx_nc_mkt_confirm   "
            "ON news_clusters(market_confirmation_status)",
        ]:
            conn.execute(_idx43)

        # Extend news_intelligence_signals with real-time hardening columns
        _nis43_cols = {
            r[1] for r in conn.execute(
                "PRAGMA table_info(news_intelligence_signals)"
            ).fetchall()
        }
        for _col43s, _def43s in [
            ("provider_latency_score",     "REAL    NOT NULL DEFAULT 1.0"),
            ("market_confirmation_status", "TEXT    NOT NULL DEFAULT 'PENDING'"),
            ("fake_news_risk_score",       "REAL    NOT NULL DEFAULT 0.0"),
            ("conflict_flag",              "INTEGER NOT NULL DEFAULT 0"),
        ]:
            if _col43s not in _nis43_cols:
                conn.execute(
                    f"ALTER TABLE news_intelligence_signals "
                    f"ADD COLUMN {_col43s} {_def43s}"
                )

        # Seed known real-time provider rows (disabled by default)
        _now_iso43 = __import__("datetime").datetime.utcnow().isoformat()
        for _prov in ("cryptopanic", "benzinga", "reuters", "generic"):
            conn.execute(
                """INSERT OR IGNORE INTO real_time_news_provider_status
                   (provider, is_enabled, health_status, created_at, updated_at)
                   VALUES (?, 0, 'DISABLED', ?, ?)""",
                (_prov, _now_iso43, _now_iso43),
            )

        # ================================================================
        # 44) Dynamic Symbol Universe Shadow Diagnostics
        # ================================================================
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dynamic_universe_shadow_diagnostics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                run_id TEXT,
                cycle_id TEXT,
                bot_instance_id TEXT,
                symbol TEXT NOT NULL,
                rank INTEGER,
                in_live_config INTEGER NOT NULL DEFAULT 0,
                was_evaluated INTEGER NOT NULL DEFAULT 0,
                would_pass_strategy INTEGER NOT NULL DEFAULT 0,
                signal TEXT,
                confidence REAL,
                threshold REAL,
                reason TEXT,
                quote_volume_24h REAL,
                spread_bps REAL,
                exclusion_reasons_json TEXT NOT NULL DEFAULT '[]',
                diagnostics_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dusd_created "
            "ON dynamic_universe_shadow_diagnostics(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dusd_cycle "
            "ON dynamic_universe_shadow_diagnostics(cycle_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dusd_symbol "
            "ON dynamic_universe_shadow_diagnostics(symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dusd_pass "
            "ON dynamic_universe_shadow_diagnostics(would_pass_strategy)"
        )

        # ================================================================
        # 45) Symbol Universe Rankings (shadow-only auto-selection ledger)
        # ================================================================
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_universe_rankings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ranking_run_id TEXT,
                created_at TEXT NOT NULL,
                bot_instance_id TEXT,
                mode TEXT NOT NULL,
                symbol TEXT NOT NULL,
                rank INTEGER,
                score REAL,
                recommended_action TEXT NOT NULL,
                selected_for_trading INTEGER NOT NULL DEFAULT 0,
                preserved_for_management INTEGER NOT NULL DEFAULT 0,
                quote_volume_24h REAL,
                spread_bps REAL,
                volatility_quality REAL,
                candle_sufficiency INTEGER,
                funding_stability REAL,
                open_interest REAL,
                signal_frequency REAL,
                average_confidence REAL,
                would_pass_count INTEGER,
                recent_performance_score REAL,
                inclusion_reason TEXT,
                exclusion_reason TEXT,
                diagnostics_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        _sur_cols = {
            _row["name"]
            for _row in conn.execute("PRAGMA table_info(symbol_universe_rankings)").fetchall()
        }
        if "ranking_run_id" not in _sur_cols:
            conn.execute("ALTER TABLE symbol_universe_rankings ADD COLUMN ranking_run_id TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_created "
            "ON symbol_universe_rankings(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_run "
            "ON symbol_universe_rankings(ranking_run_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_symbol "
            "ON symbol_universe_rankings(symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_universe_rankings_action "
            "ON symbol_universe_rankings(recommended_action)"
        )

        # ================================================================
        # 46) Symbol Universe Promotion Decisions (Step 1 evaluation ledger)
        # ================================================================
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_universe_promotion_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                bot_instance_id TEXT NOT NULL,
                decision_type TEXT NOT NULL,
                from_mode TEXT NOT NULL,
                to_mode TEXT NOT NULL,
                status TEXT NOT NULL,
                selected_symbols_json TEXT NOT NULL DEFAULT '[]',
                evidence_summary_json TEXT NOT NULL DEFAULT '{}',
                ranking_run_ids_json TEXT NOT NULL DEFAULT '[]',
                failure_reasons_json TEXT NOT NULL DEFAULT '[]',
                executed INTEGER NOT NULL DEFAULT 0,
                executed_at TEXT,
                audit_event_type TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_promotion_decisions_created "
            "ON symbol_universe_promotion_decisions(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_promotion_decisions_bot "
            "ON symbol_universe_promotion_decisions(bot_instance_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_promotion_decisions_type "
            "ON symbol_universe_promotion_decisions(decision_type)"
        )

        # 48) Multi-tenant isolation: scope weekly/monthly snapshots by bot_instance_id.
        # The original DDL had UNIQUE on the date column alone, so all bots shared one
        # row per period.  We recreate both tables with a composite UNIQUE(bot_instance_id,
        # date) so every bot owns its own snapshot row.
        # Idempotency guard: skip if the composite named index already exists.

        def _migrate_period_snapshots_v2(conn, table: str, date_col: str, index_name: str) -> None:
            existing_indexes = {
                r["name"]
                for r in conn.execute(f"PRAGMA index_list({table})").fetchall()
            }
            if index_name in existing_indexes:
                return  # already migrated

            existing_cols = {
                r["name"]
                for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            tmp = f"_mt_{table}_new"
            conn.execute(f"DROP TABLE IF EXISTS {tmp}")
            conn.execute(
                f"""
                CREATE TABLE {tmp} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_instance_id TEXT NOT NULL DEFAULT 'default',
                    {date_col} TEXT NOT NULL,
                    start_equity REAL NOT NULL,
                    peak_equity REAL NOT NULL,
                    low_equity REAL NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(bot_instance_id, {date_col})
                )
                """
            )
            # Copy existing rows — assign to 'default' bot if column didn't exist
            bot_src = "'default'" if "bot_instance_id" not in existing_cols else "bot_instance_id"
            conn.execute(
                f"""
                INSERT INTO {tmp}
                    (bot_instance_id, {date_col}, start_equity, peak_equity, low_equity, updated_at)
                SELECT {bot_src}, {date_col}, start_equity, peak_equity, low_equity, updated_at
                FROM {table}
                """
            )
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f"ALTER TABLE {tmp} RENAME TO {table}")
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {table}(bot_instance_id, {date_col})"
            )

        _migrate_period_snapshots_v2(
            conn,
            table="weekly_snapshots",
            date_col="week_start_date",
            index_name="idx_weekly_bot_date",
        )
        _migrate_period_snapshots_v2(
            conn,
            table="monthly_snapshots",
            date_col="month_start_date",
            index_name="idx_monthly_bot_date",
        )
        # Index for efficient per-bot decision log lookups (used by dynamic threshold & strategy perf)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_decision_logs_config_symbol "
            "ON decision_logs(config_id, symbol)"
        )

    # 47) Event/News automatic runtime mode controller.
    # Keep this after the main migration connection closes so the helper can
    # self-heal temporary test databases through the normal DB context manager.
    ensure_event_news_mode_schema(db)
