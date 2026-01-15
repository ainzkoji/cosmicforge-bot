from app.persistence.db import DB


def _add_column_if_missing(conn, table: str, col: str, col_type: str):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {r["name"] for r in rows}
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def migrate():
    db = DB()
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

        # 8) Users Table Enhancements
        _add_column_if_missing(conn, "users", "status", "TEXT DEFAULT 'pending_verification'")
        _add_column_if_missing(conn, "users", "role", "TEXT DEFAULT 'user'")
        _add_column_if_missing(conn, "users", "totp_secret", "TEXT")
        _add_column_if_missing(conn, "users", "is_2fa_enabled", "BOOLEAN DEFAULT 0")

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

