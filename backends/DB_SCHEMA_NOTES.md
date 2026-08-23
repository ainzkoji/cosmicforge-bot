# Database Schema Documentation

**Database**: `bot.db` (SQLite)  
**Migration Source**: `shared_lib/persistence/migrations.py`  
**Last Updated**: 2026-01-20

---

## Table Inventory by Service Ownership

### User-Backend Tables (Write Authority)

#### Authentication & Identity

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `users` | Multi | User accounts | id, email, password_hash, status, role, is_2fa_enabled |
| `auth_sessions` | Multi | JWT refresh tokens | id, user_id, refresh_token_hash, expires_at, revoked_at |
| `email_verifications` | Multi | Email verification codes | id, user_id, code_hash, expires_at, used_at |
| `password_resets` | Multi | Password reset tokens | id, user_id, code_hash, expires_at, used_at |
| `login_attempts` | Multi | Rate limiting | id, email, ip, success, attempted_at |
| `auth_audit_log` | Multi | Auth event log | id, event_type, user_id, email, ip, created_at |

#### Broker Integration

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `broker_accounts` | Multi | Broker connection metadata | id, user_id, broker_id, market_type, status, environment |
| `broker_credentials` | Multi | **Encrypted API keys** | account_id (PK), encrypted_blob, key_metadata, updated_at |
| `broker_audit_log` | Multi | Broker connection events | id, broker_account_id, user_id, event_type, timestamp_utc |

**Security Note**: `broker_credentials.encrypted_blob` contains Fernet-encrypted JSON with `api_key`, `api_secret`, and `environment`.

#### Billing & Subscriptions

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `subscriptions` | 1 per user | Stripe subscription state | user_id (PK), plan_id, status, provider_sub_id, current_period_end |
| `invoices` | Multi | Billing history | id, user_id, amount, currency, status, period_start, period_end |
| `pricing_intents` | Multi | Marketing funnel tracking | id, user_id, plan_id, session_id, created_at |

#### Onboarding

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `onboarding_profiles` | 1 per user | Wizard progression | user_id (PK), status, current_step, data_json, completed_at |

#### Strategy Management

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `strategies` | Multi | Strategy catalog | id, owner_id, visibility, status, name, description, market_types |
| `strategy_versions` | Multi | Strategy code versions | id, strategy_id, version_number, spec_json, param_schema_json |
| `official_strategies_cache` | Multi | Marketplace cache | strategy_id (PK), name, version, params_schema_json |

#### User Strategy Configurations

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `user_strategy_configs` | Multi | User strategy instances | id, user_id, broker_account_id, strategy_id, status, symbols_json |
| `risk_parameters` | 1 per config | Risk settings | config_id (PK), risk_profile, per_trade_risk_pct, stop_loss_multiplier |
| `strategy_parameters` | 1 per config | Strategy overrides | config_id (PK), overrides_json |
| `protection_state` | 1 per config | Account protection state | config_id (PK), is_protected, daily_loss_today, current_drawdown_pct |

#### Bot Instances

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `bot_instances` | Multi | Bot deployment records | id, user_id, broker_account_id, strategy_id, mode, status, last_run_at |

**Note**: Bot instances are **created** by user-backend but **updated** by bot-backend during execution.

---

### Bot-Backend Tables (Write Authority)

#### Trade Execution

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `trade_fills` | Multi | Executed trades | id, symbol, side, quantity, price, pnl, strategy, broker_id, timestamp |
| `events` | Multi | Trading audit log | id, timestamp_utc, run_id, symbol, event_type, action, details_json |

#### Trading State (Per-Bot)

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `symbol_state` | Multi | Per-symbol trading state | symbol, bot_instance_id, position, entry_price, quantity, stop_loss |
| `daily_state` | Multi | Daily risk state (legacy) | day, bot_instance_id, realized_pnl, kill, trade_count |
| `bot_daily_state` | Multi | Per-bot daily state | bot_instance_id, day, realized_pnl, kill, trade_count |

**Migration Note**: `bot_daily_state` is the new table; `daily_state` is legacy (both exist for backward compatibility).

#### Performance Tracking (Phase 3 Updates)

> **IMPORTANT**: As of Phase 3 Analytics Hardening, `trade_fills` is the **single source of truth** for all execution and performance analytics. 

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `strategy_performance` | Multi | **[DEPRECATED]** Aggregated performance | strategy, symbol, broker_id... |
| `signal_outcomes` | Multi | **[DEPRECATED]** Signal tracking | strategy, symbol, confidence... |
| `weekly_snapshots` | Multi | Weekly equity snapshots | week_start_date (PK), start_equity, peak_equity, low_equity |
| `monthly_snapshots` | Multi | Monthly equity snapshots | month_start_date (PK), start_equity, peak_equity, low_equity |

#### Decision Logging

| Table | Rows | Purpose | Key Columns |
|-------|------|---------|-------------|
| `decision_logs` | Multi | Trade decision pipeline | id, config_id, symbol, strategy_signal_json, final_action, created_at |

**Purpose**: Comprehensive audit trail of strategy signals, risk gates, sizing decisions, and execution results.

---

## Shared Tables (Both Services Read)

| Table | User-Backend | Bot-Backend | Notes |
|-------|--------------|-------------|-------|
| `users` | Write | Read | Auth checks in bot-backend |
| `broker_accounts` | Write | Read | Bot reads for account metadata |
| `broker_credentials` | Write | **Read + Decrypt** | **CRITICAL**: Bot decrypts for trading |
| `strategies` | Write | Read | Bot loads strategy code |
| `strategy_versions` | Write | Read | Bot uses versioned strategies |
| `user_strategy_configs` | Write | Read | Bot reads config for execution |
| `risk_parameters` | Write | Read | Bot reads for risk management |
| `strategy_parameters` | Write | Read | Bot reads for strategy params |
| `protection_state` | Write | Read/Write | Bot checks and updates protection state |
| `bot_instances` | Write | Read/Write | User creates, bot updates runtime state |

---

## Schema Highlights

### Encrypted Credentials

**Table**: `broker_credentials`
```sql
CREATE TABLE broker_credentials (
    account_id TEXT PRIMARY KEY,
    encrypted_blob TEXT NOT NULL,  -- Fernet encrypted JSON
    key_metadata TEXT,              -- "fernet_v1"
    updated_at TEXT NOT NULL,
    FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
);
```

**Encrypted Blob Structure** (after decryption):
```json
{
  "api_key": "binance_api_key",
  "api_secret": "binance_api_secret",
  "environment": "testnet"
}
```

**Encryption Method**: Fernet symmetric encryption using `CREDENTIAL_KEY` from `.env`.

---

### Multi-Bot State Isolation

**Table**: `bot_daily_state`
```sql
CREATE TABLE bot_daily_state (
    bot_instance_id TEXT NOT NULL,
    day TEXT NOT NULL,
    realized_pnl REAL DEFAULT 0.0,
    kill INTEGER DEFAULT 0,        -- 1 = kill switch active
    trade_count INTEGER DEFAULT 0,
    last_updated_at TEXT,
    PRIMARY KEY (bot_instance_id, day)
);
```

**Purpose**: Each bot instance maintains independent daily state for risk limits.

---

### Bot Instance Configuration

**Table**: `bot_instances`
```sql
CREATE TABLE bot_instances (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    broker_account_id TEXT NOT NULL,
    market_type TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    config_id TEXT NOT NULL,         -- Links to user_strategy_configs
    risk_profile_id TEXT NOT NULL,
    symbols_json TEXT NOT NULL,      -- ["BTCUSDT", "ETHUSDT"]
    timeframes_json TEXT NOT NULL,   -- ["1m", "5m"]
    allocation_type TEXT NOT NULL,   -- "fixed_amount" or "percent_balance"
    allocation_value REAL NOT NULL,  -- USDT amount or percentage
    mode TEXT NOT NULL,              -- "paper" or "live"
    status TEXT NOT NULL,            -- "draft", "running", "paused", "stopped"
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    stopped_at TEXT,
    last_run_at TEXT,                -- Updated by bot-backend on each cycle
    last_error TEXT,
    total_trades INTEGER DEFAULT 0,
    active_positions INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(broker_account_id) REFERENCES broker_accounts(id),
    FOREIGN KEY(config_id) REFERENCES user_strategy_configs(id)
);
```

---

## Important Indexes

```sql
-- Performance-critical indexes
CREATE INDEX idx_bot_instances_user_id ON bot_instances(user_id);
CREATE INDEX idx_bot_instances_status ON bot_instances(status);
CREATE INDEX idx_broker_accounts_user_id ON broker_accounts(user_id);
CREATE INDEX idx_trade_fills_symbol ON trade_fills(symbol);
CREATE INDEX idx_decision_logs_config_id ON decision_logs(config_id);
CREATE INDEX idx_decision_logs_created_at ON decision_logs(created_at);
```

---

## Migration History

All migrations are in `shared_lib/persistence/migrations.py::migrate()`.

**Key Migrations**:
1. Base trading tables (`trade_fills`, `strategy_performance`, `events`)
2. Auth & identity tables (`users`, `auth_sessions`, `email_verifications`)
3. Broker connection tables (`broker_accounts`, `broker_credentials`)
4. Billing tables (`subscriptions`, `invoices`)
5. Onboarding (`onboarding_profiles`)
6. Strategy management (`strategies`, `strategy_versions`)
7. User configurations (`user_strategy_configs`, `risk_parameters`, `protection_state`)
8. Bot instances (`bot_instances`)
9. Multi-bot state (`bot_daily_state`)
10. Decision logging (`decision_logs`)

---

## Database File Locations

```
backends/
├── user-backend/
│   └── data/
│       └── bot.db          <-- User-backend database
└── bot-backend/
    └── data/
        └── bot.db          <-- Bot-backend database
```

> ⚠️ **CRITICAL**: These are SEPARATE FILES with IDENTICAL SCHEMAS.  
> Changes in one do NOT sync to the other.

**Recommended Fix**: Use a single shared database file or implement database replication.

---

## Connection Configuration

**User-Backend**: `user-backend/app/core/config.py`
```python
DATABASE_URL: str = "sqlite:///../bot.db"
# Resolves to: user-backend/data/bot.db
```

**Bot-Backend**: `bot-backend/app/core/config.py`
```python
DATABASE_URL: str = "sqlite:///../bot.db"
# Resolves to: bot-backend/data/bot.db
```

**Shared DB Class**: `shared/shared_lib/persistence/db.py`
```python
class DB:
    def __init__(self, db_url: str | None = None):
        self.db_url = db_url or settings.DATABASE_URL
        # Uses sqlite3 with WAL mode enabled
```

---

## Summary

- **Total Tables**: 36 tables
- **User-Backend Owns**: 24 tables (auth, billing, broker, strategies, configs)
- **Bot-Backend Owns**: 10 tables (trades, events, performance, state)
- **Shared Access**: 12 tables (read by both, written by one)
- **Critical Security Table**: `broker_credentials` (encrypted, read by both)
- **Database Files**: 2 separate files with identical schemas (synchronization risk)
