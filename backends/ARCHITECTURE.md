# CosmicForge System Architecture

**Last Updated:** 2026-01-20  
**Purpose:** Document service boundaries, data ownership, and security patterns

---

## Service Overview

### User-Backend (Port 8000)
- **Purpose**: User-facing API for authentication, account management, billing, KYC, and strategy configuration
- **Public Access**: YES - Internet-facing  
- **Primary Responsibility**: User identity, billing, broker account management, strategy catalog

### Bot-Backend (Port 9000)
- **Purpose**: Trading execution engine and bot orchestration
- **Public Access**: **NO - Should be internal only**
- **Primary Responsibility**: Trade execution, bot instances, performance tracking, exchange API interaction

---

## Database Architecture

### Shared Database Pattern
**Both services use the SAME SQLite database**: `bot.db`

**Location:**
- User-backend: `backends/user-backend/data/bot.db`
- Bot-backend: `backends/bot-backend/data/bot.db`

> ⚠️ **CRITICAL**: Both backends currently point to separate database files but use *identical schema migrations*. This creates data synchronization risks.

### Migration System
Both services use the same migration script from `shared_lib/persistence/migrations.py`:
- **Shared Migration**: Ensures schema consistency
- **Applied at startup**: Both services run migrations independently
- **Risk**: Race conditions if both services modify DB simultaneously

---

## Data Ownership by Service

### User-Backend Owns (Writes)

| Table | Purpose | Writer Service |
|-------|---------|----------------|
| `users` | User accounts, credentials | user-backend |
| `auth_sessions` | JWT refresh tokens | user-backend |
| `email_verifications` | Email verification codes | user-backend |
| `password_resets` | Password reset tokens | user-backend |
| `login_attempts` | Rate limiting | user-backend |
| `auth_audit_log` | Auth event log | user-backend |
| `broker_accounts` | Broker connection metadata | user-backend |
| `broker_credentials` | **Encrypted API keys** | user-backend |
| `broker_audit_log` | Broker connection events | user-backend |
| `subscriptions` | Stripe billing subscriptions | user-backend |
| `invoices` | Billing history | user-backend |
| `pricing_intents` | Marketing funnel tracking | user-backend |
| `onboarding_profiles` | Onboarding wizard state | user-backend |
| `strategies` | Strategy catalog | user-backend |
| `strategy_versions` | Strategy code versions | user-backend |
| `user_strategy_configs` | User strategy instances | user-backend |
| `risk_parameters` | Risk settings per config | user-backend |
| `strategy_parameters` | Strategy overrides | user-backend |
| `protection_state` | Account protection state | user-backend |
| `bot_instances` | Bot deployment records | user-backend |
| `official_strategies_cache` | Marketplace cache | user-backend |

### Bot-Backend Owns (Writes)

| Table | Purpose | Writer Service |
|-------|---------|----------------|
| `trade_fills` | Executed trades | bot-backend |
| `events` | Trading audit log | bot-backend |
| `daily_state` | Daily risk state (legacy) | bot-backend |
| `symbol_state` | Per-symbol trading state | bot-backend |
| `strategy_performance` | Performance metrics | bot-backend |
| `signal_outcomes` | Signal tracking | bot-backend |
| `weekly_snapshots` | Weekly equity snapshots | bot-backend |
| `monthly_snapshots` | Monthly equity snapshots | bot-backend |
| `decision_logs` | Trade decision pipeline | bot-backend |
| `bot_daily_state` | Per-bot daily state | bot-backend |

### Shared Read Access

Both services READ from:
- `users` (for authentication)
- `broker_accounts` + `broker_credentials` (for trading)
- `strategies`, `strategy_versions` (for strategy execution)
- `user_strategy_configs`, `risk_parameters` (for bot configuration)
- `bot_instances` (user-backend for UI, bot-backend for execution)

---

## Broker Credential Access Flow

### How Bot-Backend Obtains Credentials for Trading

**The Pattern: Direct Shared Database Access**

```
1. User submits credentials via user-backend API:
   POST /api/brokers/{account_id}/credentials
   ↓
2. User-backend encrypts and stores in broker_credentials table:
   - Encryption: Fernet symmetric encryption
   - Key: CREDENTIAL_KEY from .env (same key in both services)
   - Storage: broker_credentials.encrypted_blob
   ↓
3. Bot starts trading via bot-backend:
   MultiBotRunner.run_once()
   ↓
4. Bot-backend queries shared database directly:
   get_decrypted_credentials(broker_account_id)
   ↓
5. Decrypts using same CREDENTIAL_KEY
   ↓
6. Creates BinanceFuturesClient with decrypted keys
   ↓
7. Executes trades
```

**Code Location:**
- **Encryption**: `user-backend/app/core/broker_security.py`
- **Storage**: `user-backend/app/core/broker_service.py::submit_broker_credentials()`
- **Retrieval**: `bot-backend/app/core/broker_service.py::get_decrypted_credentials()`
- **Usage**: `bot-backend/app/runner/multi_runner.py::run_once()` (line 89)

> ⚠️ **SECURITY NOTE**: Bot-backend has FULL access to encrypted credentials table. No API boundary enforces access control.

---

## Service Boundary Rules

### ✅ Allowed Cross-Service Patterns

| Pattern | Direction | Purpose |
|---------|-----------|---------|
| **Shared DB Read** | Both ↔ DB | Authentication, configuration, credentials |
| **User-backend proxies to bot-backend** | user → bot | `/api/v1/bot-instances/*` endpoints |
| **User-backend proxies to bot-backend** | user → bot | `/api/v1/strategies/marketplace/*` endpoints |

### ❌ Prohibited Patterns (Not Currently Enforced)

| Anti-Pattern | Risk |
|--------------|------|
| Bot-backend writing to user/billing tables | Data integrity violation |
| Bot-backend modifying broker_credentials | Security breach |
| User-backend writing to trade_fills | Audit trail corruption |
| Direct internet access to bot-backend | Security exposure |

### 🔒 Security Boundaries That MUST Be Enforced

1. **Bot-Backend Must Be Internal Only**
   - No public internet exposure
   - Access only via user-backend proxy or internal network
   - Rationale: Holds decrypted credentials in memory during execution

2. **Credential Encryption**
   - CREDENTIAL_KEY must be same in both .env files
   - Must be 32+ bytes for Fernet
   - Rotate keys require re-encrypting all credentials

3. **Database Transaction Isolation**
   - Both services use SQLite with WAL mode (recommended)
   - Risk: Concurrent writes may cause locks
   - Mitigation: User-backend writes config, bot-backend writes execution data

---

## Current Proxying Architecture

### User-Backend Proxies to Bot-Backend

**Endpoints:**
```
/api/v1/bot-instances/*           → bot-backend:9000/api/v1/bot-instances/*
/api/v1/strategies/marketplace/*  → bot-backend:9000/api/v1/strategies/marketplace/*
```

**Implementation:**
- File: `user-backend/app/api/bot_instances_proxy.py`
- File: `user-backend/app/api/strategies_proxy.py`
- Method: HTTP proxy using `httpx.AsyncClient`

**Why Proxy?**
- Bot-backend should not be publicly accessible
- User authentication happens at user-backend
- User-backend validates JWT before forwarding request

---

## Recommended Improvements

### 1. Database Separation (Future)
- **Current**: Both services share same `bot.db` file
- **Recommended**: Separate databases with clear ownership
  - `user.db`: Users, auth, billing, broker_accounts, broker_credentials
  - `trading.db`: trade_fills, events, performance metrics
- **Credential Access**: Bot-backend calls user-backend API to decrypt credentials (not direct DB)

### 2. Network Security
- **Current**: Bot-backend port 9000 may be accessible
- **Recommended**: 
  - Firewall bot-backend to localhost/internal network only
  - All external access via user-backend proxy
  - Add API key authentication between services

### 3. Audit Logging
- **Current**: Minimal cross-service audit trail
- **Recommended**: Log all credential decryption events with requestor identity

---

## Configuration Files

### Database Connection Strings

**User-Backend** (`user-backend/app/core/config.py`):
```python
DATABASE_URL: str = "sqlite:///../bot.db"
# Resolves to: user-backend/data/bot.db
```

**Bot-Backend** (`bot-backend/app/core/config.py`):
```python
DATABASE_URL: str = "sqlite:///../bot.db"
# Resolves to: bot-backend/data/bot.db  
```

> ⚠️ **WARNING**: Despite identical schema, these are DIFFERENT FILES. Changes in one don't reflect in the other.

### Encryption Key Configuration

Both `.env` files **MUST** have matching:
```
CREDENTIAL_KEY=changeme_in_production_credential_key_32bytes!!!!
```

---

## Multi-Bot Execution Flow

1. **MultiBotRunner** (bot-backend) runs every 10-15 seconds
2. Queries `bot_instances` table for `status='running'` bots
3. For each bot:
   - Fetches `broker_account_id` from bot_instance
   - Calls `get_decrypted_credentials(broker_account_id)` **directly from shared DB**
   - Loads `user_strategy_configs` and `risk_parameters`
   - Creates `BinanceFuturesClient` with decrypted keys
   - Executes `PaperRunner.run_cycle()` with bot context
   - Writes `trade_fills`, `events`, `bot_daily_state` to DB
   - Updates `bot_instances.last_run_at`, `active_positions`

**Key Insight**: No API calls between services during trading. Everything is direct DB access.

---

## Summary

- **Database**: Shared schema, separate files (risk of drift)
- **Credentials**: Encrypted in user-backend, decrypted in bot-backend via shared DB
- **Service Communication**: User-backend → bot-backend via HTTP proxy for user-facing APIs
- **Security Posture**: Bot-backend should be internal-only (not currently enforced)
- **Data Ownership**: Clear logical separation, but physical access is shared


## API Prefix Standard (Added 2026-01-20)

To ensure consistency across the microservices architecture, we enforce the following routing standard:

### Bot Backend (bot-backend)
All entity-based resources MUST be exposed under the /api/v1 prefix.

- **strategies**: /api/v1/strategies
- **marketplace**: /api/v1/strategies/marketplace
- **bot-instances**: /api/v1/bot-instances
- **analytics**: /api/v1/analytics
- **monitoring**: /api/v1/monitoring
- **strategy-configs**: /api/v1/strategy-configs
- **risk-profiles**: /api/v1/risk-profiles

### User Backend (user-backend)
Acts as the API Gateway for the frontend.
- Proxies requests to bot-backend using the explicit /api/v1 paths.
- Exposes routes to Frontend matching the structure (e.g., /api/v1/monitoring -> proxies to /api/v1/monitoring).

