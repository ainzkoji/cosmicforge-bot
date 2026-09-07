# Crypto Signal Center — Operational Handover

**Phase:** 12 (Final Validation & Handover)
**Date:** 2026-05-23
**Status:** Ready for controlled dev/staging validation

---

## 1. What This Feature Is

The Crypto Signal Center is a **manual-only crypto trade signal system**. It generates, stores, and displays structured trading signals for a controlled V1 crypto seed list, while the full-version foundation now records a larger pair universe for future safe expansion. Users receive signals as informational guidance only.

**This system does NOT:**
- Place orders
- Open or close positions
- Connect to broker APIs
- Interface with TradingView execution queues
- Trigger Auto Pilot or any automated trading

---

## 2. Signal Lifecycle

```
PENDING_ENTRY
    └─▶ ACTIVE (entry zone touched)
          ├─▶ TP1_HIT  (partial progress — NOT a WIN)
          │     └─▶ TP2_HIT  (WIN)
          │           └─▶ TP3_HIT  (strong WIN)
          ├─▶ TP2_HIT  (WIN, skips TP1)
          ├─▶ SL_HIT   (LOSS)
          ├─▶ EXPIRED  (time expired — TP2 by expiry = WIN, TP1-only = EXPIRED)
          ├─▶ AMBIGUOUS (TP and SL both hit on same candle — conservative result)
          └─▶ CANCELLED (admin action)
INVALIDATED (admin/manual lifecycle action only)
```

**TP/SL rules:**
| Level | Multiplier | Result |
|-------|-----------|--------|
| TP1   | 1.5 × risk | Partial progress only (NOT WIN) |
| TP2   | 2.0 × risk | WIN |
| TP3   | 3.0 × risk | Strong WIN |
| SL    | 1.0 × risk | LOSS |

Risk/reward ratio is always calculated using TP2 (not TP1 or TP3).

**Dynamic latest-entry window:** `expires_at` represents the latest recommended entry time for the user, not necessarily the expected full trade duration. New signals start from compact V1 entry-window caps by timeframe:
- 15m: 45 minutes
- 30m: 90 minutes
- 1h: 120 minutes
- 4h: 360 minutes
- unknown/missing timeframe: 120 minutes

The engine then adjusts the window mathematically by ATR volatility and stop-distance risk. Low ATR volatility can increase the base by 25% but never above the timeframe cap. High ATR volatility reduces the window by 25%, extreme volatility reduces it by 50%, and very tight stop distance reduces it by 25%. The final value is clamped to at least 15 minutes and no more than the timeframe cap.

**Pre-entry invalidation:** pending signals are removed from active visibility early if the stop is touched, TP1/TP2/TP3 is touched before entry, price drifts too far from entry, or volatility spikes. These market-invalidated signals move to `SL_HIT` or `INVALIDATED` so `/dashboard/signals` only shows fresh entry opportunities.

**Intraday duration gate:** Crypto Signal Center is designed for daily/intraday trading signals. It should not publish multi-day swing signals. Every valid setup must have an estimated TP2 duration under 24 hours based on ATR/market movement. The engine estimates `distance_to_tp2 / ATR`, converts that candle count into minutes for the signal timeframe, and rejects setups above 1440 minutes with `EXPECTED_DURATION_TOO_LONG`.

---

## 3. Pair Universe and Tiers

Current live signal generation still defaults to the original controlled V1 seed list:

BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, ADAUSDT, DOGEUSDT, LINKUSDT, AVAXUSDT, LTCUSDT

The full-version expansion foundation adds pair tiers without blindly expanding live generation:

- `TIER_1`: BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT.
- `TIER_2`: curated high-liquidity altcoins such as ADAUSDT, DOGEUSDT, LINKUSDT, AVAXUSDT, LTCUSDT, DOTUSDT, NEARUSDT, ATOMUSDT, AAVEUSDT, APTUSDT, SUIUSDT, INJUSDT, OPUSDT, ARBUSDT, MATICUSDT, FILUSDT, UNIUSDT, ETCUSDT, BCHUSDT, TRXUSDT, XLMUSDT, HBARUSDT, ICPUSDT, RNDRUSDT, TIAUSDT, SEIUSDT, WIFUSDT, ORDIUSDT, FETUSDT, GRTUSDT.
- `TIER_3` / `DISCOVERED`: exchange-discovered USDT perpetual pairs that must pass strict liquidity, spread, history, volatility, reliability, and blacklist filters before future use.

Discovery and tier storage are preparation only. They do not auto-enable all discovered symbols for signal generation.

---

## 4. Quality Gates (Auto-Publish Validation)

A real signal must pass all of these before the engine auto-publishes it. The admin publish endpoint remains available only for manual recovery/testing and enforces the same gates:

| Gate | Threshold | Error Code |
|------|-----------|-----------|
| Confidence score | ≥ 70.0 | LOW_CONFIDENCE |
| Risk/reward (TP2) | ≥ 1.8 | LOW_RISK_REWARD |
| Entry price | present | MISSING_ENTRY_PRICE |
| Stop loss | present | MISSING_STOP_LOSS |
| Take profit 1 | present | MISSING_TP1 |
| Expiry | present and future | MISSING_EXPIRY / SIGNAL_EXPIRED |
| Status | PENDING_ENTRY or ACTIVE | SIGNAL_CANCELLED / SIGNAL_INVALIDATED |
| Dev signal in production | not allowed | DEV_SIGNAL_BLOCKED_IN_PRODUCTION |
| Already published | not re-publishable through manual endpoint | ALREADY_PUBLISHED |

Pair discovery safety filters currently require: `TRADING` status, `USDT` quote asset, `PERPETUAL` contract type, not blacklisted, 24h quote volume at least 50,000,000, spread at or below 0.20%, at least 200 candles when candle validation is enabled, non-extreme ATR/candle volatility, and reliable candle data. Unsafe pairs are stored with skip reasons such as `LOW_VOLUME`, `SPREAD_TOO_WIDE`, `INSUFFICIENT_HISTORY`, `BLACKLISTED_SYMBOL`, `EXTREME_VOLATILITY`, or `UNRELIABLE_CANDLES`.

---

## 5. Database Tables (SQLite)

Signal tables are created by the shared migration at `backends/shared/shared_lib/persistence/migrations.py`.

| Table | Purpose |
|-------|---------|
| `signal_candidates` | All generated candidates (accepted + rejected) |
| `trading_signals` | Accepted real signals (`is_published=1` immediately); dev/manual recovery records may remain unpublished |
| `signal_performance` | Per-signal TP/SL/expiry outcome tracking |
| `signal_delivery` | User delivery receipts (future use) |
| `user_signal_preferences` | Per-user signal settings (future use) |
| `signal_pair_universe` | Known symbols, tiers, enabled/whitelist/blacklist state |
| `signal_pair_metrics` | Liquidity, spread, candle, volatility, and reliability metrics |
| `signal_scan_runs` | Discovery/generation/status scan summaries |
| `signal_scan_results` | Per-symbol scan outcomes and skip reasons |

---

## 6. Key File Locations

### Backend (Bot)
| File | Purpose |
|------|---------|
| `backends/bot-backend/app/signals/crypto_signal_engine.py` | Main engine: scans symbols, creates candidates + signals |
| `backends/bot-backend/app/signals/pair_discovery.py` | Read-only pair discovery, tier seeding, liquidity/spread/history safety filters |
| `backends/bot-backend/app/signals/signal_risk.py` | TP/SL calc, confidence scoring, stop distance validation |
| `backends/bot-backend/app/signals/signal_performance.py` | Status updater: entry trigger, TP/SL/expiry detection |
| `backends/bot-backend/scripts/generate_daily_crypto_signals.py` | Daily signal generation CLI |
| `backends/bot-backend/scripts/discover_signal_pairs.py` | Read-only pair discovery CLI for future expansion preparation |
| `backends/bot-backend/scripts/update_signal_statuses.py` | Periodic status update CLI |

### Backend (User API)
| File | Purpose |
|------|---------|
| `backends/user-backend/app/api/signals.py` | User-facing signal endpoints |
| `backends/user-backend/app/api/admin_signals.py` | Admin signal management endpoints |
| `backends/user-backend/app/api/admin_signal_pairs.py` | Admin pair universe, metrics, discovery refresh, and scan-run endpoints |

### Shared
| File | Purpose |
|------|---------|
| `backends/shared/shared_lib/persistence/signals.py` | All signal persistence functions |
| `backends/shared/shared_lib/persistence/migrations.py` | Schema migrations (idempotent) |

### Frontend
| File | Purpose |
|------|---------|
| `frontends/user-frontend/src/pages/Signals.tsx` | User signal dashboard |
| `frontends/admin-frontend/src/pages/admin/Signals.tsx` | Admin signal management UI |
| `frontends/admin-frontend/src/pages/admin/SignalPairs.tsx` | Admin pair universe control UI |

---

## 7. Operational Scripts

### Generate daily signals
```bash
python backends/bot-backend/scripts/generate_daily_crypto_signals.py \
  --db-path /path/to/app.db \
  [--symbol BTCUSDT ETHUSDT] \
  [--dry-run]
```

Output JSON: `scanned_symbols`, `candidates_created`, `accepted`, `rejected`, `signals_created`, `published`, `errors`

- `published` equals the number of valid real signals auto-published by the engine
- `scan_run_id` is included for DB-writing generation runs; each scanned/skipped symbol receives a `signal_scan_results` row
- `dry_run=True` → no database writes and no scan-run logging
- Duplicate prevention: a second run for the same symbols produces 0 new signals (DUPLICATE_SIGNAL rejection)
- Controlled eligible-universe mode is available but opt-in only: `--use-eligible-universe --max-symbols 50 [--tiers TIER_1,TIER_2] [--min-volume 50000000] [--max-spread 0.20]`
- Default generation remains the controlled seed list; discovered pairs are not scanned unless explicitly selected through eligible-universe mode or a safe CLI override.
- Larger scans are chunked with `--chunk-size` and `--sleep-between-chunks`, with bounded retry/backoff for temporary market-data errors.
- The generation path uses an in-memory candle cache for each run to avoid duplicate kline fetches for the same symbol/timeframe/limit.
- Accepted candidates are ranked before publishing. Ranking weighs confidence, risk/reward, liquidity, spread, volatility suitability, expected TP2 duration, and neutral symbol performance until symbol stats exist.
- Publishing limits default to `--max-published-per-scan 5`, `--max-active-signals 10`, and `--max-signals-per-symbol-per-day 1`. Valid candidates over the limit remain non-user-visible instead of being auto-published.

### Update signal statuses
```bash
python backends/bot-backend/scripts/update_signal_statuses.py \
  --db-path /path/to/app.db
```

Output JSON: `checked`, `expired`, `entry_triggered`, `tp1_hit`, `tp2_hit`, `tp3_hit`, `sl_hit`, `ambiguous`, `errors`

Run this on a schedule (e.g., every 15–30 minutes) to keep signal statuses current.

### Create mock dev signals (non-production only)
```bash
DEV_SIGNAL_MODE=true python backends/bot-backend/scripts/generate_daily_crypto_signals.py \
  --create-mock-dev-signals \
  --symbol BTCUSDT
```

Dev signals are stored with `dev_mode=1` and source `dev_mock_signal_engine`. They are:
- Hidden from user APIs in production (`APP_ENV=production`)
- Blocked from admin publish in production
- Visible in admin UI in non-production

---

## 8. Admin Workflow

1. **Monitor** generated signals at `/admin/signals` → "Generated Signals" tab
2. **Inspect** confidence score, risk/reward, entry zone, TP/SL levels
3. **Review** automatically published real signals via "Published Signals" tab
4. **Review** unpublished/manual/dev records via "Unpublished / Manual Review"
5. **Cancel** a signal if needed (clears `is_published`, marks performance as CANCELLED, removes from user active list)
6. **Unpublish** a signal without cancelling (removes from user view, preserves data)
7. **Publish** endpoint remains for manual recovery/testing only, not the normal real-signal flow
8. **Manage pair eligibility** at `/admin/signals/pairs` by enabling/disabling, whitelisting, blacklisting, refreshing discovery, and inspecting scan-run skip reasons

Pair universe controls are discovery and eligibility controls only. Refresh Discovery updates pair universe and metrics; it does not generate signals, publish signals, or touch any execution system.

---

## 9. User Workflow

1. Log in and navigate to `/dashboard/signals`
2. **Active** tab: currently open signals with time remaining
3. **Completed** tab: finished signals (TP2/TP3 wins, SL, expired)
4. Use filters for search, side, timeframe, confidence, status, favorites-only, majors-only, hidden symbols, and sort order
5. Manage display preferences: favorite symbols, hidden symbols, minimum confidence, risk style, and in-app notification preferences
6. Read the signal detail for entry zone, stop loss, TP targets, and disclaimer
5. Manually execute in their own brokerage if they choose to — the platform does not trade for them

---

## 10. API Endpoints

### User API (`/api/signals/`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/signals` | List all published signals |
| GET | `/api/signals/active` | Active signals (excludes expired-by-time) |
| GET | `/api/signals/history` | Completed signals (TP2, TP3, SL, EXPIRED, CANCELLED) |
| GET | `/api/signals/{id}` | Signal detail (includes `time_left_seconds`, `disclaimer`) |
| GET | `/api/signals/performance` | Aggregate performance stats |
| GET | `/api/signals/preferences` | Get/create user signal display and notification preferences |
| PUT | `/api/signals/preferences` | Update user signal preferences |
| POST | `/api/signals/preferences/favorites/{symbol}` | Add favorite signal symbol |
| DELETE | `/api/signals/preferences/favorites/{symbol}` | Remove favorite signal symbol |
| POST | `/api/signals/preferences/hidden/{symbol}` | Hide a signal symbol |
| DELETE | `/api/signals/preferences/hidden/{symbol}` | Unhide a signal symbol |
| GET | `/api/signals/notifications` | List in-app/broadcast signal notification records |
| POST | `/api/signals/notifications/{id}/read` | Mark an in-app signal notification read |

Signal list endpoints support scalable user filters: `search`, `side`, `timeframe`, `status`, `min_confidence`, `sort`, `favorites_only`, `majors_only`, and `include_hidden`.

`signal_notifications` is an in-app/event foundation only. It does not send email, Telegram, WhatsApp, broker, or TradingView messages.

### Admin API (`/api/admin/signals/`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/admin/signals` | List all signals (with `?dev_mode=1` filter) |
| GET | `/api/admin/signals/candidates` | List all candidates |
| POST | `/api/admin/signals/{id}/publish` | Manual recovery/testing publish (gates enforced) |
| POST | `/api/admin/signals/{id}/unpublish` | Unpublish a signal |
| POST | `/api/admin/signals/{id}/cancel` | Cancel a signal |
| GET | `/api/admin/signals/pairs` | List pair universe records |
| GET | `/api/admin/signals/pairs/metrics` | List pair liquidity/spread/safety metrics |
| POST | `/api/admin/signals/pairs/{symbol}/enable` | Enable a known pair |
| POST | `/api/admin/signals/pairs/{symbol}/disable` | Disable a known pair |
| POST | `/api/admin/signals/pairs/{symbol}/blacklist` | Blacklist a known pair; blacklist overrides whitelist |
| POST | `/api/admin/signals/pairs/{symbol}/whitelist` | Whitelist a known pair without bypassing safety metrics |
| POST | `/api/admin/signals/pairs/refresh` | Refresh pair discovery/metrics only; no signal generation |
| GET | `/api/admin/signals/scan-runs` | List discovery/generation/status scan runs |
| GET | `/api/admin/signals/scan-runs/{id}` | Inspect one scan run and per-symbol results |

---

## 11. Environment Variables

| Variable | Effect |
|----------|--------|
| `APP_ENV=production` | Hides dev signals from users; blocks dev publish |
| `ENVIRONMENT=production` | Same effect as APP_ENV |
| `ENV=production` | Same effect |
| `NODE_ENV=production` | Same effect |
| `DEV_SIGNAL_MODE=true` | Required to create mock dev signals |

---

## 12. Confidence Scoring

Confidence is deterministic and components sum to 100. Minimum threshold is 70.0.

Components evaluated per signal:
- Trend alignment
- Volume confirmation
- Strategy agreement (ensemble)
- Risk/reward quality

---

## 13. What Is NOT Connected

The following systems are **completely isolated** from the Crypto Signal Center:

- Binance / Bybit order execution
- TradingView webhook execution queue
- Auto Pilot trading engine
- Broker authentication / API key system
- Position manager
- Trade sizing engine

This has been verified by source scan of all 8 signal modules — zero forbidden references found.

---

## 14. Known Pre-existing Test Failures (Unrelated)

Two test files fail due to pre-existing non-signal issues:

| Test File | Error | Cause |
|-----------|-------|-------|
| `test_adaptive_engine_validation.py` | `timestamp_utc` column missing | Unrelated schema issue |
| `test_break_even_update.py` | `TypeError: Pos...` | Unrelated position manager issue |

These failures predate the Crypto Signal Center and are not regression indicators for this feature.

---

## 15. Phase 11 & 12 Test Coverage

**Bot-backend signal suite:** 153 passed (22.74s)
- `test_crypto_signal_engine.py`
- `test_signal_lifecycle_update.py`
- `test_generate_daily_crypto_signals.py`
- `test_phase11_signal_center.py`
- `test_tradingview_webhook.py`
- `test_external_signal_processor.py`

**User-backend signal suite:** 30 passed (11.03s)
- `test_signals_api.py`
- `test_admin_signals_api.py`
- `test_phase11_signal_center.py`

**Frontend builds:** Both pass (TypeScript + Vite)
- `frontends/user-frontend`: 2905 modules, built in ~10s
- `frontends/admin-frontend`: 2943 modules, built in ~10s

---

## 16. Deployment Checklist

Before deploying to production:

- [ ] Run `python backends/shared/shared_lib/persistence/migrations.py` against production DB (idempotent)
- [ ] Set `APP_ENV=production` in user-backend and admin-backend environment
- [ ] Do NOT set `DEV_SIGNAL_MODE=true` in production
- [ ] Schedule `update_signal_statuses.py` (every 15–30 minutes recommended)
- [ ] Schedule `generate_daily_crypto_signals.py` (once per day or per session)
- [ ] Verify `/api/admin/signals` is protected by `require_admin` dependency
- [ ] Verify `/api/signals` requires authenticated user (`get_current_active_user`)
- [ ] Confirm `published` field in generation script output equals valid real auto-published signals

---

## 16A. Scheduled Operations And Expanded Pair Rollout

Scheduler-ready defaults are defined in `backends/bot-backend/app/signals/signal_scheduler_config.py`.

Recommended schedules:
- Status updater: every 5 minutes.
- Pair discovery: daily, or every 6-24 hours.
- Signal generation: 07:00 UTC, 12:00 UTC, 16:00 UTC, and 20:00 UTC.

Linux cron examples:

```bash
*/5 * * * * cd /path/to/backends/bot-backend && python scripts/update_signal_statuses.py --scheduled
0 7,12,16,20 * * * cd /path/to/backends/bot-backend && python scripts/generate_daily_crypto_signals.py --scheduled --use-eligible-universe --tiers TIER_1,TIER_2 --max-symbols 50 --max-published-per-scan 5 --max-active-signals 10 --min-volume 50000000 --max-spread 0.20
0 2 * * * cd /path/to/backends/bot-backend && python scripts/discover_signal_pairs.py --scheduled --min-volume 50000000 --max-spread 0.20 --skip-candle-validation
```

Windows Task Scheduler should use the same commands with the working directory set to `backends/bot-backend`.

Operational safety:
- Scheduled scripts use `signal_operation_locks` to prevent overlapping `SIGNAL_GENERATION`, `PAIR_DISCOVERY`, and `STATUS_UPDATE` runs.
- Locks have TTLs so crashed runs can be taken over safely.
- Pause settings live in `signal_system_settings`: `signal_generation_paused`, `pair_discovery_paused`, and `status_updater_paused`.
- Manual/admin runs may use `--ignore-pause` explicitly; scheduled/default runs respect pause.
- Dry-run remains write-free.

Expanded rollout:
- `V1_SEED_ONLY`: original 10 controlled symbols.
- `TIER_1_ONLY`: major symbols only.
- `TIER_1_TIER_2`: curated Tier 1 + Tier 2 rollout, safe metrics required.
- `TIER_1_TIER_2_TIER_3`: future dynamic mode; not enabled by default and requires explicit Tier 3 enablement.

Current recommended rollout mode is `TIER_1_TIER_2`, but Tier 3 remains disabled until 1-2 weeks of stable Tier 1/Tier 2 monitoring shows acceptable scan duration, rate-limit behavior, expired/invalidated rate, SL rate, and TP2/TP3 performance.

Monitoring for the rollout should use existing `trading_signals`, `signal_performance`, `signal_scan_runs`, and `signal_scan_results` data. A dedicated rollout health report script is deferred; the current tables already expose published counts, rejection reasons, skipped symbols, scan errors, SL/TP/expired/invalidated results, and scan duration.

---

## 17. Rollback Plan

The Crypto Signal Center uses only additive database tables and new API routes. Rollback steps:

1. Remove the 5 new signal tables (or simply stop routing to them)
2. Remove `signals.py` and `admin_signals.py` from the router includes
3. Hide `Signals.tsx` from the user navigation
4. Hide `admin/Signals.tsx` from the admin navigation

No existing tables, routes, or execution flows are modified by this feature.

---

*Crypto Signal Center is ready for controlled dev/staging validation. Full production rollout should wait until wider unrelated bot test failures are reviewed.*
