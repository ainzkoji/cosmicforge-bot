# CosmicForge Tradingbot: DEMO activation and blocker resolution

**Date:** 2026-11-01  **Baseline:** `362e5d8`  **Commits:** `def0bf4`, `68eee73`, `d09421c` (+ this report)
**Account:** `brk_c729454e6c98` (Binance USD-M, DEMO, `https://demo-fapi.binance.com`), bot `bot_a8117dc719fc`

## Summary

| Area | Result |
|---|---|
| Binance DEMO connection | **Repaired.** The cause was this server's own request budget, not Binance, the keys or the endpoint. Account `SYNCED`, reconciliation `SYNCED`. |
| Authenticated account reads | Balance (417.58 USDT wallet, margin and available), positions (flat) and open orders (none) read from Binance DEMO every cycle. |
| DEMO venue certification | **PASSED, recorded `CERTIFIED`** for Binance DEMO: real orders, 30 of 30 checks green, account flat at the end. LIVE stays uncertified. |
| Experimental DEMO authorization | Built, tested, and **active** (owner-approved): `xdemo_a2e102e911db4144b29b`, 72 hours, at most 100 USDT notional per entry. |
| DEMO market-entry policy (L-08) | **Recorded** (owner-approved): policy `81df3fbfd14448cb90da5f6823d75b39`, all symbols, this account only, 100 USDT cap, 7 days, never calibrated. |
| DEMO orders executed | Only the certification orders below. **No strategy order yet**: no qualifying signal has met the bot's settings (see "Entry eligibility"). |
| CATI strategy evaluation | Running hourly on live market data. Latest decision: `NO_SIGNAL` (no symbol reached a score of 2). |
| Entry eligibility | Every account, strategy, venue and policy gate is clear. With the bot's own saved sizing (120 USDT fixed margin), a typical signal is refused: the order (480 USDT notional at 4x, about 6% of equity at risk) exceeds the 100 USDT experimental cap and the 0.40 % per-trade risk limit. The bot's setting was not changed, at the owner's instruction. |
| Frontend | DEMO and LIVE states are now truthful. The customer trading panel was rebuilt, and the admin console has experimental-DEMO and market-policy controls. |
| Tests / CI | See "Tests and CI". |
| LIVE | **Not activated.** No LIVE order is possible in this build; the remaining LIVE requirements are listed at the end. |

## 1. Binance DEMO connection

**What was wrong.** Account reads failed with `READ_FAILED / AdapterError`. A sanitized trace through the real path (credential resolver → client factory → Binance adapter → production sync) showed the following:
- The credentials resolve and decrypt (credential version 2, DEMO environment, canonical DEMO host). The authenticated `/fapi/v2/account` read **succeeds**.
- The next read was refused before it left the process: `AdapterError(RATE_LIMITED, "LOCAL_HTTP_BUDGET_REFUSED")` from `shared_lib/broker/http_budget.py`.
- The budget charged a flat **100 units per Binance request**, against 1,920 usable units per minute. One account cycle (time, exchangeInfo, account, positionRisk, openOrders, openAlgoOrders, income) cost about 800 units.
- Every refusal also rebuilt the client, which spent another exchangeInfo and time sync, so the cycle kept failing.
- The market-data collector shared the same allowance and had been refused 853 times, which starved CATI of candles. Binance itself reported only about 97 weight used.

Ruled out:
- Endpoint selection: the DEMO host was correct.
- Credentials and encryption key: they decrypt, and the account read succeeds.
- Permissions: `canTrade: true`.
- Timestamp and signature: the account read is signed and was accepted.
- Account environment mismatch: DEMO everywhere.

**Fix (`def0bf4`):**
- `BINANCE_ENDPOINT_WEIGHTS`, a versioned catalogue (`binance-usdm-ip-weights-2026-11-01`) of Binance's documented **worst-case** IP weight per path. For example, account-wide openOrders and openAlgoOrders cost 40. The 2024 change log raised `/fapi/v2/account` and positionRisk to 10, and the catalogue uses 10.
  - Order placement costs 0 IP weight at Binance and is charged 1.
  - `fundingRate` has no documented weight; 10 is assumed.
  - Unknown endpoints still fail closed.
  - The 2,400 per minute production limit is kept, even though demo publishes 6,000.
- A local budget refusal no longer rebuilds the client (`client_doubt`).
- A failed read is recorded with a classified, sanitized reason, never the exception text: `broker_read_diagnosis.classify`. Examples:
  - `BROKER_RATE_LIMITED_LOCAL`, `BROKER_CREDENTIALS_REJECTED` (Binance -2015/-2014/401), `BROKER_SIGNATURE_REJECTED`, `BROKER_TIMESTAMP_REJECTED`, `BROKER_CREDENTIAL_DECRYPT_FAILED`, `BROKER_IP_BANNED`.
  - Each has a customer message and action in the reason registry. For rejected credentials the action is the exact reconnection step: a new key with Futures read/trade, no withdrawal permission, the server IP whitelisted.

**Measured after the fix:** reconciliation uses about 280 units per minute (10 requests), down from about 1,500. The account has stayed `SYNCED` and the collector is no longer refused.

## 2. Runtime

- The runtime owner holds the lease. The broker cycle runs every ~30 s, the market-data collector and residual scheduler are alive, and CATI records an evaluation for the account every cycle (`cati_production_decisions`). Each record carries its stage, signal and final result.
- `/health` distinguishes an HTTP-healthy server from a working engine: lease, scheduler, last cycle age, collector heartbeat, last decision age.
- **How the local bot runs now:**
  - It runs from a clean export of `d09421c`, against the same database (`cosmicforge.db`).
  - It adds a local SQLite platform database for order coordination (`PLATFORM_DATABASE_URL=…/platform.db`, migrated to version 61, gitignored) and `BROKER_CYCLE_ASSIGNMENT_MODE=owner`.
  - Without the platform database the spread gate refuses every entry (`PLATFORM_ORDER_BUDGET_UNAVAILABLE`).
  - It runs from an export because an uncommitted contributor change in the working tree removes `platform_runtime.budgeted_archive_get`, which `portfolio_runtime.py` and `runtime_jobs.py` still import. That change was left untouched and uncommitted, as before.
- The admin backend (:8100) had stopped serving: its `--reload` supervisor held the port with a dead worker. It was restarted with `PYTHONPATH=backends/shared`.

## 3. Experimental DEMO authorization

Module `app/trading_intelligence/governance/experimental_demo.py`. Ledger: `cati_experimental_demo_events`, append-only by trigger, hash-chained, classified IMMUTABLE_GOVERNANCE.

- **What a grant waives:** only the family gate's research-certification and stage checks (`pipeline/provenance.py`), for one (account, user, family) on DEMO.
- **What still applies:** registration, rule hash, disabled, kill switch, mode and environment, market, decision verification, venue certification, account ownership and consent, the DEMO order switch, capital, hard risk, the L-08 policy (depth, spread, slippage, worst-price risk), the account fence, native reduce-only stops and reconciliation.
- **Verified DEMO:**
  - The stored environment must be DEMO, the credentials must resolve to DEMO, and the base URL must be the venue's DEMO host.
  - All three are re-checked on every use. A LIVE plan, an account moved to LIVE or a non-DEMO host gets no waiver.
  - There is no LIVE form of a grant.
- **Never certification or promotion:** the module writes only its own ledger. Tests prove the family stays FAILED / M0 with no DEMO or LIVE mode, the governance log is unchanged, the research register is byte-identical and the holdout registry is untouched.
- **Bounded:** each grant records an operator, owner approval reference, reason, duration (1 to 336 h) and notional cap (at most 1,000 USDT).
  - The operator must state the family's current research status (here FAILED).
  - Grants expire on their own. Revocation is immediate, and the boundary re-checks right before the order.
  - The notional cap refuses an order and never resizes it.
- **Labelled:** every order claimed under a grant writes an `ENTRY_CLAIMED` event (trade plan, attempt, symbol, notional). Positions and trades from those plans show as `EXPERIMENTAL_DEMO`. If that label cannot be recorded, the order is refused.
- **Admin actions:**
  - Bot: `GET/POST /api/v1/admin/governance/experimental-demo` and `POST …/{id}/revoke` (operator token).
  - Admin control plane: proxied 1:1 at `/api/admin/governance/experimental-demo…` in the user backend (admin-portal login, `admins` table, minted 60-second operator token, audit rows).
  - Admin console page "Experimental DEMO trading" (`/admin/experimental-demo`).
  - The separate admin-backend service (:8100) is read-only analytics and cannot mint the operator token, so the controls sit on the governance proxy like the existing family controls.
- **Activation:** the owner chose "Claude activates now". It was recorded with a one-time 60-second operator token for the owner's admin id, minted the way the admin proxy mints it. The recorded operator is `operator:b04f9242-…@admin_api:/api/v1/admin/governance`.

## 4. DEMO market-entry policy (L-08)

The existing governed ledger (`platform_market_entry_policy_events`) is used: hash-chained, append-only, single account, never calibrated, terms equal to or tighter than the conservative default, expiry, revocation, LIVE refused in this build. Two changes:

- **All-symbols scope `*`:**
  - Allowed for DEMO only, and only on a record that names its one account.
  - The residual strategy picks one of about 140 symbols each hour, so symbol-by-symbol approval was not workable.
  - A symbol-specific record always takes precedence; a revoked specific record is not silently replaced by `*`.
  - The uncalibrated order budget still allows one account per venue/symbol/side (`ORDER_SPREADING_CALIBRATION_REQUIRED`).
- **Venue-match defect:**
  - Account eligibility compared the platform registry's venue (`binance`) with the plan's (`BINANCE_USDM`). In production that refused every governed entry with `MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE`.
  - Both are now compared as the platform venue they denote.

Recorded policy terms: participation ≤ 5 %, slippage ≤ 20 bps, spread ≤ 20 bps, depth no older than 3 s, ≥ 5 levels, worst-executable-price risk ≤ 0.40 % of equity (gate constant), cap 100 USDT, expires 2026-11-08.

Proxy: `/api/admin/order-spreading/market-entry-policies…` (new, admin-only, audited). Console: the "Market-entry policy (L-08)" section of the same page.

## 5. Binance DEMO order lifecycle certification

- **Dry run:** `scripts/run_venue_certification.py --venue binance --dry-run` → rehearsal PASSED (mocked transport, venue not contacted).
- **Pre-run checks:**
  - The runtime was stopped first, so the runtime lease and port were free.
  - The account was flat: no position, no open order, 417.58 USDT available.
  - A real read-only run passed the account environment, account state, instrument rules, time sync, rate-limit observation and user-stream checks.
- **Genuine run:**

| | |
|---|---|
| Run | `cert_6852df804410b16bee8ad4d1`, source commit `def0bf4`, DEMO host only, leverage 1, symbol ADAUSDT, max notional 60 USDT |
| Operator / authorization | `claude-code-opus-5.5 for owner CosmicforgeHealthnet` / "Owner written brief TRADINGBOT DEMO ACTIVATION 2026-11-01 section 5" |
| Outcome | **PASSED → `CERTIFIED`** (entry, protective stop, stop replacement, cancel, close, user stream). 30 of 30 checks PASS, cleanup complete |
| Evidence | `evidence/binance-demo-certification-20261101/orders/binance_demo_certification_20261101T085334Z.json` (hash-chained register: events, checks, 32 actions) |

What happened at Binance DEMO, from the register:

1. Leverage was set to 1.
2. A reduce-only order with no position was correctly rejected (`-2022`).
3. **Entry:** MARKET BUY 56 ADAUSDT, filled (order 1333417962). The idempotent repeat returned the same order.
4. **Native stop:** STOP_MARKET reduce-only at 0.1841 (Algo Service `algo:1000000268023071`), read back with its geometry checked.
5. **Stop replacement:** a new stop at 0.1889 was confirmed before the old one was cancelled.
6. A resting LIMIT was placed and cancelled. Lost-acknowledgment, connection-interruption, rate-limit and clock-drift drills were run, plus restart with a protected position.
7. An external manual reduce of 28 was reconciled.
8. **Reduce-only exit:** MARKET SELL 28, filled (order 1333418135). The obsolete stop was cancelled.
9. **Final state:** flat, no orders, reconciliation PASS.

The family gate now reads Binance DEMO as certified (`adapter_certified(binance, DEMO)` → True). Binance LIVE remains `NO_CERTIFICATION_EVIDENCE`.

## 6. Autonomous experimental-DEMO trading through CATI

- **Signal source:** the strategy evaluates on its own hourly decisions from live Binance public data. No trade request was fabricated.
- **Before activation:** the 11:00 UTC decision selected 1INCHUSDT LONG (score 2.29). It was refused at the boundary with `GOVERNANCE_NOT_AUTHORIZED / FAMILY_FAILED_RESEARCH`, and earlier evaluations failed on the budget refusal (`AdapterError`).
- **After activation:** the latest decisions are `NO_SIGNAL` (no score ≥ 2). That is recorded as such, and the dashboard shows "No signal".
- **What the next qualifying signal meets** (replayed through the real production path with the exchange mocked, in `tests/trading_intelligence/test_bot_sizing_config.py`):
  1. Hard risk **approves** the bot's own configuration. The 120 USDT fixed margin per trade gives 4x leverage (compound-risk limit 0.225 ÷ 5.33 % stop) and 480 USDT notional, so the loss at the stop is about 25.6 USDT, **6.13 % of equity**.
  2. The experimental grant refuses: **`EXPERIMENTAL_DEMO_NOTIONAL_LIMIT_EXCEEDED:480.0>100.0`**.
  3. Behind it, L-08 refuses: `MARKET_SPREAD_EXCESSIVE` on 1INCH (one tick is wider than the frozen plan's 2 bps spread bound), or **`EXECUTABLE_PRICE_RISK_EXCEEDED`** when the spread passes.
- **Explanation recorded:** the refusal carries the margin, leverage, notional, planned risk against the 0.40 % ceiling, and "your configured trade amount was not changed and the order was not resized". It goes into the evaluation record (`refusal_explanation`) and onto the dashboard.
- **Not yet observed live:** with a size that fits, the lifecycle (sizing → reservation → liquidity → DEMO order → fill → native stop → monitoring → protection updates → exit → P&L → notification) runs on the components certified in section 5. This report does not claim an autonomous strategy trade: none has happened.

### Per-bot sizing audit (owner's instruction: do not change the bot)

- **Saved settings:** `allocation_type=fixed_amount`, `allocation_value=120`, `capital_allocation=120`, `capital_allocation_type=fixed_amount`, `risk_level=balanced`, `risk_profile_id=__auto_pilot__`, `mode=live`, `universe_mode=BROKER`; `max_position_usdt`, `daily_loss_limit_pct` and `risk_profile_version` are NULL. The bot was created 2026-09-07 through the old Auto Pilot form.
- **Meaning:** these are two saved settings that happen to be equal. `capital_allocation` is the allocated capital; `allocation_value` is the **margin per trade** (notional = 120 × leverage). Effective policy, the production factory, the policy engine and the capital ledger agree on this. Neither value is a notional cap.
- **Result:** with 417.58 USDT equity and the strategy's 5 to 15 % stops, no leverage makes a fixed 120 USDT margin fit the 0.40 % per-trade limit; at 1x the loss is still 1.53 %. Trades are refused with that explanation, as instructed. Two routes exist, both through the supported bot-settings workflow and both the owner's choice: a smaller fixed amount, or risk-based mode. A risk-based bot with the same 120 USDT budget sizes about 9 USDT notional at 2x on this signal and passes L-08 (tested).
- **Defects found and fixed (`d09421c`):**
  1. A `percent_balance` position allocation never reached CATI production sizing.
  2. A percentage capital budget was handed to the executor as USDT; it is now resolved from each cycle's equity and fails closed.
  3. Account-wide daily loss read the legacy preset for risk-based bots.
  4. The size refusals had no customer explanation.
- **Multiple bots on one account:** each keeps its own settings and the strictest daily loss applies, but entries are refused while more than one bot is active (`ACCOUNT_EXECUTION_OWNER_AMBIGUOUS`). That existing fail-closed rule is tested and unchanged.
- **Open decisions for the owner:**
  - The portfolio path (`portfolio_runtime.py:101`, inactive for this account) treats a fixed amount as a maximum notional and sizes by risk, which conflicts with "never resize a fixed amount".
  - The frozen residual plans carry a 2 bps spread bound, so low-priced symbols whose one tick is wider are refused `MARKET_SPREAD_EXCESSIVE` under L-08.
  - The old deploy form's "Trade Amount per Position" does not say it is margin.

## 7. LIVE activation path (not activated)

No LIVE capability was added or switched on in this pass. These remain in force and must each be satisfied with real evidence:

- `app/golive/gate.BUILD_PERMITS_LIVE = False` and the two-key live build (`shared_lib/core/live_build.py`). The release allowlist is empty: only the owner plus two reviewers may pin a release.
- `LIVE_ORDER_SUBMISSION_ENABLED=false`. Note that it also blocks reduce-only and flatten at the transport (an earlier open owner finding).
- **Strategy:** a research-CERTIFIED family promoted to M7 with a governance scope. The residual family is FAILED / M0, and experimental grants cannot be LIVE.
- **Venue:** Binance LIVE adapter certification (`NO_CERTIFICATION_EVIDENCE`). A DEMO certification does not carry over.
- **Market policy:** L-08 records for LIVE are refused in this build, and the all-symbols scope is DEMO-only by construction.
- **Go-live gate:** its 17 conditions apply, including customer consent, operator approvals with `APPROVAL_MAX_VALIDITY_DAYS`, beta records and a platform PostgreSQL database.

## 8. Frontend (`68eee73`)

- **Contradiction fixed:** the banner and the start/deploy confirmation classify a bot by its broker account's environment. `mode: live` only means "executes at a broker". A live-mode bot on a DEMO account is now "DEMO exchange — no real money". An unknown or unloaded account still counts as real money.
- **Trading panel:** reads the new customer endpoint `/api/v1/cati/runtime/dashboard`. Per account it shows:
  - exchange, the DEMO/LIVE badge and connection health with the problem's message and action;
  - bot running, last evaluation, latest signal or "No signal";
  - entry permission with every blocker it would meet (strategy authority, venue certification, market policy), pre-checked so it never says "Ready" when a signal would be refused;
  - the EXPERIMENTAL DEMO badge, positions with protective-stop state, and recent trades labelled Strategy / Experimental demo / Exchange test.
  - The order switch is shown as configuration, separate from entry permission.
- **Main dashboard:** no raw configuration or JSON dump; every button has an action. The owner's own dashboard cleanup of 2026-11-01 is included; `fix_trading_dashboard.py` is not committed. The garbled "Loading…" text and a fake 75 % bar were fixed.
- **Admin console:** "Experimental DEMO trading", with grant and revoke controls, L-08 record and revoke controls, ledger integrity, confirmations, client-side bounds and verbatim backend refusals.

## 9. Services and end-to-end check

All five run against the same local database:
- user backend :8000
- admin backend :8100
- bot :9000
- user portal :5173
- admin console :4173

End to end, the owner's dashboard was read through the portal API path (user backend → bot) with a short-lived read token for the owner's user id. It returned:
- Binance / DEMO / no real money, connection CONNECTED;
- bot running, last evaluation WAITING_SIGNAL, latest signal NO_SIGNAL;
- strategy authority EXPERIMENTAL_DEMO, experimental grant ACTIVE, no pre-sizing blockers;
- 5 recent trades labelled "Exchange test", and real-money trading possible: false.

## Tests and CI

All runs used clean `git archive` exports with CI's global environment (`APP_ENV=TEST` …) and the `ci_quarantine` plugin. The quarantine list was not extended.

| Suite | Commit | Result |
|---|---|---|
| Bot backend, full (`backends/bot-backend/tests`) | `d09421c` | 8,492 collected: **8,006 passed, 2 failed, 484 skipped** (66 min) |
| The 2 failures, after the fix in the final commit | final | `test_legacy_persistence_inventory` (the new `cati_experimental_demo_events` table registered in `legacy_first_use.first_use_creators`) and `test_vps_runtime_operations` (it expected the old bare `ConnectionError` reason; it now asserts the classified `BROKER_NETWORK_UNAVAILABLE` with the exception class kept in `read_diagnostic`): **50 passed, 1 skipped (POSIX-only)** |
| User backend | `68eee73` | **904 passed**, 0 failed |
| Admin backend | `68eee73` | **98 passed** |
| Shared library | `68eee73` | **299 passed** |
| Root tests | `68eee73` | **7 passed** |
| User portal (`npm test`, `tsc -b && vite build`, eslint) | `68eee73` | **102/102**, build OK, lint 0 errors (210 existing warnings) |
| Admin console (`npm test`, `tsc -b`, build, eslint) | `68eee73` | **73/73** (5 suites), build OK, lint 0 errors (151 existing warnings) |
| New focused suites | final | experimental DEMO (20), broker-read diagnosis (24), L-08 all-symbols (7 + the existing 101 L-08/spread tests), customer dashboard (8), bot sizing (15), market-policy proxy (4), governance API and proxy |

PostgreSQL: the L-08 tests' PostgreSQL variants and the "Legacy runtime on PostgreSQL" job run in CI only (no local PostgreSQL cluster was used; the system PostgreSQL service holds other data and was not touched).

GitHub CI on the exact final commit: CI_PLACEHOLDER

## Files

- Backend: `shared_lib/broker/http_budget.py`, `integration/broker_read_diagnosis.py` (new), `integration/production_runtime.py`, `integration/production_execution.py`, `integration/reason_registry.py`, `integration/customer_dashboard.py` (new), `governance/experimental_demo.py` (new), `pipeline/provenance.py`, `pipeline/retention.py`, `execution/boundary.py`, `scale/market_entry_policy.py`, `scale/spread_gate.py`, `core/trading_orchestrator.py`, `api/family_governance.py`, `api/cati_runtime.py`.
- User backend: `api/admin_governance.py`, `api/admin_market_policy.py` (new), `api/cati_proxy.py`, `main.py`.
- Frontends: `user-frontend` (banner, trading-mode utilities, confirmation dialog, trading panel, dashboard), `admin-frontend` (experimental-DEMO page).
- Evidence: `evidence/binance-demo-certification-20261101/` (read-only and order-lifecycle runs).
- Not committed: `.env` files, `platform.db`, the contributor's three files, `fix_trading_dashboard.py`.
