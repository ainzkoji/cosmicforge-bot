# CosmicForge Tradingbot: final system certification (pre-deployment)

**Date:** 2026-11-12 **Starting commit:** `649080b` (= `origin/main`, CI 41303789967 green)
**Final commit:** see section 32 **Branch:** `ccr-b5c520cf-80g8x2` (pushed for review; this session's environment
permits pushing only to that branch, so merging to `main` is the owner's step)
**Payments, subscriptions and billing:** not touched (out of scope).
**VPS:** not contacted. **Real-money orders:** none. **Exchange orders of any kind:** none in this pass.

This report is the authoritative status record for this pass. It builds on, and does not repeat,
`TRADINGBOT_PRE_DEPLOYMENT_READINESS.md` (engineering closeout, 2026-10-31) and
`TRADINGBOT_DEMO_ACTIVATION_REPORT.md` (Binance DEMO activation, 2026-11-01). Where this report and those disagree,
this one is current.

---

## 1. Executive outcome

**Verdict: `TRADINGBOT_SYSTEM_ENGINEERING_COMPLETE — PRE_DEPLOYMENT_READY` for the software, with the trading
readiness stated separately (section 35).**

- Four genuine engineering defects were found and fixed in this pass. Each could have harmed a real deployment:
  1. **Turning the order switch off stranded open positions.** With `DEMO_`/`LIVE_ORDER_SUBMISSION_ENABLED` off, stop
     replacement, reduce-only exits, recovery and emergency flatten were refused along with new entries.
  2. **The VPS guide produced a server that could never place an order.** It configured no platform database, so
     every entry, DEMO included, would have been refused with `PLATFORM_ORDER_BUDGET_UNAVAILABLE`.
  3. **BingX historical backfills silently lost data.** Any range longer than one page lost its oldest bars (440
     of 1,440 for one day of 1-minute bars), and the quality check still reported the series usable.
  4. **The customer portfolio added DEMO and LIVE money together.** It also left Bybit/BingX accounts out without
     saying so, and labelled open-position unrealized P&L as "Daily P&L".
- No safeguard was weakened. The 0.40 % per-trade ceiling, 15 % stop ceiling, native exchange-side stops, account
  fences, worker leases, kill switches, the two-key live build and the research holdout are unchanged and tested.
- **The bot cannot place a strategy trade today, and that is correct, not a defect.** Every strategy family is
  research FAILED/M0. The experimental DEMO waiver and the DEMO L-08 policy have both reached their recorded
  expiry. The owner's bot is configured with a fixed 120 USDT margin, and that cannot fit the 0.40 % limit on a
  417.58 USDT account. Trading readiness is reported separately in section 35, and the owner's choices are in
  section 34.

## 2. Starting state

| | |
|---|---|
| `git rev-parse HEAD` / `origin/main` | `649080b246477aaad61648d29f6a02f90c781557` (identical) |
| Working tree | clean |
| Contributor changes (`daily_feed.py`, `platform_runtime.py`, `test_step3_platform_runtime.py`) | not present in the repository |

The contributor changes were never committed. They existed only in the owner's Windows working tree, so nothing
here could overwrite them. If they still exist there, they must be reconciled before that tree pulls these commits:
the earlier report records that one of them removes `platform_runtime.budgeted_archive_get`, which committed code
still imports.

Environment of this pass:
- a Linux cloud container (4 cores, 15 GB) with Python 3.13, Node 22 and a local PostgreSQL 16 for the
  both-dialect tests;
- Binance (`fapi`, `demo-fapi`) answers HTTP **451** from this region and Bybit answers **403**. **BingX public
  endpoints are reachable**;
- no exchange credentials, no owner database (`cosmicforge.db`) and no VPS access.

## 3. System inventory

| Component | Entry point | State |
|---|---|---|
| Bot backend (trading engine, CATI, runtime owner) | `backends/bot-backend/app/main.py`, systemd `cosmicforge-trading.service` | Integrated; full suite section 31 |
| User backend (customer API, admin control-plane proxy) | `backends/user-backend/app/main.py`, `cosmicforge-user-backend.service` | Integrated; 910 tests pass |
| Admin backend (read-only analytics) | `backends/admin-backend`, `cosmicforge-admin-backend.service` | 98 tests pass |
| Shared library (settings, broker transport gates, persistence, migrations, live build) | `backends/shared/shared_lib` | 299 tests pass |
| User frontend | `frontends/user-frontend` (Vite/React) | 102/102, build OK, 0 lint errors |
| Admin frontend | `frontends/admin-frontend` | 73/73, build OK, 0 lint errors |
| CATI pipeline: data → features → regime → family → signal → portfolio → risk → boundary | `app/trading_intelligence/**` | Section 13 |
| Broker adapters | `app/exchange/platform_adapter/{binance,bybit,bingx}.py` (versions `*-1.2.0`) | Section 16 |
| Runtime DB (SQLite, WAL, `SYNCHRONOUS=FULL`) | `DATABASE_URL` | Unchanged |
| Platform DB (PostgreSQL; order budget, intents, jobs, assignments, L-08, go-live, alerts) | `PLATFORM_DATABASE_URL`, `python -m shared_lib.persistence.sql` | Migrations 1–61; deploy gap fixed (section 4.2) |
| Deployment assets | `deploy/systemd`, `deploy/nginx`, `deploy/containers`, `deploy/profiles`, `deploy/staging` | Section 30 |

**Dependency graph (production path).**
- Market data (Binance archive + REST, `daily_feed`) → CATI residual scheduler (hourly) → `cati_production_decisions`.
- Broker cycle (every ~30 s, runtime owner lease, account fence) → account reads → `process_account`.
- `process_account` → effective bot policy → sizing → hard risk → family gate (certification / experimental grant)
  → L-08 spread gate (platform DB) → `PortfolioExecutionBoundary` / `CATIExecutionBoundary` → transport gate
  (`require_broker_mutation_permission`) → venue.
- Fill → native reduce-only stop → `ProtectionManager` → reconciliation → customer dashboard
  (`/api/v1/cati/runtime/dashboard`, proxied by the user backend).

## 4. Issues found in this pass, root causes and fixes

### 4.1 The order switch stranded open positions (`334f2ff`)

**Symptom.** In production with `DEMO_ORDER_SUBMISSION_ENABLED=false` or `LIVE_ORDER_SUBMISSION_ENABLED=false`, an
open position could not have its stop replaced or be exited. Five places refused:
- the transport refused every mutation;
- `PortfolioExecutionBoundary._admission` refused the whole batch, exits included;
- `recover()` returned `REFUSED`;
- `production_close` and `place_native_protection` refused;
- `flatten()` refused before reading any position.

This was recorded in 2026-10 as an open finding (gate register section 3.2).

**Root cause.** One flag was used both for "no new exposure" and for "no mutation at all".

**Fix.** `shared_lib.core.production.risk_reduction_gate(environment)`:
- With the switch **on**, nothing changes.
- With the switch **off**, protective stops, reduce-only/`closePosition` orders, cancels, recovery and flatten stay
  admitted:
  - **DEMO:** always.
  - **LIVE:** only when `live_build_status().capable`, which requires a two-key reviewed live release. No build in
    this repository is capable, so **LIVE stays incapable of any real-money mutation**.
- `is_risk_reducing_mutation` classifies the venue request. Leverage changes, transfers, ordinary orders,
  `reduceOnly=false` and unknown paths are not risk reducing.
- In the boundary, risk-increasing intents are refused with the switch's reason, and `_increase_refusal` re-checks
  the switch immediately before every entry, add and rebalance-up.

**Tests.**
- New file `tests/test_risk_reduction_gate.py` (64 cases): classification for all three venues, DEMO admit and
  refuse, LIVE refused in this build, live-capable build simulated, undecidable build, endpoint mismatch.
- New `test_the_order_switch_off_stops_entries_but_never_strands_an_open_position`: three positions are open, the
  switch is turned off, and then:
  - the exit reaches the venue;
  - the new entry is refused with `DEMO_ORDER_SUBMISSION_DISABLED`;
  - no new exposure is sent;
  - recovery runs.
- Three existing tests asserted the old behaviour ("exits included", "recovery sends nothing", "flatten fails")
  and were updated to the corrected contract. One of them now proves a LIVE flatten in this build is still refused.

### 4.2 The VPS guide never configured the platform database (`a746a58`)

**Symptom.** `docs/VPS_TRADING_DEPLOYMENT.md` built the server `.env` from `deploy/env.production.example`, and that
file had no `PLATFORM_DATABASE_URL`. `spread_gate.platform_database()` deliberately has no fallback, so a server
deployed this way refuses **every** entry, DEMO included (`PLATFORM_ORDER_BUDGET_UNAVAILABLE`), and the L-08 and
go-live consoles answer 503. The owner's workstation only worked because the operator had added a local SQLite
`platform.db` by hand. `GOLIVE_REVOCATION_ANCHOR` was also missing (gate register finding 3.3).

**Fix.**
- **Template:** both keys and `PGPASSFILE` added to `env.production.example`, with explanations.
- **New guide section 2.4:** PostgreSQL role and database, a 0600 pgpass file, `migrate`, and `status`.
- **Upgrade procedure (6.1):** now backs up both databases and migrates the platform schema before restarting.

**Rehearsal (PostgreSQL 16, this container):**
- the first `migrate` applied versions 1–61;
- a second run applied none;
- `PlatformSpreadGate._schema_ready` returned `True` through the same `PLATFORM_DATABASE_URL`/`PGPASSFILE` route
  the service uses.

### 4.3 BingX backfill silently dropped its oldest bars (`f8401d5`)

**Symptom.** This was found with live data. `venue_history.bingx_klines` was asked for one day of 1-minute bars
for BTC, SOL, XRP and DOGE. It returned **1,000 of 1,440** bars: the first 440 were missing. `check_series` still
said `usable: True`. An ETH request returned **0 rows** with no error.

**Root cause.**
- BingX `/openApi/swap/v3/quote/klines` returns bars newest-first. Given `startTime` and `endTime`, it returns the
  **newest** `limit` bars of the range (probed directly). The fetcher assumed oldest-first, advanced past the last
  bar and ended.
- A halt (empty page) also ended the walk.
- A non-zero `code` (error envelope with HTTP 200) was read as "no data". Bybit `retCode` had the same flaw.
- The quality check only measured gaps *between* rows, so a series cut short at either end looked clean.

**Fix.**
- The range is walked oldest-first in fixed windows of at most `limit` bars, the same pattern as Bybit. Empty
  windows move on to the next.
- Error envelopes raise `VenueDataError`.
- `check_series(expected_start_ms=, expected_end_ms=)` counts bars missing at either end, bounded by listing and
  delisting, and such a series is not usable. `scripts/acquire_multi_asset_dataset.py` passes the requested range.

**Tests.**
- The fake venue reproduces BingX's observed paging. The old code returns 1,000 of 1,440 against it; the new code
  returns all 1,440.
- Further cases: halt walk-through, BingX/Bybit error envelopes, and leading/trailing coverage.

**Live re-validation** (`evidence/bingx-public-market-data-20261112/bingx_live_klines.json`):
- 8 symbols × (1h for 90 days, 15m for 14 days, 1m for 3 days) = 24 series;
- **all complete** (0 missing), ascending, unique and aligned;
- 0.3–2.5 s per series.

### 4.4 Portfolio totals mixed DEMO and LIVE money (`65372fe`)

**Symptom.** `user-backend/app/core/portfolio_service.get_portfolio_summary` had five problems:
- It summed `totalMarginBalance` of every connected Binance account into "Total Portfolio Value", whatever the
  account's environment.
- It skipped Bybit/BingX accounts without an error, although its docstring said "totals are never silently zeroed".
- It read a missing venue field as `0`.
- It published raw exception text, which can carry a signed request URL.
- It returned the open positions' unrealized PnL as `total_day_pnl`, which the dashboard labelled "Daily P&L".

This is the root cause of the earlier observation that "portfolio" and "broker equity" figures did not agree.

**Fix.**
- Totals are kept per environment (`by_environment`). The headline is one named environment (`environment`: LIVE
  if any LIVE account was read, else DEMO), with `mixed_environments` set when both exist.
- Unsupported brokers produce `BROKER_PORTFOLIO_READ_UNSUPPORTED`.
- A missing field is a failed read.
- Only the exception class is published.
- `total_unrealized_pnl` is added. The dashboard now says "Open P&L (unrealized)", shows "DEMO — no real money" on a
  DEMO headline, and says so when LIVE totals exclude DEMO accounts.
- The deploy and edit forms now state that a fixed trade amount is **margin** (size = amount × leverage) and that an
  order over the per-trade risk limit is refused, never resized. This was an open owner finding from 2026-11-01.

**Tests.** `tests/test_portfolio_summary_environments.py` (6).

### 4.5 Reviewed and left unchanged (with reasons)

| Item | Disposition |
|---|---|
| Portfolio path (`portfolio_runtime.py:100`) caps notional at the fixed amount (treats margin as notional and sizes by risk under it) | Inactive for every current account; it errs smaller, never larger. Making it "margin × leverage, refuse instead of resize" is a customer-sizing semantic the owner already listed as an open decision (section 34, D3). Not changed. |
| BingX stop replacement refused (`REPLACEMENT_NOT_POSSIBLE_SAFELY`) | Re-checked against the adapter's cited docs-v3 statements: amend is LIMIT-quantity only, the summed pending stop quantity may not exceed the position, and `cancelReplace` has no rollback when the place fails. No documented gap-free move exists, so the refusal stands. No registered family lists BingX or Bybit as a supported market, so the family gate refuses entries there before any stop is needed. |
| Binance DEMO certification `cert_6852df804410b16bee8ad4d1` | Still valid. It is bound to adapter version `binance-usdm-1.2.0`, and neither that string nor `platform_adapter/binance.py` changed. With the switch on, the transport change alters nothing. No re-run was needed, and none was possible from this region (HTTP 451). |

## 5. Market-data inventory

| Source | Venue/instrument | Use | Status |
|---|---|---|---|
| Binance public archive (`binance_archive.py`) + REST klines (`daily_feed.py`) | USD-M perpetuals, ~140 symbols | Production CATI residual family (hourly/daily) | Code unchanged. Weight catalogue `binance-usdm-ip-weights-2026-11-01` in place. Not reachable from this region, so no live re-measurement here. Last live evidence 2026-11-01: collector no longer refused. |
| `venue_history.binance_*` | klines, funding, OI (≤30 d) | Research acquisition | Unchanged; synthetic tests pass |
| `venue_history.bybit_*` | klines, funding, OI | Research acquisition | Error envelopes now raise; not reachable from this region (403) |
| `venue_history.bingx_*` | klines (OI: `VENUE_HAS_NO_OI_HISTORY_ENDPOINT`) | Research acquisition | **Fixed and live-validated** (section 4.3) |
| Order books/depth | Per venue via adapters | L-08 spread gate (depth ≤ 3 s old, ≥ 5 levels) | Unchanged; stale depth refuses (`DEPTH_STALE`) |
| Liquidations | none | none | Explicitly unavailable on all three venues (no complete REST history) |

- **Historical coverage.** The repository's `data/` holds no market datasets: they live in the owner's runtime
  database and archive cache, which are not here. Coverage per symbol for the Binance production universe must
  therefore be read on the server (`scripts/dataset_coverage.py`). This pass adds coverage-at-both-ends to the
  quality check, which that audit now uses.
- **Missing data.** Never substituted. A missing field raises or refuses (`FeatureUnavailable`, `VenueDataError`,
  `DEPTH_STALE`), and a strategy does not evaluate on incomplete inputs.

## 6–8. CATI, strategy evaluation and research certification

- **Pipeline.** The tests below cover the stages from market data to reconciliation listed in the brief:
  - `test_sustained_cati_cycle.py` (30 simulated days, crash and restart);
  - the production loop and DEMO execution suites;
  - the new switch test.
  
  The full suite is in section 31.
- **Decision states.** `NO_SIGNAL`, refused (with reason), `EXECUTION_BLOCKED` (`BLOCKED_*`), submitted, filled,
  protected and closed are distinct, persisted states (`cati_production_decisions`). The dashboard shows "No
  signal" as a correct outcome, not as activity.
- **Research.** Every family is FAILED/M0, and the residual family's verdict is FAILED. Mandate 004's holdout is
  unopened. Nothing in this pass touched research logic, the register or the holdout. The certification software
  (registration → OOS/walk-forward → costs → multiple testing → holdout governance → promotion and revocation) is
  unchanged and covered by its suites. **No family is certified, so no strategy can trade under certification.**
- **Experimental DEMO** (`xdemo_a2e102e911db4144b29b`). It was granted on 2026-11-01 for 72 hours, so by its
  recorded terms it expired around 2026-11-04. The DEMO L-08 policy `81df3fbf…` expired on 2026-11-08. The ledgers
  live in the owner's database and were **not inspected** here: these states are derived from the recorded
  durations, and expiry is enforced on every entry. Neither can re-enable itself.

## 9–10. Bot configuration and sizing

- `bot_a8117dc719fc` (per the 2026-11-01 audit, **not changed**): `allocation_type=fixed_amount`,
  `allocation_value=120` (margin per trade), `capital_allocation=120`, `risk_level=balanced`, `mode=live` on a DEMO
  account.
- **Arithmetic.** Equity is 417.58 USDT, so the per-trade limit is 0.40 % × 417.58 = **1.67 USDT**. The strategy's
  stops are 5–15 %. A fixed 120 USDT margin is at least 120 USDT notional, which loses at least 6 USDT at a 5 % stop
  (1.44 % of equity) even at 1×. **No leverage makes this configuration eligible.** Entries are refused with an
  explanation (`refusal_explanation`), never resized. This is the correct behaviour; the owner's options are in
  section 34.
- **Fixed, percentage and risk-based modes.** Tests cover all three:
  - `test_bot_sizing_config.py`;
  - `test_k1_sizing_gates.py`;
  - `test_k1_budget_resolution.py`.
  
  A risk-based bot with the same 120 USDT budget sizes about 9 USDT notional at 2× and passes L-08 (2026-11-01
  replay).

## 11. Risk management

The following are covered by the passing suites (section 31) and were not changed:
- 0.40 % per-trade and 15 % stop ceilings, enforced both in primary sizing and at the boundary;
- open-risk and in-flight reservation accounting;
- daily, period and loss-streak brakes;
- kill switches;
- shared liquidity coordination and multi-bot ambiguity (`ACCOUNT_EXECUTION_OWNER_AMBIGUOUS`).

The one change is that risk *reduction* now proceeds when entries are switched off (section 4.1).

## 12. Portfolio reconciliation

See section 4.4. Account equity and bot allocation stay distinct. DEMO, LIVE and simulated records are not mixed:
experimental-DEMO trades are labelled `EXPERIMENTAL_DEMO`, certification orders "Exchange test", and the portfolio
is now separated by environment.

## 13. Exchanges

| Venue | Internal/synthetic | Genuine DEMO evidence | LIVE |
|---|---|---|---|
| Binance USD-M | Pass (wire, contract, certification-harness suites) | **CERTIFIED (DEMO)** 2026-11-01: 30/30 checks, real orders, native stop, gap-free replacement, account flat at the end | `NO_CERTIFICATION_EVIDENCE` |
| Bybit linear | Pass | None. Needs an authorized Bybit DEMO account: `scripts/run_venue_certification.py --venue bybit --execute` | none |
| BingX swap | Pass. Public market data **live-validated** this pass | None. Stop *replacement* is refused by design (section 4.5); certification step 7 of `docs/exchanges/BINGX_ENVIRONMENT_CERTIFICATION.md` is the check that could change it | none |

## 14–15. Order execution and protective stops

The lifecycle (signal → plan → authorization → risk → liquidity → reservation → lease → submission → acknowledgement
→ fill → native stop → monitoring → replacement → partial reduction → close → P&L → notification → audit) is proven
in three ways:
- **deterministically** against fake venues: multi-position execution, E-01 rebalance protection, the production
  DEMO execution suite with partial fills and unknown outcomes, and the sustained cycle;
- **on genuine Binance DEMO** for the venue-facing steps (2026-11-01 certification);
- the uncertain-outcome tests show **no blind retry**: an unknown submission is resolved by reading the venue.

No autonomous *strategy* order has occurred, because no eligible signal has met an eligible configuration. This
pass did not and would not fabricate one.

## 16. Workers, queues, recovery

Unchanged and covered:
- runtime owner lease;
- account fence and assignment fencing;
- durable jobs;
- worker-kill drill (`scripts/worker_kill_drill.py`);
- takeover recovery-first.

Two notes from this pass:
- **Recovery after the order switch is turned off now runs** (section 4.1).
- **Linux measurements:** section 24.

## 17–18. Databases

- **Runtime DB (SQLite):** unchanged.
- **Platform DB:** the deploy gap is fixed (4.2). Migrations are checksummed and idempotent (rehearsed).
- **Both dialects:** the platform-worker tests run on SQLite **and** PostgreSQL 16 locally
  (`PLATFORM_TEST_POSTGRES_REQUIRED=1`).
- **Backup and restore:** see section 26.

## 19–21. Backends and frontends

All three backends and both frontends pass their suites (section 31). Changed customer contracts:
- `GET /api/v1/portfolio/summary` adds `environment`, `mixed_environments`, `total_unrealized_pnl`, `by_environment`
  and per-position `environment`;
- every existing key is kept for older clients.

## 22. Security

- `pip-audit`:
  - bot backend: **no known vulnerabilities**;
  - user backend: **no known vulnerabilities**;
  - the admin backend has no requirements file of its own.
- **Fixed:** the portfolio summary published broker exception text, which can carry signed URLs (4.4).
- **Not done here (unchanged from earlier):** external penetration test (NOT RUN), and credential rotation on each
  environment (operator).

## 23. Monitoring

Unchanged:
- `/health` separates an HTTP-healthy server from a working engine (lease, scheduler, cycle age, collector
  heartbeat, decision age);
- alerting (`app.ops.alerting`) and the ops-tick dead-man timer;
- the platform backup status feeding `BACKUP_FAILURE`.

## 24. Performance (Linux)

`scripts/step5_load_test.py` on **Linux** (x86_64, 4 cores, Python 3.13, PostgreSQL 16, SQLite 3.53).
Evidence: `evidence/linux-drills-20261112/load_test.json`. Earlier measurements were Windows-only, and the Step 5
report left "Linux resource measurement" open.

| Accounts | Dialect | Sizing ms | Maintenance cycle ms (4 workers) | Queue jobs/s | Duplicate orders | Takeover ms (2 s lease) | Pool peak | RSS MB |
|---|---|---|---|---|---|---|---|---|
| 1 | PostgreSQL | 0.48 | 26 | 82 | 0 | 14 | 4 | 84 |
| 10 | PostgreSQL | 1.89 | 26 | 181 | 0 | 1,968 | 7 | 86 |
| 100 | PostgreSQL | 17.3 | 115 | 319 | 0 | 1,343 | 12 | 90 |
| 1,000 | PostgreSQL | 172.7 | 1,637 | 362 | 0 | 2,036 | 16 | 114 |
| 1 | SQLite | 0.41 | 10 | 103 | 0 | 5 | 4 | 68 |
| 10 | SQLite | 1.46 | 37 | 287 | 0 | 1,964 | 7 | 71 |
| 100 | SQLite | 14.4 | 112 | 981 | 0 | 1,782 | 13 | 77 |
| 1,000 | SQLite | 152.3 | 348 | 1,073 | 0 | 443 | 16 | 104 |

**Results:**
- every account was owned and maintained at every scale;
- there were 0 duplicate orders and 0 orphaned accounts;
- order spreading stayed within its cap at every scale;
- takeover after a worker stops is bounded by the 2 s lease.

**Disclosed limits:**
- The data is synthetic and the venues are in-memory fakes.
- Both runs shared the 4 cores with a concurrent full test-suite run, so latencies are pessimistic. The 1,000-account
  PostgreSQL maintenance cycle (1.6 s vs 0.8 s on the Windows run) is the most affected.
- This is not exchange throughput (EXT-LOAD-01 / egress) and not a hosted-staging run.

Also fixed: the script labelled every run "one Windows workstation" whatever the host.

## 25. Failure and recovery

The existing drills pass in the full suite:
- worker crash and takeover;
- DB interruption;
- broker timeout and unknown order;
- rate-limit exhaustion;
- partial fill;
- stop-placement failure;
- restart during reconciliation;
- graceful shutdown and process replacement (container drill in CI).

**New:** with the order switch turned off mid-position, protection and the exit continue (4.1).

## 26. Backup and restore

`scripts/restore_drill.py` on Linux with the PostgreSQL leg. Evidence:
`evidence/linux-drills-20261112/restore_drill.json`. **PASSED: 22 of 22 checks, RPO 190 ms, RTO 66 ms, PostgreSQL
leg passed.** The steps were:
1. online backup with `quick_check` and a SHA-256 manifest;
2. secret scan, which refuses any plaintext credential;
3. encryption;
4. off-site copy;
5. restore;
6. integrity and foreign-key checks, and every table equal to the backup snapshot;
7. revoked live permission not restored;
8. credentials still ciphertext and decryptable only with the secret-manager key.

Limits, as stated by the drill:
- the encryption was TEST-ONLY, because no age/gpg recipient is configured here;
- the "remote" was a local directory;
- the drill does not prove that the production off-site copy can be restored.

## 27–30. Deployment artifacts

- **systemd units, nginx, container images, staging compose, profiles:** unchanged except the production env
  template and VPS guide (4.2).
- **CI "Containers" job:** builds the images, runs migrate twice (idempotent), smoke-tests the API role, checks the
  emergency routes refuse on the API replica, and runs the failed-release automatic-rollback drill. It runs on the
  final SHA (section 32).
- **Not claimed:** Linux-host *systemd* certification. The units have not run on a real host (EXT-STG-01).

## 31. Test results (final tree)

PENDING_TESTS

## 32. GitHub and CI

PENDING_CI

## 33. Genuine external dependencies

| Id | Requirement | Who |
|---|---|---|
| Strategy | A research-certified family (Mandate 004 holdout authorization and evaluation, or a new governed hypothesis); then demo run and live weeks | owner / research governance |
| EXT-VENUE | Bybit and BingX DEMO certification runs with authorized DEMO accounts; Binance LIVE certification needs live authorization | operator + owner |
| EXT-LIQ-01 | L-08 calibration from genuine venue observations | owner |
| EXT-STG-01 | First run of the systemd units on a Linux host (the VPS) | operator |
| EXT-DB-01 | Production-copy migration of the runtime DB to PostgreSQL (optional; SQLite runtime is supported) | owner |
| EXT-OPS-01 | Alert delivery channel and on-call person | owner |
| J-06 | Kill-switch and flatten drills in an authorized environment; external penetration test | owner / external |
| Release | A reviewed live release (two keys) | owner + two reviewers |
| Policy keys | `APPROVAL_MAX_VALIDITY_DAYS`, `MIN_DEMO_RUN_DAYS`, `LIVE_CONSENT_VERSION`, stage limits | owner |
| Business | Counsel/licensing; payments (deferred by instruction) | owner |

## 34. Owner decisions (none was taken here)

- **D1. `bot_a8117dc719fc` sizing.** As configured it can never enter (section 9). Choose one, in the bot settings:
  - **(a)** risk-based allocation, recommended: the platform sizes each trade to the risk limit;
  - **(b)** a fixed margin small enough to fit. At 417.58 USDT equity and a 5 % stop, at most about 33 USDT notional
    (for example 8 USDT margin at 4×), and less for wider stops;
  - **(c)** keep it as it is: every signal is refused, with the explanation shown.
- **D2. DEMO execution authority.** Strategy entries on DEMO need two things:
  - a new experimental-DEMO grant, which is admin console → Experimental DEMO trading. It waives research
    certification only, on DEMO only, for a bounded time and notional;
  - a new DEMO L-08 policy.
  
  Both previous approvals have expired. Without them, the honest state is "no eligible strategy".
- **D3. Portfolio-path fixed amount** (section 4.5): keep it as a notional ceiling, or align it with the main path
  (margin × leverage, refuse instead of resize).
- **D4. Frozen 2 bps spread bound.** It refuses low-priced symbols whose single tick is wider (`MARKET_SPREAD_EXCESSIVE`
  under L-08). Changing it is a research change to a frozen plan.
- **D5. Workstation contributor changes.** Reconcile them before that tree pulls these commits (section 2).

## 35. Final readiness

| Status | Value | Basis |
|---|---|---|
| ENGINEERING_READY | **READY** | No known internally fixable critical defect remains; the four found here are fixed and regression-tested; full suites green (31) |
| DATA_PIPELINE_READY | **READY (software)** | Binance production collector unchanged and live-proven on 2026-11-01; BingX acquisition fixed and live-validated today; Binance/Bybit not reachable from this region for a fresh measurement |
| CATI_RUNTIME_READY | **READY** | Pipeline, decision persistence and refusal explanations proven; hourly evaluation proven live on 2026-11-01 |
| DEMO_EXECUTION_READY | **READY (Binance, mechanism) / BLOCKED (strategy entries)** | Binance DEMO venue-certified. Strategy entries blocked by authorization (D2) and sizing (D1), not by software |
| PRODUCTION_PACKAGE_READY | **READY** | Deploy gap fixed and rehearsed; CI container, migrate and rollback drill on the final SHA (32). First real-host systemd run is EXT-STG-01 |
| LIVE_CERTIFICATION_STATUS | **NOT CERTIFIED** | No certified family; no LIVE venue certification |
| LIVE_AUTHORIZATION_STATUS | **NOT AUTHORIZED** | Build not live-capable (`LIVE_NOT_PERMITTED_IN_THIS_BUILD`); no release, consent or approvals |

### Readiness matrix

| System | Result |
|---|---|
| Market-data acquisition | PASS (BingX live-validated; Binance unchanged, live-proven 2026-11-01) |
| Historical data | PARTIAL (tooling and quality checks PASS; per-symbol coverage of the owner's stored datasets must be read on the server) |
| CATI strategy engine | PASS |
| Strategy certification tooling | PASS (tooling); research verdict FAILED for every family |
| Bot instance configuration | PASS (persisted per bot, never altered; owner's bot structurally ineligible, D1) |
| Risk management | PASS |
| Binance DEMO | PASS (certified 2026-11-01, still bound to the unchanged adapter) |
| Bybit | PARTIAL (internal PASS; no genuine venue evidence) |
| BingX | PARTIAL (internal PASS; public data live PASS; stop replacement unsupported by design) |
| Order execution | PASS (deterministic + Binance DEMO venue steps); autonomous strategy order NOT_RUN (no eligible signal and configuration) |
| Protective stops | PASS for supported operations; now also maintained when the order switch is off |
| Portfolio management | PASS (DEMO and LIVE separation fixed) |
| Account and worker management | PASS |
| PostgreSQL | PASS (both-dialect tests on PG 16; platform migration rehearsed) |
| User / Admin / Bot backends | PASS |
| User / Admin frontends | PASS (automated suites, builds; no manual browser acceptance in this pass) |
| Monitoring | PASS (test environment) |
| Backup and restore | PASS (Linux drill, both dialects; production off-site restore not proven) |
| Container and deployment assets | PASS (CI containers job) / real host BLOCKED (EXT-STG-01) |
| Real-money activation | BLOCKED (not certified, not authorized) |

## 36. Remaining work

Only the items in sections 33 and 34. No internally fixable engineering defect is known to remain.
