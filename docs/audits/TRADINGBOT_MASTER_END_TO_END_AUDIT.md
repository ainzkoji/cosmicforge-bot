# CosmicForge Tradingbot — Master End-to-End System Audit & Production Trading Readiness Assessment

| | |
|---|---|
| Audit type | Independent, read-only, evidence-based full-system audit (architecture, CATI, research, data, brokers, risk, execution, customer/admin, API, persistence, workers, security, scale, VPS, LIVE gates) |
| Date | 2026-11-13 |
| Repository | `ainzkoji/cosmicforge-bot` |
| Audited source | `origin/main` = **`649080b`** (unchanged since 2026-11-01); engineering branch `origin/ccr-b5c520cf-80g8x2` = **`732e0e2`** (7 commits ahead, inspected by diff and a temporary worktree only) |
| Audit branch | `ccr-b0495102-vs44aw` (audit documents only; no application source, configuration or data changed) |
| Payment / billing | Excluded (billing routes are listed in the API inventory and marked out of scope) |
| Method | Lead auditor plus seven independent area auditors (architecture/persistence/workers, CATI pipeline, strategy research, market data, brokers/risk/execution, customer/admin/portfolio, security/infra/LIVE gates); every high-impact claim cross-checked by a second auditor or by the lead directly |

**Evidence levels** (used in every document): L1 observed authenticated behaviour · L2 real venue evidence · L3 integration test with real
service boundaries · L4 contract/unit test with mocks · L5 static source inspection · L6 documentation/historical claim · L7 inference.

**Limits of this audit (stated once, applies everywhere):** no production database, VPS, authenticated broker session or owner workstation
was available. Nothing in this audit is L1. Every statement about the live state of ledgers (experimental-DEMO grants, L-08 market-entry
policies, venue certifications, bot `bot_a8117dc719fc` settings) is derived from source code, committed evidence and prior reports, and is
marked as such. Host-level infrastructure is NOT VERIFIED. Unavailable evidence is recorded as unavailable, not as a defect.

## Deliverables

| Document | Content |
|---|---|
| **This file** | Executive report, verdicts, decisions, consolidated findings, next engineering assignment |
| [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv) | 124 area findings in the required 19-field format plus a `Master ID` column consolidating duplicates into M-01…M-34 |
| [TRADINGBOT_SYSTEM_INVENTORY.md](TRADINGBOT_SYSTEM_INVENTORY.md) | Services, entry points, workers, duplicate implementations, persistence (Sections 4, 20, 21) |
| [TRADINGBOT_SERVICE_DEPENDENCY_MAP.md](TRADINGBOT_SERVICE_DEPENDENCY_MAP.md) | Mermaid topology, runtime call graph, decision→order transitions, unreached systems |
| [TRADINGBOT_API_INVENTORY.csv](TRADINGBOT_API_INVENTORY.csv) | 709 routes, machine-readable (method, path, handler file:line, auth, permission deps, schemas, consumers, OpenAPI, test reference) |
| [TRADINGBOT_STRATEGY_RESEARCH_AUDIT.md](TRADINGBOT_STRATEGY_RESEARCH_AUDIT.md) | Strategy inventory, autonomous-research capability table, certification records (Sections 6, 28) |
| [TRADINGBOT_TRADING_EXECUTION_AUDIT.md](TRADINGBOT_TRADING_EXECUTION_AUDIT.md) | CATI pipeline, decision frequency, no-trade causes, broker journey, risk, order lifecycle, certification matrix (Sections 5, 9, 11–14) |
| [TRADINGBOT_MARKET_DATA_AUDIT.md](TRADINGBOT_MARKET_DATA_AUDIT.md) | Data sources/consumers, quality, acquisition, datasets, multi-venue (Sections 7, 8) |
| [TRADINGBOT_CUSTOMER_JOURNEY_AUDIT.md](TRADINGBOT_CUSTOMER_JOURNEY_AUDIT.md) | 27-step journey, settings trace, sizing case, portfolio, admin portal (Sections 10, 15, 17, 18) |
| [TRADINGBOT_SECURITY_OPERATIONS_AUDIT.md](TRADINGBOT_SECURITY_OPERATIONS_AUDIT.md) | Multi-tenant, security, scalability, VPS, LIVE gates, blocker-register reconciliation (Sections 16, 22–25, 29) |
| [TRADINGBOT_PRODUCTION_READINESS_MATRIX.md](TRADINGBOT_PRODUCTION_READINESS_MATRIX.md) | 23 category verdicts, release levels A–D, broker certification matrix (Sections 13, 26, 35) |
| [TRADINGBOT_IMPLEMENTATION_ROADMAP.md](TRADINGBOT_IMPLEMENTATION_ROADMAP.md) | Phases A–H with tasks, reuse, dependencies, acceptance tests (Section 34) |

---

## A. Executive conclusion

CosmicForge Tradingbot is a large, seriously engineered, **safety-first trading platform that does not yet trade**. Roughly 300,000 lines
across three FastAPI backends, a shared library and two React portals implement customer onboarding, encrypted broker connections, a
lease-owned single trading runtime (CATI), two execution tails with layered risk gates, durable idempotent order handling, tamper-evident
research and certification ledgers, and a LIVE release chain that is disabled by seven independent layers. The engineering quality of the
safety controls is high and the platform fails closed almost everywhere.

What it is **not**, today:

1. **Not a strategy-trading system yet.** No strategy family is certified or holds DEMO/LIVE order authority. The only family wired to
   place orders (RESIDUAL_MOMENTUM_PORTFOLIO_TOP1) FAILED research; the only promising family (DAILY_TREND) is REGISTERED and waiting on
   owner decisions and a never-opened holdout. **No CATI-originated strategy order has ever reached a venue** (only certification orders).
2. **Not an autonomous strategy developer.** Research is a set of rigorous, tamper-evident libraries and CLIs that a developer runs by hand;
   nothing generates hypotheses, chains evaluation stages or schedules re-evaluation.
3. **Not deployable from `main` as-is.** Main's VPS package configures no platform database (every entry, DEMO included, is refused),
   sums DEMO and LIVE money in the customer portfolio, and — most seriously — refuses protective stops, reduce-only exits and emergency
   flatten when the documented order switch is turned off (**M-01, P0**). Seven fixes exist only on the engineering branch, and the
   protection fix there is still incomplete in three call sites.
4. **Not operationally proven.** Nothing has run on the production VPS; ops timers are not installed by the guide; there is no on-call,
   alert channel, external penetration test, or credential-rotation record.

The platform is close to **Level A (platform available)** and **Level B (broker-connected DEMO without strategy trading)** after a short
list of fixes. **Level C (autonomous DEMO strategy trading)** is blocked chiefly by research governance and owner decisions, not by missing
execution code. **Level D (real money)** is blocked by design and by external evidence that cannot exist until Level C has run.

## B. Current architecture

Full detail: [SYSTEM_INVENTORY](TRADINGBOT_SYSTEM_INVENTORY.md), [SERVICE_DEPENDENCY_MAP](TRADINGBOT_SERVICE_DEPENDENCY_MAP.md).

- **Topology (L5):** nginx → user-backend `:8000` (customer + admin API, proxies to the engine) and admin-backend `:8100` (read-only reporting).
  The trading engine (bot-backend `:9000`, `cosmicforge-trading.service`, one uvicorn worker, `Type=notify`, watchdog 180 s) is reachable from
  nginx only at `/api/v1/cati/runtime/status`. All three backends share **one runtime SQLite file**; a platform PostgreSQL database
  (order budget, durable jobs, worker assignments, L-08 policies, go-live records, alerting) is required by the code but **not configured by
  main's deployment guide** (M-03).
- **What runs in production:** a runtime lease loop and supervisor; once the lease is held, `production_runtime.run` starts the
  ShadowScheduler (30 s, DAILY_TREND shadow + retention), VenueRuntime (account streams), the residual 15m collector (60 s) and the broker
  cycle (~30 s: sync → maintain → decide → execute per account, under one cycle lock shared with emergency flatten).
- **Two live execution tails with different gate sets (M-05, M-07):** path A (RESIDUAL TradePlan → `CATIExecutionBoundary` → legacy
  `TradingOrchestrator` + `BinanceExecutor`, with go-live gate, platform spread/L-08 gate, order budget and account fence) and path B
  (DAILY_TREND portfolio intents → `PortfolioExecutionBoundary` → certified platform adapter, with family gate and governance authority but
  **without** go-live, L-08, order budget, depth or fence checks).
- **Unused / dormant (no production caller):** the regime/forecast/ranking/veto/controller intelligence stack, the four 15m setup specialists,
  every legacy strategy (Master/Robust Ensemble, Donchian, Bollinger, VWAP, Supertrend, Squeeze, SMA Cross, Trend Pullback V2) and IOFS,
  PaperRunner/MultiBotRunner (410 in production), standalone TradingWorker, DailySnapshotScheduler, ML drift/retrain, venue_history,
  TradFi/MT/OANDA/IBKR, user-backend SQL migrations. The legacy `TradingOrchestrator`/`BinanceExecutor` are **not** dead: path A uses them.
- **Duplicates:** two risk engines (M-26), two execution stacks (M-07), two admin APIs (LEAD-03), three conflicting "production" database
  stories (ARCH-05). Authoritative implementations are identified per concern in the inventory; nothing should be deleted without the consumer
  traces recorded there.

## C. CATI assessment

Full detail: [TRADING_EXECUTION_AUDIT Part 1](TRADINGBOT_TRADING_EXECUTION_AUDIT.md).

- The **runtime** pipeline (data → decision → eligibility → sizing → risk → order → fill → protection → monitoring → exit → reconciliation)
  is implemented and wired for path A, with a genuine stale-signal re-check right before submission and deterministic idempotent order ids
  (L5, L4: 119 targeted tests passed).
- The **intelligence** pipeline described in the brief (features → regime → strategy evaluation → candidate ranking → portfolio decision) is
  **not** what production runs: production decisions come from one frozen hourly cross-sectional rule (RESIDUAL) and one daily rule
  (DAILY_TREND, shadow only). Path A trade plans fill every regime/veto/ranking lineage id with the residual decision id (CATI-07).
- **Decision frequency:** RESIDUAL once per UTC hour (must be recorded within 15 min; missed hours are written as skips and never recovered,
  by design); broker cycle every 30 s; DAILY_TREND daily with 24 h TTL. All intervals are hard-coded. Residual collection is single-threaded
  over ~138 symbols and bypasses the shared HTTP budget, so a degraded venue can cost the hour; accounts are processed sequentially
  (M-17, M-19).
- **Verdict: CATI runtime PARTIAL** — correctly wired and fail-closed, but no authorized strategy, the intelligence stack is not wired, and no
  venue evidence of a strategy order.

## D. Autonomous strategy development

Full detail: [STRATEGY_RESEARCH_AUDIT](TRADINGBOT_STRATEGY_RESEARCH_AUDIT.md).

- **Correction to prior reports:** not every family is FAILED. RESIDUAL (H-015) FAILED (fold-1 gross return not positive; day-clustered lower
  bound not positive in 5/5 folds; pooled net +0.073R over 326 trades). DAILY_TREND (H-018) is REGISTERED/NOT_CERTIFIED/PENDING_HOLDOUT
  (development 2020–2024: net +33.7 %, Sharpe 0.99, max DD 11.5 %, 1,192 trades; p = 0.0147 raw, 0.264 after 18-hypothesis correction).
  The four 15m specialists were never registered. H-001…H-017: rejected or FAILED, almost all for weak evidence (gross edge below cost);
  H-005/H-006 for missing 5m data; none for a software defect. The branch certification draft's "every family is FAILED/M0" is wrong (RES-06).
- **Capability classification (18 items):** absent — opportunity detection, hypothesis generation, improvement proposals; manual —
  retirement; callable library/CLI only — registration, freezing, cost model, backtest, walk-forward, holdout, multiple-testing, verdict
  recording, reproduction; scheduled — only shadow evaluation of an already-coded family (DAILY_TREND). The official evaluator is hard-wired
  to Mandate 004, and the governance CLI cannot register a new hypothesis.
- **Statistical gate thresholds are unset**, so no hypothesis can pass (RES-05); at the plausible standard the holdout only detects Sharpe ≈ 2.7.
- Development testing **can** continue without touching protected holdouts (development data ends 2024-12-31; Section 22's reserved window from
  2026-07-12 must stay sealed; data from 2026-10-01 onward is fresh forward data).

## E. Market data

Full detail: [MARKET_DATA_AUDIT](TRADINGBOT_MARKET_DATA_AUDIT.md).

- Three production collectors, all **Binance-only**, all started by the trading service under the lease: 15m candles for 138 symbols (feeds
  RESIDUAL), daily bars/funding/exchangeInfo for ~900 contracts from the Binance archive (feeds DAILY_TREND shadow; strongest quality controls in
  the repo), and BTC/ETH depth/OI/trades every 5 min (no decision consumer; unbounded storage ≈ 120 MB/day, DATA-03). No market-data websocket.
- The 2026-11-01 `AdapterError` rate-budget incident is **fixed on main** (endpoint weight catalogue, `def0bf4`). BingX pagination is fixed on the
  branch only (research impact only).
- Gaps: the 15m collector bypasses the shared budget and a DEMO-host 429 closes the budget for all Binance traffic including protection (M-19, P1
  for real money); `/health` monitors only the failed family's collector and reports COLLECTING when every symbol fails (DATA-06); research data
  lives on one workstation with no offsite copy and no licence record (M-33). Hard-coded Binance/USDT assumptions in 55–68 core files block
  non-crypto expansion but not the initial Binance launch.
- **Verdicts:** market-data collection PARTIAL; historical research data PARTIAL.

## F. Trading execution

Full detail: [TRADING_EXECUTION_AUDIT Part 2](TRADINGBOT_TRADING_EXECUTION_AUDIT.md).

- **Order handling is strong (L5/L4):** durable `PENDING_SUBMIT` claim before the venue call, plan-derived client order id with no time bucket,
  unknown outcomes reconciled by client id and never resent, restart sweep of orphan claims. No duplicate-order path found in the documented
  single-process topology. In multi-container mode (off by default) the runtime lease has no fencing token (L7 TOCTOU window).
- **M-01 (P0):** on `main`, setting `DEMO_ORDER_SUBMISSION_ENABLED=false` (the documented operational switch) also refuses native stops,
  reduce-only exits, recovery and emergency flatten — main's own test asserts this behaviour. The branch fix `334f2ff` correctly unblocks the
  transport and flatten, but **three call sites still gate risk-reducing actions on the order switch**: post-fill protection / fail-safe close /
  48 h horizon close (`production_execution._reconcile_attempt`, branch ~l.1473 — verified by the lead), recovered-fill protection after an
  unknown ack (`execution/boundary.py:~624`) and Bybit/BingX protection (`execution/perpetual_protection.py:23`). An entry in flight when the
  switch is turned off is left with no stop. Separately, every DELETE (including cancelling the only stop) is classed as risk-reducing.
- **Verdicts:** DEMO execution PARTIAL; LIVE execution BLOCKED (by design).

## G. Brokers

| Venue | Adapter path | Genuine evidence | Status |
|---|---|---|---|
| **Binance DEMO** | platform adapter (certified) + legacy `BinanceExecutor` (strategy path A) | `cert_6852df804410b16bee8ad4d1`, 2026-11-01: hash chain recomputed independently, `mocked_transport=false`, venue order ids present, 30 evidence hashes match; MARKET + GTC LIMIT, leverage 1, ADAUSDT only; expires 2027-01-30 (L2) | **CERTIFIED for the platform adapter only.** Strategy orders run through the legacy tail the certificate never exercised (M-07). Certificate is a local DB row with no import tool: a fresh VPS starts NOT_CERTIFIED (M-06). `adapter_version` is a hand-maintained constant; `record_adapter_version` has no production caller. |
| **Binance LIVE** | same | none | **NOT CERTIFIED**; register refuses LIVE certification in this build (by design). |
| **Bybit** | platform adapter (in-place trading-stop) | tests only (L4) | **NOT CERTIFIED**; no emergency flatten path, rate limits unverified (M-20). |
| **BingX** | platform adapter; stop replacement refused by design; stops carry no client id | tests + public klines only | **NOT CERTIFIED**; no trailing stop (WORKING_AS_DESIGNED but limits strategy support). |
| **Broker-neutral layer** | environment URL table, instrument parsers, venue adapters, certification harness | L5 | Exists and is reusable; families and collectors are still Binance/USDT-specific. |

Broker connection journey: user-backend and CATI share the same `broker_accounts` row and `resolve_broker_auth` (one authoritative identity and
environment; environment mismatch refused). Defects: disconnect/delete leaves the encrypted v2 credentials and soft-deletes bots with open
positions without flattening (M-11); the withdrawal-permission refusal fails open on probe error, BingX or DEMO, and is never re-probed (M-21);
credential validation performs exchange HTTP inside `BEGIN IMMEDIATE` (EXEC-11). Verdict: **Broker connections PARTIAL.**

## H. Risk management

Enforcement at the final boundary is layered (L5/L4): path A hard risk plus re-authorization immediately before submit, the executor's
call-scoped entry permit, and — in production — a transport check that raises `CATI_ENTRY_AUTHORITY_REQUIRED` on any non-reduce-only order POST.
Daily-loss latch, weekly/monthly drawdown, kill switches, trade caps, leverage, margin, liquidation distance, slippage, partial-fill cut and
reservations are implemented on both tails. No order placement exists in user-backend or admin-backend.

Gaps: path B lacks the go-live/L-08/budget/depth/fence gates (M-05); two duplicated risk engines and no correlation check on path B (M-26); the
0.40 % per-trade ceiling for fixed/percent bots is enforced only inside the market-entry gate, not hard risk (CUST-16); the transport gate is a
no-op outside `APP_ENV=PRODUCTION`; `close_btc_raw.py` signs raw LIVE requests bypassing every gate, and legacy admin routes
(`/binance/cancel-all`, `/binance/leverage`, `/trade/protection`) act with the `.env` keys outside the cycle lock (M-25); no risk budget across a
user's multiple accounts (OPS-12). **Verdict: Risk management PARTIAL.** Customer funds on DEMO are protected adequately once M-01 is closed;
real-money protection additionally requires M-05, M-19 and M-25.

## I. Customer functionality

Full detail: [CUSTOMER_JOURNEY_AUDIT](TRADINGBOT_CUSTOMER_JOURNEY_AUDIT.md).

Every live UI call reaches an existing, authenticated, owner-scoped route (both lead and area scans; the only broken calls are dead client
functions). Registration, verification, login, MFA, KYC, broker connect/validate, balances, bot create/deploy (risk-based preview), pause,
resume, stop, notifications and logout are wired and tested (L4). Defects: main sums DEMO and LIVE portfolio money and labels unrealized P&L as
"Daily P&L" (fixed on branch only, M-10); Edit and the legacy deploy body allow fixed/percent sizing with no preview or compatibility check —
the cause of `bot_a8117dc719fc` being untradeable (M-09); no customer close-position action; the stop dialog misdescribes engine behaviour;
negative open P&L shown without its sign; BotDetails delete action never attached to a button; no DOM/E2E tests (M-34, M-32).
**Verdict: Customer Frontend PARTIAL.**

**Sizing case `bot_a8117dc719fc`** (allocation 120 USDT, fixed margin 120, equity 417.58, Balanced): hard risk approves 120 × 4x = 480
notional, which loses 6.13 % of equity at the 5.3 % stop; the 0.40 % ceiling is enforced only in the governed market-entry gate. The trade is
refused in every production configuration (no L-08 record → `MARKET_FALLBACK_DISABLED_BY_POLICY`; with a record → `EXECUTABLE_PRICE_RISK_EXCEEDED`;
with the 100 USDT experimental grant → `EXPERIMENTAL_DEMO_NOTIONAL_LIMIT_EXCEEDED`). It is never silently resized (correct), and the refusal
reaches the dashboard, but **nothing explains the incompatibility before start**. Even at 1x, 120 USDT fits 0.40 % only with a stop under 1.39 %.
The current saved settings could not be verified against the owner's database (NOT VERIFIED); saved settings were not changed.

## J. Administration

Admin authorization is sound: every `/api/admin/*` route requires an admin token, an `admins` row and MFA; emergency, governance,
experimental-DEMO and market-entry actions write durable audit rows (L4). But the console is **not operations-ready** (M-12): System Health shows
hard-coded "healthy" CPU/memory/disk figures; Platform Settings (maintenance mode, signups, risk cap, max bots) persist values nothing reads;
Bot Monitor reads legacy runner tables production never writes; the engine's admin CATI/evidence/operations/readiness endpoints are not proxied,
so admins cannot see evaluations, refusals, positions or readiness. There is one all-powerful admin role (M-29), and suspending a user does not
stop their bots (CUST-11). **Verdict: Admin Frontend PARTIAL.**

## K. Infrastructure

Full detail: [SECURITY_OPERATIONS_AUDIT](TRADINGBOT_SECURITY_OPERATIONS_AUDIT.md).

The package (systemd units with watchdog/restart/alert hooks, nginx TLS configs, Dockerfiles, staging compose, backups, health checks, profile
validator) is substantial. Gaps: main configures no platform DB (M-03, fixed on branch); neither main nor the branch guide installs the ops-tick,
dead-man or platform-backup timers, and platform dumps are not sent offsite (M-13); the profile validator refuses `env.production.example`
(`PROFILE_NAME_MISSING`) and is never enforced at startup; shutdown releases the lease while a cycle may be mid-flight and `force_exit` always
calls `os._exit(0)` 1.5 s later on Python 3.12 (M-27); admin-backend silently creates an empty SQLite file when given a PostgreSQL URL; the
revocation anchor is placed on the same disk. **Nothing has run on the production VPS** (NOT VERIFIED). **Verdicts:** VPS deployment package
PARTIAL; production-host testing NOT VERIFIED / BLOCKED.

## L. Testing

| Suite | Result in this audit | Evidence level |
|---|---|---|
| CI on main `649080b` (run 41303789967) | success | L3/L4 |
| CI on branch `732e0e2` (run 43327108079) | success (previous run on 0da95d6 cancelled) | L3/L4 |
| user-backend (local, Python 3.12, SQLite) | ≈903 passed, 0 failed, 1 xfailed | L4 |
| admin-backend / shared / root | 98 / 299 / 7 passed | L4 |
| bot-backend full suite (local) | see addendum at the end of this section | L4 |
| user-frontend / admin-frontend | 102/102 and 23/23 passed; both builds succeed; ESLint 0 errors (210 / 151 warnings) | L4 |
| Area auditors' targeted runs | ~1,000 targeted tests across CATI, research, sizing, execution, risk-reduction gate (branch), security, deploy suites — all passed except `test_phase2_broker_security.py` which fails 3/10 when run in isolation without `BROKER_SECRET_KEY` (order-dependent, OPS-14) | L4 |

**Genuine vs simulated:** the only genuine venue evidence is the Binance DEMO certification run (L2). Bybit, BingX, Binance LIVE, every
strategy order, partial fills, stop rejection, unknown-ack recovery, emergency flatten, reboot recovery and multi-account operation are tested
against mocked transports only (L4). The 1,000-account load test exercises the coordination layer against in-memory fake venues
(0.05 ms per-account stage time); the repository's own evidence shows one egress IP reconciles only ~30 Binance accounts per minute
(`evidence/predeploy/scale_1000_accounts.json`). Practical estimate: **≈8–15 actively traded Binance accounts per egress IP** (L7, assumptions in
the security/operations audit). There are no frontend DOM or end-to-end tests and no systematic cross-tenant ownership test (M-32).

## M. Complete gap register

124 area findings (P0-class 1 consolidated, P1 39, P2 54, P3 30) are in [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv).
The consolidated master list below removes cross-area duplicates; area IDs in brackets.

### P0 — critical release blocker

| ID | Finding | Root cause | Class | State | Phase |
|---|---|---|---|---|---|
| **M-01** | Turning the order switch off blocks protective stops, reduce-only exits and emergency flatten on main; on the branch three call sites still block post-fill protection, fail-safe/horizon close, recovered-fill protection and Bybit/BingX protection; every DELETE counts as risk-reducing [EXEC-01, EXEC-02, CATI-05, OPS-20] | Entry authority and risk-reducing authority share one gate (`order_submission_gate`) | EXECUTION_GAP / RISK_GAP | Main OPEN; branch PARTIALLY FIXED (verified by lead) | Before any deployment that trades (B) |

### P1 — required before customer trading (or before the stated phase)

| ID | Finding | Class | State | Phase |
|---|---|---|---|---|
| M-02 | Seven verified fixes exist only on the engineering branch; main is the deployment source [LEAD-01] | INTEGRATION_GAP | FIXED_ON_BRANCH_ONLY | A |
| M-03 | Main's deploy package configures no platform PostgreSQL → every entry refused (`PLATFORM_ORDER_BUDGET_UNAVAILABLE`), L-08/go-live consoles 503, durable jobs never run [ARCH-01, CATI-12, OPS-01] | INFRASTRUCTURE_GAP | FIXED_ON_BRANCH_ONLY (a746a58) | B |
| M-04 | No family holds DEMO/LIVE order authority; RESIDUAL FAILED; DAILY_TREND blocked on owner decisions (21 interpretation points, statistical thresholds unset, admission route, holdout authorization, mandate vs executable policy — 86 % of entries have stops >15 % that the engine refuses) [CATI-02, RES-01, RES-02, RES-05, RES-17] | RESEARCH_GAP / OWNER_POLICY_DECISION | OPEN | C |
| M-05 | Portfolio tail (path B) skips go-live gate, platform order budget, L-08 policy, depth freshness and account fence; no tests cover these on path B [ARCH-02, CATI-04] | RISK_GAP / INTEGRATION_GAP | OPEN | C (must close before DAILY_TREND gets DEMO authority) |
| M-06 | Venue certification, experimental grant and L-08 policy are local DB rows; recorded ones expired (grant ≈2026-11-04, L-08 2026-11-08); a fresh VPS has none; no export/import [CATI-11, RES-08, EXEC-06] | OPERATIONS_GAP | OPEN (DB state NOT VERIFIED) | B/C |
| M-07 | DEMO strategy orders (path A) run through legacy `BinanceExecutor`, which the certificate never exercised; `DEMO_VALIDATED` hard-coded; `adapter_version` unchecked [EXEC-05, ARCH-03] | EXECUTION_GAP / TESTING_GAP | OPEN | C |
| M-08 | No venue evidence of any CATI strategy order [CATI-13] | TESTING_GAP | OPEN | C |
| M-09 | Fixed/percent sizing accepted via Edit and legacy deploy body with no preview or compatibility check; Edit has no risk-based option; legacy body bypasses DEMO-only check, consent, idempotency and one-bot rule [CUST-04, -05, -06, -16, CATI-06] | FRONTEND_GAP / LOGIC_DEFECT | OPEN | B |
| M-10 | DEMO and LIVE summed on main; unrealized shown as "Daily P&L"; unsupported brokers silently omitted; analytics/reports still mix environments and currencies and omit funding [CUST-01, -02, -03] | LOGIC_DEFECT | Partly FIXED_ON_BRANCH_ONLY (65372fe) | B |
| M-11 | Disconnect/delete leaves encrypted v2 credentials and orphans open positions (no flatten) [EXEC-03, CUST-08] | SECURITY_GAP / EXECUTION_GAP | OPEN | B |
| M-12 | Admin console misleading/incomplete (fake System Health, no-op Platform Settings, Bot Monitor on legacy tables, CATI/evidence/readiness not proxied) [CUST-07, -07b, -09, -10] | FRONTEND_GAP / OPERATIONS_GAP | OPEN | A/B |
| M-13 | Ops-tick, dead-man and platform-backup timers never installed by the guide; platform dumps not offsite [OPS-02, ARCH-04] | OPERATIONS_GAP | OPEN | A |
| M-14 | No real-host validation of units, TLS, firewall, backups, alert delivery, offsite restore [OPS-03] | INFRASTRUCTURE_GAP | NOT_VERIFIED | A |
| M-15 | Administrator credentials committed in history (db4580b, cc13da0, b35c198); no rotation recorded [OPS-04] | SECURITY_GAP | OPEN | A |
| M-16 | No external penetration test [OPS-05] | SECURITY_GAP / EXTERNAL_DEPENDENCY | BLOCKED | B |
| M-17 | Capacity ≈8–15 active Binance accounts per egress IP; market data starves beyond 3–4 accounts/IP; sequential broker cycle and single-threaded collector [OPS-06, DATA-07, CATI-09, CATI-10, OPS-10] | INFRASTRUCTURE_GAP | OPEN | C (multi-customer) |
| M-18 | No on-call, no alert channel configured, no kill-switch/flatten drill in an authorized environment [OPS-21] | OPERATIONS_GAP | BLOCKED | B |
| M-19 | 15m collector bypasses shared budget; budget keyed by venue not host, so a DEMO-host 429 blocks all Binance traffic incl. protection [DATA-02, DATA-04] | RISK_GAP | OPEN | D (P2 for DEMO) |
| M-20 | Bybit/BingX: no genuine venue evidence, no emergency flatten, unverified limits, decisions from Binance data [EXEC-07, DATA-12] | EXTERNAL_DEPENDENCY / EXECUTION_GAP | BLOCKED | C for those venues |
| M-21 | Withdrawal-permission refusal fails open; no periodic re-probe [EXEC-04] | SECURITY_GAP | OPEN | D |
| M-22 | Legal counsel status and country eligibility not enforced before trading [OPS-17] | OWNER_POLICY_DECISION / MISSING_FEATURE | OPEN | D |
| M-23 | LIVE chain blocked: no certified family, no LIVE venue certification, no live-capable release, policy keys unset, no beta weeks [EXEC-15, OPS-22] | OWNER_POLICY_DECISION / EXTERNAL_DEPENDENCY | BLOCKED (by design) | D |

### P2 — production quality (consolidated)

| ID | Finding | Area IDs |
|---|---|---|
| M-24 | Autonomous research not implemented: no orchestrator, evaluator hard-wired to Mandate 004, no register-hypothesis command, drift/retirement and forward-DEMO tracking manual, cost collector unscheduled (strategic product gap; not a safety blocker) | RES-03, -04, -09, -10, -12 |
| M-25 | Ungated broker access outside CATI: `close_btc_raw.py` raw LIVE signing; legacy admin routes with `.env` keys; transport gate no-op outside PRODUCTION; `emergency_flatten_all` cancels stops before closing | EXEC-09, ARCH-08, EXEC-10 |
| M-26 | Two duplicated risk engines; no correlation check on path B | EXEC-12 |
| M-27 | Shutdown releases the lease mid-cycle; `force_exit` always `os._exit(0)` on Python 3.12, skipping shutdown hooks (naked-position window) | ARCH-09 |
| M-28 | Market-data operations: forward-observation storage ≈40 GB/yr unpruned; `/health` watches only the failed family's collector and says COLLECTING on total failure; archive re-request storm | DATA-03, -06, -08 |
| M-29 | Security hardening: no `BROKER_SECRET_KEY`/`CREDENTIAL_KEY`/KYC key rotation path; `AUTH_BEARER_COMPAT=true`; admin IP allow-list commented out; single admin role | OPS-07, -08, -09, -16, CUST-13 |
| M-30 | Persistence model: three conflicting production DB stories; admin-backend SQLite-only; duplicated admin API; user-backend SQL migrations never run; unversioned runtime schema; no joint restore point | ARCH-05, -06, -07, -12, -16, LEAD-03 |
| M-31 | An experimental grant can be issued for DAILY_TREND but path B ignores grants (accepted, no effect) | CATI-03, RES-07 |
| M-32 | Testing: no systematic cross-tenant test, ~350 routes without direct test reference, no frontend DOM/E2E tests | OPS-15, LEAD-05, CUST-17 |
| M-33 | Research data custody and licensing: datasets on one workstation, no offsite copy, no licence record; hourly history pruned to ~28 days | DATA-10, DATA-14, RES-11 |
| M-34 | Customer operations: suspension does not stop bots (audit row records wrong actor); no customer close-position; per-account errors and data age hidden | CUST-11, -12, -15 |

Other P2 items (one-area, not consolidated) and all P3 items are in the CSV, e.g. ARCH-10 DailySnapshotScheduler never started, ARCH-11 stuck job
claims, DATA-09 mainnet-vs-demo price divergence, RES-13 holdout overlap with inspected data, RES-14 mandate vs executable policy, LEAD-02 duplicate
route, LEAD-04 stale branches, RES-16 dead legacy strategies.

## N. Remediation roadmap

Full plan: [IMPLEMENTATION_ROADMAP](TRADINGBOT_IMPLEMENTATION_ROADMAP.md). Order by dependency:

| Phase | Purpose | Closes | Gate to next phase |
|---|---|---|---|
| **A** Critical trading correctness | Separate risk-reducing authority from entry authority everywhere; merge the branch | M-01, M-02, M-11 (credential/flatten part) | Full CI green on merged main; new tests prove protection with switch off on every tail and venue |
| **B** Core integration | Make the deploy package self-consistent; close path-B gate parity; certification portability | M-03, M-05, M-06, M-07, M-13, M-25, M-27, M-30 (decision) | Staging stack built from the guide runs end-to-end with platform DB, timers and alerts |
| **C** Data and research autonomy | Budget/health fixes; research orchestration on existing CLIs; owner decisions for DAILY_TREND | M-19, M-24, M-28, M-33, M-04 (engineering parts) | Owner ratifies statistical standard; orchestrator produces a governed candidate unattended |
| **D** Trading execution | Path-B grants, sizing compatibility, Bybit/BingX completion, risk-engine consolidation | M-09, M-17, M-20, M-26, M-31 | DEMO strategy order fully exercised in staging against Binance DEMO |
| **E** Customer and admin experience | Truthful portfolio and admin console; customer close; tests | M-10, M-12, M-32, M-34 | Journey E2E tests pass |
| **F** Production infrastructure | VPS install, rotation, pen test, on-call, drills | M-14, M-15, M-16, M-18, M-29 | Host evidence recorded; external pen test closed |
| **G** Real-world DEMO trading | Authorized DEMO trading with forward evidence | M-08, M-04 (forward evidence) | ≥ agreed forward-evidence window with reconciled fills |
| **H** Controlled LIVE certification | LIVE venue certification, live-capable release, legal | M-21, M-22, M-23 | Two-operator release, beta enrollment |

## O. Production decision

Category verdicts (Section 35) with evidence are in [PRODUCTION_READINESS_MATRIX](TRADINGBOT_PRODUCTION_READINESS_MATRIX.md). Summary:

| Release level | Verdict | Exact reason |
|---|---|---|
| **A — Platform available** | **PARTIAL** | Auth/MFA/KYC/broker/bot flows work (L4); blocked by M-02 (merge), M-12 (fake admin health/settings), M-13 (ops timers), M-14 (no host run), M-15 (credential rotation) |
| **B — Broker-connected DEMO** | **PARTIAL** | Binance DEMO connection/reads repaired and certified (L2); requires M-01, M-03, M-09, M-10, M-11, M-06 (certification import) |
| **C — Autonomous DEMO trading** | **BLOCKED** | No family has order authority (M-04); grant and L-08 lapsed (M-06); path B gate parity (M-05); no strategy order evidence (M-08) |
| **D — Controlled real money** | **BLOCKED** | By design (seven independent LIVE locks) plus M-19, M-21, M-22, M-23 and every C prerequisite |

### Final decisions (Section 35)

**Decision 1 — Deploy the complete application to a production VPS with real-money order submission disabled?**
**Not from `main` as it stands. Yes, conditionally, after Phase A.** LIVE submission is genuinely disabled (seven layers, verified L5), so real
money is not at risk. But main has no platform DB configuration (no order can be placed even on DEMO), sums DEMO/LIVE money for customers, and
strands DEMO positions without protection when the order switch is used (M-01). Required first: close M-01 fully on the branch, merge the branch
(M-02), install ops timers (M-13), rotate historical admin credentials (M-15), and make the admin health/settings pages truthful or hidden (M-12).

**Decision 2 — Continuous public market-data collection and strategy research on that VPS?**
**Data collection: yes, after Decision 1 conditions**, with the forward-observation prune rule (DATA-03) and an honest health signal (DATA-06)
added early. The daily feed and 15m collector run unattended under the trading service. **Research: no unattended research exists** —
evaluation, candidate generation, drift and re-evaluation are manual CLIs (M-24). Shadow evaluation of DAILY_TREND does run unattended.

**Decision 3 — Authorized Binance DEMO strategy trading?**
**No.** No family holds DEMO order authority; RESIDUAL can trade only under an experimental grant (it FAILED research), the recorded grant and L-08
policy have lapsed, the certification row is not portable to a new host, and strategy orders use an uncertified execution tail. M-01 must be closed
before any trading. Earliest path: Phase A + B, then either (a) an owner-approved experimental grant for RESIDUAL with a compatible risk-based bot
(diagnostic DEMO trading of a FAILED family, clearly labelled as such), or (b) DAILY_TREND certification after owner decisions plus M-05/M-31.

**Decision 4 — Can real customers use the hosted platform for broker connections, bot management and DEMO functionality?**
**Not yet.** After Phases A and B (plus M-09, M-10, M-11, M-16 pen test and M-18 on-call), customers can register, connect Binance DEMO, create
risk-based bots and monitor accounts. They would see evaluations but **no strategy trades** until Decision 3 is resolved, and the UI must say so.

**Decision 5 — Controlled real-money beta?** **No (BLOCKED).** Requires a certified family at LIVE stage, Binance LIVE venue certification, a
live-capable signed release, two-operator approval, legal/country gating, withdrawal-permission enforcement and the M-19 budget fixes — none exists.

**Decision 6 — Broader public real-money launch?** **No (BLOCKED).** Additionally requires multi-IP capacity (M-17), beta evidence, external
pen test, on-call and incident drills.

**Decision 7 — Can CATI independently research and develop new strategy candidates today?** **No.** The building blocks (register, freezing,
cost model, walk-forward, holdout protection, multiple-testing, verdict ledger) are real and tamper-evident, but nothing proposes hypotheses,
the evaluator is hard-wired to one mandate, and no scheduler chains the stages. Statistical thresholds are unset, so even a manually run
candidate cannot pass.

**Decision 8 — Are all the newer systems integrated into the production runtime?** **No.** Wired: the CATI runtime, residual and daily-trend
paths, execution boundaries, venue runtime, governance/experimental-DEMO/L-08 gates, ops alerting (code). Not wired or not active: the platform
DB layer on main's deployment (M-03), durable job runner and assignments (need platform DB), the regime/forecast/ranking/veto/controller stack,
setup specialists, drift monitoring, DailySnapshotScheduler, TradFi/MT/IBKR, and the ops timers (not installed). Path B lacks path A's newer gates.

**Decision 9 — Engineering work before the first production release (hosted, DEMO, no strategy trading):**
Phase A (M-01 complete, merge M-02, M-11) and the Phase B items M-03 verification on staging, M-06 certification export/import, M-13 timers,
M-27 shutdown, M-25 removal of ungated broker paths, plus M-12 and M-09/M-10 customer truthfulness. See Section Q.

**Decision 10 — Certification, authorization or operational evidence required after the software is complete:**
(1) owner ratification of Mandate 004 interpretation points and the statistical standard, admission route and holdout authorization (RES-01, RES-05);
(2) holdout evaluation of DAILY_TREND and governance promotion through M-stages; (3) re-run of Binance DEMO venue certification on the code path
strategy orders actually use (M-07) and on the production host; (4) genuine Bybit/BingX DEMO certification if those venues are offered;
(5) an operator-approved, time-boxed L-08 market-entry policy and, if used, experimental DEMO grants; (6) forward DEMO evidence with reconciled
fills over the agreed window; (7) VPS host evidence (units, TLS, firewall, backup/restore, alert delivery); (8) credential rotation record;
(9) external penetration test; (10) on-call roster and kill/flatten drill record; (11) legal counsel opinion and country eligibility list;
(12) for LIVE: Binance LIVE venue certification, live-capable release manifest, two-operator release, beta enrollment.

## P. GitHub evidence

| Item | State (verified 2026-11-12/13) |
|---|---|
| `main` | `649080b` "docs: record green CI run 41300468876 in the DEMO activation report" (2026-11-01); CI run 41303789967 **success** |
| `ccr-b5c520cf-80g8x2` | `732e0e2`; 7 commits not on main: `334f2ff` (order switch vs protection), `a746a58` (VPS platform DB), `f8401d5` (BingX pagination), `65372fe` (DEMO/LIVE portfolio separation), `1d552d4` (certification draft — contains the inaccurate "every family FAILED"), `0da95d6` (Linux load/restore evidence), `732e0e2` (flatten test). Merge-base = main; `git merge-tree` clean (no conflicts). CI run 43327108079 on `732e0e2` **success** (run 43322545193 on `0da95d6` cancelled). |
| Other branches | `audit-fixes`, `cleanup-dead-code`, `step1-portal-to-engine` (180–182 ahead / 50 behind, last 2026-10-07), `claude/cosmicforge-audit-multi-asset-ht7upz` (83 ahead / 50 behind, 2026-09-25): stale, superseded (LEAD-04) |
| Open pull requests | none |
| Release tags | archive tags only (`archive/*`, `v2-frozen-benchmark-ce5ff8c`); **no release tag, no live-release manifest** |
| Deployment workflows | CI only (`.github/workflows/ci.yml`: lint, bot/legacy-PG/user/admin-shared-root suites, both frontends, containers); no deploy workflow |
| Uncommitted changes | none in the clone |
| Deployable as one consistent application | Main: consistent but defective (M-01, M-03, M-10). Branch: consistent, CI green, still M-01 partially open. |

## Q. Next engineering assignment

**Title:** CosmicForge Tradingbot — Release-Candidate Engineering: Phases A and B.
**Baseline:** branch `ccr-b5c520cf-80g8x2` @ `732e0e2` (rebased on `main` `649080b`). Do not repeat this audit; use the gap register IDs.
**Constraints:** do not change strategy parameters, research verdicts, saved customer settings, or LIVE locks; do not open holdouts; no payment work;
every task ships with tests; CI must stay green.

1. **M-01 — Risk-reducing authority (P0).** Introduce a single `risk_reducing_permitted(environment, action)` decision distinct from
   `order_submission_gate`. Route through it: `production_execution._reconcile_attempt` post-fill `submit_protection`, fail-safe close and horizon
   close; `execution/boundary.reconcile_submit_unknown` recovered-fill protection; `execution/perpetual_protection.place_protection` (Bybit/BingX).
   Narrow "risk-reducing" for DELETE: a cancel is risk-reducing only if it does not remove the last protective order for an open position (or is
   part of an atomic replace). Acceptance: new tests — with `DEMO_ORDER_SUBMISSION_ENABLED=false`, (a) a fill in flight gets a native stop, (b)
   an unknown-ack recovered fill gets a stop, (c) Bybit/BingX protection is placed, (d) entries/increases are refused, (e) cancelling the only stop
   is refused, (f) LIVE remains fully refused in this build. Update `test_emergency_controls` expectations accordingly.
2. **M-02 — Merge.** Open a PR from the branch to `main` after item 1; correct the "every family FAILED" statement in
   `TRADINGBOT_FINAL_SYSTEM_CERTIFICATION.md` (RES-06); full CI green on merged main.
3. **M-11 — Disconnect/delete.** Delete `broker_credentials_v2` rows on disconnect; refuse disconnect/delete while positions or open orders exist
   unless the customer confirms a flatten, which runs through `production_runtime.flatten`; tests for both.
4. **M-03 — Platform DB on staging.** Build a staging host strictly from the merged guide (PostgreSQL 16, `PLATFORM_DATABASE_URL`, `migrate`,
   `status`); assert `/health` reports the platform layer enforced; make `scripts/validate_config_profile.py` accept the production env (add
   profile name) and run it at startup in production (fail closed). Make admin-backend refuse non-SQLite URLs instead of creating an empty file.
5. **M-05 — Path-B gate parity.** Apply go-live gate, platform order budget/spread (L-08), depth freshness and account fence to
   `PortfolioExecutionBoundary` before any family obtains DEMO authority; tests mirroring `test_step5_spread_boundary` on path B.
6. **M-06 — Certification portability.** CLI to export/import venue-certification records with their hash chain and evidence, verified on import;
   call `record_adapter_version` at startup; refuse certification reuse when the adapter version differs; refresh `EXCHANGE_CERTIFICATION.md`.
7. **M-07 — One certified execution tail.** Either certify the legacy `BinanceExecutor` path on Binance DEMO through the existing harness, or route
   path A entries through the certified platform adapter. Remove the hard-coded `DEMO_VALIDATED`; pass `adapter_version` to `adapter_certified`.
8. **M-13 / M-27 — Operations.** Add ops-tick, ops-deadman and platform-db-backup timer installation and offsite upload of platform dumps to the VPS
   guide; fix `force_exit` (use the captured loop, never `os._exit` while a cycle holds `_cycle_lock`) and keep the lease until the broker cycle is idle.
9. **M-25 — Ungated paths.** Delete or gate `close_btc_raw.py`, `close_btc.py` and other root scripts that sign broker requests; move legacy
   `/binance/cancel-all`, `/binance/leverage`, `/trade/protection` behind the cycle lock and CATI account authority or remove them.
10. **M-09 / M-10 / M-12 — Truthful customer and admin surfaces.** Add a risk-based option and a compatibility preview to Edit; route the legacy
    deploy body through the preview/consent/idempotency path or remove it; finish environment/currency separation in analytics/reports and
    funding inclusion; replace System Health with real metrics (or remove the page), wire or remove Platform Settings, retarget Bot Monitor to CATI
    tables, and proxy the engine's admin CATI/evidence/readiness endpoints.
11. **M-19 / DATA-03 / DATA-06 (data safety).** Route the 15m collector through the shared budget and record its 429/418; key the budget by host;
    add a prune rule for forward observations; make `/health` reflect DAILY_TREND feed freshness and per-symbol failure ratios.
12. **Exit criteria for this assignment:** all of the above merged to `main` with tests; staging deployment from the guide passes health,
    backup/restore and alert-delivery checks; gap register states updated with commit SHAs and test names.

Owner actions to run in parallel (not engineering): ratify Mandate 004 interpretation points and the statistical standard (RES-01/RES-05); decide
admission route and holdout authorization; decide whether to grant time-boxed experimental DEMO for RESIDUAL as a labelled diagnostic; rotate
historical admin credentials (M-15); commission an external pen test (M-16); name on-call and alert channels (M-18); obtain legal opinion (M-22).

---

## Section-coverage checklist (Definition of Audit Complete, Section 37)

| # | Requirement | Where |
|---|---|---|
| 1 | Every major service inspected | SYSTEM_INVENTORY §2.1; API inventory |
| 2 | Newer systems traced to production consumers | SERVICE_DEPENDENCY_MAP §5; Decision 8 |
| 3 | Strategies and research components inventoried | STRATEGY_RESEARCH_AUDIT §2 |
| 4 | Autonomous strategy discovery assessed | STRATEGY_RESEARCH_AUDIT capability table; Decision 7 |
| 5 | Every data pipeline evaluated | MARKET_DATA_AUDIT |
| 6 | Broker adapters have independent status | §G; PRODUCTION_READINESS_MATRIX §3 |
| 7 | CATI decision path traced | TRADING_EXECUTION_AUDIT Part 1 §2.3 |
| 8 | Execution and protection lifecycle assessed | TRADING_EXECUTION_AUDIT Part 2; M-01 |
| 9 | Bot configuration and risk behaviour verified | CUSTOMER_JOURNEY_AUDIT §2.2–2.3 (DB state NOT VERIFIED) |
| 10 | Customer and admin workflows traced | CUSTOMER_JOURNEY_AUDIT §2.1, §2.5 |
| 11 | APIs inventoried | TRADINGBOT_API_INVENTORY.csv (709 routes) |
| 12 | Database and worker responsibilities mapped | SYSTEM_INVENTORY §2.3, §2.5 |
| 13 | Security and operational controls assessed | SECURITY_OPERATIONS_AUDIT |
| 14 | GitHub branch integration verified | §P |
| 15 | Blocker reports reconciled | SECURITY_OPERATIONS_AUDIT (Section 29 table) |
| 16 | Genuine tests distinguished from simulations | §L |
| 17 | VPS readiness evaluated | §K (host NOT VERIFIED) |
| 18 | Customer trading readiness answered | §O, Decisions 3–6 |
| 19 | Every gap classified, prioritized, with correction | Gap register CSV |
| 20 | Implementation roadmap prepared | IMPLEMENTATION_ROADMAP; §Q |

**Audit completion verdict:** `COSMICFORGE_TRADINGBOT_FULL_SYSTEM_AUDIT_COMPLETE`

Issued on the basis that every required section has supporting evidence at the levels stated. The verdict certifies completion of the audit,
not readiness of the platform; items that require a production host, the owner's database or authenticated venue sessions are explicitly
recorded as NOT VERIFIED rather than assumed.
