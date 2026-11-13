# Customer Journey, Bot Configuration, Portfolio & Administration Audit (Sections 10, 15, 17, 18)

**Audit:** CosmicForge Tradingbot master end-to-end audit (read-only) · **Date:** 2026-11-13
**Baseline:** `origin/main` = `649080b` · engineering branch `origin/ccr-b5c520cf-80g8x2` = `732e0e2` (7 commits not on main, inspected by diff/worktree only)
**Master report:** [TRADINGBOT_MASTER_END_TO_END_AUDIT.md](TRADINGBOT_MASTER_END_TO_END_AUDIT.md) · **Gap register:** [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv)

Evidence levels used throughout: L1 observed authenticated behaviour · L2 real venue evidence · L3 integration test with real boundaries ·
L4 contract/unit test with mocks · L5 static source inspection · L6 documentation/historical claim · L7 inference.
No production database, VPS or authenticated venue session was available to this audit; nothing below is L1.
Finding IDs are stable and map to the gap register (column "Master ID" consolidates duplicates across areas).

## Lead auditor reconciliation
- Lead route introspection (APIRoute objects only) counted user 368 / bot 277 / admin 64 = 709 routes (see TRADINGBOT_API_INVENTORY.csv); this area's dump counted 374/288/68 because it also included mounted/static and documentation routes. Both scans agree that every live frontend call reaches an existing authenticated backend route.
- Frontend suites were run by the lead as well: user-frontend 102/102, admin-frontend 23/23, both builds succeed, ESLint 0 errors (210 / 151 warnings).

---
## CUST — Bot lifecycle & customer configuration, portfolio accounting, customer journey, admin portal

Audit sections 10, 15, 17, 18. Repo /home/user/cosmicforge-bot at origin/main 649080b (read-only). Branch
origin/ccr-b5c520cf-80g8x2 inspected with `git diff` only.

### 1. Scope and method

* **Route inventory (L5, mechanical):** imported each FastAPI app under the test env and dumped every route with its
  handler file:line (user-backend 374, bot-backend 288, admin-backend 68 routes):
  `scratchpad/cust/routes_{user,bot,admin}-backend.txt`.
* **Contract check (L5, mechanical):** `scratchpad/cust/contract.py` / `contract2.py` extract every literal API path
  (incl. `${CONST}/...` prefixes) from `frontends/*/src` (tests excluded), normalise params, and match method+path
  against the backend that nginx routes them to (deploy/nginx/cosmicforge-app.conf:145-318 — both SPAs' `/api/` go to
  user-backend :8000; admin-backend :8100 only via `/admin-api/api/admin/`). Second stage matched every user-backend
  proxy target (`proxy_request(...)`, `BOT_BACKEND_BASE_URL` f-strings) against bot-backend routes.
  Results: `scratchpad/cust/fe_user_contract.txt`, `fe_admin_contract.txt`.
* **AuthZ check (L5, mechanical):** walked each user-backend route's FastAPI dependency tree; every `/api/admin/*`
  route depends on `require_admin` → `get_current_admin` (core/deps.py:70-96: admin_access token type + `admins`
  row + mandatory-MFA check). Only expected public routes lack auth.
* **Tests run (all green):**
  user-frontend `npm test` 102/102 (node:test, source-level, no DOM); admin-frontend `npm test` 23/23;
  `tsc -b --noEmit` both frontends clean;
  user-backend: test_auth_hardening, test_broker_flow, test_step1_proxies, test_admin_emergency_proxy,
  test_admin_governance_proxy, test_admin_market_policy_proxy, test_section_h84_security, test_cati_product_controls,
  test_notifications_auth → 94 passed;
  bot-backend: tests/trading_intelligence/test_bot_sizing_config.py + tests/test_capital_allocation_semantics.py → 76
  passed; test_deployment_contract_step1, test_bot_lifecycle_states, trading_intelligence/test_customer_dashboard,
  test_auto_pilot_capital_flow → 51 passed.
* Production entry points used for "wired" judgements: deploy/systemd/cosmicforge-{user-backend,admin-backend,trading}
  .service → each `app.main:app`; bot-backend main.py:1118 "Production: … the legacy runner is not built".

### 2. Detailed analysis

#### 2.1 Customer journey (27 steps)

Legend: FE = frontends/user-frontend/src; UB = backends/user-backend/app; BB = backends/bot-backend/app.
"Tests": FE tests are source-text checks (node:test) — there are **no component/DOM tests and no vitest** in either
frontend; backend tests are L4 unless noted.

| # | Step | Frontend | API call (client) | Backend handler | DB | AuthN/Z | Error handling | Tests | Status |
|---|---|---|---|---|---|---|---|---|---|
| 1 | Open | pages/LandingPage.tsx, Features/Pricing | GET /public/home etc. (api/client.ts:1003-1045) | UB api/public.py | public_* tracking | none (public) | fetch errors → page fallback | placeholderPages.test.ts | WORKING |
| 2 | Register | pages/Register.tsx | POST /api/v1/auth/register (client.ts:858) | UB api/auth.py:470 | users | public, rate-limited | detail surfaced | test_auth_hardening | WORKING |
| 3 | Verify email | pages/VerifyEmail.tsx | POST /api/v1/auth/verify-email (client.ts:879), resend :889 | UB auth.py:587, :619 | users.is_verified | token | message | test_auth_hardening, test_magic_link_flow | WORKING |
| 4 | Login | pages/Login.tsx | POST /api/v1/auth/login (client.ts:840, form + totp_code) | UB auth.py:642 (403 unverified :700, suspended :705) | auth_sessions, cookies+CSRF | password(+TOTP) | timeout message; 401 refresh single-flight (api/http.ts) | sessionAuth.test.ts, test_section_h84_security | WORKING |
| 5 | Security / MFA | pages/Setup2FA.tsx, SecuritySettings.tsx, components/Auth/DeviceManagement.tsx | POST /api/v1/auth/2fa/{setup,verify,disable} (client.ts:949-970); GET/POST sessions (982-990) | UB auth.py:1063/1091/1118/878/1150 | users.totp, auth_sessions | user session | messages | test_auth_hardening | WORKING (customer MFA optional) |
| 6 | Connect broker | pages/BrokerConnection.tsx (1359 lines) | POST /api/v1/brokers/connect (client.ts:1280), /{id}/credentials (1307); IBKR POST /brokers/ibkr/link/start (1292) | UB api/brokers.py:57, :74, :276 | broker_accounts, broker_credentials_v2 (Fernet, shared_lib broker_security) | user; ownership | ReasonedApiError with reason_code (client.ts:815-830) | brokerEnvironment.test.ts, test_broker_flow, test_phase2_broker_security | WORKING (Binance/Bybit/BingX) |
| 7 | DEMO / LIVE | BrokerConnection env selector, lib/brokerEnvironment.ts | part of connect body | stored broker_accounts.environment; deploy refuses non-DEMO BB core/deployment_service.py:229-231 | broker_accounts | server-derived env | LIVE_NOT_AVAILABLE blocker | test_deployment_contract_step1 | WORKING (LIVE blocked by design) |
| 8 | Validate | BrokerConnection | POST /api/v1/brokers/{id}/validate (client.ts:1319/1380); test-connection 1389 | UB brokers.py:167 → validate_and_activate_credential_v2 | broker_credentials_v2.status | user, owner-scoped SQL | reason codes | test_broker_flow | WORKING |
| 9 | Balances | pages/Portfolio.tsx AccountCard, BrokerConnection.tsx:38 | GET /api/v1/brokers/{id}/summary (client.ts:1350); GET /api/portfolio/summary (1059) | UB brokers.py:208 → broker_service.get_broker_summary (binance/bybit/bingx/oanda :478-500); UB api/portfolio.py:8 → core/portfolio_service.py | live venue read | user | per-account summary 404-hides foreign ids; aggregate see §2.4 | none for portfolio_service on main (branch adds test_portfolio_summary_environments) | PARTIAL — aggregate sums DEMO+LIVE (CUST-01) |
| 10 | Create bot | components/AutoPilot/CatiDeployment.tsx (crypto); AutoPilot.tsx legacy form (forex) | POST /api/v1/auto-pilot/deploy (client.ts:1914 Step-1 body; :1877 legacy body) | UB auto_pilot_proxy.py:113 dispatches: Step-1 → BB /auto-pilot/deployments (api/auto_pilot.py:143 → deployment_service.deploy :338); legacy → BB /auto-pilot/deploy (auto_pilot.py:179) | bot_instances, consent table, deploy_request_id idempotency | user + require_permission("strategy:execute") + ownership (_assert_account_owned) | DeploymentRefusedError with blockers (lib/deployment.ts) | deployment.test.ts, test_step1_proxies, test_deployment_contract_step1 | WORKING (Step-1); legacy path bypasses Step-1 gates (CUST-06) |
| 11 | Strategy / AutoPilot | AutoPilot.tsx (strategies routes redirect to auto-pilot, App.tsx:115-116) | — (strategy fixed: cati) | BB bot_instance_service.create_bot_instance (CATI_ONLY_ENGINE) | strategy_id='cati' | — | — | test_bot_lifecycle_states | WORKING; 19 dead client functions to /api/strategies*, /api/strategy-configs*, /api/risk/templates with no backend route and no caller (CUST-14) |
| 12 | Allocation / risk | CatiDeployment (budget type/value, risk level, advanced: symbols, max_position_usdt, daily_loss_limit_pct) | inside deploy/preview body (shared_lib/deployment/contract.py:115-176) | deployment_service.evaluate :202-316 | — | — | blockers INVALID_ADVANCED_SETTINGS, BUDGET_* | test_deployment_contract_step1 | WORKING |
| 13 | Eligibility review | CatiDeployment preview panel (api.previewDeployment client.ts:1898) | POST /api/v1/auto-pilot/preview | UB auto_pilot_proxy.py:93 → BB auto_pilot.py:128 → deployment_service.preview | read-only (+1 authoritative venue read) | user + bot:read | blockers list, consent text, typical_position | deployment.test.ts, test_deployment_contract_step1 | WORKING for Step-1 risk-based; no equivalent for edits / fixed-amount (CUST-04) |
| 14 | Deploy | CatiDeployment.tsx:187 api.deployBot | POST /api/v1/auto-pilot/deploy | as #10; bot created active; broker capability gate bot_instance_service.py:100; user-capital readiness gate for LIVE :119 | bot_instances | as #10 | 409 ACCOUNT_ALREADY_HAS_BOT, 422 blockers | as #10 | WORKING |
| 15 | View evaluations | Dashboard → components/Dashboard/CatiTrading.tsx:576 (5 s poll); BotDetails → CatiBotPanel.tsx | GET /api/v1/cati/runtime/dashboard; /api/v1/cati/bots/{id}/{status,events} | UB cati_proxy.py:26-58 → BB cati_runtime.py:16 (customer_dashboard.py:237) and cati_account.py:74-114 | cati_production_decisions (latest per account), user_events | user + bot:read + owner (`_owned_bot` 404) | Retry buttons, isError states | apiRoutes.test.ts, botEvents.test.ts, test_customer_dashboard | WORKING (latest evaluation only; no history list) |
| 16 | Decisions / refusals | CatiTrading entry_permission + reason_registry text; CatiBotPanel events | same | customer_dashboard._entry_permission, reason_registry.describe | — | — | refusal_explanation (d09421c) shown | test_customer_dashboard, test_bot_sizing_config | PARTIAL — BB /bot-instances/{id}/decision-diagnostics (bot_instances.py:254) not proxied, no UI |
| 17 | Positions | CatiBotPanel positions; pages/analytics/OpenPositions.tsx; Portfolio positions | GET /api/v1/cati/bots/{id}/positions; /api/portfolio/summary | BB cati_account.py:80; UB portfolio_service | venue + DB | owner | error states | test_customer_dashboard | WORKING (portfolio mixes envs on main) |
| 18 | Stop/protection status | CatiTrading open_positions with stop state | runtime/dashboard | customer_dashboard._protective_stops :171 | protective orders | owner | — | test_customer_dashboard | WORKING |
| 19 | History | pages/analytics/CompletedTrades.tsx, TradesTable.tsx | GET /api/analytics/trades, /api/analytics/positions/history (+export) (client.ts:1614, 2153, 2179) | UB analytics_reporting.py:149/336/397 → BB analytics_reporting.py:209/681/990 | trade_fills, positions | user | Retry | — (no FE test) | WORKING; not environment-segregated (CUST-03) |
| 20 | Performance | pages/Analytics.tsx, analytics/AnalyticsOverview.tsx, components/analytics/EquityCurve.tsx | GET /api/analytics/{overview,equity-curve,drawdown,benchmarks}; /api/v1/reports/* (api/reports.ts) | UB analytics_reporting.py:114-264, reports_proxy.py → BB analytics_reporting.py, reports.py, analytics/pnl_service.py | equity_snapshots, trade_fills | user | engine status passed through | test_step1_proxies (proxy only) | PARTIAL — no DEMO/LIVE split, single-currency fetchone, no funding (CUST-03) |
| 21 | Notifications | components/Dashboard/NotificationBell.tsx, pages/NotificationSettings.tsx | GET /api/notifications/alerts, POST acknowledge(-all), preferences, telegram link | UB notifications.py:114/147/171/50/63/188 | notification tables | user | — | test_notifications_auth | WORKING |
| 22 | Edit | pages/EditBot.tsx (BotDetails.tsx:213, BotInstanceRow.tsx:197) | PATCH /api/v1/bot-instances/{id} (client.ts:2005) | UB bot_instances_proxy.py:184 → BB bot_instances.py:326 (_EDITABLE_FIELDS :321) | bot_instances | owner + bot:control | 422 reason | — | PARTIAL — no preview, no risk_based option, misleading helper text (CUST-04/05) |
| 23 | Pause | MyBots.tsx:112, BotDetails.tsx:79 | POST /api/v1/bot-instances/{id}/pause | UB proxy :127 → BB bot_instances.py:406 → service :639 | status='paused' | owner + bot:control | actionError shown | test_bot_lifecycle_states | WORKING (engine keeps maintaining open positions: production_execution.py:697-703) |
| 24 | Resume | MyBots.tsx:96 / BotDetails.tsx:62 (Start) + LiveTradingConfirmDialog | POST /{id}/start | UB proxy :72 (KYC for LIVE) → BB bot_instances.py:377 (plan gate for live non-DEMO) | status='active' | owner | dialog keeps server reason | test_bot_lifecycle_states | WORKING; resume does not re-run deployment preview (balance/ceiling) |
| 25 | Stop | MyBots.tsx:120/166 (Stop all), BotDetails.tsx:91; confirmation text MyBots.tsx:370 | POST /{id}/stop | BB bot_instances.py:426 → service :671 (stopped_reason USER_STOP) | status | owner | — | test_bot_lifecycle_states | WORKING; no customer "close position" action anywhere (CUST-12) |
| 26 | Disconnect broker | BrokerConnection.tsx:294-358 (confirm says positions not closed) | POST /api/v1/brokers/{id}/disconnect (client.ts:1330); DELETE /{id} (1341) | UB brokers.py:184 → broker_service.disconnect_broker_account :925-963; :977-990 delete | deletes **legacy** broker_credentials only; bots → 'deleted' | owner check :931 | 404 for foreign id | test_broker_flow | PARTIAL — v2 credential blobs not removed; allowed with open positions (CUST-08) |
| 27 | Logout | DashboardLayout → AuthContext | POST /api/v1/auth/logout (client.ts:919) | UB auth.py:858 (+ logout-all :1032) | auth_sessions.revoked_at | session | clears session | sessionAuth.test.ts | WORKING |

**Broken contracts found** (frontend → user-backend → bot-backend): the only frontend calls without a backend route
are dead client functions with **no UI caller** — user client.ts:1490-1841 (`getStrategyCatalog`, `getStrategies`,
`createStrategy*`, `getMyStrategies`, `validateStrategySpec`, `saveStrategyVersion`, all `*StrategyConfig*`,
`get/resetProtection*`, `getRiskTemplates`) and admin client.ts:561-979 (`/monitoring/{dashboard,traces,trace,
violations}`, `/api/brokers/*` (wrong prefix), `/api/strategies*`, `/api/strategy-configs*`, `/api/risk-profiles/*`,
`/api/analytics/leaderboard`). All live UI paths resolve. User-backend dead proxy: `POST /api/v1/bot-instances`
(bot_instances_proxy.py:17) targets a bot-backend route that does not exist (bot-backend has only GET). Admin
`getMLRuntimeStatus` (api/admin.ts:905, used by hooks/useAdminML.ts:136) calls `/api/admin/ml/runtime-status`,
served only by admin-backend; with the default `VITE_USE_ADMIN_BACKEND_ML` unset it goes to user-backend → 404.
**Buttons not wired:** BotDetails.tsx:104-117 builds a delete `useMutation` whose result is discarded and no Delete
button exists (comment :427) — harmless dead code; delete is reachable from MyBots.

#### 2.2 Bot lifecycle settings: Frontend → API → DB → effective policy → CATI → risk → execution

| Setting | Frontend | DB column | Effective policy / CATI use (file:line) | Used or display-only |
|---|---|---|---|---|
| Strategy | fixed "CATI" | strategy_id | create_bot_instance CATI_ONLY_ENGINE | used |
| Broker / env / account | CatiDeployment account select | broker_account_id; env from broker_accounts.environment (never request) | deployment_service.py:223-231; customer_dashboard env from account | used; DEMO-only for Step-1 |
| Capital budget (fixed / %) | CatiDeployment budget; EditBot capital_allocation(_type) | capital_allocation, capital_allocation_type | production_execution.resolve_executor_capital_budget :228-251 (percent resolved per cycle from equity, fail-closed); daily-loss base; NOT an aggregate cap (risk/capital_ledger.py:14-22) | used |
| Position allocation | Step-1 always writes allocation_type='risk_based', value=profile.per_trade_risk_pct (deployment_service.py:369); EditBot/legacy can write fixed_amount / percent_balance | allocation_type, allocation_value | production_execution.py:502-555 → use_fixed_size/fixed_size_usdt :530-531 → trading_orchestrator.py:765-770 trade_amount_mode fixed | used |
| Risk profile | CatiDeployment risk level | risk_level, risk_profile_version | risk_profile_params / shared_lib risk_levels (per-trade 0.40 % system ceiling risk_levels.py:60) | used |
| Leverage | not user-editable | — (profile leverage_ceiling) | execution/leverage.py:48 resolve_cati_leverage (compound-risk limit), production_execution.py:529 requested_leverage from policy.max_leverage | used (derived) |
| Max open positions | not shown | — | hard-coded `max_open_positions=1` production_execution.py:527 | used (fixed) — EditBot text "~N concurrent trades max" contradicts it (CUST-05) |
| Universe / symbols | CatiDeployment advanced.symbols | symbols, universe_mode | production_execution.py:520-524 (BROKER → frozen family universe) | used |
| Timeframes | not shown | timeframes ['15m'] | not read by CATI production path (grep) | display/legacy only |
| max_position_usdt | advanced | max_position_usdt | production_execution.py:532-536 (risk-based only) | used for risk_based; ignored for fixed/percent |
| Daily loss limit | CatiDeployment advanced, EditBot | daily_loss_limit_pct | account_daily_loss_policy :197 (strictest of account's bots) | used |
| Validation / preview | CatiDeployment | consent table, deploy_request_id | deployment_service.evaluate | used (Step-1 only) |
| Activate/pause/resume/stop/delete/archive | MyBots, BotDetails | status, stopped_reason | engine reads status='active' (production_execution.py:745, 968, 1006); positions of non-active bots still maintained (:697-703) | used |
| Recovery | — | bot_instances + attempt ledger | runtime owner host rebuilds from DB (other auditors' scope) | n/a |

#### 2.3 Sizing case bot_a8117dc719fc

Found in repo: backends/bot-backend/app/risk/capital_ledger.py:14-22 (worked example of the old ledger capping each
entry at `capital_budget - committed` → 60.04 / 59.98 margins); tests/trading_intelligence/test_bot_sizing_config.py
:11-13, :44-54 (OWNER_EQUITY=417.58, OWNER_ROW id=bot_a8117dc719fc, Binance DEMO account brk_c729454e6c98,
allocation_type=fixed_amount 120, capital_allocation 120, Balanced, mode live); tests/test_capital_allocation_semantics.py;
scripts/phase12/bootstrap.py, scripts/build_master_ensemble_baseline.py. Commit d09421c (on main) = "each bot's
saved allocation reaches CATI sizing; size refusals explained, never resized".

**Resolution order (code):**
1. Fixed margin: `fixed_amount` value is MARGIN per trade, not notional, not an aggregate cap
   (capital_ledger.py:1-12; production_execution.py:530-531; trading_orchestrator.py:765-770).
2. Fixed notional: does not exist as a mode; notional = margin × resolved leverage.
3. % allocation: notional = pct × min(equity, budget) (production_execution.py:228-251; test_bot_sizing_config
   `test_a_percentage_allocation_is_that_percentage_of_the_usable_balance_as_notional`).
4. Risk-based: size = budget × min(profile risk, 0.40 %) / stop distance (execution/risk_sizing.py:123
   size_risk_based; MAX_STOP_DISTANCE 15 % :45).
5. Leverage: resolve_cati_leverage (leverage.py:48) binding COMPOUND_RISK_LIMIT 0.225/5.33 % → 4×.
6. Chain may only shrink (capital_ledger.py:46-58): risk-derived size → allocation → explicit portfolio limit (none
   configured) → per-symbol → venue minimums; never rounded up. Available margin: executor live preflight
   (AccountMarginReservations). Portfolio risk: max_open_positions=1, account daily loss.
7. **Per-trade risk ceiling 0.40 %**: enforced for fixed/percent bots only at the executable-price check of a
   **governed market entry** (scale/spread_gate.py:604-621 `_executable_market_entry`, called :333-335 only when a
   L-08 market_entry_policy record resolves). Hard risk itself APPROVES the 120 USDT × 4× = 480 USDT notional with a
   ~25.6 USDT (6.13 % of equity) loss at the 5.3 % stop (test `test_the_owner_fixed_120_is_approved_at_full_margin…`).

**Would the trade be refused? Yes, in every production configuration (L4 + L5):**
* no L-08 record (the stated production default, spread_gate.py:51-53): MARKET entry refused
  `MARKET_FALLBACK_DISABLED_BY_POLICY` (spread_gate.py:312-316; every entry adapter sends MARKET, :14);
* with an L-08 record: `EXECUTABLE_PRICE_RISK_EXCEEDED` (6.13 % > 0.40 %), test
  `test_the_owner_trade_is_refused_at_the_executable_price_for_risking_more_than_040` (passed); with a real tick,
  `MARKET_SPREAD_EXCESSIVE` first;
* with an experimental DEMO grant cap 100: `EXPERIMENTAL_DEMO_NOTIONAL_LIMIT_EXCEEDED:480.0>100.0` (passed).
* Structurally: at 417.58 equity the ceiling is 1.67 USDT; 120 USDT margin fits only at ≤1.39 % stop at 1× (test
  `test_no_leverage_lets_a_fixed_120_margin_fit_the_040_limit…`). The configuration can essentially never trade.
* No resizing; the refusal explanation (boundary.py `sizing_refusal`) reaches the evaluation record and dashboard.

**Does the deployment UI explain incompatible settings before start? No for this bot's configuration.** Step-1
preview (deployment_service.py:244-300) only models risk-based sizing and always creates `risk_based` bots (:369),
so a fixed-amount bot like this one can only originate from the legacy `/deploy` body (auto_pilot.py:179, still
accepted for crypto via UB auto_pilot_proxy.py:113-128) or from EditBot (PATCH, bot_instances.py:326-362 validates
only `resolve_effective_bot_policy`, no equity/stop/ceiling compatibility check). Neither path previews. Main shows
no warning; branch 65372fe adds static text ("refused if loss at stop > 0.40 %") to AutoPilot.tsx/EditBot.tsx but
no computed per-account check, and EditBot still shows "~{capital/allocation} concurrent trades max"
(EditBot.tsx:210-212), contradicting max_open_positions=1 and the per-trade (not aggregate) semantics.

#### 2.4 Portfolio accounting

* **Main sums DEMO and LIVE: YES (L5).** UB core/portfolio_service.py:130-142 adds every connected Binance account's
  totalMarginBalance/wallet/uPnL/margin into one total regardless of environment; headline
  `total_portfolio_value` (:249) and `account_details` (:256-263). Fixed only on branch 65372fe (per-environment
  `by_environment`, headline one env, `mixed_environments`) → FIXED_ON_BRANCH_ONLY. Branch does not touch
  Portfolio.tsx (still shows `account_details.margin_balance` with no environment label, Portfolio.tsx:125-131).
* Open P&L labelled as daily: main `total_day_pnl = total_unrealized_pnl` (:253-256) rendered as "Daily P&L"
  (AdvancedDashboard.tsx:95, :200); percent = uPnL / wallet. Fixed on branch only. Also negative values render
  without a minus sign (AdvancedDashboard.tsx:202 `{dayChange >= 0 ? '+' : ''}$${Math.abs(...)}`), main and branch.
* Unsupported brokers: Bybit/BingX connected accounts silently omitted from aggregate totals on main (:187-188);
  branch reports BROKER_PORTFOLIO_READ_UNSUPPORTED. Per-account summary (broker_service.get_broker_summary :478-500)
  supports them.
* Missing venue field read as 0 (`acc_data.get(..., 0)` :133-138) — fixed on branch. `str(exc)` published in
  `account_errors` (:116, :178) — fixed on branch. Frontend never renders `account_errors` (no reference in
  Portfolio.tsx / AdvancedDashboard.tsx) → failed accounts silently drop out of totals (main and branch).
* bot-backend `GET /api/v1/analytics/equity-latest` (api/equity.py:135-205) sums equity across all accounts and
  currencies, hard-codes `"currency": "USDT"`, no environment split (main and branch). Client has getEquityLatest
  but no UI caller found → P2/P3.
* Realized P&L (analytics/pnl_service.py:55-121): sums CLOSE fills across all accounts (no environment filter
  unless broker_account_id passed), only CLOSE-fill fees (entry fees excluded from `total_fees` here), no funding,
  `GROUP BY quote_currency … fetchone()` returns one arbitrary currency group. Reports UI (EquityCurve.tsx,
  TaxReportExport.tsx) calls without broker filter.
* `GET /api/v1/auto-pilot/status` (BB auto_pilot.py:288-330) fabricates equity = capital_allocation and uPnL =
  equity − allocation; not proxied, no UI caller (dead) → P3.
* Authoritative exchange reconciliation: per-account CATI dashboard is venue-snapshot based with engine freshness
  (customer_dashboard.py:25, ENGINE_FRESH_SECONDS) and environment per account (:255-256) — correct and not summed.
  Aggregate portfolio page has no observed_at/staleness indicator (live read on each request; 30 s poll).
* Hard-coded totals: none in customer pages on main besides the above; admin SystemHealth is fully fake (§2.5).

#### 2.5 Admin portal

AuthN/Z: every `/api/admin/*` route in user-backend depends on `require_admin` (mechanically verified); admin
tokens are a separate type (`admin_access`), admins table, mandatory MFA (core/deps.py:70-90); admin-backend
mirrors it (admin-backend/app/core/auth.py:41-79). Nginx returns 404 for `/api/admin/` and `/api/v1/admin-auth/` on
the customer vhost (cosmicforge-app.conf:181-182). Emergency calls mint a 60 s `act=admin-emergency` service token
(UB admin_emergency.py:74-102) checked by BB `require_admin_emergency` (BB core/auth.py:174). **No role granularity**:
every admin can kill-switch, flatten, approve governance promotions, grant plans, decide KYC.

| Capability | Admin screen / API | Verdict |
|---|---|---|
| Customers | UserManagement → /api/admin/users, /users/{id} (admin.py:157-214), suspend/activate (:216-262) | PARTIAL — user row only (no broker accounts, bots, positions); suspend revokes sessions but does not stop the user's bots (CATI path never reads users.status) and audit row's user_id is the customer, not the acting admin (CUST-11) |
| Broker connections | none | MISSING |
| Bots | BotMonitor → /api/admin/bot/{overview,runs,live} (admin.py:843-990) | MISLEADING — reads legacy runner tables (runs, daily_state, symbol_state, decision_traces) which production never writes (BB main.py:1118 "legacy runner is not built") (CUST-09) |
| Account health / CATI / evaluations / refusals / positions / trades | CatiStatus → /api/admin/cati/{capabilities,multi-asset,datasets} only | MISSING — BB has /api/v1/admin/cati/bots/{id}/*, /admin/trading/evidence/*, /admin/trading/operations/*, /admin/trading/threshold/* (routes_bot-backend.txt:51-85) but user-backend proxies none of them (CUST-10) |
| Worker health / exchange connectivity / data freshness | SystemHealth page | FAKE — SystemHealth.tsx:25-36 sets hard-coded "healthy, CPU 45.2 %, mem 62.8 %…" every 5 s, no API (CUST-07). Operations overview (/api/admin/operations/overview) has alerts/on-call/backup drill |
| Certification evidence / deployment readiness | none; readiness approve/revoke only at BB /api/v1/admin/bots/{id}/readiness/* (readiness_admin.py:49-192), not proxied, no UI | MISSING (CUST-10) |
| Experimental DEMO authorization | ExperimentalDemo → /api/admin/governance/experimental-demo (+revoke) (admin_governance.py:246-258) | WORKING, audited (auth_audit_log via emergency._write_audit_row) |
| Market-entry policies (L-08) | ExperimentalDemo → /api/admin/order-spreading/market-entry-policies (admin_market_policy.py:56-75) | WORKING, audited |
| Risk approvals / family governance | FamilyGovernance → /api/admin/governance/* (18 routes) | WORKING, audited |
| Security incidents / status incidents | Operations → /api/admin/operations/status/incidents | WORKING |
| Emergency controls | BotMonitor EmergencyControls → /api/admin/emergency/{status,kill-switch,flatten} | WORKING, intent logged before call, durable audit row, unknown-outcome messages (admin_emergency.py:186-262) |
| Audit records | AuditLogs → /api/admin/audit-logs (+export) | WORKING (attribution gaps above) |
| Platform settings | PlatformSettings → GET/PUT /api/admin/settings (admin.py:1510-1560) | MISLEADING — maintenance_mode, signup_enabled, default_risk_cap, max_bots_per_user are written to system_settings but nothing reads them (grep: only retention.py lists the table) (CUST-07b) |
| KYC review | Compliance → /api/admin/compliance/* | WORKING, kyc_audit_log + auth_audit_log |
| Activity feed / Transactions | honest "no source connected" placeholders | OK (honest) |

### 3. Gap list

| ID | Component | File:lines | Problem | Root cause | Evidence (+level) | Customer impact | Trading impact | Sev | Classification | Existing implementation | Required correction | Dependency | Owner | Validation test | State | Effort | Phase |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| CUST-01 | Portfolio aggregate | UB core/portfolio_service.py:130-142, 249-263 | DEMO and LIVE equity/wallet/uPnL summed into one headline total on main | verified | source L5; branch test test_portfolio_summary_environments (not on main) | Customer sees demo money as real portfolio value | none (display) | P1 | LOGIC_DEFECT | branch 65372fe per-environment totals | merge 65372fe; also label Portfolio.tsx with environment | — | Engineering | test_portfolio_summary_environments | FIXED_ON_BRANCH_ONLY | S | B |
| CUST-02 | Dashboard P&L label | UB portfolio_service.py:253-256; FE AdvancedDashboard.tsx:95,200,202 | Unrealized open P&L shown as "Daily P&L"; negative values lose minus sign | verified | L5 | Misread performance | none | P2 | FRONTEND_GAP | branch relabels to "Open P&L (unrealized)" | merge branch; fix sign rendering | CUST-01 | Engineering | FE unit test on formatter | FIXED_ON_BRANCH_ONLY (label) / OPEN (sign) | S | B |
| CUST-03 | Analytics / reports P&L | BB analytics/pnl_service.py:55-121; api/equity.py:135-205; analytics_reporting.py | No DEMO/LIVE segregation; one quote currency via fetchone after GROUP BY; equity-latest hard-codes USDT; funding not included; fees only on CLOSE fills in realized report | verified (static) | L5 | Performance/tax figures mix demo and real, may be wrong | none | P2 | LOGIC_DEFECT | per-account filters exist as optional query params | filter/group by environment and currency; include funding & entry fees | — | Engineering | new tests with mixed-env fills | OPEN | M | B |
| CUST-04 | Edit / legacy deploy sizing | FE EditBot.tsx:42-66; BB bot_instances.py:326-362; BB auto_pilot.py:179-258; UB auto_pilot_proxy.py:113-128 | Fixed-amount / percent allocations can be set (EditBot, legacy body) with no preview; nothing tells the customer the configuration can never pass the 0.40 % ceiling (bot_a8117dc719fc) | verified | L5 + L4 test_bot_sizing_config (refusal) | Bot silently never trades; customer learns only from refusal after the fact | trades refused (safe) | P1 | FRONTEND_GAP | Step-1 preview (risk-based only); branch static hint text | run deployment_service.evaluate (or a fixed-amount variant computing margin×lev×stop vs 0.40 % equity) on PATCH and legacy deploy; block or warn | — | Engineering | test: PATCH fixed 120 @ equity 417.58 returns blocker | OPEN | M | B |
| CUST-05 | EditBot form semantics | FE EditBot.tsx:139-212 | allocation_type select lacks `risk_based` (all Step-1 bots are risk_based) → blank select, touching it converts bot to fixed; helper "~capital/allocation concurrent trades max" contradicts per-trade semantics and max_open_positions=1 | verified | L5 (production_execution.py:527, capital_ledger.py:1-22) | Accidental conversion of risk-based bot to fixed sizing; wrong mental model | conversion leads to refusals | P2 | FRONTEND_GAP | — | add risk_based option (or hide allocation for CATI risk bots), remove helper | — | Engineering | FE test asserting options/labels | OPEN | S | B |
| CUST-06 | Legacy deploy path | UB auto_pilot_proxy.py:113-198; BB auto_pilot.py:179-258; bot_instance_service.deploy_auto_pilot :773-873 | Legacy body still accepted for crypto: bypasses Step-1 DEMO-only check, preview, consent record, request_id idempotency and one-bot-per-account check | verified (static) | L5 | Customer/stale client can create bots without consent/preview | LIVE still blocked by readiness gate (bot_instance_service.py:119); DEMO bots may be duplicated per account | P2 | INTEGRATION_GAP | Step-1 contract | restrict legacy body to forex (or remove); require Step-1 for crypto | — | Engineering | test: legacy crypto body → 422 | OPEN | S | B |
| CUST-07 | Admin SystemHealth | FE admin pages/admin/SystemHealth.tsx:20-36 | Page displays hard-coded "healthy" CPU/mem/disk/errors refreshed every 5 s; no backend | verified | L5 | Operators shown fabricated health | could hide outage | P1 | FRONTEND_GAP | Operations page, BB /api/v1/admin/trading/operations/health (not proxied) | wire to real health (proxy BB operations/health, worker heartbeats) or replace with honest placeholder | CUST-10 | Engineering | FE test: no literal metrics | OPEN | S | A |
| CUST-07b | Admin PlatformSettings | UB api/admin.py:1510-1560; FE PlatformSettings.tsx:72-100 | maintenance_mode, signup_enabled, default_risk_cap, max_bots_per_user persisted but read by nothing | verified (grep) | L5 | Operator believes signups/maintenance/risk cap changed | none enforced | P1 | INTEGRATION_GAP | system_settings table | enforce in auth/register, plan_gate, risk; or remove controls | — | Engineering | test: signup_enabled=false → register 403 | OPEN | M | A |
| CUST-08 | Broker disconnect/delete | UB core/broker_service.py:925-963, 977-990 | Disconnect deletes only legacy `broker_credentials`; encrypted v2 credentials remain; "delete permanently" likewise, and DELETE broker_accounts with v2 FK rows (migrations.py:1613, no cascade) may fail on enforcing DBs; allowed with open positions | verified (static); FK failure unverified | L5 | Key material retained contrary to UI text; possible 500 on delete | engine refuses disconnected accounts (resolver status check) so positions become unmanaged | P2 | SECURITY_GAP | resolver refuses non-connected status | delete/supersede v2 rows; warn/block when open positions or lineage exist | — | Engineering | test: after disconnect no v2 active row | OPEN | S | B |
| CUST-09 | Admin BotMonitor | UB api/admin.py:843-990; FE BotMonitor.tsx | Reads legacy runner tables never written in production | verified | L5 (BB main.py:1118) | Admin sees empty/stale "bots" | blind operations | P1 | INTEGRATION_GAP | BB admin cati/evidence routes | rebuild on bot_instances + cati_production_decisions + positions | CUST-10 | Engineering | integration test with CATI decision rows | OPEN | M | B |
| CUST-10 | Admin ↔ engine visibility | UB has no proxy for BB /api/v1/admin/{cati/bots,trading/evidence,trading/operations,trading/threshold,bots/*/readiness} (routes_bot-backend.txt:51-85) | Admins cannot view per-customer CATI status, evaluations, refusals, positions, trades, readiness or approve readiness from the console | verified | L5 | Support cannot answer "why didn't my bot trade" | readiness approval impossible without direct engine access | P1 | MISSING_FEATURE | engine endpoints exist and are require_admin | add audited admin proxies + screens | — | Engineering | proxy tests like test_admin_governance_proxy | OPEN | M | B |
| CUST-11 | Admin user suspend | UB api/admin.py:216-262 | Suspend revokes sessions but bots keep trading (CATI never checks users.status); audit rows store the customer id, not the acting admin | verified | L5 | Suspended account keeps trading | continued exposure after suspension | P2 (P1 for D) | RISK_GAP | session revocation | pause user's bots on suspend; record admin id in audit | — | Engineering | test: suspend → bots paused, audit actor=admin | OPEN | S | B |
| CUST-12 | Customer exposure reduction | FE (no close/flatten action); MyBots.tsx:370 | Customer cannot close a position from the platform; must use exchange. Stop/pause dialogs say "no longer manages them" while engine maintains protection (production_execution.py:697-703) | verified | L5 | Must leave platform to exit; message inaccurate (conservative) | none new | P2 | MISSING_FEATURE | admin flatten exists | add customer close-position (reduce-only) action; correct dialog text | — | Engineering/Owner | e2e DEMO close test | OPEN | M | C |
| CUST-13 | Admin RBAC granularity | UB core/deps.py:92-96 | require_admin is a single role: any admin can kill-switch/flatten/approve promotions/grant plans | verified | L5 | — | concentration of authority | P2 | SECURITY_GAP | MFA + audit | role/permission scopes (operator vs owner approvals, 4-eyes for flatten/promotion) | Owner policy | Owner/Engineering | permission matrix tests | OPEN | M | D |
| CUST-14 | Dead client contracts | user client.ts:1490-1841; admin client.ts:561-979; UB bot_instances_proxy.py:17; admin api/admin.ts:905 | 19+ client functions call non-existent routes (no UI caller); UB POST /bot-instances proxies to missing engine route; ML runtime-status 404 in default mode | verified (mechanical) | L5 | none today (one ML panel error) | none | P3 | TESTING_GAP | apiRoutes.test.ts guards a few | delete dead functions; add generated contract test (route dump vs client) to CI | — | Engineering | contract script in CI | OPEN | S | A |
| CUST-15 | Portfolio UI error/staleness | FE Portfolio.tsx, AdvancedDashboard.tsx | `account_errors` never displayed; no observed_at / stale indicator on aggregate | verified | L5 | Failed accounts silently missing from totals | none | P2 | FRONTEND_GAP | backend returns account_errors | render degraded accounts and read time | CUST-01 | Engineering | FE test | OPEN | S | B |
| CUST-16 | Hard-risk vs 0.40 % ceiling | BB trading_orchestrator.py:765-770; scale/spread_gate.py:604-621 | For fixed/percent bots the 0.40 % per-trade ceiling is enforced only inside the governed MARKET-entry gate; hard risk approves 6.13 % equity risk (test) | verified | L4 test_bot_sizing_config | — | any future non-MARKET entry path or non-governed policy would lose the ceiling | P2 | RISK_GAP | executable-price check | enforce ceiling in hard risk for every allocation mode (refuse, not resize) | — | Engineering | test: fixed 120 refused by hard risk | OPEN | S | C |
| CUST-17 | Frontend test depth | FE src/tests/*.test.ts (node:test source checks) | No component/DOM or e2e tests for journey (deploy, edit, stop, disconnect) | verified | L5 | regressions undetected | — | P2 | TESTING_GAP | 102+23 source tests | add RTL/vitest + Playwright journey on DEMO | — | Engineering | CI job | OPEN | M | B |

### 4. Verdicts

* **Customer Frontend: PARTIAL.** All live UI calls resolve to real, authenticated, owner-scoped backend routes
  (mechanical contract check; 102 FE tests, tsc clean); Step-1 deploy/preview, lifecycle and CATI panels are wired
  with honest error states. Gaps: DEMO/LIVE summed and open P&L labelled daily on main (branch-only fix), EditBot
  semantics/no preview (CUST-04/05), no customer close action, no DOM/e2e tests.
* **Admin Frontend: PARTIAL (not operations-ready).** AuthZ/MFA/audit are solid and emergency, governance,
  experimental-DEMO and market-entry policy management are real and audited. But SystemHealth is fabricated,
  PlatformSettings controls are inert, BotMonitor reads tables production never writes, and there is no view of
  customer broker connections, CATI evaluations/refusals/positions or readiness approval (CUST-07/07b/09/10).
* **Portfolio accounting: BLOCKED on main / PARTIAL on branch.** Main sums DEMO+LIVE and labels unrealized P&L as
  daily (CUST-01/02); branch 65372fe fixes the aggregate, but analytics/reports still mix environments and
  currencies (CUST-03) and the UI hides account errors (CUST-15). Per-account CATI dashboard is correct.
* **Release level A (platform available): PARTIAL → READY after CUST-07/07b are fixed or the pages are made honest.**
  Register/verify/login/MFA/sessions/logout/KYC/legal/support work and are tested (L4).
* **Release level B (broker-connected DEMO): PARTIAL.** Connect/validate/balances/deploy/pause/resume/stop work for
  DEMO with server-derived environment and Step-1 gates; requires merging 65372fe (CUST-01), CUST-04/05/06 (no
  incompatible-sizing bots, legacy path), CUST-09/10 (admin visibility to support customers) and CUST-08.
  Note: per spread_gate.py:51-53 every production MARKET entry is refused unless an L-08 record is approved, so a
  DEMO bot will evaluate but not trade without that operator action (consistent with bot_a8117dc719fc's history).

