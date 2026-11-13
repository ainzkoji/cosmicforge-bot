# Multi-Tenant, Security, Scalability, VPS Infrastructure, LIVE Gates & Blocker Reconciliation (Sections 16, 22-25, 29)

**Audit:** CosmicForge Tradingbot master end-to-end audit (read-only) · **Date:** 2026-11-13
**Baseline:** `origin/main` = `649080b` · engineering branch `origin/ccr-b5c520cf-80g8x2` = `732e0e2` (7 commits not on main, inspected by diff/worktree only)
**Master report:** [TRADINGBOT_MASTER_END_TO_END_AUDIT.md](TRADINGBOT_MASTER_END_TO_END_AUDIT.md) · **Gap register:** [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv)

Evidence levels used throughout: L1 observed authenticated behaviour · L2 real venue evidence · L3 integration test with real boundaries ·
L4 contract/unit test with mocks · L5 static source inspection · L6 documentation/historical claim · L7 inference.
No production database, VPS or authenticated venue session was available to this audit; nothing below is L1.
Finding IDs are stable and map to the gap register (column "Master ID" consolidates duplicates across areas).

## Lead auditor reconciliation
- OPS-04 (historical administrator credentials) is corroborated by the repository's own record `docs/implementation/MASTER_PLAN_I_L_COMPLIANCE.md` §5.3 (commits db4580b, cc13da0, b35c198; rotation date/operator never recorded). Values were not inspected or reproduced.
- No P0 security defect was found. No VPS access was available: every host-level item is NOT VERIFIED.
- This file holds the full blocker-register reconciliation (Section 29).

---
## OPS audit: multi-tenant, security, performance, VPS/infrastructure, LIVE release gates, blocker reconciliation

Auditor area OPS (audit sections 16, 22, 23, 24, 25, 29). Repo `origin/main` = 649080b; branch `origin/ccr-b5c520cf-80g8x2` = 732e0e2 (inspected with `git show`, not checked out).

### 1. Scope and method

- Static read of the production entry points: `deploy/systemd/*` (16 units), `backends/{bot,user,admin}-backend/app/main.py`, auth deps, broker credential crypto, go-live gate, order-submission gate, live-build keys, rate budget, broker cycle (`production_runtime.py`, `production_execution.py`), deployment service, nginx, env templates, profiles, staging scripts, docs listed in the brief.
- An AST route scan of all three backends for handlers without an auth dependency (script at `scratchpad/ops/routescan.py`). Every hit was then checked by hand. Path-parameter routes in the customer APIs were sampled for ownership scoping.
- Secret scan of tracked files (`git grep` with key/token regexes) and of history for added `.env`/`.db`/key files. The clone is shallow (240 commits), so the history scan is incomplete.
- Targeted tests run with the prepared venv:
  - bot-backend auth/security/deploy-contract: **129 passed**;
  - deploy/VPS static suites (`tests/deploy`, `test_vps_*`): **229 passed**;
  - user-backend H8.4 security, notifications auth, KYC hardening: all passed;
  - `test_phase2_broker_security.py`: 3 of 10 **fail in isolation** without `BROKER_SECRET_KEY` and pass (10/10) with an ephemeral key. See OPS-14.
- `scripts/validate_config_profile.py` was run against every example profile and against `deploy/env.production.example`.
- No VPS, no exchange and no credentials were used. Every host-level property is **NOT VERIFIED**.

Evidence levels: L1 authenticated behavior, L2 real venue, L3 integration with real boundaries, L4 unit/contract with mocks, L5 static source, L6 doc claim, L7 inference.

### 2. Detailed analysis

#### 2.1 Multi-user / multi-account / multi-bot

**Customer isolation (IDOR). Good in what was sampled (L5, plus L4 tests).**
- The route scan found no unintentionally public handler.
  - bot-backend: every legacy money route (`/trade/market`, `/binance/cancel-all`, `/risk/kill`, `/runner/live/start`, `/binance/leverage`, ...) carries `dependencies=_LEGACY_ADMIN_ONLY` (`bot-backend/app/main.py:158`, routes 1186-2879).
  - The only public bot routes are `/`, `/cati`, `/health` and `public_track_record`.
  - user-backend public routes are auth flows, legal/public content, the Stripe and Telegram webhooks (signature-checked), MT connector claim/pair, and the SSE stream. SSE validates the cookie/bearer itself (`user-backend/app/api/events_proxy.py:34-70`).
- Sampled owner-scoped routes all bind `user_id` from the token and check ownership:
  - `bot_instances.py:46-60` (`instance.user_id != user["id"]` → 403);
  - `cati_account.py` (`_owned_bot`); `backtesting.py:196` (`WHERE id=? AND user_id=?`); `equity.py:499` (broker owner check); `shadow_routes.py:184` (bot owner);
  - `broker_transfers.py` (service is called with `user_id`);
  - user-backend `brokers.py` (`get_broker_account(user_id, account_id)` everywhere; the SQL joins `a.user_id=?`);
  - `support.py:85-97`, `signals.py:626`, `auth.py:896` (`WHERE id=? AND user_id=?`), `kyc.py:945` (signature bound to user + file ref).
- Admin-only analytics (`trading_evidence`, `threshold_diagnostics`, `trading_operations`, `monitoring`) use `require_admin`.
- `tests/test_api_auth_hardening.py::test_every_route_requires_auth_unless_explicitly_public` passed (L4).
- There is no automated cross-tenant (user A → user B object) test over ALL path-param routes. Only per-feature tests exist (OPS-15).

**Credential isolation (L5).**
- Broker credentials are resolved by `(account_id, user_id)`: `resolve_broker_auth(account["id"], account["user_id"], db)` in `production_runtime.py`, and `get_decrypted_credentials(user_id, account_id)` in user-backend.
- `AccountExecutionAuthority.authorize_entry` rejects `EXECUTION_ACCOUNT_SCOPE_MISMATCH` and `BROKER_ACCOUNT_OWNERSHIP_MISMATCH` (`governance/account_authority.py:13-21`).
- Reservations check that the bot owner equals the account owner (`portfolio/reservation_store.py:436-440`).

**Multiple accounts and brokers per user.**
- Supported: `broker_accounts` per user, with catalog brokers Binance/Bybit/BingX/IBKR/MT.
- Execution is only Binance/Bybit/BingX (`production_execution.py`, `supported = broker_id in {binance,bybit,bingx}`).
- Only Binance DEMO has venue evidence (`evidence/binance-demo-certification-20261101`, L2 per the EXEC/ARCH auditors).

**ACCOUNT_EXECUTION_OWNER_AMBIGUOUS: an intentional, fail-closed limitation, not an incomplete feature.**
- The engine allows exactly one active bot per broker account:
  - `account_authority.py:24-27` (`len(bots)!=1` → refuse);
  - `production_execution.py:1070-1071`;
  - `shared_lib/broker/auto_trading.py:22`.
- The deploy transaction enforces one occupying (active/paused) bot (`core/deployment_service.py:305,388,396`, `ACCOUNT_ALREADY_HAS_BOT`, BEGIN IMMEDIATE).
- A unique index was deliberately dropped (`shared_lib/persistence/migrations.py:899-913`). Tests:
  - `test_deployment_contract_step1.py::test_the_one_bot_per_account_invariant_is_the_deploy_transaction_not_an_index`;
  - `test_bot_sizing_config.py:366`;
  - `test_perpetual_operational_contract.py:83`.
- Position maintenance (protection repair, recovery) runs BEFORE and independently of the bot count (`production_execution.py:1019-1047`). Ambiguity blocks entries only, never protection.
- Residual: `BotInstanceService.start_bot_instance` (`core/bot_instance_service.py:593-625`) and `create_bot_instance` do not re-check the one-occupying-bot invariant. Example: a STOPPED bot restarted after a new bot was deployed to the same account gives two active bots, and every entry on that account is then refused (fail-closed, confusing UX) (OPS-11).
- Multi-bot per account (sub-account allocation) is therefore NOT a product feature.

**Shared risk budgets.**
- Risk is account-scoped. The strictest bot daily loss applies per account (`TRADINGBOT_DEMO_ACTIVATION_REPORT.md:154`, L6).
- No per-USER aggregate risk across several accounts was found: grep over `trading_intelligence` for user-level budgets is empty (L5/L7). This is an owner policy decision (OPS-12).

**Account execution ownership.**
- One canonical runtime-owner lease per runtime database.
- Per-account durable assignments in the platform DB: `platform_workers/broker_assignment.py`, `BROKER_CYCLE_ASSIGNMENT_MODE=owner` (default) or `assigned`.
- The account fence is checked at the authority, the boundary and the transport (`account_fence.require_transport` inside `require_broker_mutation_permission`, `shared_lib/core/production.py:165-173`).
- Cross-host protection depends on procedure:
  - the lease lives in the SQLite file;
  - the VPS guide itself warns that a second machine with a copy can trade the same account (`docs/VPS_TRADING_DEPLOYMENT.md:31-33`) (OPS-13).
- `assigned` multi-worker mode has not been drilled on a real host (EXT-WRK-01).

**Fair scheduling, queue isolation, error isolation.**
- The broker cycle is ONE sequential loop over accounts every 30 s:
  - `production_runtime.py` `_broker_loop`, `await asyncio.sleep(max(1., 30. - elapsed))`;
  - `broker_cycle`, `for account in accounts`.
- There is no per-tenant fairness or time-slicing. A slow account delays all others in owner mode.
- Errors are isolated per account: a broker read failure is logged and the account marked `BLOCKED_ACCOUNT` (`production_runtime.py:490-499`). A maintenance failure refuses entries for that account only (`production_execution.py:1037-1047`).
- The durable job queue (`platform_workers/jobs.py`) is scheduled only for the accounts this worker owns (`production_runtime.py:866-880`).

**Rate budgets.**
- A DB-backed sliding-log budget per venue and egress IP has priority ceilings: EMERGENCY 1.0, PROTECTION 0.95, RECONCILIATION 0.80, ENTRY 0.65, MARKET_DATA 0.50, with utilisation 0.8 (`shared_lib/broker/rate_budget.py:38-49`). The Binance limit is 2400 weight/min (`rate_budget.py:92`, VERIFIED_BY_DOC).
- Bybit (150/5 s) and BingX (20/10 s) are UNVERIFIED placeholders (`docs/exchanges/EXCHANGE_OPERATIONS.md:285-287`).
- There is no per-tenant share inside an IP budget: one tenant's accounts can consume the RECONCILIATION class for everyone (L7, OPS-10).

**What the 1,000-account load test proves.**
- Script: `scripts/step5_load_test.py`; evidence `evidence/step5/load_test.json` and branch `evidence/linux-drills-20261112/load_test.json` (0da95d6).
- It proves the platform coordination layer scales: assignments, fenced maintenance, queue, order spreading and takeover on SQLite and PostgreSQL, with in-memory `FakeVenue`s and 4 threads.
- At 1,000 accounts on PostgreSQL: maintenance cycle 1.64 s, per-account stage p95 0.07 ms, queue 362 jobs/s, 0 duplicates, pool peak 16 connections, RSS 114 MB (L4, synthetic).
- It does NOT prove venue throughput. The per-account stage of 0.05 ms contains no network; a real venue read is roughly 50–300 ms per request.
- The repo's own account-scale evidence shows the binding constraint: `evidence/predeploy/scale_1000_accounts.json` → `reconciliation_and_request_budget.by_venue.binance`.
  - On one egress IP, only **30 of 334 Binance accounts reconcile per 60 s window** (1500 of a 1536 reconciliation ceiling, about 50 weight each).
  - The other 304 accounts were refused `BUDGET_EXHAUSTED_FOR_PRIORITY`.
  - One full round takes **12 minutes** (`derived_full_round_ms_one_egress_address: 720000`).
- EXT-LOAD-01 (exchange-demo load) is open.

#### 2.2 Security

**AuthN.**
- PyJWT HS256 with one shared `SECRET_KEY` across the three services (`user-backend/app/core/security.py:126-148`).
- Claims: iss/aud/exp/nbf/jti, `tv` token_version and `sid` session.
- Access tokens last 15 min and refresh tokens 14 days (`config.py:233-234`).
- Every validator re-reads the account state:
  - user-backend `core/deps.py:require_current_user`;
  - bot-backend `core/auth.py:authenticate_access_token`.
- Admin tokens use a separate issuer/audience (`admin-portal`).
- Admin MFA (TOTP with replay protection, `security.py:match_totp_counter`) is forced on in production. An explicit false is ignored (`session_cookies.py:69-78`) and an unenrolled admin is refused (`deps.py get_current_admin`).
- Customer MFA is optional.
- Production refuses default, placeholder or short secrets at import (`config.py:445-455,592`).
- Login rate limits are stored in the DB per account and per IP (`api/auth.py:161-216`), and nginx limits `auth` to 30 r/m (`deploy/nginx/cosmicforge-app.conf:63-67,155-157`).

**Sessions and CSRF.**
- httpOnly SameSite=Strict cookies with a double-submit CSRF token (`session_cookies.install_session_cookie_middleware`, `main.py:137-140`).
- `AUTH_BEARER_COMPAT=true` in `deploy/env.production.example:114` keeps bearer headers, URL `?token=` on SSE and tokens in login bodies (EXT-F8-01 open, OPS-08).
- No frontend stores tokens in localStorage (grep empty, L5).

**AuthZ and admin model.**
- user-backend admin routes require MFA'd `admins`-table tokens.
- bot-backend `require_admin` accepts any user-audience access token with `role=admin` (`bot-backend/app/core/auth.py:141-157`). That role comes from `users.role`, a different identity store than `admins`, with no MFA. No API sets `users.role='admin'` (`admin.py:1396` always inserts `'user'`), and port 9000 is loopback-only, so the risk is low (OPS-16).
- Emergency routes need a dedicated 60–120 s service token minted after `admins` verification (`require_admin_emergency`, `auth.py:174-202`).
- Two-operator rule: `SELF_APPROVAL_REFUSED` (`app/golive/authorization.py:44,80`).
- Any admin can create further admin identities (`admin.py:1380` uses `require_admin`, not superadmin). A single person controlling two admin accounts defeats the two-operator rule. This is inherent and procedural (P3).

**Broker credential encryption.**
- Fernet (AES-128-CBC + HMAC-SHA256).
- The key is `BROKER_SECRET_KEY` only. Production fails closed when it is missing, a placeholder or shorter than 32 characters (`shared_lib/core/security/broker_security.py:96-125`).
- Legacy keys (`sha256(SECRET_KEY)` and a constant zero key) are decrypt-only, and in production only with `BROKER_LEGACY_KEY_DECRYPT=1`.
- `backends/scripts/rotate_broker_credential_encryption.py` → `shared_lib/broker/credential_migration.py`:
  - migrates legacy → primary only, transactionally, with verification and an audit row;
  - is SQLite-only (`sqlite3.connect`);
  - **cannot rotate the primary key to a new one**: there is no previous-key or MultiFernet support anywhere (grep empty). The same holds for `CREDENTIAL_KEY` (v1→v2 HKDF migration only, `user-backend/app/core/security.py:345-395`) and `KYC_ENCRYPTION_KEY`.
  - A suspected key compromise therefore forces every customer to reconnect (OPS-07).

**Secrets handling.**
- No live secret found in tracked files. The hits are test fixtures such as `AKIAIOSFODNN7EXAMPLE` and fake bot tokens.
- Templates use `CHANGE_ME`/empty values. No `.env` is tracked.
- History contains `data/bot.db` (db4580b, b35c198). It was checked (counts only): `broker_credentials` 0 rows, `users` 0, `admins` 0.
- Administrator e-mail/password literals were committed in db4580b, cc13da0 and b35c198. This is documented in `MASTER_PLAN_I_L_COMPLIANCE.md:187-205` and EXT-SEC-02.
- Rotation evidence is **absent**: the "record date and operator" line is empty. The workstation DB to be migrated to the VPS is the one at risk (OPS-04).

**Input validation, SQLi, SSRF, XSS.**
- Every f-string SQL in the API layers interpolates only internally built WHERE fragments or placeholders. Values are bound (e.g. `analytics_reporting.py:128-163`, `admin.py:1094`). No SQLi was found (L5).
- SSRF is guarded for caller-supplied gateway URLs: `shared_lib/core/security/url_guard`, with the allow-list for admins only (`bot-backend/app/api/brokers.py:22-60`).
- CSP, HSTS, X-Frame-Options and nosniff are set in nginx (`cosmicforge-app.conf:92-114`). Body size is capped at 2 MB (11 MB for KYC).

**Broker API scopes.**
- Withdrawal-capable keys are rejected (`REJECTED_WITHDRAW_PERMISSION`, `shared_lib/broker/permissions.py:95-117`). Transfers require positive evidence that withdraw is denied.
- Test: `test_withdraw_capable_binance_key_is_restricted_not_connected` (L4, passes with a key).

**Audit logging.**
- `auth_audit_log`, `broker_audit_log`, go-live ledger (hash chain + revocation anchor), emergency steps (L5).

**Environment separation.**
- The broker account `environment` drives the base URL (`require_execution_account`, `production.py:145-151`).
- The production profile forces mainnet URLs and refuses paper/legacy flags (`production.py:56-90`).

**Pen test.**
- NOT RUN (EXT-SEC-01; `SECURITY_HARDENING_REPORT.md` §1 row 20, §4; the branch certification §22).
- Internal claims match the code where checked (MFA, cookies/CSRF, token_version, secret refusal, broker key fail-closed).

**Admin host exposure.**
- The admin vhost IP allow-list is commented out by default (`cosmicforge-app.conf:226-228`). The admin console is internet-reachable unless the operator enables it (OPS-09).

#### 2.3 Performance and capacity

| Resource | Proven | Not proven |
|---|---|---|
| Coordination layer (DB, assignments, queue) | 1,000 accounts, 0 duplicates, 1.6 s cycle, 16 PG connections, 114 MB (L4 synthetic, Linux 4-core, shared with a test run) | Real network latency, real venue responses |
| Exchange per-IP limit | Budget logic + 1,000-account math: 30 Binance accounts/min reconcile on one IP (L4) | Real demo-account load (EXT-LOAD-01) |
| Market data | One shared signal per cycle per family (`scale.fanout`, asserted) | Public-data weight under real cadence |
| DB | SQLite WAL shared by 3 processes (trading, user, admin) with a 10 s busy timeout and 503 on busy (VPS doc §10.4) | Contention under user load + trading writes (L7) |
| CPU/RAM | VPS doc sizing 2 vCPU / 4 GB; the DB is about 8.4 GB | Real runtime RSS with numpy/CATI loaded (NOT VERIFIED) |

**Practical capacity estimate for an initial single VPS with one egress IP (L7).** Assumptions:
- Binance USD-M only;
- 2400 weight/min × 0.8 utilisation × 0.80 reconciliation ceiling ≈ 1536 weight/min for account work;
- a FULL account evaluation every 30 s costs about 50–100 weight. That range is the repo's measured 50 weight/reconcile, up to positionRisk 10 + account 10 + openOrders 40 + openAlgoOrders 40;
- idle accounts (no active bot, no position) are skipped (`production_execution.py:1012-1015`).

Estimate:
- **about 8–15 actively-traded Binance accounts per IP** at the full 30 s cadence;
- up to about 30 if the cadence degrades to 60 s;
- beyond that, snapshots go stale (`BROKER_SNAPSHOT_STALE` after 120 s, `production_runtime.py:517-519`) and entries are refused. This is fail-closed, not unsafe.
- Bybit/BingX limits are unverified placeholders. BingX's 20 req/10 s supports only about 1–3 accounts.
- CPU, RAM and SQLite are not the bottleneck at this scale.
- The "1,000 accounts" figure must not be presented as customer capacity. Multiple egress IPs plus `assigned` multi-worker mode on PostgreSQL would be needed, and none of that is host-tested.

#### 2.4 VPS / infrastructure package

**Inventory.**
- 16 systemd units/timers, 2 nginx sites, 3 Dockerfiles, staging compose/deploy/rollback/restore scripts, 6 profiles, env templates.
- Every unit's ExecStart target exists:
  - `scripts/vps_health_check.py`, `notify_failure.py`, `offsite_backup.py`, `backup_trading_db.py`, `backup_platform_db.py`;
  - modules `app.ops.alerting`, `app.main` for the three backends;
  - docs `docs/operations/ALERTING.md` and `BACKUP_AND_RESTORE.md`.
- Units run by system python use the stdlib only (checked imports).
- Static deploy tests: 229 passed (L4).

**Hardening.**
- Non-root user; NoNewPrivileges, ProtectSystem, PrivateTmp; loopback binds; one worker.
- `Type=notify` with READY only after the lease is held; WatchdogSec=180; Restart=always; alerts through `ExecStopPost` (`cosmicforge-trading.service`).
- UFW deny-incoming; chrony; TLS 1.2/1.3 via Let's Encrypt; HSTS.

**Defects and gaps.**
1. main: no `PLATFORM_DATABASE_URL` / `GOLIVE_REVOCATION_ANCHOR` in `deploy/env.production.example` or the VPS guide.
   - Every production entry, DEMO included, is refused (`scale/spread_gate.py:96`, `PLATFORM_ORDER_BUDGET_UNAVAILABLE`).
   - `cosmicforge-ops-tick` would exit 1 every minute (it needs the platform DB).
   - **FIXED_ON_BRANCH_ONLY** (a746a58, rehearsed on PG 16 per the commit message, L6) (OPS-01).
2. The VPS runbook never installs or enables `cosmicforge-ops-tick.timer`, `cosmicforge-ops-deadman.timer` or `cosmicforge-platform-db-backup.timer`.
   - grep: 0 mentions of ops-tick/deadman on main and branch. The branch mentions platform-db-backup only in an upgrade step and a dangling "section 10" reference.
   - So the alert evaluators (BACKUP_FAILURE, worker heartbeat, escalation) and the platform `pg_dump` are not part of the documented deployment (OPS-02).
   - Platform dumps are also not off-site (`BACKUP_AND_RESTORE.md:12`).
3. `deploy/profiles/*` are not consumed by any runtime or deployment step (grep `CONFIG_PROFILE`/`validate_profile` outside the validator: none).
   - The VPS path uses `env.production.example`, which the validator refuses (`PROFILE_NAME_MISSING`).
   - Validator output: demo-certification, development, shadow and test are admissible; live-beta and scaled-live are refused `PROFILE_REQUIRES_LIVE_CAPABLE_BUILD:LIVE_NOT_PERMITTED_IN_THIS_BUILD` (as designed) (OPS-18).
4. `deploy/env.production.example` (target: the bot-backend `.env`) carries user-backend keys that the bot-backend never reads: `AUTH_COOKIE_*`, `KYC_ENCRYPTION_KEY`, `KYC_URL_SECRET`, `TELEGRAM_WEBHOOK_SECRET`. The VPS guide §10.4 tells the operator to set `ENGINE_URL`, which nothing consumes; the code reads `BOT_BACKEND_URL` (`user-backend/app/api/proxy_utils.py:16`) and defaults correctly (OPS-19).
5. `X-ENGINE-KEY` and `SERVICE_AUTH_TOKEN` protect nothing (documented, `VPS_TRADING_DEPLOYMENT.md:209-214`). Inter-service trust is loopback + the shared JWT key (OPS-16).
6. Host checks (systemd on a real host, nginx/TLS issuance, UFW, journald, backup timers, off-site restore, alert delivery) are **NOT VERIFIED**. There is no host evidence in `evidence/` (EXT-STG-01).

**Main vs a746a58.**
- Only `deploy/env.production.example` (+16 lines) and `docs/VPS_TRADING_DEPLOYMENT.md` (+57) change. The branch adds §2.4: PG role, pgpass, migrate, status, and migrate on upgrade.
- The anchor is placed on the same disk ("separate storage when you have it"). That weakens the separation the gate intends (P3).

#### 2.5 LIVE release gates

How LIVE is disabled. There are several independent layers (all L5; the tests listed are L4):

| # | Layer | Mechanism |
|---|---|---|
| 1 | Env flag | `LIVE_ORDER_SUBMISSION_ENABLED` defaults to False (`shared_lib/core/production.py:49`); `order_submission_gate` (`:136-142`) |
| 2 | Startup refusal | `production.py:81-88`: flag true + non-capable build → `PRODUCTION_PROFILE_CONFLICT` |
| 3 | Two-key build | (a) `BUILD_PERMITS_LIVE=False` (`production.py:19`); (b) `APPROVED_LIVE_RELEASES = frozenset()` (`live_release_allowlist.py`); (c) a manifest baked only by `deploy/containers/live-release.Dockerfile`, which no CI/compose/unit uses. Nothing exists: no manifest, empty allowlist → **no live-release artifact**. |
| 4 | Transport | In production, `require_broker_mutation_permission` refuses LIVE mutations while the flag is off (`production.py:174-182`) |
| 5 | Go-live gate | 17 conditions (`app/golive/gate.py:1-41`), consulted for every LIVE entry at the boundary (`execution/boundary.py:278-281,889-906`); errors refuse |
| 6 | Governance | `GovernanceAuthority.authorize_entry`: phase below M6 refused, `M6_IS_DEMO_ONLY` (`governance/promotion.py:212-229`); families capped at M6 (GATE-FAMILY-MAX-M6) |
| 7 | Venue | LIVE certification refused in this build (`platform_adapter/certification.py:214,291`, `certification_harness.py:181`) → **no Binance LIVE certification possible** |

**Gate status.**

| Gate | Implemented | Wired | Evidence | Missing type |
|---|---|---|---|---|
| Strategy certification | yes (family registry, research register) | yes (family gate at boundary) | no family certified (RESIDUAL FAILED; DAILY_TREND PENDING_HOLDOUT; all M0 — lead correction, see STRATEGY_RESEARCH_AUDIT) | certification evidence / research |
| Governance stage | yes | yes | phase M6 max, demo only | policy decision |
| Venue certification | yes (harness + register) | yes (gate cond. 1) | Binance DEMO only (2026-11-01, L2); LIVE refused in build | certification evidence / external |
| Live-capable build | yes (two keys + overlay Dockerfile) | yes (gate cond. 15, startup) | allowlist empty, no manifest | policy decision (owner + 2 reviewers) |
| Release manifest/allowlist | yes (`scripts/release/live_release_manifest.py`) | yes | none | policy decision |
| Market-entry policy (L-08) | yes | yes (spread gate) | DEMO policy expired 2026-11-08 (a746a58 msg); none for LIVE | policy decision |
| Liquidity calibration | yes (observations table) | yes (`calibrated=0` refuses multi-account) | none | certification evidence (EXT-LIQ-01) |
| User risk consent | yes (cond. 5) | yes | `LIVE_CONSENT_VERSION` not set → BLOCKED_PENDING_POLICY | policy decision |
| Account ownership | yes (cond. 7 + authority) | yes | L4 | — |
| Per-account approval | yes (cond. 11, self-approval refused) | yes | `APPROVAL_MAX_VALIDITY_DAYS` unset | policy decision |
| Two-operator release | yes (switch cond. 10) | yes | none recorded | operational |
| Revocation ledger | yes (hash chain + anchor, cond. 16) | yes | anchor unset on main | operational (fixed in branch template) |
| Kill switch | yes (platform/account/family) | yes | simulated drills; not drilled in an authorized env (J-06) | operational |
| Protective recovery | yes (maintenance first; switch-off strands protection on main) | yes | FIXED_ON_BRANCH_ONLY (334f2ff) | implementation (branch) |
| Monitoring | yes | partial (ops tick not in runbook; channels unset; no on-call) | test env only | operational |
| Live beta enrollment | yes (cond. 17) | yes | `STAGE_A_MAX_NOTIONAL` etc. unset; 8 weeks Stage A required | policy + elapsed time |
| Legal/compliance | documents DRAFT_PENDING_COUNSEL; country eligibility UNDETERMINED | **not enforced in code** (register finding 3.7) | none | external (counsel) + missing integration |

**Claims verified.**

| Claim | Verdict | Evidence |
|---|---|---|
| No live-certified strategy | TRUE | L5 + other auditors |
| No Binance LIVE certification | TRUE: refused by code in this build | — |
| LIVE submission disabled | TRUE | env flag + startup refusal + build keys + transport + go-live gate |
| No live-release artifact | TRUE | empty allowlist; no manifest in tree or `evidence/` |
| No production hosting evidence | TRUE | no host logs; EXT-STG-01 open; the branch cert §27-30 says "Not claimed: Linux-host systemd certification" |

#### 2.6 Blocker register reconciliation

Sources:
- `ACTIVATION_GATE_REGISTER.md` §3 (findings 3.1–3.9), `activation_gates.json` (68 intentional gates);
- `STEP5_STEP8_EXTERNAL_GATES.md` (EXT-*, J-01..J-09), `predeploy_readiness_matrix.json`;
- `TRADINGBOT_FINAL_ENGINEERING_CLOSEOUT.md` §21-22, `TRADINGBOT_PRE_DEPLOYMENT_READINESS.md`;
- branch `TRADINGBOT_FINAL_SYSTEM_CERTIFICATION.md` §33-36.

The 68 `activation_gates.json` entries are deliberate gates (types B–E; A=0), not defects. The table covers recorded blockers and findings.

| Blocker | Still exists (main) | Implementation / owning service | Root cause remains? | Fix on branch? | Evidence | Missing test | Impact | Remediation | Owner |
|---|---|---|---|---|---|---|---|---|---|
| Reg 3.1 APPROVAL_MAX_VALIDITY_DAYS read by nothing | No (fixed) | `app/golive/gate.py _operator_approval_audited` | No; value unset → BLOCKED_PENDING_POLICY | n/a | `test_an_approval_is_never_open_ended` (L4, per register) | — | LIVE only | owner sets value | Owner |
| Reg 3.2 order switch off strands protection | **Yes on main** | `shared_lib/core/production.py:174-182` refuses all mutations incl. reduce-only/flatten (DEMO and LIVE) | Yes on main | **Yes, 334f2ff** (`risk_reduction_gate`); LIVE stays incapable | branch tests `test_risk_reduction_gate.py`, `test_the_order_switch_off_stops_entries_but_never_strands_an_open_position` (L4, not run by me) | merge + full suite on merged tree | DEMO position unprotected if operator turns switch off (P1 for phase C) | merge 334f2ff | Engineering |
| Reg 3.3 env template lacks PLATFORM_DATABASE_URL / anchor | **Yes on main** | `deploy/env.production.example`, VPS guide | Yes on main | **Yes, a746a58** | grep: absent on main | runbook-level test that the template contains every key the gates need | No order possible on a VPS built from main | merge a746a58 | Engineering/Operations |
| Reg 3.4 flags that look like locks but enforce nothing (`TRADING_ENABLED`, `REAL_TIME_NEWS_SHADOW_MODE`) | Yes | config/production profile | Yes (by design) | No | register; L6 | `TRADING_ENABLED=false` refusal test (3.9) | operator misunderstanding | document / remove | Engineering |
| Reg 3.5 DEMO skips global phase | Yes (by design) | `account_authority.py:46-50` | n/a | No | L5 | — | DEMO held by family gate | owner accept | Owner |
| Reg 3.6 tradfi guard display-only | Yes | `markets/guard.py` | Yes | No | L6 | — | none while no tradfi adapter | keep blocked | Research/Owner |
| Reg 3.7 counsel / country eligibility not enforced | **Yes** | legal service (user-backend), no trading gate | Yes | No | L6 + no gate found | eligibility refusal test | public trading in unassessed jurisdictions | add gate when counsel decides | External + Engineering |
| Reg 3.8 env switches need restart | Yes (by design) | settings at startup | n/a | No | L6 | — | operator procedure | runbook | Operations |
| Reg 3.9 untested refusals | Yes | — | Yes | No | L6 | the three tests named | low | add tests | Engineering |
| EXT-DB-01 production-copy PG migration | Yes | legacy runtime on SQLite | n/a (optional) | No | drills on synthetic only | real-copy drill | none for SQLite VPS | owner decision | Owner |
| EXT-WRK-01 multi-worker cycle on staging | Yes | `broker_assignment` `assigned` mode | n/a | No | synthetic takeover (L4) | host drill | scale beyond one process | staging drill | Operations |
| EXT-STG-01 hosted staging / first systemd run | **Yes** | deploy/* | n/a | No | none | host run | production host unproven | run on VPS | Operations |
| EXT-LOAD-01 exchange-demo load | Yes | — | n/a | Branch adds Linux synthetic only | branch load_test.json (L4) | demo-account load | capacity unknown (est. 8-15 accts/IP) | demo load per IP | External/Operations |
| EXT-LIQ-01 calibration / L-08 | **Yes** | `scale/spread_gate.py`, `market_entry_policy.py` | n/a | No | DEMO policy expired 2026-11-08 | — | **every production entry refused (MARKET_FALLBACK_DISABLED_BY_POLICY)** | owner records policy; research calibrates | Owner/Research |
| EXT-GL-01 live build | Yes | two-key build | n/a | No | empty allowlist | — | LIVE impossible | reviewed release | Owner + 2 reviewers |
| EXT-GL-02 policy keys | Yes | go-live policy | n/a | No | conditions BLOCKED_PENDING_POLICY | — | LIVE | owner sets | Owner |
| EXT-GL-03 revocation anchor | Yes on main | env | partial | template yes (same-disk path) | — | — | LIVE ledger | separate storage | Operations |
| EXT-BETA-A/B | Yes | golive beta | n/a | No | none | elapsed time | LIVE | 8 weeks Stage A | Owner |
| EXT-DRILL-01 / J-06 drills | Yes | emergency | n/a | No | simulated drills (L4) | authorized env drill | incident readiness | drill on staging/VPS DEMO | Owner/Operations |
| EXT-SEC-01 pen test | **Yes** | all | n/a | No | none | — | public launch | external assessor | External |
| EXT-SEC-02 secrets + historical admin creds rotation | **Yes** | user-backend admins | Yes (creds in history) | No | `MASTER_PLAN_I_L_COMPLIANCE.md:187-205`; no rotation record | — | admin takeover of migrated DB | rotate before go-live (phase A) | Operations |
| EXT-OPS-01 on-call, alert channel | **Yes** | ops tick reports OPERATIONS_OWNER_REQUIRED | n/a | No | unit comment `cosmicforge-ops-tick.service` | — | nobody paged | name on-call; configure channels | Owner |
| EXT-F8-01 bearer compat | Yes | `AUTH_BEARER_COMPAT=true` | n/a | No | env template :114 | — | token in URL/logs | flip after observation | Operations |
| EXT-BILL-01, EXT-LEGAL-01, EXT-KYC-01, EXT-TR-01, EXT-TF-* | Yes | — | n/a | No | none | — | public/commercial | external | External/Owner |
| EXT-OWN-I / L-04 removal of legacy batches | Yes | legacy runner (admin-gated, loopback) | n/a | No | 21 legacy routes still mounted (main.py 1186-2879) | — | attack surface (admin-only) | owner approves deletion | Owner |
| Closeout E-01..E-05 engineering follow-ups | Unverified by me (EXEC/CATI scope) | — | — | — | L6 | — | — | — | Engineering |
| Branch cert D1 owner bot sizing (120 USDT on a 417 USDT acct cannot fit 0.40 %) | Yes | bot config | n/a | No | L6 | — | owner's bot never enters | owner choice | Owner |
| Branch cert D2 experimental-DEMO grant + DEMO L-08 expired | Yes | governance | n/a | No | L6 | — | no DEMO strategy entries | owner choice | Owner |
| (new) runbook omits ops-tick/deadman/platform-backup timers | Yes main + branch | `docs/VPS_TRADING_DEPLOYMENT.md` | Yes | No | grep 0 | runbook/unit consistency test | alerting/backup not installed | add to §3/§10 | Engineering/Operations |
| (new) no BROKER_SECRET_KEY primary rotation | Yes | `broker_security.py`, `credential_migration.py` | Yes | No | grep: no previous-key support | rotation test | compromise → all reconnect | add key-ring | Engineering |

Branch-cert claims checked:
- "PRODUCTION_PACKAGE_READY: READY" is **overstated**. Host never run; runbook omits the alerting and backup timers; profiles not wired.
- "No internally fixable engineering defect is known to remain" is contradicted by OPS-02, OPS-07, OPS-11 and OPS-14 (none P0).

### 3. Gap list

| ID | Component | File:lines | Problem | Root cause | Evidence | Customer impact | Trading impact | Sev | Classification | Existing implementation | Required correction | Dependency | Owner | Validation test | State | Effort | Phase |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| OPS-01 | Deploy env/VPS guide | deploy/env.production.example (main, no PLATFORM_DATABASE_URL/GOLIVE_REVOCATION_ANCHOR); docs/VPS_TRADING_DEPLOYMENT.md §2 | VPS built from main refuses every entry, DEMO included; ops tick fails each minute | verified (spread_gate.py:96; grep) | L5 | DEMO bots never trade | No orders | P1 | INFRASTRUCTURE_GAP | Branch a746a58 adds §2.4 + keys | Merge a746a58; run migrate on host | PG on host | Engineering | `shared_lib.persistence.sql status` ok on host; spread gate ready | FIXED_ON_BRANCH_ONLY | S | B |
| OPS-02 | VPS runbook vs units | docs/VPS_TRADING_DEPLOYMENT.md (no ops-tick/deadman/platform-db-backup install); deploy/systemd/cosmicforge-ops-tick.*, -ops-deadman.*, -platform-db-backup.* | Alert evaluators, dead-man and platform pg_dump never installed by the documented procedure; platform dumps not off-site | verified (grep 0 on main and branch) | L5 | Silent backup/heartbeat failures | Recovery from platform DB loss impossible | P1 | OPERATIONS_GAP | Units + ALERTING.md exist | Add install/enable steps; include platform dumps in offsite_backup | OPS-01 | Engineering/Operations | Static test: every shipped timer is enabled by runbook | OPEN | S | A |
| OPS-03 | Production host | deploy/systemd/*, deploy/nginx/* | No real-host run of units, TLS, UFW, backups, alert delivery, off-site restore | external | none (EXT-STG-01) | Unknown availability | Unknown | P1 | INFRASTRUCTURE_GAP | 229 static tests pass; CI container drill | Execute runbook on VPS, keep logs as evidence | VPS access | Operations | Section 8 acceptance A–E + `vps_health_check` VERDICT HEALTHY | NOT_VERIFIED | M | A |
| OPS-04 | Admin credentials | MASTER_PLAN_I_L_COMPLIANCE.md:187-205; history db4580b/cc13da0/b35c198 | Admin e-mail/password literals in public history; no rotation recorded | verified (doc); values not inspected | L6 | Admin takeover of the migrated DB | Kill switch / go-live control | P1 | SECURITY_GAP | `create_admin.py --reset-password`, MFA reset tool | Rotate on every env before exposure; record in EXT-SEC-02 | DB access | Operations | Login with old creds fails; MFA re-enrolled | OPEN | S | A |
| OPS-05 | Security assessment | docs/security/EXTERNAL_ASSESSMENT_SCOPE.md | No external penetration test | external | L6 | Unknown vulns in public portal | — | P1 | EXTERNAL_DEPENDENCY | Scope doc, internal suites | Commission pen test, remediate | — | External | Assessor report closed | BLOCKED | L | B |
| OPS-06 | Exchange capacity | shared_lib/broker/rate_budget.py:38-92; evidence/predeploy/scale_1000_accounts.json | One egress IP reconciles about 30 Binance accounts/min; 1,000-account claim is coordination-only; Bybit/BingX limits unverified | verified (repo evidence) | L4 + L7 | Stale snapshots → entries refused at more than about 15 active accounts/IP | Fail-closed throughput ceiling | P1 | ARCHITECTURE_GAP | Shared DB budget with priority ceilings | Capacity plan: accounts per IP, multi-egress, assigned mode; verify Bybit/BingX limits | EXT-LOAD-01 | Engineering/External | Demo-account load per IP | OPEN | L | C |
| OPS-07 | Credential crypto | shared_lib/core/security/broker_security.py:84-140; shared_lib/broker/credential_migration.py; backends/scripts/rotate_broker_credential_encryption.py | No primary-key rotation (no key ring) for BROKER_SECRET_KEY, CREDENTIAL_KEY, KYC key; rotation script SQLite-only | verified (grep) | L5 | Key compromise → every customer reconnects | Trading stops for all accounts | P2 | SECURITY_GAP | Legacy→primary migration with verify + audit | Add BROKER_SECRET_KEY_PREVIOUS (MultiFernet) + rewrap script | — | Engineering | Rotation test: old→new, both readable during window | OPEN | M | B |
| OPS-08 | Sessions | deploy/env.production.example:111-114; user-backend/app/core/session_cookies.py:65,135-145; events_proxy.py:57 | Bearer compat on in production: URL `?token=` and body tokens accepted | verified | L5 | Token leakage via logs/XSS | — | P2 | SECURITY_GAP | Cookie + CSRF; switch tested | Set AUTH_BEARER_COMPAT=false after observation | EXT-F8-01 | Operations | test both ways (exists) | OPEN | S | B |
| OPS-09 | Admin vhost | deploy/nginx/cosmicforge-app.conf:226-228 | Admin console IP allow-list commented out by default | verified | L5 | Internet-reachable admin login (MFA-protected) | — | P2 | SECURITY_GAP | MFA mandatory; rate limits | Enable allow/deny or VPN by default | — | Operations | curl from unlisted IP → 403 | OPEN | S | A |
| OPS-10 | Fairness | production_runtime.py `_broker_loop` (sequential, 30 s); rate_budget.py | No per-tenant share of IP budget or cycle time; slow account delays all | verified (code), impact inferred | L5/L7 | Noisy neighbour | Delayed protection checks under load | P2 | ARCHITECTURE_GAP | Priority classes (protection > reconciliation) | Per-account fair share; parallel I/O; assigned mode | OPS-06 | Engineering | Load test with one slow venue account | OPEN | M | C |
| OPS-11 | Bot lifecycle | backends/bot-backend/app/core/bot_instance_service.py:84-140,593-625 | start/create don't re-check one-occupying-bot invariant; restart of stopped bot creates ambiguity | verified | L5 | Confusing "blocked" state | Entries refused on account (fail-closed) | P3 | LOGIC_DEFECT | Deploy transaction enforces it | Apply same BEGIN IMMEDIATE check in start | — | Engineering | Test: deploy B, restart stopped A → 409 | OPEN | S | C |
| OPS-12 | Risk scope | trading_intelligence (no user-level budget) | No aggregate risk budget across a user's multiple accounts | verified absence (grep) | L5/L7 | Multi-account user exceeds intended total risk | Risk concentration | P2 | OWNER_POLICY_DECISION | Per-account 0.40 %/daily loss | Owner decides; implement if required | — | Owner | — | OPEN | M | D |
| OPS-13 | Runtime ownership | docs/VPS_TRADING_DEPLOYMENT.md:31-33; runtime lease in SQLite | Two hosts with DB copies can both trade (lease is per DB) | verified (doc) | L6/L5 | Duplicate orders during migration | Duplicate exposure | P2 | OPERATIONS_GAP | Procedure + clientOrderId idempotency | Platform-DB-level owner lease across hosts, or exchange-side guard | OPS-01 | Engineering | Two-host drill refuses second | OPEN | M | B |
| OPS-14 | Tests | user-backend/tests/test_phase2_broker_security.py; broker_security.is_production (DATABASE_ROLE default "production") | 3 tests fail in isolation without BROKER_SECRET_KEY (order-dependent pass) | verified (run) | L4 | — | — | P3 | TESTING_GAP | Suite green in CI order | Fixture sets BROKER_SECRET_KEY / DATABASE_ROLE=test | — | Engineering | Run file alone → 10 passed | OPEN | S | A |
| OPS-15 | IDOR testing | all customer path-param routes | No systematic cross-tenant test over every owner-scoped route | verified absence | L5 | Future regression risk | — | P2 | TESTING_GAP | Per-feature tests; auth route-walk test | Route-walk with two users asserting 403/404 | — | Engineering | New route-walk test | OPEN | M | B |
| OPS-16 | Inter-service auth | bot-backend/app/core/auth.py:66-157; VPS doc :209-214 | bot `require_admin` trusts `users.role=admin` (non-MFA store); shared HS256 key across 3 services; X-ENGINE-KEY unchecked | verified | L5 | Low (loopback only; no API sets role) | Legacy money routes reachable to such a token | P3 | SECURITY_GAP | Loopback bind; emergency service token | Require admins-table service token for legacy routes or delete them (L-04) | EXT-OWN-I | Engineering | Test: users.role=admin token → 403 on /trade/market | OPEN | S | B |
| OPS-17 | Legal gating | user-backend legal service; register 3.7 | Counsel status and country eligibility not enforced for trading | verified (register + no gate) | L6/L5 | Service in unassessed jurisdictions | — | P1 | MISSING_FEATURE | Documents + eligibility table | Eligibility gate before trading/deploy | EXT-LEGAL-01 | External/Engineering | Undetermined country → refused | OPEN | M | D |
| OPS-18 | Config profiles | deploy/profiles/*; scripts/validate_config_profile.py | Profiles not consumed by runtime or the VPS path; production template fails validator (PROFILE_NAME_MISSING) | verified (run) | L5 | — | Config drift unchecked | P3 | INTEGRATION_GAP | Validator + rules | Validate the real .env in ExecStartPre or drop profiles | — | Engineering | Unit refuses non-admissible env | OPEN | S | A |
| OPS-19 | Env templates | deploy/env.production.example:108-143; VPS doc §10.4 `ENGINE_URL` | User-backend keys in bot .env template; documented ENGINE_URL unconsumed (code reads BOT_BACKEND_URL) | verified | L5 | Operator confusion; KYC 503 if keys put in wrong file | — | P3 | OPERATIONS_GAP | Defaults correct | Split templates; fix key name | — | Engineering | Template key-consumption test | OPEN | S | A |
| OPS-20 | Protective recovery | shared_lib/core/production.py:174-182 (main) | Order switch off strands protection (reg 3.2) | verified | L5 | DEMO positions unprotected | Cannot reduce exposure while switch off | P1 | LOGIC_DEFECT | — | Merge 334f2ff | — | Engineering | test_risk_reduction_gate.py on merged tree | FIXED_ON_BRANCH_ONLY | S | C |
| OPS-21 | Monitoring ops | cosmicforge-ops-tick.service (OPERATIONS_OWNER_REQUIRED); alerts.env | No on-call, no alert channel configured, no authorized kill/flatten drill | external | L6 | Incidents unnoticed | Unattended positions | P1 | OPERATIONS_GAP | Alerting, escalation, dead-man | Name on-call; configure channels; drill | EXT-OPS-01, J-06 | Owner | notify_failure test message received | BLOCKED | S | B |
| OPS-22 | LIVE release | shared_lib/core/live_release_allowlist.py; production.py:19; golive/gate.py | No certified family, no LIVE venue cert, no release, policy keys unset, no beta weeks | by design/external | L5 | — | No real-money trading | P1 | EXTERNAL_DEPENDENCY | Full 17-condition gate + two-key build | Research cert → venue LIVE cert → owner release → beta | many | Owner/Research/External | Gate evaluate → all PASS | BLOCKED | XL | D |

### 4. Verdicts

| Area | Verdict | Reasons |
|---|---|---|
| Multi-account operation | **PARTIAL** | Tenant/credential isolation and account-scoped execution are sound in code (L5/L4). One bot per account is deliberate and fail-closed. Several accounts per user are supported. Not ready for scale: one sequential cycle per process; an IP budget that allows about 8–15 actively traded Binance accounts per egress IP (repo evidence: 30 reconciles/min); multi-worker `assigned` mode not host-drilled; no per-user aggregate risk; only Binance DEMO venue-proven; load test synthetic. |
| Security | **PARTIAL** | Strong internal controls: admin MFA forced, cookie+CSRF, token_version revocation, fail-closed secrets, withdraw-key refusal, SSRF guard, parameterised SQL. Open items: no external pen test; historical admin creds not rotated; bearer compat on; no primary-key rotation; admin vhost not IP-restricted by default. |
| Monitoring and recovery | **PARTIAL** | Implemented: health check, alert unit, ops tick, dead-man, backups, a Linux restore drill (branch, synthetic, 22/22). Missing: the runbook never installs ops-tick, dead-man or platform backup; no channel; no on-call; platform dumps not off-site; production off-site restore unproven; switch-off protection fix only on the branch. |
| VPS deployment package | **PARTIAL** | Units complete and internally consistent at file/module level (229 static tests). On main the platform DB is missing, so no order is possible (fixed only on the branch). The runbook omits three timers. Templates are confused; profiles are not wired. |
| Production-host testing | **NOT VERIFIED / BLOCKED** | No host run evidence (EXT-STG-01); no VPS access in this audit. |
| Public customer trading | **BLOCKED** | No certified strategy; L-08 policy expired; no pen test; legal/eligibility not enforced; billing/KYC external; no on-call. |
| Release level D (real money) | **BLOCKED** | LIVE disabled by 7 independent layers (env flag, startup refusal, two-key build with empty allowlist, transport, 17-condition go-live gate, M6 cap, LIVE certification refused). No live-release artifact, no LIVE certification, no live-certified strategy, no production-host evidence. These are correct engineering blocks, plus owner/external dependencies. |

