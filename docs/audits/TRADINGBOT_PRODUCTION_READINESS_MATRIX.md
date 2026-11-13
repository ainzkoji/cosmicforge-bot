# Production Readiness Matrix

**Audit:** CosmicForge Tradingbot master end-to-end audit (read-only) · 2026-11-13 · baseline `origin/main` `649080b`, branch `732e0e2`
Verdict scale: READY / PARTIAL / BLOCKED / NOT VERIFIED. Evidence levels L1–L7 as defined in the master report. "M-xx" = consolidated finding
in the master report; area IDs map to [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv).

## 1. Category verdicts (Section 35)

| Category | Verdict | Evidence | Exact remaining requirements |
|---|---|---|---|
| Source integration | **PARTIAL** | main `649080b` CI green; 7 fixes only on branch `732e0e2` (CI green, merges cleanly) — L3/L5 | Complete M-01 on branch, merge (M-02), re-run CI on main |
| Architecture | **PARTIAL** | Single lease-owned runtime, fail-closed (L5, 119 tests L4); two execution tails with different gate sets; three DB stories | M-05 path-B gate parity; M-07 one certified tail; M-30 decide one production DB model |
| Market-data collection | **PARTIAL** | 3 Binance collectors wired under lease (L5; 94 tests L4); budget incident fixed (def0bf4) | M-19 budget gaps; DATA-03 prune; DATA-06 honest health; M-17 per-IP capacity plan |
| Historical research data | **PARTIAL** | Frozen checksummed manifests (Binance daily 911 contracts 2020–2026, 15m 136 symbols, BTC/ETH 1m) — L5 | M-33 offsite custody + licence record; hourly history retention; microstructure/liquidation/Bybit/BingX data per research need |
| Autonomous strategy research | **BLOCKED** | Libraries/CLIs only; no generator/orchestrator/scheduler (L5) | M-24 orchestrator over existing CLIs, generic evaluator, register-hypothesis command, scheduled drift/forward checks; owner-set thresholds (RES-05) |
| Strategy certification | **PARTIAL** | Register hash chain verified (27 records, 18 hypotheses); 0 certified; DAILY_TREND PENDING_HOLDOUT (L4/L5) | Owner decisions RES-01/RES-05; holdout evaluation; governance promotion |
| CATI runtime | **PARTIAL** | Wired end-to-end for path A; stale-signal guard; idempotent orders (L5/L4) | M-04 authorized family; CATI-01 decide fate of unwired intelligence stack; M-17 concurrency |
| Broker connections | **PARTIAL** | Shared authoritative account identity; Binance DEMO reads repaired (L2/L5) | M-11 credential deletion + flatten; M-21 withdrawal check fail-closed; EXEC-11 HTTP outside DB lock; M-20 Bybit/BingX |
| DEMO execution | **PARTIAL** | Binance DEMO certification genuine for the platform adapter (L2) | M-01; M-07 certify the tail strategies use; M-06 certification portability; M-08 first strategy order evidence |
| LIVE execution | **BLOCKED** | Seven independent LIVE locks (L5) — correct by design | M-23 (certified family at LIVE stage, LIVE venue cert, live-capable release, two-operator approval) + M-19, M-21, M-22 |
| Risk management | **PARTIAL** | Layered final-boundary enforcement on both tails (L5/L4) | M-01; M-05; M-26 one risk engine + correlation; M-25 remove ungated broker paths; CUST-16 0.40 % in hard risk |
| Portfolio accounting | **BLOCKED on main / PARTIAL on branch** | main sums DEMO+LIVE (`portfolio_service.py:130-142`); fixed by 65372fe (L4) | Merge 65372fe; M-10 analytics/report separation, multi-currency realized P&L, funding, environment labels, data-age display |
| Multi-account operation | **PARTIAL** | Isolation sound in code (L5/L4); one active bot per account is a deliberate fail-closed rule | M-17 capacity; OPS-12 cross-account risk budget; OPS-13 single-host guarantee; OPS-11 restart re-check; M-32 cross-tenant tests |
| Customer Frontend | **PARTIAL** | All live UI calls hit real authenticated routes; 102/102 tests; build OK (L4) | M-09 sizing preview; M-10; M-34 close-position/error display; BotDetails delete; E2E tests |
| Admin Frontend | **PARTIAL** | Admin authZ + MFA + audit rows sound (L4); 23/23 tests | M-12 truthful health/settings/bot monitor + CATI views; M-29 roles; CUST-11 suspension stops bots |
| API integrations | **PARTIAL** | 709 routes; every non-public route has an auth dependency; no broken live frontend contract (L5) | LEAD-02 duplicate route; user-backend `POST /api/v1/bot-instances` → missing engine route; dead client functions; LEAD-03 duplicated admin API; LEAD-05 route smoke tests |
| Database integrity | **PARTIAL** | Strong order idempotency (L5/L4); runtime schema unversioned; shared SQLite file | M-03 platform DB deployed; M-30 schema versioning, joint restore point; ARCH-11 stuck job reclaim |
| Worker reliability | **PARTIAL** | Core loops lease-owned and supervised (L5/L4) | M-27 shutdown; ARCH-13 lease-gate remaining loops; ARCH-10 snapshot scheduler; M-13 timers |
| Monitoring and recovery | **PARTIAL** | Supervisor/watchdog, health checks, alert engine, backup scripts, restore drill evidence (L4/L6) | M-13 timers installed; M-18 on-call + channel; DATA-06 honest health; offsite platform dumps; host restore drill |
| Security | **PARTIAL** | Encryption fail-closed without key; MFA admin; parameterised SQL; no P0 found (L5/L4) | M-15 rotation; M-16 pen test; M-29 key rotation, bearer compat off, admin allow-list; M-21 |
| VPS deployment package | **PARTIAL** | Units, nginx, containers, profiles, backups exist; branch adds platform DB (L5) | M-03 (merge); M-13; validator enforced at startup; anchor off-disk; admin-backend URL check |
| Production-host testing | **NOT VERIFIED / BLOCKED** | No host access; no host evidence in repo (EXT-STG-01) | M-14 staging-then-VPS run with recorded evidence |
| Public customer trading | **BLOCKED** | No authorized strategy; LIVE locked; no host evidence | Phases A–H |

## 2. Customer release levels (Section 26)

| Level | Verdict | Reason | Minimum to reach READY |
|---|---|---|---|
| A — Platform available | **PARTIAL** | Register/verify/login/MFA/KYC/logout wired and tested (L4) | M-02, M-12 (or hide pages), M-13, M-14, M-15 |
| B — Broker-connected DEMO | **PARTIAL** | Binance DEMO connection, balances, positions, bot creation/monitoring work (L2/L4); main mixes DEMO/LIVE money and strands protection | Level A + M-01, M-03, M-06, M-09, M-10, M-11, M-16, M-18 |
| C — Autonomous DEMO trading | **BLOCKED** | No family with order authority; grant/L-08 lapsed; path-B gates; no strategy-order evidence | Level B + M-04 (owner decisions or labelled experimental grant), M-05, M-07, M-08, M-31; M-17 for >~10 accounts |
| D — Controlled real money | **BLOCKED** | By design | Level C forward evidence + M-19, M-21, M-22, M-23 |

## 3. Broker certification matrix (Section 13)

| Venue / env | Adapter version | Auth | Bal read | Order read | Entry | Fill rec | Native stop | Stop repl. | Reduce-only exit | Final state | Expiry | Evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Binance DEMO | binance-usdm-1.2.0 (factory.py:59-60), source def0bf4 | PASS | PASS | PASS | PASS (MARKET, 1333417962) | PASS | PASS (algo:1000000268023071) | PASS (create-confirm-retire) | PASS (1333418135) | flat, PASS | 2027-01-30 (90 d after 2026-11-01T08:53:33Z) | L2 |
| Binance LIVE | n/a | - | - | - | - | - | - | - | - | - | - | NOT CERTIFIED; the register refuses LIVE CERTIFIED (certification.py `LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD`) |
| Bybit DEMO/LIVE | bybit-linear-1.2.0 | none | | | | | | | | | | No venue evidence (synthetic wire tests only, L4) |
| BingX DEMO/LIVE | bingx-swap-1.2.0 | none | | | | | | | | | | No venue evidence. Stop replacement refused by design |

Binance DEMO 30/30 verification:
- The hash chain is intact across both files, seq 1-4: genesis -> readonly run (INCOMPLETE, source "unknown", 08:50:29Z, before the def0bf4 commit at 08:51:46Z) -> orders run (08:52:16Z-08:53:33Z, source def0bf4).
- Each run's `actions_hash` and all 30 `evidence_hashes` recompute correctly.
- `mocked_transport: false`, venue order ids present, permit bounded (ADAUSDT, leverage 1, 60 USDT, 1 h).
- The hash is self-referential (not externally anchored), so it proves integrity, not authenticity (L2 at best; venue ids not independently verifiable here).

Adapter code after certification: `git diff def0bf4 origin/main -- backends/bot-backend/app/exchange/platform_adapter backends/shared/shared_lib/broker` is empty. The branch changes `shared_lib/core/production.py` (transport gate) and `pipeline/execution.py`, neither of which is an adapter file.

Retest triggers are weak:
- `adapter_version` is a hand-maintained constant.
- `record_adapter_version` (the VERSION_CHANGED trigger) is called only by `AccountSession`, which has no production caller.
- The certification lives only in the DB where it ran (`cati_adapter_cert_events`). There is no import tool, so a fresh VPS starts NOT_CERTIFIED until a run on the VPS.
- `docs/exchanges/EXCHANGE_CERTIFICATION.md:12-14, 174` still says every venue is NOT_CERTIFIED and "Not wired" (stale).

Unverified Binance operations: post-only, leverage above 1, hedge mode, multiple symbols, partial fills of market orders, take-profit orders, and the legacy executor path.

Certification rules applied: a DEMO certificate is not carried into LIVE; a certificate does not cover a code path it did not exercise
(the legacy `BinanceExecutor` tail used by RESIDUAL strategy orders, M-07); a fresh host database holds no certificate until imported or re-run (M-06).

## 4. LIVE release gates (Section 25)

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
| Strategy certification | yes (family registry, research register) | yes (family gate at boundary) | no family certified; RESIDUAL FAILED, DAILY_TREND PENDING_HOLDOUT, all M0 (see STRATEGY_RESEARCH_AUDIT; the branch cert §1 wording 'every family FAILED' is inaccurate) | certification evidence / research |
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
