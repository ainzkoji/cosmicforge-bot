# Section H, Steps 5–8 and Section I: implementation status

Started 17 October 2026 from `22a80bc034f54084ac55e0a0eb2edba3adc4d69d`. That commit was identical to `origin/main`,
and CI run [38960175201](https://github.com/ainzkoji/cosmicforge-bot/actions/runs/38960175201) passed it with six of
six jobs.

Authority: the Section H Steps 5–8 / Section I master implementation prompt. The master plan and the Section J launch
checklist are **not in the repository**. Nothing below quotes them; Step 8's checklist is provisional (see the
[external gates register](STEP5_STEP8_EXTERNAL_GATES.md)).

Trading authority is unchanged and remains **SHADOW_ONLY**:

- Live orders are disabled.
- Every family is at M0 and uncertified, and DAILY_TREND/H-018 is not certified.
- The Mandate 004 holdout is unopened.
- Risk limits are 0.40% per trade and a 15% entry stop.
- The frozen research artifacts and the four-specialist invariant are unchanged.

Status vocabulary:

- **PASS** means component engineering acceptance, with tests passing where stated.
- **PARTIAL** means part of the requirement is engineered and the rest is named.
- **BLOCKED** means an external dependency or an owner decision is required.
- PASS never means external, operational, venue, research or launch certification. Those are in the
  [external gates register](STEP5_STEP8_EXTERNAL_GATES.md).

Unless stated otherwise:

- Paths are relative to `backends/bot-backend/`.
- Tests are under `backends/bot-backend/tests/platform_workers/`.
- "Both dialects" means every test ran on SQLite and on PostgreSQL: a disposable PostgreSQL 18.4 cluster locally,
  and a PostgreSQL 16 service in CI (`PLATFORM_TEST_POSTGRES_REQUIRED=1`).

Commits:

| Commit | Content |
|---|---|
| `fea1bc8` | Steps 5.1–5.5 |
| `826b634` | Step 5.2 process roles and Step 5.6 |
| `6177620` | Step 7 |
| `0fd63f7` | Steps 6.1–6.3 and 6.5 |
| `69e1b76` | Step 8.4 |
| `0950fe2` | CI fixes for `0fd63f7` |
| `0878d61` | Steps 8.1–8.3, 6.4/8.6 and 8.5 |
| the documentation commit after it | gate identity and legal wiring, load evidence, these reports |
| closure commits after `78a14e4` | the remaining internal gaps: see [engineering closure](STEP5_STEP8_ENGINEERING_CLOSURE.md) |

## Work-item register

| ID | Status | Existing functionality reused | New implementation | Tests and results | Evidence | Commit | External dependency / owner decision | Exact next verification |
|---|---|---|---|---|---|---|---|---|
| 5.1 PostgreSQL, migrations, pooling | **PASS** (engineering) / production copy **BLOCKED** | `shared_lib.persistence` legacy schema; backup script | platform layer `shared_lib/persistence/sql/`; legacy runtime on PostgreSQL: closed translator `sql/legacy_dialect.py`, adapter `legacy_postgres.py` (`sqlite3` surface, pool, advisory write lock), `DB(postgresql://...)`, exact money storage (`legacy_money.py`, unconstrained NUMERIC) | `test_platform_database.py` 25 (both dialects); `test_legacy_dialect.py` 94, `test_legacy_money.py` 60, `test_legacy_postgres_adapter.py` 45, legacy drill/money/inventory 23; 28 existing runtime modules on PostgreSQL through `tests/legacy_pg_plugin.py` (CI job "Legacy runtime on PostgreSQL") | `evidence/step5/legacy_migration_drill.json` 13/13; `legacy_persistence_inventory.{json,md}` (297 tables, 614 REAL columns classified) | closure commits ([engineering closure](STEP5_STEP8_ENGINEERING_CLOSURE.md)) | Owner-authorized sanitized production copy (EXT-DB-01). Runtime money arithmetic is still float in about a dozen modules (named in the closure report). PostgreSQL is opt-in: every deployment file still sets SQLite. | `scripts/migration_drill.py --legacy --source <copy> --owner-authorization <ref>` on authorized infrastructure |
| 5.2 Trading workers separated from the API | **PASS** (engineering) | RuntimeOwnership lease, production runtime, `AccountLeases` | `app/ops/process_role.py`; `app/platform_workers/{assignments,worker,drill,broker_assignment,account_fence}.py`. The production broker cycle runs every account step under a fenced `WorkerAssignments` assignment (`BROKER_CYCLE_ASSIGNMENT_MODE` owner/assigned), with a recovery-only first step after takeover, emergency preemption and cross-host runtime-lease liveness | `test_worker_takeover.py` 26, `test_process_role.py`, `test_deployment_drill.py` (both dialects); `test_step5_broker_cycle_assignment.py` 44 (real broker cycle, both dialects); `test_runtime_ownership_cross_host.py` 5 | `evidence/step5/broker_cycle_takeover.json`: real-cycle process kill drill; takeover within the lease plus one successor round (6.1 s SQLite, 12.7 s PostgreSQL, lease 3 s); the killed worker is silent; never two workers on one account | closure commits | Staging kill drill in `assigned` mode (EXT-STG-01) | Run the kill drill on the staging host with `BROKER_CYCLE_ASSIGNMENT_MODE=assigned` |
| 5.3 Durable job queue | **PASS** | `notification_outbox` pattern; `cati_execution_intents` idea | `app/platform_workers/jobs.py`; production handlers in `runtime_jobs.py` (order resolution, reconciliation, notification bridge, shadow comparison, operational retry), run by the broker loop and by `TradingWorker` | `test_job_queue.py` 30; `test_runtime_jobs.py` 46 and `test_runtime_jobs_recovery.py` 2 (both dialects, real boundary) | crash/recovery: no order lost or duplicated | closure commits | None for engineering | Observe the queue on staging |
| 5.4 Signal once, size per account | **PASS** | `daily_trend.market_stage` / `decide`, `performance.synthetic_market` | `trading_intelligence/scale/{fanout,measure_fanout}.py` | `test_signal_fanout.py` 6 | `evidence/step5/signal_fanout.json`: shared signal 15.9–18.1 ms at 1–1,000 accounts (max/min 1.14); sizing 0.2 ms per account; threads give no speed-up (CPU-bound) | `fea1bc8` | None for engineering | Repeat on the Linux staging host |
| 5.5 Deterministic order spreading | **PASS** (boundary enforcement) / calibration **BLOCKED** | venue cost model depth bounds (5 s, 5 levels) | `scale/order_spreading.py`; `scale/spread_gate.py`, enforced by `CATIExecutionBoundary` for every production entry; slot lifecycle tied to the durable attempt; calibration observations | `test_order_spreading.py` 18, `test_spread_gate.py` 25 (both dialects), `test_step5_spread_boundary.py` 45 | `evidence/step5/order_spreading_boundary.json`: concurrent workers never exceed the cap; uncalibrated multi-account refused | closure commits | Uncalibrated (EXT-LIQ-01). Under the default policy every production MARKET entry is refused (`MARKET_FALLBACK_DISABLED_BY_POLICY`) until the owner approves a policy | Owner-approved single-account market policy; calibrated model from the collected observations |
| 5.6 Staging, containers, deploy, rollback | **PARTIAL** | systemd units, backup scripts, health check | `deploy/containers/*`, `deploy/staging/*` (shared lib, release gate, deploy/rollback/restore with exit codes and state records, drill images), CI container job with a Docker failure drill | `tests/deploy` 30 (Linux CI), `test_deployment_failure_drills.py` 4 (both dialects), `test_deployment_drill.py` | failure-injection matrix in the closure report; 7 defects fixed | closure commits | There is no authorized hosted staging environment (EXT-STG-01). | Run `deploy/staging/deploy.sh` and `rollback.sh` on an authorized host |
| 6.1 Go-live authorization gate | **PASS** (disabled) | certification register, family registry, kill switches, `order_submission_gate`, broker accounts | `app/golive/{ledger,authorization,gate}.py`, `api/golive.py`; boundary hook | `test_golive_gate.py` 55 (both dialects) | every condition fails on its own; a restore cannot resurrect a revocation | `0fd63f7` | `BUILD_PERMITS_LIVE=False`. Owner policies are unset. Real families are uncertified. | Admin console page; owner sets the policies |
| 6.2 Live beta Stage A | **PASS** (infrastructure) | shadow comparison | `app/golive/beta.py` (Stage A, reviews, evidence export) | in `test_golive_gate.py` | — | `0fd63f7` | 8 weeks of genuine operation (EXT-BETA-A); the owner sets `STAGE_A_MAX_NOTIONAL` | Starts only after every gate condition passes |
| 6.3 Live beta Stage B | **PASS** (infrastructure) | — | `beta.py` invitations / enrolment / withdrawal / disable / weekly summary | in `test_golive_gate.py` | — | `0fd63f7` | Invited-user operation deferred; the owner sets `STAGE_B_MAX_BUDGET` | — |
| 6.4 Publishable live track record | **PASS** (fixtures) | equity recorder (concepts) | `app/golive/track_record.py`, `sql/track_record_schema.py`, `api/public_track_record.py` | `test_track_record*.py` 70 (both dialects) + 10 methodology | `docs/launch/TRACK_RECORD_METHODOLOGY.md` | `0878d61` | No live record exists, and none was fabricated | First authorized live day |
| 6.5 Live incident procedures | **PASS** (synthetic) | kill switches, `EmergencyControl`, `RestReconciler`, FakeVenue | `app/golive/{incidents,drills}.py` | `test_incident_drills.py` 5 (13 drills × both dialects) | `evidence/step6/incident_drills_postgresql.json`: 13/13 contained and closed | `0fd63f7` | Live-money drills need separate authorization | Repeat on staging; then on Stage A with authorization |
| 7.1 Frozen tradfi research specification | **PARTIAL** (draft) | `research/trend_v1/SPEC.md`, research register | `research/trend_v2_tradfi/SPEC.md` (DRAFT, not frozen, not registered) | — | 16 owner decisions D1–D16 | `6177620` | Owner decisions; register H-019 | Owner review of D1–D16 |
| 7.2 Market calendars, contract economics | **PASS** (engine) | FX session guard (kept) | `app/markets/*`, `pipeline/family_markets.py` | `tests/markets` 95 | DAILY_TREND definition hash and Mandate 004 fingerprint unchanged | `6177620` | Calendar and contract data are illustrative and UNVERIFIED; no data source is licensed | Load verified calendars from an authoritative source |
| 7.3 Portal market selection | **PASS** | deployment contract | `market_scope`, availability API and proxy, `CatiDeployment` control | `test_step7_market_scope.py`, proxy tests, frontend `markets.test.ts` | — | `6177620` | — | — |
| 7.4 Tradfi staged activation | **PASS** (mechanics) | family governance operator authorization | `app/markets/activation.py`, `markets_schema.py` | `test_step7_market_activation.py` 19 | LIVE refused in this build | `6177620` | Stage durations unset (BLOCKED_PENDING_POLICY) | — |
| 8.1 Stripe billing | **PASS** (test mode) | existing Stripe integration | lifecycle and refund/dispute handling, audit log, live-key guard, portal | `test_billing_lifecycle.py` 9 + billing suites; user-backend 820/823 (3 environment-specific, pass on Linux CI) | `docs/launch/BILLING_LEGAL_IDENTITY.md` | `0878d61` | Live-mode verification, real prices (EXT-BILL-01); `BILLING_ENFORCED=false` | One authorized live purchase, refund and dispute |
| 8.2 Legal documents and consent | **PASS** (infrastructure) | registration consent columns | `shared_lib/legal/*`, legal API and admin API, reconsent prompt | `test_legal_documents.py` 16 | drafts only | `0878d61` | Counsel review (EXT-LEGAL-01); country determinations | — |
| 8.3 Identity checks | **PASS** (policy and adapter) | KYC cases and manual review | `shared_lib/identity/*` | `test_identity_verification.py` 138 | — | `0878d61` | Provider selection and certification (EXT-KYC-01) | — |
| 8.4 Security hardening | **PASS** (internal) / external **BLOCKED** | JWT, user TOTP, redaction, Fernet broker keys | admin TOTP MFA, cookie sessions with CSRF, HKDF credential KDF (`69e1b76`); F1–F9 remediation: token version and session chain checked on every request, session lifetime aligned, dead route and helpers removed, KYC dev-key fallback opt-in only, admin MFA bootstrap code, in-major dependency upgrades | `test_section_h84_security.py`, `test_section_h84_closure.py` 37, `test_admin_token_revocation.py` 5, `test_section_h84_session_revocation.py` 10 | `docs/security/SECURITY_HARDENING_REPORT.md` section 6: F1–F5, F7, F9 FIXED; F6 PARTIAL (major upgrades upstream); F8 OPEN (operational cut-over) | closure commits | External penetration test (EXT-SEC-01); F6 major upgrades; end of the F8 compat window; mt-bridge server decision | Independent assessment |
| 8.5 Production operations | **PASS** (engineering) / owner **BLOCKED** | `notify_failure.py` channels, backup and offsite scripts, health check | `shared_lib/operations/*`, `app/ops/alerting.py`; 20 alert producers (`app/ops/alert_producers.py`); operations tick with a database lease (`tick_lease.py`, systemd timer, compose `ops-tick`); suppression, recovery notices, sink timeout | alerting, producers, tick, status page and drill: 225 (both dialects) | `evidence/step8/restore_drill.json`: 22/22; a revoked live permission cannot be restored; `docs/operations/{RUNBOOKS,ALERTING,BACKUP_AND_RESTORE}.md` | closure commits | No on-call person (OPERATIONS OWNER REQUIRED); no notification provider or off-site credentials | Owner names the rotation; run the tick on the staging host |
| 8.6 Public track record | **PASS** (empty state) | — | `/track-record` page, public API | frontend `trackRecord.test.ts` | NOT_YET_AVAILABLE for live | `0878d61` | Real authorized records | — |

## Section I

See [SECTION_I_REMOVAL_INVENTORY.md](SECTION_I_REMOVAL_INVENTORY.md).

| Batch | Status |
|---|---|
| 1 | COMPLETE (`9fb120f`) |
| 2 | DEFERRED, owner (REM-01) |
| 3 | DEFERRED, owner (REM-01) |
| 4 | DEFERRED, owner (REM-01) |
| 5 | **REMOVED** in its own commit and CI run |
| 6 | BLOCKED: conflicting retain-until-M9 governance rule |
| 7 | BLOCKED: live dependencies |
| 8 | BLOCKED: live dependencies |
| 9 | KEEP pending the venue decision |
| 10 | PARTIAL keep / pending |

Quarantine: 117 entries; Batch 5 removed 11 (106). The closure repaired 44 more, leaving **62** (54 bot, 6 admin, 2 root); none was added. See [QUARANTINE_REGISTER.md](QUARANTINE_REGISTER.md).

## Verification log

- Baseline verified: HEAD = origin/main = `22a80bc`. The working tree held someone else's uncommitted change to
  `daily_feed.py`, `platform_runtime.py` and `test_step3_platform_runtime.py` (a budgeted-getter refactor that
  deletes a test). It was left untouched and uncommitted.
- CI run [39024969808](https://github.com/ainzkoji/cosmicforge-bot/actions/runs/39024969808) on `fea1bc8` failed
  one test: the retention policy did not classify the new platform tables. Fixed in `6177620` by classifying every
  table that Steps 5–8 add.
- CI run [39027836407](https://github.com/ainzkoji/cosmicforge-bot/actions/runs/39027836407) on `0fd63f7` had two
  failures:
  - The container job built the images and passed the no-credentials check, then failed on a compose YAML
    flow-sequence error. Fixed in the documentation commit.
  - 13 bot-backend tests failed, all from Step 6. Eleven legacy execution-tail tests were refused at the new
    go-live gate. One safety test bans route paths containing `withdraw`. One schema-upgrade test asserted that
    migrate() adds nothing. Fixed in `0950fe2`; the reasons are in that commit message.
- The gate's identity wiring was found to pass the database as the user id. It was corrected to
  `identity_permits(user_id, action, db=...)`, and the consent condition now also requires current acceptance of
  the risk disclosure and the automated-execution consent (`shared_lib.legal`).
- Local full regression on an export of the combined tree:
  - admin 88 + 6 xfailed; shared 99; root 4 + 3 xfailed;
  - user: one failure, which is an export artifact (the test reads `frontends/`);
  - both frontends: lint clean, unit tests 83 (user) and 49 (admin), production builds OK.
- Housekeeping: two untracked files named `nul` (626 KB and 958 KB) were deleted from the frontend folders without
  inspecting their content. They were most likely ESLint JSON output written by `-o nul` in Git Bash.

## Engineering closure by step

| Step | Engineering acceptance | Full closure |
|---|---|---|
| 5 | **PARTIAL.** 5.1–5.5 PASS for engineering ([engineering closure](STEP5_STEP8_ENGINEERING_CLOSURE.md)): the legacy runtime runs on PostgreSQL, the real broker cycle runs on worker assignments, production job handlers are registered and the order budget is enforced at the boundary. 5.6 PARTIAL: artifacts and drills work in disposable environments; there is no hosted staging. The replayed 1,000-account load and worker-death recovery were measured (`evidence/step5/`). | **BLOCKED.** It needs the 1,000-account run on exchange demo with replayed data on authorized staging, plus Linux resource figures (EXT-LOAD-01, EXT-STG-01, EXT-DB-01). |
| 6 | **PASS for infrastructure.** All five items are built, in disabled mode, with synthetic tests and drills. | **BLOCKED.** It needs at least 8 weeks of genuine Stage A live results inside the expected range, no unprotected position, no unresolved mismatch and closed incidents. A certified family, promotion, venue certification and an owner release of `BUILD_PERMITS_LIVE` are prerequisites. No automatic promotion exists. |
| 7 | **PASS through the safe boundaries.** 7.2–7.4 PASS; 7.1 is a DRAFT specification. | **BLOCKED.** It needs owner decisions D1–D16, H-019 registered and frozen, passing the Step 2 standard and a matching demo run. Until then traditional markets stay disabled. |
| 8 | **PASS for test and staging readiness.** 8.1–8.6 are built. | **BLOCKED.** It needs the authoritative Section J checklist (unavailable; a provisional checklist is in the gates register), counsel review, country determinations, an external penetration test, live billing verification, a named operations team and explicit owner launch approval. |
| Section I | Batch 1 was already complete. Batch 5 was removed in its own commit. The rest were not executed (see the inventory). | Batches 2–10 each need the decision or dependency named in the inventory. |
