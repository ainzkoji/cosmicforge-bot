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

## Work-item register

| ID | Status | Existing functionality reused | New implementation | Tests and results | Evidence | Commit | External dependency / owner decision | Exact next verification |
|---|---|---|---|---|---|---|---|---|
| 5.1 PostgreSQL, migrations, pooling | **PARTIAL** | `shared_lib.persistence` legacy schema; backup script | `shared_lib/persistence/sql/` (database, migrations, platform/launch/track-record/operations schemas, CLI) | `test_platform_database.py` 25/25 on both dialects (concurrent migration ×10 stable) | `evidence/step5/migration_drill.json`: 13/13 checks, PostgreSQL dump and restore; finding MIG-F1 | `fea1bc8`, `826b634`, `0fd63f7` | Done: the platform layer (assignments, jobs, intents, budgets, go-live, incidents, track record, operations) runs on PostgreSQL with FKs, NUMERIC money and server time. Not done: the legacy runtime schema (raw sqlite3, REAL money, about 250 tables) still runs on SQLite; porting it is a separate migration project (EXT-DB-01). | Owner-authorized migration of a sanitized production copy with `scripts/migration_drill.py --source` |
| 5.2 Trading workers separated from the API | **PASS** (engineering) | RuntimeOwnership lease, production runtime, `AccountLeases` | `app/ops/process_role.py`; `app/platform_workers/{assignments,worker,drill}.py` | `test_worker_takeover.py` 26, `test_process_role.py` 5, `test_deployment_drill.py` 2 (both dialects) | `evidence/step5/worker_kill_drill.json`: SIGKILL takeover 2.75 s (PostgreSQL) and 2.64 s (SQLite) against a 3 s lease; drain 0.22 / 0.36 s; 0 duplicates | `fea1bc8`, `826b634` | The production broker cycle still runs as one owner process (`PROCESS_ROLE=trading`). Its per-account loop is not yet sharded over `WorkerAssignments` (EXT-WRK-01). | Wire `production_runtime._sync_accounts` onto assignments behind a flag, then repeat the kill drill on staging |
| 5.3 Durable job queue | **PASS** | `notification_outbox` pattern; `cati_execution_intents` idea | `app/platform_workers/jobs.py` | `test_job_queue.py` 30 (both dialects): crash before/after claim and submit, lost ack, timeout, duplicate delivery, takeover, out-of-order, poison, notification retry, delayed comparison, restart, no open transaction | load test: 0 duplicates at 1,000 jobs | `fea1bc8` | No production handlers are registered yet. Notifications still use the existing outbox. | Register the reconciliation and shadow-comparison handlers in the trading worker on staging |
| 5.4 Signal once, size per account | **PASS** | `daily_trend.market_stage` / `decide`, `performance.synthetic_market` | `trading_intelligence/scale/{fanout,measure_fanout}.py` | `test_signal_fanout.py` 6 | `evidence/step5/signal_fanout.json`: shared signal 15.9–18.1 ms at 1–1,000 accounts (max/min 1.14); sizing 0.2 ms per account; threads give no speed-up (CPU-bound) | `fea1bc8` | None for engineering | Repeat on the Linux staging host |
| 5.5 Deterministic order spreading | **PASS** (mechanism) | venue cost model depth bounds (5 s, 5 levels) | `trading_intelligence/scale/order_spreading.py` | `test_order_spreading.py` 18 (both dialects) | load test: cap respected at 1,000 accounts | `fea1bc8` | The cap policy is **uncalibrated** (stored `calibrated=0`). A calibrated policy needs an approved participation and impact model (owner, EXT-LIQ-01). It is not yet wired into the execution boundary. | Calibrate from measured depth and fills, then integrate before any executable multi-account order |
| 5.6 Staging, containers, deploy, rollback | **PARTIAL** | systemd units, backup scripts, health check | `deploy/containers/*`, `deploy/staging/*` (compose, deploy, rollback, restore), `.dockerignore`, CI container job, migration CLI | `test_deployment_drill.py`; CI container job (image build passed; compose fixed after run 39027836407) | kill drill; migration drill; staging runbook | `826b634` | There is no authorized hosted staging environment and no Docker on the workstation (EXT-STG-01). | Run `deploy/staging/deploy.sh` and `rollback.sh` on an authorized host |
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
| 8.4 Security hardening | **PASS** (internal) / external **BLOCKED** | JWT, user TOTP, redaction, Fernet broker keys | admin TOTP MFA (two-step login, recovery codes, enrolment-only token); httpOnly, SameSite=Strict cookies with double-submit CSRF (bearer compat window); HKDF credential KDF with legacy decrypt; 7 defects fixed (cross-tenant disconnect, account-existence leak, refresh-as-access token, admin refresh reuse, dead logouts, token in SSE URL, missing audit row) | `test_section_h84_security.py` 49, `test_section_h84_token_type.py` 5, `test_auth_hardening.py` 59; frontend `sessionAuth`, `admin_mfa` | `docs/security/SECURITY_HARDENING_REPORT.md` (open findings F1–F9: access-token lifetime after logout, session/refresh lifetimes, `npm audit` advisories, first-enrolment TOFU, ...) | `69e1b76` | External penetration test BLOCKED (EXT-SEC-01); `npm audit` fixes not applied | Independent assessment; apply the dependency upgrades with a regression run |
| 8.5 Production operations | **PASS** (engineering) / owner **BLOCKED** | `notify_failure.py` channels, backup and offsite scripts, health check | `shared_lib/operations/*` (alerting with severity, routing, dedup, escalation; on-call; status page; secrets scan), `app/ops/alerting.py` evaluators and sinks, support desk, admin Operations and Support pages, `scripts/restore_drill.py` | alerting/status/drill 88 (both dialects), support 15, operations API 9, frontend tests | `evidence/step8/restore_drill.json`: 20/20, RTO 191 ms (SQLite) / 372 ms (PostgreSQL), plaintext secrets refused; `docs/operations/{RUNBOOKS,ALERTING,BACKUP_AND_RESTORE}.md` | `0878d61` | No on-call person (OPERATIONS OWNER REQUIRED); real off-site credentials; most alert producers not yet wired to `raise_alert`; no systemd timer for the operations tick | Owner names the rotation; wire producers; restore drill on production-like infrastructure |
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

Quarantine: 117 entries, all still failing on Linux. Batch 5 removed the 11 that covered only removed tests, leaving 106. The list is not emptied.

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
| 5 | **PARTIAL.** 5.2–5.5 PASS. 5.1 PARTIAL: the platform layer runs on PostgreSQL; the legacy runtime schema is still SQLite. 5.6 PARTIAL: artifacts and drills work in disposable environments; there is no hosted staging. The replayed 1,000-account load and worker-death recovery were measured (`evidence/step5/`). | **BLOCKED.** It needs the 1,000-account run on exchange demo with replayed data on authorized staging, plus Linux resource figures (EXT-LOAD-01, EXT-STG-01, EXT-DB-01). |
| 6 | **PASS for infrastructure.** All five items are built, in disabled mode, with synthetic tests and drills. | **BLOCKED.** It needs at least 8 weeks of genuine Stage A live results inside the expected range, no unprotected position, no unresolved mismatch and closed incidents. A certified family, promotion, venue certification and an owner release of `BUILD_PERMITS_LIVE` are prerequisites. No automatic promotion exists. |
| 7 | **PASS through the safe boundaries.** 7.2–7.4 PASS; 7.1 is a DRAFT specification. | **BLOCKED.** It needs owner decisions D1–D16, H-019 registered and frozen, passing the Step 2 standard and a matching demo run. Until then traditional markets stay disabled. |
| 8 | **PASS for test and staging readiness.** 8.1–8.6 are built. | **BLOCKED.** It needs the authoritative Section J checklist (unavailable; a provisional checklist is in the gates register), counsel review, country determinations, an external penetration test, live billing verification, a named operations team and explicit owner launch approval. |
| Section I | Batch 1 was already complete. Batch 5 was removed in its own commit. The rest were not executed (see the inventory). | Batches 2–10 each need the decision or dependency named in the inventory. |
