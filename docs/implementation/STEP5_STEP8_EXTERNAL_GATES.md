# Steps 5–8: external gates and owner decisions (closure register)

Updated 17 October 2026. This register grants no trading or commercial authority. Every entry is a dependency that
engineering cannot close. None of them may be replaced by simulated elapsed time, synthetic fills, a manual PASS, or
a test that bypasses the unavailable dependency. The Step 3/4 register ([STEP3_STEP4_CARRY_FORWARD.md](STEP3_STEP4_CARRY_FORWARD.md))
still applies in full. Its gates H-RESEARCH, H-PROMOTION, H-VENUE, H-BINGX, H-SHADOW-7D/28D, H-THROUGHPUT and
H-CLOSE-EVIDENCE are prerequisites of several entries below.

Gate types: certification separation (C), owner decision (O), external provider (P), elapsed time (T), infrastructure (I).

| ID | Type | Blocks | Dependency / owner | Required action and acceptance evidence |
|---|---|---|---|---|
| EXT-DB-01 | I/O | Full Step 5.1 | Engineering + owner | Port the legacy runtime schema (about 250 tables, raw sqlite3, REAL money) to the dialect-neutral layer, or decide to keep SQLite for the single trading process. Acceptance: `scripts/migration_drill.py --source <sanitized production copy>` with every check passing, on authorized infrastructure. |
| EXT-WRK-01 | I | Multi-worker production broker cycle | Engineering | Move `production_runtime._sync_accounts` onto `WorkerAssignments` behind a flag. Repeat `scripts/worker_kill_drill.py` against the real cycle on staging. |
| EXT-STG-01 | I | Step 5.6 certification, the 1,000-account demo load | Owner: authorized staging host | Provide a host with Docker. Run `deploy/staging/deploy.sh <tag>`, a forced failure (rollback), `rollback.sh` and `restore.sh`. Keep the logs in `deploy/staging/.deploy-state`. |
| EXT-LOAD-01 | C/I | Step 5 closure | Venue/infrastructure owner | The 1,000-account test on **exchange demo** accounts with replayed data, worker death mid-cycle, Linux resource figures. The synthetic results (`evidence/step5/load_test.json`) do not satisfy it. Needs H-VENUE and H-THROUGHPUT. |
| EXT-LIQ-01 | O | Calibrated order-spreading capacity | Owner / research | Approve a participation-of-depth and impact model from measured fills. Until then budgets are stored `calibrated=0`. |
| EXT-GL-01 | O/C | Any live entry | Owner (release decision) | `BUILD_PERMITS_LIVE` is a reviewed code constant, false in this build, and LIVE certification is refused by the certification register. Changing either is a separate reviewed release after every gate below is met. |
| EXT-GL-02 | O | Go-live gate conditions 3, 5, 9 | Owner | Set `MIN_DEMO_RUN_DAYS`, `LIVE_CONSENT_VERSION`, `STAGE_A_MAX_NOTIONAL` and `STAGE_B_MAX_BUDGET` through `POST /api/v1/admin/golive/policy`. Until then the conditions report BLOCKED_PENDING_POLICY. |
| EXT-GL-03 | I | Revocation durability | Operator | Set `GOLIVE_REVOCATION_ANCHOR` to a path on storage that is separate from the database and its backups. Revocations are refused without it. |
| EXT-BETA-A | T/C | Step 6 closure | Owner + operator | At least 8 weeks of genuine Stage A results inside the registered expected range. No unprotected position, no unresolved mismatch, every incident closed with corrective action. Needs H-RESEARCH, H-PROMOTION, H-VENUE and EXT-GL-01. |
| EXT-BETA-B | O/T | Invited-user beta | Owner | After EXT-BETA-A: invitation policy and the weekly summaries. |
| EXT-DRILL-01 | C | Live incident readiness | Owner | Repeat the 13 drills on staging. Live-money drills only with separate explicit authorization. |
| EXT-TR-01 | C | Public live track record | — | The first authorized, reconciled live day. Set `TRACK_RECORD_FIRST_AUTHORIZED_LIVE_DATE` and `TRACK_RECORD_PSEUDONYM_KEY`. The public page shows NOT_YET_AVAILABLE until then. |
| EXT-TF-01 | O | Step 7 research | Owner | Decisions D1–D16 in `research/trend_v2_tradfi/SPEC.md`. Freeze and register H-019 through the governed register. |
| EXT-TF-02 | P | Tradfi calendars and contracts | Data provider / owner | Licensed, authoritative calendars and contract specifications. The current data files are UNVERIFIED illustrations, and the guard refuses them for submission. |
| EXT-TF-03 | C | Step 7 closure | Research + venue | H-019 passes the Step 2 standard, a matching demo run is completed, and a traditional-venue execution adapter is certified. Otherwise the family stays disabled. |
| EXT-BILL-01 | P/O | `BILLING_ENFORCED` | Business owner | Real Stripe products and prices, live webhook secret, portal configuration, and one authorized live purchase / refund / dispute (`BILLING_LIVE_MODE_AUTHORIZED=true` only for that). |
| EXT-LEGAL-01 | P | Public launch | Qualified counsel | Review every legal document (all are DRAFT_PENDING_COUNSEL). Record each approval through the admin review endpoint. Determine eligibility country by country (all are UNDETERMINED). |
| EXT-KYC-01 | P/O | Identity for live trading | Owner + provider | Choose an identity provider (or keep manual review) and certify it. Schedule `expire_due_verifications` if a validity period is set. |
| EXT-SEC-01 | P | Public launch | Independent assessor | External penetration test of the portal, APIs and servers (scope: `docs/security/EXTERNAL_ASSESSMENT_SCOPE.md`). Remediate the findings. A self-run test suite is not equivalent. |
| EXT-SEC-02 | O | Production secrets | Operator | Real `SECRET_KEY`, `CREDENTIAL_KEY`, `BROKER_SECRET_KEY`, `LEGAL_EVIDENCE_HASH_KEY`, `TRACK_RECORD_PSEUDONYM_KEY`; administrator passwords from Git history rotated (SEC-01, SEC-02 of Step 1); admin MFA enrolled. |
| EXT-OPS-01 | O | Public launch | Owner | A named on-call rotation (today: OPERATIONS OWNER REQUIRED), off-site backup credentials, a restore drill on production-like infrastructure, help-desk staffing. |
| EXT-J-01 | O | Step 8 closure | Owner | Section J launch checklist: the authoritative text is not in the repository. The provisional checklist below must be reconciled with it. |
| EXT-LAUNCH | O | Public launch | Owner | Explicit public-launch approval after every entry above. |

## Provisional Step 8 launch checklist

Built from the Step 8 requirements in the assignment, not from Section J, which is unavailable.

| # | Item | State |
|---|---|---|
| 1 | Billing verified (test mode, then one authorized live verification) | test mode PASS; live BLOCKED (EXT-BILL-01) |
| 2 | Legal documents approved by counsel | BLOCKED (EXT-LEGAL-01) |
| 3 | Country-specific regulatory determination | BLOCKED (EXT-LEGAL-01) |
| 4 | Identity-verification readiness | policy and adapter PASS; provider BLOCKED (EXT-KYC-01) |
| 5 | Independent security assessment | BLOCKED (EXT-SEC-01) |
| 6 | Admin MFA | engineering: see 8.4; enrolment: operator |
| 7 | Verified backups and recovery | synthetic drill: see 8.5; production BLOCKED (EXT-OPS-01) |
| 8 | Named operations team | BLOCKED: OPERATIONS OWNER REQUIRED |
| 9 | Customer support | help desk engineered; staffing BLOCKED |
| 10 | Accurate performance disclosure | public page shows NOT_YET_AVAILABLE; methodology published |
| 11 | Strategy and venue certification for any enabled trading | BLOCKED (H-RESEARCH, H-VENUE) |
| 12 | Explicit owner launch approval | BLOCKED (EXT-LAUNCH) |
