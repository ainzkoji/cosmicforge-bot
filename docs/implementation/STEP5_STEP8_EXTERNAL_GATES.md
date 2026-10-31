# Steps 5–8: external gates and owner decisions (closure register)

Updated 17 October 2026. EXT-DB-01, EXT-WRK-01 and EXT-LIQ-01 updated 30 October 2026 (see the [engineering closure](STEP5_STEP8_ENGINEERING_CLOSURE.md)). The Section J launch checklist and EXT-J-01, EXT-SEC-02 and EXT-OWN-I updated 31 October 2026 (see [MASTER_PLAN_I_L_COMPLIANCE.md](MASTER_PLAN_I_L_COMPLIANCE.md)). This register grants no trading or commercial authority. Every entry is a dependency that
engineering cannot close. None of them may be replaced by simulated elapsed time, synthetic fills, a manual PASS, or
a test that bypasses the unavailable dependency. The Step 3/4 register ([STEP3_STEP4_CARRY_FORWARD.md](STEP3_STEP4_CARRY_FORWARD.md))
still applies in full. Its gates H-RESEARCH, H-PROMOTION, H-VENUE, H-BINGX, H-SHADOW-7D/28D, H-THROUGHPUT and
H-CLOSE-EVIDENCE are prerequisites of several entries below.

Gate types: certification separation (C), owner decision (O), external provider (P), elapsed time (T), infrastructure (I).

| ID | Type | Blocks | Dependency / owner | Required action and acceptance evidence |
|---|---|---|---|---|
| EXT-DB-01 | I/O | Running production on PostgreSQL | Owner + operator | Engineering is done: the legacy runtime runs on PostgreSQL through the compatibility layer, and the legacy drill passes 13/13 on a synthetic source. Remaining: `scripts/migration_drill.py --legacy --source <sanitized production copy> --owner-authorization <ref>` with every check passing, on authorized infrastructure, then the owner's decision to switch `DATABASE_URL`. |
| EXT-WRK-01 | I | Multi-worker production broker cycle | Owner: staging host | Engineering is done (`BROKER_CYCLE_ASSIGNMENT_MODE`, default `owner`). Remaining: run `scripts/worker_kill_drill.py` in orchestrator mode against the real cycle on the staging host with `assigned` mode enabled. |
| EXT-STG-01 | I | Step 5.6 certification, the 1,000-account demo load | Owner: authorized staging host | Provide a host with Docker. Run `deploy/staging/deploy.sh <tag>`, a forced failure (rollback), `rollback.sh` and `restore.sh`. Keep the logs in `deploy/staging/.deploy-state`. |
| EXT-LOAD-01 | C/I | Step 5 closure | Venue/infrastructure owner | The 1,000-account test on **exchange demo** accounts with replayed data, worker death mid-cycle, Linux resource figures. The synthetic results (`evidence/step5/load_test.json`) do not satisfy it. Needs H-VENUE and H-THROUGHPUT. |
| EXT-LIQ-01 | O | Calibrated order-spreading capacity; any production entry | Owner / research | The budget is enforced at the execution boundary. Under the default policy (`allow_market_fallback=False`) every production entry, which is a MARKET order, is refused. First: approve a single-account policy that permits market entries under the conservative cap. Then: approve a participation-of-depth and impact model from the collected `platform_spread_observations` (a policy with `calibrated=True` needs an `approval_ref`). Until then budgets stay `calibrated=0` and multi-account spreading is refused. |
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
| EXT-SEC-02 | O | Production secrets | Operator | Real `SECRET_KEY`, `CREDENTIAL_KEY`, `BROKER_SECRET_KEY` (at least 32 characters; production now refuses shorter), `KYC_ENCRYPTION_KEY` (at least 32), `LEGAL_EVIDENCE_HASH_KEY`, `TRACK_RECORD_PSEUDONYM_KEY`. Administrator passwords from Git history (`db4580b`, `cc13da0`, `b35c198`) rotated with `create_admin.py <email> --reset-password`, then MFA reset and re-enrolled (procedure: MASTER_PLAN_I_L_COMPLIANCE.md section 5.3). Record the date and operator here. |
| EXT-OPS-01 | O | Public launch | Owner | A named on-call rotation (today: OPERATIONS OWNER REQUIRED), off-site backup credentials, a restore drill on production-like infrastructure, help-desk staffing. |
| EXT-J-01 | O | Step 8 closure | Owner | The Section J launch checklist J-01…J-09 below. It replaces the provisional checklist of 17 October. Every item needs its external, elapsed or owner part before it is accepted. |
| EXT-OWN-I | O | Section I Batches 2, 3, 4, 6, 9 and 10 | Owner | Approve or reject each deletion plan in [SECTION_I_REMOVAL_INVENTORY.md](SECTION_I_REMOVAL_INVENTORY.md) (31 October update): REM-01 for Batches 2–4; amend or keep the M9 rule for Batch 6; the traditional venue (H-019 D1/D3) for Batch 9; the news and Signal Center product surfaces for Batch 10. |
| EXT-F8-01 | O | Removal of the authentication compatibility window | Operator | No in-repository client needs bearer headers or URL tokens. Log their presence (never the value) for an observation window. Then set `AUTH_BEARER_COMPAT=false` in the user and admin backends. |
| EXT-LAUNCH | O | Public launch | Owner | Explicit public-launch approval after every entry above. |

## Section J launch checklist (J-01 … J-09)

Taken from the master plan's Section J (as quoted in the Sections I–L assignment). It replaces the provisional list
of 17 October.

Columns:
- **E**: engineering complete;
- **CI**: CI verified;
- **S**: staging verified;
- **X**: external certification or elapsed time;
- **O**: owner approval;
- **Accepted**: full acceptance.

No item is accepted. A page, an API or a passing test is not acceptance.

| Item | Requirement | E | CI | S | X | O | Accepted | What is missing |
|---|---|---|---|---|---|---|---|---|
| J-01 | Strategy passes its research test, a demo run and eight weeks of live operation | gates built | yes | — | **missing** | promotion | **No** | No family passes (H-018 uncertified; residual failed Mandate 003; Mandate 004 holdout unopened). No demo run, no live weeks (H-RESEARCH, H-PROMOTION, EXT-BETA-A). |
| J-02 | All three exchanges certified; every beta position has exchange-side protection | harness, register, protection gate | yes | — | **missing** | permits | **No** | No venue certification run (H-VENUE, H-BINGX). Protection is enforced in code (fail-safe close otherwise). |
| J-03 | Daily shadow decisions match the certified strategy rules | shadow comparisons recorded daily | yes | — | **missing** | — | **No** | Nothing is certified to compare against; H-SHADOW-7D/28D. |
| J-04 | A new user reaches a running, authorized bot without operator help; the interface shows positions, trades, fees, results, reasons and notifications | yes | yes | not run | real demo journey | — | **No** | A staging run and a real demo journey. This build has no authorized strategy to run, so a bot shows SHADOW / blocked reasons, honestly. |
| J-05 | Stop, pause and risk changes take effect according to their documented semantics | yes | yes | not run | — | — | **Engineering only** | Staging confirmation. |
| J-06 | Independent security assessment; admin MFA enforced; session tokens protected; kill switch and flatten drilled in an authorized environment | MFA mandatory in production (both backends), sessions, kill switch, flatten | yes | drills not on staging | **penetration test** | drill authorization | **No** | EXT-SEC-01, EXT-DRILL-01, EXT-F8-01. |
| J-07 | Every valid remaining test failure resolved; quarantine empty | partly (QUARANTINE_REGISTER.md) | — | — | — | Batch 7 owner items | **No** | The remaining entries test the legacy runner. |
| J-08 | 1,000-account load, PostgreSQL restore, off-site backups, staging, monitoring, reachable on-call, runbooks | synthetic load, restore and backup drills, alerting, runbooks | yes | **no host** | demo-account load | on-call person | **No** | EXT-STG-01, EXT-LOAD-01, EXT-OPS-01. |
| J-09 | Approved terms, privacy and risk disclosures; licensing per served country; billing verified; accurate live track record; working support and status page | drafts, help desk, status page, track-record page (NOT_YET_AVAILABLE), billing in test mode | yes | — | **counsel, licensing advice, live track record** | billing activation | **No** | EXT-LEGAL-01, EXT-BILL-01, EXT-TR-01. |
