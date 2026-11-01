# CosmicForge Tradingbot — pre-deployment readiness

Pass of 31 October – 1 November 2026 on `main`. It continues
[TRADINGBOT_FINAL_ENGINEERING_CLOSEOUT.md](TRADINGBOT_FINAL_ENGINEERING_CLOSEOUT.md) and supersedes it where they
differ (engineering follow-ups E-01..E-05, the rebalance-up reservation note in its section 7, the "Left" list of its
section 12, its BingX and Binance notes in section 8, and the "21 legacy routes" count). The machine-readable twin of
the matrix in section 35 is [predeploy_readiness_matrix.json](predeploy_readiness_matrix.json).

**Status words.** PASS = implemented, integrated, tested, and green on the final commit's CI. PARTIAL = part done, the
rest named. BLOCKED = needs a named owner decision, external party, infrastructure or elapsed time. FAIL = a required
result was not met. NOT RUN = no evidence exists. A green CI is engineering evidence only: it never counts as strategy
certification, venue certification, staging, a penetration test or live operation.

**Engineering state: `PRE_DEPLOYMENT_READY` (engineering only), not `LIVE_AUTHORIZED`.** The software for safe demo
and live operation is implemented, integrated and tested inside this repository. What stands between it and live
money is external evidence and owner decisions (section 27), not another software project.

**Trading safety is unchanged.**
- SHADOW_ONLY. Every family is at M0; none is certified; DAILY_TREND/H-018 is uncertified. The Mandate 004 holdout is
  unopened.
- Limits: 0.40 % per trade, 15 % maximum entry stop (D8 equality test).
- `BUILD_PERMITS_LIVE=False`, `APPROVED_LIVE_RELEASES` empty, no release manifest anywhere, `LIVE_ORDER_SUBMISSION_ENABLED=false`,
  `BILLING_ENFORCED=false`. No L-08 market-entry record exists; nothing is `calibrated`.
- Nothing was sent anywhere real: no exchange call of any kind (documentation was read on the web only), no order, no
  Stripe call, no production database, no deployment.
- The research evaluation fingerprint is unchanged at
  `59a3b6689f63bd8e61c7db5f9251bc58162d0bdb5d37a9c866478f76dc50a7aa` (recomputed on the final tree); nothing under
  `families/`, `research/` or `docs/research/` changed.

---

## 1. Baseline and final commit

- Verified baseline: `d9010c262fa5297d3bd38541774f1a4e8259ad31` = `origin/main` at the start, no newer commits. Its CI:
  run [41205866654](https://github.com/ainzkoji/cosmicforge-bot/actions/runs/41205866654), 8 jobs green.
- Final commit: the commit that adds this document. A commit cannot cite its own CI run; the run id and its job
  results are in the hand-over.
- Another contributor's uncommitted change to `daily_feed.py`, `platform_runtime.py` and
  `test_step3_platform_runtime.py` is still in the working tree, byte for byte, never staged (closeout section 11: it
  would break three importers; its intent was met in `207f104`).

## 2. Functionality inventory

| Capability | Where | State |
|---|---|---|
| Market-data acquisition, freshness | `app/market_data`, `pipeline/market_data.py`, `pipeline feed-update` | PASS (engineering) |
| CATI analysis, family evaluation, immutable decisions | `pipeline/driver.py`, `daily_trend.py`, `provenance.py` | PASS |
| Research and certification workflow | `research/governance`, `pipeline/family_registry.py`, `family_governance.py` | PASS (software); verdicts authentic |
| Account-specific sizing and portfolio management | `pipeline/portfolio.py`, `reservations.py`, `brakes.py` | PASS |
| Multi-account, multi-position execution | `pipeline/execution.py`, `platform_workers/` | PASS |
| Market and limit orders, partial fills, unknown outcomes | `pipeline/execution.py` | PASS |
| Exchange-side stops, tightening, replacement | `pipeline/protection.py` | PASS (synthetic); venue PENDING |
| Rebalance up and down with the ratchet | E-01 (section 5) | PASS |
| Risk brakes, kill switches, flatten | `pipeline/brakes.py`, `production_execution.flatten_account`, admin emergency routes | PASS (simulated drills) |
| Durable execution, reconciliation, recovery | `cati_execution_*`, `recover()` | PASS |
| Worker leases, takeover, fencing | `platform_workers/assignments.py`, `RuntimeOwnerLoop` | PASS |
| Shared liquidity coordination | `scale/spread_gate.py`, `order_spreading.py` | PASS (mechanism); calibration BLOCKED |
| Exchange rate-limit management | `platform_adapter/rate_budget`, HTTP budget | PASS |
| Customer and admin APIs | user/admin backends, `/api/v1/admin/*` | PASS (contract tests) |
| Monitoring and alerting | `ops/alert_producers.py`, `shared_lib/operations` | PASS (engineering); delivery channel BLOCKED |
| Configuration profiles | `deploy/profiles/`, `shared_lib/core/config_profiles.py` | PASS (new) |
| PostgreSQL infrastructure | `shared_lib/persistence/sql`, both-dialect tests | PASS; production copy BLOCKED |
| Live eligibility and release controls | go-live gate, `shared_lib/core/live_build.py` | PASS (refusal paths); no release exists |

## 3. Engineering defects closed in this pass

| Id | Defect | Fix | Evidence |
|---|---|---|---|
| E-01 | Rebalance-down day's ratchet waited a day (rule I10) | section 5 | `test_e01_rebalance_protection.py` |
| In-flight risk | Rebalance-up reserved risk at the tighter stop: undercount of `qty x (ratchet - resting)` | section 6 | same, `test_k1_sizing_gates.py` |
| Latent widening | A protection record kept the pre-replacement stop as its intention: a re-placed stop could come back wider | `ProtectionManager._confirmed` adopts a tighter confirmed stop | `test_the_protection_record_follows_the_tightened_stop` |
| E-02 | Ten float defects in legacy executor and fill-evidence money, one margin-release logic bug | section 7 | `test_e02_legacy_money_exact.py` |
| E-03 | Readiness gate counted a sizing event nothing emits | section 8 | `test_e03_sizing_readiness.py` |
| E-04 | Dead IOFS code in `runner.py` | removed (95 lines) | AST/grep evidence in `LEGACY_RUNTIME_DISPOSITION.md` |
| E-05 | Production runtime ownership built `MultiBotRunner` | section 17 | `test_runtime_owner_readers.py` |
| Grant rule | One-active-grant uniqueness not enforceable as an index | transactional rule (section 18) | `test_operator_grant_single_active.py` |
| Approval validity | A go-live approval requested without expiry never expired (`APPROVAL_MAX_VALIDITY_DAYS` unread) | enforced from the grant; unset blocks | `test_an_approval_is_never_open_ended` |
| Adapters | 12 contract defects, Binance/Bybit/BingX (section 10) | fixed with wire tests | `tests/exchange/*` |
| Harness | BingX STOP_REPLACEMENT could be certified by cancel-then-create; 5 harness defects | fixed | `test_venue_certification.py`, `test_certification_harness.py` |

## 4. Remaining legacy-system disposition

Full table: [LEGACY_RUNTIME_DISPOSITION.md](LEGACY_RUNTIME_DISPOSITION.md).
- The route families the earlier records called "21 legacy routes" hold **33** routes: 9 replace-with-CATI-equivalent
  (`/runner/status` done through E-05), 6 retain-as-compatibility, 6 retire-obsolete, 12 blocked-pending-owner.
- No frontend, proxy, script, CI job, deployment file or runbook calls any of them. No supported production behaviour
  is served only by the legacy runner.
- `multi_runner.py`, `runner.py`, `cycle_shadow`/`cati_dispatch`, `session_monitor`: retained for the isolated
  non-production simulation (the only non-production CATI cycle; not removed without a replacement).
- Nothing was deleted: removal authority is the owner's (L-04).

## 5. Rebalance-down protection (E-01)

- **Rule.** Research rule I10 ratchets the stop at every close. One symbol carries one intent per decision, so on a
  rebalance-down day the ratchet rides on the reduction: `daily_trend._with_ratchet` now applies to a rebalance in
  either direction (the frozen evaluator, its timing and its stop formula are untouched).
- **Authorization.** `portfolio._remaining_stop`: a reduction that states a TIGHTER stop, protective at the mark, is
  authorized with it (on the tick, toward the market only). A wider or non-protective stated stop keeps the resting
  stop; a reduction is never withheld for its stop.
- **Execution.** `PortfolioExecutionBoundary._tighten_for_remainder` moves the venue stop BEFORE the reduction leaves,
  through the same gap-free replacement every stop move uses (Bybit amend in place; Binance create-confirm-retire),
  for the whole position as it is. The reduction then only shrinks what that stop covers, and the read-back after its
  fill confirms coverage of the remainder. No interval exists in which the remaining quantity is uncovered, and the move
  does not depend on the reduction's outcome.
- **Durability.** The replacement row (`request_key=reduce|<intent_id>`) is written before any venue call;
  `recover()` finishes it after a crash, and a replay of the batch replays it.
- **Venues without a safe replacement (BingX):** refused `REPLACEMENT_NOT_POSSIBLE_SAFELY`, the resting stop stays,
  the reduction still goes.
- **Edge cases**: a venue back-off halts the run before anything is sent (the same decision continues after it); the
  stop firing during the move ends the intent `POSITION_ALREADY_FLAT` without a closing order; a stop that vanishes
  during the move closes the position by the fail-safe path; an uncertain move leaves entries blocked and the
  reduction proceeds.
- **Tests** (`test_e01_rebalance_protection.py`, every venue profile, the venue's own gap record): full close, partial
  close, rebalance up, rebalance down, partial fill, stop firing during the rebalance, disconnection, a crash at every
  venue call (both before and after the effect) with recovery, unknown order outcome, duplicate replay, replacement
  refusal, two accounts concurrently.

## 6. In-flight risk accounting

- **Before.** A rebalance-up carrying a ratchet was sized at the wider resting stop but reserved at the order's
  tighter stop: while in flight, open risk was undercounted by `quantity x (ratchet - resting)` (the lease kept a
  second decision from using it, but the record understated enforceable exposure).
- **Now.** The reservation states its risk at the stop the order FILLS UNDER (`portfolio._sizing_stop`; for an entry,
  its own stop). The executable-price check measures at that same stop (`Reservation.risk_stop_price`, derived exactly
  from the stored row, so no schema change): a price above the reference still cannot spend more risk than reserved.
  After the fill the venue snapshot measures the position at the stop that actually rests, until the replacement
  confirms the tighter one.
- So the accounted risk is never below enforceable exposure at any point of the supported lifecycle: reserved →
  submitted → partial → filled → stop confirmed. Tests: `test_a_rebalance_up_reserves_its_risk_at_the_stop_it_fills_under`,
  `..._refused_when_the_mark_rose_above_the_reference`, `test_the_reservation_states_the_risk_stop_the_order_fills_under`.

## 7. Financial precision (E-02)

Converted to exact Decimal (one float conversion on output, stored values and column types untouched):
`execution/executor.py`, `paper_executor.py`, `evidence/fill_bridge.py`, `shared_lib/persistence/trade_fills.py`, and
the SQL `SUM`s that feed a limit or a reconciliation (`entry_protection` exposure, `capital_ledger` committed margin,
`platform_adapter/reconcile.py` fill total, now summed exactly in Python).

Defects found (old → new): a lost lot in `_size_qty` (9.999 → 10.000); a leverage cap truncated at an exact bound
(9x → 10x, the existing inclusive rule); TP1 one lot short (0.006 → 0.007); trailing and break-even stops a tick loose;
the exposure guard refused an entry exactly at its limit (300.33000000000004 > 300.33); SQL totals 1.4000000000000001;
900 paper fees 189.99999999999488 → 190; position-row PnL/fees drift; and one logic bug: a partial close scaled the
committed margin by remaining ÷ ORIGINAL quantity, so legs of 0.1 of 0.3 left 6.67 instead of 10 of 30 committed
(under-counting margin still at risk). Already exact and unchanged: `position_reconciliation.py`, `fill_resolution.py`,
CATI sizing. `pipeline/brakes.py` mirrors the frozen evaluator and is unchanged by design.

## 8. Readiness gate repair (E-03)

- The sizing check reads durable `cati_risk_decisions` rows (`status='REJECTED'`, `rejection_family='SIZING'`), written
  by both CATI sizing refusal points; each counted once by `risk_decision_id`; attributed by its own bot and account,
  family from the plan (`UNRECORDED` otherwise). A missing table fails closed (`SIZING_EVIDENCE_UNAVAILABLE`). Old
  `SIZING_FAILURE` events are still counted, separately, so a period that began before `6e0b42c` is not loosened.
- Consumers: real-capital activation, `evaluate_execution_readiness`, the approve route, a new admin GET
  `/api/v1/admin/bots/{bot_id}/readiness`, and the alert `REPEATED_SIZING_REFUSALS` (≥ 3 in 24 h per account,
  self-resolving; [ALERTING.md](../operations/ALERTING.md)).

## 9. Exchange adapter status

| Venue | Contract audit (docs read 31 Oct 2026) | Synthetic | Genuine venue certification |
|---|---|---|---|
| Binance USDⓈ-M | Algo Service stops, Query Algo Order, routed private stream, error codes | PASS | PENDING (EXT-VENUE) |
| Bybit V5 | linear order filters, deprecated `tradeMode`, error codes | PASS | PENDING |
| BingX perpetual | current error numbering, v3 balance, one-way leverage, fills | PASS | PENDING |

Adapter versions are `*-1.2.0` (a certification binds one adapter version). Full documentation record:
[VENUE_ADAPTER_CONTRACT.md](../exchanges/VENUE_ADAPTER_CONTRACT.md) section 13.

## 10. Binance stop certification requirements

Create-confirm-retire (closeout section 8) plus, from this pass: a lost acknowledgement of a stop is resolved by Query
Algo Order (a fired or cancelled stop is no longer read as "not found" and is never sent twice); a cancel is read back.
A genuine run must prove on the demo host: the Algo Service and `/private` stream exist there (not documented), a
reduce-only stop rests beside a `closePosition` stop, the conditional-order limit (FAQ says 10/symbol, change log 200
in total), and the original stop firing during a replacement. Certification step list:
[EXCHANGE_CERTIFICATION.md](../exchanges/EXCHANGE_CERTIFICATION.md) section 10.

## 11. BingX capability determination

No documented gap-free stop move exists: amend is LIMIT-quantity only; "the accumulative quantity of the pending
stop loss orders cannot be greater than the quantity of open positions" (a second full-size stop is refused); and
`cancelReplace` documents no rollback when the cancel succeeds and the new order fails. **Supported-venue limitation:**
BingX keeps `REPLACEMENT_NOT_POSSIBLE_SAFELY`; the existing stop stays and is never widened; increases that would need
a moved stop are refused (`PROTECTION_CANNOT_BE_TIGHTENED`); a rebalance down keeps the resting stop. The runner's
certification can no longer certify STOP_REPLACEMENT on BingX. Demo steps 7a–7c in
[BINGX_ENVIRONMENT_CERTIFICATION.md](../exchanges/BINGX_ENVIRONMENT_CERTIFICATION.md) (7c needs an owner decision).

## 12. Bybit capability status

Amend in place (trading-stop Full mode). Partial mode adds stops only (≤ 20); atomicity of the amend is not
documented and is a certification step. Margin mode is account-level (`tradeMode` deprecated, now reported unknown).
Demo has no WS Trade; REST only. 10009 is now a region restriction (`PERMISSION_DENIED`).

## 13. Market-entry policy (L-08)

Mechanism unchanged since `d685ff3` (closeout section 9) and re-proven at scale (section 24): the admin workflow is
review (`GET …/market-entry-policies`, `…/history`), record with an approval reference (`POST`), revoke
(`POST …/{id}/revoke`), emergency-token only, append-only and hash-chained. The LIVE check now reads the two-key build
status (section 30). **No record exists; every production market entry is refused
`MARKET_FALLBACK_DISABLED_BY_POLICY`.** Owner act only.

## 14. Shared liquidity limits

At 1,000 accounts on one symbol (threads and 4 processes, `test_liquidity_at_scale.py`): the production default
refuses 1,000/1,000; an uncalibrated record admits exactly 1 and refuses 999 `ORDER_SPREADING_CALIBRATION_REQUIRED`;
under a test-only calibrated mechanism in a temp DB: shared cap exact, pending slots count, partial fills shrink slots,
cancellation and crash sweeps settle from durable state, expiry frees capacity, stale/thin/crossed depth and book-walk
limits refuse everything, deterministic fair ordering. Calibration tooling: `scripts/perf/liquidity_impact_report.py`
always reports `UNCALIBRATED`; synthetic observations are labelled `SYNTHETIC`. **Calibration: BLOCKED (EXT-LIQ-01).**

## 15. Strategy certification workflow

The software path exists end to end and needs no further coding: hypothesis registration → frozen specification →
data provenance → development and out-of-sample evaluation → multiple-testing adjustment → cost/slippage assumptions →
risk-limit verification → holdout governance → report → approval/rejection → immutable evidence → family registration
→ governance promotion (M0–M9, two-person) → revocation/demotion → demo and live eligibility (go-live gate). The
sustained-cycle test proves the real family is refused at the boundary 90/90 times (`FAMILY_NOT_CERTIFIED`). No
verdict was changed; Mandate 004's holdout is unopened.

## 16. Autonomous CATI cycle

`test_sustained_cati_cycle.py`: 30 simulated days, real `ShadowScheduler.tick` and `PlatformCycleDriver.run_due`, the
30-second maintenance step (65 per simulated day around the close) distinct from one daily decision per day, three
FakeVenue accounts, durable SQLite, crash and restart mid-day. End state: no open protection gap, no duplicate order
id, reservations all final, maximum risk 0.004000. **Simulated days are not shadow evidence.**

## 17. Account and worker integration (E-05)

Production runtime ownership is held by `integration/runtime_owner.RuntimeOwnerHost` (the lease loop only) on the
same database and session id as before; `MultiBotRunner` is no longer built in production and `app.main` no longer
imports it. Migrated readers: `runner_loop`, `_may_run_background_jobs`, `/runner/status`, `/health`,
`bot_instances.get_effective_policy`, `runtime_shutdown.quiesce`, `runtime_supervisor.observe_runtime`. Response
shapes and the lease rules (`ops/runtime_ownership.py`) are unchanged; 8 characterization tests passed before and after.

## 18. PostgreSQL compatibility

Both-dialect tests run in CI (`PLATFORM_TEST_POSTGRES_REQUIRED=1`); this pass's new modules run on both, and were also
run locally on a disposable PostgreSQL 18 cluster. Grant rule: no unique index (a partial index cannot reference
`now()`; a trigger over TEXT timestamps would depend on collation). `grant()` supersedes every grant in force inside
one serialized transaction and refuses `ACTIVE_GRANT_CONFLICT` otherwise; expired, unrevoked history stays legitimate.
Owner choice still open: revoke on expiry (then the K2 partial index applies) or another key. Production-copy migration
BLOCKED (EXT-DB-01).

## 19. Containers and deployment artifacts

Unchanged from closeout section 17 (staging compose, migrate job, API-only replicas, one trading process, emergency
path, SIGTERM grace, health and readiness checks, scheduled `pg_dump`, dead-man timer, rollback drill in CI). New:
`deploy/containers/live-release.Dockerfile`, the only path by which a release manifest can enter an image (never
used), and six profile templates in `deploy/profiles/`. Hosted staging: BLOCKED (EXT-STG-01).

## 20. Customer API acceptance

User backend suite (section 25); the portal workflow (register, verify, authenticate, identity, connect, budget, risk,
preview, deploy, decisions, positions, trades, fees, funding, PnL, equity, notifications, pause, resume, adjust, stop,
disconnect) is covered by the existing contract suites and Step 1 closing tests; simulated activity is never shown as
real trades (environment-scoped read models).

## 21. Admin API acceptance

Admin backend suite plus the new admin routes of this pass (`/api/v1/admin/golive/build`, `/api/v1/admin/bots/{id}/readiness`),
both authenticated and audited like the rest; the route-walk security test covers every route. No administrative
override can bypass research, risk or venue certification.

## 22. Security results

Internal: admin MFA, httpOnly cookies, CSRF, server-side authorization, tenant isolation, broker-key encryption,
token revocation, webhook verification, non-enumerating signup, `pip-audit` clean per service, secret-shaped
literals built at run time. External penetration test: NOT RUN (external). Historical credential rotation: operator
procedure ready, not executed.

## 23. Emergency drills

Simulated: kill switches stop entries and never exits; a killed family manages to exit; flatten during 1,000-account
load leaves everything flat with no orphan stop; takeover after a worker kill; database restart; user-stream loss;
credential revocation. **Finding (owner decision, not changed):** turning the deployment flag
`LIVE_ORDER_SUBMISSION_ENABLED` off also refuses reduce-only orders and flatten at the transport (it is the hard
"send nothing" switch). Revoking the go-live operator switch, an approval or a certification refuses only new entries
and keeps protective recovery, which is the supported revocation path. Authorized-environment drills: BLOCKED (J-06).

## 24. 1,000-account performance

`scripts/perf/account_scale.py`, synthetic FakeVenue on this Windows workstation (not venue or Linux evidence),
SQLite and PostgreSQL 18; files `evidence/predeploy/scale_{1,10,100,1000}_accounts.json`.

| Accounts | Sizing total (SQLite / PG) | Cycle latency p95 (SQLite / PG) | Takeover | DB connections peak |
|---:|---|---|---|---|
| 1 | 0.25 / 0.78 ms | 0.11 / 22.9 ms | 5.0 s | 5 / 4 |
| 10 | 14.8 / 1.8 ms | 138 / 57.8 ms | 5.1 s | 8 / 9 |
| 100 | 45 / 18.9 ms | 56.5 / 13.2 ms | 5.1–5.2 s | 11 / 13 |
| 1,000 | 375 / 372 ms | 0.2 / 26.5 ms | 7.1 s | 18 / 16 |

Shared signal computed once at every N. Failure cases at every N and both dialects: worker killed mid-round (0
orphans, stale holder fenced), database restart, user-stream disconnect, lost acknowledgement (0 duplicates),
flatten under load, queue consumer crash, interrupted deployment, stale data. **Capacity findings:** the lease TTL
must exceed one worker round with margin (a 1.5 s drill TTL lapsed under load; production is 60 s); **one egress IP
allows about 30 Binance REST reconciliations per minute**, so 1,000 accounts need egress capacity planning. Demo-venue
load test: BLOCKED (external).

## 25. Full regression results

**How it was run.** A pristine export: `git archive HEAD` of the baseline plus every change of this pass, WITHOUT the
contributor's uncommitted patch (verified identical to the committed tree apart from two documents). CI's environment
variables, `-p ci_quarantine` (the list is empty), and a disposable PostgreSQL 18 cluster (C locale) for the
both-dialect and legacy-runtime suites. Windows workstation: no Linux evidence (that is the CI run).

| Suite | Result |
|---|---|
| Bot backend (SQLite + PostgreSQL platform tests) | **8,437 passed, 1 failed, 38 skipped** of 8,476, 1 h 10 min. The failure was the timing bound `test_killed_worker_is_taken_over_within_one_ttl_and_never_mutates_again[sqlite-account]` (2.625 s against 2.6 s) while three other suites shared the CPU; rerun alone it passed 40/40 (5 runs x 8 cases). Nothing in this pass touches broker-cycle takeover; the bound was not loosened. |
| Legacy runtime on PostgreSQL | **1,228 passed, 0 failed**, 3 skipped, 11 deselected (the SQLite-only list), 37 min (baseline 1,212: the 16 new E-03 and grant tests run here too) |
| User backend | **892 passed, 0 failed** |
| Admin backend | **98 passed, 0 failed** |
| Shared library | **299 passed, 0 failed** |
| Root unit tests | **7 passed, 0 failed** |
| Python lint (CI ruff rules over every backend, shared and scripts) | all checks passed |
| Frontends | unchanged in this pass; covered by CI |
| Containers | CI only (no Docker here) |

Quarantine: `.github/known-test-failures.list` is still empty; no skip or xfail was added. The 38 skips are the baseline's
37 environment cases (Linux-only deploy matrix and systemd, the operator's private `.env` and untracked artifacts,
opt-in measurements) plus one new one: `test_live_release_static` needs a git checkout, which the export is not (CI's
checkout is, so it runs there).

Focused evidence of this pass, per area: E-01 module 56 tests on four venue profiles (and the 923-test pipeline set
unchanged); E-02 22; E-03 10 + grants 6 (both dialects); runtime owner readers 10; live build/profiles/register 201;
exchange 750; scale, liquidity and sustained cycle 60 (both dialects).

## 26. GitHub CI

The final commit's run id and job results are in the hand-over (a commit cannot contain its own run).

## 27. Genuine remaining external requirements

| Id | Requirement | Owner |
|---|---|---|
| Strategy | Mandate 004 holdout authorization and evaluation; a certified family; demo run; 8 live weeks | owner / research governance |
| EXT-VENUE | Demo certification runs on Binance, Bybit, BingX (`scripts/run_venue_certification.py --execute`) | operator, with demo accounts |
| EXT-LIQ-01 | Liquidity calibration from real observations | owner |
| L-08 | Approve a single-account market-entry policy | owner |
| EXT-STG-01 | Hosted staging; first Linux run of the systemd units | operator |
| EXT-DB-01 | Production-copy migration | owner |
| EXT-OPS-01 | Alert delivery channel and on-call person | owner |
| J-06 | Kill switch and flatten drills in an authorized environment; penetration test | owner / external |
| Release | A reviewed live release (both keys of section 30) | owner + two reviewers |
| Policy keys | `APPROVAL_MAX_VALIDITY_DAYS`, `MIN_DEMO_RUN_DAYS`, `LIVE_CONSENT_VERSION`, stage limits | owner |
| Egress | IP/egress capacity for 1,000 accounts (section 24) | operator |
| Security | F8 cut-over, credential rotation on every environment, pip upgrade on the server | operator |
| Business | Counsel and licensing (J-09); billing activation | owner |

## 28. Go-live operational checklist

1. Hosted staging up from `deploy/staging`; profile validated (`scripts/validate_config_profile.py`); migrate job
   green; drills re-run on Linux.
2. Venue certification on DEMO for each venue (`--dry-run` first, then `--execute --authorization-ref …`); evidence
   recorded in the certification register.
3. A family research-certified through Mandate governance; promoted by two operators; demo run of at least
   `MIN_DEMO_RUN_DAYS`.
4. Owner sets the go-live policy keys; L-08 decision; liquidity calibration or single-account limits.
5. Alert delivery channel and on-call live; backup off-site copy; restore drill on staging.
6. Reviewed live release: commit C0 (`BUILD_PERMITS_LIVE=True`), manifest validated by
   `scripts/release/live_release_manifest.py`, commit C1 pinning its hash, image from `live-release.Dockerfile`.
7. Operator live switch (two operators), per-account approval, customer consent, live-beta Stage A enrollment.
8. `LIVE_ORDER_SUBMISSION_ENABLED=true` on the bot backend only, by the operator; the go-live gate re-checks every
   condition before every live entry; revocation of any condition refuses new entries at once.

---

## 29. Activation gates

[ACTIVATION_GATE_REGISTER.md](ACTIVATION_GATE_REGISTER.md) (and `activation_gates.json`): 68 gates — Type A 0, B 12,
C 30, D 8, E 18. No hard-coded engineering lock remains that would block an otherwise certified and authorized
deployment; every remaining gate is a certification, risk/security, infrastructure or owner gate. Findings it
recorded: the approval-validity gap (fixed, section 3); the hard-off semantics of the submission flag (section 23);
`env.production.example` lacks `GOLIVE_REVOCATION_ANCHOR` and `PLATFORM_DATABASE_URL`; `TRADING_ENABLED` is not a
working off switch; DEMO entries skip the global phase; counsel and country eligibility are not enforced in code.

## 30. Live build capability

`shared_lib/core/live_build.live_build_status()` returns capable only with **two independent keys**: (A) reviewed
source — `BUILD_PERMITS_LIVE=True` and the manifest's SHA-256 pinned in `live_release_allowlist.APPROVED_LIVE_RELEASES`
(empty here); (B) the artifact — a manifest baked only by `live-release.Dockerfile`, with ≥ 2 distinct reviewers,
approval references, `git_commit` equal to the running build, and a source-tree hash that matches the installed code.
Fourteen stable reason codes; this build reports `LIVE_NOT_PERMITTED_IN_THIS_BUILD`. A capable build authorizes
nothing by itself: the go-live gate (17 conditions), K4 startup refusal and the L-08 LIVE check read the status, and
every certification, consent, credential, switch and revocation still applies. The release script refuses to run in
CI and writes nothing. Remaining hard LIVE locks a release must also lift: `LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD`
(certification runner), the M6 cap in `family_governance`, the tradfi LIVE lock; only the bot backend may carry the
live flag in such a release.

## 31. Demo activation

Two workflows with distinct authority: infrastructure certification (`CertificationHarness`, an `OperatorPermit` that
is DEMO-only, for the exact journalled `cert*` order of a running harness, never usable by a strategy) and strategy
demo operation (a certified, promoted family through the go-live/governance gates; a batch in the certification
mode is refused `MODE_NOT_EXECUTABLE`). Both are audited separately.

## 32. Pre-deployment certification matrix

Machine-readable: [predeploy_readiness_matrix.json](predeploy_readiness_matrix.json). PASS rows are conditional on the
final commit's CI being green.

| Category | State |
|---|---|
| CATI engine | PASS |
| Market data | PASS |
| Strategy governance | PASS (approval and refusal behaviour) |
| Risk sizing | PASS (0.40 % and 15 % boundaries) |
| Portfolio management | PASS |
| Execution boundary | PASS |
| Binance adapter | synthetic PASS; venue certification PENDING |
| Bybit adapter | synthetic PASS; venue certification PENDING |
| BingX adapter | synthetic PASS; venue certification PENDING; stop moves refused by design |
| Native stops | PASS (synthetic); per-venue capability documented |
| Worker failover | PASS |
| Durable queue | PASS (idempotency) |
| PostgreSQL | PASS (migrations and integrity on disposable clusters); production copy BLOCKED |
| 1,000-account load | PASS (synthetic replay); real venue load test BLOCKED |
| Customer APIs | PASS |
| Admin APIs | PASS |
| Security | internal PASS; external assessment NOT RUN |
| Emergency operations | simulated PASS; authorized-environment drill BLOCKED |
| Deployment assets | PASS (container, migrate, rollback drill in CI); hosted staging BLOCKED |
| Research certification | BLOCKED — actual: every family M0, holdout unopened |
| Live eligibility | PASS (gate logic); this build is not live-capable |
| Operational approval | BLOCKED — actual: no release, no switch, no policy |

**Go-live readiness:** engineering PRE_DEPLOYMENT_READY · hosted staging BLOCKED · venue certification PENDING ·
strategy certification BLOCKED · demo-trading eligibility BLOCKED (no certified family) · live-trading eligibility
BLOCKED · operational release NOT AUTHORIZED.
