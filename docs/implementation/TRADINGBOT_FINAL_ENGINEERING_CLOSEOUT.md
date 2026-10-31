# CosmicForge Tradingbot — final engineering closeout

Pass of 31 October 2026 on `main`. It continues [MASTER_PLAN_I_L_COMPLIANCE.md](MASTER_PLAN_I_L_COMPLIANCE.md) and
supersedes it where they differ (quarantine count, D2–D4/D8, F6, L-07, L-08, takeover figures).

**Status words.** PASS = implemented, integrated, tested, and green on the final commit's CI. PARTIAL = part done,
the rest named. BLOCKED = needs a named owner decision, external party, infrastructure or elapsed time. A green CI is
engineering evidence only. It never counts as strategy certification, venue certification, staging, a penetration
test or live operation.

**Trading safety is unchanged.**
- SHADOW_ONLY. Every family is at M0, none is certified, and DAILY_TREND/H-018 is uncertified.
- The Mandate 004 holdout is unopened.
- Limits: 0.40 % per trade; 15 % maximum entry stop.
- Flags: `BUILD_PERMITS_LIVE=False`, `LIVE_ORDER_SUBMISSION_ENABLED=false`, `BILLING_ENFORCED=false`.
- Nothing was sent anywhere real: no exchange order or mutation, no Stripe call, no production database access and
  no deployment.
- The research evaluation fingerprint is unchanged at
  `59a3b6689f63bd8e61c7db5f9251bc58162d0bdb5d37a9c866478f76dc50a7aa`, and nothing under `families/`,
  `research/` or `docs/research/` changed.

---

## 1. Baseline

- `7aa13e4865b82e84b77ec120cad8c42c47efd73a` = `origin/main` at the start, with no newer commits.
- Its CI was run [41177684638](https://github.com/ainzkoji/cosmicforge-bot/actions/runs/41177684638): all 8 jobs
  green.

## 2. Final commit

The commit that adds this document. A commit cannot cite its own CI run; the run id and the job results are in the
hand-over and in section 20 of the next record.

## 3. Branch and worktree

- Work went directly to `main` (the owner's standing rule: no new branches), with no force push and no reset.
- Three files still carry another contributor's uncommitted change, preserved byte for byte and never staged:
  `daily_feed.py`, `platform_runtime.py` and `test_step3_platform_runtime.py` (section 11).

| Commit | Content |
|---|---|
| `207f104` | Daily feed update through the shared request budget; retention independent of the cycles |
| `09976a1` | Retention test without `caplog` (CI 41194270537 had failed this one test) |
| `4537ff0` | Batch 7: replay engine, external signal gate and activity targets retired; two runner modules moved |
| `fadd901` | Quarantine 23 → 0; close-fill bug fixed; `RuntimeOwnerLoop` |
| `d0dedfe` | Batch 8 characterization tests |
| `26a46cb` | `shared_lib.core.money_math` |
| `7e3447c` | Exact money arithmetic in the runtime modules |
| `840cd47` | Read-only duplicate check for the K2 unique indexes |
| `51038a2` | K1: D2, D3, D8, strength, ratchet on rebalance-up days |
| `88f1bb9` | K1 D4: Binance create-confirm-retire; BingX refusal explicit |
| `d685ff3` | L-08 governed market-entry policy (not activated) |
| `f80369a` | F6: PyJWT; FastAPI 0.136.3 / Starlette 1.3.1; lifespan |
| `88e4392` | L-07 non-enumerating sign-up |
| `4c6a0f7` | F8 switch-off tests |
| `e6a7533` | Takeover: dead worker no longer counted as capacity; claims between accounts |
| `18b31ce` | Alert reliability; notifications off the trading loop |
| `3a15265` | Platform `pg_dump`, dead-man timer and healthcheck, static deployment tests |
| this commit | Documentation, the lease-TTL comment in `config.py` |

## 4. Legacy-runner migration (Section I Batch 7) — PARTIAL

**Done in this pass.**
- **Removed:** `decision/external_signal_gate.py` and `strategy/activity_targets.py`.
- **Moved:**
  - `runner/market_snapshot.py` → `app/market_data/`;
  - `runner/effective_policy.py` → `app/policy/`;
  - the old paths are `sys.modules` alias shims; every CATI importer uses the new path.
- **Retired:** `replay/engine.py` and `replay/fill_models.py`, with `scripts/run_historical_baseline.py`, their only
  other importer (a V2 Phase-15 script).
  - The engine drove `PaperRunner`, which only observes since `6e0b42c`.
  - Kept: the cost-model tests, `test_correlation_filter.py`, and `replay/cost_model.py` and `identity.py` (both
    inside the research fingerprint).
- **`RuntimeOwnerLoop`** (`integration/runtime_owner_loop.py`) now runs the runtime-ownership lease loop.
  - `MultiBotRunner` delegates to it and keeps the attributes its readers use.
  - The lease rules in `ops/runtime_ownership.py` are unchanged.
  - Characterization tests passed against the old code before the move and after it.
- **Bug fixed** in `PaperRunner._close_managed_position`, which the kill switch, daily close, eviction and
  `flatten_all` all use.
  - Before: a failed close-fill save after the broker had closed left the position open locally, and `flatten_all`
    reported an error.
  - Now: it logs `TRADE_FILL_PERSISTENCE_FAILED`, finishes the local close and returns `fill_persisted: False`.

**Evidence.**
- An AST import scan of `backends/` and `scripts/` finds no importer of the removed modules.
- `import app.main` succeeds.
- The production runtime (`production_runtime`, `production_execution`, `portfolio_runtime`, `platform_workers`)
  imports neither the replay engine nor the removed modules.

**What still exists, and why** (no removal authority, or live readers):
- **`runner/multi_runner.py`.** In production `run_once` returns at once; the object wraps `RuntimeOwnerLoop`.
  - Its readers are `main.runner_loop`, `_may_run_background_jobs`, the status routes, `api/bot_instances.py`,
    `ops/runtime_shutdown.py` and `ops/runtime_supervisor.py`.
  - Outside production it runs `PaperRunner` per bot (the non-production simulation).
  - Next step: point the readers at `RuntimeOwnerLoop`.
- **`runner/runner.py`.** It has no order-path use: `get_runner()` answers HTTP 410 in production.
  - It still backs 21 legacy routes in `main.py` (`/runner/*`, `/risk/*`, `/trade/close*`, `/debug/*`,
    `/strategy/signal`, `/logs/events/tail`), the reporting-only probe in `activation/cati.py`, `session_monitor.py`,
    `scripts/phase12/harness.py` and 30 test files.
  - It hosts the non-production CATI cycle (`cycle_shadow`, `cati_dispatch`).
  - Removing it needs an owner decision on the 21 routes and a new home for that cycle.
- **Dead code** for the next commit: `_run_iofs_pre_ensemble`, `_effective_iofs_mode`, the `iofs_*` attributes and
  the unused `build_hold_breakdown` import.
- **The V2 modules** governance keeps until M9 (Batch 6).

**Finding, not changed.** `product_safety/readiness_gate.py` counts `SIZING_FAILURE` audit events, but nothing has
emitted them since `6e0b42c`: CATI records a sizing refusal as a RiskDecision of family `SIZING`. That readiness
check can never fire. Fixing it tightens a gate; it is listed as engineering follow-up E-03.

## 5. Remaining Section I batches

**Batch 8 — DEFERRED (not duplicates).**
- The three `boundary_for` factories differ in:
  - boundary class;
  - authority (`AccountExecutionAuthority` / `GovernanceAuthority` + `FamilyExecutionGate` / default);
  - adapter (`BinanceExecutor` / the certified, budgeted platform adapter);
  - limits and reservation (`UserConfigurableLimits` / `AccountLimits` + `ReservationLedger`);
  - protection and recovery.
- The supposedly identical prelude differs in two ways, now pinned by 21 tests:
  1. `production_execution` also accepts the residual family id as a strategy;
  2. it refuses a client from another environment first and normalizes the environment (`testnet` → `demo`).
- Extraction would have to carry both differences as parameters, so it is not done.
- `cati_dispatch.boundary_for` is reached only through the legacy runner.
- There is still one traceable production path per account kind: CATI boundary → family gate → governance →
  go-live → capital → hard risk → submission switch → adapter certification → worker fence → protective-stop
  capability → spread gate → durable attempt → raw permit.
- No order route was added.

**Batches 2, 3, 4, 6, 9 and 10 — BLOCKED on the owner (L-04).** The 31 October deletion plans in
[SECTION_I_REMOVAL_INVENTORY.md](SECTION_I_REMOVAL_INVENTORY.md) still hold. They were re-checked against the
removals of this pass:
- `app/backtest`, `app/shadow`, the nine callerless routers, `ml/scorer.py`, OANDA / IBKR / MetaTrader, news and
  Signal Center all still have readers;
- no further cleanup is unquestionably authorized.

## 6. Quarantine — PASS (engineering)

**23 → 0.** `.github/known-test-failures.list` holds no test id. Nothing was skipped, xfailed or excluded.

| Disposition | Entries |
|---|---|
| Retired with the replay engine (`test_replay_production_parity`) | 13 |
| Rewritten as behaviour tests (`test_logging_harden` 2, `TestTradeFillPersistence` close path, `test_fill_resolution_and_slots`, `test_paper_live_parity`) | 5 |
| Retired with code removed in `6e0b42c`, each with the active CATI test covering the capability (open fill path, HTF halving, IOFS enforce order, hold-breakdown log, sizing-audit source) | 5 |

Per-test detail: [QUARANTINE_REGISTER.md](QUARANTINE_REGISTER.md). J-07 is satisfied on the engineering side.

## 7. The sizing and stop deviations (Section K1)

| Item | Kind | Resolution | State |
|---|---|---|---|
| D1 percentage budget read as USDT | defect | fixed earlier (`cffa9ed`) | PASS |
| D2 no liquidation or position-mode gate | defect | refusal `LIQUIDATION_TOO_CLOSE` above `1/(2d + LIQUIDATION_DEDUCTION)` by margin mode; `POSITION_MODE_UNSUPPORTED` for hedge mode | PASS |
| D3 caps measured on equity | defect | per-trade clamp, open risk and gross leverage on `min(budget, equity)` | PASS |
| D4 ratchet inert on Binance and BingX | venue | Binance: create-confirm-retire (section 8); BingX: explicit refusal | PARTIAL (venue certification) |
| D4 ratchet skipped on rebalance days | defect (rule I10) | rebalance up: the intent carries the ratcheted stop, sized at the wider resting stop | PARTIAL: rebalance down still waits for the next close (needs a `pipeline.execution` change, E-01) |
| D5 legacy presets refused, not resized | policy | unchanged | owner |
| D6 no `max_slippage_bps` on DAILY_TREND | policy (frozen cost model) | unchanged; risk at the executable price is still bounded | owner |
| D7 initial stop anchored to the decision close | document | unchanged; bounded by `_executable_risk_refusal` | documented |
| D8 15 % stated in three places | engineering | equality test over `spec.py`, `SystemLimits`, `risk_sizing`, `AccountLimits`, executable levels; 0.40 % at one value | PASS |
| D9 `ATR_RISK_CAP_EXCEEDED` cannot fire on the plan path | document | redundant with the portfolio caps | documented |
| Strength outside 0..1 | defect | `DECISION_STRENGTH_INVALID` | PASS |
| Missing bar or mark price codes | gap | tested and registered | PASS |

**Rebalance-up reservation note.** On a rebalance-up that carries a ratchet, the increase is sized at the wider
resting stop, but its reserved risk is stated at the order's tighter stop. Stating it at the resting stop instead
would let the executable-price check admit prices above the reference.
- While the order is in flight, open risk is undercounted by `quantity × (ratchet − resting stop)`.
- The account lease admits one decision per account, so no second decision can use that gap.
- Once filled, the whole position's stop tightens, which more than offsets it.
- The ratchet was kept: skipping it would leave the exchange stop wider than the rule for a day.
- To remove the ratchet instead, delete the single `_with_ratchet` line in `pipeline/daily_trend.py`.

**Risk-sizing requirements.** All are verified on the portfolio path:
- fixed and percentage budgets, free capital (margin factor) and existing exposure (D3);
- per-trade risk, total open risk, maximum notional and position count;
- exchange minimums (refused, never raised), step rounding (round down; exact integer division in the money helper),
  leverage and liquidation distance (D2);
- worst executable price (`_executable_risk_refusal`; on L-08 also the walked book), and partial fills (stop
  coverage and slot shrink).

## 8. Binance and BingX trailing stops

**Binance USDⓈ-M** (documentation read 2026-10-31):
- Sources: [change log](https://developers.binance.com/docs/derivatives/change-log),
  [New Algo Order](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order),
  [FAQ](https://www.binance.com/en/support/faq/detail/360039707291).
- What they say:
  - conditional orders live on the Algo service since 2025-12-09;
  - an untriggered conditional order cannot be modified;
  - an algo stop is either `closePosition` or `reduceOnly` + `quantity` (the latter not in hedge mode);
  - at most 10 conditional orders per symbol.
- What the code does now:
  1. create a reduce-only stop for the whole quantity at the new trigger;
  2. read it back resting and covering the position;
  3. only then cancel the old stop (a failed cancel leaves both resting and is retried);
  4. run a second journaled create-confirm-retire at the same trigger to restore the whole-position form.
- Every step is journaled before it is taken and resumes after a restart.

**BingX perpetual swap.**
- `/trade/amend` changes quantity only.
- `/trade/cancelReplace` cancels first, so a refused replacement leaves no stop.
- Nothing documents a second reduce-only stop accepted beside the first.
- So BingX keeps `REPLACEMENT_NOT_POSSIBLE_SAFELY`: the existing stop stays and is never widened. Certification step 7
  now checks that the first stop survives.

**No application-only stop** substitutes for an exchange stop anywhere.

**Tests** (fake venue, both protocols):
- advancement, no widening, partial fill, and a native stop fill during replacement;
- replacement, out-of-order fills, user-stream disconnect with REST reconciliation, ambiguous cancellation;
- a crash at every venue call, a duplicate key, refusals, and 429 backoff.

**The Binance path rests on documentation:** it is trustworthy only after a venue certification run (EXT-VENUE).

## 9. Single-account market-entry policy (L-08) — mechanism PASS, activation BLOCKED (owner)

**Storage.** Platform migration 61, `platform_market_entry_policy_events`:
- append-only (triggers on both dialects) and hash-chained, verified at every resolve;
- one active record per venue/symbol/environment(/account);
- the schema refuses a missing `approval_ref`, `calibrated ≠ 0` and any scope other than `SINGLE_ACCOUNT`.

**Terms** can only be equal to or tighter than `CONSERVATIVE_DEFAULT`; calibration keys are refused; LIVE can be
neither recorded nor resolved in this build.

**API.** Admin emergency token only (401 / 403 otherwise). There is no order route.
- `GET /api/v1/admin/order-spreading/market-entry-policies` and `GET …/history`;
- `POST …` (record) and `POST …/{policy_id}/revoke`.

**Check order.**
1. Unchanged gates first: family certification and promotion, governance and kill switch, go-live, capital, hard
   risk, submission switch, adapter certification, worker fence, native reduce-only stop.
2. Then, inside the spread gate, each with its own code:
   - a fresh depth stamp;
   - an intact, active, unexpired, in-scope record, with its tighter depth terms;
   - spread, then account eligibility;
   - the book walk, then predicted slippage;
   - risk at the worst walked price (≤ 15 % stop distance, ≤ 0.40 % of equity; refused, never resized);
   - the atomic symbol-budget slot.

**Tests** cover each required scenario:
- one eligible account under a test-only record;
- two concurrent accounts (the second is refused `ORDER_SPREADING_CALIBRATION_REQUIRED`, including a 4-thread race);
- the shared-symbol cap; stale, thin or changed depth; spread; slippage;
- partial fill, unknown submission, worker takeover, reconciliation, the limit-to-market fallback and the kill
  switch;
- certified against uncertified family, disabled live, revoked and expired records, and API authentication.

**Activation state.** No record exists; every production market entry is still refused
`MARKET_FALLBACK_DISABLED_BY_POLICY`.

**Owner notes:**
- a plan sized to exactly 0.40 % at the reference price is refused as soon as the ask is above the reference;
- revocations are not anchored outside the database (a restored old backup could revive a revoked record).

## 10. Liquidity calibration — BLOCKED (EXT-LIQ-01)

- No impact model is calibrated and `calibrated=True` cannot be set by any code path.
- Calibration evidence keeps accumulating per entry attempt in `platform_spread_observations`; there are no real
  attempts today.

## 11. Daily-feed integration issue — PASS (engineering)

**The defect** (verified in current source):
- The contributor's change removes `budgeted_archive_get` from `platform_runtime.py`, so
  `ConnectedPortfolioRuntime._market`, `runtime_jobs.shadow_source` and the pipeline CLI would fail with
  `ImportError`.
- It makes `daily_feed.py` import the order-path HTTP budget, which fails
  `test_the_feed_cannot_reach_the_research_dataset_an_order_or_a_strategy_evaluation`.
- It deletes the routing test.

**The real gap behind it:** the `daily_feed update` CLI (`daily_feed.py:1056` at baseline) read `fapi.binance.com`
without the shared request budget.

**Repair** (`207f104`, a separate clean commit):
- The update now runs from `python -m app.trading_intelligence.pipeline feed-update`, which passes the budgeted
  getter. `daily_feed update` refuses with exit 2 and points there; `status` is unchanged.
- The feed module stays free of order-path imports. The guard test needs no exception.
- Also: `ShadowScheduler.tick` runs retention in a `finally`, at most once a day; a retention failure is logged and
  retried on the next tick.

**The contributor's change:**
- not committed and not discarded; the remaining diff of the three files is byte-identical to the original patch;
- it conflicts with the committed code: it would still break the three importers, so it should be discarded by its
  owner (its intent is met).

## 12. Decimal arithmetic — PARTIAL

**Converted** (Decimal inside, one float conversion on output; stored values untouched):
- `production_execution`: account risk and periods, certification PnL, fill VWAP and exit quantity;
- `portfolio_runtime.close_brakes`;
- `metrics/performance`;
- `analytics` (tax report, PnL, equity snapshots) and `api/equity`;
- `risk/capital_ledger`, `risk_budget`, and the money lines of `adaptive_daily_budget`.

**Boundary bugs fixed:**
- the daily-loss latch missed an exact cap hit (124.99999999999999 < 125), so an account could trade past its hard
  cap;
- an exact 20 % concentration was refused;
- `0.1 + 0.7 < 0.8` in the exit-complete check.

**Already exact:** CATI sizing (`risk_sizing`), exchange quantization, `cati_read_model`.

**Left, each with its reason:**
- the legacy executor and fill-evidence modules (`execution/executor.py`, `paper_executor.py`,
  `evidence/fill_bridge.py`, `position_reconciliation.py`, `trade_fills.py`): large; a dedicated pass is
  follow-up E-02;
- SQL `SUM` aggregates in read models;
- `pipeline/brakes.py`: mirrors the research evaluator, so parity is required;
- `pipeline/portfolio.py`: quantities are rounded down to the step already, and its 1e-9 tolerance only absorbs float
  noise on exact multiples.

**Tests:** small balances, fractional quantities, large notionals, 900 fees of 0.1, signed funding, the precision
round trip, restart, reconciliation sums. Both dialects.

## 13. Database migration evidence

On a disposable PostgreSQL 18 cluster with C locale, and synthetic sources only:
- **Platform drill** (`scripts/migration_drill.py`): 13/13 checks; 264 tables, 5,883 rows; versions 1–61.
- **Legacy drill** (`--legacy`): 13/13 checks; 298 tables, 1,515 rows; 1,655 money values equal as Decimal; 57
  foreign keys with 0 orphans; restore hashes equal; 0 rows on a rerun.
- **`scripts/check_unique_index_candidates.py`** (read-only):
  - clean on the synthetic copies;
  - refuses the operator database and non-`cftest_*` PostgreSQL databases without `--owner-authorization`.
- **Finding:** the one-active-grant unique index cannot be applied as written. An expired grant is never revoked, so a
  re-granted user legitimately has two unrevoked rows. Owner choice: revoke on expiry, or change the key.

A production-copy migration remains BLOCKED (EXT-DB-01, owner). No real record was read or changed, and no table was
dropped.

## 14. F6 and F8

**F6 — PASS for the services; pip OPEN-OPERATIONAL.**
- FastAPI 0.128.0 → 0.136.3 and Starlette 0.50.0 → 1.3.1. 0.136.3 is the last release before `app.routes` stopped
  being flattened, which breaks the security route walk.
- The bot backend's 12 `on_event` handlers run through a lifespan, in the same order and with the same refusal.
- python-jose and ecdsa are replaced by PyJWT behind one shim. This is stricter: python-jose ignored
  `require`, so a signed token without `exp` never expired.
- `pip-audit` per service requirement set: 13 → 0 advisories.
- Left: pip 24.0 of the venv toolchain (install time only). The image now upgrades to pip 26.2.2; the operator
  must do the same in the server venv.
- mt-bridge pins wait for Batch 9.

**F8 — engineering PASS; cut-over BLOCKED (operator).**
- Both frontends use cookies only; the MT connector uses pairing codes; the bot backend's service bearer is
  unaffected.
- `AUTH_BEARER_COMPAT=false` is tested both ways in both backends.
- The default stays `true` until the operator's observation window ends.
- Switch-off and rollback: [SECURITY_HARDENING_REPORT.md](../security/SECURITY_HARDENING_REPORT.md) section 6.11.
  Rollback means setting the flag back to `true` in both env files and restarting; nothing is migrated and no
  session is lost.

**L-07 — PASS.** `/register` gives the same answer for every address:
- an existing address gets a decoy id and does similar hashing work;
- no account is created, and its owner is notified by mail;
- `/verify-email` gives the generic answer.

## 15. Historical credential rotation — OPERATOR-REQUIRED

- The procedure ran end to end against a throwaway database, 23/23 checks, and a test was added. The checks:
  - a password argument is refused and nothing is printed;
  - the old password stops working and every session is revoked;
  - `token_version` is bumped;
  - an MFA bootstrap is issued once and stored as a hash.
- The MFA reset script is `backends/user-backend/scripts/security_maintenance.py`.
- **Nothing was rotated.** Every account named in `db4580b`, `cc13da0` and `b35c198` stays compromised until the
  operator runs the procedure on every environment.

## 16. Runtime alerting — PASS (engineering); delivery channel and on-call BLOCKED (EXT-OPS-01)

**Producers.** Each exists and is wired into the operations tick: API outage, governance bypass, rejected Stripe
signatures, worker failure, queue backlog, exchange outage, reconciliation, unprotected positions, backup age and
status, and service health.

**New in this pass:**
- the platform `pg_dump` status;
- the tick dead-man (`--check-tick`, timer, compose healthcheck, `OPERATIONS_TICK_STALE`);
- `evaluate_alert_delivery` (failing or overdue deliveries);
- provider receipts, so a silent rejection counts as a failed attempt.

**Order path.**
- Notification jobs ran inline in the trading loop; they now run on a background thread.
- A test runs the real emergency flatten and takeover recovery while every provider hangs, and both finish.

**Reliability tests:** failing producer, database outage, duplicates across processes, missing provider, restart,
escalation, recovery.

**On-call** stays `OPERATIONS_OWNER_REQUIRED`. Detail: [ALERTING.md](../operations/ALERTING.md).

## 17. Deployment and rollback — engineering PASS; hosted staging BLOCKED (EXT-STG-01)

**Static tests** (`tests/deploy/test_deployment_readiness_static.py`, 15) prove the staging composition has:
- the PostgreSQL platform database and a migrate step;
- two API-only replicas and one trading process;
- the emergency path, SIGTERM grace, restart policy, volumes, secrets from files, trading flags off;
- nginx headers.

**Added:**
- the scheduled platform `pg_dump` with verification;
- the dead-man timer;
- `ProtectSystem=strict` on the three units whose write paths are known.

**Drills.** The Python drills (drain and handover, rollback handover, queue consumer kill) pass on both dialects.
The containers CI job builds, migrates and smoke-tests the images.

**Never executed:** the systemd units on Linux, and a hosted staging run.

**Open:**
- no off-site copy of the platform dumps;
- no staging backup schedule.

## 18. Worker takeover

**Root cause.** The earlier "lease plus one successor round" explanation was wrong.
- A dead worker counted as live capacity for 2 × TTL, so the successor, already at its fair share, took nothing.
- In production (TTL 60 s) that meant about 120 s plus a round.

**Fix** (fencing unchanged):
- an expired, unrenewed holder no longer counts as capacity;
- a busy successor claims between accounts, at most every TTL/6.

40 accounts, 3 s TTL, this workstation, from kill issued:

| | SQLite before → after | PostgreSQL before → after |
|---|---:|---:|
| every account owned by the successor | 6.1 → 3.7 s | 7.9 → 3.8 s |
| every account recovered | 7.9 → 5.0 s | 9.6 → 5.5 s |

- "Before" was re-measured today from a baseline export. The old 12.7 s PostgreSQL figure was not reproduced (7.9 s
  today).
- Takeover is TTL + about 0.7 s, **not within the bare 3 s lease**.
- In production (TTL 60 s, idle wait ≤ 30 s between rounds) the bound is TTL + one account step + at most one idle
  wait. That is reasoned from the code, not measured.
- Evidence: `evidence/step5/broker_cycle_takeover_baseline_7aa13e4.json`,
  `evidence/step5/broker_cycle_takeover_mid_round_claims.json`.

## 19. Full test results

**How the regression was run.**
- A pristine export of the final code: `git archive HEAD` plus every change, without the contributor's patch. It
  was verified identical, apart from line endings, to the committed tree.
- A fresh venv with the new pins (no python-jose or ecdsa installed).
- CI's environment, `-p ci_quarantine` (the list is empty), and a disposable PostgreSQL 18 cluster for the
  both-dialect and legacy-runtime suites.

| Suite | Result |
|---|---|
| Bot backend (SQLite + PostgreSQL platform tests) | **8,075 passed, 0 failed, 0 xfailed**, 37 skipped (Linux-only or opt-in measurement cases), 1 h 17 min |
| Legacy runtime on PostgreSQL | **1,212 passed, 0 failed**, 3 skipped, 11 deselected (the SQLite-only list) |
| User backend | **892 passed, 0 failed** |
| Admin backend | **98 passed, 0 failed** |
| Shared library | **299 passed, 0 failed** |
| Root unit tests | **7 passed, 0 failed** |
| Python lint (CI ruff rules over all backends, shared and scripts) | all checks passed |
| User frontend | eslint 0 errors (210 warnings, unchanged), 91/91 tests, production build OK |
| Admin frontend | unchanged in this pass; covered by CI |
| Containers (build, migrate, API smoke, new dead-man step) | CI only (no Docker here) |

The focused evidence per area is in sections 4–18. **Windows runs do not certify Linux:** the Linux evidence is the
final commit's CI.

**Safety invariants, each covered by a test that runs in the suites above:**
- uncertified strategies cannot submit (`test_step3_no_uncertified_orders`, L-08 uncertified family);
- M0 cannot be promoted implicitly (family governance suites);
- live stays disabled (K4 startup refusal, go-live gate, L-08 LIVE refusal);
- the holdout stays unopened (research governance tests; fingerprint unchanged);
- the ceilings are unchanged (D8 equality test);
- stops are never silently removed or widened (stop replacement suite);
- tenant isolation holds (`test_step1_closing_synthetic::test_f_*`, SSE replay isolation);
- revoked permissions are not resurrected (`test_section_h84_session_revocation`, token-version tests);
- uncalibrated concurrent orders stay refused (L-08 two-account and race tests);
- a new worker assignment cannot double-submit (`test_step5_broker_cycle_assignment`, takeover latency tests).

## 20. CI evidence

- `207f104`: run 41194270537 failed one test (the order-dependent `caplog` assertion, fixed in `09976a1`); every
  other job and test passed (bot 7,767 passed, 23 xfailed).
- The final commit's run id and job results are recorded in the hand-over: a commit cannot contain its own run.

## 21. Unresolved owner decisions

| Id | Decision | State kept |
|---|---|---|
| L-01 | 0.40 % → 0.75 % per trade | 0.40 % |
| L-02 | Rule-based research admission route | not authorized; research policy unchanged |
| L-03 | Failed family on demo | refused |
| L-04 | Removal authority for Batches 2–4, 6, 9, 10; the 21 legacy runner routes | nothing removed without approval |
| L-05 | Placeholder pages | honest "not available yet" |
| L-06 | H-019 / D1–D16 | open |
| L-07 | Signup enumeration | **implemented** (secure default) |
| L-08 | Approve a single-account market-entry policy | mechanism ready; no record exists |
| — | Mandate 004 holdout authorization | unopened |
| — | One-active-grant unique index (revoke on expiry, or another key) | not applied |
| — | Whether to keep the rebalance-up ratchet bookkeeping (section 7) | kept |

## 22. Outstanding external gates

**Certification and staging.**
- Strategy certification: Mandate 004 holdout, demo run, 8 live weeks.
- Venue certification on Binance, Bybit and BingX, including the new Binance stop replacement.
- Liquidity calibration (EXT-LIQ-01).
- Hosted staging and the first Linux run of the systemd units (EXT-STG-01).
- Production-copy migration (EXT-DB-01).

**Operations and security.**
- Alert delivery channel and on-call person (EXT-OPS-01).
- Penetration test; kill switch and flatten drills in an authorized environment (J-06).
- F8 cut-over after the observation window.
- Historical credential rotation on every environment.
- pip upgrade in the server venv.
- Off-site copy of the platform dumps.

**Business.**
- Counsel and licensing (J-09).
- Billing activation (`BILLING_ENFORCED`).

**Engineering follow-ups** (not external, recorded for the next pass):
- E-01: the ratchet on rebalance-down days (`pipeline.execution`).
- E-02: exact arithmetic in the legacy executor and fill-evidence modules.
- E-03: the dead `SIZING_FAILURE` readiness check.
- E-04: the Batch 7 dead IOFS code.
- E-05: point the `MultiBotRunner` readers at `RuntimeOwnerLoop`.
