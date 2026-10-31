# Section I: removal inventory, impact assessment and quarantine disposition

Assessed 17 October 2026 against `main` at `0fd63f7` (dependency scan of production code, frontends, scripts,
deploy files and tests; Git history; route registration; CI run 39024969808 for the quarantine). The Section I
master plan text is not in the repository. The batch scopes and approvals below are those stated in the Section H
Steps 5–8 assignment. Where the repository holds a contrary rule, it is quoted and the batch is left pending.

Only **Batch 5** was executed, in its own commit with its own CI run (see the Batch 5 record below). Every other batch
is retained for the reason given. Historical evidence, research records and database tables are retained in every case.

## Summary

| Batch | Scope | Approval in the assignment | Status | Reason |
|---|---|---|---|---|
| 1 | Unreferenced modules, one-off scripts, dumps, orphan frontend files | done | **COMPLETE** (`9fb120f`) | In history: 197 files, 20,798 deletions. Not repeated. |
| 2 | Retired backtest engine, 409 route, three portal pages | scheduled for Step 1 | **DEFERRED — OWNER DECISION** | REM-01 in the Step 1 record: "owner confirmation of prerequisites". An audit document asks to keep historical read and export. `BacktestWorker` is still started by `main.py`; list, detail and export routes are live and proxied; the portal nav links to them. Not used by the CATI research evaluator (verified: no import of `app.backtest` under `trading_intelligence/`, `app/research/` or `research/`). |
| 3 | Old shadow-trade subsystem | scheduled for Step 1 | **DEFERRED — OWNER DECISION** | REM-01. Off by default, unused by CATI and by both frontends. Still registered (`/api/v1/shadow`, 10 routes), hooked in `multi_runner.py` (guarded), and asserted by `test_api_auth_hardening.py`. The CATI `ShadowScheduler` (`integration/platform_runtime.py`) is separate and does not import it. |
| 4 | Callerless routers | scheduled for Step 1 | **DEFERRED — OWNER DECISION** | REM-01. Routers with no repository caller: `news_intelligence`, `threshold_diagnostics`, `trading_evidence`, `trading_operations`, `readiness_admin`, `shadow_routes`, `cati_simulation` (script caller), `ibkr`. `monitoring` has a proxy but no frontend caller. External or operator callers cannot be ruled out from the repository. |
| 5 | TradingView signal path | **approved** | **REMOVED** (separate commit and CI run) | Executed after the concurrent work was committed; see the Batch 5 record below. |
| 6 | Legacy V2 ML scorer | **approved** | **BLOCKED — CONFLICTING GOVERNANCE RULE** | `app/ml/scorer.py` is listed in `governance/destination_map.LEGACY_AFTER_PROMOTION` ("runtime files that become legacy AFTER promotion -- NOT deleted before M9"). `tests/trading_intelligence/test_sections23_26.py:308` asserts that every listed file exists ("retained until M9"). `ml/legacy.py` tags the scorer `OBSOLETE_AFTER_M9`. All families are at M0. Deleting it means changing an approved governance rule, which needs the owner. |
| 7 | Legacy runner | scheduled during Step 3 | **BLOCKED — LIVE DEPENDENCIES** | `runner/multi_runner.py` is the runtime loop that takes the runtime-ownership lease (`main.py runner_loop`). `activation/cati.py runtime_authority_switch()` reads the source of `PaperRunner` and `BinanceExecutor`. `destination_map.verify` reports MISSING without `runner.py`, `multi_runner.py`, `market_snapshot.py`, `replay/engine.py`, `replay/historical_provider.py` and `exchange/interface.py`. CATI imports `runner/effective_policy.py`, `market_snapshot.py`, `bot_context.py` (through `exchange/factory.py`), `strategy_framework.py`, `regime` and most of `app/replay/`. 28 test files import `runner.py`. |
| 8 | Duplicate CATI execution glue | scheduled after Step 3 | **BLOCKED — ACTIVE CALLERS** | `production_execution._process_account` and `build_plan` (hand-built residual plan) run whenever `portfolio_runtime.dispatch_account` returns None. Three `boundary_for` factories are live (`production_execution`, `portfolio_runtime`, `cati_dispatch`). The nine `LEGACY_EXECUTION_TAIL_MODULES` test modules (conftest.py:214) drive the residual path. Removing it needs the portfolio runtime to cover every account first. |
| 9 | OANDA, IBKR, IBKR-TWS, MetaTrader, `mt-bridge`, MT pairing | decide after Step 7 | **KEEP PENDING OWNER DECISION** | Step 7 built venue-neutral calendars and contracts but no traditional-market execution adapter. None of these adapters is wired into `platform_adapter.factory` (crypto only). They remain reachable through the legacy factory and the broker-connection page. Recommendation: keep until the owner selects the traditional-market venue(s) for the H-019 draft (decision D1 in `research/trend_v2_tradfi/SPEC.md`); then remove the unselected ones. |
| 10 | News, events, Signal Center | decide after Step 3 | **PARTIAL KEEP; REMAINDER PENDING** | **Keep:** `app/events` calendar sync and ingestion, `economic_events` / `event_blackout_windows` and `shared_lib/persistence/economic_events.py`. CATI reads them through `integration/context_adapters.py` (cycle_shadow, cati_dispatch, venue_context, pipeline/readiness). **Pending owner:** `app/news` (31 files), Signal Center (`app/signals`, user page `/dashboard/signals`, admin pages). These are live product surfaces, so their removal is a product decision. |

## Batch 5 record (approved, dependency-checked, executed)

Evidence of current wiring:

- `app/queue/external_signal_processor.py` is the only module in `app/queue`. It is TradingView-specific and
  unrelated to the Step 5 job queue, which is `app/platform_workers/jobs.py` and imports nothing from it.
- `multi_runner.py:938-946` imports the processor inside a block whose `_ext_enabled = False`. That is dead code
  today.
- `main.py` uses it in four places:
  - the webhook router `/api/v1/tradingview`;
  - three legacy admin routes (`/api/admin/tradingview/*`);
  - a phase-6 check;
  - the `tradingview_runtime_fingerprint` field of `/health`.
- `vps_health_check.py` must be checked for that `/health` field before it is removed.
- Elsewhere:
  - `admin_tradingview.py` (201 lines) in the user backend;
  - `api/tradingview.py` and its repository in the admin backend;
  - the `TradingView.tsx` admin page, with its route and sidebar entry;
  - 10 `scripts/*tradingview*` files (5,813 lines).

Order of the removal commit:

1. Remove the inbound webhook and the admin routes in all three backends.
2. Remove the disabled `multi_runner` block and the `/health` field, after updating its readers.
3. Remove `app/queue`, the admin page and the scripts.
4. Remove the tests dedicated to the path: `test_tradingview_webhook.py`, `test_external_signal_processor.py`,
   `test_tradingview_phase5_proof.py`, `test_phase6_limited_validation_runtime.py`, and user-backend
   `test_admin_tradingview_api.py`.
5. Edit the mixed tests case by case:
   - `test_api_auth_hardening.py` route lists;
   - `test_cati_sole_authority_surfaces.py`;
   - `test_section_b_code_fixes_b5_b8.py` (`TestExternalSignalConfidence`, `TestSmaCrossUserBotBlock`);
   - `test_crypto_signal_engine.py`;
   - `test_remaining_fixes_audit.py`;
   - `test_signal_lifecycle_update.py`;
   - admin-backend `test_admin_backend_shell.py`.
6. Remove the 12 quarantine entries that belong only to those tests.

**Keep** `shared_lib/persistence/tradingview.py` and its tables, which hold historical signal and decision data. The
legacy `migrate()` creates them and `fill_provenance.py` references them; dropping them would be a destructive
migration and is out of scope.

### What was removed

- `app/queue/` (the external-signal processor).
- The TradingView webhook router and its registration.
- The `/api/admin/tradingview/limited-status` and `/processor-status` routes, with their helpers.
- The disabled external-signal block in `multi_runner.py`. It was dead code (`_ext_enabled = False`).
- `shared_lib/tradingview_symbol_mapper.py`, used only by the webhook.
- The user-backend `admin_tradingview.py` and the admin-backend TradingView API and repository, with their
  registrations.
- The admin TradingView page, route, sidebar entry, API functions, axios client and environment flag.
- The 10 TradingView scripts.
- The five dedicated test files: `test_tradingview_webhook.py`, `test_external_signal_processor.py`,
  `test_tradingview_phase5_proof.py`, `test_phase6_limited_validation_runtime.py`, and user-backend
  `test_admin_tradingview_api.py`.
- The tests in mixed files that target the removed code:
  - `TestExternalSignalConfidence` / `TestExternalConfidence`;
  - the webhook section of `test_api_auth_hardening.py`;
  - the admin-backend TradingView route tests.
- 11 quarantine entries that covered only removed tests. The list goes from 117 to 106.

### What was kept, and why

- `shared_lib/persistence/tradingview.py`, its tables and the payload-redaction helper (with its unit test). They hold
  historical signal and decision data; dropping them would be a destructive migration.
- `reports/tradingview_phase6/`: historical evidence.
- The configuration flags and the production-profile guards that force them off. A safety guard is not removed in a
  cleanup.
- `_phase6_runtime_fingerprint` and `/api/admin/tradingview/runtime-fingerprint`. Despite the name they are the
  generic process fingerprint behind the `/health` redaction security tests. Renaming them is a follow-up.

### Verification

- Admin backend: 80 passed, 6 xfailed.
- Auth hardening: 82 passed.
- The TradingView-adjacent and runner suites: 343 passed.
- Admin frontend: type check and lint clean; 49 tests passed.
- Exact-commit CI: see the tracker.

## Batch 6 plan (blocked on the rule above)

If the owner lifts the retain-until-M9 rule for the scorer, the change set is:

- `app/ml/scorer.py` and the scorer exports of `app/ml/__init__.py`. Keep `calibrated_model.py`: CATI's
  `ml/training.py` uses it.
- The `scripts/ml` tooling dedicated to the V2 scorer: `promote_model.py`, `train_entry_model.py`, `analyze_shadow.py`,
  `retrain_pipeline.py`, `validate_model.py`, `monitor_*` and `live_gating_*`.
- Keep the four `REUSE_AS_INFRASTRUCTURE` scripts, and `watch_training_readiness.py`, which
  `jobs/nightly_dataset_builder.py:140` imports.
- `tests/test_ml_runtime_boundaries.py`, the `FEATURE_COLUMNS` case of `test_ml_event_features.py`, and the three
  ML quarantine entries.
- `test_sections23_26.py`: change the inertness assertions to absence assertions.
- The `destination_map` and `legacy.py` entries: record the removal there; do not silently drop the files.

## Test quarantine disposition

`.github/known-test-failures.list` held **117 entries** (108 bot-backend, 6 admin-backend, 3 root) before Batch 5, and
holds **106** after it. On Linux CI run 39024969808 every one of the 117 failed again (108 + 6 + 3 xfailed,
**0 xpassed**). The quarantine is therefore an
accurate list of live failures. None can be removed without fixing the test or retiring its subsystem. The
assignment says the list should be emptied after Batch 7 and before Step 6. Batch 7 is blocked (above), so the list
is **not emptied**, and nothing was skipped or broadened to make it look emptier.

| Originating subsystem | Entries | Still exists | Disposition |
|---|---|---|---|
| Legacy runner, replay, V2 capital and sizing | about 60: `test_replay_production_parity` 13, `test_capital_ledger_both_modes` 10, `test_capital_allocation_semantics` 9, `test_fill_resolution_and_slots` 7, `test_policy_side_normalization` 6, `test_strategy_loader_typeerror` 5, `TestStrategyDefault` 5, executor, paper-lifecycle, orchestrator, logging, iofs, runner breakdown, activation | yes | Retire with Batch 7 or repair. They test the runner that is still the runtime loop, so deleting them now would hide defects in supported code. |
| TradingView / external signals | 12 (`test_tradingview_phase5_proof` 7, `TestExternalSignalConfidence` 4, `TestSmaCrossUserBotBlock` 1) | yes | Remove with Batch 5 together with the implementation. |
| V2 ML pipeline | 3 (`test_ml_retrain_pipeline`, `test_ml_scheduled_promotion_safety`, `test_nightly_dataset_scheduler`) | yes | Remove with Batch 6 once unblocked. |
| Signal scheduler / Signal Center | 4 | yes | Batch 10 decision. |
| Old shadow trades / early CATI observability | 7 (`test_shadow_safety` 2, `section21`, `pre_section22`, `epoch_perf`, `performance_and_runtime_evidence`, `cati_section26`) | yes | Repair: the CATI observability tests concern supported code. `test_cati_section26_observability` is order-dependent (`caplog`). |
| Admin backend events/news shell, root ML service, analytics | 9 | yes | Batch 10 decision for events/news; repair the analytics test. |
| Other (threshold policy, system hardening, VPS runtime, phase 2a broker, Bybit integration, fill provenance, remaining fixes) | about 10 | yes | Repair individually; each concerns a module still in use. |

## Required evidence for any future removal commit

Each batch must be its own commit series with its own CI run:

1. A current dependency scan with zero production importers after the change.
2. The authorization reference.
3. Byte-identical `docs/research/**`: compare the hash before and after.
4. Unchanged evaluation fingerprint: `research/governance/mandates.source_fingerprint()`, recorded at
   `59a3b6689f63bd8e61c7db5f9251bc58162d0bdb5d37a9c866478f76dc50a7aa`.
5. Removed tests listed with their replacement coverage.
6. Quarantine entries removed only together with the implementation they covered.
7. A green exact-commit CI run.

---

# Update of 31 October 2026 (Master plan Sections I–L pass)

The summary table above is the 17 October assessment. This section supersedes it where they differ. The overall
record is [MASTER_PLAN_I_L_COMPLIANCE.md](MASTER_PLAN_I_L_COMPLIANCE.md).

## Status per batch

| Batch | Status 31 Oct | Change since 17 Oct |
|---|---|---|
| 1 | COMPLETE (`9fb120f`) | Verified in history again |
| 2 | DEFERRED — owner (REM-01) | Deletion plan below |
| 3 | DEFERRED — owner (REM-01) | Deletion plan below |
| 4 | DEFERRED — owner (REM-01) | Deletion plan below |
| 5 | REMOVED (`d16ebe1`) | Verified: no route, script or page remains |
| 6 | BLOCKED — conflicting governance rule | Conflict report below |
| 7 | **PARTIAL** | Behavioural probes (`570984e`); 4 modules removed (`129353a`) |
| 8 | DEFERRED — the factories are not duplicates | Comparison below |
| 9 | KEEP pending the owner's venue choice | Decision matrix below |
| 10 | PARTIAL KEEP; remainder owner | Classification below |

## Batch 7: done in this pass

**Corrections to the 17 October dependency list.** CATI does not import `app/strategy/regime`, and it does not import
`runner/bot_context.py`: `exchange/factory.py` is imported only by `multi_runner.py`.

**`570984e`: the runtime authority switch no longer reads source text.** `activation/cati.py`
`runtime_authority_switch()` used `inspect.getsource` on three objects:
- `PaperRunner._execute_signal_with_evidence`;
- `BinanceExecutor._execute_impl`;
- `cycle_shadow._portfolio_stage`.

Each part is now a behavioural probe:
- a scalar BUY given to the legacy runner must be refused before a trap executor is called;
- the executor must refuse an entry without a CATI boundary permit;
- the cycle's `PLAN_ROUTER` must resolve to `cati_dispatch.dispatch_trade_plans`. `PLAN_ROUTER` is a new explicit
  constant, resolved at call time.

Any error counts as missing. If the legacy runner is absent, the entry gate is no longer reported missing; the
executor permit is still required. Fail-closed tests break each part. The switch is reporting only: no order path
reads it (consumers are the engine-status route, the admin `/cati` route and market eligibility).

**`129353a`: removed** `app/strategy/router.py`, `mode.py`, `timeframe.py` and `activation.py` (1,441 lines).
- No production module, script or deploy file imported them; they imported only each other.
- The only test importer was `test_regime_ensemble.py::TestActivationEngine`, removed with them. The regime,
  risk-compression, execution-filter and ensemble tests stay.
- Evidence: an AST import scan of `backends/` is clean after the change; `import app.main` succeeds; the strategy,
  section 23–26, multi-asset and runtime-authority suites pass.

**Next Batch 7 commits** (safe, not done in this pass):
- `decision/external_signal_gate.py` (TradingView leftover) and `strategy/activity_targets.py`. Their tests were being
  repaired in the same pass.
- Move `runner/market_snapshot.py` to `app/market_data/` and `runner/effective_policy.py` to `app/policy/`, each with a
  re-export shim.
- Retire `replay/engine.py` and `fill_models.py` (no production importer). This retires
  `test_replay_production_parity` (13 quarantined) and needs the `destination_map` entry edited.

**Still blocked:**
- `runner.py`: it hosts the non-production CATI cycle and about 18 `get_runner()` callers in `main.py`; 29 test files
  import it.
- `multi_runner.py`: in production it only holds the runtime-ownership lease. A `RuntimeOwnerLoop` migration is
  designed; medium risk, needs Linux CI and staging.
- `replay/cost_model.py` and `identity.py`: inside the research evaluation fingerprint; they must not move.
- The V2 modules listed until M9 (Batch 6).

## Batch 6: conflict report

1. **The rule.** `trading_intelligence/governance/destination_map.py:94`: `LEGACY_AFTER_PROMOTION`, "runtime files
   that become legacy AFTER promotion -- NOT deleted before M9". It lists `app/ml/scorer.py` ("legacy V2 ML entry
   scorer -- never CATI authority") and twelve V2 strategy, threshold and framework files.
2. **Where it is enforced.** `tests/trading_intelligence/test_sections23_26.py::test_destination_map_is_semantically_compliant`
   asserts that every listed file exists ("retained until M9"). `trading_intelligence/ml/legacy.py` tags the scorer
   `OBSOLETE_AFTER_M9`.
3. **What M9 is.** It is not a different governance system: it is stage M9 "CLEANUP" of CATI's own phase ladder
   (`governance/phases.py`). M0–M6 are research and demo, M7 limited production, M8 full promotion; M9 needs a
   `rollback_evidence_ref` and an elapsed `migration_window`. The rule keeps the V2 code as a rollback path until CATI
   has run as sole alpha in production and the rollback window has passed. This build caps families at M6, and every
   family is at M0.
4. **Callers today.** The scorer has none in the order path. It is reachable through the user backend's mounted
   `admin_ml` router (ML dashboards) and `scripts/ml`. `calibrated_model.py` is used by CATI `ml/training.py` and stays.
5. **Conflict.** The master plan approves deleting the scorer now; the governance rule forbids it before M9. Both are
   owner-level statements.
6. **What the owner must decide.** Either amend `LEGACY_AFTER_PROMOTION` (remove the scorer entry, recorded as a
   governance change) and delete per the Batch 6 plan above, or keep the scorer until M9.

## Batches 2–4: deletion plans (ready; need REM-01)

**Batch 2: retired backtest engine.**
- Remove `app/backtest/` (6 files, 2,195 lines), the `BacktestWorker` start in `main.py`, the HTTP 409 route, the
  list/detail/export routes and their user-backend proxies, the three portal Backtesting pages with their routes and
  navigation, and `tests/backtest_replay.py`.
- Keep the backtest database tables (historical runs; export first if the owner wants the read view gone) and
  everything under `research/` and `trading_intelligence/research/`.
- Owner question: does the historical read/export view stay (an audit document asks for it)?

**Batch 3: old shadow-trade subsystem.**
- Remove `app/shadow/` (6 files, 1,763 lines), `api/shadow_routes.py` and its registration, the guarded hook in
  `multi_runner.py:1075`, and the shadow route entries of `test_api_auth_hardening.py`.
- Keep the CATI `ShadowScheduler`, shadow books, shadow comparisons and their APIs: they share only the word.
  `trading_intelligence/config.py` names `app/shadow/config.py` only in a docstring.
- Keep the `shadow_trades` table (historical).

**Batch 4: callerless routers.**
- Candidates (no repository caller), with lines:
  - `news_intelligence` (192);
  - `threshold_diagnostics` (179);
  - `trading_evidence` (311);
  - `trading_operations` (364);
  - `readiness_admin` (193);
  - `shadow_routes` (325, with Batch 3);
  - `cati_simulation` (23, script caller);
  - `ibkr` (239, with Batch 9).
- `monitoring` (573) has a proxy but no frontend caller. It keeps operational value; the owner decides.
- Before removal: confirm with operators that no external tool calls them, and check the access logs of a deployed
  instance.

## Batch 8: why it stays

The three `boundary_for` factories are not duplicates:

| | `production_execution.py:405` | `portfolio_runtime.py:66` | `cati_dispatch.py:55` |
|---|---|---|---|
| Boundary | `CATIExecutionBoundary` | `PortfolioExecutionBoundary` | `CATIExecutionBoundary` |
| Authority | `AccountExecutionAuthority` (scope, consent, lease; DEMO below M6) | `GovernanceAuthority` and the family gate | default `GovernanceAuthority` (M6) |
| Adapter | `BinanceExecutor` | platform `adapter_for`, budgeted, certified | the runner's executor |
| Checks | environment mismatch; strategy cati/FAMILY; preflight permissions | strategy cati; `AccountLimits` | no ownership or policy checks |

Merging any two changes a gate.
- `_process_account` and `build_plan` run for every account without a manageable family. At M0 that is all of them;
  this is their maintenance, reconciliation and risk cycle, and the family gate refuses their plans.
- `cati_dispatch.boundary_for` is reached only from the legacy runner (outside production) and disappears with
  Batch 7.
- The only safe consolidation is extracting the identical ownership, CATI-bot, capability and policy prelude
  (`production_execution.py:421-441`, `portfolio_runtime.py:77-89`), after characterization tests.

## Batch 9: decision matrix

| Component | Wired into the platform adapter factory? | Recommendation |
|---|---|---|
| Binance, Bybit, BingX (crypto) | yes | RETAIN (the supported venues) |
| OANDA adapter | no (legacy factory only) | BLOCKED_PENDING_OWNER: retain if H-019 D3 picks FX via OANDA, else REMOVE |
| Interactive Brokers (Client Portal) and IBKR-TWS | no; reachable from the broker-connection page and `api/ibkr.py` | BLOCKED_PENDING_OWNER: candidate for futures/equities (D3). The portal's IBKR connection test is now SSRF-guarded (`62f2ee8`). |
| MetaTrader adapters, `backends/mt-bridge`, MT pairing routes and pages | no; pairing works; mt-bridge pins carry 19 advisories | BLOCKED_PENDING_OWNER: REPLACE or REMOVE unless D3 selects MetaTrader. If it is kept, its dependencies must be upgraded first. |

No traditional-market execution adapter is certified or wired; Step 7 built venue-neutral calendars and contracts
only. H-019 (draft) is not registered or frozen.

## Batch 10: classification

| Component | Class | Reason |
|---|---|---|
| `app/events` calendar sync, `economic_events`, `event_blackout_windows`, `shared_lib/persistence/economic_events.py` | ACTIVE AND NEEDED | CATI reads them (`integration/context_adapters.py`); Step 7 needs session and blackout controls |
| `app/news` (31 files), news routers, admin news pages | OWNER DECISION | A live product surface with no CATI dependency; candidate for removal |
| Signal Center (`app/signals`, 10 files; user page `/dashboard/signals`; admin pages) | OWNER DECISION | The legacy generator is already retired (it returns BLOCKED); the pages show historical signals |
| Legacy signal scheduler | REPLACED BY CATI | No generation jobs are registered; the tests now assert that |

Tables of every class are kept until a destructive migration is approved.
