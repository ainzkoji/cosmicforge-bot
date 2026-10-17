# Section I: removal inventory, impact assessment and quarantine disposition

Assessed 17 October 2026 against `main` at `0fd63f7` (dependency scan of production code, frontends, scripts,
deploy files and tests; Git history; route registration; CI run 39024969808 for the quarantine). The Section I
master plan text is not in the repository. The batch scopes and approvals below are those stated in the Section H
Steps 5–8 assignment. Where the repository holds a contrary rule, it is quoted and the batch is left pending.

**Nothing in this document was deleted.** Section I removes nothing in this delivery, for the reasons given per batch.
Historical evidence, research records and database tables are retained in every case.

## Summary

| Batch | Scope | Approval in the assignment | Status | Reason |
|---|---|---|---|---|
| 1 | Unreferenced modules, one-off scripts, dumps, orphan frontend files | done | **COMPLETE** (`9fb120f`) | In history: 197 files, 20,798 deletions. Not repeated. |
| 2 | Retired backtest engine, 409 route, three portal pages | scheduled for Step 1 | **DEFERRED — OWNER DECISION** | REM-01 in the Step 1 record: "owner confirmation of prerequisites". An audit document asks to keep historical read and export. `BacktestWorker` is still started by `main.py`; list, detail and export routes are live and proxied; the portal nav links to them. Not used by the CATI research evaluator (verified: no import of `app.backtest` under `trading_intelligence/`, `app/research/` or `research/`). |
| 3 | Old shadow-trade subsystem | scheduled for Step 1 | **DEFERRED — OWNER DECISION** | REM-01. Off by default, unused by CATI and by both frontends. Still registered (`/api/v1/shadow`, 10 routes), hooked in `multi_runner.py` (guarded), and asserted by `test_api_auth_hardening.py`. The CATI `ShadowScheduler` (`integration/platform_runtime.py`) is separate and does not import it. |
| 4 | Callerless routers | scheduled for Step 1 | **DEFERRED — OWNER DECISION** | REM-01. Routers with no repository caller: `news_intelligence`, `threshold_diagnostics`, `trading_evidence`, `trading_operations`, `readiness_admin`, `shadow_routes`, `cati_simulation` (script caller), `ibkr`. `monitoring` has a proxy but no frontend caller. External or operator callers cannot be ruled out from the repository. |
| 5 | TradingView signal path | **approved** | **READY — NOT EXECUTED IN THIS DELIVERY** | Dependency-safe on the evidence below. Not executed because its registrations are in files the concurrent security and operations work was still changing; it needs its own isolated commit and CI run. Plan below. |
| 6 | Legacy V2 ML scorer | **approved** | **BLOCKED — CONFLICTING GOVERNANCE RULE** | `app/ml/scorer.py` is listed in `governance/destination_map.LEGACY_AFTER_PROMOTION` ("runtime files that become legacy AFTER promotion -- NOT deleted before M9"). `tests/trading_intelligence/test_sections23_26.py:308` asserts that every listed file exists ("retained until M9"). `ml/legacy.py` tags the scorer `OBSOLETE_AFTER_M9`. All families are at M0. Deleting it means changing an approved governance rule, which needs the owner. |
| 7 | Legacy runner | scheduled during Step 3 | **BLOCKED — LIVE DEPENDENCIES** | `runner/multi_runner.py` is the runtime loop that takes the runtime-ownership lease (`main.py runner_loop`). `activation/cati.py runtime_authority_switch()` reads the source of `PaperRunner` and `BinanceExecutor`. `destination_map.verify` reports MISSING without `runner.py`, `multi_runner.py`, `market_snapshot.py`, `replay/engine.py`, `replay/historical_provider.py` and `exchange/interface.py`. CATI imports `runner/effective_policy.py`, `market_snapshot.py`, `bot_context.py` (through `exchange/factory.py`), `strategy_framework.py`, `regime` and most of `app/replay/`. 28 test files import `runner.py`. |
| 8 | Duplicate CATI execution glue | scheduled after Step 3 | **BLOCKED — ACTIVE CALLERS** | `production_execution._process_account` and `build_plan` (hand-built residual plan) run whenever `portfolio_runtime.dispatch_account` returns None. Three `boundary_for` factories are live (`production_execution`, `portfolio_runtime`, `cati_dispatch`). The nine `LEGACY_EXECUTION_TAIL_MODULES` test modules (conftest.py:214) drive the residual path. Removing it needs the portfolio runtime to cover every account first. |
| 9 | OANDA, IBKR, IBKR-TWS, MetaTrader, `mt-bridge`, MT pairing | decide after Step 7 | **KEEP PENDING OWNER DECISION** | Step 7 built venue-neutral calendars and contracts but no traditional-market execution adapter. None of these adapters is wired into `platform_adapter.factory` (crypto only). They remain reachable through the legacy factory and the broker-connection page. Recommendation: keep until the owner selects the traditional-market venue(s) for the H-019 draft (decision D1 in `research/trend_v2_tradfi/SPEC.md`); then remove the unselected ones. |
| 10 | News, events, Signal Center | decide after Step 3 | **PARTIAL KEEP; REMAINDER PENDING** | **Keep:** `app/events` calendar sync and ingestion, `economic_events` / `event_blackout_windows` and `shared_lib/persistence/economic_events.py`. CATI reads them through `integration/context_adapters.py` (cycle_shadow, cati_dispatch, venue_context, pipeline/readiness). **Pending owner:** `app/news` (31 files), Signal Center (`app/signals`, user page `/dashboard/signals`, admin pages). These are live product surfaces, so their removal is a product decision. |

## Batch 5 removal plan (approved, dependency-checked, ready)

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

`.github/known-test-failures.list` holds **117 entries**: 108 bot-backend, 6 admin-backend, 3 root. On Linux CI
run 39024969808 every one of them failed again (108 + 6 + 3 xfailed, **0 xpassed**). The quarantine is therefore an
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
