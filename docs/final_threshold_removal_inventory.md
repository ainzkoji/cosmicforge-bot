# Final Threshold Removal — Repository Inventory

Frozen before any deletion.

| | |
| --- | --- |
| Branch | `phase-0-4-runtime-baseline` |
| HEAD | `049a287bc90abdb6115685cd3b9c6212e8aaad4e` |
| Working tree | clean — no modified or untracked files, no other session's work at risk |
| Frozen at | 2026-09-09T22:56:37Z |
| DB backup | `backups/cosmicforge_20260909T225655Z_pre_legacy_threshold_removal.db` (3,482,271,744 bytes, online `sqlite3.backup()` snapshot taken while the runtime was live) |

---

## Classification key

* **DELETE** — remove from the repository entirely.
* **MIGRATE_TO_NEW_ENGINE** — the value or behaviour moves into
  `AdaptiveEntryThresholdEngine` / `EffectiveThresholdPolicy`, then the source is
  deleted.
* **HISTORICAL_DOCUMENTATION** — a report describing what the system used to do.
  Left untouched; it is a record, not a control.
* **HISTORICAL_EVIDENCE** — database rows. Never deleted or rewritten.
* **UNRELATED_FALSE_POSITIVE** — matches the search string, is not an entry
  threshold.

---

## 1. Source files whose only purpose is the old threshold system

| Path | Classification | Note |
| --- | --- | --- |
| `backends/bot-backend/app/risk/dynamic_threshold.py` | **DELETE** | The entire old calculator: `DynamicThresholdCalculator`, `get_dynamic_threshold_calculator`, `log_threshold_event`, module-level `MIN_THRESHOLD`/`MAX_THRESHOLD`/`FALLBACK_THRESHOLD`/`MIN_SAMPLES`/`WINDOW_SIZE`/`PERCENTILE`. Independently calculates an entry threshold, so §3 forbids keeping it even as research. |
| `backends/bot-backend/tests/test_dynamic_threshold.py` | **DELETE** | Asserts the deleted algorithm (12 legacy-term hits). Replaced by `test_adaptive_entry_threshold_engine.py`. |

## 2. Production call sites of the old calculator

| Path:line | Classification | What it does |
| --- | --- | --- |
| `app/risk/safety_engine.py:25-28` | **DELETE** | Imports `DynamicThresholdCalculator`, `get_dynamic_threshold_calculator`, `log_threshold_event`. |
| `app/risk/safety_engine.py:216,224` | **DELETE** | `self._dyn_threshold` — a second live threshold calculator inside the safety layer. |
| `app/risk/safety_engine.py:315-318` | **DELETE** | Recomputes a threshold purely for a log line. |
| `app/risk/safety_engine.py:380-437` | **DELETE** | **Gate 3** — a complete duplicate entry-quality gate: resolves a dynamic threshold, caps it at `min_confidence_hard`, compares confidence and returns `BlockReason.LOW_CONFIDENCE`. §9. |
| `app/risk/safety_engine.py:150,154,185-194` | **DELETE** | `min_confidence_hard` / `min_confidence_soft` config fields and their normalisation/swap logic. |
| `app/runner/runner.py:745-747` | **DELETE** | `reconstruct_memory_from_db` for the rolling window. |
| `app/runner/runner.py:3844-3845` | **DELETE** | `dyn_res = dyntc.get_threshold(symbol)` feeding `base_threshold` into the adaptive engine. |
| `app/runner/runner.py:4069` | **DELETE** | Writes `dynamic_threshold` into the gate trace. |
| `app/runner/runner.py:5457-5462` | **DELETE** | Second `get_threshold` call feeding a second `get_adaptive_state`. |
| `app/runner/runner.py:6030` | **DELETE** | `_pe_gate_details["dynamic_threshold"]`. |
| `app/strategy/master_ensemble.py:56,251` | **DELETE** | `self._threshold_calc` and its `record()` call. |
| `app/strategy/router.py:32` | **DELETE** | `from app.risk.dynamic_threshold import log_threshold_event`. |
| `app/adaptive/audit_log.py:118` | **DELETE** | `affected.append("DynamicThresholdCalculator")`. |
| `app/runner/multi_runner.py:49,707` | **DELETE** | Comments describing the rolling window as live behaviour. |

## 3. Configuration keys

| Key | Classification | Where |
| --- | --- | --- |
| `MIN_CONFIDENCE_THRESHOLD` | **MIGRATE_TO_NEW_ENGINE** then **DELETE** | `config.py:204`, `.env:120`, `.env.example:120`, `safety_startup.py:133-135`, `migration.py` |
| `ENSEMBLE_MIN_THRESHOLD_FLOOR` | **DELETE** | `config.py:289`, `.env:141`, `.env.example:131`, `migration.py` |
| `DYNAMIC_THRESHOLD_MIN` | **DELETE** | `dynamic_threshold.py:76`, `.env.data_collection:54` |
| `DYNAMIC_THRESHOLD_MAX` | **DELETE** | `dynamic_threshold.py:77`, `.env.data_collection:58` |
| `DYNAMIC_THRESHOLD_FALLBACK` | **DELETE** | `dynamic_threshold.py:78`, `.env.data_collection:50` |
| `DYNAMIC_THRESHOLD_MIN_SAMPLES` | **DELETE** | `dynamic_threshold.py:79`, `.env.data_collection:67` |
| `DYNAMIC_THRESHOLD_ENABLED` | **DELETE** | `dynamic_threshold.py:73` |
| `DYNAMIC_THRESHOLD_WINDOW_SIZE` | **DELETE** | `dynamic_threshold.py:74`, `.env.data_collection:61` |
| `DYNAMIC_THRESHOLD_PERCENTILE` | **DELETE** | `dynamic_threshold.py:75`, `.env.data_collection:64` |
| `.env.backup_before_strong_trend_experiment` | **HISTORICAL_DOCUMENTATION** | A dated backup of a previous experiment. Not loaded by anything; left as the record it is. |

## 4. In-code threshold concepts

| Symbol | Classification | Where |
| --- | --- | --- |
| `consensus_threshold` | **DELETE** | `master_ensemble.py` constructor arg; `decision_engine.py` docstring |
| `consensus_required` | **DELETE** | `decision_recorder.py` field + row; `evidence_schema.py` column stays as historical evidence but is never written |
| `min_confidence_gate` | **DELETE** | `adaptive/engine.py:97,685,740`; `runner.py:5479` |
| `AdaptiveState.min_confidence_gate` | **DELETE** | The last thing that still looks like a threshold outside the engine (§7) |
| `BotContext.min_confidence` | **DELETE** | `bot_context.py:73,269`; `runner.py:1029`; §8 forbids the ambiguous name |
| `PolicyEngine.min_confidence` | **DELETE** | `policy_engine.py:411,419,663-666,1196,1205` — a confidence comparison outside the engine |
| `confidence_absolute_floor` | already removed | Retained only in `migration.py` prose |

## 5. Scripts (research / validation)

| Path | Classification |
| --- | --- |
| `scripts/validation/replay_runtime_equivalent.py` | **DELETE** the threshold portions — imports the calculator and reproduces a threshold. Superseded by `app/replay/`, which drives the real engine. |
| `scripts/validation/analyze_signal_thresholds.py` | **DELETE** legacy setting reads |
| `scripts/validation/analyze_strong_trend_block.py` | **DELETE** legacy setting reads |
| `scripts/validation/audit_runtime_replay_mismatch.py` | **DELETE** legacy setting reads |
| `scripts/validation/replay_strategy_components.py` | **DELETE** legacy setting reads |
| `scripts/validation/run_paper_cycle_diagnostic.py` | **DELETE** legacy setting reads |
| `scripts/ml/analyze_strategy_expectancy.py` | **DELETE** legacy setting reads |
| `scripts/cycle_trace.py:213-214` | **DELETE** `dynamic_threshold` trace field |

## 6. Tests

| Path | Classification |
| --- | --- |
| `tests/test_dynamic_threshold.py` | **DELETE** (whole file) |
| `tests/test_fix_e_strategy_expectancy.py` | **DELETE** the `ENSEMBLE_MIN_THRESHOLD_FLOOR` assertions (5 hits) |
| `tests/test_master_ensemble_components.py` | **DELETE** the floor monkeypatch |
| `tests/test_quant_audit_fixes.py` | **DELETE** the `MIN_CONFIDENCE_THRESHOLD` / `PolicyEngine(min_confidence=)` assertions |
| `tests/test_runtime_equivalent_replay.py` | **DELETE** the `min_confidence_gate` fixture field |
| `tests/test_adaptive_engine_validation.py` | **DELETE** the `min_confidence_gate` contract assertion |
| `tests/test_risk_execution_responsibility.py` | **DELETE**/rewrite the `min_confidence_gate` and `PolicyEngine.min_confidence` source assertions |
| `tests/test_safety_recovery*.py`, `test_section_d_risk_management.py`, `test_safety_*` | **MIGRATE** — `PolicyEngine(min_confidence=0.10)` constructor calls |
| `tests/test_confidence_gating.py`, `test_strategy_confidence.py` | **UNRELATED_FALSE_POSITIVE** — per-strategy `min_confidence` inside individual sub-strategies (SuperTrend etc.), which is signal generation, not the entry threshold |

## 7. Documentation

| Path | Classification |
| --- | --- |
| `docs/entry_threshold_provenance_report.md` | **HISTORICAL_DOCUMENTATION** — the forensic trace of the 0.70 saturation. Untouched. |
| `docs/master_ensemble_no_trade_forensic_report.md` | **HISTORICAL_DOCUMENTATION** |
| `docs/phase_5_8_implementation_report.md` | **HISTORICAL_DOCUMENTATION** |
| `docs/adaptive_entry_threshold_engine_rebuild_report.md` | **HISTORICAL_DOCUMENTATION** — describes the rebuild, including the RESEARCH_ONLY disposition that this pass supersedes |
| `backends/bot-backend/docs/ml_retraining_plan.md` | **DELETE** the stale threshold reference |
| `app/strategy/activity_targets.py:15` | **DELETE** the `MIN_CONFIDENCE_THRESHOLD` prose |

## 8. Database

| Object | Classification |
| --- | --- |
| `trading_decisions.threshold_base` / `threshold_dynamic` / `threshold_adaptive_modifier` / `threshold_regime_modifier` / `effective_entry_threshold` / `consensus_required` | **HISTORICAL_EVIDENCE** — `LEGACY_READ_ONLY_EVIDENCE_COLUMN` where the new engine no longer writes them. Not dropped: dropping them would destroy the record of what the old stack decided. |
| `trading_decisions`, `fills`, `orders`, `positions`, `position_events` rows | **HISTORICAL_EVIDENCE** — immutable |
| `threshold_decisions`, `expert_evaluations`, `adaptive_threshold_state` | keep — the new evidence model |

## 9. Not an entry threshold — explicitly excluded

| Match | Why it stays |
| --- | --- |
| `frontends/user-frontend/.../Signals.tsx`, `SignalFilters.tsx` — `minConfidence` / `min_confidence` | A **display filter** on the user's signal list, bound to the user preference `minimum_confidence` and sent as a query parameter to the signals endpoint. It filters what a human sees; it cannot influence the bot's entry threshold. No frontend control writes any bot threshold setting. |
| `app/news/news_market_validation_service.py:19` | A news-confidence constant in a docstring, unrelated to entry quality |
| `app/execution/position_manager.py` — `reentry_min_confidence` | Re-entry policy after a close, not entry quality |
| `app/execution/add_manager.py` — `min_confidence_precision/flow` | Position-add policy, not entry quality |
| `app/metrics/calibration.py` — `fallback_min_confidence` | ML calibration reporting |
| `app/news/news_intelligence_signal_service.py` — `min_confidence` | News signal filtering |
| Individual sub-strategies' `min_confidence` (SuperTrend, SMA cross, Bollinger) | Each expert's own signal-emission floor. These produce the confidences the ensemble aggregates; they are inputs to the threshold decision, not competing authorities over it. |
| `backends/admin-backend/.../ml_monitoring_repo.py:275` | A historical deployment note string in an ML monitoring record |
| `frontends/*/node_modules/**` | Third-party packages |

---

## Scope summary

* **2 files deleted outright.**
* **~14 production call sites** stripped of threshold responsibility.
* **9 configuration keys** removed from the active contract.
* **6 in-code threshold concepts** removed.
* **8 scripts** and **9 test files** updated.
* **0 historical database rows** touched.
