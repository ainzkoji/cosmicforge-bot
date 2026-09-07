# Runtime Root-Cause Audit & Repair — Auto Pilot `bot_e5fe913972a9`

Date: 2026-09-07 · Branch: `phase-0-4-runtime-baseline` · Baseline: `8a2ed071`

---

## A. Root causes

### A1. `ERROR_STRATEGY_UNAVAILABLE` — wrong `BotRunContext` attribute (PRIMARY)

* **File / function:** `backends/bot-backend/app/runner/runner.py` → `PaperRunner._load_orchestrator`
* **Defect:** the `UserConfigurableLimits(...)` construction read `self.context.max_daily_loss`.
  `BotRunContext` has no such field — it exposes `daily_max_loss_usdt`
  (`app/runner/bot_context.py`). Introduced by Phase 0-4 commit `b35c1980`.
* **Why it was invisible:** the whole method was wrapped in
  `except Exception: print(...); traceback.print_exc()`. The `AttributeError` was
  printed to console and swallowed, leaving `self.orchestrator = None` while the
  runner stayed cached in `MultiBotRunner._runners` and continued to be scheduled.
* **Runtime consequence:** `step_symbol` fell through the `if self.orchestrator:`
  dispatch into the fail-closed branch, emitting `ERROR_STRATEGY_UNAVAILABLE` for
  BTCUSDT and ETHUSDT on every ~10 s tick, indefinitely, with no persisted cause.
* **Why tests missed it:** every orchestrator test
  (`tests/test_orchestrator_context_integrity.py`) builds
  `TradingOrchestrator.__new__(TradingOrchestrator)` directly. No test ever
  exercised the `PaperRunner` → `BotRunContext` attribute contract.

**Verified reproduction before the fix** (offline, real bot row + real policy):

```
Failed to load orchestrator: 'BotRunContext' object has no attribute 'max_daily_loss'
  orchestrator: None
```

**After the fix:** `Loaded TradingOrchestrator for Bot bot_e5fe913972a9`, all five
collaborators present.

### A2. Silent partial initialization

* **File:** `app/runner/runner.py` → `PaperRunner._load_orchestrator`, `__init__`
* **Defect:** orchestrator construction failure did not stop runner registration.
  A context-bound Auto Pilot runner could live indefinitely with no strategy path.
* **Consequence:** A1 was able to persist for hours instead of failing loudly once.

### A3. Stale `last_error` contradicting current health

* **Files:** `app/runner/runner.py` → `_set_bot_health`;
  `app/core/bot_instance_service.py` → `update_bot_health`
* **Defect:** `last_error = COALESCE(?, last_error)` and "only write when
  explicitly supplied". `last_error` could never be cleared or superseded.
  Additionally `_set_bot_health` de-duplicated writes on `status` alone, so a
  changed `reason_code` under an unchanged status wrote nothing.
* **Observed:** `bot_health_reason_code = ERROR_STRATEGY_UNAVAILABLE` while
  `last_error = "Auto Pilot capital budget is missing..."` — a problem repaired
  hours earlier.

### A4. `reason_codes = NONE` on decisions that had a reason

* **File:** `backends/shared/shared_lib/persistence/trace_recorder.py` →
  `record_gate`, `finalize`
* **Defect:** `record_gate` set `gate_reason` but never `reason_codes`, which kept
  its `"NONE"` column-default sentinel. `finalize` then split that sentinel into
  the secondary-reason list, producing `secondary_reason_codes_json = ["NONE"]`.

### A5. `UnboundLocalError` from a function-local import (SECOND LIVE DEFECT)

* **File:** `app/runner/runner.py` → `_step_symbol_orchestrated`
* **Defect:** an unaliased `from shared_lib.persistence.trace_recorder import
  get_trace_recorder` at the ML-scoring site bound that name as a **function
  local for the entire method**. The earlier same-candle branch called
  `get_trace_recorder()` before that line executed → `UnboundLocalError`,
  swallowed by its `except Exception: pass`.
* **Consequence:** every same-candle heartbeat lost its gate reason and persisted
  a bare `SKIP` with `gate_reason=''`, `reason_codes='NONE'` instead of
  `NO_NEW_CANDLE`. Every sibling local import in that method was already aliased
  (`_gtr`, `_gtr_mkt`, ...); this one was missed.
* **Found by:** live evidence review after A1 was fixed, confirmed by AST analysis.

### A6. Heartbeat ticks polluted the canonical decision ledger

* **File:** `shared_lib/persistence/trace_recorder.py` → `finalize`
* **Defect:** `canonical_trade_decisions` is specified as one authoritative record
  per evaluated symbol per **new candle**, but received one row per 10 s tick.
  At 15m / 2 symbols that is ~17,000 rows/day.
* **Consequence:** `GET /bot-instances/{id}/decision-quality` reads the newest
  5,000 canonical decisions — a window covering roughly 7 hours of heartbeats and
  effectively **zero real evaluations**. This is precisely the "why did this bot
  not trade at 20:39 requires archaeology" problem.

---

## B. Files changed

| File | Purpose |
|---|---|
| `app/runner/runner.py` | A1 root-cause fix; fail-fast `_load_orchestrator`; `_assert_initialized` + `initialization_report`; atomic `effective_policy` constructor arg; `last_error` semantics; A5 import alias |
| `app/runner/errors.py` *(new)* | `RunnerInitializationError` with stable reason codes and a redacted, persistable cause summary |
| `app/runner/system_events.py` *(new)* | `record_bot_system_event` — append-only history, secret redaction, never raises |
| `app/runner/multi_runner.py` | Quarantines a runner that fails construction (never cached, never scheduled); records `RUNNER_INITIALIZATION_FAILED`, `EFFECTIVE_POLICY_REJECTED`, `RUNNER_POLICY_CHANGE`; passes policy atomically |
| `app/core/bot_health.py` *(new)* | Canonical `last_error` semantics: current state, not history |
| `app/core/bot_instance_service.py` | `update_bot_health` uses those semantics |
| `app/main.py` | `/runner/status` reports `execution_mode`, `broker_environment`, `is_mainnet`, `runner_initialization_status`, `runner_policy_hash`, `runner_components`, `process_execution_mode` |
| `shared_lib/persistence/trace_recorder.py` | A4 reason consistency; A6 heartbeat exclusion from the canonical ledger |
| `shared_lib/persistence/db.py` | `bot_system_events` table + two indexes |

New tests: `test_runner_initialization_contract.py`, `test_bot_health_error_consistency.py`,
`test_decision_reason_consistency.py`, `test_runner_import_shadowing.py`.

---

## C. Database / schema changes

* **Added** `bot_system_events` (append-only) via the existing
  `CREATE TABLE IF NOT EXISTS` path in `db.py` — no manual DDL against the live
  database, applied on normal startup.
  Columns: `event_id, bot_instance_id, user_id, run_id, event_type, severity,
  reason_code, message, details_json, provenance, created_at`.
  Indexes: `(bot_instance_id, created_at DESC)`, `(reason_code, created_at DESC)`.
  `provenance` defaults to `RUNTIME`.
* **No table dropped, no row deleted, no historical row rewritten.** The A4/A6
  fixes change *recording* behaviour only; existing rows are untouched.
* **Backward compatible:** `new_candle_count` in
  `GET /bot-instances/{id}/decision-quality` still computes correctly once
  heartbeats stop entering the ledger.

---

## D. Tests

Canonical invocation, `backends/venv` (the interpreter that runs the server):

```
../venv/Scripts/python.exe -m pytest tests -q
1799 passed, 0 failed, 0 skipped, 27 warnings, 4 subtests passed in 250.33s
```

The 27 warnings are all `ConstantInputWarning` from
`shared_lib/ml/readiness.py:404` (Spearman on a constant array) in
`test_phase_i_readiness.py` — pre-existing, unrelated.

`conftest.py` blocks any POST/PUT/DELETE to a broker order endpoint across every
adapter, so no test can place a real order. Verified before the run.

---

## E. Runtime proof

Single canonical runtime, `backends/venv`, cwd `backends/bot-backend`,
DB `backends/shared/shared_lib/persistence/cosmicforge.db`:

```
runner_initialization_status  = READY
runner_components             = {effective_policy: True, strategy: True,
                                 orchestrator: True, executor: True,
                                 position_manager: True}
execution_mode                = broker
broker_environment            = demo
is_mainnet                    = False
runner_policy_hash            = 4f48c8f5...89d76   (== currently resolved policy)
bot_health_status             = WAITING_FOR_SIGNAL
bot_health_reason_code        = NO_NEW_CANDLE
last_error                    = None          <- stale capital error cleared
ERROR_STRATEGY_UNAVAILABLE since restart: 0
CAPITAL_BUDGET_REQUIRED       : 0
```

**Master Ensemble reached — natural HOLD with complete evidence:**

```
ts               = 2026-09-07T21:15:19Z      symbol = ETHUSDT   timeframe = 15m
regime_state     = LOW_VOLATILITY_CHOP       regime_confidence = 0.552
adx              = 13.9                      atr_pct = 0.175
chosen_strategy  = master_ensemble
signal           = hold      confidence = 0.0     threshold = 0.7
buy_score        = 0.0       sell_score = 0.0     htf_opposed = 0
reason_codes     = REGIME_LOW_VOL_CHOP
gate_reason      = REGIME_LOW_VOL_CHOP
submit_attempted = 0
equity           = 806.54580081              last_price = 2494.09
```

BTCUSDT is equivalent (`LOW_VOLATILITY_CHOP`, ADX 14.4, ATR% 0.071).
No thresholds were lowered and no trade was forced.

**Reason attribution after A5:** heartbeats now persist
`reason_codes = NO_NEW_CANDLE`, `gate_reason = NO_NEW_CANDLE`,
`secondary_reason_codes_json = []` (was `SKIP` / `''` / `["NONE"]`).

**Ledger separation after A6:** over a sampled window, `decision_traces` = 4
(heartbeats, full detail retained), `canonical_trade_decisions` = 0.

**Single execution authority.** The earlier "two uvicorn processes" finding is a
**false positive**: `backends/venv/Scripts/python.exe` is a launcher stub that
re-execs `C:\Program Files\Python312\python.exe`. Parent and child share a
creation timestamp and command line; only the child binds port 9000. One backend.

**No competing Auto Pilot authority.** The global contextless
`paper_runner_instance` in `main.py` never loops and never trades:
`/runner/paper/once` and `/trade/close-record` both raise HTTP 410;
`/runner/paper/state` is read-only. It is now explicitly marked
`initialization_status = "LEGACY_NO_CONTEXT"` and can never report `READY`.

**`run_cycle` is the sole authority.** `PaperRunner.run_once` is a four-line
deprecated delegate to `run_cycle`. AST comparison confirms **no** method is
called by `run_once` that `run_cycle` does not call. Kill switch, daily close,
reconciliation, drawdown snapshots and per-symbol stepping all live in `run_cycle`.

---

## F. Remaining known issues

### Repaired and verified now
A1–A6 above.

### Verified already correct (no change needed)
* **Capital contract.** `resolve_effective_bot_policy` fails closed with
  `CAPITAL_BUDGET_REQUIRED`; there is no `10000` fallback anywhere in the runtime
  path. 18 tests in `test_auto_pilot_capital_flow.py` cover deploy → request →
  instance → policy → context → sizing.
* **Mode / environment separation.** `normalize_execution_mode` maps legacy
  `mode='live'` to the execution dimension `broker`. Real-money permission comes
  solely from `broker_accounts.environment`, which is `demo`. Now reported as
  separate fields.
* **Runner policy refresh.** Policy hash is deterministic (5/5 identical
  resolutions). The live runner's hash equals the currently-resolved hash.
  Material changes evict and rebuild with lifecycle restoration.
* **Candle gating.** `bot_candle_evaluations` already claims one evaluation per
  `(bot, symbol, timeframe, closed_candle)`. The 10 s loop does position
  management only; entry evaluation runs once per closed 15m candle.

### Deferred — next mandatory phase
* **Full canonical evidence system.** `runtime_sessions`, `policy_snapshots`,
  `bot_runs`, `trading_cycles`, `market_snapshots`, `execution_attempts`,
  `orders`, `fills`, `positions`, `position_events`, `risk_events`,
  `reconciliation_events`, `data_quality_events`, and the full provenance
  enumeration are **not** implemented. `bot_system_events` and the existing
  `canonical_trade_decisions` / `cycle_decision_summaries` /
  `bot_candle_evaluations` are the partial foundation.
  *Deliberately deferred:* ~13 new tables plus backfill is a schema program, not a
  runtime repair, and mixing it into this batch would have obscured A1–A6.
  **Next step:** define `runtime_sessions` + `bot_runs` first — they carry the
  correlation ids (`runtime_session_id`, `run_id`) every other table depends on.
* **Data-integrity invariant checks** (§18) exist as behaviour but not as a
  standing checker emitting `data_quality_events`.
* **Strategy quality.** Master Ensemble returning HOLD under
  `LOW_VOLATILITY_CHOP` is correct behaviour, not a defect. The multiple-
  confidence-authority consolidation (Master → `TradingOpportunity` → one
  `TradingDecisionEngine`) remains future work. **Nothing here was tuned.**
* **Note for the capital path:** `app/api/auto_pilot.py:74` defaults
  `total_capital_budget` to `allocation_value` when a client omits it. Explicit
  and bounded (allocation ≤ budget is enforced), not a fabricated figure, but it
  should become a required field once the frontend always sends it.

---

## Final verdict

```
RUNTIME_REPAIR_STATUS:           PASS
AUTO_PILOT_STRATEGY_PATH:        PASS
CAPITAL_CONTRACT:                PASS
MODE_ENVIRONMENT_SEPARATION:     PASS
RUNNER_POLICY_REFRESH:           PASS
PAPER_LIFECYCLE:                 PASS
EVIDENCE_INTEGRITY:              PARTIAL
SAFE_TO_CONTINUE_PRE_AI_PROGRAM: YES (with the deferral below)
```

`EVIDENCE_INTEGRITY` is **PARTIAL**, not PASS: decision-level evidence is now
correct, attributed and free of heartbeat pollution, but the full canonical
entity set of §15 does not exist. Reconstructing a decision is possible today;
reconstructing a complete position lifecycle from `position_id` across every
listed entity is not.

`SAFE_TO_CONTINUE_PRE_AI_PROGRAM: YES` covers resuming normal paper/demo
operation and Phase 5 work. It does **not** authorise AI training data
collection, which requires the provenance enumeration and the canonical evidence
system first — synthetic, replay, paper, testnet and live evidence are not yet
distinguishable at the schema level.
