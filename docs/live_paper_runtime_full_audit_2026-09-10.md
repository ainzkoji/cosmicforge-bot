# Live Paper Runtime — Full Read-Only Health / Architecture Audit

**Date:** 2026-09-10
**Audit window:** 17:49:51Z (runtime process start) → 18:14:10Z
**Mode:** strict read-only. No source file, config, database row or process was changed.
The SQLite database was opened only with `file:...?mode=ro`. Nothing was restarted,
stopped or killed. The only artefact created is this report.

| Item | Value |
|---|---|
| Repository | `C:\Users\favou\OneDrive\Desktop\cosmicforge-bot` |
| HEAD | `a4d3106c3605467fcd024ed9054270e9248eb971` |
| Branch | `phase-0-4-runtime-baseline` |
| Runtime session | `rts_5548a9cb6dee44579790` |
| RunManager run | `dac86b06-d165-46cd-866f-5644b9cc6cf5` |
| Bot run | `2ee0ef5a31844b1493536e592fb054dd` |
| Bot | `bot_a8117dc719fc` (master_ensemble, paper, BTCUSDT + ETHUSDT, 15m) |
| Canonical DB | `backends\shared\shared_lib\persistence\cosmicforge.db` (3.5 GB, WAL) |

---

## Executive summary

The runtime is **operationally healthy**. It runs as one process, holds one lease (heartbeat
about 2 s old), cycles every ~10 s with zero errors, has placed zero broker orders and holds
zero positions. The adaptive threshold engine is the only final threshold authority for
internally generated opportunities. The session-gate fix is present in the running source.

The audit found six problems the startup log does not show:

1. **P0 — The test suite writes into the canonical paper database.** 9,339 rows each in
   `canonical_trade_decisions` and `decision_traces` belong to test bots
   (`bot_replay_*`, `bot_determinism_*`, `bot_sensitivity`), written by 11 full-suite runs.
   **849 of them were written during this live session** (17:51:58Z–18:02:58Z), by a
   concurrent `pytest -q` run from another session. Those two tables have no provenance
   column.
2. **P1 — The `sma_cross` expert has been broken on every evaluation since
   2026-09-08T05:00Z.** The cause is a signature mismatch with `SnapshotMarketClient.klines`. The
   failure is recorded as a neutral `HOLD` vote with `eligible=1, executed=1, weight=0.9`.
3. **P1 — The capital ledger is never consulted in paper mode.** The executor's paper branch
   returns before `_authorize_capital`. The bot is configured for 2 slots × 120 USDT margin
   against a 120 USDT budget.
4. **P1 — The TradingView external-signal entry path never calls
   AdaptiveEntryThresholdEngine.** The processor is enabled and runs every cycle. The queue for
   this bot is empty, so this is latent.
5. **P2 — The threshold performance calibration query fails on every evaluated candle**
   (`positions.risk_amount` does not exist). The failure is swallowed and reported as
   `INSUFFICIENT_SAMPLE`.
6. **P2 — The running code is not reproducible from any commit.** Eight live-relevant files
   are uncommitted or untracked, including the background-job ownership gate and
   `ENVIRONMENT_NAME`.

---

## A. Frozen state

### Git

```
HEAD   a4d3106c3605467fcd024ed9054270e9248eb971
branch phase-0-4-runtime-baseline
staged (none)
```

`git log -15 --oneline`:

```
a4d3106c Record the session-gate consistency forensic report
8622fc73 Fix the session-gate divergence: my float(None) crash, silently relabelled
4fb218d5 Correct the acceptance block: the first live EVALUATED threshold was observed
8f917d0f Record the final threshold removal report and runtime acceptance
28341638 Delete the old threshold architecture; AdaptiveEntryThresholdEngine is the only one
049a287b Record the threshold engine rebuild report and live acceptance
e00ad262 Keep threshold state out of the production database under test
61b4ef43 Rebuild the entry threshold as one adaptive engine, retiring the old stack
a2571fb0 Capital invariant, Phase 13 parity, Phase 14 data contract, provenance applied
977ee2d5 Trace entry-threshold provenance: 0.70 is an override that makes the dynamic system inert
87e58723 Add Master Ensemble zero-trade forensic audit (read-only)
7fe03173 Phase 16 plan: the gate is the opportunity rate, not the three weeks
a2f1f7bb Phase 15: measure the ensemble baseline, and name the real bottleneck
d6e9d3de Phase 13: fill models, cost model, replay identity, and two config audits
d5229d69 Phase 13 start: quarantine the legacy backfill, and a leak-proof historical clock
```

### Dirty files, and whether the live process loaded them

The runtime process (PID 46412) was created at **17:49:51Z**. Every dirty file was last
written between **04:13Z and 04:51Z** (06:13–06:51 local), and `.env` at 04:26:46Z. Every
one of them therefore existed before startup, and the Python modules among them were
imported by the live process.

| File | State | mtime (local) | Class | What it does in the live process |
|---|---|---|---|---|
| `app/core/config.py` | M | 06:26:46 | **LIVE_RUNTIME_RELEVANT** | Adds `ENVIRONMENT_NAME`. Without it, sessions record `development_local`/`unknown` |
| `app/evidence/writers.py` | M | 06:31:54 | **LIVE_RUNTIME_RELEVANT** | `reap_abandoned_sessions` ran at startup and marked dead sessions `ABANDONED` (49 now) |
| `app/main.py` | M | 06:42:14 | **LIVE_RUNTIME_RELEVANT** | Import-time preflight, stop-file watcher, shutdown quiesce, **lease-gated background jobs** |
| `app/ops/database_registry.py` | M | 06:27:24 | **LIVE_RUNTIME_RELEVANT** | `VALIDATION` classification, used in the startup `database_registry` write |
| `app/ops/runtime_ownership.py` | M | 06:13:45 | **LIVE_RUNTIME_RELEVANT** | `pid_is_alive`, `lease_is_stale` helpers used by preflight and the reaper |
| `app/runner/multi_runner.py` | M | 06:24:42 | **LIVE_RUNTIME_RELEVANT** | `owns_runtime` / `ownership_decided`, read by the job gate |
| `app/ops/runtime_preflight.py` | ?? | 06:23:20 | **LIVE_RUNTIME_RELEVANT** | Imported and executed at module import by `main.py` |
| `app/ops/runtime_shutdown.py` | ?? | 06:37:03 | **LIVE_RUNTIME_RELEVANT** | Imported by the stop-file watcher and the shutdown hook |
| `tests/test_strategy_clock_and_snapshot.py` | M | 06:29:36 | TEST_ONLY | — |
| `tests/test_runtime_activation_and_ownership.py` | ?? | 06:42:59 | TEST_ONLY | — |
| `scripts/start_trading_runtime.ps1` | M | 06:51:00 | OPERATOR_SCRIPT | Not used for this launch (see B) |
| `scripts/activate_manual_runtime.ps1` | ?? | 06:50:36 | OPERATOR_SCRIPT | — |
| `scripts/trading_runtime.ps1` | ?? | 06:48:01 | OPERATOR_SCRIPT | — |
| `scripts/runtime_status.py` | ?? | 06:17:28 | OPERATOR_SCRIPT | — |

None of the dirty files touch strategy, threshold, regime, session, risk-sizing or executor
decision code. `trading_orchestrator.py`, `master_ensemble.py`, `hold_breakdown.py`,
`runner.py`, `app/threshold/*` and `executor.py` are identical to HEAD. **Decision logic is
therefore HEAD's decision logic. The process as a whole is `a4d3106c + 8 uncommitted
lifecycle files`.** The recorded `code_revision=a4d3106c` does not by itself identify what is
running.

One consequence to note: the `BACKGROUND_JOBS_OWNER reason=RUNTIME_OWNER` gate, and the
`environment_name=paper_forward_local` value, both exist only in the working tree. Reverting
or cleaning the tree removes them silently.

---

## B. One live runtime

| Check | Result |
|---|---|
| Port 9000 listeners | **1**: `0.0.0.0:9000` → PID **46412** |
| PID 46412 command line | `"C:\Program Files\Python312\python.exe" -m uvicorn app.main:app --host 0.0.0.0 --port 9000` |
| Parent | PID 33848 (`backends\venv\Scripts\python.exe` launcher shim, same command) |
| Grandparent | PID 27036, a VS Code PowerShell terminal (started 05:56 local) |
| Process start | 2026-09-10 19:49:51 local = **17:49:51Z** |
| Working directory | `backends\bot-backend` (from `runtime_sessions`) |
| Python | venv `python.exe`, 3.12.2 |
| `/health` | `status=ok`, `code_version=a4d3106c`, `pid=46412`, `process_started_at=17:49:52Z` |
| Lease (`runtime_ownership`) | 1 row: `trading_scheduler`, owner `own_c5d9993ef8164d3c836f`, session `rts_5548a9cb6dee44579790`, pid 46412, `released_at NULL`, heartbeat **18:14:08Z** (fresh) |
| Active leases | **1** |
| `runtime_sessions` RUNNING | **1** (this one). 49 ABANDONED, 7 STOPPED |
| MultiBotRunner | **1** (inferred: one process, one `RUNNER_CREATED` event at 17:50:00Z, every cycle under one `run_id`) |
| Background-job owner | **1** (APScheduler jobs are registered only through the lease-gated `_when_owner`) |

**Other processes checked, for DB-scoped singleton purposes:**

- VS Code `black` and `isort` language servers (PIDs 24148/25808/27828/25828). Not app code.
- **`pytest -q`** (PIDs 39260/32456), started by another Claude session at 17:48:28Z and
  finished 18:05:16Z with *2474 passed*. It is not a runtime, but **it wrote to the canonical
  DB during this session** (see P0-1).
- **PID 47920**: an orphan `multiprocessing` spawn child. Its parent 43436 is gone; it was
  started 2026-09-07 23:47 local and has `python312.dll`, `_sqlite3.pyd` and `sqlite3.dll`
  loaded. Whether it holds `cosmicforge.db` open cannot be established without a handle tool.
  No session, lease or evidence row is attributable to it. It is unidentified (P2-12).

There is no other runtime session, no other lease, and no cycle, decision or threshold row
from any other session since 17:45Z. **The DB-scoped runtime singleton holds. The DB-scoped
writer singleton does not**, because the test suite wrote to the DB.

The runtime was launched with a bare `uvicorn` in a VS Code terminal, not through
`start_trading_runtime.ps1`. The newest file in `logs/runtime/` is from 06:48 local, so
**this session's stdout and stderr are not on disk** (P2-11).

---

## C. `[THRESHOLD_CALIBRATION] positions query failed: no such column: risk_amount`

1. **Emitter:** `app/threshold/calibration.py:241`, in
   `SqlitePerformanceSource.recent_r_multiples`.
2. **SQL** (`calibration.py:225-238`):
   ```sql
   SELECT realized_pnl, risk_amount FROM positions
   WHERE bot_instance_id=? AND symbol=? AND status='CLOSED'
     AND realized_pnl IS NOT NULL AND risk_amount IS NOT NULL AND risk_amount > 0
   ORDER BY closed_at DESC LIMIT ?
   ```
3. **Table:** the canonical `positions` table in `cosmicforge.db`. It is created by
   `evidence_schema._execution_and_positions`, which `migrations.migrate()` calls at
   `migrations.py:2713`. Its DB handle is `DB()`, resolved from `DATABASE_URL`, and
   `runtime._db()` returns `None` only under `COSMICFORGE_TEST_MODE`.
4. **Actual schema:** `position_id, bot_instance_id, user_id, broker_account_id, run_id,
   decision_id, execution_attempt_id, symbol, side, execution_mode, broker_environment,
   provenance, original_qty, remaining_qty, realized_qty, entry_price, realized_pnl, fees,
   status, opened_at, updated_at, closed_at, close_reason, leverage, committed_margin`.
   **There is no `risk_amount` and no stop price.** The table has 0 rows.
5. **What happened to `risk_amount`:** it was **never part of the `positions` schema**. It is
   not in the DDL. The only `ALTER`s on `positions` add `leverage` and `committed_margin`
   (`capital_ledger.ensure_position_capital_columns`). It was not renamed and not deleted.
   The column exists on **`trading_decisions.risk_amount`** (`evidence_schema.py:263`, next to
   `stop_distance` and `quantity`). The query was written against a column that does not exist,
   and it has done so since `calibration.py` was introduced in `61b4ef43`.
6. **Dependent input:** `PerformanceCalibrator.evaluate`, called from
   `AdaptiveEntryThresholdEngine._compute` (`engine.py:459`). It populates
   `performance_score`, `performance_adjustment`, `performance_sample_size` and
   `performance_status` on `AdaptiveThresholdDecision`.
7. **What happens on failure:** SQLite rejects the statement at prepare time, so the query
   fails regardless of row count. The inner `except` logs the warning and **returns `[]`**.
   `PerformanceCalibrator.score([])` then returns `adjustment=0.0, sample_size=0,
   status=INSUFFICIENT_SAMPLE`. The outer `except` in `evaluate()`, which would report
   `UNAVAILABLE`, never fires because the inner one swallowed the error.

**Full path:** `positions` (no such column) → `SqlitePerformanceSource` (swallows, returns
`[]`) → `PerformanceCalibrator` (mislabels the empty list as `INSUFFICIENT_SAMPLE`) →
`AdaptiveEntryThresholdEngine._compute` (`calibration_adjustment += 0.0`) →
`AdaptiveThresholdDecision` → `threshold_decisions.performance_status='INSUFFICIENT_SAMPLE'`.

| Can it affect… | Now | Latent |
|---|---|---|
| final threshold | No. The correct value is also 0: 0 closed canonical positions against 30 required | **Yes.** The performance term stays 0 forever, even after 30+ closed trades |
| smoothing | No | No |
| performance adjustment | Forced to 0 (coincides with the correct value today) | Permanently 0, silently |
| distribution adjustment | No (separate path via `adaptive_threshold_state`) | No |
| readiness evidence | **Yes.** `performance_status=INSUFFICIENT_SAMPLE` describes a data shortage that is really a hard defect | Same |

**Frequency:** the query runs once per `EVALUATED` threshold decision; the `NOT_EVALUATED` and
`HARD_BLOCKED` paths return before `_compute`. Since the engine went live there have been
**5** EVALUATED decisions (01:30Z, 03:30Z, 04:00Z, 17:50Z and 18:00Z, all ETH). The on-disk
stderr logs confirm 3 of them: `runtime-20260910-022031.log.err` = 2 and
`runtime-20260910-055838.log.err` = 1. This session's 2 went to the VS Code terminal. **Rate:
100% of evaluated candles.**

**Correct canonical source of realised R (proposal, not implemented):**
`positions.realized_pnl` joined through `positions.decision_id → trading_decisions.risk_amount`,
counting only rows where the decision's risk amount was populated at approval. Three things
need confirming first: whether `realized_pnl` should be net of `positions.fees`, how partial
closes aggregate, and that the approval path writes `risk_amount`. That last point is
unverifiable today because no approved decision exists yet.

---

## D. `[DYNAMIC_SHADOW_DEBUG] hook entry mode=static shadow_enabled=False auto_enabled=False`

**Emitter:** `runner.py:1685-1703`, `_run_dynamic_universe_shadow_diagnostics`.

- `mode` = `SYMBOL_UNIVERSE_MODE` (`'static'`). That is the **symbol-universe** mode, not a
  threshold mode.
- `shadow_enabled` = `DYNAMIC_UNIVERSE_SHADOW_ENABLED` (`False`).
- `auto_enabled` = `AUTO_SYMBOL_SELECTION_ENABLED` (`False`).
- It returns `{"status": "disabled"}` on the next line. With everything disabled it does
  nothing further.

**Domain:** dynamic symbol-universe discovery, ranking and promotion diagnostics. When
enabled, it writes `dynamic_universe_shadow_diagnostics`, `symbol_universe_rankings` and
`symbol_universe_promotion_decisions`, and runs `EventNewsModeController`. It never builds a
`TradingOpportunity`, never calls the threshold engine, and never touches the executor.

**`mode=static` belongs to the symbol-universe subsystem. It is not AdaptiveEntryThresholdEngine**
(whose mode is `ADAPTIVE`, see E) **and not the deleted threshold stack.**

| Can DYNAMIC_SHADOW… | Answer | Proof |
|---|---|---|
| calculate an entry threshold | NO | No import of `app.threshold`. Returns before any work |
| alter or override the final threshold | NO | Final threshold comes only from `threshold_decisions.final_threshold` (see below) |
| block or approve entry | NO | Returns a dict nobody gates on. Called for diagnostics only |
| change confidence or opportunity | NO | Never constructs or mutates strategy output |
| change risk sizing or leverage | NO | Docstring and body: no allocation, leverage or risk writes |
| change executor behaviour | NO | No executor reference |

**Repository sweep** (patterns: `DYNAMIC_SHADOW, dynamic_shadow, shadow_threshold,
dynamic_threshold, threshold_shadow, shadow_enabled, auto_enabled, mode=static, confidence
floor/gate, threshold floor`, case-insensitive):

| Location (hits) | Classification |
|---|---|
| `app/runner/runner.py` (34) | OTHER_DOMAIN / OBSERVABILITY_ONLY: `DYNAMIC_SHADOW_DEBUG` prints (symbol universe) and `confidence_gate_modifier=caution_modifier` written to traces |
| `app/threshold/migration.py` (24) | LEGACY_HISTORICAL: inventory and dispositions of deleted settings |
| `app/core/config.py` (14) | LEGACY_HISTORICAL: tombstones plus `detect_legacy_threshold_keys`, which **rejects** legacy keys at startup |
| `app/strategy/hold_breakdown.py` (11) | OBSERVABILITY_ONLY: `threshold_floor` = the adaptive final threshold, used only to label HOLD reasons |
| `app/symbols/symbol_selector.py` (8), `symbol_promotion.py` (2), `symbol_demotion.py` (1) | OTHER_DOMAIN (symbol universe) |
| `app/strategy/master_ensemble.py` (7) | LEGACY_HISTORICAL comments about the deleted floor |
| `app/strategy/activity_targets.py` (6) | **DEAD_CODE**: `min_confidence_floor=0.20`, exported from `app.strategy`, no runtime caller |
| `app/shadow/config.py` (4) | RESEARCH_NON_AUTHORITY: shadow-trade recorder, `SHADOW_ENABLED` default False and unset |
| `app/ml/scorer.py` (3) | OBSERVABILITY_ONLY (ML feature; ML disabled) |
| `app/adaptive/engine.py` (3), `policies.py` (2), `audit_log.py` (1) | OTHER_DOMAIN: `caution_modifier` feeds size and leverage only; `min_confidence_gate` is deleted |
| 6 component experts (1 each: "Apply confidence gate") | OTHER_DOMAIN: each expert's own emission gate inside opportunity construction, not a final threshold |
| `app/runner/effective_policy.py`, `app/decision/decision_engine.py` (1 each) | LEGACY_HISTORICAL comments |
| `shared_lib/persistence/trace_recorder.py` (5), `migrations.py` (1) | OBSERVABILITY_ONLY (`confidence_gate_modifier` trace column) |
| tests (17 files), scripts (5), docs (7) | not runtime |

**ACTIVE_THRESHOLD_AUTHORITY: exactly one, `AdaptiveEntryThresholdEngine`
(`app/threshold/engine.py`).**

- All 11 `threshold_decisions` rows carry `threshold_engine_version=1.0.0,
  threshold_mode=ADAPTIVE` and one policy hash.
- Each decision's `effective_entry_threshold` equals its `threshold_decisions.final_threshold`
  (0.784643 at 17:50Z, 0.754643 at 18:00Z).
- The PolicyEngine confidence gate is deleted (`runner.py:632`).
- `detect_legacy_threshold_keys` refuses to start if a legacy key reappears.

**Architecture violation found elsewhere (P1-3):** the TradingView external-signal adapter
opens entries without consulting the threshold engine at all. It does not compute a second
threshold; it bypasses the only one. See section S.

---

## E. Active adaptive threshold policy

Resolved in a fresh interpreter from the same `.env` (mtime 04:26Z, before startup). It
matches every persisted `threshold_decisions` row and `adaptive_threshold_state`
byte-for-byte by hash.

| Field | Value |
|---|---|
| threshold_engine | AdaptiveEntryThresholdEngine |
| engine_version | 1.0.0 |
| mode | **ADAPTIVE** |
| base / min / max | **0.70 / 0.50 / 0.90** (`.env` `THRESHOLD_BASE`, `THRESHOLD_MIN`, `THRESHOLD_MAX`) |
| regime / volatility / agreement / htf / market_quality / performance / distribution | 0.06 / 0.05 / 0.08 / 0.05 / 0.04 / 0.05 / 0.05 (code `DEFAULTS`); total 0.38 |
| smoothing alpha / max_step_up / max_step_down | 0.35 / 0.05 / 0.03 |
| performance min_samples / lookback | 30 / 100 |
| distribution min_samples / window / percentile | 40 / 200 / 0.60 |
| hard_block_regimes | `LOW_VOLATILITY_CHOP` |
| policy_version | 1.0.0 |
| **policy_hash** | **`afd2b4636b9d89e9d99dabc46ea08338386e717e66d7b8cb226911c601f17802`** |
| precedence | GLOBAL → ASSET_CLASS → VENUE → SYMBOL → BOT |
| source_scopes | `[GLOBAL]` (`THRESHOLD_SCOPED_OVERRIDES=''`; `master_ensemble` passes no `bot_overrides`) |

**Exactly one EffectiveThresholdPolicy:** resolving for BTC and ETH, with venue `None`,
`unknown` or `BINANCE` and `market_type=CRYPTO`, gives one distinct hash. `bot_runs.policy_hash
= cbdf8436…` is the *EffectiveBotPolicy* hash, a different object; it is not a second
threshold policy.

The live mechanics can be checked from the rows. ETH at 18:00Z: raw 0.7222 is smoothed
toward the previous 0.7846, then limited by `max_step_down` to **0.754643**. That is exactly
0.784643 − 0.03, with `rate_limit_applied=1`.

---

## F. Session-gate fix

- `8622fc73` is an ancestor of HEAD, and none of its files are dirty. **The running source
  contains the fix.**
  - `trading_orchestrator.py:943-973`: `_is_signature_rejection` inspects the signature and
    fails closed.
  - `trading_orchestrator.py:996-1016`: `LegacyStrategyAdapter` re-raises TypeErrors raised
    inside the strategy body.
  - `runner.py:5451-5461`: same guard on the legacy path. The path is unreachable for Auto
    Pilot, which returns `ERROR_STRATEGY_UNAVAILABLE` at 5409-5418.
  - `hold_breakdown.py`: `threshold_floor: float | None`. `None` never fires
    `CONFIDENCE_BELOW_FLOOR`.

**All `except TypeError` sites in `app/`:**

| Site | What is retried | Verdict |
|---|---|---|
| `core/trading_orchestrator.py:996` | strategy `get_signal(**kwargs)` | Guarded ✓ |
| `runner/runner.py:5451` | strategy `get_signal(**kwargs)` | Guarded ✓ |
| `strategy/loader.py:71` | strategy **constructor** `cls(client, interval, **params)` → `cls(client, interval)` | **Unguarded, same anti-pattern.** A TypeError inside `__init__` silently builds the strategy with default params. Construction only, not a double execution (P2-9) |
| `execution/executor.py:2429` | broker read `get_algo_orders(raise_on_error=True)` → without the kwarg | OTHER_DOMAIN. Live protection path only; paper returns at 2368 (P3) |
| `risk/realized_pnl.py:79` | broker read `user_trades` parameter style | OTHER_DOMAIN (P3) |

**Proof of single execution on current evidence:** every decision in this session has exactly
7 `expert_evaluations` rows for 7 distinct strategies. No `(decision_id, strategy)` pair is
duplicated anywhere in the table. There is exactly one canonical decision per
(symbol, candle). `market_type=CRYPTO` is present in both symbols' trace metadata, so the
kwargs survived.

**Decisions since this runtime started (genuine, non-heartbeat):**

| Reason | BTCUSDT | ETHUSDT |
|---|---|---|
| SESSION_BLOCKED | 0 | 0 |
| CRYPTO_SESSION_24_7_BYPASS (session status) | 0 (gate not reached) | **2** |
| REGIME blocked (`REGIME_LOW_VOL_CHOP`) | **2** | 0 |
| NO_OPPORTUNITY | 0 | 0 |
| ENTRY_CONFIDENCE_BELOW_THRESHOLD | 0 | **2** (0.3014 vs 0.7846; 0.3145 vs 0.7546) |

**Historically contaminated rows (35, not modified by this audit):**

| Session | Code revision | Symbol / n | First → last `evaluated_at` |
|---|---|---|---|
| `rts_1d57cb6fc2534762b093` | `e00ad262` | BTC 8 | 2026-09-09T21:45:09Z → 2026-09-10T00:15:02Z |
| `rts_a28204303a1a46f2a253` | `28341638` | BTC 14, ETH 12 | 2026-09-10T00:30:23Z → 03:45:08Z |
| `rts_1784a8692fdc4e39a96f` | `4fb218d5` | BTC 1 | 2026-09-10T04:00:07Z |

Before the window, there were 133 `NO_OPPORTUNITY` and 0 `SESSION_BLOCKED`. After it, there
are 6 `NO_OPPORTUNITY` and 0 `SESSION_BLOCKED`.

**Exclusion rule for research and readiness:** `primary_reason='SESSION_BLOCKED' AND
evaluated_at BETWEEN '2026-09-09T21:45:09Z' AND '2026-09-10T04:00:08Z' AND
runtime_session_id IN (the three above)`. The EVALUATED ETH threshold rows from that window
(01:30Z, 03:30Z, 04:00Z) came from first calls that completed. They are valid, and they sit in
the ETH distribution sample.

---

## G. Same-candle BTC/ETH consistency

Two organic same-candle pairs were observed in the same cycle, bot and candle:

| Candle close | Cycle | Symbol | market_type | session_status | session_allowed | Threshold policy hash | Bot policy hash |
|---|---|---|---|---|---|---|---|
| 17:44:59.999Z | `de99af26…` | BTC | CRYPTO | `null` | `false` ⚠ | — (never resolved) | cbdf8436… |
| | | ETH | CRYPTO | **CRYPTO_SESSION_24_7_BYPASS** | `true` | afd2b463… | cbdf8436… |
| 17:59:59.999Z | `f7d4fe86…` | BTC | CRYPTO | `null` | `false` ⚠ | — | cbdf8436… |
| | | ETH | CRYPTO | **CRYPTO_SESSION_24_7_BYPASS** | `true` | afd2b463… | cbdf8436… |

Source: `decision_traces.gate_details_json` for run `2ee0ef5a…`.

- BTC was `LOW_VOLATILITY_CHOP`. The ensemble activation matrix has no strategies for that
  regime, so it returned `regime_low_vol_chop_suspended` at Step 3 (`master_ensemble.py:655-664`),
  **before** the session gate at Step 3.6. BTC's session verdict therefore does not exist.
- `session_allowed=false` for BTC is a **mislabel**: the gate was `skipped_no_active_strategies`,
  not failed (P2-5).
- The inputs were consistent: both symbols carried `market_type=CRYPTO` with the same global
  session policy (`ENSEMBLE_SESSION_FILTER_ENABLED=True`, `06:00-19:00` UTC).
- **Neither candle discriminates**, because both are inside 06:00–19:00 UTC. ETH's
  `CRYPTO_SESSION_24_7_BYPASS` is still conclusive, since the bypass branch is checked first and
  its reason code is written explicitly.
- A discriminating observation needs a candle after 19:00Z, **and** BTC out of
  `LOW_VOLATILITY_CHOP` so that it reaches the gate. No trade is required.

---

## H. Market-quality input

`master_ensemble._market_quality_context` (`master_ensemble.py:407-450`), consumed by
`engine.market_quality_component`:

| Field | Status | Detail |
|---|---|---|
| volume percentile | **AVAILABLE_DERIVED** | Last candle's volume rank within the last 120 canonical candles |
| spread | **NULL_BY_DESIGN** | `spread_percentile=None`; MarketSnapshot carries no order book |
| estimated slippage | **NULL_BY_DESIGN** | `estimated_slippage_bps=None` |
| order-book depth | **NULL_BY_DESIGN** | Not a contract field; no depth source |
| liquidity score | **NULL_BY_DESIGN** | `liquidity_score=None` |
| stale-data indicator | **AVAILABLE_REAL** | `snapshot.is_stale`. A hard gate before the engine (`REASON_MARKET_DATA_STALE`), never an adjustment |
| price discontinuity | AVAILABLE_DERIVED | Open-vs-previous-close gap > 0.5% adds 0.5 to the factor |
| composite `market_quality_score` | **AVAILABLE_DERIVED, single-factor** | With every other part `None`, the score *is* the volume percentile. Observed ETH: 0.375 → +0.010 adj; 0.841667 → lower |

Nothing neutral is invented, and absent inputs are skipped rather than filled. The engine
reads only generic fields, with no Binance-specific logic. The input is still **PARTIAL**: a
one-factor composite presented as "market quality" (P2-3).

---

## I. HTF input

`master_ensemble._htf_context` (`master_ensemble.py:332-405`) and `MarketSnapshot`
(`runner/market_snapshot.py`):

- **Closed candles only.** `MarketSnapshot.build` passes both the primary and the HTF series
  through `closed_candles()` (`close_ms <= now_ms`, lines 19-25 and 96).
- **Alignment.** `htf_is_timestamp_aligned` requires the HTF close to be at or before the
  primary close (line 75). Misaligned or short (< 200) series return an empty context, which
  the engine treats as "no information".
- **Evidence** (`market_snapshots`): for both symbols at both candles, `higher_timeframe=4h`,
  `higher_timeframe_closed_candle_time=1789055999999` (**2026-09-10T15:59:59.999Z**), and
  `higher_timeframe_aligned=1`. The next 4h close is 19:59:59.999Z. **No future leakage.**
- **No symbol leakage.** Each evaluation has its own `market_snapshot_id` and a distinct
  `data_hash` (BTC `cc949d…`/`568740…`, ETH `4e1e84…`/`39a017…`). `_htf_context` is a
  stateless classmethod over the snapshot, with no cache.
- **No broker shortcut.** HTF candles come through the snapshot (`source=PaperBookClient` →
  delegate), not a separate venue call.
- **Values.** ETH `htf_alignment_score=1.0` at both candles: 4h price ≥ 5% above EMA200,
  aligned with the BUY side, so `htf_adjustment=-0.05`. The 01:30Z–04:00Z rows had −1.0
  (opposed). BTC never reached the engine this session, so no BTC score exists yet.
  Direction and strength are not persisted separately (P3).

**HTF_RUNTIME_INPUT: PASS**, with BTC's score not yet exercised.

---

## J. Event filter staleness

| Item | Finding |
|---|---|
| Source | `economic_events` (84 rows). Staleness = `MAX(updated_at)` (`shared_lib/persistence/economic_events.get_last_sync_utc`) |
| Last update | **2026-04-26T15:00:50Z**, ≈ **3,291 h** before the audit. Newest scheduled event 2026-05-29; **0 future events**. `event_blackout_windows` = 0 rows |
| Refresh | `event_ingestion_worker` is **disabled** (`EVENT_INGESTION_ENABLED=False`, the config default, not set in `.env`). `calendar_sync_worker` starts but has nothing to sync. The feed is in manual-calendar mode and nobody has updated it since April |
| Credentials | Not exercised, because ingestion is disabled |
| Influence on entries | `EventBlackoutFilter.check` is called at `runner.py:5315` (entry), `5954` and `2488` (external). **Currently inert:** a stale feed is warn-only, and with no future events there can be no active HIGH window. This run's traces show `event_blocked=0` across 154 rows |
| What "warn-only" means | Log a warning and return `is_blocked=False, reason=EVENT_FEED_STALE_WARN_ONLY`. Entries proceed |
| Why warn-only | `stale_warn_only = (BINANCE_ENV in {testnet, demo} OR EVENT_INGESTION_ENABLED=False) AND EVENT_FILTER_STALE_FAILSAFE_TESTNET_WARN_ONLY` |
| Controlled beta / mainnet | **Still warn-only while `EVENT_INGESTION_ENABLED=False`**, because the manual-calendar clause is venue-independent. Mainnet would trade through an empty or stale calendar with only a warning. With ingestion enabled and stale > 24 h, the failsafe blocks all entries |
| Contributes to | threshold NO · hard veto NO (inert now; would block only on an active HIGH window) · session NO · regime NO · opportunity NO · risk NO · execution NO |

**The economic-calendar feed is dead data.** The filter is wired correctly but has nothing to
filter (P2-4).

---

## K. Database and schema

- **Path, role, environment:** explicit configuration, not guessed at runtime. `.env` line 2
  sets `DATABASE_URL=…/cosmicforge.db`, line 10 `DATABASE_ROLE=paper`, line 11
  `ENVIRONMENT_NAME=paper_forward_local`. `database_registry` classifies the file as
  `ACTIVE` ("resolved from DATABASE_URL"); the other three candidates are FORENSIC, BACKUP
  and VALIDATION and are never auto-selected. (`ENVIRONMENT_NAME` exists only in the dirty
  `config.py`, see A.)
- **`schema_version=0` is expected, and it means "unversioned".** It reads
  `PRAGMA user_version`, and nothing writes it: `user_version` appears only in the readers at
  `main.py:175`, `database_registry.py:60` and `runtime_baseline.py:62`. The migration
  mechanism is the idempotent, additive `migrate()` run at import (`main.py:168`). It uses
  `CREATE … IF NOT EXISTS` and add-column-if-missing, and it calls `ensure_evidence_schema`
  and `ensure_position_capital_columns`. The 0 therefore does not indicate missing
  migrations. It is also not evidence of anything, since every database reads 0 (P3-1).
- **Required objects are present in the canonical DB:** `threshold_decisions` (all 54
  columns, including `reconciles`), `expert_evaluations`, `adaptive_threshold_state`,
  `positions` (with `leverage` and `committed_margin`, which the capital ledger reads),
  `runtime_ownership`, `runtime_sessions`, `bot_runs`, `trading_cycles`, `trading_decisions`,
  `execution_attempts`, `position_events`, `market_snapshots`.

---

## L. Capital contract

| Item | Value |
|---|---|
| capital_allocation (budget) | **120.0** (`capital_allocation_type=fixed_amount`) |
| allocation_type / value | fixed_amount / **120.0** per position (margin) |
| risk profile | `balanced`: `per_trade_risk_pct=0.0025` (ceiling 0.004), `max_position_slots=2`, `daily_loss_limit_pct=0.05` |
| effective max_open_positions | **2** = min(profile 2, operator `MAX_OPEN_POSITIONS=3`, system ceiling) |
| default leverage | 3 |
| committed margin | **0.0** (`positions` has 0 rows) |
| remaining bot capital | **120.0** |

**Ledger status: FAIL in paper mode.**

- `CapitalLedger` exists (`app/risk/capital_ledger.py`: correct chain, fails closed on an
  unreadable ledger).
- Its only caller is `LiveExecutor._authorize_capital` at `executor.py:1091`, inside
  `_execute_impl`. `_execute_impl` returns at `executor.py:774-799` for any
  `execution_mode != "live"`, and hands the order to `PaperExecutor.open_position` with **no
  budget check**.
- In the paper runtime, `committed + proposed <= budget` is therefore not enforced by
  anything.
- The tests pin `_authorize_capital` to come before `_size_qty(` and before `margin_required
  =` in the source text, but not before the paper branch. They give false assurance.
- In the live path, `_authorize_capital` also **fails open**: any exception returns `None`,
  and the caller proceeds (P2-8).

**Configuration is oversubscribed.** 2 slots × 120 margin = 240 theoretical against a
120 budget. `resolve_effective_bot_policy` only checks a single allocation against the budget
(`effective_policy.py:190`). No breach has happened, because zero positions have been opened.

---

## M. Paper mode and broker order endpoints

- **Routing:**
  - `_execute_impl` sends OPEN and CLOSE to `PaperExecutor` and returns at line 799, before
    every broker call: `place_order` 1355, `place_protection` 1516, `close_position_market`
    870/904/1561, `cancel_all_orders` 864/901.
  - `execute_tp1_partial_close` returns at 1796 on the paper path, before 1799-1986.
  - `ensure_protection` returns at 2368 for paper (`source=paper_book`).
- **Defense in depth is incomplete.** `PaperBookClient` intercepts only
  `cancel_all_orders`, `open_orders`, `get_algo_orders` and position reads. `place_order`,
  `place_protection` and `close_position_market` pass through `__getattr__` to the real
  client, so one future call site outside `_execute_impl` would reach the broker (P2-7).
- **Runtime evidence since start:**
  - `execution_attempts` = 0 (global).
  - `decision_traces.submit_attempted` = 0 and `position_opened` = 0.
  - `trading_cycles.execution_attempt_count` = 0 and `fill_count` = 0.
  - `live_audit.jsonl` for this run contains only `IOFS_GATE` events and one
    `INFO RECONCILE_POSITIONS_STARTUP {updated: 0}`. There are no order events.
  - Market-data reads go to the public testnet endpoints, which is allowed.

**PAPER_BROKER_ORDER_CALLS = 0.**

---

## N. Background-job ownership

**Lease-gated APScheduler jobs** (dirty `main.py:_startup_background_jobs` → `_when_owner` →
`_startup_signal_scheduler`, which also has a process-global duplicate guard):

| Job | Schedule | Requires ownership |
|---|---|---|
| `signal_gen_*` | cron (`SIGNAL_GENERATION_TIMES_UTC`) | Yes, registered only when `RUNTIME_OWNER` |
| `signal_expiry` | every 5 min | Yes |
| `organic_dataset_nightly` | 02:00Z (**enabled**) | Yes |
| `ml_monthly_retrain` | day 1, 03:00Z | Yes |
| `daily_paper_validation_monitor` | 23:30Z | Yes |

A non-owner process logs `BACKGROUND_JOBS_REFUSED` and registers none of these. **Owner
count: 1.**

**Not gated (P2-6):** `main.py:697-706` starts seven asyncio workers unconditionally in any
process that serves the app: `notification_worker`, `backtest_worker`,
`event_ingestion_worker` (self-disabled), `calendar_sync_worker`, `event_reaction_worker`,
`news_ingestion_worker` and `real_time_news_worker` (shadow). A second, non-owner process on
the same DB would run all seven. The trading loop and the per-cycle `ExternalSignalProcessor`
sit inside `MultiBotRunner`, which is lease-gated.

---

## O. ML / AI

- `ML_ENABLED=False`, `ML_SHADOW_MODE=False`, `ML_MODEL_PATH=''`.
- `ml_runtime_status`: `enabled=0, shadow_mode=0, loaded=0, model_version=NULL`.
- `models/production/` contains only `README.md`.

**What `ml_monthly_retrain` (`scripts/ml/retrain_pipeline.py:retrain_entry_model_if_ready`)
does:**

1. Checks readiness (organic rows ≥ 500, IOFS rows ≥ 300, closed IOFS trades ≥ 20).
2. Builds the dataset, trains, and runs Section 5A validation.
3. Runs a promotion preflight and requires an AUC gain of ≥ 0.01 over the current model.
4. Calls `run_promotion(dry_run=False)`. This **copies the model, meta and encoders into
   `models/production/`** and writes a *shadow-suggestion* env snippet to a separate file and
   a manual-activation report.
5. **It never writes `.env`.** It hashes `.env` before and after and flags
   `ACTIVE_ENV_CHANGED` / `ML_ENABLED_CHANGED`. That is detection after the fact, not
   prevention.
6. It never touches `THRESHOLD_*` or the threshold engine.
7. Last run: 2026-06-15, `ready_to_retrain=false`, `promoted=false`, `BLOCKED`. Next fire:
   2026-10-01 03:00Z.

- **Can training run?** Yes, if readiness passes.
- **Can a model influence entries?** Only if an operator sets `ML_ENABLED=True` (plus a model
  path) and restarts. Never automatically.
- **Auto-activation?** No. But promotion into the production directory is automatic, with no
  human approval (P2-10).

**AI_DECISION_AUTHORITY = DISABLED.**

The nightly dataset (enabled) is `decision_traces INNER JOIN trade_fills`. The P0 test rows
have no fills, so they are currently excluded by the join. The builder has no bot or
provenance filter.

---

## P. Evidence health (17:49:57Z → 18:14Z)

| Count | Value |
|---|---|
| trading_cycles | 140 (all completed, `error_count=0`) |
| genuine closed-candle evaluations | **4** (2 candles × 2 symbols) |
| NO_NEW_CANDLE heartbeats | ≥ 206 (cycle counters). **Not** written to `trading_decisions` or `canonical_trade_decisions` ✓ |
| threshold_decisions | 2 (ETH, EVALUATED) |
| expert_evaluations | 14 (7 × 2, ETH) |
| execution_attempts / positions / fills / position_events | 0 / 0 / 0 / 0 |

**Lineage** (`runtime_session → bot_run → cycle → decision → threshold_decision → experts`):

- 0 decisions without a cycle; 0 orphan threshold rows; 0 orphan expert rows.
- 0 duplicate (symbol, candle) decisions; 0 duplicate threshold rows per candle; 0 duplicate
  experts per decision.
- No cross-symbol leak (distinct snapshots and hashes). No cross-candle leak (the candle times
  agree across the decision, snapshot and threshold rows).
- All 4 decisions are `PAPER_FORWARD`.
- **Gaps:**
  - Regime and no-active-strategy blocks write no `threshold_decisions` row, while
    `NO_OPPORTUNITY` writes a `NOT_EVALUATED` row. BTC has no threshold lineage this session
    (P3-4).
  - The current `bot_runs` row still reads `cycles=0, decisions=0`, and 31 older `bot_runs`
    remain `RUNNING` under dead sessions (P3-2).

**Contamination: FOUND (P0-1).**

| Table | Test rows | Test bots | Span |
|---|---|---|---|
| `canonical_trade_decisions` | **9,339** | bot_replay_unmodified, bot_replay_lifecycle, bot_determinism_a, bot_determinism_b, bot_sensitivity | 2026-09-09T05:08:48Z → 2026-09-10T18:02:58Z |
| `decision_traces` | **9,339** | same | same |
| `bot_daily_state` | 2 | `test-bot` | 2026-02-21, 2026-06-02 |

- 55 test run_ids = 11 suite runs × 5 bots, at exactly 849 rows per suite run.
- **849 rows landed during this live session.**
- Synthetic candle times (e.g. `1699999259999`, from 2023), `ADAPTER_ERROR` reasons.
- Source: `tests/test_replay_production_parity.py`. Mechanism:
  - `backends/bot-backend/conftest.py` sets only `COSMICFORGE_TEST_MODE` and
    `EXECUTION_MODE`, never `DATABASE_URL`.
  - `get_trace_recorder()` caches a process-wide recorder bound to `DB().path`, so replay
    traces go to whichever DB the first caller resolved.
- Neither table has a provenance column.
- The Phase-11 canonical tables (`trading_decisions`, `threshold_decisions`,
  `expert_evaluations`, `adaptive_threshold_state`, `market_snapshots`) are **clean**.

**CANONICAL_EVIDENCE_HEALTH: PARTIAL.**

---

## Q. Current live bot health (at ≈ 18:14Z)

**OPERATIONAL HEALTH: PASS**

| Item | Value |
|---|---|
| server | `/health` 200 `status=ok` |
| lease | heartbeat 18:14:08Z (≈ 2 s old), not released |
| runner | last cycle completed 18:14:01.7Z; cadence ≈ 10 s; 0 errors |
| strategy / market-data clock | last closed 15m candle 17:59:59.999Z, evaluated 18:00:00–05Z; next at 18:15Z |
| last BTC eval | 18:00:02.73Z → `REGIME_LOW_VOL_CHOP` |
| last ETH eval | 18:00:05.57Z → `ENTRY_CONFIDENCE_BELOW_THRESHOLD` (0.3145 < 0.7546) |
| last threshold decision | ETH 18:00:05.14Z, EVALUATED, 0.754643 |
| positions | 0 open (`bot_symbol_state` NONE; 0 lifecycle rows for this bot) |
| in-flight executions | 0 (0 attempts; 0 `pending_entries` for this bot) |
| kill switch | OFF (`bot_daily_state.kill=0`; traces `kill_switch_state=NORMAL`) |
| daily loss | realized 0.0, trades 0 |
| consecutive losses | 0, no cooldown |

**STRATEGY SELECTIVITY: HIGH.** This is separate from operational health, and zero trades is
not an operational failure.

- None of the 4 evaluations produced an approved entry.
- BTC is structurally suspended in `LOW_VOLATILITY_CHOP`.
- ETH confidence is about 0.30–0.31 against a threshold of about 0.75–0.78.
- Only 1 of the 4 eligible experts votes directionally (supertrend), and one of the other
  three is broken (P1-1).

---

## R. Findings by severity

### P0: runtime or evidence integrity threat (1)

**P0-1. The test suite writes into the canonical paper DB, concurrently with the live runtime.**
9,339 rows each in `canonical_trade_decisions` and `decision_traces` (849 during this session),
with no provenance column. `conftest.py` does not isolate `DATABASE_URL`, and the
`TraceRecorder` singleton binds to `DB().path`. Anything that reads those two tables without
a bot filter is contaminated. That includes dashboards, analytics, readiness and, once fills
exist, the ML dataset builder. It also puts an uncontrolled second writer on the live WAL
database.

### P1: affects decision correctness (3)

**P1-1. The `sma_cross` expert has been silently broken since 2026-09-08T05:00Z.**
`sma_cross.py:82` calls `client.klines(symbol, interval=…, limit=…)` positionally, but
`SnapshotMarketClient.klines(self, *, symbol, …)` is keyword-only. The TypeError is caught as
`data_error` and the expert returns HOLD 0.0.
- It is 50 of 50 evaluated decisions with component metadata, across 5 sessions.
- `expert_evaluations` records it as `eligible=1, executed=1, signal=HOLD, weight=0.9`.
- The effect: the ensemble has lost a directional voter, the agreement score is diluted
  (weight 0.9 of 4.7), which raises the threshold, and a failure is booked as a real neutral
  vote.
- Every Phase-15 opportunity-rate measurement since 2026-09-08 describes a degraded ensemble.

**P1-2. The capital ledger is not enforced in paper mode.** The paper branch returns before
`_authorize_capital`. With 2 × 120 allocation against a 120 budget, the invariant
`committed + proposed ≤ budget` is unenforced in the running mode. The source-order tests do
not cover the paper branch.

**P1-3. The external-signal entry path bypasses AdaptiveEntryThresholdEngine (architecture
violation, latent).** `process_external_signal_candidate` (`runner.py:2358-2860`) runs:
event filter → PolicyEngine (risk and sizing; the confidence gate is deleted) → TradingView
cap → execution filter → `_execute_signal_with_evidence`. Confidence is only capped at 0.75,
never compared with any threshold.
- `TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=true`; the processor runs every cycle (heartbeat
  18:03:10Z).
- 0 queue rows for this bot; last queue activity 2026-05-22.
- Invariant broken for external entries: *TradingOpportunity → AdaptiveEntryThresholdEngine
  → AdaptiveThresholdDecision → TradingDecisionEngine*.

### P2: degraded subsystem or incomplete evidence (12)

- **P2-1. Performance calibration query.** Fails on 100% of evaluated candles
  (`positions.risk_amount` never existed). The failure is swallowed and mislabelled
  `INSUFFICIENT_SAMPLE` instead of `UNAVAILABLE`. It becomes P1 once 30 or more closed trades
  exist.
- **P2-2. Running code is not reproducible from a commit.** Eight live-relevant dirty or
  untracked files, including the lease gating of background jobs and `ENVIRONMENT_NAME`.
- **P2-3. Market-quality composite is single-factor** (volume percentile). Spread, slippage,
  depth and liquidity are null by design.
- **P2-4. Economic calendar is stale by 3,291 h** and ingestion is disabled. The filter is
  inert. Mainnet with ingestion disabled would still be warn-only.
- **P2-5. BTC traces record `session_allowed=false`** when the session gate was skipped. This
  is the same misreadable-evidence family as the 35-row incident.
- **P2-6. Seven asyncio workers start unconditionally**, not gated by the lease.
- **P2-7. `PaperBookClient` does not intercept** `place_order`, `place_protection` or
  `close_position_market`. Routing is safe today; defense in depth is missing.
- **P2-8. `_authorize_capital` fails open** in the live path: an exception returns `None` and
  the trade proceeds.
- **P2-9. `strategy/loader.py:71` constructor TypeError fallback** silently drops strategy
  params.
- **P2-10. ML monthly retrain auto-copies artefacts into `models/production`** without human
  approval. Inert while ML is disabled.
- **P2-11. This session's stdout and stderr are not persisted.** Direct `uvicorn` from a VS Code
  terminal; warnings cannot be audited from disk.
- **P2-12. Unidentified orphan Python process, PID 47920**, with sqlite loaded and its parent
  dead since 2026-09-07.

### P3: cleanup or observability only (10)

- **P3-1.** `schema_version=0` is a non-signal; `user_version` is never written.
- **P3-2.** 31 `bot_runs` rows left `RUNNING` under dead sessions; the current run's
  `cycles`/`decisions` counters stay 0.
- **P3-3.** `threshold_decisions.venue='unknown'`; the venue kwarg is not supplied.
- **P3-4.** Lineage asymmetry: regime and no-active-strategy blocks write no
  threshold_decisions or expert rows, but `NO_OPPORTUNITY` does.
- **P3-5.** `LOW_VOLATILITY_CHOP` is blocked twice: the empty activation matrix, and the
  policy's `hard_block_regimes`. The policy field is not the effective control for that regime.
- **P3-6.** The IOFS shadow gate uses session windows `07-10,13-16` with no crypto bypass, so
  it would mis-block crypto if ever enforced. It is evaluated on every 10-s heartbeat
  (`live_audit.jsonl` is 108 MB).
- **P3-7.** Stale `pending_entries` row for the deleted `bot_e5fe913972a9`: DOTUSDT
  `OPEN_CONFIRMED` since 2026-05-20, broker order `211861324`.
- **P3-8.** Dead `activity_targets` floor code; `DYNAMIC_SHADOW_DEBUG` print noise every cycle.
- **P3-9.** Broker read-endpoint TypeError fallbacks (`executor.py:2429`, `realized_pnl.py:79`).
- **P3-10.** `bot_daily_state` row for day 2026-09-10 was created at 2026-09-09T22:00Z, so the
  day key appears to roll at local midnight, not UTC. Not traced.

---

## Proposed fixes (not implemented, for operator decision)

| Finding | Proposal |
|---|---|
| P0-1 | Make the root `conftest.py` set `DATABASE_URL` to a per-session temp DB *before* any app import, and fail the session if `DB().path` resolves to a file whose registry role is `paper` or `live`. Reset the `get_trace_recorder()` singleton per test. Add a `provenance` column to `canonical_trade_decisions` and `decision_traces`. Quarantine the 9,339 + 9,339 rows by bot id (label them, do not delete) in a separately approved data task. Until then, **do not run the full suite from this checkout while the runtime is up** |
| P1-1 | Call `self.client.klines(symbol=symbol, interval=…, limit=…)` in `sma_cross.py`, and audit every expert for positional `klines(` calls. Make an expert `data_error` a distinct non-eligible state (`executed=0` or `signal=ERROR`), never HOLD. Add a contract test that runs every expert against `SnapshotMarketClient`. Flag Phase-15 measurements since 2026-09-08T05:00Z |
| P1-2 | Apply the ledger before the paper/live split, one chain for both modes, and add a behavioural test (not source-order) that two 120-margin paper entries against a 120 budget cannot both open. Separately, decide whether `max_position_slots × fixed allocation > budget` should be refused at policy resolution |
| P1-3 | Either route external candidates through the same TradingOpportunity → AdaptiveEntryThresholdEngine → TradingDecisionEngine path, or set `TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=false` until they do. This is the operator's call |
| P2-1 | Take R from `positions.realized_pnl` joined to `trading_decisions.risk_amount` via `decision_id` (define fee and partial-close handling first). Stop swallowing the error in `SqlitePerformanceSource`, so a failure reports `UNAVAILABLE`. Add a schema contract test for the query |
| P2-2 | Commit or discard the lifecycle work, then restart from a clean tree at a time of your choosing |
| P2-3 | Accept it as one factor and label it so, or add venue-neutral spread and depth inputs to the snapshot contract |
| P2-4 | Enable ingestion or retire the filter explicitly. Make the failsafe venue-independent for `live` |
| P2-5 | Record `session_allowed=NULL` and `session_status=NOT_EVALUATED` when the gate is skipped |
| P2-6 | Move the seven workers behind the same ownership gate, or document them as per-process |
| P2-7 | Intercept order-placing methods in `PaperBookClient` and raise on use in paper mode |
| P2-8 | Make `_authorize_capital` fail closed on exception |
| P2-9 | Apply the `_is_signature_rejection` guard in `loader.py` |
| P2-10 | Force `dry_run=True` in the scheduled job, and require explicit promotion |
| P2-11 | Always launch through the script that writes `logs/runtime/*.log` |
| P2-12 | Identify PID 47920 (operator), then decide |
| P3-x | Housekeeping, as listed |

---

## Final verdict

```
RUNTIME_RUNNING:
YES

RUNTIME_HEALTH:
DEGRADED

PORT_9000_OWNER_COUNT:
1

RUNTIME_LEASE_OWNER_COUNT:
1

MULTIBOTRUNNER_COUNT:
1

BACKGROUND_JOB_OWNER_COUNT:
1

CURRENT_HEAD:
a4d3106c3605467fcd024ed9054270e9248eb971

WORKING_TREE_CLEAN:
NO

DIRTY_RUNTIME_RELEVANT_FILES:
backends/bot-backend/app/core/config.py
backends/bot-backend/app/evidence/writers.py
backends/bot-backend/app/main.py
backends/bot-backend/app/ops/database_registry.py
backends/bot-backend/app/ops/runtime_ownership.py
backends/bot-backend/app/runner/multi_runner.py
backends/bot-backend/app/ops/runtime_preflight.py (untracked)
backends/bot-backend/app/ops/runtime_shutdown.py (untracked)

DATABASE_PATH:
C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db

DATABASE_ROLE:
paper

ENVIRONMENT_NAME:
paper_forward_local

DATABASE_SCHEMA_VERSION:
0 — PRAGMA user_version, never written by the additive migrate(); means "unversioned",
expected; all required tables verified present

ACTIVE_BOT:
bot_a8117dc719fc

ACTIVE_SYMBOLS:
BTCUSDT, ETHUSDT

ADAPTIVE_THRESHOLD_ENGINE_PRESENT:
YES

ACTIVE_FINAL_THRESHOLD_AUTHORITY_COUNT:
1

ACTIVE_FINAL_THRESHOLD_AUTHORITY:
AdaptiveEntryThresholdEngine (app/threshold/engine.py) — note P1-3: the external-signal
entry path bypasses it

THRESHOLD_ENGINE_MODE:
ADAPTIVE

THRESHOLD_POLICY_HASH:
afd2b4636b9d89e9d99dabc46ea08338386e717e66d7b8cb226911c601f17802

THRESHOLD_CALIBRATION_QUERY:
FAIL

THRESHOLD_CALIBRATION_ROOT_CAUSE:
SqlitePerformanceSource (calibration.py:227) selects positions.risk_amount; the canonical
positions table never had that column (risk_amount lives on trading_decisions); the query
fails on every EVALUATED candle, the exception is swallowed, returns [] and is reported
as INSUFFICIENT_SAMPLE

PERFORMANCE_CALIBRATION_USABLE:
NO

DISTRIBUTION_CALIBRATION_USABLE:
PARTIAL

DYNAMIC_SHADOW_PRESENT:
YES

DYNAMIC_SHADOW_CAN_INFLUENCE_ENTRY:
NO

LEGACY_THRESHOLD_AUTHORITY_FOUND:
NO

HTF_RUNTIME_INPUT:
PASS

MARKET_QUALITY_RUNTIME_INPUT:
PARTIAL

SESSION_GATE_FIX_PRESENT:
PASS

INTERNAL_TYPEERROR_RETRY_PRESENT:
NO (strategy execution paths guarded); constructor-level variant remains at
strategy/loader.py:71 (P2-9)

STRATEGY_DOUBLE_EXECUTION_RISK:
NO

BTC_CRYPTO_24_7_BYPASS:
NOT_YET_OBSERVED

ETH_CRYPTO_24_7_BYPASS:
PASS

CROSS_SYMBOL_SESSION_CONSISTENCY:
NOT_YET_OBSERVED

FALSE_SESSION_BLOCKED_HISTORY_DOCUMENTED:
YES

EVENT_FEED_STATUS:
STALE

EVENT_FEED_AFFECTS_PAPER_DECISIONS:
NO

CAPITAL_BUDGET_LEDGER:
FAIL

CAPITAL_CONFIGURATION_OVERSUBSCRIBED:
YES

PAPER_BROKER_ORDER_CALLS:
0

AI_DECISION_AUTHORITY:
DISABLED

ML_MONTHLY_RETRAIN_ROLE:
Guarded offline retrain (day 1, 03:00Z); on passing gates it auto-copies model files into
models/production (dry_run=False) and writes a shadow-suggestion file, but never edits
.env or threshold policy; inert while ML_ENABLED=False; last run 2026-06-15 BLOCKED

CANONICAL_EVIDENCE_HEALTH:
PARTIAL

TEST_REPLAY_CONTAMINATION:
FOUND

OPEN_POSITIONS:
0

IN_FLIGHT_EXECUTIONS:
0

KILL_SWITCH:
OFF

OPERATIONAL_HEALTH:
PASS

STRATEGY_SELECTIVITY:
HIGH

P0_FINDINGS:
1 — test suite writes into the canonical paper DB (9,339 + 9,339 rows; 849 during this
session; no provenance column)

P1_FINDINGS:
3 — sma_cross expert broken since 2026-09-08 and booked as HOLD; capital ledger not
enforced in paper mode; TradingView external entry path bypasses
AdaptiveEntryThresholdEngine (latent)

P2_FINDINGS:
12 — calibration query failing/mislabelled; running code not reproducible (8 dirty files);
single-factor market quality; dead event calendar; BTC session_allowed mislabel;
ungated workers; PaperBookClient pass-through; ledger fails open (live);
loader TypeError fallback; ML auto-promotion; runtime logs not persisted; orphan PID 47920

P3_FINDINGS:
10 — schema_version non-signal; stale bot_runs; venue 'unknown'; lineage asymmetry;
duplicate LOW_VOL_CHOP block; IOFS shadow session/noise; stale pending_entries row;
dead code/print noise; broker-read TypeError fallbacks; local-midnight day key

SOURCE_CODE_CHANGED:
NO

DATABASE_MUTATED_BY_AUDIT:
NO

RUNTIME_RESTARTED:
NO

SAFE_TO_KEEP_RUNNING_IN_PAPER:
YES

SAFE_FOR_PHASE_15_EVIDENCE:
NO

SAFE_FOR_PHASE_16:
NO

SAFE_FOR_AI_ACTIVATION:
NO

SAFE_FOR_MAINNET:
NO
```
