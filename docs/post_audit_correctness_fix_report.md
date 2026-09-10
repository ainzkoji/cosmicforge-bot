# Post-Audit Correctness and Evidence-Integrity Fix

Branch `phase-0-4-runtime-baseline`. Base `a4d3106c`. 2026-09-10 / 2026-09-11.

This fixes the defects found by `docs/live_paper_runtime_full_audit_2026-09-10.md`:
the P0, all three P1s, and the seven P2s this pass asked for. No threshold
parameter, expert weight, regime threshold, risk percentage or the 120-USDT
capital configuration was changed. No trade was forced, and AI was not enabled.

The other session's runtime-lifecycle work (preflight, graceful stop, ownership-gated
background jobs, `ENVIRONMENT_NAME`) was preserved unchanged and is committed
separately, before this fix.

> Sections marked **PENDING** are completed after the final commit and restart.

---

## 1. P0: tests wrote into the canonical paper database

### Root cause

The audit named the trace-recorder singleton. The full cause has two parts, and
both had to go:

1. **`app/main.py` loaded `.env` with `override=True` at import.** The first test
   that imported the app replaced whatever `DATABASE_URL` the test process had with
   the canonical one, for the rest of the session. `conftest.py` never set a
   database at all, so there was nothing to protect in the first place.
2. **`get_trace_recorder()` built one recorder and reused it forever**, bound to
   whatever `DB().path` resolved to first. When `test_replay_production_parity`
   pointed `DATABASE_URL` at a temporary file, its `trading_decisions` went there,
   but its traces kept going to the cached canonical recorder. That split is why
   the newer canonical tables were clean and the two trace ledgers were not.

### Fix

| Layer | Change |
|---|---|
| `backends/bot-backend/conftest.py` | Before any application import: creates a unique temporary database per session, sets `DATABASE_URL` to it, `DATABASE_ROLE=test`, `ENVIRONMENT_NAME=test`, `COSMICFORGE_TEST_MODE=1`. Refuses to start (`pytest.UsageError`) if the inherited environment names a runtime database or role. At session start it verifies `DB()` resolves to the temp file and prints `TEST_DATABASE_PATH` / `TEST_DATABASE_ROLE`, even under `-q`. Resets the trace recorder after every test. |
| `shared_lib/persistence/test_isolation.py` (new, stdlib-only) | The single guard. Under test mode it refuses, by raising, a database that is: named `cosmicforge.db`; any `*.db` in the canonical persistence directory; the `DATABASE_URL` written in `bot-backend/.env`; or opened with role `paper` / `live` / `production`. |
| `shared_lib/persistence/db.py` | `DB()` enforces the guard **before** creating any directory or table, so a refused database is never touched. |
| `shared_lib/persistence/trace_recorder.py` | `TraceRecorder` enforces the guard itself (it opens its own connections). `get_trace_recorder()` is keyed on the database currently in force and rebuilds when it changes. |
| `app/main.py` | `load_dotenv(override=...)` is off in test mode. This is the only line of mine in that file. |

### Provenance on both trace ledgers

`decision_traces` and `canonical_trade_decisions` gain a `provenance` column. It
is added by migration, by `DB()` for fresh databases, and lazily by the recorder
for unmigrated ones. It is **written by the caller at write time**: the runner
passes the same authority its canonical decisions use (`run_provenance` →
`PAPER_FORWARD`, `REPLAY`, `PAPER_FORWARD_VALIDATION`, …). A caller that says
nothing is stored as NULL rather than guessed. Nothing is inferred later from
bot names.

### Historical contamination: classified, not deleted

`scripts/classify_test_evidence_provenance.py` is dry-run by default and opens
the DB read-only. It identifies only the known test families and, on an explicit
`--apply`, sets `provenance='TEST_FIXTURE'` where provenance is still NULL:

* the UPDATE sets one column, so every value field is immutable;
* rows already carrying a provenance are untouched;
* a family matching a real `bot_instances` row is LOW confidence and is never applied;
* `--apply` refuses if the provenance column does not exist yet, because the script never alters the schema.

Dry run against the canonical DB (read-only, 2026-09-10):

| Table | Family | Rows | Runs | First → last |
|---|---|---|---|---|
| `canonical_trade_decisions` | `bot_replay_*` | 4,708 | 22 | 2026-09-09T05:08:48Z → 2026-09-10T17:55:50Z |
| `canonical_trade_decisions` | `bot_determinism_*` | 3,036 | 22 | 2026-09-09T05:11:43Z → 2026-09-10T18:00:02Z |
| `canonical_trade_decisions` | `bot_sensitivity` | 1,595 | 11 | 2026-09-09T05:15:28Z → 2026-09-10T18:02:58Z |
| `decision_traces` | same three families | 4,708 / 3,036 / 1,595 | 22 / 22 / 11 | same |
| `bot_daily_state` | `test-bot` | 2 | — | 2026-02-21 → 2026-06-02 (report only; table has no provenance column) |

That totals **9,339 + 9,339**, matching the audit, all HIGH confidence.
`will_apply=False` everywhere because the live DB gains the column only on
restart. **Nothing has been applied.** Running `--apply` is an operator decision.

### Proof

* `tests/test_test_database_isolation.py`:
  * the session DB is temporary with role `test`;
  * a `cosmicforge.db`-named file, the persistence directory and a `paper` role are each refused before anything is created;
  * the recorder refuses and follows `DATABASE_URL`;
  * both ledgers persist the provenance given.
* **Two subprocess tests start `pytest` with `DATABASE_URL` pointing at a paper-named DB, or with `DATABASE_ROLE=paper`.** Both exit non-zero with `TEST_DATABASE_ISOLATION_VIOLATION`, and the target DB still holds only its sentinel table: **not one evidence table was created.**
* **Canonical DB, measured around every pytest run in this pass:** a read-only max-rowid baseline of all 159 tables was taken before each run and compared after. Every new row was attributed to the live runtime (`bot_a8117dc719fc`, session `rts_5548a9cb6dee44579790`, run `2ee0ef5a…`). The newest test-family row is still 2026-09-10T18:02:58Z, before this fix.

---

## 2. P1: `sma_cross` broken, and the expert contract

### The call

`sma_cross.py` called `client.klines(symbol, ...)` positionally, and
`SnapshotMarketClient.klines` is keyword-only. The fix is `klines(symbol=symbol, ...)`.

### All seven experts audited against the snapshot contract

| Expert | Call | Timeframe | Verdict before |
|---|---|---|---|
| supertrend | `klines(symbol=, interval=, limit=200)` | 15m | OK |
| trend_pullback | keyword | 15m | OK |
| donchian_breakout | keyword | 15m | OK |
| squeeze_breakout | keyword | 15m | OK |
| bollinger_reversion | keyword | 15m | OK |
| sma_cross | **positional** | 15m | **broken on every candle since 2026-09-08T05:00Z** |
| vwap_reversion | keyword | **5m** | **broken whenever active** (not in the audit) |

**`vwap_reversion` had a second, independent defect.** It is built with its
default 5m interval, and the snapshot carried only 15m and 4h, so every read
raised `snapshot_timeframe_unavailable:5m`. It is active only in RANGE, which has
not occurred live since 2026-09-08, so no live row shows it.

**Fix, with no formula or weight changed:** `MarketSnapshot` gains
`auxiliary_candles`, closed-candle series keyed by timeframe and **cut at the
strategy candle's close**, so no series can look ahead. `SnapshotMarketClient`
serves them. `MasterEnsembleStrategy.snapshot_timeframes` declares which extra
timeframes its experts read (`("5m",)`), and the runner fetches those into the
entry snapshot. A snapshot without auxiliary series keeps its old `data_hash`.

**Limitation:** replay history has no 5m candles (`historical_candles` holds 15m
and 1h only), so in replay a RANGE candle's vwap still reports ERROR and fails
closed. That is the correct, visible outcome of missing data.

### ERROR is a distinct expert outcome

Experts catch their own exceptions and return `HOLD 0.0 "error:…"` or
`"data_error:…"`. That was a vote. Now:

* `master_ensemble._run` classifies an expert's own error report, or an exception, as **ERROR**. The component is recorded with signal `ERROR`, confidence 0 and the reason, and is excluded from votes.
* `experts_from_votes(errors=…)` records `ExpertEvidence(signal="ERROR", eligible=<kept>, executed=True, weight=<kept>, weighted_contribution=0)`. The reason is one line, capped at 500 characters. It lands in `expert_evaluations` as ERROR, never HOLD.
* **Policy: fail closed.** `AdaptiveEntryThresholdEngine` returns `status=ERROR, reason=EXPERT_EVALUATION_ERROR` whenever an eligible expert is in ERROR. No threshold, agreement, or pass/fail is computed. The rule sits in the one threshold authority, so every caller behaves the same.
  * **Why not "drop it from the denominator":** that raises agreement and lowers the bar on the strength of a failure. It is the permissive option.
  * **The cost, accepted deliberately:** a broken expert now stops its regime's candles, loudly, instead of silently diluting them. That is exactly what would have exposed `sma_cross` on day one.
* An all-errored candle continues to the engine, which records the ERROR, rather than taking the old "no valid votes" exit.
* `TradingDecisionEngine`: when there is no directional candidate, an ERROR threshold decision's reason now outranks `NO_OPPORTUNITY`. **This was found by the tests.** Without it, a candle whose experts all failed was still recorded as "nothing pointed anywhere".

---

## 3. P1: the capital ledger governs paper and live, and fails closed

* **One gate before the split.** `BinanceExecutor._capital_gate` resolves leverage once and calls `_authorize_capital` once. It is called at the top of `_execute_impl` for every BUY/SELL, **before** the paper/live branch.
  * Paper opens at the approved notional.
  * Live reuses the same verdict rather than asking the ledger again.
  * Both paper and live result details carry `capital` and the executed `leverage`.
* **Fail closed:**

  | Condition | Result |
  |---|---|
  | Managed bot, no budget | `CAPITAL_BUDGET_REQUIRED` |
  | Budget but no bot id or no DB | `CAPITAL_LEDGER_UNAVAILABLE` |
  | Unreadable ledger | `CAPITAL_LEDGER_UNAVAILABLE` (own reason, not "budget exhausted") |
  | Any exception | `CAPITAL_LEDGER_UNAVAILABLE` |
  | Unmanaged executor (no bot id and no budget: backtests, legacy) | `None`: the only case with no bot budget to protect |

* **Committed margin used the wrong leverage.** This was not in the audit.
  * The canonical decision row is finalized *after* the evaluation that produced the fill, so `fill_bridge` found no row at OPEN time and committed the full notional as margin (1×).
  * It now takes the executed leverage from the fill, then from the sizing evidence the runner stashes before executing, then the decision row.
  * Only after all three does it default to 1×, which over-reserves: the safe direction.
* The two old source-order tests (`test_capital_budget_invariant.py`) checked the ledger only against `_size_qty`, which is why they stayed green while paper bypassed it. They now require the gate before the paper/live split.
* **The configuration is still oversubscribed:** 2 slots × 120 margin against a 120 budget. As instructed it was not changed. The ledger now enforces the budget in both modes: after one 120 commitment, the second is rejected.

---

## 4. P1: external signals use the one threshold authority

`app/decision/external_signal_gate.py` (new) takes a TradingView candidate through
the canonical chain:

1. It normalises the candidate into a `TradingOpportunity`, using closed candles only and the same regime classifier, volatility and market-quality inputs as the ensemble.
2. `AdaptiveEntryThresholdEngine` evaluates it.
3. The `AdaptiveThresholdDecision` is **persisted and read back before anything can execute**.
4. `TradingDecisionEngine` compares.

`process_external_signal_candidate` runs this immediately after market data and
before the PolicyEngine. A second guard directly before the executor call refuses
anything without a persisted, passing threshold decision.

There is no second calculator, and the choices are deliberately conservative:

* **No expert evidence.** One external opinion is not ensemble agreement, and counting it as unanimous agreement would lower the bar.
* **No HTF.** It is recorded as unavailable rather than invented.
* **Separate adaptive-state partition** (`external_tradingview/1`). External confidences never enter the ensemble's distribution calibration.

A candidate is rejected explicitly when:

* its side or confidence is invalid;
* there are fewer than 100 closed candles (`OPPORTUNITY_CONTRACT_UNSATISFIED`);
* the threshold evidence cannot be persisted (`THRESHOLD_EVIDENCE_UNAVAILABLE`).

---

## 5. P2: performance calibration source and failure semantics

`positions.risk_amount` never existed; `risk_amount` lives on `trading_decisions`.
Before writing the query, I resolved the economics from the writers rather than
assuming them:

| Question | Answer (from the code) |
|---|---|
| Is `positions.realized_pnl` gross or net? | **Gross**: the sum of each close leg's `(exit − entry) × qty` |
| Fees | `positions.fees` holds **close legs only**; the OPEN fee is never projected onto the position. The complete fee is the sum of every `trade_fills` leg for the position. |
| Partial closes | Accumulate into `realized_pnl` and `trade_fills`; only CLOSED with nothing remaining is complete |
| One position ↔ one decision | Yes (`positions.decision_id`). A position that was ADDED to carries more risk than its decision approved and is excluded. |
| Is `risk_amount` the approved risk? | **It was never written.** `TradingDecision.set_risk()` had no callers, so `risk_amount`, `quantity`, `leverage` and `stop_price` were always NULL. |

Fixes:

* **`REALIZED_R_QUERY`** joins `positions` to `trading_decisions` and computes **R = (gross realised − all fees) / approved risk**. It requires CLOSED, nothing remaining, `risk_amount > 0`, organic provenance on both sides (`PAPER_FORWARD` / `TESTNET` / `LIVE_MAINNET`), no ADDED event, and a known entry fee.
* **The approved sizing is now recorded.** The runner stashes quantity, executed leverage, stop, target, `stop_distance`, and `risk_amount = |entry − stop| × executed qty` as evidence. `apply_evidence` calls `set_risk`.
* **The orchestrated OPEN fill now records its fee.** It was `fee=None`, which made every position's net R unknowable.
* **Failure semantics:**
  * A source failure raises `PerformanceSourceUnavailable` → `UNAVAILABLE`.
  * A scoring failure → `ERROR` (new `CalibrationStatus`).
  * Both are neutral, and neither is ever reported as `INSUFFICIENT_SAMPLE`. The status persists in `threshold_decisions.performance_status`.
* **Tests:**
  * a schema contract test runs the exact query against a freshly migrated DB;
  * fee and partial-close economics;
  * every exclusion;
  * fewer than 30 samples → `INSUFFICIENT_SAMPLE`;
  * an unreadable source → `UNAVAILABLE`;
  * 30 or more → a deterministic, bounded adjustment.

---

## 6. P2: the remaining items

| Item | Fix |
|---|---|
| Paper broker mutations | `PaperBookClient` refuses `place_*`, `cancel_*`, `close_position*`, order create/submit/amend/modify/replace, reduce-only, leverage/margin/position-mode changes and `_signed_post/put/delete`, raising `PAPER_BROKER_MUTATION_FORBIDDEN`. None is forwarded. `cancel_all_orders` is no longer a silent no-op. Public market data passes through. |
| Loader TypeError | Retries without params **only** when `inspect.signature(cls).bind(...)` rejects them. A TypeError raised inside `__init__`, or with no params, propagates. |
| Session semantics | A gate that never ran records `session_status=NOT_EVALUATED`, `session_allowed=None`, both in the ensemble meta and in `build_hold_breakdown`. `blocked` stays False. The same contract applies to the HTF bias veto: `htf_opposed=None` when it did not run, and the four runner trace sites no longer coerce None to False. Historical rows are not rewritten. |
| ML scheduled promotion | `retrain_entry_model_if_ready(operator_approved_promotion=False)`. The scheduled job always dry-runs promotion and reports `AWAITING_OPERATOR_APPROVAL`. Only an explicit operator flag (`--operator-approved-promotion`) copies artifacts. It never writes `.env`, toggles `ML_ENABLED`, activates a model, or touches thresholds or weights. |

---

## 7. Tests

New test files:

| File | Covers |
|---|---|
| `test_test_database_isolation.py` | temp DB, refusals, recorder, provenance, two subprocess refusals |
| `test_expert_error_contract.py` | 7 experts × snapshot contract, sma_cross, vwap 5m, auxiliary cut, fingerprint, ERROR ≠ HOLD, engine fail-closed, ensemble end to end |
| `test_capital_ledger_both_modes.py` | paper/live same gate, first 120 allowed, second rejected, shrink to remainder, fail-closed ×4, executed-leverage margin |
| `test_external_signal_threshold_path.py` | below threshold rejected with exactly one decision, pass persisted, hard block, unpersistable, contract, runner never reaches risk or executor |
| `test_performance_calibration_source.py` | schema contract, net-of-fees R, partials, 8 exclusions, UNAVAILABLE, ERROR, <30, 30+ deterministic |
| `test_paper_broker_mutation_block.py` | 19 mutations refused and never forwarded, public data passes |
| `test_strategy_loader_typeerror.py` | internal TypeError propagates; signature-only fallback |
| `test_session_not_evaluated_semantics.py` | NOT_EVALUATED / None for skipped gates, ensemble end to end |
| `test_ml_scheduled_promotion_safety.py` | dry-run by default, explicit approval only, scheduler grants none |
| `test_classify_test_evidence_provenance.py` | dry-run writes nothing, family scan, apply sets provenance only, refuses without column |

Existing tests updated to the new contracts:

* `test_capital_budget_invariant.py`: two source-order tests now require the gate before the paper/live split.
* `test_entry_protection_never_again.py`: the fixture migrates its DB and gives its managed test bot a non-binding budget. A managed bot with no budget is now rejected by design.

**Full suite**, isolated, run on the final code:

```
TEST_DATABASE_PATH=C:\Users\favou\AppData\Local\Temp\cosmicforge_pytest_fbcc5qpa\test_session.db
TEST_DATABASE_ROLE=test
2598 passed, 1 skipped, 27 warnings, 4 subtests passed in 879.26s (0:14:39)
```

* 0 failed.
* The skip is the isolation probe that must only ever run inside the refusal subprocess.
* The previous full run failed 5 tests, all in `test_entry_protection_never_again.py`: its managed test bot had no budget and was, correctly, now rejected. The fixture was fixed.

**Canonical DB during the full suite** (read-only max-rowid baseline of all 159 tables, taken before the run and compared after):

* Every new row belongs to the live runtime: `bot_a8117dc719fc`, session `rts_5548a9cb6dee44579790`, run `2ee0ef5a31844b1493536e592fb054dd`.
* That includes `events` and `decision_logs`, which carry only a `run_id`.
* **Rows written by pytest: 0.**

---

## 8. Commits

PENDING.

## 9. Restart and live acceptance

PENDING.

## 10. Phase-15 clean evidence boundary

PENDING.

## Final verdict

PENDING.
