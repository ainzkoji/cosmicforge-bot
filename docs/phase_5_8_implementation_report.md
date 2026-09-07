# CosmicForge — Phase 5–8 Implementation Report

Batch 2 of the Pre-AI Master Blueprint. Phase 9+ was not started.

---

## A. Baseline

| Item | Value |
| --- | --- |
| Branch | `phase-0-4-runtime-baseline` |
| Starting commit | `8a2ed071` (end of Batch 1) |
| Runtime repair commit | `577435e1` — committed separately, **before** this batch |
| Batch 2 commit | `6d4e6e8d` |
| Interpreter | `backends/venv/Scripts/python.exe` (Python 3.12.2) |
| Test command | `cd backends/bot-backend && ..\venv\Scripts\python.exe -m pytest tests -q` |

### The runtime repair was already in the working tree

`ERROR_STRATEGY_UNAVAILABLE` work (`app/runner/errors.py`, `app/core/bot_health.py`,
`app/runner/system_events.py`, atomic runner construction, honest `last_error`
semantics) was present **uncommitted** when this batch began — complete and
green, but entangled with the files Phases 5–8 needed to edit.

It was committed unmodified as `577435e1` **before** any Phase 5–8 work, so the
two remain separable and rebasable. Root cause, for the record: a broad
`except Exception: print(...)` in `PaperRunner._load_orchestrator` left
`self.orchestrator = None` while the runner stayed registered, so every symbol
on every tick produced `ERROR_STRATEGY_UNAVAILABLE` with no persisted cause.

Two further repair artifacts (`tests/test_runner_import_shadowing.py`,
`docs/runtime_repair_report.md`) appeared later in the session and were swept
into the Batch 2 commit. Both pass; the attribution is imperfect but nothing
was lost.

---

## B. Phase 5 — `run_cycle` authority

### Audit result: mostly already correct

The §5.9 comparison found **no business logic unique to `run_once`**:

```python
def run_once(self, max_symbols: int = 10) -> Dict[str, Any]:
    """Deprecated compatibility wrapper; all business logic lives in run_cycle."""
    logger.warning("[DEPRECATED] PaperRunner.run_once delegates to run_cycle")
    return self.run_cycle()
```

`run_cycle` already owned the canonical sequence, in order:

| Step | Implementation |
| --- | --- |
| 1–2. Resolve state, reconcile | `reconcile_positions_on_startup()` |
| 3. Protection repair | `ensure_protection` / `_persist_protection_result` on PM restore |
| 4. Kill switch | `activate_kill_switch()` |
| 5. Daily/session close | `_run_daily_close_from_cycle()` |
| 6–8. Manage, then decide entry | `step_symbol()` → `_step_symbol_orchestrated()` |
| 9–10. Evidence, health | `[CYCLE_SUMMARY]`, health/metrics |

Nothing was moved. Tests now pin both the ordering and `run_once`'s emptiness so
a second trading system cannot reappear.

### Gap closed: durable daily-close idempotency

Daily close relied on the in-memory position going flat. A restart inside the
close window — or a re-entry on the same symbol — could re-issue a close on
every 10-second tick.

Added `bot_daily_close_marks (bot_instance_id, symbol, close_window)`. The
window identity keeps the date it **started** on, so an overnight window is one
window and not two. The lookup **fails closed**: if a storage error means we
cannot prove a position is unclosed, we do not close it again.

### Reconciliation

Already mode-correct and unchanged: `_step_symbol_orchestrated` reads
`get_position_info` only inside `if self._effective_execution_mode() == "broker"`.
A demo exchange being flat while a paper position is open is expected, not a
mismatch. Tests assert the exchange read sits *after* the broker-mode guard.

---

## C. Phase 6 — Strategy clock and `MarketSnapshot`

### Candle marker persistence

`bot_candle_evaluations (bot_instance_id, symbol, timeframe, last_closed_candle_time)`
with an atomic `claim_candle()`: the first heartbeat on a newly closed candle
claims it, every later heartbeat on the same candle returns `False`. Markers are
independent per bot, per symbol, per timeframe, and survive restart because they
are in the database rather than in memory.

**Catch-up policy:** the marker stores only the *latest* evaluated close time and
`claim_candle` rejects anything at or below it. A bot that was offline therefore
evaluates the latest eligible closed candle **once** on return — it never
replays the intervening candles through the live runtime.

### `MarketSnapshot` — fields added this batch

| Field | Purpose |
| --- | --- |
| `market_snapshot_id` | Correlation key tying opportunity → decision → fills back to one market view |
| `source_environment` | demo / testnet / mainnet provenance |
| `latest_closed_candle_open_time` | Candle identity, not just its close |
| `higher_timeframe_closed_candle_time` | HTF alignment evidence |
| `data_hash` | Deterministic fingerprint of the pinned candles |
| `htf_is_timestamp_aligned()` | Look-ahead detector (below) |

### Fetch count

`SnapshotMarketClient` pins every strategy read to the snapshot. Measured in the
runtime smoke below: **0 component klines fetches** for one full Master Ensemble
evaluation. Every component saw the identical latest candle.

### HTF alignment — closes OLD-R2

`htf_is_timestamp_aligned()` returns `False` when the HTF candle closes *after*
the strategy candle. A 15m decision at 14:45 cannot consult a 1h candle closing
at 15:00. `build()` additionally filters HTF candles that have not closed in wall
time. Both a future HTF candle and an unclosed one are covered by tests.

### `NO_NEW_CANDLE` is no longer a fake HOLD

Before: `{"decision": "HOLD", "reason": "NO_NEW_CANDLE"}` — which claims the
strategy looked and found nothing.

After:

```python
{"symbol": symbol, "decision": "NO_NEW_CANDLE", "evaluated": False,
 "reason_code": "NO_NEW_CANDLE", "timeframe": self.interval}
```

The strategy did not run. That is a different fact from `NO_OPPORTUNITY`, and
the two now have different codes. Position management still runs on every
heartbeat — verified: `position_manager.update_price` executes *before* the
entry gate.

---

## D. Phase 7 — `TradingOpportunity` and `TradingDecisionEngine`

### Confidence authority: before → after

| # | Location | Before | After |
| --- | --- | --- | --- |
| 1 | Master Ensemble Step 6/7 | `buy_pct >= effective_threshold` | **removed** — delegates to the engine |
| 2 | Runner adaptive gate | `min_confidence_gate` passed down and compared | feeds the engine's threshold resolution |
| 3 | SafetyEngine `check_pre_trade` | `confidence < threshold` → `LOW_CONFIDENCE` | suppressed by `confidence_already_approved=True` |
| 4 | PolicyEngine | `ctx.confidence < self.min_confidence` | suppressed by `confidence_already_approved=True` |

**After: exactly one** — `TradingDecisionEngine.evaluate`, asserted at source level
by `test_exactly_one_confidence_comparison_exists_in_the_decision_engine`.

### New package `app/decision/`

| Module | Contents |
| --- | --- |
| `reasons.py` | 48 canonical codes across `CycleReason`, `QualityReason`, `RiskReason`, `ExecutionReason`, `ProtectionReason`, `LifecycleReason` |
| `opportunity.py` | `TradingOpportunity` (frozen), `NoOpportunity`, `build_opportunity()` |
| `decision_engine.py` | `TradingDecisionEngine`, `EntryQualityDecision`, `veto()` |
| `reason_mapping.py` | Legacy `ReasonCode` → canonical adapter |

`TradingOpportunity` deliberately excludes sizing, risk permission and execution
approval — asserted by test. Including any of them would recreate the
multi-authority problem the model exists to remove.

### Behaviour preservation

Master Ensemble's `final_signal` and `final_confidence` are identical for every
input. This moves **authority**, not values:

* direction selection (`buy_pct` vs `sell_pct`) stays in the ensemble — that is
  market interpretation;
* the threshold comparison moves to the engine — that is entry quality.

`consensus_required` is passed as `0.0` on purpose. The ensemble has never
applied a separate consensus gate; switching one on here would silently change
strategy behaviour, which §19 forbids. The engine supports one for opt-in callers.

### Reason taxonomy — three outcomes that no longer collapse

| Code | Meaning |
| --- | --- |
| `NO_OPPORTUNITY` | Nothing pointed anywhere. There was nothing to be confident about. |
| `CONSENSUS_INSUFFICIENT` | A direction existed, but the strategies cancelled out. |
| `ENTRY_CONFIDENCE_BELOW_THRESHOLD` | They agreed on direction, but the setup was weak. |

`LOW_CONFIDENCE` has **no canonical equivalent**. After Phase 7 a downstream
engine emitting it is a duplicate authority, not a reason code, and
`assert_no_duplicate_confidence_authority()` raises on it.

Hard vetoes (HTF, regime, session, event) are applied via `engine.veto()`, which
records the veto as the primary reason and preserves the quality verdict in
`secondary_reasons` — it never re-runs the comparison.

---

## E. Phase 8 — Risk and execution responsibility

### Audited, not assumed — both named bugs were already absent

**R:R (§8.4, OLD-R3)** is already enforced on the **final resolved** entry/SL/TP
in absolute price units:

```python
_risk   = _ep - _sl        # BUY
_reward = _tp - _ep
_rr     = _reward / _risk
if _rr < ctx.min_risk_reward: ...
```

Non-positive risk and reward are rejected *before* the division. The result is
recorded as `gross_risk_reward`, so the cost basis is explicit: **gross, not
cost-adjusted**. Verified against the blueprint's worked example — entry 100,
SL 98, TP 103.6 → risk 2, reward 3.6, R:R exactly 1.8.

**Percentage units (§8.5/§8.6).** `STOP_LOSS_PCT = 0.02` is stored as a
*fraction* and consumed as `float(settings.STOP_LOSS_PCT)` with no second
division. **No active double-`/100` bug exists.** The `/100` occurrences that do
exist were each traced and are correct:

| Site | Input unit | Verdict |
| --- | --- | --- |
| `executor.py:322`, `runner.py:990` — `allocation_value / 100` | human percentage (10 = 10%) | correct |
| `sizing_engine.py:117` — `custom_trade_amount_value / 100` | human percentage | correct |
| `safety_engine.py:57` — `normalize_threshold` | divides only when `value > 1.0` | correct |
| `drawdown.py`, `pnl_pct`, `atr_pct`, `margin_level` | fraction → display percentage | correct |

Both are now pinned by tests, including the blueprint's exact ATR regression:
BTC at 100,000 with ATR 1,000 and multiplier 2 → stop distance 2,000, stop
fraction **0.02**, explicitly asserted *not* to be 0.0002.

### Responsibility split

`RISK_*` answers *can this account afford this trade, and at what size?*
`EXECUTION_*` answers *can this already-sized trade be placed right now?* The two
namespaces are asserted disjoint. Min-notional is **execution**-owned; stale
data, spread and liquidity are **execution**-owned; drawdown, capacity,
leverage, correlation and R:R are **risk**-owned.

No `RISK_*` or `EXECUTION_*` code contains the substring `CONFIDENCE` — asserted
by test. The executor is asserted to contain no confidence comparison at all.
`EntryProtection` remains the duplicate/idempotency barrier; the decision engine
is asserted (over code, not docstrings) to contain no duplicate-protection and
no sizing logic.

---

## F. Files changed

### New

| File | Purpose |
| --- | --- |
| `app/decision/__init__.py` | Package surface |
| `app/decision/reasons.py` | Canonical reason taxonomy |
| `app/decision/opportunity.py` | `TradingOpportunity`, `NoOpportunity` |
| `app/decision/decision_engine.py` | `TradingDecisionEngine`, `EntryQualityDecision` |
| `app/decision/reason_mapping.py` | Legacy → canonical adapter |

### Modified

| File | Change |
| --- | --- |
| `app/strategy/master_ensemble.py` | Step 6/7 produce an opportunity and delegate the one comparison; decision evidence in `meta` |
| `app/runner/market_snapshot.py` | `market_snapshot_id`, `source_environment`, candle-time fields, `data_hash`, `htf_is_timestamp_aligned()` |
| `app/runner/runner.py` | `NO_NEW_CANDLE` no longer a HOLD; daily-close idempotency helpers |
| `backends/shared/shared_lib/persistence/db.py` | `bot_daily_close_marks` table |

### Tests

| File | Tests |
| --- | --- |
| `tests/test_trading_decision_engine.py` | **New** — 40 |
| `tests/test_strategy_clock_and_snapshot.py` | **New** — 34 |
| `tests/test_risk_execution_responsibility.py` | **New** — 43 |

**117 added, 0 deleted, 0 skipped, 0 weakened.**

---

## G. Test results

```
cd backends/bot-backend && ..\venv\Scripts\python.exe -m pytest tests -q
```

| | Passed | Failed | Skipped | Errors | Warnings |
| --- | --- | --- | --- | --- | --- |
| Start of batch (with runtime repair) | 1,795 | 0 | 0 | 0 | 27 |
| After Phase 7 core refactor | 1,799 | 0 | 0 | 0 | 27 |
| **Final** | **1,916** | **0** | **0** | **0** | **27** |

Plus 4 subtests passed. Reconciliation: 1,799 + 117 = 1,916.

No test submitted a real order — the Batch 1 autouse transport guard remains
active for the whole suite.

---

## H. Runtime proof

The runtime repair is merged (`577435e1`), so the pipeline was exercised
in-process against the real `MasterEnsembleStrategy` with a 320-candle series
and an aligned 4h HTF series:

```
snapshot id        : ms_6e76dff98f414e8e
component fetches  : 0            ← every component used the one snapshot
HTF aligned        : True
regime             : WEAK_TREND   ← real regime evidence, not UNKNOWN
signal/confidence  : HOLD 0.4
primary reason     : ENTRY_CONFIDENCE_BELOW_THRESHOLD
opportunity_id     : opp_f9ac2c8a320d408e
market_snapshot_id : ms_6e76dff98f414e8e   ← propagated into the opportunity
ENTRY QUALITY      : approved=False reason=ENTRY_CONFIDENCE_BELOW_THRESHOLD
                     raw=0.4 thr=0.55 src=dynamic_threshold
OPPORTUNITY        : side=SELL raw_confidence=0.4 supporting=['supertrend']
```

Confirmed: no `ERROR_STRATEGY_UNAVAILABLE`; one `MarketSnapshot` id per
evaluation, propagated to the opportunity; one entry-quality decision; real
regime evidence; a natural non-trade (acceptable per §17).

Two earlier runs with different synthetic series returned `REGIME_BLOCKED`
(STRONG_TREND) and `SESSION_BLOCKED`, showing the pre-opportunity filters still
fire ahead of the decision layer.

**What has NOT been verified:** the live demo bot. `bot_e5fe913972a9` is still
blocked on `CAPITAL_BUDGET_REQUIRED` (legacy NULL capital, see Batch 1 §15), so
the "same 15m candle is not re-evaluated" and "broker environment remains demo"
checks cannot be observed on the real bot until an operator sets a budget.

---

## I. Remaining issues

### Blocking (for declaring the batch fully accepted)

1. **Live-bot runtime smoke not performed.** `bot_e5fe913972a9` is
   configuration-blocked. Requires an operator to set an explicit capital
   budget, or to retire the row. The value is a business decision and was not
   inferred.

### Non-blocking

2. **Pre-opportunity filters emit no opportunity evidence.** `REGIME_BLOCKED`,
   `SESSION_BLOCKED`, volatility-spike and stale-data paths return via the
   ensemble's early `_hold()` before Step 6/7, so `meta` carries no
   `entry_quality` or `opportunity` block. Architecturally correct — they are
   not quality verdicts — but §7.12 observability is thinner on those paths.
3. **Tests write to the production audit log** (carried over from Batch 1).
   `deploy_auto_pilot` fixtures append to `logs/live_audit.jsonl`. Not a
   database contamination.
4. **`legacy_secondary_confidence_gate`** remains in Master Ensemble as an
   explicit opt-in for unmigrated callers. Nothing in the active path sets it.

### Phase 9+

5. Canonical `TradingDecision` persistence — the opportunity/decision objects
   carry stable identifiers ready for it, but nothing writes them to a canonical
   table yet.
6. Readiness/product-safety completion (Phase 10), evidence restructuring
   (Phase 11), replay/AI (Phase 12+).

### Strategy-quality research (explicitly not touched)

7. No threshold, weight, activation matrix or consensus value was changed. The
   `consensus_threshold=0.40` the ensemble carries is still not enforced as a
   gate — enabling it is a strategy decision, not an architectural one.

---

## Final verdict

```
PHASE_5_RUN_CYCLE_AUTHORITY:        PASS
PHASE_6_STRATEGY_CLOCK:             PASS
CANONICAL_MARKET_SNAPSHOT:          PASS
HTF_TIMESTAMP_ALIGNMENT:            PASS
PHASE_7_TRADING_OPPORTUNITY:        PASS
TRADING_DECISION_ENGINE:            PASS
SINGLE_ENTRY_QUALITY_AUTHORITY:     PASS
PHASE_8_RISK_RESPONSIBILITY:        PASS
EXECUTION_FEASIBILITY_RESPONSIBILITY: PASS
RESOLVED_RR_ENFORCEMENT:            PASS
PERCENTAGE_UNIT_NORMALIZATION:      PASS
FULL_TEST_SUITE:                    PASS  (1916 passed, 0 failed, 0 skipped)
RUNTIME_SMOKE_AFTER_INTEGRATION:    PENDING
SAFE_TO_PROCEED_TO_PHASE_9:         NO
```

`RUNTIME_SMOKE_AFTER_INTEGRATION` is **PENDING**, not PASS: the in-process
pipeline smoke passed, but the live demo bot cannot run while it is
configuration-blocked, so the on-bot checks in §17 are unobserved.

`SAFE_TO_PROCEED_TO_PHASE_9` is therefore **NO** — per §20, unit tests alone do
not complete the batch.

**One operator action unblocks both:** set an explicit capital budget for
`bot_e5fe913972a9` (or retire it), let it run one 15m candle cycle, and confirm
the §17 checklist.

Phase 9 was not started.
