# CosmicForge — Phase 12: full paper lifecycle proof

Workstream B. The previous Phase 12 result was produced by a test suite that
called the canonical evidence writers directly. This one drives the real
`PaperRunner` and reports what the database actually held afterwards.

That difference turned out to matter a great deal.

| Item | Value |
| --- | --- |
| Validation bot | `bot_phase12_live` (fresh; `status='validation'`) |
| User / broker | `user_phase12_live` / `brk_phase12_live` |
| Symbol / timeframe | BTCUSDT / 1m, HTF 15m |
| Execution mode | `paper`, broker environment `demo` |
| Provenance | `PAPER_FORWARD_VALIDATION` (outside `ORGANIC_PROVENANCE`) |
| Database | `backends/shared/shared_lib/persistence/phase12_paper_validation.db` |
| Harness | `scripts/phase12/` |
| Suite after changes | see **Tests** below |

`bot_a8117dc719fc` and `bot_e5fe913972a9` were not touched. The validation bot
is deliberately not `status='active'`, so the canonical scheduler — which
selects `WHERE status='active'` — can never pick it up and drive it with a real
broker client.

---

## Why a separate database

The harness refuses to run against `cosmicforge.db` and says so:

```
refusing to run the Phase 12 harness against the canonical runtime database.
A synthetic position there would contaminate operational health, position
counts and readiness evidence.
```

The proof needed is of the *runtime*, not of one database file: same
`resolve_effective_bot_policy`, same `BotRunContext`, same `PaperRunner`, same
`TradingOrchestrator`, same schema built by the same `migrate()`. A synthetic
OPEN position in the production evidence database would appear in
`/operations/health` position counts and in every readiness query, which is
exactly the evidence mixing the programme forbids. The restart proof still
needs a durable file, and it has one.

---

## What the controlled opportunity actually controls

`scripts/phase12/controlled.py` replaces the ensemble's **seven component
strategies** with deterministic ones. Nothing downstream is touched:

```
component strategies      <- CONTROLLED (the market interpretation)
  -> regime classification            real
  -> regime weighting / aggregation   real
  -> dynamic threshold resolution     real
  -> build_opportunity                real
  -> TradingDecisionEngine.evaluate   real   <- the one quality comparison
  -> HTF bias veto                    real
  -> Risk / ExecutionFeasibility      real
  -> EntryProtection                  real
  -> PaperExecutor / PositionManager  real
```

An earlier draft overrode the ensemble's own decision step. That was wrong: it
would have stepped over the regime, session, volatility and HTF gates, which
the programme explicitly forbids disabling. It was replaced.

The gates are demonstrably still live. The first synthetic market was refused:

```
regime=STRONG_TREND  adx=100.0  regime_gate_result=blocked
execution_block_reason=REGIME_BLOCKED_STRONG_TREND
```

That refusal was correct, so the *market* was changed, not the gate.
`scripts/phase12/calibrate.py` sweeps the real `RegimeClassifier` over the
generator and the harness is pinned to the least trend-extreme tradeable point
it found (WEAK_TREND, ADX ~34). The approved evaluation reads:

```
signal=BUY  confidence=1.0  effective_threshold=0.7  threshold_type=adaptive_engine
regime=WEAK_TREND  htf_opposed=false  approved=true  quality_reason=APPROVED_FOR_EXECUTION
```

No production threshold was lowered anywhere. `REGIME_BLOCKED` still appears in
the run's decision counts, on a cycle where regime hysteresis moved.

---

## The headline finding: the canonical chain had no writer

Driving the real runner and then reading the database showed the lineage
stopping dead after the decision. The same query against the **production**
runtime database:

| table | rows |
| --- | --- |
| `runtime_sessions` | 28 |
| `bot_runs` | 17 |
| `trading_cycles` | 2,598 |
| `market_snapshots` | 62 |
| `trading_decisions` | 4,478 |
| **`execution_attempts`** | **0** |
| **`positions`** | **0** |
| **`position_events`** | **0** |
| **`risk_events`** | **0** |
| `trade_fills` | 17,561 |

`app/evidence/writers.py` defines `open_execution_attempt`,
`record_position_opened`, `update_position_quantities` and
`record_position_event` correctly. A search for their call sites outside the
module and the tests returned nothing. They had never been wired.

So §16.5 — *every trade must be reconstructable: decision → execution attempt →
fill → position → lifecycle → close → realized result* — was not achievable
from canonical evidence, regardless of what the tests reported. The earlier
Phase 12 PASS was a test of the writers, not of the runtime.

**`app/evidence/fill_bridge.py`** is the missing edge:

* `record_fill_with_evidence` wraps `record_fill`, so a fill and its canonical
  position row are written together. All eight fill sites in `runner.py` route
  through `PaperRunner._record_fill`; the signature is unchanged, so only the
  callee name moved.
* `execution_attempt` brackets the executor, so an attempt that never produces
  an order still leaves a row saying we tried and why it failed. All five
  `executor.execute_signal` sites route through
  `_execute_signal_with_evidence`.
* `sync_lifecycle_events` watches the PositionManager for break-even, trailing
  and stop moves — transitions that produce no fill.

Two design points worth stating:

* **Evidence failure never blocks trading.** A failed write is reported as
  `CANONICAL_EVIDENCE_WRITE_FAILED`; refusing to manage an open position
  because a log row failed is the more dangerous failure.
* **The decision id is allocated before the evaluation runs.** Execution
  happens *inside* the evaluation, so the attempt, fill and position all need
  to name their decision before `record_decision()` finalizes the row.

### Provenance is inherited, not re-derived

`run_provenance()` reads the `bot_run` this evidence belongs to. In production
that agrees with the old derivation, because the run is opened with exactly it.
They diverge where it matters: this validation run is opened as
`PAPER_FORWARD_VALIDATION`, so every decision, attempt, position and event it
produces carries that label. Verified across the whole run:

```
trading_decisions    [{'provenance': 'PAPER_FORWARD_VALIDATION', 'n': 4}]
execution_attempts   [{'provenance': 'PAPER_FORWARD_VALIDATION', 'n': 2}]
positions            [{'provenance': 'PAPER_FORWARD_VALIDATION', 'n': 1}]
position_events      [{'provenance': 'PAPER_FORWARD_VALIDATION', 'n': 6}]
```

Not one organic row.

---

## Four further defects, each found by running the thing

### D-1 — a paper position looked flat to the runtime

`ensure_protection` asked `client.get_position_amt(symbol)` and, seeing zero,
concluded the position was flat and let the caller mark the PositionManager
flat. In paper mode that reading is *always* zero: the fill exists only in
`PaperExecutor`, no order was ever sent. Observed on the cycle after the open:

```
lifecycle: phase=FLAT  reconciliation_reason=PM_RESTORE:exchange_flat
```

So TP1, break-even and trailing could never advance for a paper position, and
the earlier smoke *had* to call the writers directly — the runtime could not
have produced that lifecycle.

Fixed by `app/execution/paper_book_client.py`. In paper mode the exchange *is*
the paper book: market data passes through to the real client, questions about
positions and orders are answered from `PaperExecutor`. Wrapping the client
fixes every reconciliation reader at once — including ones nobody has found —
and, being a property, survives `MultiBotRunner` reassigning `.client` each
cycle, which a one-shot wrap in `__init__` would not.

### D-2 — a paper bot was calling broker order endpoints

The same function called `cancel_all_orders(symbol)` unconditionally, and the
protection path called `place_protection`. The harness's client raises on every
order method, which is how they were caught:

```
blocked broker calls: ['cancel_all_orders', 'cancel_all_orders']
blocked broker calls: ['place_protection', 'place_protection', 'place_protection']
```

`ensure_protection` now returns early in paper mode: protection there is
bookkeeping, and broker-side protection for a position that exists only in the
paper book would be orphaned. Final state of the whole lifecycle:
`blocked broker calls: []`.

Three tests in `test_lifecycle_persistence.py` were exercising the *broker*
protection path while inheriting `EXECUTION_MODE=paper` from conftest. They now
say `executor.execution_mode = "live"` explicitly. They were only passing
because paper mode wrongly reached the broker.

### D-3 — TP1 could never fire

`PositionManager.update_price` sets `pos.phase = TP1_TAKEN` and *then* returns
`"HIT_TP1"`. The runner calls `execute_tp1_partial_close`, whose duplicate
guard rejected anything already in `{TP1_EXECUTING, TP1_FILLED, TP1_TAKEN,
RUNNER_TRAILING, EXITING}`.

The transition that triggers TP1 was the one the guard treated as proof TP1 had
already happened. Every TP1 was swallowed:

```
requested_tp1_qty: 0.0   normalized_tp1_qty: 0.0   fill_qty: 0.0
failure_reason: 'TP1_DUPLICATE_IGNORED'   skipped: True
```

`TP1_TAKEN` is documented in the enum as a *legacy alias*, set at detection.
The states that genuinely mean "in flight or finished" are `TP1_EXECUTING` and
`TP1_FILLED`, both set by the executor itself. `TP1_TAKEN` was removed from the
guard.

This is consistent with the historical data. Restricted to the *real*
orchestrated runner — `strategy='orchestrated'`, excluding the 16,494 legacy
`backfill_ensemble` rows — `trade_fills` holds 567 opens and 489 closes, of
which **26 closed with `exit_reason='TP1'` and not one is a `PARTIAL_CLOSE`**:

```
action  exit_reason   n
OPEN    NULL        567
CLOSE   SL          220
CLOSE   OTHER       137
CLOSE   TIME_EXIT    60
CLOSE   TP2          46
CLOSE   TP1          26     <- full closes, never partials
```

TP1 always became a full close. The partial-close leg of the strategy had never
run.

### D-4 — a mis-scoped `else` erased the post-TP1 lifecycle

The PositionManager "restore" branch was the `else` of the trailing-stop-update
condition:

```python
if (_trail_pos is not None and _trail_pos.sl.be_exchange_confirmed
        and _trail_pos.phase == PositionPhase.RUNNER_TRAILING
        and (_trail_anchor_moved or _trail_desync)):
    ...update the trailing stop...
else:
    ...open_position(...)      # "PositionManager is out of sync"
```

It therefore fired whenever a trailing update simply was not due — almost every
cycle — and re-arming a live position resets phase to `SEEKING_TP1`, clears
`tp1_hit` and restores the original stop. Traced directly:

```
>>> PM.open_position(qty=48.85051488929245) called from runner.py:3712
AFTER phase=SEEKING_TP1 tp1_hit=False stop=101.3295358548
```

TP1, break-even and trailing were destroyed as fast as they were created, and
TP1 was re-armed to fire again on the next tick. The branch is now conditional
on the PositionManager actually lacking a live position for the symbol.

---

## The lifecycle, end to end, through `run_cycle`

Canonical `position_events` for one position, written by the runtime:

| event | qty | remaining | price | realized PnL | stop |
| --- | --- | --- | --- | --- | --- |
| `OPENED` | 97.7010 | 97.7010 | 102.3531 | — | — |
| `TP1` | 48.8505 | 48.8505 | 103.9792 | 79.4375 | — |
| `BREAK_EVEN_ACTIVATED` | — | 48.8505 | — | — | 102.5169 |
| `TRAILING_ACTIVATED` | — | 48.8505 | — | — | 102.5169 |
| `STOP_UPDATED` | — | 48.8505 | — | — | 104.2917 |
| `FINAL_CLOSE` | 48.8505 | 0 | 103.4793 | 55.0171 | — |

Final position row:

```
original_qty 97.7010   remaining_qty 0.0   realized_qty 97.7010
realized_pnl 134.4545  fees 2.0318  status CLOSED  close_reason BREAK_EVEN
```

Invariants:

* `97.7010 − 48.8505 − 48.8505 = 0` — open minus partials minus final close.
* `79.4375 + 55.0171 = 134.4546` — event PnL sums to the position's realized PnL.
* `original_qty − partial_close_qty = remaining_qty` at the TP1 step.

Correlation, all three tables agreeing on one decision:

```
decision   dec_758545fa041d4e5caece  APPROVED_FOR_EXECUTION
attempt    exa_6228a88f79944822b7b9  BUY -> PAPER_POSITION_OPENED
                                     broker_order_id=paper_order_37b7fcbaf172...
position   5232b3b2-d88d-4a54-9fee-cc0d166c16d3  LONG OPEN
```

### The restart (B5)

A **separate process**, new PID and new runtime session, built a fresh runner
and rehydrated from persisted state alone:

```
symbol_state  position=LONG entry=102.35306652 entry_qty=48.85051488929245
              position_id=2917fcab-e16f-41be-ab9a-3dd77d5147d3
PositionManager  phase=RUNNER_TRAILING  tp1_hit=True  is_break_even=True
                 current_qty=48.85051488929245  current_stop=102.51692183403983
                 tp1_price=103.9302  tp2_price=104.0900
canonical position  remaining=48.8505  realized=48.8505  status=OPEN
runner_initialization_status  READY
```

The restored quantity is the **post-TP1 remainder**, not the original, and the
stop is the **break-even** stop, not the original. This is the gap flagged as
"proven by tests, not with a live open paper position"; it is now proven live.

### Daily close (B7)

Five cycles inside the window, then a **fresh process** and three more:

```
position   remaining=0  realized=97.7010  status=CLOSED  close_reason=DAILY_CLOSE
marks      1  ({'close_window': '2026-09-08:21:44', 'reason': 'DAILY_CLOSE'})
DAILY_CLOSE events  1
```

One close, one mark, one event, across eight cycles and a restart. The close
*window* was moved to bracket "now" so the proof need not wait for 23:55
Europe/Rome — that is a schedule, not a safety threshold; the profit floors,
position checks and the idempotency marker were left exactly as configured.

### Kill switch (B8)

Armed through persisted daily state, the way a real daily-loss breach arms it:

```
cycle_status=paused  reason=KILL_SWITCH_ACTIVE  health=PAUSED_KILL_SWITCH
new entry attempts: 0
position: CLOSED  close_reason=KILL_SWITCH_ACTIVE
events: OPENED -> KILL_SWITCH_CLOSE
```

### Duplicate protection (B9)

Four further cycles on the same candle, after a restart, with the controlled
strategy still signalling BUY every evaluation:

```
new_execution_attempts: 0
new_positions: 0
total positions: 1
```

Across the whole first run: 4 decisions, 2 execution attempts (one BUY, one
CLOSE), 1 position.

---

## Open findings — not fixed, deliberately

1. **Re-entry inside the daily-close window.** After the daily close the bot
   opened a new position in a later cycle in the same window. The mark prevents
   a duplicate *close*, nothing prevents a new *entry*. The contract in the
   programme is satisfied; whether a daily close should also block entries for
   the rest of the window is a policy decision for the operator.
2. **`bot_daily_close_marks.position_id` is NULL.** `_mark_daily_close` is
   passed `st.position_id`, which the close has already cleared. Harmless to
   the idempotency key, but it weakens the audit trail.
3. **`FINAL_CLOSE` carries no fee.** The closing fill did not supply a fee
   field the bridge could read, so the position's `fees` reflects the TP1 leg
   only. Realized PnL is unaffected.
4. **A free-text primary reason.** One decision recorded
   `primary_reason='Protective orders validated'` — prose where a canonical
   reason code belongs. `app/decision/reason_mapping.py` should own it.

---

## Tests

| Suite | Result |
| --- | --- |
| `tests/test_phase12_canonical_execution_evidence.py` | **New — 18 passed** |
| `tests/test_lifecycle_persistence.py` | 11 passed (3 annotated for the broker path) |
| Full suite | *(recorded in the acceptance summary)* |

The new module pins each defect: the canonical projection and its accounting
invariant, attempts surviving an executor that raises, provenance inheritance,
restart-durable lifecycle events, `TP1_TAKEN` staying out of the duplicate
guard, `update_price` setting that phase before signalling, the restore branch
being conditional, and paper mode never placing broker protection.

---

## Verdict

| § | Proof | Status |
| --- | --- | --- |
| B1 | Controlled opportunity through the real engine at an unlowered threshold | **PASS** |
| B2 | OPEN: decision → attempt → fill → position → event, correlated | **PASS** |
| B3 | TP1 partial; `original − partial = remaining` | **PASS** |
| B4 | Break-even and trailing on the post-TP1 remainder | **PASS** |
| B5 | Restart restores the remainder, TP1/BE/trailing state, policy hash | **PASS** |
| B6 | Final close to FLAT with realized PnL | **PASS** |
| B7 | Daily close: one close, one mark, restart-safe | **PASS** |
| B8 | Kill switch blocks entries and closes with canonical evidence | **PASS** |
| B9 | At most one execution per opportunity across ticks and restarts | **PASS** |
| B10 | Phase 12 gate | **PASS** |

No broker order API was reached at any point. No mainnet. No production
threshold was lowered. No organic evidence was written.
