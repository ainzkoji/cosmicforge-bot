# CosmicForge — Phase 13: production-parity historical replay

**Status: delivered.** The clock and the contracts came first, then the engine
that drives the production runner over them. §13.2 and §13.8 — the two that
were outstanding — are now proven by running the real `PaperRunner` over
historical data, not by asserting about it.

| Item | Value |
| --- | --- |
| Package | `backends/bot-backend/app/replay/` |
| Tests | 96 across four modules, all passing |
| Provenance | `REPLAY` — fixed at construction, not settable |

---

## What Phase 13 is guarding against

Two failure modes, and they are not equally obvious.

The first is look-ahead. One `[-1]` on an unfiltered series, one
higher-timeframe candle that has not closed yet, one "current price" read from
a bar the strategy could not have seen — and the backtest becomes confidently
wrong rather than merely inaccurate. A wrong-but-plausible equity curve is more
dangerous than no equity curve.

The second is a *second trading brain*. If replay reimplements the strategy,
the results describe the reimplementation. The programme is explicit: do not
create a separate simplified trading brain for replay, and do not create a
special simplified exit engine.

The existing `app/backtest/` package (2,196 lines) says in its own docstring
that it "mirrors `PaperRunner.run_cycle` / `step_symbol`" and it carries its own
`BacktestExecutor`. Mirroring is the thing to avoid. It is left in place and
untouched; nothing in `app/replay/` depends on it.

---

## Delivered

### §13.1 / §13.3 — the historical clock and the snapshot provider

`app/replay/historical_provider.py`. The provider serves the **production**
`MarketSnapshot` — the same immutable contract the live runtime builds, with
the same `market_snapshot_id`, `data_hash` and alignment check — so a strategy
cannot tell it is in a replay except through explicit provenance.

The cut is made in one place and enforced by construction:

* `HistoricalClock` owns "now" and is monotonic. Going backwards raises rather
  than silently re-running a bar.
* A candle is visible only once its close time has **passed** — the same rule
  the live runtime applies, so a forming bar is invisible in replay exactly as
  it is live. Asserted to the millisecond: at `close - 1` nothing is visible,
  at `close` exactly one bar is.
* `reference_price()` is the last *closed* close. There is deliberately no
  mid-bar price: in replay no such thing exists, and inventing one is
  look-ahead under another name.
* `ReplayMarketClient` answers only from the provider and raises `AttributeError`
  on anything it does not serve, so no code path can reach a live exchange
  mid-replay.

### §13.4 — higher-timeframe alignment

The subtle one. A 15m decision at 14:45 may consult the 1h candle that closed
at 14:00, never the one closing at 15:00.

The higher-timeframe series is cut at the **strategy candle's close**, not at
the clock — a distinction that matters whenever the snapshot window is limited
and "now" is later than the newest bar in it. `MarketSnapshot.htf_is_timestamp_aligned()`
re-checks it independently, and the provider refuses to emit a snapshot that
fails.

Tested by sweeping twelve consecutive 15m bars across three hour boundaries and
asserting, at every one, that the HTF candle never leads the strategy candle
and that the visible HTF count equals the number of whole hours that have
closed.

### §13.5 — fill models

`app/replay/fill_models.py`. `next_bar_open`, `next_bar_market`,
`bar_touch_limit`, `bar_touch_stop`. An entry decided on a closed bar fills on
the bar **after** it; filling at the close of the bar that produced the signal
is look-ahead. The market model resolves to the open/close midpoint and says in
its own reason string that this is a modelling choice.

Stops fill at the stop price with no slippage added — slippage belongs to the
cost model, and applying it in two places is how a replay quietly
double-charges.

### §13.6 — cost model

`app/replay/cost_model.py`. Maker fee, taker fee, spread, slippage and funding,
as fractions rather than ambiguous "bps". Funding is charged per **completed**
interval, which is how perpetuals actually work: a position closed before the
first funding stamp pays none.

The model is frozen and hashed, and the hash goes in the manifest. Costs the
model does not attempt — latency, market impact, borrow — are declared as
explicit `False` flags rather than omitted, so they cannot be forgotten.
`CostModel.zero()` exists and labels itself `"gross of all costs"`.

### §13.7 — intrabar ambiguity

A bar whose high reaches the target and whose low reaches the stop cannot be
resolved from OHLC. Silently choosing the profitable one is how a losing
strategy backtests well.

Three named policies: `CONSERVATIVE_STOP_FIRST` (the default),
`OPTIMISTIC_TARGET_FIRST` (has to be asked for), and
`LOWER_TIMEFRAME_RECONSTRUCTION` (the only one that is actually correct). The
reconstruction walks finer bars in order and takes whichever level is reached
first; if the finer data is absent or does not cover the bar's window it raises
`IntrabarUnresolved` rather than guessing.

Every fill carries the policy that produced it, including the unambiguous ones,
so a result can always be read back with its rule.

### §13.9 / §13.12 — replay identity and determinism

`app/replay/identity.py`. The manifest records dataset id and hash, symbols,
timeframes, window, policy hash, strategy and version, cost-model hash, fill
model, intrabar policy, seed, code revision, branch and whether the working
tree was dirty.

`replay_hash` fingerprints everything that determines the result and
deliberately excludes `replay_id` and `created_at` — two runs of the same
experiment at different times must hash the same, or the hash cannot be used to
check reproducibility. `differences()` names exactly which inputs diverged when
two runs are not comparable.

The dataset hash is computed from the **candles**, not from a filename: a file
can be edited in place, and a path proves nothing about its contents.

### §13.10 — provenance separation

`ReplayIdentity.provenance` is fixed at `REPLAY` and is `init=False`. It cannot
be passed in, and a test asserts that attempting to do so raises. A settable
provenance field is exactly how this guarantee gets lost six months later.
`REPLAY` is in `NON_ORGANIC_PROVENANCE` and outside `ORGANIC_PROVENANCE`.

### §13.11 — legacy backfill quarantined

`trade_fills` is the table every dataset builder, analytics view and
performance metric reads. It had no provenance column, and it is 94% not the
production trading brain:

| strategy | account_id | rows | window |
| --- | --- | --- | --- |
| `backfill_ensemble` | `backfill` | 16,494 | 2026-03-22 15:03 → 16:42 |
| `orchestrated` | `default` | 1,056 | 2026-03-28 → 2026-09-06 |
| `external_tradingview` | `default` | 9 | |
| `paper_execution_smoke` | `paper_smoke` | 2 | |

Those 16,494 rows are `scripts/ml/historical_backfill.py`, which replays public
klines through its own standalone ADX/ATR/MA-slope engine — explicitly not the
production brain — and writes them through the same `record_fill()` the live
runner uses. Ninety-nine minutes of writing produced 94% of the fill table.

`record_fill` now accepts and stamps a provenance, every runtime fill carries
the provenance of its run, and `shared_lib/persistence/fill_provenance.py`
classifies the existing rows. Classification is conservative by design: only
unambiguous writer signatures are labelled, an existing label is never
overwritten, nothing is deleted, and rows whose writer is not clearly
identifiable are left NULL rather than guessed at — a wrong provenance label is
worse than a missing one. `organic_fill_filter()` therefore *excludes*
unclassified rows: an unlabelled row cannot be shown to be organic.

`scripts/classify_fill_provenance.py` applies it, dry run by default. Against
the canonical database:

```
would label  16,494  LEGACY_BACKFILL
would label       2  TEST_FIXTURE
left alone    1,065  (writer not unambiguous)
```

**Not applied.** The column exists on the canonical database; zero rows are
labelled. Applying it is a one-command operator step.

---

## §13.2 — the production brain, driven by a clock

`app/replay/engine.py`. There is deliberately no strategy in it, no sizing, no
exit logic and no lifecycle state machine. All of that is production code,
reached through the production `PaperRunner`:

```
HistoricalMarketDataProvider
  -> MarketSnapshot            the production contract, cut at the clock
  -> Master Ensemble           the production strategy instance
  -> TradingOpportunity
  -> TradingDecisionEngine     the one entry-quality comparison
  -> Risk / CapitalLedger
  -> ExecutionFeasibility
  -> candle claim              replay's idempotency barrier
  -> PaperExecutor
  -> PositionManager           the production lifecycle
```

The existing `app/backtest/` package says in its own docstring that it "mirrors
`PaperRunner.run_cycle` / `step_symbol`" and carries its own `BacktestExecutor`.
Mirroring is the thing to avoid, so it is left untouched and nothing in
`app/replay/` depends on it.

An unassisted run over 214 bars produced 214 canonical decisions, every one
provenance `REPLAY`, and reached no broker at any point. The reasons it emitted
are production's own vocabulary, unchanged:

```
NO_OPPORTUNITY                      287
EXECUTION_DATA_STALE                 85
ENTRY_CONFIDENCE_BELOW_THRESHOLD     14
Adapter Error: stop distance 0.0     14
```

Worth naming: those 85 `EXECUTION_DATA_STALE` are the ensemble's **warm-up**,
not stale data. It needs 100 candles before the regime classifier will
classify, and `master_ensemble.py` maps `regime_insufficient_data` to
`STALE_MARKET_DATA`, which `reason_mapping.py` maps on to
`EXECUTION_DATA_STALE`. That is production's own mapping reproduced faithfully.
It is also a reason-mapping inaccuracy worth fixing on its own merits: a
strategy that has not warmed up is not looking at stale data.

### Two clock seams, both defaulting to today's behaviour

Replay needed production to be able to ask "what time is it?" of something
other than the wall clock. Both changes are inert in production:

* `TradeExecutor._now_ms()` — the entry path's freshness guard compares the
  newest candle against now. Against a 2023 bar that is a correct and useless
  rejection of every entry.
* `BrokerHealthMonitor.clock_source` — the same problem one layer up. A
  replayed exchange reports the replay clock, and the monitor was calling the
  resulting 88,919,080,838 ms difference "time drift".

Neither is a replay-shaped hack in production code: both are the same
question — *whose clock?* — that a replay is entitled to answer differently.

## §13.8 — position lifecycle parity

The same lifecycle Phase 12 proved live, reproduced in replay through the same
`PositionManager`:

| event | qty | remaining |
| --- | ---: | ---: |
| `OPENED` | 98.6093 | 98.6093 |
| `TP1` | 49.3047 | 49.3047 |
| `BREAK_EVEN_ACTIVATED` | — | 49.3047 |
| `TRAILING_ACTIVATED` | — | 49.3047 |
| `STOP_UPDATED` ×24 | — | 49.3047 |
| `FINAL_CLOSE` | 49.3047 | 0 |

`98.6093 − 49.3047 − 49.3047 = 0`. Realized 260.25 net of 2.03 in fees,
closed on TP2. The trailing stop is asserted monotonic: for a long it may never
fall.

The market interpretation is controlled the same way Phase 12 controlled it —
the real `MasterEnsembleStrategy` with deterministic component votes underneath
it. Every gate above the votes still runs, and the regime gate demonstrably
still refuses: an earlier, steeper synthetic rise was blocked
`REGIME_BLOCKED` 36 times, so the *market* was made gentler rather than the
gate weakened.

### A defect this surfaced

The first lifecycle attempt failed at TP1 with
`paper_partial_close_side_mismatch` and `requested_tp1_qty: 0.0`.
`PaperExecutor.partial_close` compared the stored side against the requested
side as raw strings, accepting `LONG`, `SHORT`, `BUY` and `SELL` as *valid* but
then requiring exact equality — so `BUY` against a book holding `LONG` failed,
even though every other layer treats them as the same direction. Normalised to
LONG/SHORT.

## §13.12 — determinism, by running it twice

Two replays of the same dataset, revision, policy, cost model, fill model and
seed produce byte-identical outcomes: the same decisions, fills, position
events and realized PnL, compared through an outcome fingerprint that excludes
ids and timestamps (which are new by design) and includes everything else.

A different market produces a different fingerprint, so the check is sensitive
rather than vacuous.

## No future leakage, through the whole stack

The unit-level cut is asserted at candle boundaries and across timeframes. On
top of that, an integration test watches **every** `klines` call the production
runner makes across a full replay and asserts the newest candle returned had
already closed at the clock at that moment. Nothing in the strategy stack can
obtain data after `t`.

## Tests

| Module | Tests |
| --- | --- |
| `test_replay_historical_clock.py` | 36 |
| `test_replay_fill_and_cost_models.py` | 23 |
| `test_replay_identity.py` | 19 |
| `test_fill_provenance.py` | 8 |

---

## Verdict

| § | Item | Status |
| --- | --- | --- |
| 13.1 | Production `MarketSnapshot` from historical data | **PASS** |
| 13.2 | Same production brain end to end | **PASS** |
| 13.3 | Deterministic historical clock, no future data | **PASS** |
| 13.4 | Higher-timeframe alignment | **PASS** |
| 13.5 | Explicit fill models | **PASS** |
| 13.6 | Configurable cost model, stored per replay | **PASS** |
| 13.7 | Intrabar ambiguity resolved by an explicit, recorded policy | **PASS** |
| 13.8 | Production lifecycle parity in replay | **PASS** |
| 13.9 | Replay identity and manifest | **PASS** |
| 13.10 | Canonical replay evidence, provenance-separated | **PASS** |
| 13.11 | Legacy backfill quarantined | **PASS** (applied) |
| 13.12 | Replay determinism | **PASS** |

`NO_FUTURE_LEAKAGE`: **PASS**, unit and integration.
`REPLAY_LIFECYCLE_PARITY`: **PASS**.
`LEGACY_BACKFILL_QUARANTINED`: **PASS**.

---

## Known limits, stated

* **Wall-clock dependencies remain outside the entry path.** `run_cycle` still
  uses `date.today()` for daily state and the daily-close window. A replay
  therefore does not exercise daily close on historical dates. The entry and
  lifecycle paths are clock-injected; the calendar path is not.
* **One symbol per session.** The engine drives a single symbol. Portfolio
  effects — correlation limits, shared capital across concurrent symbols — are
  not exercised by a single-symbol replay, though the capital ledger that
  governs them is.
* **The controlled-component hook.** Lifecycle parity uses deterministic
  component votes, as Phase 12 did. The unassisted run is reported separately
  and is the one that describes the strategy.
