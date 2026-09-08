# CosmicForge — Phase 13: production-parity historical replay

**Status: partially delivered.** The foundation is built and tested; the engine
that drives the production runner over it is not. This report says which is
which, because a replay capability that is half-present and described as
finished is worse than one that is honestly incomplete.

| Item | Value |
| --- | --- |
| Package | `backends/bot-backend/app/replay/` |
| Tests | 78 across three modules, all passing |
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

## Not delivered

| § | Requirement | State |
| --- | --- | --- |
| 13.2 | Replay drives the *same* production brain end to end — snapshot → ensemble → `TradingDecisionEngine` → risk → feasibility → `EntryProtection` → `PositionManager` | **Not built.** The pieces exist and Phase 12's harness shows the shape, but nothing yet wires the provider into a real `PaperRunner`. |
| 13.8 | Lifecycle parity: OPEN, TP1, partial, remainder, break-even, trailing, final stop/target, daily close, kill switch through the production lifecycle semantics | **Not built.** Depends on 13.2. |
| 13.10 | Replay emits canonical evidence (`replay_session`, `bot_run`, `trading_cycle`, `market_snapshot`, `trading_decision`, risk, attempt, fill, position, event) | **Partially.** The identity and provenance contract exist; nothing writes the rows yet. |
| 13.12 | Determinism proven by *running* the same replay twice and comparing results | **Contract only.** `replay_hash` proves the inputs match; no test yet runs two replays and compares their outputs. |

The honest summary is that Phase 13's *correctness guarantees* are in place and
its *engine* is not. That ordering was deliberate: an engine built before the
clock would have had to be re-verified afterwards anyway.

---

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
| 13.2 | Same production brain end to end | **NOT STARTED** |
| 13.3 | Deterministic historical clock, no future data | **PASS** |
| 13.4 | Higher-timeframe alignment | **PASS** |
| 13.5 | Explicit fill models | **PASS** |
| 13.6 | Configurable cost model, stored per replay | **PASS** |
| 13.7 | Intrabar ambiguity resolved by an explicit, recorded policy | **PASS** |
| 13.8 | Production lifecycle parity in replay | **NOT STARTED** |
| 13.9 | Replay identity and manifest | **PASS** |
| 13.10 | Canonical replay evidence, provenance-separated | **PARTIAL** |
| 13.11 | Legacy backfill quarantined | **PASS** (labelling not yet applied) |
| 13.12 | Replay determinism | **PARTIAL** — inputs pinned, outputs not yet compared |

`NO_FUTURE_LEAKAGE`: **PASS** for everything the provider serves.
`REPLAY_LIFECYCLE_PARITY`: **NOT STARTED**.
`LEGACY_BACKFILL_QUARANTINED`: **PASS**.

---

## Operator blockers carried forward

Two configuration findings, audited read-only. Neither is fixed here: both are
operator decisions about intent, and guessing at them would be worse than
carrying them forward explicitly.

### Capital over-commitment on `bot_a8117dc719fc`

`scripts/audit_bot_capital_config.py`, against the live configuration:

```
capital_budget             120.0
allocation_type            fixed_amount
allocation_value           120.0
max_open_positions         2
worst_case_exposure        240.0
risk_per_trade             0.0025
risk_budget_per_trade      0.3
implied_position_notional  15.0
minimum_notional           5.0
policy_warnings            []
policy_clamps              []
!!  CAPITAL_OVERCOMMITMENT: 2 slots x 120 = 240 against a budget of 120
    (200% of budget, 120 over)
```

`resolve_effective_bot_policy` already refuses a single allocation larger than
the budget, but it never multiplies by the slot count. So this configuration
resolves **cleanly, with no warning and no clamp** — the runtime cannot see the
problem at all. Two concurrent positions would deploy twice the stated capital.

One correction to the original framing: the 0.30 USDT risk budget is **not**
currently below the minimum notional. At the configured 2% stop it implies a
15.00 position against a 5.00 minimum. It would bite at a stop wider than 6%,
so it is a latent constraint rather than an active one.

Whether to reduce the allocation, reduce the slots or raise the budget is a
decision about how much capital this bot is meant to deploy. The audit reports;
it changes nothing.

### Database role

`DATABASE_ROLE` is `development` on the canonical paper runtime. The role is
recorded in every `runtime_sessions` row and in the ownership lease, so the
mislabel is already propagating into evidence. `DATABASE_ROLES` accepts
`("development", "paper", "research", "live")`, so `paper` is available. Making
the change means an environment edit and a runtime restart, without switching
the database file — deliberately left to the operator, per the same reasoning.

**Phase 16 must not start until both are resolved.**
