# CosmicForge — Phase 14: research data contract and historical dataset

The contract came first and the data second, because a dataset acquired before
there is a way to check it is a dataset nobody can trust afterwards.

| Item | Value |
| --- | --- |
| Contract | `backends/bot-backend/app/research/dataset.py` |
| Acquisition | `scripts/acquire_market_data.py` |
| Venue | Binance spot, public klines API |
| Symbols | BTCUSDT, ETHUSDT |
| Base resolution | 1m |
| Window | 2026-05-12 → 2026-09-08 (120 days) |
| Rows | 172,800 × 2 at 1m, plus derived 5m/15m/1h/4h |
| Provenance | `REAL_HISTORICAL` — fixed at construction, not settable |
| `dataset_hash` | `f059310fba212fe4d7f6a45d9cc9e6c1` |
| Tests | 29 |

---

## What the contract guards

Four failure modes, in order of how quietly each one ruins a result.

### 1. A dataset that is not what it claims to be

The manifest (§14.11) records venue, symbols, base and derived timeframes, the
window, per-series row counts, per-series checksums, the quality assessment,
the partitions, the code revision and both schema versions.

`dataset_hash` is computed from the **checksums of the candles**, not from a
filename or a path. A file can be edited in place; a path proves nothing about
its contents.

### 2. A dataset that is silently broken — or silently repaired

`assess_quality` detects missing bars, duplicate opens, out-of-order rows,
inconsistent intervals, non-positive prices, negative volume, OHLC violations
(a high below the close, a low above the open) and unrealistic gaps.

It reports. It never fills anything in. An imputed candle is indistinguishable
from a real one once it is in the file, and that is exactly the kind of error
that makes a backtest confident and wrong.

The distinction the report draws is deliberate:

* a **gap** is a fact about the market data — the series is *incomplete*;
* a **duplicate or out-of-order bar** is a fact about the loader — the series
  is *unusable*, because it cannot be reasoned about at all.

`is_usable` is false only for the second kind.

### 3. Derived timeframes that disagree with their base

5m, 15m, 1h and 4h are **derived** from 1m by whole-number aggregation, never
downloaded separately. Two independently-fetched series disagree at their edges
and nobody notices until a result depends on it.

An incomplete window is **dropped, not approximated**: a 15m bar built from 13
minutes is not a 15m bar, and nothing downstream could tell the difference. A
timeframe that is not a whole multiple of a minute is refused rather than
fudged.

### 4. A split that lets a model see its own future

Partitions are chronological, never shuffled. With a time series a shuffled
split lets a model see the future of its own training window, and every metric
that follows is meaningless.

`guard_final_holdout()` raises `FinalHoldoutViolation` when anything reads a
timestamp inside the final holdout. Calling it at the point of use is cheaper
than discovering afterwards that a number was contaminated.

---

## The dataset

Acquired 2026-09-09 from Binance's public klines API. 1m base, 120 days, both
symbols, day-sharded and resumable.

### Quality

| symbol | timeframe | rows | missing | dup | out-of-order | bad OHLC | completeness |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTCUSDT | 1m | 172,800 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 5m | 34,560 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 15m | 11,520 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 1h | 2,880 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 4h | 720 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 1m | 172,800 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 5m | 34,560 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 15m | 11,520 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 1h | 2,880 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 4h | 720 | 0 | 0 | 0 | 0 | 100.0000% |

Not one missing minute across 345,600 base bars, and the derived counts are
exactly `base / factor` — 172,800 / 15 = 11,520 — which is what a correct
aggregation of a gapless series produces.

### Partitions (§14.9)

| partition | window | rows |
| --- | --- | ---: |
| `TRAIN` | 2026-05-12 → 2026-07-22 | 103,680 |
| `VALIDATION` | 2026-07-23 → 2026-08-09 | 25,920 |
| `TEST` | 2026-08-10 → 2026-08-27 | 25,920 |
| `FINAL_HOLDOUT` | 2026-08-28 → 2026-09-08 | 17,280 |

### The final holdout is untouched (§14.10)

`scripts/run_historical_baseline.py` excludes it by default and says so on
every run:

```
final holdout excluded: baselining N of M bars
```

Including it requires `--include-final-holdout`, which prints a warning that
the result must not inform any decision. Nothing in this work has read it.

---

## Not delivered

**Multi-year acquisition.** The programme asks for multi-year data; this is 120
days. The acquisition tool is the same one either way — it is day-sharded and
resumable, so extending the window is a matter of running it for longer, not of
writing anything further. At the observed rate, ~3 years for two symbols is
roughly 2,200 day-shards and a few hours of wall time.

120 days was chosen because it is enough to make the Phase 15 baseline
meaningfully larger than the 150-evaluation live sample while remaining
verifiable inside one session. **The dataset is not yet large enough to
characterise behaviour across market cycles**, and no conclusion in the Phase
15 report leans on it as if it were.

**§14.1–§14.5, the training-example contract.** The *market data* contract is
built. The per-example research schema — identity, raw context, derivatives
context, deterministic strategy context, future outcome labels, cost-adjusted
labels — is not. It depends on decisions that Phase 15 is supposed to inform
(which strategy context is worth recording), so building it first would have
been guessing.

What exists today that it will need: canonical `trading_decisions` already
carries regime, regime confidence, component signals, supporting and opposing
strategies, buy/sell scores, consensus, raw confidence, every threshold input
and the resolved effective threshold. That is §14.2 in all but name, already
being written on every evaluation.

**Derivatives context** (funding, open interest, basis, liquidations) is not
acquired. The programme says not to fabricate unavailable fields, so it is
absent rather than zero-filled.

---

## Verdict

| § | Item | Status |
| --- | --- | --- |
| 14.1 | Immutable training-example contract | **NOT STARTED** |
| 14.2 | Deterministic strategy context | **PARTIAL** — already recorded on every decision, not yet extracted as examples |
| 14.3 | Future outcome labels | **NOT STARTED** |
| 14.4 | Cost-adjusted labels | **NOT STARTED** (cost model exists, from Phase 13) |
| 14.5 | Provenance on every example | **PASS** for market data (`REAL_HISTORICAL`, fixed) |
| 14.6 | Historical acquisition, BTCUSDT + ETHUSDT | **PASS** (120 days, not multi-year) |
| 14.7 | 1m base, deterministic derivation | **PASS** |
| 14.8 | Data-quality validation, no silent filling | **PASS** |
| 14.9 | Chronological partitions | **PASS** |
| 14.10 | Final holdout untouched | **PASS** |
| 14.11 | Dataset manifest | **PASS** |

`PHASE_14_RESEARCH_DATA_CONTRACT`: **PARTIAL** — the market-data contract is
complete and tested; the training-example contract is not started.
`PHASE_14_DATASET`: **PASS** for 120 days at perfect completeness; multi-year
acquisition remains outstanding.
`HISTORICAL_DATA_QUALITY`: **PASS**.
`FINAL_HOLDOUT_PROTECTED`: **PASS**.
