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
| Window | 2024-09-15 → 2026-09-14 (730 days) |
| Rows | 1,051,200 × 2 at 1m, plus derived 5m/15m/1h/4h |
| Provenance | `REAL_HISTORICAL` — fixed at construction, not settable |
| `dataset_hash` | `ee07204498be499166bba78d90bded00` |
| Tests | 39 focused Phase 14 tests in `test_research_dataset_contract.py` |

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

Acquired 2026-09-15 from Binance's public klines API. 1m base, 730 days, both
symbols, day-sharded and resumable.

### Quality

| symbol | timeframe | rows | missing | dup | out-of-order | bad OHLC | completeness |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTCUSDT | 1m | 1,051,200 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 5m | 210,240 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 15m | 70,080 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 1h | 17,520 | 0 | 0 | 0 | 0 | 100.0000% |
| BTCUSDT | 4h | 4,380 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 1m | 1,051,200 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 5m | 210,240 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 15m | 70,080 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 1h | 17,520 | 0 | 0 | 0 | 0 | 100.0000% |
| ETHUSDT | 4h | 4,380 | 0 | 0 | 0 | 0 | 100.0000% |

Not one missing minute across 2,102,400 base bars, and the derived counts are
exactly `base / factor` — 1,051,200 / 15 = 70,080 — which is what a correct
aggregation of a gapless series produces.

### Partitions (§14.9)

| partition | window | rows |
| --- | --- | ---: |
| `TRAIN` | 2024-09-15 → 2025-11-26 | 630,720 |
| `VALIDATION` | 2025-11-27 → 2026-03-16 | 157,680 |
| `TEST` | 2026-03-16 → 2026-07-03 | 157,680 |
| `FINAL_HOLDOUT` | 2026-07-04 → 2026-09-14 | 105,120 |

### The final holdout is untouched (§14.10)

`scripts/run_historical_baseline.py` excludes it by default and says so on
every run:

```
final holdout excluded: baselining N of M bars
```

Including it requires `--include-final-holdout`, which prints a warning that
the result must not inform any decision. Nothing in this work has read it.

---

## Final Closure Pass

The 120-day dataset remains as historical evidence, but the certified Phase 14
dataset is now `data/research/binance_1m_btcusdt_ethusdt_2y_v1.manifest.json`.
It was acquired through the existing day-sharded, resumable Binance public
klines pipeline:

| symbol | 1m rows | 5m | 15m | 1h | 4h | completeness |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTCUSDT | 1,051,200 | 210,240 | 70,080 | 17,520 | 4,380 | 100.0000% |
| ETHUSDT | 1,051,200 | 210,240 | 70,080 | 17,520 | 4,380 | 100.0000% |

No duplicate opens, out-of-order rows, inconsistent intervals, non-positive
prices, negative volume, OHLC violations, or missing bars were found. The
manifest records source, venue, provenance, checksums, quality statistics,
chronological partitions, code revision, and schema versions.

Chronological partitions:

| partition | window | rows |
| --- | --- | ---: |
| `TRAIN` | 2024-09-15 → 2025-11-26 | 630,720 |
| `VALIDATION` | 2025-11-27 → 2026-03-16 | 157,680 |
| `TEST` | 2026-03-16 → 2026-07-03 | 157,680 |
| `FINAL_HOLDOUT` | 2026-07-04 → 2026-09-14 | 105,120 |

### Purge / Embargo

`PurgeEmbargoPolicy` and `purge_embargo_partitions()` now explicitly remove
examples whose label horizon crosses a partition boundary and optionally
embargo examples immediately after the boundary. The report persists label
horizon, purge duration, embargo duration, excluded counts, purge ranges, and
embargo ranges. Boundary tests cover the exact case where an example one bar
before a split with a 12-bar future label is purged.

### Materialized Examples

`scripts/materialize_research_examples.py` materialized real Phase 14 examples
from the 2-year dataset:

| item | value |
| --- | --- |
| Example manifest | `data/research/binance_1m_btcusdt_ethusdt_2y_v1_examples_v1.manifest.json` |
| Example dataset | `data/research/examples/binance_1m_btcusdt_ethusdt_2y_v1_examples_v1.jsonl` (ignored artifact) |
| Examples | 128 |
| Provenance | `REAL_HISTORICAL` only |
| Partitions | TRAIN 86, VALIDATION 22, TEST 20 |
| Final holdout | excluded |
| Legacy/synthetic | excluded |
| Horizon | 12 × 15m bars |
| Output checksum | `7e9842c9d229d2579c30a619e451abf9aa004c6f80c7af0c3910b1c9866b83f5` |

Funding, open interest, basis, and liquidation history remain explicitly
unavailable in this free spot-klines dataset and are represented as
`available=false`; no zero fabrication is used.

---

## Training-example contract

`app.research.dataset` now defines an immutable, broker-neutral example
contract:

* `InstrumentIdentity` records venue, venue symbol, canonical symbol,
  instrument type, asset class, base/quote/settlement assets, contract type,
  multiplier, tick size, step size, and optional expiry/option fields.
* `ResearchFeatureContext` contains only information available at decision time:
  raw OHLCV, HTF context, funding/OI/basis observations at or before `t`,
  regime, expert outputs, ensemble state, threshold state, decision state,
  risk context and quality metadata.
* `FutureLabelSet` is separate from features and contains horizon, MFE, MAE,
  gross return, net return, R multiple, TP/SL barrier outcome and explicit
  costs.
* `TrainingExample.example_id` is deterministic from dataset identity,
  instrument, timeframe, decision timestamp, policy/strategy version and schema
  version.

Default training provenance now excludes `SYNTHETIC` and `LEGACY_BACKFILL`.
The canonical research provenance taxonomy is:

`REAL_HISTORICAL`, `PAPER_FORWARD`, `TESTNET`, `BROKER_DEMO`, `SYNTHETIC`,
`LEGACY_BACKFILL`, `LIVE`, `REPLAY`.

The builder rejects unknown provenance, cuts features at `t`, cuts HTF context
at `t`, and uses future rows only for labels. Missing funding/OI/basis is
represented as `available=false`.

---

## Verdict

| § | Item | Status |
| --- | --- | --- |
| 14.1 | Immutable training-example contract | **PASS** |
| 14.2 | Deterministic strategy context | **PASS** contract fields present; production values can be imported without schema change |
| 14.3 | Future outcome labels | **PASS** for MFE/MAE/return/R/barrier labels |
| 14.4 | Cost-adjusted labels | **PASS** with explicit fee/spread/slippage/funding cost fields |
| 14.5 | Provenance on every example | **PASS** with default synthetic/legacy exclusion |
| 14.6 | Historical acquisition, BTCUSDT + ETHUSDT | **PASS** (730 days, multi-year) |
| 14.7 | 1m base, deterministic derivation | **PASS** |
| 14.8 | Data-quality validation, no silent filling | **PASS** |
| 14.9 | Chronological partitions | **PASS** |
| 14.10 | Final holdout untouched | **PASS** |
| 14.11 | Dataset manifest | **PASS** |

`PHASE_14_RESEARCH_DATA_CONTRACT`: **PASS** for schema, builder, provenance
and no-lookahead contract.
`PHASE_14_DATASET`: **PASS** for 730 days at perfect completeness.
`HISTORICAL_DATA_QUALITY`: **PASS**.
`FINAL_HOLDOUT_PROTECTED`: **PASS**.

Overall Phase 14 is **COMPLETE** for the pre-AI research-data gate: immutable
schema, broker-neutral identity, real multi-year BTCUSDT/ETHUSDT data,
strict multi-timeframe derivation, quality gates, provenance controls,
purge/embargo, final-holdout protection, deterministic labels, and real
materialized examples are present and evidenced.
