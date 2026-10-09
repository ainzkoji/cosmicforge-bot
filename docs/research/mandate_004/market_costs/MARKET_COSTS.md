# Mandate 004: market-cost evidence

Generated 2026-10-09T02:09:38Z from `market_costs.json` (deliverable D11, Step 2.7).

## Status

| Item | Status |
|---|---|
| Measurement system (records, provenance, validation, cost table, versioned calibration) | IMPLEMENTED AND TESTED |
| Observed-cost calibration | **INSUFFICIENT_DATA** |
| Cost model used by the evaluation | unchanged: RETAINED: the frozen mandate's assumed costs (no calibration possible) |

## What was actually observed

| Provenance | Records | What it is |
|---|---|---|
| HISTORICAL_PUBLIC | 0 | published history |
| LIVE_PUBLIC_OBSERVATION | 240 | public top-of-book snapshots (no account, no order) |
| DEMO_EXECUTION | 2 | fills on the exchange demo environment (not representative of real money) |
| SIMULATED | 0 | simulator output |
| ASSUMED | 0 | research assumptions |

Observation days: 2026-10-05, 2026-10-08, 2026-10-09.

### Top-of-book spread, most traded contracts

240 snapshots of 20 contracts. Median of the per-contract medians: 1.17 bps; tightest 0.01 bps, widest 4.28 bps.

| Contract | Snapshots | Median (bps) | 95th percentile (bps) | Largest (bps) | Days | Status |
|---|---|---|---|---|---|---|
| BTCUSDT | 12 | 0.01 | 0.01 | 0.01 | 1 | INSUFFICIENT_DATA |
| ETHUSDT | 12 | 0.04 | 0.04 | 0.04 | 1 | INSUFFICIENT_DATA |
| ZECUSDT | 12 | 0.08 | 0.08 | 0.08 | 1 | INSUFFICIENT_DATA |
| HYPEUSDT | 12 | 0.12 | 0.12 | 0.12 | 1 | INSUFFICIENT_DATA |
| BNBUSDT | 12 | 0.14 | 0.14 | 0.14 | 1 | INSUFFICIENT_DATA |
| ENAUSDT | 12 | 0.47 | 0.47 | 0.47 | 1 | INSUFFICIENT_DATA |
| XRPUSDT | 12 | 0.72 | 0.72 | 0.72 | 1 | INSUFFICIENT_DATA |
| SOLUSDT | 12 | 0.91 | 0.91 | 0.91 | 1 | INSUFFICIENT_DATA |
| SUIUSDT | 12 | 0.95 | 0.95 | 0.95 | 1 | INSUFFICIENT_DATA |
| AVAXUSDT | 12 | 0.98 | 0.98 | 0.98 | 1 | INSUFFICIENT_DATA |
| RLCUSDT | 12 | 1.17 | 2.34 | 2.34 | 1 | INSUFFICIENT_DATA |
| DOGEUSDT | 12 | 1.18 | 1.18 | 1.18 | 1 | INSUFFICIENT_DATA |
| UNIUSDT | 12 | 1.37 | 1.37 | 1.37 | 1 | INSUFFICIENT_DATA |
| PUMPUSDT | 12 | 1.80 | 1.81 | 1.81 | 1 | INSUFFICIENT_DATA |
| WLDUSDT | 12 | 2.04 | 2.05 | 2.05 | 1 | INSUFFICIENT_DATA |
| ONDOUSDT | 12 | 2.11 | 2.11 | 2.11 | 1 | INSUFFICIENT_DATA |
| NEARUSDT | 12 | 2.13 | 2.14 | 2.14 | 1 | INSUFFICIENT_DATA |
| METUSDT | 12 | 2.30 | 3.33 | 4.60 | 1 | INSUFFICIENT_DATA |
| OGNUSDT | 12 | 2.32 | 3.37 | 4.64 | 1 | INSUFFICIENT_DATA |
| ADAUSDT | 12 | 4.28 | 4.29 | 4.29 | 1 | INSUFFICIENT_DATA |

For comparison, the frozen evaluation charges 5.0 bps of slippage per side on top of a 5.0 bps fee. A top-of-book spread says nothing about the cost of an order larger than the quoted size.

### Demo fills already recorded by the engine (Step 1 certification)

- ADAUSDT: 2 fill(s) with a recorded slippage, 2 with a recorded fee. They are counted here and are not used for calibration.

No order-book snapshot and no bid or ask was captured at the time of those fills, so no spread can be attributed to them. Their expected price is the plan's reference price at decision time; the difference to the fill includes the time between decision and submission, not only execution cost.

### Funding history (published by the archive, development period)

Through 2024-12-31, 119 contracts the strategy held a target in: 468,923 funding records. Mean 0.908 bps per 8 hours (9.9% a year for a long position), median 1.000 bps, 95th percentile 6.594 bps, extremes -1000.0 to 83.4 bps.

The evaluation does not use these summary figures: it charges each position the actual rate of each event.

## Why this is not a calibration

- A component is calibrated only with at least 30 representative observations on at least 5 different days; today's observations come from one short window.

## Limits

- Top-of-book spread only: no depth, no market impact, no fill was made.
- A short window on one day is not a distribution over time of day or over market conditions.
- Demo fills validate the integration; their slippage and fees are not representative of real money.
- No real-money order was placed and none is needed for this collection.

## How to collect more

```bash
cd backends/bot-backend && ../venv/Scripts/python.exe -m app.trading_intelligence.research.costs collect --rounds 12 --interval 10
```

Run it on different days and at different hours. It places no order and needs no credential. When the table reaches the sample requirement the calibration produces a new cost-model version; the frozen evaluation keeps the model it was run with.
