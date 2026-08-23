# Strong Trend Block Impact Audit

Generated: `2026-06-15T16:23:49.974857+00:00`

## Recommendation

`ALLOW_STRONG_TREND_IN_PAPER_ONLY`

- Positive expectancy, profit factor above 1.2, and max drawdown at or below 5R.
- The sample has fewer than 20 trades, so any trial must remain paper-only for data collection.
- Live use recommended: false
- Active `.env` modified: false

## Runtime Consistency

- Raw TRADE_SYMBOLS: `BTCUSDT,ETHUSDT`
- Parsed/runtime symbols: `['BTCUSDT', 'ETHUSDT']`
- Current blocked regimes: `STRONG_TREND`
- Health count before fix: `15`
- Correct health count: `2`
- Recent blocked decisions/cycles: `541` / `480`

## Replay Comparison

| Scenario | Trades | Win rate | Profit factor R | Expectancy R | Max DD R | TP1 / TP2 / SL |
|---|---:|---:|---:|---:|---:|---:|
| STRONG_TREND blocked | 8 | 0.7500 | 3.1500 | 0.5375 | 1.0000 | 6 / 3 / 2 |
| STRONG_TREND allowed (analysis only) | 15 | 0.6667 | 2.2800 | 0.4267 | 2.0000 | 10 / 6 / 5 |
| STRONG_TREND only | 7 | 0.5714 | 1.7000 | 0.3000 | 1.4000 | 4 / 3 / 3 |

- Strong-trend cycles: `217`
- Strong-trend trade candidates blocked by current config: `7`
- Additional allowed-scenario trades: `7`

## Strong Trend Breakdown

### Long vs short

| Group | Trades | Win rate | PF R | Expectancy R | Max DD R |
|---|---:|---:|---:|---:|---:|
| SELL | 7 | 0.5714 | 1.7000 | 0.3000 | 1.4000 |

### BTC vs ETH

| Group | Trades | Win rate | PF R | Expectancy R | Max DD R |
|---|---:|---:|---:|---:|---:|
| BTCUSDT | 2 | 0.0000 | 0.0000 | -1.0000 | 2.0000 |
| ETHUSDT | 5 | 0.8000 | 5.1000 | 0.8200 | 1.0000 |

### Session

| Group | Trades | Win rate | PF R | Expectancy R | Max DD R |
|---|---:|---:|---:|---:|---:|
| 06:00-10:00 | 4 | 0.7500 | 4.5000 | 0.8750 | 1.0000 |
| 13:00-16:00 | 2 | 0.5000 | 0.6000 | -0.2000 | 1.0000 |
| 16:00-19:00 | 1 | 0.0000 | 0.0000 | -1.0000 | 1.0000 |

### Score bucket

| Group | Trades | Win rate | PF R | Expectancy R | Max DD R |
|---|---:|---:|---:|---:|---:|
| 0.80-1.00 | 7 | 0.5714 | 1.7000 | 0.3000 | 1.4000 |

### Primary component

| Group | Trades | Win rate | PF R | Expectancy R | Max DD R |
|---|---:|---:|---:|---:|---:|
| supertrend | 6 | 0.5000 | 1.2000 | 0.1000 | 2.4000 |
| trend_pullback | 1 | 1.0000 | n/a | 1.5000 | 0.0000 |

## Repair Comparison

Models the pre-repair bug by pinning each symbol to its first classified regime.

- Legacy pinned strong-trend trades: `0`
- Repaired strong-trend trades: `7`
- Trade-count delta after repair: `7`
- Expectancy delta after repair: `n/a`

## Safety And Limitations

- This is an offline comparative replay and does not replace Section 4 forward paper validation.
- The ATR-normalized simulator is uniform comparison evidence, not exact production execution parity.
- Overlapping trades are allowed; fees and slippage are excluded.
