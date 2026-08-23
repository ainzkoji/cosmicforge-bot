# IOFS Historical Replay Report

- Date range: 2025-12-12 to 2026-06-10
- Symbols: BTCUSDT, ETHUSDT
- Sessions UTC: 07:00-10:00,13:00-16:00
- Risk profiles: balanced
- Historical replay passed: false
- Capital deployment allowed: false

Historical replay is fast validation only. It does not replace Section 4 forward paper validation.

## Profile Comparison

| Profile | Evaluated | Accepted | Pass rate | Win rate | Profit factor | Expectancy R | Max DD R |
|---|---:|---:|---:|---:|---:|---:|---:|
| balanced | 0 | 0 | N/A | N/A | N/A | N/A | 0.0000 |

## Balanced

- Total cycles: 1000
- Evaluated cycles: 0
- IOFS pass rate: N/A
- Accepted trades: 0
- Win rate: N/A
- Profit factor R: N/A
- Expectancy R: N/A
- Baseline expectancy R: N/A
- Expectancy improvement vs baseline R: N/A
- TP1 / TP2 / SL: 0 / 0 / 0
- Break-even buffer / time exit: 0 / 0
- TP1:TP2 ratio: N/A
- Max drawdown R: 0.0000
- Replay passed: false
- Blocking reasons: accepted_trades < 20, win_rate < 58%, profit_factor_r <= 1.2, expectancy_r <= 0, tp1_to_tp2_ratio is unavailable or >= 20

### Score Buckets

| Bucket | Accepted | Win rate | Profit factor | Expectancy R |
|---|---:|---:|---:|---:|
| 0-49 | 0 | N/A | N/A | N/A |
| 50-64 | 0 | N/A | N/A | N/A |
| 65-71 | 0 | N/A | N/A | N/A |
| 72-79 | 0 | N/A | N/A | N/A |
| 80-100 | 0 | N/A | N/A | N/A |

### Failure Reasons

| Reason | Count |
|---|---:|
| TREND_NOT_ALIGNED | 0 |
| STRUCTURE_NOT_ACTIVE | 0 |
| TRIGGER_NOT_CONFIRMED | 0 |
| QUALITY_SCORE_TOO_LOW | 0 |
| MISSING_TIMEFRAME | 252 |
| ATR_UNAVAILABLE | 0 |
| INVALID_CANDLES | 0 |
| SESSION_BLOCKED | 748 |
| SYMBOL_BLOCKED | 0 |
| INVALID_RISK | 0 |

## Warnings

- Historical replay does not replace Section 4 forward paper validation.
- Overlapping historical trades are allowed and may overstate practical capacity.
- No fees or slippage are included in R-multiple outcomes.
