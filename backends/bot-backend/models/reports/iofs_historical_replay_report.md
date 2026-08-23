# IOFS Historical Replay Report

- Date range: 2025-12-12 to 2026-06-10
- Symbols: BTCUSDT, ETHUSDT
- Sessions UTC: 07:00-10:00,13:00-16:00
- Risk profiles: balanced
- Historical replay passed: false
- Recommendation: tune IOFS
- Capital deployment allowed: false

Historical replay is fast validation only. It does not replace Section 4 forward paper validation.

## Profile Comparison

| Profile | Evaluated | Accepted | Pass rate | Win rate | Profit factor | Expectancy R | Max DD R |
|---|---:|---:|---:|---:|---:|---:|---:|
| balanced | 6888 | 21 | 0.38% | 47.62% | 1.1182 | 0.0619 | 5.4000 |

## Balanced

- Total cycles: 34560
- Evaluated cycles: 6888
- IOFS pass rate: 0.38%
- Accepted trades: 21
- Win rate: 47.62%
- Profit factor R: 1.1182
- Expectancy R: 0.0619
- Baseline expectancy R: 0.0619
- Expectancy improvement vs baseline R: 0.0000
- TP1 / TP2 / SL: 10 / 7 / 11
- Break-even buffer / time exit: 3 / 0
- TP1:TP2 ratio: 1.4286
- Max drawdown R: 5.4000
- Best-performing score bucket: 72-79
- Most common failure reason: SESSION_BLOCKED
- Most common evaluated failure reason: TREND_NOT_ALIGNED
- Worst-performing trigger pattern: ENGULFING
- Replay passed: false
- Blocking reasons: win_rate < 58%, profit_factor_r <= 1.2

### Score Buckets

| Bucket | Accepted | Win rate | Profit factor | Expectancy R |
|---|---:|---:|---:|---:|
| 0-49 | 0 | N/A | N/A | N/A |
| 50-64 | 0 | N/A | N/A | N/A |
| 65-71 | 0 | N/A | N/A | N/A |
| 72-79 | 5 | 60.00% | 1.8000 | 0.3200 |
| 80-100 | 16 | 43.75% | 0.9667 | -0.0187 |

### Failure Reasons

| Reason | Count |
|---|---:|
| TREND_NOT_ALIGNED | 3500 |
| STRUCTURE_NOT_ACTIVE | 3120 |
| TRIGGER_NOT_CONFIRMED | 242 |
| QUALITY_SCORE_TOO_LOW | 0 |
| MISSING_TIMEFRAME | 1752 |
| ATR_UNAVAILABLE | 0 |
| INVALID_CANDLES | 0 |
| SESSION_BLOCKED | 25920 |
| SYMBOL_BLOCKED | 0 |
| INVALID_RISK | 5 |

## Warnings

- Historical replay does not replace Section 4 forward paper validation.
- Overlapping historical trades are allowed and may overstate practical capacity.
- No fees or slippage are included in R-multiple outcomes.
