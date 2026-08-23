# Post-Restart In-Session Runtime Confirmation

Confirmation time: `2026-06-15T15:45:30.035848+00:00`

## Result

- Post-restart in-session runtime confirmation completed: **yes**
- Active configured session: `06:00-19:00 UTC`
- Complete natural runtime cycles observed: `5`
- BTCUSDT observations: `5`
- ETHUSDT observations: `5`
- Runtime actions: `BUY=0`, `SELL=0`, `HOLD=10`
- Component diagnostics active: **yes**
- Paper executor reached by a natural signal: **no**
- Submit attempts: `0`
- Paper orders created: `0`
- Positions opened: `0`
- Trade fills: `0`

Every selected trace timestamp is within the configured session. The persisted
`hold_breakdown.session_allowed` field is `true` for the cycle that reached
component evaluation. It is `false` on strong-trend traces because the regime
gate stops evaluation before the session gate records an allowed result.

## Component Runtime Proof

The BTCUSDT trace at `2026-06-15T15:36:12.896477+00:00` reached the repaired
strategy component path in runtime:

- regime: `WEAK_TREND`
- session allowed: `true`
- final action: `HOLD`
- hold reason: `NO_PATTERN`
- component records: `7`
- active components evaluated: `4`
- disabled-for-regime components: `3`
- indicator snapshot present: **yes**

Active component diagnostics:

| Component | Signal | Confidence | Exact failed condition |
|---|---|---:|---|
| supertrend | HOLD | 0.0 | `supertrend_flip_or_continuation` |
| sma_cross | HOLD | 0.0 | `fresh_fast_slow_sma_cross` |
| trend_pullback | HOLD | 0.0 | `rsi_reset_and_turn` |
| donchian_breakout | HOLD | 0.0 | `fresh_donchian_breakout` |

The component indicator snapshots included EMA slope, supertrend direction,
fast/slow SMA values, ADX, RSI, EMA alignment, Donchian levels, ATR, and current
OHLC values. This closes the pending runtime component-observability item.

The remaining selected traces were `STRONG_TREND` and correctly produced
`REGIME_BLOCKED_STRONG_TREND` before component evaluation. They still persisted
regime indicator summaries and the exact failed condition `REGIME_BLOCKED`.

## Selected Complete Cycles

| Cycle | BTCUSDT | ETHUSDT |
|---:|---|---|
| 1 | `15:36:12Z`, WEAK_TREND, NO_PATTERN, 7 components | `15:37:03Z`, STRONG_TREND, REGIME_BLOCKED |
| 2 | `15:37:57Z`, STRONG_TREND, REGIME_BLOCKED | `15:39:07Z`, STRONG_TREND, REGIME_BLOCKED |
| 3 | `15:40:42Z`, STRONG_TREND, REGIME_BLOCKED | `15:42:46Z`, STRONG_TREND, REGIME_BLOCKED |
| 4 | `15:44:43Z`, STRONG_TREND, REGIME_BLOCKED | `15:45:03Z`, STRONG_TREND, REGIME_BLOCKED |
| 5 | `15:45:13Z`, STRONG_TREND, REGIME_BLOCKED | `15:45:30Z`, STRONG_TREND, REGIME_BLOCKED |

For all selected traces, IOFS shadow and the paper executor were not reached
because the final strategy action was HOLD. No gate was bypassed and no trade
was forced.

## Runtime Safety

- `EXECUTION_MODE=paper`
- exchange environment: `testnet`
- `ML_ENABLED=False`
- `IOFS_GATE_MODE=shadow`
- `TRADE_SYMBOLS=BTCUSDT,ETHUSDT`
- live symbols count: `0`
- active `.env` SHA256 unchanged:
  `1CF36622DD75B4AB091BB0D35CE9950FC641749A9857BA916B2435617E13D97C`
- `models/production` contains only `README.md`
- no live capital used

Strict controlled-restart acceptance is now **closed**. Section 4 paper
validation remains **In Progress** and is not passed.
