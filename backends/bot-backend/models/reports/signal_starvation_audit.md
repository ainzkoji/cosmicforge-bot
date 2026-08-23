# Signal Starvation Audit

Generated: `2026-06-14T20:13:16.961579+00:00`

## Conclusion

HOLD dominates because active strategy components rarely produce any directional pattern on BTCUSDT/ETHUSDT 15m. The current floor, STRONG_TREND block, and runtime session are not the primary cause in the analyzed sample.

## Decision Summary

- Decisions analyzed: `500`
- BUY / SELL / HOLD: `0 / 0 / 500`
- Average confidence: `0.0`
- Maximum confidence: `0.0`
- Signals in 0.50-0.54 just below current floor: `0`
- HOLD reasons: `{'SESSION_BLOCKED': 409, 'NO_PATTERN': 91}`
- Confidence distribution: `{'0.00-0.39': 500, '0.40-0.49': 0, '0.50-0.54': 0, '0.55-0.59': 0, '0.60+': 0}`
- Regime distribution: `{'WEAK_TREND': 500}`
- Session distribution: `{'OUTSIDE_RUNTIME_WINDOW': 409, 'RUNTIME_ONLY_WINDOW': 91}`
- Grouped analysis rows: `6`

## Exact Component Rules

Within valid-session WEAK_TREND cycles, the active components overwhelmingly returned HOLD: Supertrend found no qualifying flip/continuation, Trend Pullback found no ADX + RSI-reset + EMA reaction setup, SMA Cross found no fresh cross, and Donchian Breakout found no aligned confirmed breakout. Historical component reason details were not persisted; the new HOLD breakdown logging records them going forward.

Component distribution: `{'trend_pullback': {'HOLD': 91}, 'supertrend': {'HOLD': 91}, 'sma_cross': {'HOLD': 91}, 'donchian_breakout': {'HOLD': 91}}`

## Threshold Sensitivity

- `0.55`: total `0`, additional `0`, BUY `0`, SELL `0`, outcomes `NO_OUTCOME_DATA_FOR_THRESHOLD_EXPECTANCY`
- `0.50`: total `0`, additional `0`, BUY `0`, SELL `0`, outcomes `NO_OUTCOME_DATA_FOR_THRESHOLD_EXPECTANCY`
- `0.45`: total `0`, additional `0`, BUY `0`, SELL `0`, outcomes `NO_OUTCOME_DATA_FOR_THRESHOLD_EXPECTANCY`

## Regime Impact

`{'decisions': {'STRONG_TREND': 0, 'WEAK_TREND': 500, 'RANGE': 0, 'HIGH_VOLATILITY': 0}, 'signals_before_regime_block': 0, 'signals_after_regime_block': 0, 'strong_trend_block_impact_in_sample': 0, 'conclusion': 'No STRONG_TREND decisions occurred in the analyzed sample; the block did not cause the observed starvation.'}`

## Session Impact

`{'runtime_window': '06:00-19:00', 'narrow_replay_windows': '07:00-10:00,13:00-16:00', 'runtime': {'decisions': 91, 'nonzero_confidence': 0, 'valid_signals_at_current_floor': 0}, 'narrow_replay': {'decisions': 0, 'nonzero_confidence': 0, 'valid_signals_at_current_floor': 0}, 'outside_runtime_decisions': 409, 'valid_signals_outside_runtime': 0, 'runtime_missing_valid_signals': False}`

## Safety

- Active `.env` hash before/after unchanged: `True`
- Runtime mode remains paper: `True`
- ML remains disabled: `True`
- This audit used read-only database access and did not call an executor.
