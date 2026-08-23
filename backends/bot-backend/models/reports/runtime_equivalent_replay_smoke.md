# Runtime-Equivalent Replay

Generated: `2026-06-16T19:10:06.501942+00:00`

## Summary

- Date range: `2026-06-01` to `2026-06-03`
- Symbols: `BTCUSDT, ETHUSDT`
- Session windows: `06:00-19:00`
- Uses MasterEnsemble: `True`
- MasterEnsemble calls: `76`
- Adaptive multipliers included: `True`
- Adaptive cache enabled: `True` (provider calls `2`, hits `74`)
- Candle mode: `closed`
- Fees/slippage bps: `4.0` / `2.0`
- No overlap: `True`
- Total cycles: `76`
- BUY / SELL / HOLD: `0` / `3` / `73`
- Runtime-equivalent trades: `3`
- Old replay trades: `7`
- Overlap with old replay: `0`
- Missing old reasons: `{'candle_timing_difference': 7}`

## Metrics

- accepted_trades: `3`
- closed_trades: `3`
- win_rate: `0.333333`
- profit_factor_r: `0.546441`
- expectancy_r: `-0.359474`
- gross_expectancy_r: `-0.166667`
- max_drawdown_r: `2.377688`
- TP1 / TP2 / SL / BE / TIME: `1` / `1` / `2` / `0` / `0`
- fees_impact_r: `0.385615`
- slippage_impact_r: `0.192807`
- overlap_skipped_count: `0`

## STRONG_TREND Runtime-Equivalent Replay

- strong_trend_cycles: `55`
- strong_trend_signals: `1`
- strong_trend_trades: `1`
- BTC / ETH strong trend trades: `0` / `1`
- BUY / SELL: `0` / `1`
- win_rate: `0.0`
- profit_factor_r: `0.0`
- expectancy_r: `-1.175442`
- max_drawdown_r: `1.175442`
- recommendation: `STOP_STRONG_TREND_PAPER_EXPERIMENT`

## Candle Timing

- recommendation: `SWITCH_RUNTIME_TO_CLOSED_CANDLE_ONLY_IN_PAPER`
- Closed-candle replay is leakage-safe and the mismatch audit found runtime may use forming candles.
- Any change should be paper-only and separately tested before Section 4 acceptance.

## Safety

`{'active_env_modified': False, 'active_env_sha256_before': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'active_env_sha256_after': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'production_changed': False, 'production_files': ['README.md'], 'paper_only': True, 'ml_disabled': True, 'iofs_shadow': True, 'live_mode_enabled': False, 'live_mode_recommended': False, 'ml_enable_recommended': False, 'capital_deployment_recommended': False, 'strong_trend_experiment_left_running': True, 'recommendation_allowed': True}`

No active runtime config was changed.
