# Runtime-Equivalent Replay

Generated: `2026-06-16T19:11:17.093002+00:00`

## Summary

- Date range: `2025-12-12` to `2026-06-10`
- Symbols: `BTCUSDT, ETHUSDT`
- Session windows: `06:00-19:00`
- Uses MasterEnsemble: `True`
- MasterEnsemble calls: `1000`
- Adaptive multipliers included: `True`
- Adaptive cache enabled: `True` (provider calls `1`, hits `999`)
- Candle mode: `closed`
- Fees/slippage bps: `4.0` / `2.0`
- No overlap: `True`
- Total cycles: `1000`
- BUY / SELL / HOLD: `1` / `8` / `991`
- Runtime-equivalent trades: `8`
- Old replay trades: `7`
- Overlap with old replay: `0`
- Missing old reasons: `{'candle_timing_difference': 7}`

## Metrics

- accepted_trades: `8`
- closed_trades: `8`
- win_rate: `0.5`
- profit_factor_r: `0.819555`
- expectancy_r: `-0.116781`
- gross_expectancy_r: `0.1375`
- max_drawdown_r: `2.903105`
- TP1 / TP2 / SL / BE / TIME: `4` / `3` / `4` / `1` / `0`
- fees_impact_r: `1.356166`
- slippage_impact_r: `0.678082`
- overlap_skipped_count: `1`

## STRONG_TREND Runtime-Equivalent Replay

- strong_trend_cycles: `135`
- strong_trend_signals: `7`
- strong_trend_trades: `6`
- BTC / ETH strong trend trades: `6` / `0`
- BUY / SELL: `1` / `5`
- win_rate: `0.5`
- profit_factor_r: `0.714168`
- expectancy_r: `-0.191754`
- max_drawdown_r: `2.474526`
- recommendation: `STOP_STRONG_TREND_PAPER_EXPERIMENT`

## Candle Timing

- recommendation: `SWITCH_RUNTIME_TO_CLOSED_CANDLE_ONLY_IN_PAPER`
- Closed-candle replay is leakage-safe and the mismatch audit found runtime may use forming candles.
- Any change should be paper-only and separately tested before Section 4 acceptance.

## Safety

`{'active_env_modified': False, 'active_env_sha256_before': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'active_env_sha256_after': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'production_changed': False, 'production_files': ['README.md'], 'paper_only': True, 'ml_disabled': True, 'iofs_shadow': True, 'live_mode_enabled': False, 'live_mode_recommended': False, 'ml_enable_recommended': False, 'capital_deployment_recommended': False, 'strong_trend_experiment_left_running': True, 'recommendation_allowed': True}`

No active runtime config was changed.
