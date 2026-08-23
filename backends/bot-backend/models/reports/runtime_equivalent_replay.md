# Runtime-Equivalent Replay

Generated: `2026-06-16T19:21:23.770010+00:00`

## Summary

- Date range: `2025-12-12` to `2026-06-10`
- Symbols: `BTCUSDT, ETHUSDT`
- Session windows: `06:00-19:00`
- Uses MasterEnsemble: `True`
- MasterEnsemble calls: `34060`
- Adaptive multipliers included: `True`
- Adaptive cache enabled: `True` (provider calls `2`, hits `34058`)
- Candle mode: `closed`
- Fees/slippage bps: `4.0` / `2.0`
- No overlap: `True`
- Total cycles: `34060`
- BUY / SELL / HOLD: `141` / `230` / `33689`
- Runtime-equivalent trades: `220`
- Old replay trades: `7`
- Overlap with old replay: `3`
- Missing old reasons: `{'risk_rejected': 4}`

## Metrics

- accepted_trades: `220`
- closed_trades: `220`
- win_rate: `0.418182`
- profit_factor_r: `0.465802`
- expectancy_r: `-0.395346`
- gross_expectancy_r: `-0.129409`
- max_drawdown_r: `91.819922`
- TP1 / TP2 / SL / BE / TIME: `92` / `49` / `128` / `42` / `1`
- fees_impact_r: `39.004103`
- slippage_impact_r: `19.502062`
- overlap_skipped_count: `37`

## STRONG_TREND Runtime-Equivalent Replay

- strong_trend_cycles: `6646`
- strong_trend_signals: `264`
- strong_trend_trades: `135`
- BTC / ETH strong trend trades: `85` / `50`
- BUY / SELL: `56` / `79`
- win_rate: `0.4`
- profit_factor_r: `0.431391`
- expectancy_r: `-0.432406`
- max_drawdown_r: `61.168778`
- recommendation: `STOP_STRONG_TREND_PAPER_EXPERIMENT`

## Candle Timing

- recommendation: `SWITCH_RUNTIME_TO_CLOSED_CANDLE_ONLY_IN_PAPER`
- Closed-candle replay is leakage-safe and the mismatch audit found runtime may use forming candles.
- Any change should be paper-only and separately tested before Section 4 acceptance.

## Safety

`{'active_env_modified': False, 'active_env_sha256_before': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'active_env_sha256_after': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'production_changed': False, 'production_files': ['README.md'], 'paper_only': True, 'ml_disabled': True, 'iofs_shadow': True, 'live_mode_enabled': False, 'live_mode_recommended': False, 'ml_enable_recommended': False, 'capital_deployment_recommended': False, 'strong_trend_experiment_left_running': True, 'recommendation_allowed': True}`

No active runtime config was changed.
