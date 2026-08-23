# Runtime-Equivalent Replay

Generated: `2026-06-16T19:29:57.706337+00:00`

## Summary

- Date range: `2025-12-12` to `2026-06-10`
- Symbols: `BTCUSDT, ETHUSDT`
- Session windows: `06:00-19:00`
- Uses MasterEnsemble: `True`
- MasterEnsemble calls: `34062`
- Adaptive multipliers included: `True`
- Adaptive cache enabled: `True` (provider calls `2`, hits `34060`)
- Candle mode: `runtime-current`
- Fees/slippage bps: `4.0` / `2.0`
- No overlap: `True`
- Total cycles: `34062`
- BUY / SELL / HOLD: `142` / `236` / `33684`
- Runtime-equivalent trades: `229`
- Old replay trades: `7`
- Overlap with old replay: `1`
- Missing old reasons: `{'risk_rejected': 1, 'confidence_below_floor': 4, 'master_ensemble_no_consensus': 1}`

## Metrics

- accepted_trades: `229`
- closed_trades: `229`
- win_rate: `0.899563`
- profit_factor_r: `5.970962`
- expectancy_r: `0.596124`
- gross_expectancy_r: `0.864406`
- max_drawdown_r: `2.454276`
- TP1 / TP2 / SL / BE / TIME: `206` / `107` / `22` / `99` / `1`
- fees_impact_r: `40.957807`
- slippage_impact_r: `20.4789`
- overlap_skipped_count: `23`

## STRONG_TREND Runtime-Equivalent Replay

- strong_trend_cycles: `6646`
- strong_trend_signals: `271`
- strong_trend_trades: `145`
- BTC / ETH strong trend trades: `93` / `52`
- BUY / SELL: `62` / `83`
- win_rate: `0.903448`
- profit_factor_r: `6.514867`
- expectancy_r: `0.601884`
- max_drawdown_r: `2.332851`
- recommendation: `FIX_RUNTIME_CANDLE_TIMING_FIRST`

## Candle Timing

- recommendation: `AUDIT_RUNTIME_CANDLE_TIMING_MORE`
- runtime-current mode is diagnostic; closed-candle mode is the safer replay authority.

## Safety

`{'active_env_modified': False, 'active_env_sha256_before': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'active_env_sha256_after': '6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9', 'production_changed': False, 'production_files': ['README.md'], 'paper_only': True, 'ml_disabled': True, 'iofs_shadow': True, 'live_mode_enabled': False, 'live_mode_recommended': False, 'ml_enable_recommended': False, 'capital_deployment_recommended': False, 'strong_trend_experiment_left_running': True, 'recommendation_allowed': True}`

No active runtime config was changed.
