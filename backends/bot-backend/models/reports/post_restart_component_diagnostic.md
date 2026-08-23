# Strategy Component Replay

Generated: `2026-06-15T04:54:57.436062+00:00`

## Summary

- Total candles tested: `2000`
- Component BUY/SELL activity present: `True`
- Ensemble BUY/SELL/HOLD: `{'HOLD': 1984, 'SELL': 11, 'BUY': 5}`
- Top failed conditions: `{'fresh_fast_slow_sma_cross': 1508, 'fresh_donchian_breakout': 1340, 'adx_above_threshold': 1028, 'rsi_reset_and_turn': 941, 'supertrend_flip_or_continuation': 867, 'ema_trend_alignment': 514, 'rsi_extreme': 114, 'bollinger_extreme': 102, 'confirmed_close_within_atr_distance': 86, 'recent_squeeze': 82, 'ema50_ema200_trend_alignment': 56, 'ema_reaction': 26, 'squeeze_release': 21, 'ema_slope_confirmation': 20, 'directional_momentum': 7, 'reversion_candle': 4, 'reversal_candle': 1}`

## Components

- `supertrend`: counts `{'HOLD': 887, 'DISABLED': 443, 'SELL': 463, 'BUY': 207}`, weight `1.5`, min confidence `0.5`, timeframe `15m`
- `trend_pullback`: counts `{'HOLD': 1481, 'BUY': 17, 'DISABLED': 443, 'SELL': 59}`, weight `1.3`, min confidence `0.75`, timeframe `15m`
- `sma_cross`: counts `{'HOLD': 1508, 'SELL': 23, 'DISABLED': 443, 'BUY': 26}`, weight `0.9`, min confidence `0.55`, timeframe `15m`
- `donchian_breakout`: counts `{'HOLD': 1482, 'DISABLED': 443, 'SELL': 70, 'BUY': 5}`, weight `1.0`, min confidence `0.75`, timeframe `15m`
- `bollinger_reversion`: counts `{'DISABLED': 1881, 'HOLD': 119}`, weight `1.0`, min confidence `0.55`, timeframe `15m`
- `squeeze_breakout`: counts `{'DISABLED': 1881, 'HOLD': 110, 'SELL': 6, 'BUY': 3}`, weight `1.1`, min confidence `0.6`, timeframe `15m`
- `vwap_reversion`: counts `{'DISABLED': 1881, 'HOLD': 116, 'BUY': 2, 'SELL': 1}`, weight `1.2`, min confidence `0.6`, timeframe `5m`

## BTCUSDT

- Candles tested: `1000`
- In-session candles: `545`
- Regimes: `{'WEAK_TREND': 450, 'LOW_VOLATILITY_CHOP': 212, 'STRONG_TREND': 299, 'RANGE': 39}`
- Ensemble counts: `{'HOLD': 990, 'SELL': 6, 'BUY': 4}`
- Ensemble reasons: `{'SESSION_BLOCKED': 455, 'NO_PATTERN': 200, 'REGIME_BLOCKED_STRONG_TREND': 169, 'NO_ACTIVE_STRATEGIES': 93, 'CONFIDENCE_BELOW_FLOOR': 73, 'ENSEMBLE_SELL': 6, 'ENSEMBLE_BUY': 4}`
- Latest stored candle: `2026-06-10T11:45:00+00:00`
- Stored indicator health: `[{'indicator_name': 'ema_fast_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 61233.422105, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'ema_slow_50', 'valid_count': 951, 'nan_count': 49, 'latest_value': 61412.74657624, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'ema_long_200', 'valid_count': 801, 'nan_count': 199, 'latest_value': 62036.41590872, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'rsi_14', 'valid_count': 986, 'nan_count': 14, 'latest_value': 40.08250706, 'expected_range': '0..100', 'health_status': 'HEALTHY'}, {'indicator_name': 'macd_12_26', 'valid_count': 975, 'nan_count': 25, 'latest_value': -107.03645609, 'expected_range': 'finite', 'health_status': 'HEALTHY'}, {'indicator_name': 'atr_14', 'valid_count': 986, 'nan_count': 14, 'latest_value': 273.14285714, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'adx_14', 'valid_count': 973, 'nan_count': 27, 'latest_value': 31.81736324, 'expected_range': '0..100', 'health_status': 'HEALTHY'}, {'indicator_name': 'volume_average_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 1955.83865, 'expected_range': '>= 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'bollinger_upper_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 61755.7495655, 'expected_range': 'upper > middle', 'health_status': 'HEALTHY'}, {'indicator_name': 'bollinger_middle_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 61301.78, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'bollinger_lower_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 60847.8104345, 'expected_range': 'lower < middle', 'health_status': 'HEALTHY'}, {'indicator_name': 'support_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 60691.9, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'resistance_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 61783.4, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'bullish_candle_field', 'valid_count': 1000, 'nan_count': 0, 'latest_value': 1.0, 'expected_range': '0 or 1', 'health_status': 'HEALTHY'}]`

## ETHUSDT

- Candles tested: `1000`
- In-session candles: `545`
- Regimes: `{'WEAK_TREND': 505, 'STRONG_TREND': 303, 'LOW_VOLATILITY_CHOP': 112, 'RANGE': 80}`
- Ensemble counts: `{'HOLD': 994, 'SELL': 5, 'BUY': 1}`
- Ensemble reasons: `{'SESSION_BLOCKED': 455, 'NO_PATTERN': 245, 'REGIME_BLOCKED_STRONG_TREND': 157, 'CONFIDENCE_BELOW_FLOOR': 111, 'NO_ACTIVE_STRATEGIES': 26, 'ENSEMBLE_SELL': 5, 'ENSEMBLE_BUY': 1}`
- Latest stored candle: `2026-06-10T11:45:00+00:00`
- Stored indicator health: `[{'indicator_name': 'ema_fast_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 1623.63128094, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'ema_slow_50', 'valid_count': 951, 'nan_count': 49, 'latest_value': 1629.91313305, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'ema_long_200', 'valid_count': 801, 'nan_count': 199, 'latest_value': 1647.42013033, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'rsi_14', 'valid_count': 986, 'nan_count': 14, 'latest_value': 43.76292379, 'expected_range': '0..100', 'health_status': 'HEALTHY'}, {'indicator_name': 'macd_12_26', 'valid_count': 975, 'nan_count': 25, 'latest_value': -3.01974308, 'expected_range': 'finite', 'health_status': 'HEALTHY'}, {'indicator_name': 'atr_14', 'valid_count': 986, 'nan_count': 14, 'latest_value': 9.145, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'adx_14', 'valid_count': 973, 'nan_count': 27, 'latest_value': 30.82771376, 'expected_range': '0..100', 'health_status': 'HEALTHY'}, {'indicator_name': 'volume_average_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 55509.16365, 'expected_range': '>= 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'bollinger_upper_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 1641.0897901, 'expected_range': 'upper > middle', 'health_status': 'HEALTHY'}, {'indicator_name': 'bollinger_middle_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 1625.2695, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'bollinger_lower_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 1609.4492099, 'expected_range': 'lower < middle', 'health_status': 'HEALTHY'}, {'indicator_name': 'support_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 1605.0, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'resistance_20', 'valid_count': 981, 'nan_count': 19, 'latest_value': 1642.99, 'expected_range': '> 0', 'health_status': 'HEALTHY'}, {'indicator_name': 'bullish_candle_field', 'valid_count': 1000, 'nan_count': 0, 'latest_value': 1.0, 'expected_range': '0 or 1', 'health_status': 'HEALTHY'}]`
