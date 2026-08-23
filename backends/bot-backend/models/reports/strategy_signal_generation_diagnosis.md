# Strategy Signal Generation Diagnosis

Generated: `2026-06-14T21:20:06.573394+00:00`

## Root Cause

The latest runtime sample repeatedly evaluated an in-session market window in which all active WEAK_TREND components had no qualifying trigger. A separate routing defect amplified starvation: RegimeClassifier.classify_stable() reset a new-regime counter on every call, permanently pinning the ensemble to its first confirmed regime.

Signal becomes zero: At raw component evaluation: active strategies return HOLD/0.0 when their entry conditions are absent. The ensemble correctly aggregates those zero directional votes; before the fix, broken regime hysteresis could also prevent the intended components from being activated as market conditions changed.

- Strategy logic broken: `True`
- Component trigger logic broken: `False`
- Regime routing logic broken: `True`
- Strategy logic too restrictive: `False`
- Replay ensemble counts: `{'HOLD': 1984, 'SELL': 11, 'BUY': 5}`
- Most failed condition: `fresh_fast_slow_sma_cross`

## Interpretation

Component rules are selective but demonstrably active. The 500-decision runtime audit counts runner evaluations, not 500 independent closed 15m candles, so repeated HOLDs over one unchanged candle window can make starvation appear larger than it is. The hysteresis repair restores normal regime transitions without forcing a trade.

## Fix Applied

Fixed the regime hysteresis candidate counter; added structured component diagnostics, exact failed-condition classification, and explicit ERROR/DISABLED/INSUFFICIENT_DATA statuses. No component trigger, confidence threshold, session rule, or risk gate changed.

## Current Indicator Health

`{'source': 'Binance public futures 15m klines', 'verification_note': 'Read-only public market-data verification; no executor or active configuration used.', 'symbols': {'BTCUSDT': {'candle_count': 250, 'health_status': 'HEALTHY', 'latest_candle_open_utc': '2026-06-14T21:15:00+00:00', 'verified_at_utc': '2026-06-14T21:16:38.946201+00:00', 'values': {'adx_14': 32.13088828274317, 'atr_14': 123.32142857142857, 'bollinger_lower_20': 63523.89108066192, 'bollinger_middle_20': 63876.325, 'bollinger_upper_20': 64228.75891933808, 'bullish_candle_field': 1, 'ema_fast_20': 63962.46916922867, 'ema_long_200': 63982.023764646234, 'ema_slow_50': 64065.567665747614, 'macd_12_26': 2.761409923543397, 'resistance_20': 64493.2, 'rsi_14': 71.20715087295984, 'support_20': 63650.0, 'volume_average_20': 881.2749}}, 'ETHUSDT': {'candle_count': 250, 'health_status': 'HEALTHY', 'latest_candle_open_utc': '2026-06-14T21:15:00+00:00', 'verified_at_utc': '2026-06-14T21:16:39.259782+00:00', 'values': {'adx_14': 23.07216797003132, 'atr_14': 4.112857142857154, 'bollinger_lower_20': 1655.0268622523076, 'bollinger_middle_20': 1665.2585, 'bollinger_upper_20': 1675.4901377476922, 'bullish_candle_field': 1, 'ema_fast_20': 1667.2816326253512, 'ema_long_200': 1670.6002477947936, 'ema_slow_50': 1667.9723770376324, 'macd_12_26': 2.014790194093621, 'resistance_20': 1684.99, 'rsi_14': 72.72061012362838, 'support_20': 1658.08, 'volume_average_20': 19802.85165}}}}`

## Safety

`{'ensemble_threshold_floor': 0.55, 'blocked_regimes': ['STRONG_TREND'], 'session_filter_enabled': True, 'session_windows_utc': '06:00-19:00', 'execution_mode': 'paper', 'ml_enabled': False, 'iofs_gate_mode': 'shadow'}`
