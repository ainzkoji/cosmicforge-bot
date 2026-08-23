# Daily Paper Validation Status

- generated_at_utc: 2026-07-15T17:10:47.145801+00:00
- bot_process_running: True
- health_ok: true
- execution_mode: paper
- binance_env: testnet
- ML_ENABLED: False
- IOFS_GATE_MODE: shadow
- trade_symbols: BTCUSDT,ETHUSDT
- live_symbols_count: 0

## Paper Activity

- paper_orders_today: 0
- paper_fills_today: 0
- active_positions: 0
- closed_paper_trades_total: 3280
- closed_paper_trades_since_clean_start: 0
- closed_paper_trades_today: 0
- last_decision_time: 2026-07-15T17:07:03.783315+00:00
- last_order_time: 2026-06-17T12:30:44.589545+00:00
- last_fill_time: 2026-06-07T23:33:27.177152+00:00
- latest_block_reasons: `{'strategy_no_signal': 100}`
- latest_hold_reasons: `{'strategy_no_signal': 100}`
- latest_regime_distribution: `{'WEAK_TREND': 94, 'STRONG_TREND': 6}`
- latest_component_failures: `{'adx_above_threshold': 1028, 'bollinger_extreme': 102, 'confirmed_close_within_atr_distance': 86, 'directional_momentum': 7, 'ema50_ema200_trend_alignment': 56, 'ema_reaction': 26, 'ema_slope_confirmation': 20, 'ema_trend_alignment': 514, 'fresh_donchian_breakout': 1340, 'fresh_fast_slow_sma_cross': 1508, 'recent_squeeze': 82, 'reversal_candle': 1, 'reversion_candle': 4, 'rsi_extreme': 114, 'rsi_reset_and_turn': 941}`

## STRONG_TREND Experiment

- active: true
- cycles: 2647
- signals: 0
- paper_orders: 68
- closed_trades: 0
- stop_recommended: true
- stop_reason: paper order errors >= 2

## Section 5 Readiness

- organic_rows: 326
- iofs_organic_rows: 0
- closed_iofs_paper_trades: 0
- ready_to_retry_5a: false
- ready_for_5b: false
- section5b_status: BLOCKED
- t25_status: NOT_STARTED

## Active Alerts

| Alert | Severity | Evidence | Recommended action |
|---|---|---|---|
| NO_TRADES_AFTER_24H | warning | Zero closed paper trades after 732.4 hours from clean start. | continue_monitoring |
| NO_TRADES_AFTER_72H | warning | Zero closed paper trades after 732.4 hours from clean start. | run_signal_audit |
| NO_TRADES_AFTER_7D | critical | Zero closed paper trades after 732.4 hours from clean start. | run_signal_audit |
| NO_PATTERN_DOMINATES | info | No-pattern/no-signal holds are 100/100 latest traces. | run_component_replay |
| STRONG_TREND_EXPERIMENT_NO_SIGNALS | info | 2647 STRONG_TREND cycles produced zero signals. | review_strong_trend_experiment |
| STRONG_TREND_STOP_RECOMMENDED | critical | paper order errors >= 2 | review_strong_trend_experiment |

Section 4 remains in progress. This monitor does not approve live trading.
