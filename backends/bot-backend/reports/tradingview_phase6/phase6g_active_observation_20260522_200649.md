# Phase 6G — Active Observation with Controlled Alerts

Final verdict: `PHASE 6G ACTIVE OBSERVATION PASSED`

## Runtime URL
`http://127.0.0.1:9000/health`

## Runtime Fingerprint
```json
{
  "code_version": "db4580b",
  "process_started_at": "2026-05-22T19:28:21.829875+00:00",
  "config_loaded_at": "2026-05-22T19:28:21.841871+00:00",
  "pid": 30792,
  "working_directory": "c:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\bot-backend",
  "python_executable": "c:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\venv\\Scripts\\python.exe",
  "phase6_gate_available": true,
  "phase6_gate_code_version": "phase6_limited_gate_v1_2026-05-21",
  "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": true,
  "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED": true,
  "TRADINGVIEW_ALLOWED_SYMBOLS": [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "LTCUSDT",
    "APEUSDT",
    "SUIUSDT",
    "INJUSDT",
    "AAVEUSDT",
    "ZECUSDT",
    "HYPEUSDT",
    "ENAUSDT",
    "LDOUSDT",
    "MASKUSDT",
    "TAOUSDT"
  ],
  "TRADINGVIEW_ALLOWED_ACTIONS": [
    "BUY",
    "SELL"
  ],
  "TRADINGVIEW_MAX_QUEUE_PER_CYCLE": 1,
  "TRADINGVIEW_MAX_EXECUTIONS_PER_DAY": 3,
  "TRADINGVIEW_MAX_SIGNALS_PER_HOUR": 15,
  "TRADINGVIEW_MAX_SIGNALS_PER_DAY": 40,
  "TRADINGVIEW_MAX_TRADE_USDT_CAP": 400.0,
  "TRADINGVIEW_ALLOW_CLOSE": false,
  "TRADINGVIEW_ALLOW_REVERSE": false,
  "TRADINGVIEW_ALLOW_REDUCE": false,
  "TRADINGVIEW_ALLOW_CANCEL": false,
  "TRADINGVIEW_ALLOW_EXTERNAL_SLTP": false,
  "TRADINGVIEW_ALLOW_EXTERNAL_SIZE": false,
  "TRADINGVIEW_ALLOW_RISK_OVERRIDE": false,
  "TRADINGVIEW_REQUIRE_SLTP_PROTECTION": true,
  "TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL": true,
  "active_safety_lockout": false,
  "active_safety_lockout_reason": "CLEARED: Sequential Phase 6A proof retry after leftover proof rows were safely rejected",
  "fingerprint_present": true,
  "port_owner_pid": 30792,
  "health_status": "ok",
  "health_error": null
}
```

## PID Verification
```json
{
  "fingerprint_pid": 30792,
  "port_owner_pid": 30792,
  "pid_matches_port_owner": true,
  "python_executable": "c:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\venv\\Scripts\\python.exe",
  "working_directory": "c:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\bot-backend",
  "phase6_gate_available": true,
  "phase6_gate_code_version": "phase6_limited_gate_v1_2026-05-21",
  "active_safety_lockout": false,
  "runtime_url": "http://127.0.0.1:9000/health"
}
```

## Validation Result
`PHASE 6 LIMITED MODE READY`

## Baseline (Before Test)
```json
{
  "pending_claimed_rows": 0,
  "stuck_claimed_rows": 0,
  "unprotected_positions": 0,
  "active_lockout": 0
}
```

## Controlled Alerts Sent
Positive: 3  Negative: 5

## Positive Cases
```json
[
  {
    "label": "pos-1",
    "alert_id": "phase6g-pos-1-8586731b865a",
    "queue_id": "extsig_76b1ce7f130046bb98219a27e7a3b710",
    "symbol": "BNBUSDT",
    "action": "BUY",
    "is_negative_check": false,
    "queue_status": "REJECTED",
    "final_status": "REJECTED_TV_DAILY_EXECUTION_CAP",
    "final_reason": "Daily TradingView execution cap reached",
    "event_filter_result": null,
    "policy_result": null,
    "sizing_result": null,
    "execution_result": "NOT_CALLED:PHASE6_LIMITED_GATE",
    "decision_trace_id": null,
    "timed_out": false
  },
  {
    "label": "pos-2",
    "alert_id": "phase6g-pos-2-5282499f2584",
    "queue_id": "extsig_f4b914c8ab664c2692fc71b26a0ba48c",
    "symbol": "SUIUSDT",
    "action": "BUY",
    "is_negative_check": false,
    "queue_status": "FAILED",
    "final_status": "FAILED_EXECUTION",
    "final_reason": "Failed to place Stop-Loss. Entry was closed immediately to eliminate un-bracketed risk.",
    "event_filter_result": "PASS",
    "policy_result": "PASS",
    "sizing_result": "{\"account_risk_pct\": 2.0, \"admin_message\": \"Your 120.00 USDT fixed margin was respected.\", \"allocation_mode\": \"fixed\", \"allocation_type\": \"fixed_amount\", \"atr_cap_margin_usdt\": 452.3006779, \"base_margin\": 120.0, \"base_margin_usdt\": 120.0, \"base_notional_usdt\": 360.0, \"base_qty\": 339.11077619, \"calculated_qty\": 339.11077619, \"cap_applied\": false, \"cap_reason\": \"fixed_amount_strict: user fixed margin respected\", \"effective_equity\": 1165.41063468, \"entry_price\": 1.0616, \"final_margin\": 120.0, \"final_margin_usdt\": 120.0, \"final_notional_usdt\": 360.0, \"final_qty\": 339.11077619, \"is_fallback_mode\": false, \"leverage\": 3.0, \"leverage_used_for_cap\": 3.0, \"margin_cap_usdt\": 452.3006779, \"max_risk_capital\": 23.30821269, \"notional_cap_usdt\": 1356.90203369, \"risk_level\": \"low\", \"risk_level_label\": \"Conservative\", \"risk_warning\": false, \"rounded_qty\": 339.11077619, \"safe_stop_distance_pct\": 0.01747752064248148, \"sizing_method\": \"fixed_amount_strict\", \"stop_distance_fraction\": 0.0171775206, \"stop_distance_pct\": 1.71775206, \"target_risk_usdt\": 23.308212693599998, \"theoretical_risk_pct\": 0.53062047, \"theoretical_risk_usdt\": 6.18390743, \"user_fixed_margin_usdt\": 120.0}",
    "execution_result": "PROTECTION_FAILED_ENTRY_CLOSED:{\"action\": \"Force closed orphaned entry to prevent catastrophic risk. Atomic transaction rolled back.\", \"error\": \"[SEV1-S1] place_protection returned non-success: status=failed sl_id=1000000082695426 tp_id=None error=Binance HTTP 400: {\\\"code\\\":-2021,\\\"msg\\\":\\\"Order would immediately trigger.\\\"}\", \"signal\": \"BUY\", \"symbol\": \"SUIUSDT\"}",
    "decision_trace_id": "6cb11a3d-a22b-47b4-9d9b-d1a6b33e2742",
    "timed_out": false
  },
  {
    "label": "pos-3",
    "alert_id": "phase6g-pos-3-af03a8a03782",
    "queue_id": "extsig_1cfe9fc93eb748a3bccc4d53ba4dff0b",
    "symbol": "APEUSDT",
    "action": "BUY",
    "is_negative_check": false,
    "queue_status": "PROCESSED",
    "final_status": "PROCESSED_EXECUTED",
    "final_reason": "ORDER_PLACED",
    "event_filter_result": "PASS",
    "policy_result": "PASS",
    "sizing_result": "{\"account_risk_pct\": 2.0, \"admin_message\": \"Your 120.00 USDT fixed margin was respected.\", \"allocation_mode\": \"fixed\", \"allocation_type\": \"fixed_amount\", \"atr_cap_margin_usdt\": 440.30938288, \"base_margin\": 120.0, \"base_margin_usdt\": 120.0, \"base_notional_usdt\": 360.0, \"base_qty\": 2584.35032304, \"calculated_qty\": 2584.35032304, \"cap_applied\": false, \"cap_reason\": \"fixed_amount_strict: user fixed margin respected\", \"effective_equity\": 1164.30902086, \"entry_price\": 0.1393, \"final_margin\": 120.0, \"final_margin_usdt\": 120.0, \"final_notional_usdt\": 360.0, \"final_qty\": 2584.35032304, \"is_fallback_mode\": false, \"leverage\": 3.0, \"leverage_used_for_cap\": 3.0, \"margin_cap_usdt\": 440.30938288, \"max_risk_capital\": 23.28618042, \"notional_cap_usdt\": 1320.92814864, \"risk_level\": \"low\", \"risk_level_label\": \"Conservative\", \"risk_warning\": false, \"rounded_qty\": 2584.35032304, \"safe_stop_distance_pct\": 0.017928650310129534, \"sizing_method\": \"fixed_amount_strict\", \"stop_distance_fraction\": 0.0176286503, \"stop_distance_pct\": 1.76286503, \"target_risk_usdt\": 23.286180417199997, \"theoretical_risk_pct\": 0.54507128, \"theoretical_risk_usdt\": 6.34631411, \"user_fixed_margin_usdt\": 120.0}",
    "execution_result": "ORDER_PLACED:{\"entry_order\": {\"avg_fill_price\": \"0.00\", \"broker_order_id\": \"161169172\", \"client_order_id\": \"CFBOTELAPEUSDa5de0f8a0d9066d9\", \"error_message\": null, \"fee_currency\": \"\", \"fees\": \"0\", \"qty_filled\": \"0\", \"qty_ordered\": \"2584.0\", \"reduce_only\": false, \"side\": \"buy\", \"status\": \"new\", \"symbol\": \"APEUSDT\", \"timestamp\": 1779478735029, \"type\": \"market\"}, \"ep_side\": \"LONG\", \"flip_close\": null, \"normalized\": {\"avg_price\": 0.0, \"broker\": \"binance\", \"executed_qty\": 0.0, \"order_id\": \"\", \"quantity\": 2584.0, \"side\": \"BUY\", \"status\": \"NEW\", \"symbol\": \"APEUSDT\", \"timestamp\": 0, \"type\": \"MARKET\"}, \"protection\": {\"error\": null, \"sl_order_id\": \"1000000082696143\", \"status\": \"success\", \"tp_order_id\": \"1000000082696151\"}, \"qty\": 2584.0, \"side\": \"BUY\", \"signal\": \"BUY\", \"symbol\": \"APEUSDT\"}",
    "decision_trace_id": "f68871fb-e344-4489-9643-05308f623f4a",
    "timed_out": false
  }
]
```

## Negative Check Cases
```json
[
  {
    "label": "neg-1",
    "alert_id": "phase6g-neg-1-2b03d6474432",
    "queue_id": "extsig_666ff6b4915a4305ae626216250f2062",
    "symbol": "BTCUSDT",
    "action": "CLOSE",
    "is_negative_check": true,
    "queue_status": "REJECTED",
    "final_status": "REJECTED_TV_ACTION_NOT_ALLOWED",
    "final_reason": "Action 'CLOSE' is not supported for execution. Supported: ['BUY', 'SELL']",
    "event_filter_result": null,
    "policy_result": null,
    "sizing_result": null,
    "execution_result": null,
    "decision_trace_id": null,
    "timed_out": false
  },
  {
    "label": "neg-2",
    "alert_id": "phase6g-neg-2-6024edc935c2",
    "queue_id": "extsig_84da7edbc1644396a6427873a1816848",
    "symbol": "BTCUSDT",
    "action": "CANCEL",
    "is_negative_check": true,
    "queue_status": "REJECTED",
    "final_status": "REJECTED_TV_ACTION_NOT_ALLOWED",
    "final_reason": "Action 'CANCEL' is not supported for execution. Supported: ['BUY', 'SELL']",
    "event_filter_result": null,
    "policy_result": null,
    "sizing_result": null,
    "execution_result": null,
    "decision_trace_id": null,
    "timed_out": false
  },
  {
    "label": "neg-3",
    "alert_id": "phase6g-neg-3-5d819b7723cc",
    "queue_id": "extsig_e46d3dc43e754ba7b2baf2b6c1ded27e",
    "symbol": "BTCUSDT",
    "action": "REVERSE",
    "is_negative_check": true,
    "queue_status": "EXPIRED",
    "final_status": "EXPIRED",
    "final_reason": "Signal expired at 2026-05-22T19:41:49.089206+00:00 before runner processing",
    "event_filter_result": null,
    "policy_result": null,
    "sizing_result": null,
    "execution_result": null,
    "decision_trace_id": null,
    "timed_out": false
  },
  {
    "label": "neg-4",
    "alert_id": "phase6g-neg-4-194cc65a90e1",
    "queue_id": "extsig_cc867c1fab1f4b0da2d5a5d30dbb2884",
    "symbol": "BTCUSDT",
    "action": "REDUCE",
    "is_negative_check": true,
    "queue_status": "EXPIRED",
    "final_status": "EXPIRED",
    "final_reason": "Signal expired at 2026-05-22T19:41:49.089206+00:00 before runner processing",
    "event_filter_result": null,
    "policy_result": null,
    "sizing_result": null,
    "execution_result": null,
    "decision_trace_id": null,
    "timed_out": false
  },
  {
    "label": "neg-5",
    "alert_id": "phase6g-neg-5-e7cd1f9f5442",
    "queue_id": "extsig_696edada7f4544f4954000acd71022c4",
    "symbol": "FAKECOINUSDT",
    "action": "BUY",
    "is_negative_check": true,
    "queue_status": "EXPIRED",
    "final_status": "EXPIRED",
    "final_reason": "Signal expired at 2026-05-22T19:41:49.091207+00:00 before runner processing",
    "event_filter_result": null,
    "policy_result": null,
    "sizing_result": null,
    "execution_result": null,
    "decision_trace_id": null,
    "timed_out": false
  }
]
```

## Execution Evidence
```json
{
  "orders_placed": 1,
  "trades_opened": 1,
  "trades_protected": 1,
  "unprotected_positions": 0,
  "proof_open_fills": [
    {
      "id": 17081,
      "symbol": "APEUSDT",
      "side": "LONG",
      "action": "OPEN",
      "qty": 2584.3503230437905,
      "price": 0.1393,
      "order_id": "161169172",
      "position_id": "4189015d-3009-458c-bfb1-2a55bd8251d5",
      "trace_id": "f68871fb-e344-4489-9643-05308f623f4a",
      "timestamp_utc": "2026-05-22T19:38:57.222308+00:00",
      "lifecycle": {
        "symbol": "APEUSDT",
        "phase": "FLAT",
        "position_id": "4189015d-3009-458c-bfb1-2a55bd8251d5",
        "exchange_position_active": 0,
        "sl_order_id": "1000000082696143",
        "tp_order_id": "1000000082696151",
        "reconciliation_status": "FLAT",
        "reconciliation_reason": "PERSISTED:exchange_flat",
        "last_reconciled_at": "2026-05-22T19:39:25.249210"
      }
    }
  ]
}
```

## Safety Invariant Results
```json
{
  "webhook_direct_executor_calls": 0,
  "queue_direct_execution_calls": 0,
  "unsupported_actions_executed": 0,
  "close_reverse_reduce_executed": 0,
  "cancel_executed": 0,
  "sltp_update_executed_from_tradingview": 0,
  "external_size_used": 0,
  "risk_override_used": 0,
  "duplicate_processed_queue_rows": 0,
  "stuck_claimed_rows": 0,
  "unprotected_positions": 0,
  "negative_checks_correctly_rejected": 5,
  "negative_checks_total": 5,
  "negative_check_failures": []
}
```

## Admin Visibility
```json
{
  "limited_status_reachable": true,
  "processor_status_reachable": true,
  "secrets_exposed": false
}
```

## Incidents / Anomalies
```json
[]
```

## Whether Phase 6H / Broader Rollout Is Allowed
`True`