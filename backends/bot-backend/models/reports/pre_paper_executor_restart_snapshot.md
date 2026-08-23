# Pre Paper Executor Restart Snapshot

- generated_at_utc: 2026-07-15T21:10:43.1222356Z
- working_directory: C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot-backend
- active_env_sha256: 6E3A3B649427C1162D021B6F9F8E992CDA0B21B200261E3D134C0A3AA8FCC4F9
- port_9000_listener_pid: 39672
- health_status: ok
- execution_mode: paper
- binance_env: testnet
- trade_symbols: BTCUSDT,ETHUSDT
- live_symbols_count: 0
- ml_enabled: False
- iofs_gate_mode: shadow
- historical_paper_only_skipped_attempts: 68

## Bot Processes

``text
[
    {
        "process_id":  39672,
        "parent_process_id":  39960,
        "name":  "python.exe",
        "command_line":  "\"C:\\Program Files\\Python312\\python.exe\" -m uvicorn app.main:app --reload --reload-dir app --host 127.0.0.1 --port 9000"
    },
    {
        "process_id":  17464,
        "parent_process_id":  39672,
        "name":  "python.exe",
        "command_line":  "\"C:\\Program Files\\Python312\\python.exe\" \"-c\" \"from multiprocessing.spawn import spawn_main; spawn_main(parent_pid=39672, pipe_handle=3044)\" \"--multiprocessing-fork\""
    }
]
``

## Health Endpoint

``json
{
    "status":  "ok",
    "time_utc":  "2026-07-15T21:10:42.200486+00:00",
    "execution_mode":  "paper",
    "binance_env":  "testnet",
    "binance_base_url":  "https://testnet.binancefuture.com",
    "default_interval":  "15m",
    "trade_symbols_count":  2,
    "trade_symbols":  "BTCUSDT,ETHUSDT",
    "live_symbols_count":  0,
    "ml_enabled":  false,
    "iofs_gate_mode":  "shadow",
    "strong_trend_allowed_only_in_paper":  true,
    "strong_trend_configured_unblocked":  true,
    "strong_trend_effective_unblocked":  true,
    "strong_trend_guard_reason":  "paper_only_requirements_met",
    "max_live_trades_per_cycle":  1,
    "risk":  {
                 "daily_max_loss_usdt":  50.0,
                 "kill_switch_close_positions":  true,
                 "stop_loss_pct":  0.02,
                 "take_profit_pct":  0.036
             },
    "tradingview_runtime_fingerprint":  {
                                            "code_version":  "db4580b",
                                            "process_started_at":  "2026-07-15T20:52:26.951065+00:00",
                                            "config_loaded_at":  "2026-07-15T20:52:26.992092+00:00",
                                            "pid":  17464,
                                            "working_directory":  "C:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\bot-backend",
                                            "python_executable":  "C:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\venv\\Scripts\\python.exe",
                                            "phase6_gate_available":  true,
                                            "phase6_gate_code_version":  "phase6_limited_gate_v1_2026-05-21",
                                            "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED":  true,
                                            "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED":  false,
                                            "TRADINGVIEW_ALLOWED_SYMBOLS":  [
                                                                                "BTCUSDT",
                                                                                "ETHUSDT"
                                                                            ],
                                            "TRADINGVIEW_ALLOWED_ACTIONS":  [
                                                                                "BUY",
                                                                                "SELL"
                                                                            ],
                                            "TRADINGVIEW_MAX_QUEUE_PER_CYCLE":  1,
                                            "TRADINGVIEW_MAX_EXECUTIONS_PER_DAY":  3,
                                            "TRADINGVIEW_MAX_SIGNALS_PER_HOUR":  5,
                                            "TRADINGVIEW_MAX_SIGNALS_PER_DAY":  20,
                                            "TRADINGVIEW_MAX_TRADE_USDT_CAP":  150.0,
                                            "TRADINGVIEW_ALLOW_CLOSE":  false,
                                            "TRADINGVIEW_ALLOW_REVERSE":  false,
                                            "TRADINGVIEW_ALLOW_REDUCE":  false,
                                            "TRADINGVIEW_ALLOW_CANCEL":  false,
                                            "TRADINGVIEW_ALLOW_EXTERNAL_SLTP":  false,
                                            "TRADINGVIEW_ALLOW_EXTERNAL_SIZE":  false,
                                            "TRADINGVIEW_ALLOW_RISK_OVERRIDE":  false,
                                            "TRADINGVIEW_REQUIRE_SLTP_PROTECTION":  true,
                                            "TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL":  true,
                                            "active_safety_lockout":  false,
                                            "active_safety_lockout_reason":  "CLEARED: Sequential Phase 6A proof retry after leftover proof rows were safely rejected"
                                        }
}
``

## models/production

``json
{
    "name":  "README.md",
    "length":  439,
    "last_write_time":  "2026-05-01T14:34:04.4368046Z"
}
``

## Paper Execution Reports

``json
{
    "strong_trend_order_count_diagnosis_summary":  {
                                                       "duplicate_orders":  0,
                                                       "failed_attempts":  68,
                                                       "historical_paper_only_skipped_attempts":  68,
                                                       "old_orders_before_clean_start":  347,
                                                       "old_orders_before_experiment":  0,
                                                       "test_fixture_rows":  0,
                                                       "total_reported_paper_orders":  68,
                                                       "unknown_rows":  0,
                                                       "valid_post_experiment_strong_trend_orders":  0,
                                                       "wrong_bot_instance_orders":  0,
                                                       "wrong_regime_orders":  1,
                                                       "wrong_symbol_orders":  0
                                                   },
    "strong_trend_paper_experiment":  {
                                          "strong_trend_order_attempts":  68,
                                          "strong_trend_paper_orders_created":  0,
                                          "strong_trend_order_errors":  68,
                                          "strong_trend_fills":  0,
                                          "strong_trend_closed_trades":  0,
                                          "order_count_diagnosis_summary":  {
                                                                                "duplicate_orders":  0,
                                                                                "failed_attempts":  68,
                                                                                "historical_paper_only_skipped_attempts":  68,
                                                                                "old_orders_before_clean_start":  347,
                                                                                "old_orders_before_experiment":  0,
                                                                                "test_fixture_rows":  0,
                                                                                "total_reported_paper_orders":  68,
                                                                                "unknown_rows":  0,
                                                                                "valid_post_experiment_strong_trend_orders":  0,
                                                                                "wrong_bot_instance_orders":  0,
                                                                                "wrong_regime_orders":  1,
                                                                                "wrong_symbol_orders":  0
                                                                            }
                                      },
    "daily_paper_validation":  {
                                   "paper_orders_today":  0,
                                   "paper_fills_today":  0,
                                   "closed_paper_trades_since_clean_start":  0,
                                   "strong_trend_order_count_diagnosis_summary":  {
                                                                                      "failed_attempts":  68,
                                                                                      "historical_paper_only_skipped_attempts":  68,
                                                                                      "total_reported_paper_orders":  68,
                                                                                      "valid_post_experiment_strong_trend_orders":  0
                                                                                  }
                               },
    "paper_cycle_findings":  {
                                 "runner_loop_alive":  true,
                                 "market_data_loading":  false,
                                 "strategy_decisions_created":  true,
                                 "paper_executor_reached":  true,
                                 "paper_orders_attempted":  1,
                                 "top_blocking_reason":  "strategy_no_signal",
                                 "iofs_shadow_blocking_bug":  false,
                                 "ml_disabled_blocking_bug":  false,
                                 "session_filter_blocking_all_cycles":  false,
                                 "latest_sample_all_session_blocked":  false,
                                 "circuit_or_daily_limit_stuck":  false,
                                 "executor_reachable_when_gates_pass":  true
                             }
}
``
