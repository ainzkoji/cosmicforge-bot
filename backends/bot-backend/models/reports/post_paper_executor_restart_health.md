# Post Paper Executor Restart Health

- generated_at_utc: 2026-07-15T21:15:22.6035923Z
- restart_time_utc: 2026-07-15T21:13:27.089449+00:00
- port: 9000
- listener_pid: 29964
- app_pid: 18276
- working_directory: C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot-backend
- python_executable: C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\venv\Scripts\python.exe
- execution_mode: paper
- binance_env: testnet
- trade_symbols: BTCUSDT,ETHUSDT
- live_symbols_count: 0
- ml_enabled: False
- iofs_gate_mode: shadow

## Safety Confirmation

``json
{
    "execution_mode_paper":  true,
    "binance_env_testnet":  true,
    "trade_symbols_btc_eth":  true,
    "live_symbols_count_zero":  true,
    "ml_disabled":  true,
    "iofs_shadow":  true
}
``

## Health Endpoint

``json
{
    "status":  "ok",
    "time_utc":  "2026-07-15T21:15:20.037142+00:00",
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
                                            "process_started_at":  "2026-07-15T21:13:27.089449+00:00",
                                            "config_loaded_at":  "2026-07-15T21:13:27.121457+00:00",
                                            "pid":  18276,
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
                                            "active_safety_lockout_reason":  null
                                        }
}
``

## Process Rows

``json
[
    {
        "process_id":  29964,
        "parent_process_id":  34168,
        "name":  "python.exe",
        "command_line":  "\"C:\\Program Files\\Python312\\python.exe\" -m uvicorn app.main:app --reload --reload-dir app --host 127.0.0.1 --port 9000 "
    },
    {
        "process_id":  18276,
        "parent_process_id":  29964,
        "name":  "python.exe",
        "command_line":  "\"C:\\Program Files\\Python312\\python.exe\" \"-c\" \"from multiprocessing.spawn import spawn_main; spawn_main(parent_pid=29964, pipe_handle=820)\" \"--multiprocessing-fork\""
    },
    {
        "process_id":  34168,
        "parent_process_id":  6000,
        "name":  "python.exe",
        "command_line":  "\"C:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\venv\\Scripts\\python.exe\" -m uvicorn app.main:app --reload --reload-dir app --host 127.0.0.1 --port 9000 "
    },
    {
        "process_id":  21956,
        "parent_process_id":  22476,
        "name":  "powershell.exe",
        "command_line":  "\"C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe\" -Command \"try { [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 } catch {}\n$ErrorActionPreference = \u0027Stop\u0027\n$root = (Get-Location).Path\n$reports = Join-Path $root \u0027models\\reports\u0027\n$health = Invoke-RestMethod http://127.0.0.1:9000/health -TimeoutSec 20\n$netstat = netstat -ano | findstr :9000\n$listenerPid = (($netstat | Select-String \u0027LISTENING\\s+(\\d+)\u0027 | Select-Object -First 1).Matches.Groups[1].Value)\n$ids = @()\nif ($listenerPid) { $ids += [int]$listenerPid }\nif ($health.tradingview_runtime_fingerprint.pid) { $ids += [int]$health.tradingview_runtime_fingerprint.pid }\n$parents = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like \u0027*uvicorn app.main:app*9000*\u0027 }\n$ids += $parents.ProcessId\n$processRows = foreach ($id in ($ids | Select-Object -Unique)) {\n  $p = Get-CimInstance Win32_Process -Filter \\\"ProcessId=$id\\\"\n  if ($p) { [ordered]@{ process_id=$p.ProcessId; parent_process_id=$p.ParentProcessId; name=$p.Name; command_line=$p.CommandLine } }\n}\n$report = [ordered]@{\n  generated_at_utc = (Get-Date).ToUniversalTime().ToString(\u0027o\u0027)\n  restart_time_utc = $health.tradingview_runtime_fingerprint.process_started_at\n  port = 9000\n  netstat_9000 = @($netstat)\n  listener_pid = if ($listenerPid) { [int]$listenerPid } else { $null }\n  app_pid = $health.tradingview_runtime_fingerprint.pid\n  working_directory = $health.tradingview_runtime_fingerprint.working_directory\n  python_executable = $health.tradingview_runtime_fingerprint.python_executable\n  process_rows = @($processRows)\n  health = $health\n  safety_confirmation = [ordered]@{\n    execution_mode_paper = ($health.execution_mode -eq \u0027paper\u0027)\n    binance_env_testnet = ($health.binance_env -eq \u0027testnet\u0027)\n    trade_symbols_btc_eth = ($health.trade_symbols -eq \u0027BTCUSDT,ETHUSDT\u0027)\n    live_symbols_count_zero = ([int]$health.live_symbols_count -eq 0)\n    ml_disabled = ($health.ml_enabled -eq $false)\n    iofs_shadow = ($health.iofs_gate_mode -eq \u0027shadow\u0027)\n  }\n}\n$jsonPath = Join-Path $reports \u0027post_paper_executor_restart_health.json\u0027\n$mdPath = Join-Path $reports \u0027post_paper_executor_restart_health.md\u0027\n$report | ConvertTo-Json -Depth 20 | Set-Content -Path $jsonPath -Encoding UTF8\n$md = @\\\"\n# Post Paper Executor Restart Health\n\n- generated_at_utc: $($report.generated_at_utc)\n- restart_time_utc: $($report.restart_time_utc)\n- port: 9000\n- listener_pid: $($report.listener_pid)\n- app_pid: $($report.app_pid)\n- working_directory: $($report.working_directory)\n- python_executable: $($report.python_executable)\n- execution_mode: $($health.execution_mode)\n- binance_env: $($health.binance_env)\n- trade_symbols: $($health.trade_symbols)\n- live_symbols_count: $($health.live_symbols_count)\n- ml_enabled: $($health.ml_enabled)\n- iofs_gate_mode: $($health.iofs_gate_mode)\n\n## Safety Confirmation\n\n````json\n$($report.safety_confirmation | ConvertTo-Json -Depth 6)\n````\n\n## Health Endpoint\n\n````json\n$($health | ConvertTo-Json -Depth 12)\n````\n\n## Process Rows\n\n````json\n$($processRows | ConvertTo-Json -Depth 6)\n````\n\\\"@\n$md | Set-Content -Path $mdPath -Encoding UTF8\n$report | ConvertTo-Json -Depth 10\""
    }
]
``
