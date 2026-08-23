# Post-Restart Safety Verification

Verified: `2026-06-15T04:55:28.0632786Z`

## Restart

- Method: stopped only the local bot-backend process on port `9000`, then
  started Uvicorn on `127.0.0.1:9000` with UTF-8 process output.
- Successful process start: `2026-06-15T04:46:39.294507+00:00`
- Runtime config load: `2026-06-15T04:46:39.321587+00:00`
- Worker PID: `50088`
- Run ID: `793bebf7-55f0-4c92-b867-1f5d5f3d69b6`
- Code version: `db4580b`

The first localhost-only launch attempt exited during PaperRunner initialization
because redirected Windows output used CP1252 for Unicode status text. No cycle
or order path ran. The successful launch added UTF-8 process-output variables;
the active `.env` remained unchanged and the bot auto-recovered to `active`.

The separate user and admin services on ports `8000` and `8100` remained
healthy and were not restarted.

## Runtime-Loaded Safety State

- `EXECUTION_MODE=paper`
- `BINANCE_ENV=testnet`
- `ML_ENABLED=False`
- `IOFS_GATE_MODE=shadow`
- `TRADE_SYMBOLS=BTCUSDT,ETHUSDT`
- `ENSEMBLE_SESSION_FILTER_ENABLED=True`
- `ENSEMBLE_SESSION_WINDOWS_UTC=06:00-19:00`
- `MAX_TRADES_DAILY=3`
- live symbols count: `0`
- bot instance status: `active`
- bot instance mode: `paper`

## Integrity

- Active `.env` SHA256 after restart:
  `1CF36622DD75B4AB091BB0D35CE9950FC641749A9857BA916B2435617E13D97C`
- Pre-restart SHA256 matches: `True`
- `models/production` contains only `README.md`
- Production `.pkl` model count: `0`

`ML_SHADOW_MODE=False` is runtime-loaded but cannot influence decisions because
`ML_ENABLED=False`. The active `.env` was not changed.

Safety verification: **PASS**. Runtime is paper-only, ML is disabled, IOFS is
shadow, no live symbols are enabled, and no capital was deployed.
