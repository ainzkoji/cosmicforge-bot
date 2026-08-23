# Pre-Restart Safety Snapshot

Captured: `2026-06-15T04:43:37.1774343Z`

## Active Safety State

- Active `.env` SHA256: `1CF36622DD75B4AB091BB0D35CE9950FC641749A9857BA916B2435617E13D97C`
- `EXECUTION_MODE=paper`
- `ML_ENABLED=False`
- `IOFS_GATE_MODE=shadow`
- `TRADE_SYMBOLS=BTCUSDT,ETHUSDT`
- `ENSEMBLE_SESSION_FILTER_ENABLED=True`
- `ENSEMBLE_SESSION_WINDOWS_UTC=06:00-19:00`
- `MAX_TRADES_DAILY=3`
- `STRATEGY_NAME=master_ensemble`

## Runtime

- Service: local bot backend on port `9000`
- Health: healthy
- Runtime execution mode: `paper`
- Exchange environment: `testnet`
- Uvicorn root PID: `26472`
- Worker PID: `77832`
- Bot instance: `bot_e5fe913972a9`
- Bot instance mode: `paper`
- Bot instance symbols: `BTCUSDT,ETHUSDT`

Ports `8000` and `8100` are separate user/admin services and are excluded from
the controlled restart.

## Production Models

`models/production` contains only `README.md`. No `.pkl` production model
exists.

## Git Status

The top-level repository reports `backends/bot-backend` as untracked. The
active `.env` has no separate status entry and is unchanged.

Safety confirmation: paper only, ML disabled, IOFS shadow, no production model,
and no live capital.
