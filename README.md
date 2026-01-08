# CosmicForge Bot (Backend)

Automated trading **engine + API** for **Binance Futures (crypto)**.

> ⚠️ **Risk notice:** This repo can place real trades when `EXECUTION_MODE=live`. Use paper mode first, and never test with money you can’t lose.

---

## What this repo currently contains

### ✅ Implemented
- FastAPI backend service (control + inspection endpoints)
- Binance Futures REST client + request signing
- Trade execution helpers (market entries + SL/TP placement)
- Tick-size & lot-size rounding (exchange filters)
- Basic strategies:
  - SMA Cross
  - EMA/SMA/RSI ensemble
- Risk controls:
  - Daily loss kill-switch (blocks new opens/adds when triggered)
  - Optional “close positions on kill”
- Persistence:
  - SQLite DB for runs/events/audit state
  - Audit/event logging endpoint

### ❌ Not implemented yet (planned)
- Authentication/authorization (API keys/JWT)
- UI dashboard
- Backtesting framework
- Subscription/payment system
- Multi-broker support (Forex/stocks exchanges)

---

## Project layout

```
app/
  main.py                # FastAPI entrypoint + endpoints
  core/config.py         # Settings (env-driven)
  exchange/binance/      # Binance Futures client/signing/filters
  execution/             # executor, position manager, exit logic
  strategy/              # strategies + indicators
  risk/                  # daily loss, gating rules, pnl
  persistence/           # sqlite schema + audit logging
  runner/                # paper/live runner
  symbols/               # universe + leverage/sizing maps
reset_daily_kill.py      # utility to reset daily kill-switch
test_binance.py          # quick smoke test script
```

---

## Requirements
- Python **3.10+** recommended
- Binance Futures API key (only required for live mode; paper mode can run without keys, but some endpoints may still call Binance)

---

## Quick start

### 1) Create a virtual environment
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
```

### 2) Install dependencies
If you don’t have a `requirements.txt` yet, create one from the repo (see below), then:
```bash
pip install -r requirements.txt
```

### 3) Configure environment
Create a `.env` file in the repo root:

```env
# --- Exchange ---
BINANCE_API_KEY=
BINANCE_API_SECRET=
BINANCE_FAPI_BASE_URL=https://fapi.binance.com
BINANCE_RECV_WINDOW=5000

# --- Mode ---
EXECUTION_MODE=paper   # paper | live

# --- Symbols ---
TRADE_SYMBOLS=BTCUSDT,ETHUSDT
LIVE_SYMBOLS=BTCUSDT,ETHUSDT
DEFAULT_INTERVAL=1m
MAX_SYMBOLS=10

# --- Risk ---
DAILY_MAX_LOSS_USDT=100
KILL_SWITCH_CLOSE_POSITIONS=true

# --- Strategy/Execution ---
TRADE_MODE=flip
COOLDOWN_SECONDS=60
MAX_ADDS_PER_POSITION=2
STOP_LOSS_PCT=0.7
TAKE_PROFIT_PCT=1.2

RUN_INTERVAL_SECONDS=60
RUN_MAX_SYMBOLS=10

# After a stop loss, block new entries on that symbol for this many seconds
SL_COOLDOWN_SECONDS=3600
```

> Notes:
> - `TRADE_SYMBOLS` and `LIVE_SYMBOLS` accept comma-separated lists.
> - If `LIVE_SYMBOLS` is empty, the code defaults it to `TRADE_SYMBOLS`.

---

## Run the API (FastAPI)

From the repo root:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Then open:
- Swagger docs: `http://localhost:8000/docs`

---

## Common workflows

### Paper run (recommended first)
Use the API endpoint that triggers a paper cycle/run (see `/docs` once server is running). Paper mode should NOT place real orders.

### Live run (danger)
1. Set:
   - `EXECUTION_MODE=live`
   - add `BINANCE_API_KEY` and `BINANCE_API_SECRET`
2. Start with a tiny `SYMBOL_USDT_MAP` (if you enable it) and test on a single symbol.

---

## “Daily kill-switch” reset
If the daily kill-switch is active and you want to reset it:
```bash
python reset_daily_kill.py
```

---

## Next engineering tasks (MVP)
- Add auth (JWT or API key) before exposing the API anywhere
- Add unit tests for rounding/sizing/SL-TP placement (money-moving logic)
- Add a minimal dashboard (positions, PnL, kill status, last signals)
- Add Docker deployment + `.env.example`

---

## Disclaimer
This software is provided for educational purposes. You are responsible for any trades you execute and any losses incurred.
