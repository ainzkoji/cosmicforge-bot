# CosmicForge Bot Backend (Engine Service)

This is the trading engine service that runs the bot loop and handles all trading operations.

## Port
- **9000** (internal service, should NOT be publicly exposed)

## Authentication
- Requires `X-ENGINE-KEY` header for all protected endpoints
- Set `ENGINE_API_KEY` in `.env` file

## Setup

1. Copy `.env.example` to `.env` and configure:
   ```bash
   cp .env.example .env
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install shared package:
   ```bash
   pip install -e ../shared
   ```

4. Run the service:
   ```bash
   python -m app.main
   # or
   uvicorn app.main:app --port 9000 --reload --reload-dir app
   ```

## Key Endpoints

- `GET /` - Service info
- `GET /health` - Health check
- `GET /runner/status` - Runner status (requires auth)
- `GET /binance/balance` - Account balance (requires auth)
- `GET /binance/price` - Price data (requires auth)

## Features

- **Auto-start runner loop** - Starts automatically on service startup
- **Binance integration** - Direct exchange connectivity
- **Risk management** - Built-in safety gates and circuit breakers
- **Audit logging** - Comprehensive event tracking
- **State persistence** - DB-backed state management

## Notes

- This service should run on a private network
- Only user-backend should call this service
- Never expose port 9000 to the internet
