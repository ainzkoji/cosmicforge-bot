# CosmicForge Multi-Service Architecture

This is the refactored CosmicForge trading bot with separated concerns:

```
CosmicForge/
├── bot-backend/     # Trading engine (port 9000, private)
├── user-backend/    # Public API (port 8000, public)
└── shared/          # Shared DB layer
```

## Architecture

```
Frontend (5173) → user-backend (8000) → bot-backend (9000) → Binance
                       ↓                        ↓
                   Shared Database (SQLite/PostgreSQL)
```

## Quick Start

### 1. Install Shared Package

```bash
cd CosmicForge/shared
pip install -e .
```

### 2. Start Bot Backend (Engine)

```bash
cd CosmicForge/bot-backend
cp .env.example .env
# Edit .env with your Binance API keys
pip install -r requirements.txt
python -m app.main
```

### 3. Start User Backend (Public API)

In a new terminal:

```bash
cd CosmicForge/user-backend
cp .env.example .env
# Edit .env and set ENGINE_API_KEY to match bot-backend
pip install -r requirements.txt
python -m app.main
```

### 4. Verify Services

- Bot Backend: http://localhost:9000/health
- User Backend: http://localhost:8000/health

## Environment Variables

### Shared (both services)

- `DATABASE_URL` - Database connection string (default: `sqlite:///./bot.db`)

### Bot Backend

- `BINANCE_API_KEY` - Binance API key
- `BINANCE_API_SECRET` - Binance API secret
- `BINANCE_FAPI_BASE_URL` - Binance futures URL
- `ENGINE_API_KEY` - Authentication key for engine access
- `EXECUTION_MODE` - `paper` or `live`
- `TRADE_SYMBOLS` - Comma-separated symbols
- `DAILY_MAX_LOSS_USDT` - Max daily loss limit

### User Backend

- `SECRET_KEY` - JWT secret key
- `ENGINE_BASE_URL` - Bot backend URL (default: `http://localhost:9000`)
- `ENGINE_API_KEY` - Must match bot-backend
- `FRONTEND_URL` - Frontend URL for CORS

## Service Communication

user-backend → bot-backend communication uses HTTP with `X-ENGINE-KEY` header:

```python
# In user-backend
from app.clients import get_engine_client

client = get_engine_client()
status = await client.get_runner_status()
```

## Database

Both services share the same database via the `shared` package:

```python
from app.persistence.db import DB

db = DB()
with db.connect() as conn:
    # Execute queries
```

## Security Notes

1. **bot-backend** should NOT be publicly exposed
2. Only **user-backend** should call bot-backend
3. Use strong `ENGINE_API_KEY` in production
4. Never commit `.env` files
5. Use HTTPS/TLS in production

## Development

Run both services with auto-reload:

```bash
# Terminal 1 - Bot Backend
cd CosmicForge/bot-backend
uvicorn app.main:app --port 9000 --reload --reload-dir app

# Terminal 2 - User Backend
cd CosmicForge/user-backend
uvicorn app.main:app --port 8000 --reload --reload-dir app
```

## Testing

Test engine connectivity:

```bash
curl -H "X-ENGINE-KEY: your-key-here" http://localhost:9000/runner/status
```

Test user API:

```bash
curl http://localhost:8000/health
```

## Migration from Old Structure

The old monolithic `backend/app/main.py` has been split:

- **Trading/engine endpoints** → bot-backend
- **User/admin API** → user-backend
- **Database layer** → shared package

Import paths have been updated accordingly.

## Troubleshooting

### Import Errors

Make sure shared package is installed:
```bash
cd CosmicForge/shared
pip install -e .
```

### Authentication Errors

Ensure `ENGINE_API_KEY` matches in both `.env` files.

### Database Errors

Check `DATABASE_URL` is set correctly in both services.
