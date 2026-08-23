# CosmicForge User Backend (Public API Service)

This is the public-facing API service that handles user/admin operations and proxies to the engine service.

## Port
- **8000** (public service, can be exposed via reverse proxy)

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
   uvicorn app.main:app --port 8000 --reload --reload-dir app
   ```

## Key Endpoints

- `POST /auth/login` - User authentication
- `POST /auth/register` - User registration
- `GET /api/strategies` - Strategy marketplace
- `GET /api/brokers` - Broker catalog
- `GET /api/analytics` - User analytics
- `GET /api/admin/*` - Admin panel endpoints

## Features

- **User authentication** - JWT-based auth system
- **KYC workflows** - Identity verification
- **Broker integration** - API key management
- **Billing system** - Subscription management
- **Admin panel** - User management
- **Engine client** - HTTP client for bot-backend communication

## Engine Communication

This service communicates with bot-backend via HTTP using the engine client:

```python
from app.clients import get_engine_client

client = get_engine_client()
status = await client.get_runner_status()
```

## Notes

- Runs database migrations on startup
- CORS enabled for frontend (localhost:5173)
- Proxies trading operations to bot-backend
- Requires `ENGINE_API_KEY` to match bot-backend
