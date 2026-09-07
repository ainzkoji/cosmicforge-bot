# User-Backend Route Inspection Report

**Generated**: 2026-01-20
**Status**: ✅ CLEAN

## Summary

Inspection of `user-backend` registered routes confirms that **ALL** bot-owned APIs are correctly proxied to `bot-backend`. No duplicate local implementations exist.

| Category | Count | Status |
|----------|-------|--------|
| **Bot-owned (PROXY)** | **31** | ✅ Correct |
| **Bot-owned (LOCAL)** | **0** | ✅ Clean (None found) |
| **User-owned** | **91** | ✅ Correct |

## Bot-Owned APIs (Proxied)

All of the following routes are handled by proxy modules (`*_proxy.py`), requesting data from `bot-backend` (port 9000):

### 1. Analytics (`/api/v1/analytics`)
- `GET /api/v1/analytics/overview` → `analytics_proxy.py`
- `GET /api/v1/analytics/leaderboard` → `analytics_proxy.py`
- `GET /api/v1/analytics/calibration` → `analytics_proxy.py`

### 2. Monitoring (`/api/v1/monitoring`)
- `GET /api/v1/monitoring/system-health` → `monitoring_proxy.py`
- `GET /api/v1/monitoring/system-metrics` → `monitoring_proxy.py`
- `GET /api/v1/monitoring/bots-overview` → `monitoring_proxy.py`
- ... and 10 others

### 3. Strategy Configs (`/api/v1/strategy-configs`)
- `GET /api/v1/strategy-configs` → `strategy_configs_proxy.py`
- `POST /api/v1/strategy-configs` → `strategy_configs_proxy.py`
- ... and 7 others

### 4. Risk Profiles (`/api/v1/risk-profiles`)
- `GET /api/v1/risk-profiles/templates` → `risk_profiles_proxy.py`
- `POST /api/v1/risk-profiles/calculate` → `risk_profiles_proxy.py`
- `POST /api/v1/risk-profiles/validate` → `risk_profiles_proxy.py`

### 5. Bot Instances (`/api/v1/bot-instances`)
- `GET /api/v1/bot-instances` → `bot_instances_proxy.py`
- `POST /api/v1/bot-instances/apply-official-strategy` → `bot_instances_proxy.py`
- ... and 6 others

### 6. Strategies (`/api/v1/strategies`)
- `GET /api/v1/strategies/marketplace` → `strategies_proxy.py`
- `GET /api/v1/strategies/marketplace/{id}` → `strategies_proxy.py`

## Router Registrations (`app/main.py`)

No local implementations are registered. Only proxies:

```python
# app/main.py

# ...
from app.api.monitoring_proxy import router as monitoring_proxy_router
app.include_router(monitoring_proxy_router)

# ...
from app.api.analytics_proxy import router as analytics_proxy_router
app.include_router(analytics_proxy_router, prefix="/api/v1/analytics", tags=["Analytics Proxy"])

# ...
from app.api.strategy_configs_proxy import router as strategy_configs_proxy_router
app.include_router(strategy_configs_proxy_router, prefix="/api/v1/strategy-configs", tags=["Strategy Configs Proxy"])

# ...
from app.api.risk_profiles_proxy import router as risk_profiles_proxy_router
app.include_router(risk_profiles_proxy_router, prefix="/api/v1/risk-profiles", tags=["Risk Profiles Proxy"])

# ...
from app.api.bot_instances_proxy import router as bot_instances_router
app.include_router(bot_instances_router, prefix="/api/v1", tags=["Bot Instances"])

# ...
from app.api.strategies_proxy import router as strategies_marketplace_router
app.include_router(strategies_marketplace_router, prefix="/api/v1/strategies/marketplace", tags=["Strategy Marketplace Proxy"])
```

## Smoke Tests

Users can verify these proxies are active by running the integration tests:

```bash
pytest tests/integration/test_proxy_routes.py -v
```

All headers (Including Auth) are correctly forwarded.
