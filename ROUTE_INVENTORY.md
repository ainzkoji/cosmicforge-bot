# CosmicForge Route Inventory

**Last Updated**: 2026-01-20  
**Version**: 2.0 (After `/api/v1` standardization)

---

## Route Standardization Summary

All bot-owned routes now use the `/api/v1` prefix consistently. User-facing routes use various prefixes based on functionality.

---

## Bot-Owned Routes (All use `/api/v1`)

### Bot-Backend Service (Port 9000)

| Route | Handler Module | Description |
|-------|----------------|-------------|
| `POST /api/v1/bot-instances/apply-official-strategy` | strategy_marketplace.py | Deploy official strategy to brokers |
| `GET /api/v1/bot-instances` | strategy_marketplace.py | List user's bot instances |
| `GET /api/v1/bot-instances/{id}` | strategy_marketplace.py | Get specific bot instance |
| `POST /api/v1/bot-instances/{id}/start` | strategy_marketplace.py | Start bot instance |
| `POST /api/v1/bot-instances/{id}/pause` | strategy_marketplace.py | Pause bot instance |
| `POST /api/v1/bot-instances/{id}/stop` | strategy_marketplace.py | Stop bot instance |
| `PUT /api/v1/bot-instances/{id}` | strategy_marketplace.py | Update bot configuration |
|  `DELETE /api/v1/bot-instances/{id}` | strategy_marketplace.py | Delete bot instance |
| | |
| `GET /api/v1/strategies/marketplace` | strategy_marketplace.py | List official strategies |
| `GET /api/v1/strategies/marketplace/{id}` | strategy_marketplace.py | Get strategy details |
| `POST /api/v1/strategies/marketplace/{id}/validate-params` | strategy_marketplace.py | Validate strategy parameters |
| | |
| `GET /api/v1/analytics/overview` | analytics.py | Portfolio overview stats |
| `GET /api/v1/analytics/leaderboard` | analytics.py | Top performing strategies |
| `GET /api/v1/analytics/calibration` | analytics.py | Confidence calibration buckets |
| | |
| `GET /api/v1/strategy-configs` | strategy_configs.py | List strategy configurations |
| `POST /api/v1/strategy-configs` | strategy_configs.py | Create new configuration |
| `GET /api/v1/strategy-configs/{id}` | strategy_configs.py | Get specific configuration |
| `PUT /api/v1/strategy-configs/{id}` | strategy_configs.py | Update configuration |
| `DELETE /api/v1/strategy-configs/{id}` | strategy_configs.py | Archive configuration |
| `POST /api/v1/strategy-configs/{id}/activate` | strategy_configs.py | Activate configuration |
| `POST /api/v1/strategy-configs/{id}/deactivate` | strategy_configs.py | Deactivate configuration |
| `GET /api/v1/strategy-configs/{id}/protection-status` | strategy_configs.py | Get protection status |
| `POST /api/v1/strategy-configs/{id}/reset-protection` | strategy_configs.py | Reset protection manually |
| | |
| `GET /api/v1/risk-profiles/templates` | risk_profiles.py | Get risk profile presets |
| `POST /api/v1/risk-profiles/calculate` | risk_profiles.py | Calculate position size |
| `POST /api/v1/risk-profiles/validate` | risk_profiles.py | Validate risk parameters |
| | |
| `GET /api/v1/monitoring/system-health` | monitoring.py | System health status |
| `GET /api/v1/monitoring/system-metrics` | monitoring.py | Detailed system metrics |
| `POST /api/v1/monitoring/system/record-metric` | monitoring.py | Record custom metric |
| `GET /api/v1/monitoring/bots-overview` | monitoring.py | Bot activity overview |
| `GET /api/v1/monitoring/bots-executions` | monitoring.py | Recent bot executions |
| `POST /api/v1/monitoring/bots-emergency-stop` | monitoring.py | Emergency stop all bots |
| `GET /api/v1/monitoring/activity-events` | monitoring.py | Activity event feed |
| `POST /api/v1/monitoring/activity-log-event` | monitoring.py | Log new activity event |
| `GET /api/v1/monitoring/transactions` | monitoring.py | Get all transactions |
| `POST /api/v1/monitoring/transactions/{id}/approve` | monitoring.py | Approve transaction |
| `GET /api/v1/monitoring/feature-flags` | monitoring.py | Get feature flags |
| `PUT /api/v1/monitoring/feature-flags/{id}/toggle` | monitoring.py | Toggle feature flag |

---

## User-Owned Routes

### User-Backend Service (Port 8000)

| Route | Handler Module | Description |
|-------|----------------|-------------|
| **Authentication** | |
| `POST /auth/register` | auth.py | Register new user |
| `POST /auth/login` | auth.py | Login with credentials |
| `POST /auth/refresh` | auth.py | Refresh access token |
| `POST /auth/logout` | auth.py | Logout user |
| `GET /auth/me` | auth.py | Get current user profile |
| `PUT /auth/me` | auth.py | Update user profile |
| `POST /auth/2fa/enable` | auth.py | Enable 2FA |
| `POST /auth/2fa/verify` | auth.py | Verify 2FA code |
| | |
| **Public Pages** | |
| `GET /public/home` | public.py | Home page content |
| `GET /public/features` | public.py | Features list |
| `GET /public/how-it-works` | public.py | How it works content |
| `GET /public/pricing` | public.py | Pricing plans |
| `POST /public/session` | public.py | Create marketing session |
| `POST /public/pricing/intent` | public.py | Record pricing intent |
| | |
| **KYC** | |
| `POST /kyc/cases` | kyc.py | Create KYC case |
| `GET /kyc/cases/{id}` | kyc.py | Get KYC case status |
| `POST /kyc/cases/{id}/upload-url` | kyc.py | Request document upload URL |
| `POST /kyc/cases/{id}/confirm-upload` | kyc.py | Confirm document uploaded |
| `POST /kyc/cases/{id}/submit` | kyc.py | Submit for review |
| | |
| **Brokers** | |
| `GET /api/brokers/catalog` | brokers.py | List available brokers |
| `POST /api/brokers/connections` | brokers.py | Create broker connection |
| `GET /api/brokers/connections` | brokers.py | List user's connections |
| `PUT /api/brokers/connections/{id}/credentials` | brokers.py | Update credentials |
| `POST /api/brokers/connections/{id}/validate` | brokers.py | Validate connection |
| | |
| **Billing** | |
| `GET /api/billing/plans` | billing.py | List subscription plans |
| `POST /api/billing/subscriptions` | billing.py | Create subscription |
| `GET /api/billing/subscriptions/{id}` | billing.py | Get subscription details |
| `DELETE /api/billing/subscriptions/{id}` | billing.py | Cancel subscription |
| | |
| **Onboarding** | |
| `GET /api/onboarding/state` | onboarding.py | Get onboarding state |
| `POST /api/onboarding/step` | onboarding.py | Save step progress |
| `POST /api/onboarding/complete` | onboarding.py | Complete onboarding |
| `GET /api/onboarding/strategies` | onboarding.py | Get available strategies |
| `GET /api/onboarding/next-steps` | onboarding.py | Determine next steps |
| | |
| **Portfolio** | |
| `GET /api/portfolio/summary` | portfolio.py | Portfolio summary |
| `GET /api/portfolio/transactions` | transactions_router.py | Transaction history |
| | |
| **Admin** | |
| `GET /api/admin/dashboard/stats` | admin.py | Dashboard statistics |
| `GET /api/admin/users` | admin.py | List all users |
| `POST /api/admin/users/{id}/suspend` | admin.py | Suspend user |
| `POST /api/admin/users/{id}/activate` | admin.py | Activate user |
| `GET /api/admin/revenue/overview` | admin.py | Revenue analytics |
| `GET /api/admin/audit-logs` | admin.py | Audit logs |

---

## Proxied Routes

The following routes are accessed through user-backend (port 8000) but proxied to bot-backend (port 9000):

| Frontend Accesses | User-Backend Proxy | Forwards To (Bot-Backend) |
|-------------------|-------------------|---------------------------|
| `GET /api/v1/bot-instances` | bot_instances_proxy.py | `GET http://localhost:9000/api/v1/bot-instances` |
| `POST /api/v1/bot-instances/apply-official-strategy` | bot_instances_proxy.py | `POST http://localhost:9000/api/v1/bot-instances/apply-official-strategy` |
| `POST /api/v1/bot-instances/{id}/start` | bot_instances_proxy.py | `POST http://localhost:9000/api/v1/bot-instances/{id}/start` |
| `POST /api/v1/bot-instances/{id}/pause` | bot_instances_proxy.py | `POST http://localhost:9000/api/v1/bot-instances/{id}/pause` |
| `POST /api/v1/bot-instances/{id}/stop` | bot_instances_proxy.py | `POST http://localhost:9000/api/v1/bot-instances/{id}/stop` |
| `DELETE /api/v1/bot-instances/{id}` | bot_instances_proxy.py | `DELETE http://localhost:9000/api/v1/bot-instances/{id}` |
| `GET /api/v1/strategies/marketplace` | strategies_proxy.py | `GET http://localhost:9000/api/v1/strategies/marketplace` |
| `GET /api/v1/strategies/marketplace/{id}` | strategies_proxy.py | `GET http://localhost:9000/api/v1/strategies/marketplace/{id}` |
| `GET /api/v1/analytics/*` | analytics_proxy.py | `GET http://localhost:9000/api/v1/analytics/*` |
| `GET /api/v1/strategy-configs/*` | strategy_configs_proxy.py | `* http://localhost:9000/api/v1/strategy-configs/*` |
| `GET /api/v1/risk-profiles/*` | risk_profiles_proxy.py | `* http://localhost:9000/api/v1/risk-profiles/*` |
| `GET /api/v1/monitoring/*` | monitoring_proxy.py | `* http://localhost:9000/api/v1/monitoring/*` |

---

## Frontend Access Pattern

```
┌──────────────┐
│   Frontend   │
│ (Port 5173)  │
└──────┬───────┘
       │
       │ All API calls go to user-backend
       ▼
┌────────────────────────────────┐
│     User-Backend               │
│     (Port 8000)               │
│                               │
│  ┌─────────────────────────┐ │
│  │ User Routes             │ │
│  │ - /auth/*               │ │
│  │ - /kyc/*                │ │
│  │ - /api/brokers/*        │ │
│  │ - /api/billing/*        │ │
│  │ - /public/*             │ │
│  └─────────────────────────┘ │
│                               │
│  ┌─────────────────────────┐ │
│  │ Proxy Routes            │ │
│  │ - /api/v1/bot-instances│ │
│  │ - /api/v1/strategies/* │ │
│  │ - /api/v1/analytics/*  │ │
│  │ - /api/v1/monitoring/* │ │
│  └──────────┬──────────────┘ │
└─────────────┼────────────────┘
              │
              │ Forward to bot-backend
              ▼
       ┌──────────────────┐
       │   Bot-Backend    │
       │   (Port 9000)    │
       │                  │
       │ - Strategy engine│
       │ - Bot management │
       │ - Analytics      │
       │ - Monitoring     │
       └──────────────────┘
```

---

## Testing Routes

### Health Check
```bash
# User-backend
curl http://localhost:8000/health

# Bot-backend  
curl http://localhost:9000/health
```

### Bot-Owned Routes (requires auth)
```bash
# Get bot instances (through proxy)
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/v1/bot-instances

# Get strategy marketplace (through proxy)
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/v1/strategies/marketplace

# Get analytics (through proxy)
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/v1/analytics/overview

# Direct to bot-backend (should work same way)
curl -H "Authorization: Bearer <token>" http://localhost:9000/api/v1/bot-instances
```

### User Routes
```bash
# Public route (no auth)
curl http://localhost:8000/public/home

# Auth route
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "user@example.com", "password": "password"}'
```

---

## Version History

### v2.0 (2026-01-20) - Route Standardization
- ✅ Standardized all bot-owned routes to use `/api/v1` prefix
- ✅ Updated bot-backend routers: analytics, monitoring, strategy_configs, risk_profiles
- ✅ Updated user-backend proxies to match new paths
- ✅ Monitoring route changed from `/admin/monitoring` to `/api/v1/monitoring`
- ✅ All proxy targets updated to `/api/v1/*`

### v1.0 (Before standardization)
- Mixed prefixes: `/api/analytics`, `/api/strategy-configs`, `/admin/monitoring`, etc.
- Inconsistent route patterns across services
