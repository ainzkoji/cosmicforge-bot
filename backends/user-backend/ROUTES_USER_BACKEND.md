# User-Backend Routes Documentation

This document lists all API routes exposed by `user-backend` (Port 8000).

**Note:** `user-backend` acts as the primary API gateway. It handles user-management endpoints locally and proxies trading-engine endpoints to `bot-backend` (Port 9000).

## 1. User Management (Local)

These endpoints are implemented locally in `user-backend`.

### Authentication (`/auth`)
- `POST /auth/register` - Register new user
- `POST /auth/login` - Login
- `POST /auth/refresh` - Refresh token
- `GET /auth/me` - Get current user profile
- `GET /auth/user/brokers` - List linked broker accounts

### KYC (`/kyc`)
- `GET /kyc/status` - Get KYC status
- `POST /kyc/start` - Start KYC process
- `POST /kyc/documents/upload-url` - Upload KYC documents

### Billing (`/api/billing`)
- (Endpoints for subscription management)

### Onboarding (`/api/onboarding`)
- `GET /api/onboarding/strategies` - Get strategies for onboarding
- `POST /api/onboarding/wizard` - Submit onboarding wizard data

### Public (`/public`)
- `GET /public/home` - Marketing homepage content
- `GET /public/pricing` - Pricing data

---

## 2. Trading Engine Proxies (Forwarded to Bot-Backend)

These endpoints are proxies. `user-backend` authenticates the user, then forwards the request to `bot-backend`.

### Monitoring (`/api/v1/monitoring`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/monitoring/system-health` | System health status |
| GET | `/api/v1/monitoring/bots-overview` | Active bots overview |
| GET | `/api/v1/monitoring/activity-events` | System activity logs |

### Analytics (`/api/v1/analytics`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/analytics/overview` | Portfolio overview stats |
| GET | `/api/v1/analytics/leaderboard` | Strategy performance leaderboard |
| GET | `/api/v1/analytics/calibration` | Confidence calibration stats |

### Bot Instances (`/api/v1/bot-instances`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/bot-instances` | List user's bot instances |
| GET | `/api/v1/bot-instances/{id}` | Get instance details |
| POST | `/api/v1/bot-instances/{id}/start` | Start bot |
| POST | `/api/v1/bot-instances/{id}/stop` | Stop bot |

### Strategy Marketplace (`/api/v1/strategies/marketplace`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/strategies/marketplace` | Browse official strategies |
| GET | `/api/v1/strategies/marketplace/{id}` | Get strategy details |
| POST | `/api/v1/strategies/marketplace/{id}/validate-params` | Validate strategy parameters |

### User Strategies (`/api/strategies`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/strategies/` | List user-created strategies |
| POST | `/api/strategies/` | Create new strategy |

### Strategy Configs (`/api/v1/strategy-configs`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/strategy-configs` | List configurations |
| POST | `/api/v1/strategy-configs` | Create configuration |
| POST | `/api/v1/strategy-configs/{id}/activate` | Activate config (trading) |

### Risk Profiles (`/api/v1/risk-profiles`)
| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/risk-profiles/templates` | Get risk templates |
| POST | `/api/v1/risk-profiles/calculate` | Calculate position size |

## Service Boundaries

- **User-Backend (8000)**: Owns User, Auth, KYC, Billing, Brokers data.
- **Bot-Backend (9000)**: Owns Bots, Strategies, Execution, Market Data.
