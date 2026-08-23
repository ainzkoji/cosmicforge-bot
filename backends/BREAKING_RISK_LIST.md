# Breaking Change Risk Assessment

## Top 10 Endpoints to Protect

These endpoints are at highest risk of causing breaking changes if modified:

### 1. `/api/admin/dashboard/stats`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 2. `/api/admin/dashboard/revenue-overview`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 3. `/api/admin/users`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 4. `/api/admin/revenue/overview`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 5. `/api/admin/commissions/tiers`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 6. `/api/admin/audit-logs`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 7. `/api/admin/compliance/kyc-pending`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 8. `/api/admin/compliance/aml-flags`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (4 times)
- **Recommendation**: Version this endpoint before making changes

### 9. `/plans`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (2 times)
- **Recommendation**: Version this endpoint before making changes

### 10. `/api/onboarding/state`

- **Risk Level**: CRITICAL
- **Reason**: Called by frontend (2 times)
- **Recommendation**: Version this endpoint before making changes

## Issue Summary

- **Duplicate Endpoints**: 125
- **Prefix Mismatches**: 0
- **Frontend Missing**: 1
- **Potential Proxy Gaps**: 0

### Duplicate Endpoints Detail

- `GET` `/`: user=app.main, bot=app.main
- `GET` `/admin/monitoring/activity/events`: user=app.api.monitoring, bot=app.api.monitoring
- `POST` `/admin/monitoring/activity/log-event`: user=app.api.monitoring, bot=app.api.monitoring
- `POST` `/admin/monitoring/bots/emergency-stop`: user=app.api.monitoring, bot=app.api.monitoring
- `GET` `/admin/monitoring/bots/executions`: user=app.api.monitoring, bot=app.api.monitoring
- `GET` `/admin/monitoring/bots/overview`: user=app.api.monitoring, bot=app.api.monitoring
- `GET` `/admin/monitoring/feature-flags`: user=app.api.monitoring, bot=app.api.monitoring
- `PUT` `/admin/monitoring/feature-flags/{flag_id}/toggle`: user=app.api.monitoring, bot=app.api.monitoring
- `GET` `/admin/monitoring/system/health`: user=app.api.monitoring, bot=app.api.monitoring
- `GET` `/admin/monitoring/system/metrics`: user=app.api.monitoring, bot=app.api.monitoring
- `POST` `/admin/monitoring/system/record-metric`: user=app.api.monitoring, bot=app.api.monitoring
- `GET` `/admin/monitoring/transactions`: user=app.api.monitoring, bot=app.api.monitoring
- `POST` `/admin/monitoring/transactions/{transaction_id}/approve`: user=app.api.monitoring, bot=app.api.monitoring
- `GET` `/api/admin/commissions/tiers`: user=app.api.admin, bot=app.api.admin
- `PUT` `/api/admin/commissions/tiers/{tier_id}`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/admin/compliance/aml-flags`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/admin/compliance/kyc-pending`: user=app.api.admin, bot=app.api.admin
- `POST` `/api/admin/compliance/kyc/{submission_id}/approve`: user=app.api.admin, bot=app.api.admin
- `POST` `/api/admin/compliance/kyc/{submission_id}/reject`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/admin/dashboard/revenue-overview`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/admin/dashboard/stats`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/admin/revenue/overview`: user=app.api.admin, bot=app.api.admin
- `POST` `/api/admin/roles/grant`: user=app.api.admin, bot=app.api.admin
- `POST` `/api/admin/roles/revoke`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/admin/users`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/admin/users/{user_id}`: user=app.api.admin, bot=app.api.admin
- `POST` `/api/admin/users/{user_id}/activate`: user=app.api.admin, bot=app.api.admin
- `POST` `/api/admin/users/{user_id}/suspend`: user=app.api.admin, bot=app.api.admin
- `GET` `/api/analytics/calibration`: user=app.api.analytics, bot=app.api.analytics
- `GET` `/api/analytics/leaderboard`: user=app.api.analytics, bot=app.api.analytics
- `GET` `/api/analytics/overview`: user=app.api.analytics, bot=app.api.analytics
- `POST` `/api/billing/checkout`: user=app.api.billing, bot=app.api.billing
- `GET` `/api/billing/history`: user=app.api.billing, bot=app.api.billing
- `GET` `/api/billing/plans`: user=app.api.billing, bot=app.api.billing
- `GET` `/api/billing/subscription`: user=app.api.billing, bot=app.api.billing
- `POST` `/api/billing/subscription/manage`: user=app.api.billing, bot=app.api.billing
- `POST` `/api/billing/test-simulate-success`: user=app.api.billing, bot=app.api.billing
- `POST` `/api/billing/webhook`: user=app.api.billing, bot=app.api.billing
- `GET` `/api/brokers/accounts`: user=app.api.brokers, bot=app.api.brokers
- `GET` `/api/brokers/catalog`: user=app.api.brokers, bot=app.api.brokers
- `POST` `/api/brokers/connect`: user=app.api.brokers, bot=app.api.brokers
- `GET` `/api/brokers/{account_id}`: user=app.api.brokers, bot=app.api.brokers
- `POST` `/api/brokers/{account_id}/credentials`: user=app.api.brokers, bot=app.api.brokers
- `POST` `/api/brokers/{account_id}/disconnect`: user=app.api.brokers, bot=app.api.brokers
- `POST` `/api/brokers/{account_id}/validate`: user=app.api.brokers, bot=app.api.brokers
- `POST` `/api/onboarding/complete`: user=app.api.onboarding, bot=app.api.onboarding
- `GET` `/api/onboarding/next-steps`: user=app.api.onboarding, bot=app.api.onboarding
- `GET` `/api/onboarding/state`: user=app.api.onboarding, bot=app.api.onboarding
- `POST` `/api/onboarding/step`: user=app.api.onboarding, bot=app.api.onboarding
- `GET` `/api/onboarding/strategies`: user=app.api.onboarding, bot=app.api.onboarding
- `POST` `/api/risk-profiles/calculate`: user=app.api.risk_profiles, bot=app.api.risk_profiles
- `GET` `/api/risk-profiles/templates`: user=app.api.risk_profiles, bot=app.api.risk_profiles
- `POST` `/api/risk-profiles/validate`: user=app.api.risk_profiles, bot=app.api.risk_profiles
- `GET` `/api/strategies/`: user=app.api.strategies.marketplace, bot=app.api.strategies
- `GET` `/api/strategies/{strategy_id}`: user=app.api.strategies.marketplace, bot=app.api.strategies
- `POST` `/api/strategy-configs`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `GET` `/api/strategy-configs`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `GET` `/api/strategy-configs/{config_id}`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `PUT` `/api/strategy-configs/{config_id}`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `DELETE` `/api/strategy-configs/{config_id}`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `POST` `/api/strategy-configs/{config_id}/activate`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `POST` `/api/strategy-configs/{config_id}/deactivate`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `GET` `/api/strategy-configs/{config_id}/protection-status`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `POST` `/api/strategy-configs/{config_id}/reset-protection`: user=app.api.strategy_configs, bot=app.api.strategy_configs
- `GET` `/api/v1/bot-instances`: user=app.api.bot_instances_proxy, bot=app.api.strategy_marketplace
- `POST` `/api/v1/bot-instances/apply-official-strategy`: user=app.api.bot_instances_proxy, bot=app.api.strategy_marketplace
- `DELETE` `/api/v1/bot-instances/{instance_id}`: user=app.api.bot_instances_proxy, bot=app.api.strategy_marketplace
- `GET` `/api/v1/bot-instances/{instance_id}`: user=app.api.bot_instances_proxy, bot=app.api.strategy_marketplace
- `POST` `/api/v1/bot-instances/{instance_id}/pause`: user=app.api.bot_instances_proxy, bot=app.api.strategy_marketplace
- `POST` `/api/v1/bot-instances/{instance_id}/start`: user=app.api.bot_instances_proxy, bot=app.api.strategy_marketplace
- `POST` `/api/v1/bot-instances/{instance_id}/stop`: user=app.api.bot_instances_proxy, bot=app.api.strategy_marketplace
- `GET` `/api/v1/strategies/marketplace`: user=app.api.strategies_proxy, bot=app.api.strategy_marketplace
- `GET` `/api/v1/strategies/marketplace/{strategy_id}`: user=app.api.strategies_proxy, bot=app.api.strategy_marketplace
- `POST` `/auth/2fa/disable`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/2fa/setup`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/2fa/verify`: user=app.api.auth, bot=app.api.auth
- `GET` `/auth/admin/users`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/admin/users/{user_id}/suspend`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/admin/users/{user_id}/unsuspend`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/forgot-password`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/login`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/logout`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/logout-all`: user=app.api.auth, bot=app.api.auth
- `GET` `/auth/me`: user=app.api.auth, bot=app.api.auth
- `PATCH` `/auth/me`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/refresh`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/register`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/resend-verification`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/reset-password`: user=app.api.auth, bot=app.api.auth
- `GET` `/auth/sessions`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/sessions/revoke`: user=app.api.auth, bot=app.api.auth
- `DELETE` `/auth/sessions/{session_id}`: user=app.api.auth, bot=app.api.auth
- `GET` `/auth/user/brokers`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/user/brokers`: user=app.api.auth, bot=app.api.auth
- `POST` `/auth/verify-email`: user=app.api.auth, bot=app.api.auth
- `GET,HEAD` `/docs`: user=fastapi.applications, bot=fastapi.applications
- `GET,HEAD` `/docs/oauth2-redirect`: user=fastapi.applications, bot=fastapi.applications
- `GET` `/health`: user=app.main, bot=app.main
- `GET` `/kyc/checklist`: user=app.api.kyc, bot=app.api.kyc
- `GET` `/kyc/documents`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/documents/confirm`: user=app.api.kyc, bot=app.api.kyc
- `GET` `/kyc/documents/download/{file_path:path}`: user=app.api.kyc, bot=app.api.kyc
- `GET` `/kyc/documents/download/{file_ref:path}`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/documents/upload-url`: user=app.api.kyc, bot=app.api.kyc
- `PUT` `/kyc/documents/upload/{file_path:path}`: user=app.api.kyc, bot=app.api.kyc
- `PUT` `/kyc/documents/upload/{file_ref:path}`: user=app.api.kyc, bot=app.api.kyc
- `DELETE` `/kyc/documents/{doc_id}`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/face/complete`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/face/start`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/personal-info`: user=app.api.kyc, bot=app.api.kyc
- `GET` `/kyc/personal-info`: user=app.api.kyc, bot=app.api.kyc
- `GET` `/kyc/requirements`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/review`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/start`: user=app.api.kyc, bot=app.api.kyc
- `GET` `/kyc/status`: user=app.api.kyc, bot=app.api.kyc
- `POST` `/kyc/submit`: user=app.api.kyc, bot=app.api.kyc
- `GET,HEAD` `/openapi.json`: user=fastapi.applications, bot=fastapi.applications
- `GET` `/public/features`: user=app.api.public, bot=app.api.public
- `GET` `/public/home`: user=app.api.public, bot=app.api.public
- `GET` `/public/how-it-works`: user=app.api.public, bot=app.api.public
- `GET` `/public/pricing`: user=app.api.public, bot=app.api.public
- `POST` `/public/pricing/intent`: user=app.api.public, bot=app.api.public
- `POST` `/public/session`: user=app.api.public, bot=app.api.public
- `POST` `/public/track`: user=app.api.public, bot=app.api.public
- `GET,HEAD` `/redoc`: user=fastapi.applications, bot=fastapi.applications

### Frontend Calls Not Found in Backend

- `/plans`

