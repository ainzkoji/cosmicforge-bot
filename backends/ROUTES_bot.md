# Bot Backend (Port 9000) API Routes Inventory

**Total Routes:** 181

## Route Summary

| Method | Path | Name | Tag | Module |
|--------|------|------|-----|--------|
| `GET` | `/` | root | no-tag | app.main |
| `GET` | `/admin/monitoring/activity/events` | get_activity_events | admin-monitoring | app.api.monitoring |
| `POST` | `/admin/monitoring/activity/log-event` | log_activity_event | admin-monitoring | app.api.monitoring |
| `POST` | `/admin/monitoring/bots/emergency-stop` | emergency_stop_all_bots | admin-monitoring | app.api.monitoring |
| `GET` | `/admin/monitoring/bots/executions` | get_bot_executions | admin-monitoring | app.api.monitoring |
| `GET` | `/admin/monitoring/bots/overview` | get_bots_overview | admin-monitoring | app.api.monitoring |
| `GET` | `/admin/monitoring/feature-flags` | get_feature_flags | admin-monitoring | app.api.monitoring |
| `PUT` | `/admin/monitoring/feature-flags/{flag_id}/toggle` | toggle_feature_flag | admin-monitoring | app.api.monitoring |
| `GET` | `/admin/monitoring/system/health` | get_system_health | admin-monitoring | app.api.monitoring |
| `GET` | `/admin/monitoring/system/metrics` | get_system_metrics | admin-monitoring | app.api.monitoring |
| `POST` | `/admin/monitoring/system/record-metric` | record_metric | admin-monitoring | app.api.monitoring |
| `GET` | `/admin/monitoring/transactions` | get_transactions | admin-monitoring | app.api.monitoring |
| `POST` | `/admin/monitoring/transactions/{transaction_id}/approve` | approve_transaction | admin-monitoring | app.api.monitoring |
| `GET` | `/api/admin/audit/logs` | get_audit_logs | Admin, admin | app.api.admin |
| `GET` | `/api/admin/commissions/tiers` | get_commission_tiers | Admin, admin | app.api.admin |
| `PUT` | `/api/admin/commissions/tiers/{tier_id}` | update_commission_tier | Admin, admin | app.api.admin |
| `GET` | `/api/admin/compliance/aml-flags` | get_aml_flags | Admin, admin | app.api.admin |
| `GET` | `/api/admin/compliance/kyc-pending` | get_pending_kyc | Admin, admin | app.api.admin |
| `POST` | `/api/admin/compliance/kyc/{submission_id}/approve` | approve_kyc_submission | Admin, admin | app.api.admin |
| `POST` | `/api/admin/compliance/kyc/{submission_id}/reject` | reject_kyc_submission | Admin, admin | app.api.admin |
| `GET` | `/api/admin/dashboard/revenue-overview` | get_revenue_overview | Admin, admin | app.api.admin |
| `GET` | `/api/admin/dashboard/stats` | get_dashboard_stats | Admin, admin | app.api.admin |
| `GET` | `/api/admin/revenue/overview` | get_revenue_overview | Admin, admin | app.api.admin |
| `POST` | `/api/admin/roles/grant` | grant_admin_role | Admin, admin | app.api.admin |
| `POST` | `/api/admin/roles/revoke` | revoke_admin_role | Admin, admin | app.api.admin |
| `GET` | `/api/admin/users` | list_users | Admin, admin | app.api.admin |
| `GET` | `/api/admin/users/{user_id}` | get_user_details | Admin, admin | app.api.admin |
| `POST` | `/api/admin/users/{user_id}/activate` | activate_user | Admin, admin | app.api.admin |
| `POST` | `/api/admin/users/{user_id}/suspend` | suspend_user | Admin, admin | app.api.admin |
| `GET` | `/api/analytics/calibration` | get_calibration | Analytics | app.api.analytics |
| `GET` | `/api/analytics/leaderboard` | get_max_leaderboard | Analytics | app.api.analytics |
| `GET` | `/api/analytics/overview` | get_overview | Analytics | app.api.analytics |
| `POST` | `/api/billing/checkout` | create_checkout | Billing | app.api.billing |
| `GET` | `/api/billing/history` | get_billing_history | Billing | app.api.billing |
| `GET` | `/api/billing/plans` | get_plans | Billing | app.api.billing |
| `GET` | `/api/billing/subscription` | get_subscription | Billing | app.api.billing |
| `POST` | `/api/billing/subscription/manage` | manage_subscription | Billing | app.api.billing |
| `POST` | `/api/billing/test-simulate-success` | simulate_success | Billing | app.api.billing |
| `POST` | `/api/billing/webhook` | billing_webhook | Billing | app.api.billing |
| `GET` | `/api/brokers/accounts` | get_accounts | Brokers, Brokers | app.api.brokers |
| `GET` | `/api/brokers/catalog` | get_catalog | Brokers, Brokers | app.api.brokers |
| `POST` | `/api/brokers/connect` | start_connection | Brokers, Brokers | app.api.brokers |
| `GET` | `/api/brokers/{account_id}` | get_account_detail | Brokers, Brokers | app.api.brokers |
| `POST` | `/api/brokers/{account_id}/credentials` | submit_credentials | Brokers, Brokers | app.api.brokers |
| `POST` | `/api/brokers/{account_id}/disconnect` | disconnect_broker | Brokers, Brokers | app.api.brokers |
| `POST` | `/api/brokers/{account_id}/validate` | validate_connection | Brokers, Brokers | app.api.brokers |
| `POST` | `/api/onboarding/complete` | complete_onboarding | Onboarding | app.api.onboarding |
| `GET` | `/api/onboarding/next-steps` | get_next_steps | Onboarding | app.api.onboarding |
| `GET` | `/api/onboarding/state` | get_state | Onboarding | app.api.onboarding |
| `POST` | `/api/onboarding/step` | save_step | Onboarding | app.api.onboarding |
| `GET` | `/api/onboarding/strategies` | get_strategies | Onboarding | app.api.onboarding |
| `POST` | `/api/risk-profiles/calculate` | calculate_position_size | Risk Profiles | app.api.risk_profiles |
| `GET` | `/api/risk-profiles/templates` | get_risk_profile_templates | Risk Profiles | app.api.risk_profiles |
| `POST` | `/api/risk-profiles/validate` | validate_risk_parameters | Risk Profiles | app.api.risk_profiles |
| `GET` | `/api/strategies/` | list_user_strategies | User Strategies, User Strategies | app.api.strategies |
| `POST` | `/api/strategies/` | create_strategy | User Strategies, User Strategies | app.api.strategies |
| `POST` | `/api/strategies/validate` | validate_strategy | User Strategies, User Strategies | app.api.strategies |
| `GET` | `/api/strategies/{strategy_id}` | get_strategy_details | User Strategies, User Strategies | app.api.strategies |
| `DELETE` | `/api/strategies/{strategy_id}` | delete_strategy | User Strategies, User Strategies | app.api.strategies |
| `POST` | `/api/strategy-configs` | create_configuration | Strategy Configs | app.api.strategy_configs |
| `GET` | `/api/strategy-configs` | list_configurations | Strategy Configs | app.api.strategy_configs |
| `GET` | `/api/strategy-configs/{config_id}` | get_configuration | Strategy Configs | app.api.strategy_configs |
| `PUT` | `/api/strategy-configs/{config_id}` | update_configuration | Strategy Configs | app.api.strategy_configs |
| `DELETE` | `/api/strategy-configs/{config_id}` | delete_configuration | Strategy Configs | app.api.strategy_configs |
| `POST` | `/api/strategy-configs/{config_id}/activate` | activate_configuration | Strategy Configs | app.api.strategy_configs |
| `POST` | `/api/strategy-configs/{config_id}/deactivate` | deactivate_configuration | Strategy Configs | app.api.strategy_configs |
| `GET` | `/api/strategy-configs/{config_id}/protection-status` | get_protection_status | Strategy Configs | app.api.strategy_configs |
| `POST` | `/api/strategy-configs/{config_id}/reset-protection` | reset_protection | Strategy Configs | app.api.strategy_configs |
| `GET` | `/api/v1/bot-instances` | get_user_bot_instances | Strategy Marketplace | app.api.strategy_marketplace |
| `POST` | `/api/v1/bot-instances/apply-official-strategy` | apply_official_strategy | Strategy Marketplace | app.api.strategy_marketplace |
| `DELETE` | `/api/v1/bot-instances/{instance_id}` | delete_bot_instance | Strategy Marketplace | app.api.strategy_marketplace |
| `GET` | `/api/v1/bot-instances/{instance_id}` | get_bot_instance | Strategy Marketplace | app.api.strategy_marketplace |
| `PUT` | `/api/v1/bot-instances/{instance_id}` | update_bot_instance | Strategy Marketplace | app.api.strategy_marketplace |
| `POST` | `/api/v1/bot-instances/{instance_id}/pause` | pause_bot_instance | Strategy Marketplace | app.api.strategy_marketplace |
| `POST` | `/api/v1/bot-instances/{instance_id}/start` | start_bot_instance | Strategy Marketplace | app.api.strategy_marketplace |
| `POST` | `/api/v1/bot-instances/{instance_id}/stop` | stop_bot_instance | Strategy Marketplace | app.api.strategy_marketplace |
| `GET` | `/api/v1/strategies/marketplace` | list_marketplace_strategies | Strategy Marketplace | app.api.strategy_marketplace |
| `GET` | `/api/v1/strategies/marketplace/{strategy_id}` | get_strategy_details | Strategy Marketplace | app.api.strategy_marketplace |
| `POST` | `/api/v1/strategies/marketplace/{strategy_id}/validate-params` | validate_strategy_params | Strategy Marketplace | app.api.strategy_marketplace |
| `POST` | `/auth/2fa/disable` | disable_2fa | Authentication | app.api.auth |
| `POST` | `/auth/2fa/setup` | setup_2fa | Authentication | app.api.auth |
| `POST` | `/auth/2fa/verify` | verify_2fa_setup | Authentication | app.api.auth |
| `GET` | `/auth/admin/users` | admin_list_users | Authentication | app.api.auth |
| `POST` | `/auth/admin/users/{user_id}/suspend` | admin_suspend_user | Authentication | app.api.auth |
| `POST` | `/auth/admin/users/{user_id}/unsuspend` | admin_unsuspend_user | Authentication | app.api.auth |
| `POST` | `/auth/forgot-password` | forgot_password | Authentication | app.api.auth |
| `POST` | `/auth/login` | login | Authentication | app.api.auth |
| `POST` | `/auth/logout` | logout | Authentication | app.api.auth |
| `POST` | `/auth/logout-all` | logout_all | Authentication | app.api.auth |
| `GET` | `/auth/me` | get_me | Authentication | app.api.auth |
| `PATCH` | `/auth/me` | update_me | Authentication | app.api.auth |
| `POST` | `/auth/refresh` | refresh | Authentication | app.api.auth |
| `POST` | `/auth/register` | register | Authentication | app.api.auth |
| `POST` | `/auth/resend-verification` | resend_verification | Authentication | app.api.auth |
| `POST` | `/auth/reset-password` | reset_password | Authentication | app.api.auth |
| `GET` | `/auth/sessions` | list_sessions | Authentication | app.api.auth |
| `GET` | `/auth/sessions` | get_sessions | Authentication | app.api.auth |
| `POST` | `/auth/sessions/revoke` | revoke_session | Authentication | app.api.auth |
| `DELETE` | `/auth/sessions/{session_id}` | revoke_session | Authentication | app.api.auth |
| `GET` | `/auth/user/brokers` | list_brokers | Authentication | app.api.auth |
| `POST` | `/auth/user/brokers` | link_broker | Authentication | app.api.auth |
| `POST` | `/auth/verify-email` | verify_email | Authentication | app.api.auth |
| `GET` | `/binance/balance` | binance_balance | no-tag | app.main |
| `POST` | `/binance/cancel-all` | binance_cancel_all | no-tag | app.main |
| `GET` | `/binance/klines` | binance_klines | no-tag | app.main |
| `POST` | `/binance/leverage` | binance_set_leverage | no-tag | app.main |
| `GET` | `/binance/open-orders` | binance_open_orders | no-tag | app.main |
| `GET` | `/binance/order` | binance_order | no-tag | app.main |
| `GET` | `/binance/ping` | binance_ping | no-tag | app.main |
| `GET` | `/binance/position` | binance_position | no-tag | app.main |
| `GET` | `/binance/price` | binance_price | no-tag | app.main |
| `GET` | `/binance/prices` | binance_prices | no-tag | app.main |
| `GET` | `/binance/qty` | binance_qty | no-tag | app.main |
| `GET` | `/config/symbols` | config_symbols | no-tag | app.main |
| `GET` | `/debug/check-symbol` | debug_check_symbol | no-tag | app.main |
| `GET` | `/debug/config` | debug_config | no-tag | app.main |
| `POST` | `/debug/crash-next-cycle` | debug_crash_next_cycle | no-tag | app.main |
| `GET` | `/debug/db/daily` | debug_db_daily | no-tag | app.main |
| `GET` | `/debug/db/symbols` | debug_db_symbols | no-tag | app.main |
| `GET` | `/debug/position_amt/{symbol}` | debug_position_amt | no-tag | app.main |
| `GET` | `/debug/run/current` | debug_run_current | no-tag | app.main |
| `POST` | `/debug/run/cycle` | debug_run_cycle | no-tag | app.main |
| `POST` | `/debug/run/force` | debug_run_force | no-tag | app.main |
| `GET` | `/debug/sanity` | debug_sanity | no-tag | app.main |
| `POST` | `/debug/set_last_stop` | debug_set_last_stop | no-tag | app.main |
| `GET` | `/debug/settings` | debug_settings | no-tag | app.main |
| `GET` | `/debug/strategy/check` | debug_strategy_check | no-tag | app.main |
| `GET, HEAD` | `/docs` | swagger_ui_html | no-tag | fastapi.applications |
| `GET, HEAD` | `/docs/oauth2-redirect` | swagger_ui_redirect | no-tag | fastapi.applications |
| `POST` | `/emergency/flatten` | emergency_flatten | no-tag | app.main |
| `GET` | `/health` | health | no-tag | app.main |
| `GET` | `/kyc/checklist` | get_kyc_checklist | KYC, KYC | app.api.kyc |
| `GET` | `/kyc/documents` | list_documents | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/documents/confirm` | confirm_document_upload | KYC, KYC | app.api.kyc |
| `GET` | `/kyc/documents/download/{file_path:path}` | download_document_file | KYC, KYC | app.api.kyc |
| `GET` | `/kyc/documents/download/{file_ref:path}` | download_document_file | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/documents/upload-url` | request_upload_url | KYC, KYC | app.api.kyc |
| `PUT` | `/kyc/documents/upload/{file_path:path}` | upload_document_file | KYC, KYC | app.api.kyc |
| `PUT` | `/kyc/documents/upload/{file_ref:path}` | upload_document_file | KYC, KYC | app.api.kyc |
| `DELETE` | `/kyc/documents/{doc_id}` | delete_document | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/face/complete` | complete_face_verification | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/face/start` | start_face_verification | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/personal-info` | submit_personal_info | KYC, KYC | app.api.kyc |
| `GET` | `/kyc/personal-info` | get_personal_info | KYC, KYC | app.api.kyc |
| `GET` | `/kyc/requirements` | get_kyc_requirements | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/review` | submit_review_decision | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/start` | start_kyc_case | KYC, KYC | app.api.kyc |
| `GET` | `/kyc/status` | get_kyc_status | KYC, KYC | app.api.kyc |
| `POST` | `/kyc/submit` | submit_kyc_for_review | KYC, KYC | app.api.kyc |
| `GET` | `/logs/events/tail` | logs_events_tail | no-tag | app.main |
| `GET, HEAD` | `/openapi.json` | openapi | no-tag | fastapi.applications |
| `GET` | `/public/features` | get_features_content | Public | app.api.public |
| `GET` | `/public/home` | get_home_content | Public | app.api.public |
| `GET` | `/public/how-it-works` | get_how_it_works_content | Public | app.api.public |
| `GET` | `/public/pricing` | get_pricing_data | Public | app.api.public |
| `POST` | `/public/pricing/intent` | create_pricing_intent | Public | app.api.public |
| `POST` | `/public/session` | create_marketing_session | Public | app.api.public |
| `POST` | `/public/track` | track_event | Public | app.api.public |
| `GET, HEAD` | `/redoc` | redoc_html | no-tag | fastapi.applications |
| `POST` | `/risk/circuit/reset` | risk_circuit_reset | no-tag | app.main |
| `GET` | `/risk/circuit/status` | risk_circuit_status | no-tag | app.main |
| `GET` | `/risk/daily` | risk_daily | no-tag | app.main |
| `POST` | `/risk/kill` | risk_kill | no-tag | app.main |
| `POST` | `/risk/reset` | risk_reset | no-tag | app.main |
| `POST` | `/risk/reset_kill` | reset_kill | no-tag | app.main |
| `GET` | `/risk/status` | risk_status | no-tag | app.main |
| `POST` | `/risk/unkill` | risk_unkill | no-tag | app.main |
| `GET` | `/runner/audit/tail` | audit_tail | no-tag | app.main |
| `POST` | `/runner/live/once` | runner_live_once | no-tag | app.main |
| `POST` | `/runner/live/start` | runner_live_start | no-tag | app.main |
| `POST` | `/runner/live/stop` | runner_live_stop | no-tag | app.main |
| `GET` | `/runner/paper/once` | paper_run_once | no-tag | app.main |
| `GET` | `/runner/paper/state` | paper_state | no-tag | app.main |
| `GET` | `/runner/status` | runner_status_endpoint | no-tag | app.main |
| `GET` | `/strategy/signal` | strategy_signal | no-tag | app.main |
| `GET` | `/symbols/universe` | symbols_universe | no-tag | app.main |
| `POST` | `/trade/close` | trade_close | no-tag | app.main |
| `POST` | `/trade/close-record` | trade_close_record | no-tag | app.main |
| `POST` | `/trade/close-record-usertrades` | trade_close_record_usertrades | no-tag | app.main |
| `POST` | `/trade/market` | trade_market | no-tag | app.main |
| `GET` | `/trade/protection` | trade_protection | no-tag | app.main |

## Routes by Prefix


###  `/` (1 routes)

- **GET** `/` — root

###  `/admin` (12 routes)

- **GET** `/admin/monitoring/activity/events` — get_activity_events
- **POST** `/admin/monitoring/activity/log-event` — log_activity_event
- **POST** `/admin/monitoring/bots/emergency-stop` — emergency_stop_all_bots
- **GET** `/admin/monitoring/bots/executions` — get_bot_executions
- **GET** `/admin/monitoring/bots/overview` — get_bots_overview
- **GET** `/admin/monitoring/feature-flags` — get_feature_flags
- **PUT** `/admin/monitoring/feature-flags/{flag_id}/toggle` — toggle_feature_flag
- **GET** `/admin/monitoring/system/health` — get_system_health
- **GET** `/admin/monitoring/system/metrics` — get_system_metrics
- **POST** `/admin/monitoring/system/record-metric` — record_metric
- **GET** `/admin/monitoring/transactions` — get_transactions
- **POST** `/admin/monitoring/transactions/{transaction_id}/approve` — approve_transaction

###  `/api/admin` (16 routes)

- **GET** `/api/admin/audit/logs` — get_audit_logs
- **GET** `/api/admin/commissions/tiers` — get_commission_tiers
- **PUT** `/api/admin/commissions/tiers/{tier_id}` — update_commission_tier
- **GET** `/api/admin/compliance/aml-flags` — get_aml_flags
- **GET** `/api/admin/compliance/kyc-pending` — get_pending_kyc
- **POST** `/api/admin/compliance/kyc/{submission_id}/approve` — approve_kyc_submission
- **POST** `/api/admin/compliance/kyc/{submission_id}/reject` — reject_kyc_submission
- **GET** `/api/admin/dashboard/revenue-overview` — get_revenue_overview
- **GET** `/api/admin/dashboard/stats` — get_dashboard_stats
- **GET** `/api/admin/revenue/overview` — get_revenue_overview
- **POST** `/api/admin/roles/grant` — grant_admin_role
- **POST** `/api/admin/roles/revoke` — revoke_admin_role
- **GET** `/api/admin/users` — list_users
- **GET** `/api/admin/users/{user_id}` — get_user_details
- **POST** `/api/admin/users/{user_id}/activate` — activate_user
- **POST** `/api/admin/users/{user_id}/suspend` — suspend_user

###  `/api/analytics` (3 routes)

- **GET** `/api/analytics/calibration` — get_calibration
- **GET** `/api/analytics/leaderboard` — get_max_leaderboard
- **GET** `/api/analytics/overview` — get_overview

###  `/api/billing` (7 routes)

- **POST** `/api/billing/checkout` — create_checkout
- **GET** `/api/billing/history` — get_billing_history
- **GET** `/api/billing/plans` — get_plans
- **GET** `/api/billing/subscription` — get_subscription
- **POST** `/api/billing/subscription/manage` — manage_subscription
- **POST** `/api/billing/test-simulate-success` — simulate_success
- **POST** `/api/billing/webhook` — billing_webhook

###  `/api/brokers` (7 routes)

- **GET** `/api/brokers/accounts` — get_accounts
- **GET** `/api/brokers/catalog` — get_catalog
- **POST** `/api/brokers/connect` — start_connection
- **GET** `/api/brokers/{account_id}` — get_account_detail
- **POST** `/api/brokers/{account_id}/credentials` — submit_credentials
- **POST** `/api/brokers/{account_id}/disconnect` — disconnect_broker
- **POST** `/api/brokers/{account_id}/validate` — validate_connection

###  `/api/onboarding` (5 routes)

- **POST** `/api/onboarding/complete` — complete_onboarding
- **GET** `/api/onboarding/next-steps` — get_next_steps
- **GET** `/api/onboarding/state` — get_state
- **POST** `/api/onboarding/step` — save_step
- **GET** `/api/onboarding/strategies` — get_strategies

###  `/api/risk-profiles` (3 routes)

- **POST** `/api/risk-profiles/calculate` — calculate_position_size
- **GET** `/api/risk-profiles/templates` — get_risk_profile_templates
- **POST** `/api/risk-profiles/validate` — validate_risk_parameters

###  `/api/strategies` (5 routes)

- **GET** `/api/strategies/` — list_user_strategies
- **POST** `/api/strategies/` — create_strategy
- **POST** `/api/strategies/validate` — validate_strategy
- **GET** `/api/strategies/{strategy_id}` — get_strategy_details
- **DELETE** `/api/strategies/{strategy_id}` — delete_strategy

###  `/api/strategy-configs` (9 routes)

- **POST** `/api/strategy-configs` — create_configuration
- **GET** `/api/strategy-configs` — list_configurations
- **GET** `/api/strategy-configs/{config_id}` — get_configuration
- **PUT** `/api/strategy-configs/{config_id}` — update_configuration
- **DELETE** `/api/strategy-configs/{config_id}` — delete_configuration
- **POST** `/api/strategy-configs/{config_id}/activate` — activate_configuration
- **POST** `/api/strategy-configs/{config_id}/deactivate` — deactivate_configuration
- **GET** `/api/strategy-configs/{config_id}/protection-status` — get_protection_status
- **POST** `/api/strategy-configs/{config_id}/reset-protection` — reset_protection

###  `/api/v1` (11 routes)

- **GET** `/api/v1/bot-instances` — get_user_bot_instances
- **POST** `/api/v1/bot-instances/apply-official-strategy` — apply_official_strategy
- **DELETE** `/api/v1/bot-instances/{instance_id}` — delete_bot_instance
- **GET** `/api/v1/bot-instances/{instance_id}` — get_bot_instance
- **PUT** `/api/v1/bot-instances/{instance_id}` — update_bot_instance
- **POST** `/api/v1/bot-instances/{instance_id}/pause` — pause_bot_instance
- **POST** `/api/v1/bot-instances/{instance_id}/start` — start_bot_instance
- **POST** `/api/v1/bot-instances/{instance_id}/stop` — stop_bot_instance
- **GET** `/api/v1/strategies/marketplace` — list_marketplace_strategies
- **GET** `/api/v1/strategies/marketplace/{strategy_id}` — get_strategy_details
- **POST** `/api/v1/strategies/marketplace/{strategy_id}/validate-params` — validate_strategy_params

###  `/auth` (23 routes)

- **POST** `/auth/2fa/disable` — disable_2fa
- **POST** `/auth/2fa/setup` — setup_2fa
- **POST** `/auth/2fa/verify` — verify_2fa_setup
- **GET** `/auth/admin/users` — admin_list_users
- **POST** `/auth/admin/users/{user_id}/suspend` — admin_suspend_user
- **POST** `/auth/admin/users/{user_id}/unsuspend` — admin_unsuspend_user
- **POST** `/auth/forgot-password` — forgot_password
- **POST** `/auth/login` — login
- **POST** `/auth/logout` — logout
- **POST** `/auth/logout-all` — logout_all
- **GET** `/auth/me` — get_me
- **PATCH** `/auth/me` — update_me
- **POST** `/auth/refresh` — refresh
- **POST** `/auth/register` — register
- **POST** `/auth/resend-verification` — resend_verification
- **POST** `/auth/reset-password` — reset_password
- **GET** `/auth/sessions` — list_sessions
- **GET** `/auth/sessions` — get_sessions
- **POST** `/auth/sessions/revoke` — revoke_session
- **DELETE** `/auth/sessions/{session_id}` — revoke_session
- **GET** `/auth/user/brokers` — list_brokers
- **POST** `/auth/user/brokers` — link_broker
- **POST** `/auth/verify-email` — verify_email

###  `/binance` (11 routes)

- **GET** `/binance/balance` — binance_balance
- **POST** `/binance/cancel-all` — binance_cancel_all
- **GET** `/binance/klines` — binance_klines
- **POST** `/binance/leverage` — binance_set_leverage
- **GET** `/binance/open-orders` — binance_open_orders
- **GET** `/binance/order` — binance_order
- **GET** `/binance/ping` — binance_ping
- **GET** `/binance/position` — binance_position
- **GET** `/binance/price` — binance_price
- **GET** `/binance/prices` — binance_prices
- **GET** `/binance/qty` — binance_qty

###  `/config` (1 routes)

- **GET** `/config/symbols` — config_symbols

###  `/debug` (13 routes)

- **GET** `/debug/check-symbol` — debug_check_symbol
- **GET** `/debug/config` — debug_config
- **POST** `/debug/crash-next-cycle` — debug_crash_next_cycle
- **GET** `/debug/db/daily` — debug_db_daily
- **GET** `/debug/db/symbols` — debug_db_symbols
- **GET** `/debug/position_amt/{symbol}` — debug_position_amt
- **GET** `/debug/run/current` — debug_run_current
- **POST** `/debug/run/cycle` — debug_run_cycle
- **POST** `/debug/run/force` — debug_run_force
- **GET** `/debug/sanity` — debug_sanity
- **POST** `/debug/set_last_stop` — debug_set_last_stop
- **GET** `/debug/settings` — debug_settings
- **GET** `/debug/strategy/check` — debug_strategy_check

###  `/docs` (2 routes)

- **GET, HEAD** `/docs` — swagger_ui_html
- **GET, HEAD** `/docs/oauth2-redirect` — swagger_ui_redirect

###  `/emergency` (1 routes)

- **POST** `/emergency/flatten` — emergency_flatten

###  `/health` (1 routes)

- **GET** `/health` — health

###  `/kyc` (18 routes)

- **GET** `/kyc/checklist` — get_kyc_checklist
- **GET** `/kyc/documents` — list_documents
- **POST** `/kyc/documents/confirm` — confirm_document_upload
- **GET** `/kyc/documents/download/{file_path:path}` — download_document_file
- **GET** `/kyc/documents/download/{file_ref:path}` — download_document_file
- **POST** `/kyc/documents/upload-url` — request_upload_url
- **PUT** `/kyc/documents/upload/{file_path:path}` — upload_document_file
- **PUT** `/kyc/documents/upload/{file_ref:path}` — upload_document_file
- **DELETE** `/kyc/documents/{doc_id}` — delete_document
- **POST** `/kyc/face/complete` — complete_face_verification
- **POST** `/kyc/face/start` — start_face_verification
- **POST** `/kyc/personal-info` — submit_personal_info
- **GET** `/kyc/personal-info` — get_personal_info
- **GET** `/kyc/requirements` — get_kyc_requirements
- **POST** `/kyc/review` — submit_review_decision
- **POST** `/kyc/start` — start_kyc_case
- **GET** `/kyc/status` — get_kyc_status
- **POST** `/kyc/submit` — submit_kyc_for_review

###  `/logs` (1 routes)

- **GET** `/logs/events/tail` — logs_events_tail

###  `/openapi.json` (1 routes)

- **GET, HEAD** `/openapi.json` — openapi

###  `/public` (7 routes)

- **GET** `/public/features` — get_features_content
- **GET** `/public/home` — get_home_content
- **GET** `/public/how-it-works` — get_how_it_works_content
- **GET** `/public/pricing` — get_pricing_data
- **POST** `/public/pricing/intent` — create_pricing_intent
- **POST** `/public/session` — create_marketing_session
- **POST** `/public/track` — track_event

###  `/redoc` (1 routes)

- **GET, HEAD** `/redoc` — redoc_html

###  `/risk` (8 routes)

- **POST** `/risk/circuit/reset` — risk_circuit_reset
- **GET** `/risk/circuit/status` — risk_circuit_status
- **GET** `/risk/daily` — risk_daily
- **POST** `/risk/kill` — risk_kill
- **POST** `/risk/reset` — risk_reset
- **POST** `/risk/reset_kill` — reset_kill
- **GET** `/risk/status` — risk_status
- **POST** `/risk/unkill` — risk_unkill

###  `/runner` (7 routes)

- **GET** `/runner/audit/tail` — audit_tail
- **POST** `/runner/live/once` — runner_live_once
- **POST** `/runner/live/start` — runner_live_start
- **POST** `/runner/live/stop` — runner_live_stop
- **GET** `/runner/paper/once` — paper_run_once
- **GET** `/runner/paper/state` — paper_state
- **GET** `/runner/status` — runner_status_endpoint

###  `/strategy` (1 routes)

- **GET** `/strategy/signal` — strategy_signal

###  `/symbols` (1 routes)

- **GET** `/symbols/universe` — symbols_universe

###  `/trade` (5 routes)

- **POST** `/trade/close` — trade_close
- **POST** `/trade/close-record` — trade_close_record
- **POST** `/trade/close-record-usertrades` — trade_close_record_usertrades
- **POST** `/trade/market` — trade_market
- **GET** `/trade/protection` — trade_protection
