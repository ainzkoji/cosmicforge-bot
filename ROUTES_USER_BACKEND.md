# User-Backend Active Routes Report

**Generated**: inspect_user_backend_routes.py
**Total Routes**: 127

---

## Summary

| Category | Count | Status |
|----------|-------|--------|
| Bot-owned (PROXY) | 31 | ✅ Correct |
| Bot-owned (LOCAL) | 0 | ⚠️ Should be proxy! |
| User-owned | 91 | ✅ Correct |
| Unknown | 5 | ❓ Needs review |

---

## ✅ BOT-OWNED APIs WITH LOCAL IMPLEMENTATIONS

**None found!** All bot-owned APIs are correctly proxied.

---

## ✅ BOT-OWNED APIs (Proxied - Correct)

These routes correctly proxy to bot-backend.

| Method | Path | Name | Tags | Module |
|--------|------|------|------|--------|
| GET | `/api/v1/analytics/calibration` | get_calibration | Analytics Proxy | `app.api.analytics_proxy.get_calibration` |
| GET | `/api/v1/analytics/leaderboard` | get_leaderboard | Analytics Proxy | `app.api.analytics_proxy.get_leaderboard` |
| GET | `/api/v1/analytics/overview` | get_overview | Analytics Proxy | `app.api.analytics_proxy.get_overview` |
| GET | `/api/v1/bot-instances` | get_bot_instances | Bot Instances | `app.api.bot_instances_proxy.get_bot_instances` |
| POST | `/api/v1/bot-instances/apply-official-strategy` | apply_official_strategy | Bot Instances | `app.api.bot_instances_proxy.apply_official_strategy` |
| DELETE | `/api/v1/bot-instances/{instance_id}` | delete_bot_instance | Bot Instances | `app.api.bot_instances_proxy.delete_bot_instance` |
| GET | `/api/v1/bot-instances/{instance_id}` | get_bot_instance | Bot Instances | `app.api.bot_instances_proxy.get_bot_instance` |
| POST | `/api/v1/bot-instances/{instance_id}/pause` | pause_bot_instance | Bot Instances | `app.api.bot_instances_proxy.pause_bot_instance` |
| POST | `/api/v1/bot-instances/{instance_id}/start` | start_bot_instance | Bot Instances | `app.api.bot_instances_proxy.start_bot_instance` |
| POST | `/api/v1/bot-instances/{instance_id}/stop` | stop_bot_instance | Bot Instances | `app.api.bot_instances_proxy.stop_bot_instance` |
| GET | `/api/v1/monitoring/activity-events` | get_activity_events | monitoring-proxy | `app.api.monitoring_proxy.get_activity_events` |
| GET | `/api/v1/monitoring/bots-overview` | get_bots_overview | monitoring-proxy | `app.api.monitoring_proxy.get_bots_overview` |
| GET | `/api/v1/monitoring/system-health` | get_system_health | monitoring-proxy | `app.api.monitoring_proxy.get_system_health` |
| GET | `/api/v1/monitoring/system-metrics` | get_system_metrics | monitoring-proxy | `app.api.monitoring_proxy.get_system_metrics` |
| POST | `/api/v1/risk-profiles/calculate` | calculate_position_size | Risk Profiles Proxy | `app.api.risk_profiles_proxy.calculate_position_size` |
| GET | `/api/v1/risk-profiles/templates` | get_risk_profile_templates | Risk Profiles Proxy | `app.api.risk_profiles_proxy.get_risk_profile_templates` |
| POST | `/api/v1/risk-profiles/validate` | validate_risk_parameters | Risk Profiles Proxy | `app.api.risk_profiles_proxy.validate_risk_parameters` |
| GET | `/api/v1/strategies/marketplace/` | list_user_strategies | Strategy Marketplace Proxy | `app.api.strategies_proxy.list_user_strategies` |
| POST | `/api/v1/strategies/marketplace/` | create_strategy | Strategy Marketplace Proxy | `app.api.strategies_proxy.create_strategy` |
| POST | `/api/v1/strategies/marketplace/validate` | validate_strategy | Strategy Marketplace Proxy | `app.api.strategies_proxy.validate_strategy` |
| GET | `/api/v1/strategies/marketplace/{strategy_id}` | get_strategy_details | Strategy Marketplace Proxy | `app.api.strategies_proxy.get_strategy_details` |
| DELETE | `/api/v1/strategies/marketplace/{strategy_id}` | delete_strategy | Strategy Marketplace Proxy | `app.api.strategies_proxy.delete_strategy` |
| POST | `/api/v1/strategy-configs` | create_configuration | Strategy Configs Proxy | `app.api.strategy_configs_proxy.create_configuration` |
| GET | `/api/v1/strategy-configs` | list_configurations | Strategy Configs Proxy | `app.api.strategy_configs_proxy.list_configurations` |
| GET | `/api/v1/strategy-configs/{config_id}` | get_configuration | Strategy Configs Proxy | `app.api.strategy_configs_proxy.get_configuration` |
| PUT | `/api/v1/strategy-configs/{config_id}` | update_configuration | Strategy Configs Proxy | `app.api.strategy_configs_proxy.update_configuration` |
| DELETE | `/api/v1/strategy-configs/{config_id}` | delete_configuration | Strategy Configs Proxy | `app.api.strategy_configs_proxy.delete_configuration` |
| POST | `/api/v1/strategy-configs/{config_id}/activate` | activate_configuration | Strategy Configs Proxy | `app.api.strategy_configs_proxy.activate_configuration` |
| POST | `/api/v1/strategy-configs/{config_id}/deactivate` | deactivate_configuration | Strategy Configs Proxy | `app.api.strategy_configs_proxy.deactivate_configuration` |
| GET | `/api/v1/strategy-configs/{config_id}/protection-status` | get_protection_status | Strategy Configs Proxy | `app.api.strategy_configs_proxy.get_protection_status` |
| POST | `/api/v1/strategy-configs/{config_id}/reset-protection` | reset_protection | Strategy Configs Proxy | `app.api.strategy_configs_proxy.reset_protection` |

---

## ✅ USER-OWNED APIs (Local - Correct)

These are correctly implemented in user-backend (91 routes).

<details>
<summary>Click to expand user-owned routes</summary>

| Method | Path | Name |
|--------|------|------|
| GET | `/api/admin/audit-logs` | get_audit_logs |
| GET | `/api/admin/bot/live` | get_bot_live_status |
| GET | `/api/admin/bot/overview` | get_bot_overview |
| GET | `/api/admin/bot/runs` | get_bot_runs |
| GET | `/api/admin/bot/runs/{run_id}` | get_bot_run_details |
| GET | `/api/admin/commissions/tiers` | get_commission_tiers |
| PUT | `/api/admin/commissions/tiers/{tier_id}` | update_commission_tier |
| GET | `/api/admin/compliance/aml-flags` | get_aml_flags |
| GET | `/api/admin/compliance/kyc-pending` | get_pending_kyc |
| POST | `/api/admin/compliance/kyc/{submission_id}/approve` | approve_kyc_submission |
| POST | `/api/admin/compliance/kyc/{submission_id}/reject` | reject_kyc_submission |
| GET | `/api/admin/dashboard/revenue-overview` | get_revenue_overview |
| GET | `/api/admin/dashboard/stats` | get_dashboard_stats |
| GET | `/api/admin/revenue/overview` | get_revenue_overview |
| POST | `/api/admin/roles/grant` | grant_admin_role |
| POST | `/api/admin/roles/revoke` | revoke_admin_role |
| GET | `/api/admin/users` | list_users |
| POST | `/api/admin/users/create` | create_operator |
| GET | `/api/admin/users/{user_id}` | get_user_details |
| POST | `/api/admin/users/{user_id}/activate` | activate_user |
| POST | `/api/admin/users/{user_id}/suspend` | suspend_user |
| POST | `/api/billing/checkout` | create_checkout |
| GET | `/api/billing/history` | get_billing_history |
| GET | `/api/billing/plans` | get_plans |
| GET | `/api/billing/subscription` | get_subscription |
| POST | `/api/billing/subscription/manage` | manage_subscription |
| POST | `/api/billing/test-simulate-success` | simulate_success |
| POST | `/api/billing/webhook` | billing_webhook |
| GET | `/api/brokers/accounts` | get_accounts |
| GET | `/api/brokers/catalog` | get_catalog |
| POST | `/api/brokers/connect` | start_connection |
| GET | `/api/brokers/{account_id}` | get_account_detail |
| DELETE | `/api/brokers/{account_id}` | delete_broker |
| POST | `/api/brokers/{account_id}/credentials` | submit_credentials |
| POST | `/api/brokers/{account_id}/disconnect` | disconnect_broker |
| POST | `/api/brokers/{account_id}/validate` | validate_connection |
| POST | `/api/onboarding/complete` | complete_onboarding |
| GET | `/api/onboarding/next-steps` | get_next_steps |
| GET | `/api/onboarding/state` | get_state |
| POST | `/api/onboarding/step` | save_step |
| GET | `/api/onboarding/strategies` | get_strategies |
| GET | `/api/portfolio/summary` | get_portfolio |
| GET | `/api/portfolio/transactions` | get_transactions |
| POST | `/auth/2fa/disable` | disable_2fa |
| POST | `/auth/2fa/setup` | setup_2fa |
| POST | `/auth/2fa/verify` | verify_2fa_setup |
| GET | `/auth/admin/users` | admin_list_users |
| POST | `/auth/admin/users/{user_id}/suspend` | admin_suspend_user |
| POST | `/auth/admin/users/{user_id}/unsuspend` | admin_unsuspend_user |
| POST | `/auth/forgot-password` | forgot_password |
| POST | `/auth/login` | login |
| POST | `/auth/logout` | logout |
| POST | `/auth/logout-all` | logout_all |
| GET | `/auth/me` | get_me |
| PATCH | `/auth/me` | update_me |
| POST | `/auth/refresh` | refresh |
| POST | `/auth/register` | register |
| POST | `/auth/resend-verification` | resend_verification |
| POST | `/auth/reset-password` | reset_password |
| GET | `/auth/sessions` | list_sessions |
| GET | `/auth/sessions` | get_sessions |
| POST | `/auth/sessions/revoke` | revoke_session |
| DELETE | `/auth/sessions/{session_id}` | revoke_session |
| GET | `/auth/user/brokers` | list_brokers |
| POST | `/auth/user/brokers` | link_broker |
| POST | `/auth/verify-email` | verify_email |
| GET | `/kyc/checklist` | get_kyc_checklist |
| GET | `/kyc/documents` | list_documents |
| POST | `/kyc/documents/confirm` | confirm_document_upload |
| GET | `/kyc/documents/download/{file_path:path}` | download_document_file |
| GET | `/kyc/documents/download/{file_ref:path}` | download_document_file |
| POST | `/kyc/documents/upload-url` | request_upload_url |
| PUT | `/kyc/documents/upload/{file_path:path}` | upload_document_file |
| PUT | `/kyc/documents/upload/{file_ref:path}` | upload_document_file |
| DELETE | `/kyc/documents/{doc_id}` | delete_document |
| POST | `/kyc/face/complete` | complete_face_verification |
| POST | `/kyc/face/start` | start_face_verification |
| POST | `/kyc/personal-info` | submit_personal_info |
| GET | `/kyc/personal-info` | get_personal_info |
| GET | `/kyc/requirements` | get_kyc_requirements |
| POST | `/kyc/review` | submit_review_decision |
| POST | `/kyc/start` | start_kyc_case |
| GET | `/kyc/status` | get_kyc_status |
| POST | `/kyc/submit` | submit_kyc_for_review |
| GET | `/public/features` | get_features_content |
| GET | `/public/home` | get_home_content |
| GET | `/public/how-it-works` | get_how_it_works_content |
| GET | `/public/pricing` | get_pricing_data |
| POST | `/public/pricing/intent` | create_pricing_intent |
| POST | `/public/session` | create_marketing_session |
| POST | `/public/track` | track_event |

</details>

---

## Action Items

✅ **No action needed!** All bot-owned APIs are correctly proxied.

---
