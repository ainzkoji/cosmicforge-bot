# Frontend API Calls Mapping

## Summary

- **User Frontend**: 14 unique endpoints called
- **Admin Frontend**: 12 unique endpoints called

## User Frontend Calls

| Method | Path | Found In | Status |
|--------|------|----------|--------|
| `GET` | `/api/admin/audit-logs` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/commissions/tiers` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/compliance/aml-flags` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/compliance/kyc-pending` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/dashboard/revenue-overview` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/dashboard/stats` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/revenue/overview` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/users` | admin.ts | [OK] user-backend |
| `GET` | `/api/onboarding/next-steps` | onboarding.ts | [OK] user-backend |
| `GET` | `/api/onboarding/state` | onboarding.ts | [OK] user-backend |
| `GET` | `/api/onboarding/strategies` | onboarding.ts | [OK] user-backend |
| `GET` | `/plans` | client.ts | [MISSING] NOT FOUND |
| `POST` | `/api/onboarding/complete` | onboarding.ts | [OK] user-backend |
| `POST` | `/api/onboarding/step` | onboarding.ts | [OK] user-backend |

## Admin Frontend Calls

| Method | Path | Found In | Status |
|--------|------|----------|--------|
| `GET` | `/api/admin/audit-logs` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/bot/live` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/bot/overview` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/bot/runs` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/commissions/tiers` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/compliance/aml-flags` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/compliance/kyc-pending` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/dashboard/revenue-overview` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/dashboard/stats` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/revenue/overview` | admin.ts | [OK] user-backend |
| `GET` | `/api/admin/users` | admin.ts | [OK] user-backend |
| `GET` | `/plans` | client.ts | [MISSING] NOT FOUND |
