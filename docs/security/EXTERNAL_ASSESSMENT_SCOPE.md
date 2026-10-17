# External security assessment: scope and rules of engagement

Status: **not yet commissioned.** An independent penetration test is a launch
prerequisite (see [SECURITY_HARDENING_REPORT.md](SECURITY_HARDENING_REPORT.md)).
This document is the brief for the outside tester. It must be signed off by the
platform owner before any testing starts.

## 1. Objectives

1. Find vulnerabilities an internet attacker, a registered customer, or a
   compromised admin browser could exploit.
2. Verify the Section H 8.4 controls hold under adversarial testing: cookie
   sessions + CSRF, admin TOTP MFA, refresh rotation / reuse detection, tenant
   isolation of broker accounts and bots, credential encryption at rest.
3. Deliver a report with reproducible findings, CVSS v3.1 / v4 scores and fixes.

## 2. In scope

| Component | What | Notes |
|---|---|---|
| Customer web app | `https://app.<staging-domain>` (React, `frontends/user-frontend`) | Same-origin nginx layout: `/api/*`, `/kyc/*`, `/public/*` proxied to the user-backend |
| Admin console | `https://admin.<staging-domain>` (React, `frontends/admin-frontend`) | IP allow-list may be enabled; the tester's egress IPs will be added |
| User-backend API | FastAPI, port 8000 behind nginx | Auth (`/api/v1/auth/*`, `/api/v1/admin-auth/*`), brokers, billing (checkout/portal), KYC upload/review, notifications, signals, proxies to the engine |
| Admin-backend API | FastAPI, port 8100, published as `/admin-api/api/admin/*` on the admin host | Read-only reporting |
| Stripe webhook | `POST /api/billing/webhook` | Signature and replay handling (Stripe **test mode** only) |
| nginx configuration | `deploy/nginx/cosmicforge-app.conf` as deployed on staging | TLS, headers, rate limits, path exposure |
| Session/auth design | Cookies `cf_*` / `cf_admin_*`, `X-CSRF-Token`, MFA step tokens | Includes token forgery, fixation, CSRF, cross-realm confusion |
| Source code (white-box, optional) | Read-only access to this repository at the tested commit | Recommended: speeds up auth and tenant-isolation review |

Focus areas (please cover explicitly):

* Authentication: brute force / credential stuffing limits, user 2FA, admin MFA bypass
  (step-token reuse, enrolment-token abuse, recovery-code guessing, race conditions on
  single-use codes), password reset (code guessing, enumeration, takeover).
* Session management: cookie flags, CSRF on every state-changing route, refresh reuse,
  logout/revocation, cross-tab behaviour, bearer-compat window (`AUTH_BEARER_COMPAT=true`).
* Authorization: horizontal (user A vs user B: broker accounts, bot instances, KYC
  documents, notifications, billing, reports) and vertical (customer -> admin routes,
  admin step tokens -> admin routes).
* Injection and SSRF: SQL (SQLite), outbound URL handling (`url_guard`, broker gateway
  / bridge addresses), file upload (KYC documents: type/size checks, storage paths).
* Secrets handling: exchange API keys never returned, logs, error messages.
* Business logic: subscription entitlements, plan changes, webhook idempotency.

## 3. Out of scope

* The trading engine (bot-backend, port 9000) **directly**: it listens on 127.0.0.1 only.
  It is in scope only as reached through the user-backend proxies.
* Live exchange accounts and live order submission. **No real-money orders, transfers or
  withdrawals.** Exchange integrations may be exercised only with demo/testnet keys we provide.
* Stripe itself, Firebase/Google, the e-mail provider, the hosting provider and its network.
* Denial-of-service and volumetric load tests (beyond demonstrating that a rate limit
  exists or is missing), and anything that degrades the shared staging host for others.
* Social engineering, phishing, and physical attacks against staff.
* Production (`app.<production-domain>`, production database, production keys).

## 4. Environments

| Environment | Purpose | Data |
|---|---|---|
| Staging (required) | A copy of the production layout: same nginx config, `APP_ENV=PRODUCTION` profile, `ADMIN_MFA_REQUIRED` on, `AUTH_BEARER_COMPAT` as planned for launch | Synthetic only; a fresh database seeded for the test; no real customer data, no real KYC documents |
| Local (optional) | White-box verification of a finding | Tester's own checkout of the tested commit |

The staging host is rebuilt from scratch before the test and wiped afterwards.
Secrets on staging (`SECRET_KEY`, `CREDENTIAL_KEY`, `BROKER_SECRET_KEY`, `KYC_*`,
Stripe **test** keys) are unique to staging and rotated after the test.

## 5. Accounts and material provided

* 3 customer accounts (A, B, C) on different plans (free, pro, whale), each with a demo/testnet
  broker account connected; account C has user 2FA enabled (TOTP seed shared out of band).
* 2 admin accounts: one with MFA enrolled (seed + recovery codes shared out of band), one
  not yet enrolled (to test the enrolment-only path).
* Demo/testnet exchange keys (trade-only, no withdrawal permission) for the broker flows.
* Stripe test-mode access to trigger webhooks (or a signing secret for crafted events).
* The tested commit hash, this document, the hardening report and the API docs (staging may
  enable `API_DOCS_ENABLED=true` for the test).

Credentials are delivered through a password manager share, never by e-mail or chat, and
are revoked when the engagement ends.

## 6. Rules of engagement

1. Written authorization (scope, dates, tester names, source IPs) signed by the platform
   owner before the first request. Testing outside the window or from other IPs is not
   authorized.
2. Testing window: agreed dates, business hours `<TZ>` unless agreed otherwise; a point of
   contact is reachable throughout (name/phone in the authorization letter).
3. Stop and report within 4 hours on: access to real customer data, a working
   authentication bypass or remote code execution, any effect on a live exchange account,
   or anything that makes staging unstable. Critical findings are reported immediately,
   not only in the final report.
4. Data handling: no exfiltration beyond the minimum needed to prove a finding; no copies
   of secrets or personal data kept after the engagement; evidence is encrypted at rest and
   deleted after report acceptance.
5. No persistence mechanisms, no destructive actions (dropping data, deleting other testers'
   accounts), no pivoting into hosts outside scope.
6. Automated scanners are allowed against staging at a rate that stays under the published
   nginx limits unless the test case is the limit itself.
7. Retest of fixed findings is included (one round, within 30 days).

## 7. Deliverables

* Executive summary and risk rating.
* Findings with: affected component/route, reproduction steps, evidence, impact, CVSS,
  remediation; mapped to OWASP ASVS 4.0.3 level 2 where possible.
* Explicit statement for each focus area in §2 (tested / not tested / why).
* Retest letter after fixes.

## 8. Launch gate

Public launch requires: the external report delivered; no open Critical or High finding
(or each one accepted in writing by the platform owner with a dated mitigation plan); the
open items F1-F9 of the internal report reviewed with the tester.
