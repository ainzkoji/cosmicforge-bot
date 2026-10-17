# Security hardening for public launch (Section H, Step 8.4)

Date: 2026-10-17. Scope: user-backend (port 8000, users and admins), admin-backend
(8100), bot-backend (9000, internal), user and admin front ends.

> **External penetration testing: BLOCKED / NOT PERFORMED.** Everything below is an
> internal code review with automated regression tests. It is not a substitute for
> an independent assessment. An external penetration test against a staging copy of
> the production layout is **required before public launch**. Its scope, accounts
> and rules of engagement are in [EXTERNAL_ASSESSMENT_SCOPE.md](EXTERNAL_ASSESSMENT_SCOPE.md).

Status key: **PASS** reviewed, no defect found (test added or already present) ·
**FIXED** defect found and fixed in this step, with a regression test ·
**FINDING** open, not fixed here (owner/next action given) ·
**EXTERNAL** owned by another work stream or needs an outside party.

## 1. Audited areas

| # | Area | Status | Evidence |
|---|------|--------|----------|
| 1 | Admin MFA (TOTP) | FIXED (new) | Admin accounts had password-only sign-in. Now: enrolment, two-step login, replay protection, hashed one-time recovery codes, `ADMIN_MFA_REQUIRED`, audit rows. §2.1 |
| 2 | Session tokens in browser storage | FIXED | Both apps kept access and refresh tokens in `localStorage` (readable by any XSS). Now httpOnly cookies + CSRF double submit; legacy keys are deleted on load. §2.2 |
| 3 | CSRF | FIXED (new) | No CSRF protection existed. Cookie-authenticated writes need `X-CSRF-Token` = `cf_csrf` / `cf_admin_csrf` cookie. §2.2 |
| 4 | CORS with credentials | PASS | `browser_origins()` drops `*`, wildcards and non-origin entries; the main app passes only that explicit list. `test_cors_origins_are_explicit_and_never_a_wildcard`, `test_cookie_middleware_is_installed_inside_cors_in_the_main_app` |
| 5 | Credential key derivation (`CREDENTIAL_KEY`) | FIXED | Key was the secret truncated / zero-padded to 32 bytes. Now HKDF-SHA256, versioned ciphertext (`v2:`), legacy values decrypt, re-encryption helper + CLI. §2.3 |
| 6 | Access/refresh token expiry and type | FIXED (bot-backend) / PASS (user-backend) | bot-backend `app/api/deps.get_current_active_user` accepted a 14-day **refresh** token as an access token (same issuer/audience, no `type` check). Fixed; `test_section_h84_token_type.py`. User-backend already checked type: `test_a_refresh_token_is_not_an_access_token`, `test_expired_access_tokens_are_rejected`, `test_admin_and_user_tokens_are_not_interchangeable` |
| 7 | Refresh rotation and reuse detection (user) | PASS (+audit row) | Rotation and revoke-all on reuse already existed (`test_refresh_token_reuse_revokes_every_session_durably`). The `refresh_reuse_detected` audit row named in the docstring was never written: now written. Cookie path: `test_refresh_through_the_cookie_rotates_and_needs_csrf` |
| 8 | Refresh reuse detection (admin) | FIXED | A replayed rotated admin refresh token only got a 401; other sessions survived. Now every session of the admin is revoked and `admin_refresh_reuse_detected` is audited. `test_admin_refresh_reuse_revokes_every_admin_session` |
| 9 | Logout | FIXED | The customer app's "log out" only cleared localStorage (the server session stayed valid for 14 days); the admin console's Logout button did nothing. Both now call `/logout`, which revokes the session and clears the cookies; `/logout` works with the refresh token alone (expired access token). `test_logout_through_the_cookie_revokes_the_session_and_clears_the_cookies`, `test_logout_still_works_with_a_body_token_and_no_access_token`, admin `admin_mfa.test.ts` |
| 10 | Password reset single use + expiry | PASS | `test_reset_code_is_single_use_and_revokes_every_session`, `test_reset_code_expires`, plus the existing attempt-cap tests in `test_auth_hardening.py` |
| 11 | Account takeover: sessions after password change | PASS | Reset revokes every refresh session (test above). There is no separate change-password endpoint. See FINDING F1 for the 15-minute access-token window |
| 12 | User 2FA (TOTP) | PASS | Encrypted secret, replay counter, rate limits (existing tests). Secrets are now written in format `enc:v2:` |
| 13 | Tenant isolation: broker accounts | FIXED | `POST /api/v1/brokers/{id}/disconnect` deleted the legacy credentials and stopped **every bot** of ANY account id before checking ownership, and answered 200. `GET /{id}/summary` answered 200 "does not belong to user ..." for another user's account (existence oracle). Both fixed: `test_disconnecting_someone_elses_account_touches_nothing`, `test_a_user_cannot_read_or_change_another_users_broker_account` |
| 14 | Exchange credentials in API responses | PASS | List and detail responses carry `masked_key` only; no key, secret, passphrase or `encrypted_blob`. `test_broker_responses_never_contain_exchange_credentials` |
| 15 | Stripe webhook signature + replay | PASS / EXTERNAL (read only; billing work stream owns the file) | `shared_lib/billing/webhooks.verify_stripe_signature`: HMAC-SHA256 over the raw body, constant-time compare of every `v1`, 300 s timestamp tolerance, fails closed without `STRIPE_WEBHOOK_SECRET`; events are idempotent by provider event id (`billing_events` unique index), so a replay inside the window is a no-op |
| 16 | Rate limiting | PASS | App level (durable, per account / per IP / per pair): login, admin login, 2FA and admin MFA codes, e-mailed codes. Edge: `deploy/nginx/cosmicforge-app.conf` `limit_req` zones; the admin MFA endpoints were added to the strict auth zone in this step |
| 17 | Security headers | PASS | App middleware (HSTS, nosniff, `X-Frame-Options: DENY`, CSP, Referrer-Policy) and the authoritative nginx headers (strict CSP, `frame-ancestors 'none'`). Login/refresh answers now also send `Cache-Control: no-store` |
| 18 | Log redaction / plaintext in logs | PASS | `shared_lib/core/security/redaction.py` installed at start-up; new code never logs tokens, secrets or codes. `test_no_plaintext_reaches_the_logs` |
| 19 | SSE token in URL | FIXED | `useAnalyticsStream` put the access token in `?token=` (lands in proxy/access logs). Now `EventSource(..., {withCredentials: true})` with the cookie; the URL token is still read for legacy clients only during the compat window. `test_sse_stream_refuses_url_tokens_after_the_migration_window` |
| 20 | Python dependencies | FINDING F6 | `pip_audit` is not installed in `backends/venv` (not run). Manual review below |
| 21 | npm dependencies (`npm audit --omit=dev`) | FINDING F7 | user-frontend: 12 (1 critical, 10 high, 1 moderate); admin-frontend: 9 (8 high, 1 moderate). Not auto-fixed |
| 22 | External penetration test | EXTERNAL / BLOCKED | Not performed. Required before launch. |

## 2. Fixes, with their tests

### 2.1 Admin MFA (TOTP)

Code: `backends/user-backend/app/api/admin_auth.py`, `app/core/security.py`
(step tokens, recovery codes), `app/core/deps.py` (policy gate),
`shared_lib/persistence/admin_mfa_schema.py` (additive schema, called from
`migrations.migrate` step 13b and lazily by every MFA endpoint).

* Schema (additive): `admins.mfa_enabled | mfa_secret | mfa_last_counter | mfa_enrolled_at`,
  `admin_sessions.mfa_verified`, table `admin_mfa_recovery_codes` (salted SHA-256 hashes).
* `POST /api/v1/admin-auth/mfa/setup` - new secret (stored encrypted, `enc:v2:`), returns
  `secret` + `provisioning_uri`. Pending until confirmed; refused when MFA is already on.
* `POST /mfa/enroll {code}` - enables MFA, returns 10 recovery codes once, **revokes every
  earlier admin session** and opens a fresh MFA-verified one. Audit `admin_mfa_enrolled`.
* `POST /login` - enrolled admin: `{mfa_required, mfa_token}` (5 min). MFA required but
  not enrolled: `{mfa_enrollment_required, enrollment_token}` (15 min) that can do nothing
  except `/mfa/setup` + `/mfa/enroll`. Step tokens use their own audience (`admin-mfa`), so
  every admin route and the admin-backend reject them, and they die when the password changes.
* `POST /mfa/verify {mfa_token, code | recovery_code}` - TOTP codes are single use (last
  accepted time step stored, guarded UPDATE); recovery codes single use; guesses are rate
  limited (5 per 15 min). Audit `admin_mfa_recovery_code_used`, `admin_login_failed_mfa`.
* `POST /mfa/disable {code}` - needs a **current TOTP code** (a recovery code is not
  accepted) and is refused (409) while MFA is required. Audit `admin_mfa_disabled`.
* `POST /mfa/recovery-codes {code}` - regenerate (old codes stop working). Audit.
* `ADMIN_MFA_REQUIRED` - unset: true under the production profile, false in TEST/DEVELOPMENT.
  While required, `get_current_admin` refuses an admin without MFA (`403 MFA_ENROLLMENT_REQUIRED`)
  and a refresh of a password-only session is refused (`401 MFA_REQUIRED`), so sessions
  opened before the policy cannot outlive it.
* Break glass: `python scripts/security_maintenance.py reset-admin-mfa <email>` (shell access
  only) clears MFA, revokes sessions, audits `admin_mfa_reset_by_operator`.
* Console: `pages/Login.tsx` - password -> code / recovery code, or enrolment with QR code
  and a one-time recovery-code screen. Step tokens live in component state only.

Tests (`backends/user-backend/tests/test_section_h84_security.py`):
`test_setup_returns_a_provisioning_uri_and_stores_the_secret_encrypted`,
`test_enrolment_needs_a_valid_code_returns_hashed_recovery_codes_and_ends_old_sessions`,
`test_enrolled_admin_login_is_two_step_and_the_step_token_grants_nothing`,
`test_a_totp_code_cannot_be_replayed`, `test_mfa_code_guessing_is_rate_limited`,
`test_recovery_codes_work_once_and_their_use_is_audited`,
`test_challenge_token_dies_when_the_password_changes`,
`test_required_mfa_gives_an_unenrolled_admin_only_an_enrolment_token`,
`test_required_mfa_refuses_password_only_sessions_opened_before_the_policy`,
`test_admin_mfa_required_defaults_to_the_production_profile`,
`test_disabling_mfa_needs_a_current_totp_code_and_is_audited`,
`test_disabling_mfa_is_refused_while_it_is_required`, `test_setup_cannot_replace_an_enrolled_factor`,
`test_recovery_codes_can_be_regenerated_with_a_totp_code`,
`test_enrolment_with_the_step_token_works_despite_a_stale_admin_cookie`,
`test_break_glass_reset_clears_mfa_revokes_sessions_and_is_audited`,
`test_admin_mfa_schema_is_additive_and_idempotent`; front end `src/tests/admin_mfa.test.ts`.

### 2.2 Cookie sessions + CSRF

Code: `shared_lib/core/security/session_cookies.py` (realms, cookie helpers, ASGI
middleware), `user-backend/app/core/session_cookies.py` (settings binding), `auth.py`
(login/refresh/logout/csrf), `admin_auth.py`, `main.py` (middleware inside CORS),
`admin-backend/app/main.py` + `core/cors.py` (accepts the admin cookie, allows the header).

| Cookie | Realm | HttpOnly | Path | Lifetime |
|---|---|---|---|---|
| `cf_access` / `cf_admin_access` | user / admin | yes | `/` | access TTL (15 min) |
| `cf_refresh` / `cf_admin_refresh` | user / admin | yes | `/api/v1/auth` / `/api/v1/admin-auth` | refresh TTL (14 d) |
| `cf_csrf` / `cf_admin_csrf` | user / admin | **no** (double submit) | `/` | refresh TTL |

All: `SameSite=Strict` (`lax` configurable, `none` never), `Secure` (always in production;
`AUTH_COOKIE_SECURE` only matters outside production), host-only unless `AUTH_COOKIE_DOMAIN`.

* The middleware turns the realm's access cookie into `Authorization: Bearer` so every
  existing dependency and proxy works unchanged; realm = admin for `/api/admin*` and
  `/api/v1/admin-auth*`, user otherwise. Unsafe methods need `X-CSRF-Token` equal to the CSRF
  cookie, else `403 CSRF_FAILED` (CORS stays the outer layer so the browser can read it).
  Unauthenticated endpoints (login, register, reset, `mfa/verify|setup|enroll`) just ignore a
  stale cookie. Placeholder headers (`Bearer null`) are dropped.
* `/refresh` and `/logout` take the refresh token from the body (legacy) or the refresh
  cookie + CSRF header. `GET /api/v1/auth/csrf` and `/api/v1/admin-auth/csrf` return the CSRF
  token of the current cookie session for a console served from another host.
* Front ends: `credentials: "include"` / `withCredentials`, CSRF header on writes, no token
  in storage; `access_token`, `refresh_token`, `admin_access_token`, `admin_refresh_token` are
  removed from localStorage on load; cross-tab refreshes are serialised (Web Locks) and a
  refresh made by another tab is reused instead of replaying the rotated cookie.

Tests: `test_login_sets_httponly_samesite_strict_cookies_and_a_readable_csrf_cookie`,
`test_cookie_session_authenticates_safe_requests`,
`test_cookie_session_needs_the_csrf_header_on_state_changing_requests`,
`test_placeholder_bearer_headers_fall_back_to_the_cookie`,
`test_refresh_through_the_cookie_rotates_and_needs_csrf`,
`test_a_stale_cookie_does_not_block_signing_in_again`,
`test_bearer_headers_still_work_during_the_migration_window`,
`test_without_bearer_compat_tokens_leave_the_body_and_bearer_headers_are_ignored`,
`test_admin_cookies_are_separate_from_user_cookies`,
`test_secure_flag_is_forced_in_production_and_configurable_elsewhere`,
`test_samesite_none_is_never_used`; front end `src/tests/sessionAuth.test.ts` (user) and
`src/tests/admin_mfa.test.ts` (admin).

### 2.3 Credential encryption

`app/core/security.py`: key = `HKDF-SHA256(CREDENTIAL_KEY, salt="cosmicforge/credential-encryption/v2")`
-> Fernet. Ciphertext `v2:<fernet>`; `v1:<fernet>` and unprefixed Fernet tokens are decrypted with
the legacy derivation (decrypt only). TOTP secrets: `enc:v2:...` (new), `enc:v1:...` and legacy
plaintext still read. Helpers: `reencrypt_credential`, `credential_needs_reencryption`,
`reencrypt_totp_secret`, `reencrypt_stored_totp_secrets(conn)`; CLI
`python scripts/security_maintenance.py reencrypt-totp` (prints counts only). Broker API keys are
unaffected (they already use `BROKER_SECRET_KEY` / Fernet in `broker_security.py`).

Tests: `test_new_ciphertexts_are_versioned_and_use_the_hkdf_key`, `test_legacy_ciphertexts_still_decrypt`,
`test_reencryption_helpers_rewrite_legacy_values_only`, `test_any_length_credential_key_works`,
`test_totp_sweep_reencrypts_users_and_admins_and_codes_still_verify`, `test_no_plaintext_reaches_the_logs`;
`test_auth_hardening.py::test_2fa_secret_is_stored_encrypted_and_login_requires_the_code` now expects `enc:v2:`.

### 2.4 Other fixes

* bot-backend `app/api/deps.py`: only `type == "access"` authenticates (`tests/test_section_h84_token_type.py`).
* `broker_service.disconnect_broker_account`: ownership checked first; `brokers.get_summary`: someone
  else's account is a 404 like a missing one.
* Admin refresh reuse detection; user `refresh_reuse_detected` audit row; logout revokes server side;
  admin console Logout wired.

## 3. Session-architecture migration plan

1. **Now (this release, `AUTH_BEARER_COMPAT=true`)** - backends issue cookies *and* still return the
   token strings and accept bearer headers. Both front ends use cookies only and purge stored tokens,
   so every browser user signs in once more after the deploy. Nothing else changes for API clients.
2. **Observe (1-2 release cycles)** - confirm no client still depends on bearer tokens: add
   `$http_authorization`-presence (never the value) to a temporary nginx log format, or grep the
   user-backend access logs for `?token=` on `/api/v1/events/stream`. Known non-browser callers: none
   in this repository (the MT connector uses its own one-time link tokens; service-to-service calls
   mint their own short-lived tokens and never call the user-backend with a browser token).
3. **Cut over (`AUTH_BEARER_COMPAT=false`)** - login/refresh bodies no longer carry tokens, incoming
   `Authorization` headers on the user-backend and admin-backend are ignored, `?token=` is ignored.
   Roll back by setting it to true again (no data migration involved).
4. **Clean up** - remove the compat branches (`token_body`, body `refresh_token`, `?token=` in
   `events_proxy`), and the `access_token`/`refresh_token` fields of `Token`/`AdminAuthResponse`.
5. **Admin MFA roll-out** - production defaults to `ADMIN_MFA_REQUIRED=true`: at the first sign-in
   after deploy each admin is sent through enrolment (password -> QR -> code -> recovery codes).
   Existing admin sessions stop working at once. Then run `security_maintenance.py reencrypt-totp`.

Deployment notes: the same-origin nginx layout (`deploy/nginx/cosmicforge-app.conf`) is assumed - the
page and its API share a host, so host-only `SameSite=Strict` cookies work and the CSRF cookie is
readable. If the API is ever served from a different *site*, Strict cookies are not sent and the
layout (or `AUTH_COOKIE_SAMESITE`/`AUTH_COOKIE_DOMAIN`) must be revisited. The customer vhost already
returns 404 for `/api/admin/` and `/api/v1/admin-auth/`, so admin cookies are never issued there.

## 4. Open findings (not fixed here)

| ID | Severity | Finding | Owner / next action |
|----|----------|---------|---------------------|
| F1 | Low | Access tokens are stateless: after logout, password reset or suspension an already issued access token stays valid until it expires (<= 15 min). `get_current_user_id` routes do not re-check `users.status`. | Accept (short TTL) or add a per-user `token_version` claim checked on each request. |
| F2 | Low | Refresh JWT lives 14 days but the `auth_sessions` row is created with a 30-day `expires_at` (the JWT expiry wins, so no extension in practice). | Align to `REFRESH_TOKEN_EXPIRE_DAYS`. |
| F3 | Low | `GET /api/v1/auth/user/brokers` (legacy, no caller) declares a response model with fields the table no longer has: it answers 500 for any user with an account. No data is exposed. | Remove the route. |
| F4 | Low | bot-backend `app/core/security.py` still contains the old truncate/zero-pad `encrypt_credential`/`decrypt_credential`; nothing calls them. | Delete (bot-backend owner). |
| F5 | Medium (EXTERNAL: KYC stream) | `app/core/kyc_encryption.py` also tries the development default secret when decrypting in production. Data written before a real key was configured stays readable, but so would anything an attacker wrote with the public default key. | KYC owner: gate the fallback behind an explicit flag in production. |
| F6 | Info | `pip_audit` is not installed in `backends/venv`, so no automated Python CVE scan ran. Manual: python-jose 3.5.0 (fixes CVE-2024-33663/33664), cryptography 46.0.3, starlette 0.50.0, fastapi 0.128.0, python-multipart 0.0.21, urllib3 2.6.2, requests 2.32.5; ecdsa 0.19.1 (CVE-2024-23342, timing) is a python-jose dependency but only HS256 is used; passlib 1.7.4 is unmaintained. | Install `pip-audit` in CI and run `python -m pip_audit -r backends/requirements*.txt`. |
| F7 | High (supply chain) | `npm audit --omit=dev`: user-frontend 12 (critical `websocket-driver`; high `axios` <=1.19.0, `react-router`/`react-router-dom`, `@grpc/grpc-js`, `protobufjs`, `form-data`, `nanoid`, `postcss`, `browserslist`, `source-map-js`; moderate `baseline-browser-mapping`); admin-frontend 9 (high `axios`, `react-router`/`-dom`, `form-data`, `nanoid`, `postcss`, `browserslist`, `source-map-js`; moderate `baseline-browser-mapping`). Fixes are available for all; several are build-time only, `axios` and `react-router` ship to the browser. | Upgrade in a dedicated change with a full build + UI regression; not auto-fixed here. |
| F8 | Info | The legacy `?token=` on `/api/v1/events/stream` is still read while `AUTH_BEARER_COMPAT=true`. | Removed by migration step 3/4. |
| F9 | Info | First MFA enrolment is trust-on-first-use: whoever holds an un-enrolled admin's password when MFA becomes required can enrol their own authenticator. | Enrol every admin immediately after deploy; check `admin_mfa_enrolled` audit rows. |

## 5. Test runs (2026-10-17)

* `backends/user-backend`: `tests/test_section_h84_security.py` 49 passed; `tests/test_auth_hardening.py`
  59 passed; full suite 820 passed / 3 failed - the 3 failures
  (`test_phase2_broker_security.py::test_withdraw_capable_binance_key_is_restricted_not_connected`,
  `::test_trade_only_key_connects_with_evidence`, `::test_get_decrypted_credentials_is_resolver_backed_and_owned`,
  `BROKER_SECRET_KEY is required in production`) fail identically before this change.
* `backends/bot-backend`: `tests/test_section_h84_token_type.py` + `test_api_auth_hardening.py` +
  neighbours: 110 passed.
* `backends/admin-backend` (`APP_ENV=TEST`): 88 passed / 6 failed - the same 6 fail on a pristine
  export of HEAD (news/events fixtures).
* `backends/shared`: 89 passed / 10 failed - identical on a pristine export of HEAD (`BROKER_SECRET_KEY`).
* Front ends: user `npm test` 83 passed (8 new in `sessionAuth.test.ts`; the rest include other work streams' new tests); admin `npm test` 49 passed
  (9 new in `admin_mfa.test.ts`); `tsc --noEmit -p tsconfig.app.json` clean in both; ESLint: 0 errors.
