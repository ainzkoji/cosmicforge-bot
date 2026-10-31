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
| 20 | Python dependencies | FINDING F6 -> OPEN-UPSTREAM, in-major fixes applied (section 6.6) | `pip_audit` is not installed in `backends/venv` (not run). Manual review below |
| 21 | npm dependencies (`npm audit --omit=dev`) | FINDING F7 -> FIXED (section 6.7) | user-frontend: 12 (1 critical, 10 high, 1 moderate); admin-frontend: 9 (8 high, 1 moderate). Not auto-fixed |
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

> Recorded 2026-10-17. Each finding's current status and closure evidence are in section 6.

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

## 6. Remediation of F1-F9 (closure, 2026-10-30)

Status key for this section: **FIXED** (defect removed; regression tests added and passing) ·
**OPEN-UPSTREAM** (needs a fix in a third-party package) · **OPEN-OPERATIONAL** (needs an operating
action or elapsed time) · **ACCEPTED-WITH-EVIDENCE** (residual risk accepted, evidence given).
External penetration testing is still **not performed** (section 1, row 22).

| ID | Status | Disposition |
|----|--------|-------------|
| F1 | FIXED | Access tokens carry `tv` (token version) and `sid` (device session chain). Every user/admin access-token validator re-checks them and the account status on every request |
| F2 | FIXED | `auth_sessions.expires_at` follows `REFRESH_TOKEN_EXPIRE_DAYS` (admin rows already did) |
| F3 | FIXED | `GET /api/v1/auth/user/brokers` removed (no caller), now 404 |
| F4 | FIXED | bot-backend `encrypt_credential`/`decrypt_credential` deleted (no caller) |
| F5 | FIXED | Production never tries the public KYC dev key unless `KYC_ALLOW_LEGACY_DEV_KEY_DECRYPT=true` (default off; every use logged) |
| F6 | FIXED for the services (pip: OPEN-OPERATIONAL, build tooling) | 31 October 2026, second pass: fastapi 0.136.3 + starlette 1.3.1 (bot backend startup/shutdown moved to a `lifespan`), python-jose and ecdsa removed (PyJWT). pip-audit: 0 advisories in the bot, user and full requirement sets (13 before); only the venv installer pip 24.0 remains (fixed in 26.2.2, build-time). See 6.11. Earlier the same day: cryptography 50.0.2, pillow 13.0.0, pyasn1 0.6.5, black 26.10.1; before that 20 in-major pins (6.6). |
| F7 | FIXED | Both front ends: `npm audit` reports 0 advisories (full and `--omit=dev`) after in-major upgrades; see 6.7 |
| F8 | OPEN-OPERATIONAL (engineering ready) | `AUTH_BEARER_COMPAT` stays true by default. No in-repository client needs it; the switch-off is tested both ways in both backends, the bot backend's service bearer is unaffected, and the switch-off / rollback procedure is in 6.11. A log-redaction defect found on the way is FIXED; see 6.8 |
| F9 | FIXED | While `ADMIN_MFA_REQUIRED` is on, enrolment needs an operator-issued one-time bootstrap code |

### 6.1 F1 - stateless access tokens (and the session lifecycle)

* **Issue.** Access tokens (15 min) stayed valid after logout, logout-all, password reset and suspension.
  Routes using `get_current_user_id` did not re-check `users.status`.
* **Affected code.**
  * user-backend: `app/core/deps.py:get_current_user_id|get_current_active_user|get_current_admin`,
    `app/api/auth.py:get_current_user_id|get_current_active_user`, `app/api/events_proxy.py:stream_events`,
    `app/api/admin_auth.py:_enrolling_admin_id` (session path).
  * admin-backend: `app/core/auth.py:get_active_admin_from_token`.
  * bot-backend: `app/core/auth.py` (every dependency), `app/api/deps.py:get_current_active_user`,
    `app/api/events.py:stream_events`.
  * The cookie path is covered too: `SessionCookieMiddleware` only turns the cookie into an `Authorization`
    header, which these validators then check.
* **Severity.** Low (as recorded). A stolen access token kept working for up to 15 minutes after the owner reacted.
* **Exploit scenario.** An attacker copies an access token. The victim signs out everywhere or resets the
  password, and the token keeps working until `exp`. A suspended account kept calling every
  `get_current_user_id` route.
* **Remediation.**
  * **Schema.** Additive, in `shared_lib/persistence/session_version_schema.py`. It runs as `migrations.migrate`
    step 13c, and the writers also ensure it lazily.
    * New columns: `users.token_version`, `admins.token_version` (INTEGER DEFAULT 0), `auth_sessions.family_id`,
      `admin_sessions.family_id`, plus indexes.
    * No new table for F1.
  * **Tokens.** User and admin access tokens carry two new claims.
    * `tv`: the account's token version, read inside the login/refresh transaction.
    * `sid`: the first session id of the device's refresh-rotation chain. Rotation keeps it.
  * **Check.** `shared_lib/core/security/token_state.py` does one indexed read per request, with no cache. It requires:
    * the account exists and is active (`users.status='active'` / `admins.is_active=1`);
    * `tv == token_version` (a missing claim or column counts as 0);
    * if `sid` is present, the chain still has a non-revoked row.
  * **Responses.** A revoked token gets 401 `TOKEN_REVOKED`, so the client refreshes or signs in again. An inactive
    account gets 403. `type == "access"` / `"admin_access"` enforcement is unchanged.
  * **What bumps the version.**
    * Logout-all, password reset, and refresh-token reuse (user and admin).
    * An admin suspending a user; this now also revokes the user's refresh sessions.
    * Admin role grant or revoke.
    * Admin MFA enrolment and break-glass `reset-admin-mfa`.
    * Operator admin password reset (`create_admin.py --reset-password`).
  * **Multi-device.** `/logout`, `DELETE /sessions/{id}` and `/sessions/revoke` end only that device's chain: its
    access tokens die and other devices are untouched. Logout-all and password reset end every device. Admin
    `/logout` behaves the same way.
  * **SSE.** The user-backend proxy checks the session state before it opens the stream. The bot-backend stream
    re-checks every 30 s (`SSE_REAUTH_SECONDS`) and ends with an `auth_revoked` event.
  * **bot-backend: tokens whose subject has a `users` row** are fully re-checked.
  * **bot-backend: tokens whose subject has no `users` row** keep each dependency's previous behaviour:
    * `get_current_active_user` refuses them; the claim-only dependencies accept them.
    * Why that is acceptable: only SECRET_KEY holders can mint such tokens, and the user-backend - the only public
      path - refuses them.
  * **bot-backend: the admin emergency/governance service credential** (`act=admin-emergency`, at most 120 s) is not
    looked up as a user. The minting user-backend verified that admin, including the token version, for that single
    call.
* **Tests.**
  * user-backend `tests/test_section_h84_closure.py`:
    * `test_access_tokens_carry_the_token_version_and_the_device_session`
    * `test_logout_all_ends_the_access_tokens_of_every_device_at_once`
    * `test_signing_one_device_out_ends_only_that_devices_tokens`
    * `test_revoking_a_listed_session_ends_that_devices_access_token`
    * `test_refresh_rotation_keeps_the_device_chain_and_its_access_tokens_valid`
    * `test_refresh_token_reuse_also_ends_every_access_token`
    * `test_password_reset_ends_outstanding_access_tokens`
    * `test_get_current_user_id_routes_recheck_the_account_status`
    * `test_admin_suspension_ends_sessions_and_access_tokens`
    * `test_privilege_changes_end_tokens_minted_with_the_old_privileges`
    * `test_tokens_without_the_new_claims_are_accepted_only_until_the_first_bump`
    * `test_a_token_of_an_unknown_account_and_a_refresh_token_are_never_access`
    * `test_cookie_sessions_are_revoked_like_bearer_sessions`
    * `test_sse_proxy_refuses_a_revoked_access_token`
    * `test_admin_logout_ends_that_sessions_access_token`
    * `test_admin_mfa_enrolment_ends_every_earlier_admin_access_token`
    * `test_admin_refresh_rotation_keeps_the_chain_and_reuse_ends_access_tokens`
    * `test_break_glass_reset_ends_admin_access_tokens_and_voids_bootstrap_codes`
    * `test_admin_password_reset_by_the_operator_script_ends_access_tokens`
  * admin-backend `tests/test_admin_token_revocation.py`: 5 tests, including a database that does not have the new
    columns yet.
  * bot-backend `tests/test_section_h84_session_revocation.py`:
    * `test_a_current_token_passes_every_validator`
    * `test_a_bumped_token_version_is_refused_by_every_validator`
    * `test_a_signed_out_device_session_is_refused`
    * `test_a_suspended_account_is_refused_even_by_claim_only_dependencies`
    * `test_a_role_admin_token_is_rechecked_by_require_admin`
    * `test_refresh_tokens_are_still_never_access_tokens`
    * `test_unknown_subjects_keep_their_previous_per_dependency_behaviour`
    * `test_the_admin_emergency_service_credential_is_not_looked_up_as_a_user`
    * `test_an_open_sse_stream_ends_when_its_token_is_revoked`
* **Residual risk.**
  1. Changes that do not bump the version leave existing tokens valid. Examples: a user switching 2FA on or off, an
     admin disabling MFA voluntarily.
  2. An SSE stream lives up to 30 s after revocation, and a stream is not closed at its token's `exp`.
  3. The admin service credential survives at most 60 s after an admin is revoked. It covers one call and is never
     handed to a client.
  4. Per-request cost: one `DB()` construction plus one indexed read, on `get_current_user_id` routes that had no
     database access before.
  5. The schema helper uses SQLite `PRAGMA table_info`, like `admin_mfa_schema`. A PostgreSQL legacy store needs the
     same four columns through that dialect's migration path. That path belongs to workstream A1; `db.py` was not
     touched here.
* **Closure evidence.** Section 6.10.

### 6.2 F2 - session row lifetime

* **Issue and code.** `app/api/auth.py:login|refresh` wrote `expires_at = now + 30 days`, while the refresh JWT
  lives `REFRESH_TOKEN_EXPIRE_DAYS` (14).
* **Severity.** Low. The JWT `exp` won, so there was no exploit, but the session list kept showing sessions whose
  token had already expired.
* **Remediation.** `_session_expires_at()` uses `REFRESH_TOKEN_EXPIRE_DAYS` for login and for every rotation. Admin
  sessions already used the setting, and cookie lifetimes already derive from the same settings.
* **Tests.**
  * `test_session_rows_expire_with_the_refresh_token`: user login, a rotation, and an admin row; the row's expiry
    equals the JWT `exp` within one minute.
  * `test_cookie_lifetimes_follow_the_token_lifetimes`.
* **Residual risk.** None known.

### 6.3 F3 - dead legacy route

* **Issue and code.** `app/api/auth.py:list_brokers` (`GET /api/v1/auth/user/brokers`) declared a response model
  with columns the table no longer has, so it answered 500 for any user with an account.
* **Severity.** Low. No data was exposed.
* **Callers checked.** None in `frontends/*/src`, the backends, scripts or tests. The only reference is
  `test_auth_hardening.py`, which POSTs to the path and expects 404/405; that is still true.
* **Remediation.**
  * The route is removed and the `BrokerResponse` import dropped.
  * The route docs (`ROUTES_USER_BACKEND.md`, `backends/user-backend/ROUTES_USER_BACKEND.md`) are updated.
  * Broker accounts are listed by `app/api/brokers.py`.
* **Tests.** `test_the_dead_user_brokers_route_is_gone` (404, and the route is not in the router), plus the existing
  `test_legacy_broker_link_route_is_gone`.
* **Residual risk.** None.

### 6.4 F4 - dead credential helpers (bot-backend)

* **Issue and code.** `backends/bot-backend/app/core/security.py:encrypt_credential|decrypt_credential|_get_fernet`,
  which used a truncate / zero-pad key.
* **Severity.** Low. Dead code that invited reuse.
* **Callers checked.** grep over the whole repository (backends, `backends/scripts`, `scripts`, `tests`).
  * The singular names occur only in their own definition and in the user-backend's unrelated HKDF implementation.
  * Exchange credentials use `shared_lib.core.security.broker_security`, whose functions have plural names.
* **Remediation.** The functions are deleted, together with the now-unused `Fernet` and `base64` imports.
* **Tests.** `test_the_dead_credential_helpers_are_gone_and_nothing_imports_them`.
* **Residual risk.** None.

### 6.5 F5 - KYC development-key fallback in production

* **Issue and code.** `app/core/kyc_encryption.py:_decrypt_fernets` also tried the public development default secret
  in production.
* **Severity.** Medium.
* **Exploit scenario.** Anyone able to write a stored KYC value or document encrypted with the public key would have
  it accepted as genuine.
* **Remediation.** `legacy_dev_key_fallback_enabled()`:
  * Outside production: unchanged, the dev key is always tried.
  * In production: only with `KYC_ALLOW_LEGACY_DEV_KEY_DECRYPT=true`. The default is off, and only `1/true/yes/on`
    count as true.
  * Each production read that succeeds only with the legacy key logs an `AUDIT` WARNING, never the value.
    `assert_kyc_encryption_configured` also warns while the opt-in is on.
  * New data is never written with the legacy key.
  * The setting is documented in `deploy/env.production.example`.
* **Tests.**
  * `test_production_refuses_the_public_kyc_development_key_by_default`
  * `test_only_an_explicit_true_enables_the_production_fallback` (4 values)
  * `test_the_explicit_production_opt_in_reads_legacy_data_and_is_audited`
  * `test_outside_production_the_development_key_stays_readable`
  * The existing `test_kyc_hardening.py` is unchanged and passes.
* **Residual risk.** KYC data written in production before a real key was set stays unreadable until an operator
  turns the opt-in on temporarily and re-encrypts it. No KYC re-encryption CLI exists yet. Owner: KYC work stream.

### 6.6 F6 - Python dependency CVE scan

* **How it ran.** pip-audit 2.10.1 (PyPI advisory service, cross-checked once against OSV), from a separate scratch
  venv; `backends/venv` was not modified.
  * Each CI requirements file was audited together with `deploy/constraints.txt`, as CI installs it.
  * The installed set of `backends/venv` was audited too.
  * Counts are unique advisory IDs.
* **Counts before → after the safe upgrades:**

  | Set | Before | After |
  |---|---|---|
  | `backends/requirements.txt` | 124 (22 packages) | 53 (6 packages) |
  | bot-backend + constraints | 124 (22) | 53 (6) |
  | user-backend + constraints | 61 (18) | 11 (4) |
  | mt-bridge connector + constraints | 9 | 0 |
  | `tests/requirements.txt` + constraints | 13 | 0 |
  | mt-bridge server (own pins) | 19 (4) | 19 (not changed) |
  | `backends/venv` as installed | 131 (24) | 59 (7) once rebuilt with the new pins |

* **UPGRADED.** 20 pins in `deploy/constraints.txt` and `backends/requirements.txt`, all patch or minor within the
  current major. Every advisory of these packages is cleared:
  * anyio 4.14.2, certifi 2026.10.23, cryptography 46.0.7, dnspython 2.9.1, ecdsa 0.19.2, fsspec 2026.6.0
  * h2 4.4.1, hpack 4.2.0, idna 3.15, msgpack 1.2.4, pillow 12.3.0, pyasn1 0.6.4, pydantic-settings 2.14.2
  * pygments 2.20.0, pyjwt 2.15.1, pytest 9.0.3, python-dotenv 1.2.2, python-multipart 0.0.31, requests 2.33.1,
    urllib3 2.8.0

  Reachable before the upgrade:
  * python-multipart: the login forms (`OAuth2PasswordRequestForm`), mitigated by the nginx body limit.
  * certifi: every outbound TLS call (a distrusted root).
  * urllib3: outbound HTTP.

  The rest were transitive, and their vulnerable APIs are unused.
* **Verification.** A scratch venv was built from the `backends/venv` freeze with the 20 substitutions (`pip check`
  clean) and run against a `git archive HEAD` export.
  * user-backend: 819 passed / 3 failed, same as baseline.
  * admin-backend: 80/6, same as baseline.
  * shared: 89/10, same as baseline.
  * bot auth/JWT/HTTP/broker modules: 414 passed / 1 failed at baseline; with the upgrades, 413/2. The extra failure
    was a wall-clock budget test (`test_multi_symbol_workloads_stay_linear`, 6.8 s against 5.0 s, measured while
    another suite ran). Rerun alone it passed 12/12, and it touches no upgraded package.
* **ACCEPTED-WITH-EVIDENCE** (no fixed version exists):
  * python-jose 3.5.0, PYSEC-2026-4298 / CVE-2026-85394 (HMAC/DER algorithm confusion). Not reachable: every
    `jwt.decode` pins `algorithms=[HS256]` with a symmetric key, and no public keys exist. Recommendation: move to
    PyJWT.
  * ecdsa PYSEC-2026-1325 / CVE-2024-23342 (timing). ecdsa is never used to sign.
  * pyjwt PYSEC-2026-4146. pyjwt is not called by our code (it is a firebase-admin dependency).
  * passlib 1.7.4 has no advisory but is unmaintained (replacement is a package swap).
  * pip 24.0 in the server venv (installer, fixed in 26.2): upgrade pip when the venv is rebuilt.
* **OPEN-UPSTREAM / blocked on a major upgrade** (none applied here):
  * starlette 0.50.0, 5 advisories (fixes in 1.0.1-1.3.1).
    * Blocker: fastapi 0.128 requires `starlette<0.51`, so this needs fastapi with starlette 1.x.
    * Reachability is limited. Form limits are behind nginx `client_max_body_size`; Host reconstruction is behind
      nginx `server_name` / `proxy_set_header Host`; the StaticFiles issue is Windows-only.
  * cryptography, 4 advisories needing 48-50. GHSA-537c-gmf6-5ccf (the OpenSSL bundled in the wheel) applies to
    Fernet/KDF use; the X.509 and PKCS7 issues are not reachable.
  * pillow, 39 advisories needing 13.0. Nothing decodes untrusted images.
  * black, 3 advisories needing 26.x. Dev only; CI uses ruff.
  * mt-bridge server pins (fastapi 0.104.1, python-multipart 0.0.6, starlette 0.27.0, anyio 3.7.1). There are no
    tests to prove an upgrade, every endpoint takes JSON bodies, and the component awaits the owner's keep/remove
    decision.
* **Status.** OPEN-UPSTREAM for the blocked majors; every in-major fix is applied.
* **Operator action.** Rebuild `backends/venv` from the new pins:
  `pip install -r backends/requirements.txt`, or the service requirements with `-c deploy/constraints.txt`. Its editable
  `cosmicforge-shared` entry also points at a path that no longer exists.
* **Proposed CI job** (not applied; `ci.yml` is owned by the coordinator).
  * Install `pip-audit==2.10.1`.
  * For each of the bot-backend, user-backend, mt-bridge connector and `tests` requirements files, run
    `pip-audit --strict -r <req> -c deploy/constraints.txt` (through a wrapper file), plus `backends/requirements.txt`.
  * Use `--ignore-vuln` IDs read from a reviewed `.github/pip-audit-ignore.txt` (one `ID  # justification` per line,
    seeded with the accepted and blocked IDs above, each removed when its upgrade lands).

### 6.7 F7 - npm advisories (front ends)

* **Scope.** Only two workspaces exist: `frontends/user-frontend` and `frontends/admin-frontend`. No other
  `package.json` exists outside `node_modules`.
* **Before.** Today's advisory database gives these counts (low/moderate/high/critical):

  | workspace | `npm audit` (full) | `npm audit --omit=dev` |
  |---|---|---|
  | user-frontend | 1/2/15/1 | 0/1/11/1 |
  | admin-frontend | 1/2/13/0 | 0/1/9/0 |

  `follow-redirects` 1.16.0 has been added to the database since the counts in section 4.
* **After.** 0 advisories in both workspaces, for the full audit and for `--omit=dev`.
  `npm audit --omit=dev --audit-level=high` exits 0.
* **Upgrades.** All stay within the current major; nothing used `--force`. The same changes were made in both
  workspaces unless noted.
  * `axios` ^1.13.2 → ^1.20.0 (lock 1.15.2 → 1.20.0). Ships in the browser bundle.
    * The 1.16-1.21 changelog was read. The behaviour changes are in the Node adapter and redirects, plus opt-in
      `transitional` flags.
    * The code only uses `axios.create({baseURL, timeout, withCredentials})` and interceptors.
    * 1.21.0 was skipped: it was two days old and changes header defaults.
  * `react-router-dom` ^7.12.0 → ^7.18.4 (react-router 7.18.4). Ships in the browser bundle.
    * The 7.13-7.18 changes (`matchPath` optional params, relative-navigation encoding, `unstable_*` renames, stricter
      `useNavigation`/`generatePath` types) were checked.
    * None of those APIs are used; only declarative routing is.
  * `postcss` ^8.5.6 → ^8.5.30 and `vite` ^7.2.4 → ^7.3.7 (dev). Patch level, build time only.
  * `npm audit fix` without `--force`; the lockfile diff was inspected. It only changed transitive versions inside
    their existing ranges: nanoid, source-map-js, form-data, follow-redirects, browserslist,
    baseline-browser-mapping, brace-expansion, js-yaml, zod, @babel/*, websocket-driver 0.7.5, protobufjs 7.6.6.
  * user-frontend only: a scoped override `"overrides": {"@firebase/firestore": {"@grpc/grpc-js": "1.13.7"}}`.
    * Why it is needed: firebase pins `~1.9.0`, and no fix exists on the 1.9 line (GHSA-m9gg-hp2v-232j).
    * Why the risk is low: Firestore is never imported (the app uses only `firebase/app` and `firebase/messaging`),
      and none of grpc, faye-websocket or websocket-driver appear in `dist/`.
    * Drop the override when firebase moves off `~1.9`.
* **Checks after the upgrades** (identical to before):

  | check | user-frontend | admin-frontend |
  |---|---|---|
  | `npm test` | 83/83 | 49/49, then 50/50 with the F9 test added |
  | `npx tsc --noEmit -p tsconfig.app.json` | clean | clean |
  | `npx eslint .` | 0 errors (210 warnings, pre-existing) | 0 errors (151 warnings, pre-existing) |
  | `npm run build` | OK | OK |
  | `npm ci` | succeeds | succeeds |

* **Residual.** None open.

### 6.8 F8 - legacy `?token=` on the SSE stream

* **Status.** OPEN-OPERATIONAL, by design: the compat window stays governed by `AUTH_BEARER_COMPAT`.
* **Evidence that the window closes.** `test_sse_stream_refuses_url_tokens_after_the_migration_window` passes: with
  the flag false, a URL token is ignored and the request is 401. With the flag true, a refresh token is still refused.
* **Defect found and FIXED.** While the window is open, a legacy URL token appeared in clear in the user-backend's own
  access log, for two reasons:
  * The redaction key list in `shared_lib/core/security/redaction.py` did not contain a bare `token=`.
  * `install_log_redaction()` filtered only the root logger, while `uvicorn.access` does not propagate to it.
* **What changed.**
  * `token` is now redacted, without touching words like `csrf_token_present=`.
  * The filter is now also attached to `uvicorn`, `uvicorn.error` and `uvicorn.access`.
  * The filter now redacts tuple `args` in place, so uvicorn's access formatter, which unpacks `record.args`, keeps
    working.
  * nginx already had `access_log off` on that location.
* **Tests.** `test_legacy_sse_url_tokens_are_redacted_from_log_lines`, `test_uvicorn_access_lines_are_redacted_and_still_formatted`.
* **Cut-over prerequisites (section 3, steps 2-4).**
  1. Observe one to two release cycles with no `?token=` requests on `/api/v1/events/stream` and no bearer-only
     clients.
  2. Set `AUTH_BEARER_COMPAT=false` (rollback: set it to true again).
  3. Then remove the compat branches (`token_body`, the body `refresh_token`, `?token=` in `events_proxy`).
* **Owner.** Operations / release management.

### 6.9 F9 - trust-on-first-use admin MFA enrolment

* **Issue and code.** `app/api/admin_auth.py:admin_mfa_enroll`. While `ADMIN_MFA_REQUIRED` was on, the password alone
  allowed registering an authenticator for an admin who had not enrolled yet. The same was true after the
  break-glass reset.
* **Severity.** Info as recorded; in practice an account-takeover path for anyone holding a leaked admin password.
* **Remediation** (`app/core/admin_mfa_bootstrap.py`, schema `admin_mfa_bootstrap_codes` in
  `shared_lib/persistence/admin_mfa_schema.py`, classified CUSTOMER_AND_COMPLIANCE in `retention.py`).
  * **Issuing a code.** An operator with shell access runs
    `python scripts/security_maintenance.py issue-admin-mfa-bootstrap <email> [--minutes N]`.
    * The code is printed once, for out-of-band delivery.
    * Format: `xxxxx-xxxxx-xxxxx`, about 74 bits.
    * It is stored only as a salted SHA-256 hash and bound to that admin.
    * Default lifetime 30 min, maximum 24 h.
    * Issuing a new code revokes the admin's earlier unused codes.
  * **Using a code.** While MFA is required, `/mfa/enroll` needs `bootstrap_code` in addition to the TOTP code.
    * The code is checked before the TOTP code and spent only when the TOTP code is valid (guarded single-use UPDATE).
    * Each failure is audited, and counts against the existing per-admin MFA limit of 5 per 15 minutes (then 429).
  * **Login hints.** `/login` answers `mfa_bootstrap_required: true` next to the enrolment token, and `/mfa/setup`
    returns `bootstrap_required`.
  * **Audit rows.** `admin_mfa_bootstrap_issued`, `admin_mfa_bootstrap_used`, `admin_mfa_bootstrap_failed` (with a
    reason). `admin_mfa_enrolled` records whether a bootstrap code was used.
  * **Device loss.**
    * The existing recovery codes still sign the admin in.
    * Otherwise the break-glass `reset-admin-mfa` clears MFA, revokes sessions, bumps the token version and voids
      unused bootstrap codes. Re-enrolment then needs a fresh code; `--issue-bootstrap` issues one in the same step.
  * **Admin console.** `pages/Login.tsx` shows a "bootstrap code" field on the enrolment screen when the backend asks
    for it, and keeps the code in component state only. Error `MFA_BOOTSTRAP_INVALID` is explained to the user. Also
    changed: `lib/adminMfa.ts` (`bootstrapRequired`, `looksLikeBootstrapCode`) and `api/client.ts` (`mfaEnroll`
    sends `bootstrap_code`).
  * **Not decided here.** Who may issue codes, and how the operator verifies the admin's identity first, is an
    operating decision outside the code. The code provides only the mechanism and the audit rows.
* **Tests.**
  * `test_required_enrolment_without_a_bootstrap_code_is_refused_and_audited`
  * `test_a_bootstrap_code_enrols_once_is_stored_hashed_and_audited` (including: after a break-glass reset the same
    code fails and a fresh one works)
  * `test_a_bootstrap_code_is_bound_to_its_admin_and_expires` (another admin's code, an expired code, and a code
    replaced by a newer one)
  * `test_bootstrap_code_guessing_is_rate_limited`
  * `test_bootstrap_codes_have_a_short_bounded_lifetime`
  * `test_voluntary_enrolment_needs_no_bootstrap_code_when_mfa_is_not_required`
  * front end: `admin_mfa.test.ts` "enrolment asks for the operator-issued bootstrap code ..."
* **Existing tests updated, because the flow legitimately changed.** In `test_section_h84_security.py`,
  `test_required_mfa_gives_an_unenrolled_admin_only_an_enrolment_token` and
  `test_enrolment_with_the_step_token_works_despite_a_stale_admin_cookie` now issue and send a bootstrap code. In
  `admin_mfa.test.ts`, the enrolment step now has `bootstrapRequired: false`.
* **Residual risk.**
  * `/mfa/setup` does not need the bootstrap code. Anyone holding the password can still replace a *pending*
    (unconfirmed) secret. That cannot enrol a factor, but it can disrupt an enrolment in progress.
  * Whoever has shell access to the server can issue codes.

### 6.10 Closure evidence (test runs, 2026-10-30)

**Method.**
* Baseline: a `git archive HEAD` (78a14e4) export.
* After: the same export plus only this closure's changes, so other work streams' uncommitted edits are excluded.
* Both use `backends/venv`; admin and shared run with `APP_ENV=TEST`, as in CI.
* Failure sets were compared by name.

| Suite | Command | Baseline | After | Failures |
|---|---|---|---|---|
| user-backend (full) | `cd backends/user-backend && python -m pytest tests -q -o addopts="" -p no:cacheprovider` | 819 passed / 3 failed | 856 passed / 3 failed (+37 new) | identical: `test_phase2_broker_security.py` x3, `BROKER_SECRET_KEY` |
| admin-backend | `APP_ENV=TEST python -m pytest backends/admin-backend/tests -q -p no:cacheprovider` | 80 / 6 | 85 / 6 (+5 new) | identical: news/events fixtures |
| shared | `APP_ENV=TEST python -m pytest backends/shared/tests -q -p no:cacheprovider` | 89 / 10 | 89 / 10 | identical: `BROKER_SECRET_KEY` |
| bot-backend auth + neighbours | `cd backends/bot-backend && python -m pytest -q -p no:cacheprovider` (16 modules, see note) + `tests/test_section_h84_session_revocation.py` | 414 / 1 | 424 / 1 (+10 new) | identical: `test_unknown_broker_fails_closed` |
| admin front end | `npm test`, `npx tsc --noEmit -p tsconfig.app.json`, `npx eslint .`, `npm run build` | 49 pass | 50 pass; tsc clean; 0 errors; build OK | none |
| user front end | same commands (F7 only; no source change) | 83 pass | 83 pass; tsc clean; 0 errors; build OK | none |

* **The 16 bot-backend modules:**
  * Auth: `test_section_h84_token_type`, `test_api_auth_hardening`, `test_emergency_controls`, `test_step3_governance_api`.
  * Tenant isolation and SSE: `test_multitenant_isolation`, `test_sse_per_user_delivery`.
  * CATI and security: `test_cati_section20_api`, `test_cati_section25_security`, `test_phase6_certification_security`.
  * Broker: `test_phase2a_broker_foundation`, `test_phase2b_permissions_transfers`, `test_paper_broker_mutation_block`,
    `test_broker_quarantine`.
  * Other: `test_sections7_9_closure`, `markets/test_step7_market_activation`,
    `trading_intelligence/test_step3_retention` (every table classified).
* **Platform caveat.** Windows workstation only. Ubuntu CI is the Linux evidence and has not run on these changes yet.

### 6.11 Final hardening pass (2026-10-31): F6 majors, F8 switch-off readiness, N5 / L-07, historical credentials

#### F6 - the blocked majors

* **Upgraded** (`backends/requirements.txt`, `deploy/constraints.txt`):
  * `fastapi` 0.128.0 → 0.136.3 and `starlette` 0.50.0 → 1.3.1.
    * fastapi 0.128 required `starlette<0.51`; 0.133.0 is the first release without an upper bound.
    * starlette 1.3.1 is the lowest release that fixes all five starlette advisories.
    * **Why not a newer fastapi.** 0.137.0 stopped flattening included routers: `app.routes` now holds lazily
      resolved `_IncludedRouter` entries. Trialled with fastapi 0.140.13, the bot backend's security route walk
      failed, along with three route lookups in `test_api_auth_hardening.py`:
      * `test_every_route_requires_auth_unless_explicitly_public`;
      * `test_public_allow_list_is_short_and_not_stale`.

      Serving itself worked. Adapting the walk would mean depending on FastAPI internals, so 0.136.3, the last
      release before that change, was chosen. It passes everything below. Moving past 0.136 needs that walk
      rewritten first.
    * `sse-starlette` 3.2.0, `httpx` 0.28.1 and `uvicorn` 0.40.0 are unchanged and work with it.
  * `python-jose` 3.5.0 and `ecdsa` 0.19.2 **removed**. JWTs now use PyJWT 2.15.1, which was already pinned as a
    firebase-admin dependency. `rsa`, `pyasn1` and `six` stay: google-auth and python-dateutil need them.
* **lifespan migration.** Starlette 1.x has no `@app.on_event`.
  * Only the bot backend used it: 12 handlers in `app/main.py`. The user and admin backends had none.
  * The handlers keep their place in the file. `@app.on_event("startup"/"shutdown")` became `@_on_startup` /
    `@_on_shutdown`, which append to two lists. `_lifespan` (passed to `FastAPI(lifespan=...)`) runs those lists.
  * The semantics are on_event's:
    * Startup runs in registration order. The first hook that raises aborts startup: uvicorn refuses to serve,
      and a `TestClient` context raises. No later startup hook and no shutdown hook runs.
    * Shutdown also runs in registration order, not reversed (`_shutdown_run_manager` releases the runtime lease
      first). A hook that raises stops the ones after it.
  * Tests: `tests/test_app_lifespan.py`.
    * Pins the exact order: 7 startup hooks, 5 shutdown hooks.
    * Checks that no `on_event` or `add_event_handler` is left.
    * Checks that a `TestClient` context runs both phases.
    * Checks that a failing startup hook refuses startup and runs nothing after it, and that a failing shutdown
      hook stops the rest.
    * Checks that the real live-mode safety refusal (`_startup_validate_config`) still refuses startup.
  * The code is compatible with both starlette 0.50 and 1.3.1. Both runs pass.
* **Other Starlette 1.x API breaks checked.**
  * No `@app.route`, `websocket_route`, `on_startup=`/`on_shutdown=` arguments, templates or WSGI middleware anywhere.
  * `@app.middleware("http")` is FastAPI's own decorator.
  * `SessionCookieMiddleware` is pure ASGI.
  * `StreamingResponse`, `BackgroundTask`, exception handlers and `TestClient` usage are unchanged APIs. The SSE
    suites below pass.
* **python-jose → PyJWT.**
  * Every use was HS256 `encode`/`decode` with the same claims, so the swap is mechanical.
  * The one shim is `shared_lib/core/security/jwt_tokens.py`. It exports `jwt`, `JWTError`, `ExpiredSignatureError`
    and a `JWTClaimsError` tuple. Call sites changed only their import line:
    * bot `app/core/security.py`;
    * user `app/core/security.py`, `app/core/deps.py` and `app/api/admin_emergency.py`;
    * admin `app/core/auth.py`;
    * 11 test modules.
  * Tokens minted by either library verify with the other, so a rolling restart is safe.
  * **Behaviour change, deliberately stricter.** python-jose silently ignored `options={"require": [...]}`, so a
    correctly signed token without `exp` (or `iss`/`aud`/`sub`) was accepted and never expired. PyJWT enforces the
    list. Every token we mint carries all four claims.
  * Tests: `test_section_h84_token_type.py::test_a_token_missing_a_required_claim_is_refused` (4 cases) and
    `::test_foreign_keys_unsigned_tokens_and_other_algorithms_are_refused` (`alg: none`, another secret, HS512,
    expired).
* **pip-audit 2.10.1, scratch venv.** Each requirements file was audited with `-c deploy/constraints.txt`, as CI
  installs it.

  | Set | Before (raw entries / unique advisories) | After |
  |---|---|---|
  | `backends/bot-backend/requirements.txt` + constraints | 13 / 7: starlette 10/5, ecdsa 2/1, python-jose 1/1 | **0** |
  | `backends/user-backend/requirements.txt` + constraints | 13 / 7 (same three packages) | **0** |
  | `backends/requirements.txt` (full freeze) | 13 / 7 (same three packages) | **0** |
  | `pip` itself (the venv installer, not a requirement) | pip 24.0: 12 / 6 | pip 26.2.2: 0 |

  The "25 advisories in 4 packages" of compliance 5.2 were these 13 plus pip's 12.
* **pip is build-time only.**
  * The server venv's pip 24.0 comes from Python 3.12.2's `ensurepip`. It runs only while packages are being
    installed, never in a serving process.
  * CI uses `actions/setup-python`'s current pip.
  * **Operator / ops action** when a venv or image is built:
    `backends/venv/bin/python -m pip install --upgrade pip==26.2.2` before the `pip install -r ...` step. In
    `deploy/containers/backend.Dockerfile`, add `pip install --upgrade pip==26.2.2` first (deploy files other than
    the pins are owned by operations).
  * Status: OPEN-OPERATIONAL (build tooling).
* **Not changed.** The mt-bridge server's own pins (fastapi 0.104.1, starlette 0.27.0) await the Batch 9 keep/remove
  decision. Its two `@app.on_event` handlers are not on the certified runtime.
* **Status: FIXED** for the deployed services, with zero advisories in every service requirement set. pip is
  OPEN-OPERATIONAL (build tooling only).

#### F8 - bearer compatibility window: ready to close, still open by default

* **Default kept at `true`.** This covers `user-backend/app/core/config.py`, the admin backend's env fallback and
  `deploy/env.production.example`. The code default is not flipped:
  * 13 user- and admin-backend test modules (66 `Bearer` call sites, most in shared helpers) authenticate with
    bearer headers;
  * the stale `tests/integration` (INT-01) does too;
  * out-of-repository API clients cannot be ruled out from the code.

  Flipping the default is the operator's observation-window decision (section 3, step 2), not a code change.
* **No in-repository client needs the window.**
  * Both front ends send cookies only:
    * `withCredentials: true` and `credentials: "include"`;
    * legacy stored tokens are purged on load;
    * placeholder `Authorization` headers are stripped.
  * The SSE `EventSource` uses `withCredentials`, never `?token=`.
  * The MT connector calls the platform with body-carried pairing codes. Its own bearer goes only to the local
    bridge.
  * No service calls the user or admin backend with a bearer.
* **The service-to-service bearer is unaffected.**
  * The bot backend has no cookie middleware and never reads `AUTH_BEARER_COMPAT`.
  * The user backend's middleware turns the browser's cookie into `Authorization: Bearer` before the routes run.
    The proxies forward that header, and `admin_emergency` mints its own short service token.
  * Test: `test_the_service_bearer_to_the_bot_backend_does_not_depend_on_the_compat_window[True|False]`.
* **Switch-off verified both ways.**
  * user backend (`tests/test_section_h84_security.py`):
    * true: bearer headers and the body refresh token work;
    * false: login and refresh bodies carry no tokens, and a bearer header is ignored (401). The body
      `refresh_token` is refused (401) while the cookie refresh with CSRF works. Admin bearer headers are ignored
      while the admin cookie works, and `?token=` on the SSE stream is ignored.
  * admin backend (`tests/test_admin_backend_shell.py`, `false`/`0`/`off`): a bearer-only request gets 401. The
    `cf_admin_access` cookie gets 200. A cookie POST without the CSRF header gets 403 `CSRF_FAILED`.
* **Switch-off procedure (operator).**
  1. **Observe** for at least one release cycle:
     * no `?token=` requests on `/api/v1/events/stream`;
     * no requests carrying `Authorization` that did not come from a cookie. Log the presence of
       `$http_authorization` in a temporary nginx log format; never log the value.
  2. Set `AUTH_BEARER_COMPAT=false` in **both** `/etc/cosmicforge/user-backend.env` and
     `/etc/cosmicforge/admin-backend.env`, then `systemctl restart cosmicforge-user-backend cosmicforge-admin-backend`.
     The trading service needs no change.
  3. **Watch** for 24 h:
     * the 401 rate on `/api/v1/*` and `/api/admin/*`;
     * 401s on `/api/v1/auth/refresh` and `/api/v1/admin-auth/refresh`;
     * 403 `CSRF_FAILED`;
     * support tickets about being signed out;
     * MT-connector pairing success;
     * the admin console's emergency and governance actions, which go user backend → bot backend.
* **Rollback.**
  * Set `AUTH_BEARER_COMPAT=true` in the same two files and restart the same two services.
  * There is no data migration and no session loss: cookie sessions keep working in both states, and bearer clients
    work again at once.
  * Roll back if a legitimate client breaks; do not wait for the cause.
* **Clean-up (later).** Remove `token_body`, the body `refresh_token` and `?token=` in `events_proxy` once the flag
  has stayed false for a release cycle (section 3, step 4).
* **Status: OPEN-OPERATIONAL.** The engineering is ready and tested both ways. The flip is the operator's
  observation-window step.

#### N5 / L-07 - non-enumerating sign-up (authorized and implemented)

* **`POST /api/v1/auth/register`** returns the same answer for every address: 200 and
  `{id, email, status: "pending_verification", role: "user", is_verified: false, created_at}`.
  * `created_at` is always the time of the request.
  * For an existing account, `id` is an HMAC-derived decoy: stable per address, never the real id. Repeated sign-ups
    for one address therefore return one id, exactly as they do for a new address.
  * Every path performs one hash and one password comparison: new, unverified and existing alike.
  * Nothing is created or changed for an existing account.
  * An **active** account's owner is mailed a notice through the existing mail path (fire-and-forget, never raises,
    never blocks). The notice says the account exists and suggests sign-in or "Forgot password". A suspended account
    is not mailed.
  * An audit row `registration_existing_account` records the attempt (internal only).
  * Rate limits are unchanged and identical for every address: the request counts against the per-address and
    per-IP code limits before the lookup. This also bounds the notices: at most 3 per 15 minutes and 10 per day.
* **`POST /api/v1/auth/verify-email`** for an already-verified address now answers 400 `CODE_FAILED_DETAIL`. That is
  the same answer an unknown address or a wrong code gets. It used to answer "Email already verified" to anyone,
  without a code.
* **Front end.**
  * `pages/Register.tsx` dropped its unreachable "already registered / exists" branch: the backend never sent that
    text.
  * After every sign-up it shows one neutral message: "If this address is new, we've sent a verification code. If it
    already has an account, we've sent sign-in instructions instead."
  * `npm test` 91/91, `tsc` clean, eslint 0 errors.
* **Tests** (`user-backend/tests/test_auth_hardening.py`): these replace
  `test_registering_an_active_address_is_refused`.
  * Same status, content type and body for an existing and a new address. No second account, the old password still
    works, the owner gets a mail call and the existing owner gets no code.
  * Stable decoy id.
  * Suspended account: same answer, no mail.
  * Identical limits for an existing and a new address (3 × 200, then 429), with the notices bounded by them.
  * Equal hashing work on all three paths (one `hash`, one `verify`).
  * Generic `verify-email` answer.
  * Notice delivery is queued in the background and survives an SMTP failure.
* **Status: FIXED.**

#### Historical administrator credentials - procedure verified, rotation OPERATOR-REQUIRED

* **Drill.** The procedure of compliance 5.3 was run against a throwaway SQLite database, with a dummy admin, from a
  pristine export (no `.env`).
  * The two real CLIs ran as subprocesses, from `backends/user-backend`:
    * `python scripts/create_admin.py <email>`;
    * `... --reset-password`;
    * `python scripts/security_maintenance.py reset-admin-mfa <email> --issue-bootstrap`.
  * Passwords were generated at run time and passed only through `ADMIN_PASSWORD`. **23/23 checks passed:**
    * a password given as an argument is refused (exit 2) and nothing is created;
    * create succeeds without printing the password;
    * re-running without `--reset-password` changes nothing (exit 1);
    * the reset prints neither password;
    * the old password stops signing in and the new one works;
    * every session opened before the reset is revoked;
    * `token_version` 0 → 1, and an access token issued before the reset is refused;
    * `reset-admin-mfa --issue-bootstrap` clears MFA, bumps `token_version` again and leaves no live session;
    * exactly one bootstrap code is printed, and it is stored only as a hash;
    * both audit rows are written;
    * an unknown admin gets exit 1.
  * Windows caveat: with no `ADMIN_PASSWORD` and no terminal, Linux refuses with a message. On Windows, `NUL` reports
    `isatty()` and `getpass` waits on the console, so that check is Linux-only.
* **Test added.** `test_section_h84_closure.py::test_operator_rotation_procedure_end_to_end` runs the same sequence
  through both CLIs' `main`.
* **Correction to compliance 5.3, step 2.** The script is `backends/user-backend/scripts/security_maintenance.py`;
  run it from `backends/user-backend`.
* **Status: OPERATOR-REQUIRED.**
  * Nothing was rotated: there is no access to a deployed database.
  * Every administrator account named in `db4580b`, `cc13da0` and `b35c198` stays compromised until the operator has
    run steps 1-4 on every environment and recorded the date and operator in EXT-SEC-02.
  * No historical credential was read out, displayed or used in this pass.

#### Evidence (2026-10-31)

**Method.**
* Baseline: a `git archive` export of `7aa13e4`.
* After: the same export plus only this pass's files; other work streams' uncommitted edits are excluded.
* Two scratch venvs from the same Python 3.12.2:
  * one with the old pins (`backends/requirements.txt` + both service files + `deploy/constraints.txt`);
  * one with the new pins.
* `backends/venv` was not modified.
* CI environment (`APP_ENV=TEST`, `-p ci_quarantine`); failure sets compared by name.

| Suite | Baseline, old pins | After, **new pins** | After, old pins | Failures |
|---|---|---|---|---|
| user-backend (full) | 880 passed / 1 failed | 891 / 1 (+11 new) | 891 / 1 | identical: `test_step1_proxies::test_every_report_path_the_portal_calls_has_a_route` reads `frontends/`, which is not in the export |
| admin-backend (full) | 94 / 0 | 98 / 0 (+4 new) | 98 / 0 | none |
| shared (full) | 256 / 0 | 256 / 0 | 256 / 0 | none |
| bot-backend: every module that imports fastapi/starlette/jwt or uses `TestClient`/SSE/routes/auth (67 files) | 1,261 passed, 10 failed, 33 errors, 31 skipped | 1,272 / 10 / 33 / 31 (+11 new) | 1,272 / 10 / 33 / 31 | identical sets: the export has no `research/`, `docs/research/` or model files (37 `FileNotFoundError`s and their consequences) |
| bot `markets/test_step7_market_scope.py` | 10 / 0 | 10 / 0 | — | none |
| user front end | — | `npm test` 91/91; `tsc` clean; eslint on `Register.tsx` 0 errors (1 pre-existing warning) | — | none |
| rotation drill | — | 23/23 checks | — | none |

* The fastapi 0.140.13 trial is recorded above: 4 route-walk failures in `test_api_auth_hardening.py`. That is why
  0.136.3 was chosen.
* **Platform caveat.** Windows workstation only. Ubuntu CI is the Linux evidence and has not run on these changes.
