# Billing, legal documents and identity (Section H Step 8.1 - 8.3)

Status: implemented and tested in TEST MODE only (2026-10-17). Nothing here
has been verified against live Stripe, reviewed by legal counsel, or certified
with an identity provider. See "What remains external" at the end.

## 8.1 Stripe billing

Built on the existing billing code (`shared_lib/billing/*`, user-backend
`app/api/billing.py`, `app/core/billing_service.py`, `app/api/admin_billing.py`).
`BILLING_ENFORCED` stays `false` by default; nothing here changes that.

### What already existed (audited, kept)

- Signature check over the raw body (`verify_stripe_signature`, Stripe `v1`
  scheme, 5 minute tolerance, fails closed without `STRIPE_WEBHOOK_SECRET`).
- Idempotency keyed by Stripe event id: `billing_events` has a unique index on
  `(provider, event_id)`; the event id is inserted in the same transaction as
  the state change, so a redelivery is a no-op and a failed handler rolls back
  completely and is retried by Stripe.
- Out-of-order protection: `provider_updated_at` (event `created`) per
  subscription, refusal of `incomplete` snapshots after a payment, permanent
  "subscription ended" markers, newer-subscription checks, period ends only move
  forward, grace period counted from the first failure.
- Plan from the server-side Price ID mapping (env `STRIPE_PRICE_*`), user from
  the stored customer mapping or server-set `client_reference_id`/metadata,
  amounts from the signed invoice. The checkout-status endpoint is read-only;
  the success redirect grants nothing.

### What was added

| Gap | Change |
| --- | --- |
| `customer.subscription.created` not handled | Routed to the same handler as `updated` (same ordering guards). |
| Refunds / disputes ignored | `charge.refunded`, `charge.dispute.created/updated/closed` are recorded in `billing_payment_reviews` (new table). |
| Refund / dispute entitlement policy | A dispute, or a full refund, **holds** the paid entitlement (`subscriptions.billing_hold`; `resolve_subscription` serves the free plan while held). A partial refund is flagged without a hold; if it later becomes a full refund it holds. Only an operator resolves a review (`RESOLVED_KEEP_ACCESS` lifts the hold, `RESOLVED_REVOKE_ACCESS` keeps it). A renewal does not lift a hold. Refunds/disputes that cannot be matched to a user are still recorded (user NULL). Invoice status becomes `refunded` / `partially_refunded` / `disputed` and is never turned back into `paid` by a late `invoice.paid`. |
| Tracing a charge to a user | `invoices.provider_charge_id` / `provider_payment_intent_id` recorded from `invoice.paid` / `invoice.payment_failed`. |
| No billing audit log | `billing_audit_log` (append-only: UPDATE/DELETE triggers abort). Rows for every subscription write (before/after snapshot of plan/status/period/provider ids, no PII, no raw payload), every applied provider event, deferred writes under an operator grant, refund/dispute review changes, hold set/cleared, user cancel/resume, expiry downgrade, operator grant/revoke and review resolution. |
| Live key accepted without decision | `get_provider()` refuses a live key (`sk_live_`, `rk_live_`, anything that is not a test key) unless `BILLING_LIVE_MODE_AUTHORIZED=true`. |
| Missing price error unclear | `create_checkout_session` raises `BillingNotConfigured("no Stripe price is configured for plan_x: set STRIPE_PRICE_X ...")` (HTTP 503 to the client, reason in the log). |
| No config status | `GET /api/admin/billing/config`: readiness, errors, key mode, live authorization, webhook secret present, per-plan price ids (valid / missing / duplicated), `billing_enforced`. Never returns a secret value. |
| No user-visible notices / portal | `GET /api/billing/subscription` now also returns `notices` (PAYMENT_UNDER_REVIEW, PAYMENT_FAILED, CANCELLATION_SCHEDULED, SUBSCRIPTION_ENDED), `billing_hold`, `provider_status`, `portal_available`, `billing_enforced`, `source`. `POST /api/billing/subscription/manage {"action":"portal"}` returns a Stripe Billing Portal URL for the user's stored customer (return URL from server config). |

New admin endpoints: `GET /api/admin/billing/config`, `GET /api/admin/billing/reviews`,
`POST /api/admin/billing/reviews/{id}/resolve`, `GET /api/admin/billing/audit`.

Cancellation: at period end via the manage endpoint (sets
`cancel_at_period_end` in Stripe; the `customer.subscription.updated` webhook
confirms it) or the portal; immediate cancellation arrives as
`customer.subscription.deleted` and downgrades to the free plan.

Frontend: `src/utils/billingStatus.ts` (pure view model: status label,
renewal text, notices, cancel/resume/portal availability) used by
`pages/Subscription.tsx` (notice panel, "Manage payment & receipts" portal
button, receipts via the provider's hosted invoice links already in billing
history). `src/api/billingPortal.ts` calls the manage endpoint.

## 8.2 Legal documents and consent

New package `backends/shared/shared_lib/legal/` (`schema.py`, `documents.py`,
`seed.py`), migrated from `migrations.migrate()` step 50.

Tables (integrity enforced by SQLite triggers / CHECK constraints, not only by code):

- `legal_documents`: `doc_id, category, version, locale, title, body,
  content_sha256, published_at, published_by, material_change, change_summary,
  supersedes_doc_id, review_status, reviewer_name, reviewer_reference,
  reviewed_at, review_recorded_by`. Content columns cannot be updated, rows
  cannot be deleted. `review_status` is `DRAFT_PENDING_COUNSEL` unless a
  reviewer name, reference and time are present (CHECK). A recorded approval
  cannot be changed. Publishing always creates a DRAFT.
- `legal_acceptances`: `user_id, doc_id, category, version, locale,
  content_sha256, review_status_at_acceptance, accepted_at, ip_hash,
  user_agent_hash, source, withdrawn_at, withdrawal_reason`. Evidence columns
  immutable; withdrawal can be set once; no deletes. IP and user agent are
  HMAC-SHA256 hashes keyed by `LEGAL_EVIDENCE_HASH_KEY`.
- `legal_audit_log`: append-only.
- `legal_country_eligibility`: `country_code, status
  (UNDETERMINED|PERMITTED|RESTRICTED|PROHIBITED), determination_ref,
  determined_by, determined_at, notes`. Anything other than UNDETERMINED
  requires a determination reference. A country with no row is UNDETERMINED.

Functions (`shared_lib.legal.documents`): `publish`, `record_counsel_review`
(admin API only), `current_document(s)` (locale fallback to `en`),
`accept` (binds the exact `content_sha256`; refuses mismatched or superseded
versions; all-or-nothing; idempotent), `acceptance_status`,
`has_current_acceptance` (False after withdrawal or a material amendment),
`required_reconsent` (never accepted, or a material amendment since the
accepted version; non-material amendments do not re-prompt), `withdraw`
(reports effects: e.g. withdrawing `AUTOMATED_EXECUTION_CONSENT` makes
`has_current_acceptance` False so a gate can refuse), `eligibility(country)`,
`record_eligibility`, `record_registration_acceptances`.

APIs: `GET /api/legal/documents` (public; `purpose=registration` lists what
sign-up requires and which of those are unpublished), `GET
/api/legal/documents/{doc_id}`, `GET /api/legal/eligibility/{cc}`, `GET
/api/legal/status` (auth), `POST /api/legal/accept`, `POST /api/legal/withdraw`;
admin: `GET/POST /api/admin/legal/documents`, `POST
/api/admin/legal/documents/{id}/review`, `GET/POST
/api/admin/legal/country-eligibility`.

Registration: `UserCreate.legal_acceptances` (optional list of `{doc_id,
content_sha256}`); `register` records them for a newly created account inside
a savepoint, never failing the sign-up (anything not recorded is re-prompted
after sign-in). Nothing is inferred from `risk_disclaimer_accepted_at`.

Seed: only `TRADING_RISK_DISCLOSURE` v1 (en), the product's existing risk
disclosure text, titled "Risk Disclosure (Draft - pending counsel review)",
body starting with a DRAFT notice, `review_status = DRAFT_PENDING_COUNSEL`.
No Terms of Service, Privacy Policy or other legal text was written; those
categories stay unpublished (pages keep the honest placeholder) until real
text is published through the admin API.

Required categories (only when published): registration = Terms, Privacy,
Risk Disclosure, Data Processing Notice, Registration Consent; subscription =
Subscription Terms; account_connection = Account Connection Authorization;
automated_execution = Automated Execution Consent + Risk Disclosure. The
trading gates are not rewired to these yet.

Frontend: `src/utils/legalDocuments.ts` (draft detection, body parsing,
acceptance payload, pending list), `src/api/legal.ts`,
`components/Legal/LegalDocumentView.tsx` (renders the current published
version with a DRAFT banner unless counsel-approved; falls back to the
placeholder / static text), `components/Legal/ReconsentPrompt.tsx` (mounted in
`DashboardLayout`), Terms/Privacy/RiskDisclosure pages, Register.tsx (one
checkbox per published registration document, sends `legal_acceptances`).

## 8.3 Identity

New package `backends/shared/shared_lib/identity/` on top of the existing KYC
tables, API and manual review (unchanged).

- `state.py`: `VerificationState` (REGISTERED, VERIFICATION_REQUIRED, PENDING,
  VERIFIED, REJECTED, MANUAL_REVIEW, EXPIRED, REVERIFICATION_REQUIRED) derived
  from `kyc_cases`; validity from `kyc_cases.expires_at` or `approved_at +
  IDENTITY_VERIFICATION_VALIDITY_DAYS` (unset = no expiry; malformed = fail
  closed); `ALLOWED_TRANSITIONS` (actor types `user`, `reviewer`, `system`);
  `apply_transition` (refuses anything not in the table, needs a reason for
  reviewer refusals, an explicit evidence check for VERIFIED, real expiry for
  the system actor; writes `kyc_audit_log` with state names and reason codes
  only); `expire_due_verifications` (persists expiry as `expired`, which the
  existing `kyc_policy` gate already refuses).
- `policy.py`: `verification_required(country, action)` from
  `kyc_requirements_config` (unconfigured or `requires_kyc=0` actions and
  `country_exceptions` are not required; no blanket requirement);
  `identity_permits(user, action)` -> `IdentityDecision` (codes NOT_REQUIRED,
  VERIFIED, VERIFICATION_REQUIRED, PENDING, MANUAL_REVIEW, REJECTED,
  REVERIFICATION_REQUIRED, IDENTITY_PROVIDER_NOT_CONFIGURED,
  IDENTITY_UNAVAILABLE, UNKNOWN_USER); `restrictions(user)`. Exposed for the
  live-trading path; existing gates were not rewired.
- `providers.py`: `IdentityProvider` protocol, `ManualReviewProvider` (current
  flow; optional config check), `NotConfiguredProvider` (fails closed with
  `IDENTITY_PROVIDER_NOT_CONFIGURED`), `get_identity_provider()` from
  `IDENTITY_PROVIDER` (unset/`manual` -> manual review; `none` or an unknown
  name -> not configured).

## Tests (actual results, 2026-10-17, Windows, backends/venv Python)

New test files:

- `backends/user-backend/tests/test_billing_lifecycle.py` - 9 passed. Full
  TEST-MODE lifecycle over HTTP with a fake `stripe` module (no network) and
  webhooks signed locally with a test secret: checkout -> forged webhook
  refused -> subscription.created (no access) -> checkout.session.completed ->
  invoice.paid -> duplicate delivery -> late out-of-order snapshot ->
  renewal -> payment failure (past_due + notice) -> recovery -> cancel at
  period end (+ portal link) -> full refund (hold, review, resolve KEEP) ->
  dispute (hold, close, resolve REVOKE, renewal does not lift) -> immediate
  deletion; audit trail and append-only enforcement; partial refund; unmatched
  dispute; config status; live key refusal; missing price refusal.
- `backends/user-backend/tests/test_legal_documents.py` - 16 passed.
- `backends/user-backend/tests/test_identity_verification.py` - 138 passed
  (parametrized over every authorized transition and every unauthorized
  source/target/actor combination; includes an end-to-end KYC upload/approve
  flow asserting that `kyc_audit_log` and log output contain no document
  bytes and no personal data).
- `frontends/user-frontend/src/tests/billingStatus.test.ts` and
  `legalDocuments.test.ts` - 14 passed.

Existing tests changed (intended behaviour changes): `test_billing_webhook.py`
and `test_billing_api.py` used `charge.refunded` as the example of an unhandled
event (now `customer.tax_id.created`) and listed the handled types;
`test_billing_entitlements.py::test_test_mode_key_rejected_in_production_unless_opted_in`
now expects a live key to be refused without `BILLING_LIVE_MODE_AUTHORIZED`.

Targeted run: billing (5 files), legal, identity, KYC hardening + logic,
auth hardening, onboarding step 1: **440 passed, 0 failed**. bot-backend
consumers of the shared entitlements (`test_plan_entitlement_gates`,
`test_connected_account_execution_safety`, `test_deployment_contract_step1`,
`platform_workers/test_platform_database`,
`trading_intelligence/test_step1_closing_synthetic`): **79 passed, 12 skipped**
(Postgres-only). Frontend: `npm test` 69 passed; `tsc --noEmit -p
tsconfig.app.json` clean; eslint on changed files 0 errors.

## What remains external (not done, cannot be done in code)

- **Counsel review.** Every published document is `DRAFT_PENDING_COUNSEL`.
  The seeded risk disclosure is a draft. Terms of Service, Privacy Policy,
  Data Processing Notice, Subscription Terms, Account Connection
  Authorization, Automated Execution Consent, Registration Consent and Country
  Eligibility text do not exist yet and must be written/approved by counsel,
  then published and approved via the admin API with the reviewer's name and
  reference. Withdrawal effects text is product description, not legal advice.
- **Live Stripe verification.** No live key, live webhook or real charge was
  used. Before launch: configure live Price IDs, the live webhook endpoint and
  secret, set `BILLING_LIVE_MODE_AUTHORIZED=true` deliberately, enable the
  Stripe Billing Portal configuration, and verify one real purchase, renewal,
  refund and dispute flow in the Stripe dashboard. Confirm the API version's
  charge/dispute payload shapes (charge -> invoice linkage changed in recent
  API versions; the handler falls back to the stored customer mapping).
- **Identity provider certification.** Only manual review exists. Any
  third-party provider needs an adapter, a contract/DPA and certification.
  The validity period (`IDENTITY_VERIFICATION_VALIDITY_DAYS`) is a policy
  decision, unset by default; if set, `expire_due_verifications` must be
  scheduled (nothing schedules it yet).
- **Country determinations.** Every country is UNDETERMINED. Determinations
  need a legal reference and are recorded via the admin API; nothing consumes
  `eligibility()` in a gate yet.
- **Gate wiring.** `identity_permits` and `has_current_acceptance` are exposed
  but not yet called by the live-trading / automated-execution gates.
- Set `LEGAL_EVIDENCE_HASH_KEY` in production (a built-in default key is used
  otherwise, which makes the IP / user-agent hashes guessable).
