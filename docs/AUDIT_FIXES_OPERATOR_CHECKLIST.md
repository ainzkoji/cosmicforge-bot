# After pulling the audit fixes: operator checklist

The October 2026 audit fixes make several settings mandatory. A production
service that is restarted on this code without them will refuse to start or
will refuse requests, on purpose. Do these steps before restarting anything.

## 1. Before you restart any service

| Setting | Where | What happens without it |
| --- | --- | --- |
| `SECRET_KEY` (32+ characters, not a placeholder, the SAME value in all three services) | user-backend, bot-backend, admin-backend | The service refuses to start in production |
| `CREDENTIAL_KEY` (different from `SECRET_KEY`) | user-backend | The service refuses to start in production |
| `SMTP_HOST`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM_EMAIL` | user-backend | Nobody can verify an account or reset a password |
| `KYC_ENCRYPTION_KEY`, `KYC_URL_SECRET` | user-backend | Every `/kyc` endpoint answers 503 |
| `BROKER_GATEWAY_ALLOWED_HOSTS` | bot-backend | A broker gateway on localhost or a private address is refused |

Generate keys on the server, never in chat or in the repository:

```
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Changing `SECRET_KEY` signs everyone out once. Do not change `CREDENTIAL_KEY`
or `KYC_ENCRYPTION_KEY` after they are in use: stored 2FA secrets and KYC data
are encrypted with them. Back them up with the database.

Every variable is described in `backends/*/.env.example` and
`deploy/env.production.example`.

## 2. Credentials that were public

Admin passwords were hard-coded in scripts in this public repository. The
scripts are deleted, but they remain in git history.

- Change the password of every admin account now.
- Create admins with `backends/user-backend/scripts/create_admin.py <email>`;
  it asks for the password and never prints it.
- Plans that were self-granted through the old test route lapse at their
  recorded period end. To revoke them now:
  `UPDATE subscriptions SET plan_id='plan_free', status='expired' WHERE provider IS NULL;`

## 3. Behaviour that changed

- **KYC is no longer auto-approved.** New submissions wait in the admin
  console (Compliance) until an admin approves them. Cases approved by the old
  automatic step are the `kyc_reviews` rows with `reviewer_type='system'`;
  decide whether to re-review them.
- **Free plan is 1 bot, 1 broker, no live trading**, enforced from the
  database. Existing extra brokers are left alone.
- **Login is rate limited** (5 failures per email and address, 20 per email,
  per 15 minutes) and returns one generic error.
- **Two-factor is enforced at login** for users who enabled it.
- **Emergency controls** are in the admin console (Bot Monitor): kill switch
  and flatten. They act on the production engine.
- **A stuck close or stop is retried.** A legacy pending close row is
  re-checked for 60 seconds after the first cycle on this code and then, if
  its order is proven absent and the position is still open, re-sent as a
  reduce-only market close. Review open rows in `cati_production_closes`
  before the first start.
- **Bot-backend routes need a token.** Local scripts that called
  `/api/admin/tradingview/*`, `/api/v1/brokers/*` or `/api/v1/shadow/*` without
  one now get 401.

## 4. Billing (when you are ready to charge)

1. In Stripe, create two products with four recurring prices and copy the
   price ids into `STRIPE_PRICE_PRO_MONTHLY`, `STRIPE_PRICE_PRO_YEARLY`,
   `STRIPE_PRICE_WHALE_MONTHLY`, `STRIPE_PRICE_WHALE_YEARLY`.
2. Add a webhook endpoint `https://<api-host>/api/billing/webhook` for
   `checkout.session.completed`, `invoice.paid`, `invoice.payment_failed`,
   `customer.subscription.updated`, `customer.subscription.deleted`, and put
   its signing secret in `STRIPE_WEBHOOK_SECRET`.
3. Set `STRIPE_SECRET_KEY` and `FRONTEND_URL`.

Until then checkout answers 503 and the webhook rejects every request. The
Stripe calls have unit tests but have not been exercised against Stripe: run
one test-mode purchase, renewal and cancellation end to end before going live.

## 5. Alerting and backups

`docs/VPS_TRADING_DEPLOYMENT.md` section 10 has the install commands for the
alert unit, the health-check timer, the off-site backup timer and the units
for the user and admin backends. Test delivery with
`python scripts/notify_failure.py --test`. Add an external uptime monitor: a
server that is down cannot report itself.

## 6. Not fixed by code

- The strategy has no demonstrated edge after costs. Keep
  `LIVE_ORDER_SUBMISSION_ENABLED=false` until forward demo results and the
  reserved holdout say otherwise.
- Terms of Service and Privacy Policy are placeholders that need a lawyer.
- The database is still one SQLite file and the engine one serial loop.
- Tests listed in `.github/known-test-failures.list` were failing before these
  changes and run as expected failures. Fix or delete them and remove the lines.
