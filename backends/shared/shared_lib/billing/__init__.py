"""Billing primitives shared by the user-backend and the bot-backend.

Only the standard library and ``sqlite3`` are used here, so both services (and
their tests) can import it without pulling in a web framework or the Stripe SDK.

* :mod:`shared_lib.billing.plans`        -- plan ids and their machine limits
* :mod:`shared_lib.billing.entitlements` -- database-backed plan/limit checks
* :mod:`shared_lib.billing.webhooks`     -- Stripe signature check + event handling
* :mod:`shared_lib.billing.schema`       -- additive schema, called from ``migrate()``
"""
