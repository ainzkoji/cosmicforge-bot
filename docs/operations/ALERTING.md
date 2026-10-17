# Alerting, on-call ownership and the status page (Section H, Step 8.5)

Status: implemented and tested on SQLite and PostgreSQL. **Nobody is on call yet.** Until an administrator
assigns named people in the admin console (`/admin/operations`), every rotation reports
**OPERATIONS OWNER REQUIRED**, the alerting system's own health is `CRITICAL`, and every alert routed to an
unassigned rotation raises a CRITICAL `OPERATIONS_OWNER_REQUIRED` configuration alert. No person, phone number or
e-mail address was invented for this document or for the code.

## Where things live

| Piece | Code |
|---|---|
| Engine (categories, severities, routing, dedup, escalation, ack/resolve, event log, delivery + retries) | `backends/shared/shared_lib/operations/alerting.py` |
| On-call rotations and people | `backends/shared/shared_lib/operations/oncall.py` |
| Platform evaluators, sinks, periodic tick (`python -m app.ops.alerting`) | `backends/bot-backend/app/ops/alerting.py` |
| Public status page payload and admin incidents | `backends/shared/shared_lib/operations/status_page.py` |
| Secret detection (tickets, notices, backups) | `backends/shared/shared_lib/operations/secrets_scan.py` |
| Tables (platform DB migrations 30-32) | `backends/shared/shared_lib/persistence/sql/operations_schema.py` |
| Admin + public API | `backends/user-backend/app/api/operations.py` |
| Admin UI | `frontends/admin-frontend/src/pages/admin/Operations.tsx` |
| Public UI | `frontends/user-frontend/src/pages/Status.tsx` (`/status`) |

Everything is stored in the platform database (`PLATFORM_DATABASE_URL`, falling back to `DATABASE_URL`). Set
`PLATFORM_DATABASE_URL` explicitly (absolute path for SQLite) for the user-backend AND the bot-backend operations
tick, so both processes open the same database: a relative legacy `DATABASE_URL` is resolved differently by the
legacy layer and by the platform layer. Apply
the migrations with `python -m shared_lib.persistence.sql migrate` as part of the deploy; the API never migrates.
Without the operations tables the admin endpoints answer `503 OPERATIONS_SCHEMA_MISSING`, and the public page
reports `data_available: false` with every non-portal component `DEGRADED` -- never a fabricated "all operational".

## Categories, default severity and routing

| Category | Severity | Rotation | Public component |
|---|---|---|---|
| EXCHANGE_EXECUTION_UNCERTAINTY | CRITICAL | trading-operations | Exchange connectivity (aggregated) |
| UNPROTECTED_POSITION | CRITICAL | trading-operations | Trading engine |
| RECONCILIATION_MISMATCH | HIGH | trading-operations | Trading engine |
| KILL_SWITCH_ACTIVATED | HIGH | trading-operations | Trading engine |
| WORKER_OUTAGE | CRITICAL | platform-infrastructure | Trading engine |
| LEASE_TAKEOVER_FAILURE | CRITICAL | platform-infrastructure | Trading engine |
| DATABASE_UNAVAILABLE | CRITICAL | platform-infrastructure | Trading engine |
| QUEUE_BACKLOG | HIGH | platform-infrastructure | Trading engine |
| API_OUTAGE | CRITICAL | platform-infrastructure | Portal |
| STALE_INSTRUMENT_RULES | HIGH | market-data | Market data |
| STALE_MARKET_DATA | HIGH | market-data | Market data |
| AUTHENTICATION_ANOMALY | HIGH | security | (never public) |
| BILLING_WEBHOOK_FAILURE | HIGH | billing | (never public) |
| BACKUP_FAILURE | HIGH | platform-infrastructure | (never public) |
| DISK_GROWTH | MEDIUM (fill level: HIGH) | platform-infrastructure | (never public) |
| EXCESSIVE_LATENCY | MEDIUM | platform-infrastructure | Portal |
| EXPIRED_EXCHANGE_CERTIFICATION | HIGH | research-governance | (never public) |
| GOVERNANCE_VIOLATION_ATTEMPT | HIGH | security | (never public) |
| OPERATIONS_OWNER_REQUIRED | CRITICAL | operations-owner | (never public) |
| ALERT_DELIVERY_FAILURE | HIGH | operations-owner | Notifications |

A caller may override the severity of a single alert (`severity=`); the category's rotation is fixed.

## Deduplication

`fingerprint = sha256(category, key)`. At most one unresolved alert exists per fingerprint (partial unique
index). A repeat signal increments `occurrences`; within the dedup window (default 10 minutes since the last
notification) it is silent, after it an `OPEN` alert is re-notified (`REMINDER`). An acknowledged alert is
never reminded. A resolved alert that fires again is a new alert.

## Escalation

Unacknowledged alerts escalate by minutes since first seen (database clock):

| Severity | Level 1 (secondary) | Level 2 (escalation owner) |
|---|---|---|
| CRITICAL | 5 min | 15 min |
| HIGH | 15 min | 60 min |
| MEDIUM | 60 min | -- |
| LOW | -- | -- |

A level without an assigned person falls back to the nearest lower level that has one; level 2 with no
escalation owner in the rotation goes to the `operations-owner` primary. Acknowledging stops escalation.
These intervals are engineering defaults, **not** an agreed response-time commitment: the operations owner must
confirm or change them (`AlertEngine(escalation_minutes=...)`).

## Acknowledge and resolve

Admin API (`require_admin`), actor recorded as `admin:<id>`:

    POST /api/admin/operations/alerts/{id}/acknowledge   {note?}
    POST /api/admin/operations/alerts/{id}/resolve       {note}   (a note is required)

Evaluators resolve their own alert when the condition clears (actor `evaluator:<name>`). Every transition,
dedup, reminder, escalation and delivery outcome is a row in `ops_alert_events`, which the database refuses to
update or delete.

## Delivery

Sinks (`AlertSink.send(message)`; a raise is a failure):

* `JobQueueSink` -- enqueues a `NOTIFICATION` job on the platform JobQueue, idempotency key
  `ops-alert-delivery:<delivery_id>`; a worker runs `notification_job_handler`, which sends through the
  notify channels and lets the queue retry/dead-letter;
* `NotifyChannelsSink` -- the existing `scripts/notify_failure.py` channels (webhook / Telegram / e-mail from
  `/etc/cosmicforge/alerts.env`, see `deploy/alerts.env.example`), used directly so an alert still reaches a
  channel when the workers are down.

Each (alert, reason, level, sink) is one `ops_alert_deliveries` row. Failures retry after 30 s, 1, 5, 15, 30 min;
after 6 attempts the delivery is `ABANDONED` and `ALERT_DELIVERY_FAILURE` is raised. If the platform database
itself is down, `evaluate_database` sends the alert straight to the sinks without storing it.

## Evaluators (signals that exist today)

| Evaluator | Signal | Default bound |
|---|---|---|
| `evaluate_queue_backlog` | `JobQueue.backlog()` | oldest due job > 5 min; any dead letter |
| `evaluate_worker_heartbeats` | `platform_workers.heartbeat_at` | ACTIVE worker silent > 90 s |
| `evaluate_lease_takeovers` | `platform_account_assignments` | ACTIVE account unowned > 60 s |
| `evaluate_database` | `Database.health()` | not ready |
| `evaluate_disk_growth` | volume fill / path growth | > 85 % full; > 2 GiB/h |
| `evaluate_backup_age` | newest verified backup manifest | none, or > 36 h |

Not yet wired to an evaluator (the categories exist and producers can call `raise_alert` directly):
exchange execution uncertainty, unprotected positions, reconciliation mismatches, kill-switch events, API
outage, stale instrument rules / market data, authentication anomalies, billing webhook failures, latency,
certification expiry, governance violation attempts. Hooking each producer is follow-up work.

One tick: `python -m app.ops.alerting --backup-dir /var/backups/cosmicforge --disk-path /var/lib/cosmicforge`
(exit 2 while the alerting system reports a CRITICAL finding such as OPERATIONS_OWNER_REQUIRED). No systemd
timer was added for it: there is no VPS access yet.

## On-call ownership

Rotations are fixed in code (`ROTATIONS`): `operations-owner`, `trading-operations`, `platform-infrastructure`,
`market-data`, `security`, `billing`, `research-governance`. People exist only when an admin adds them:

    GET  /api/admin/operations/oncall
    POST /api/admin/operations/oncall/members   {rotation_key, level 0-2, person_name, contact_channel, contact_address}
    POST /api/admin/operations/oncall/members/{member_id}/remove
    GET  /api/admin/operations/oncall/history   (append-only)

## Public status page

`GET /api/public/status` (no authentication, cached 10 s) returns only: `generated_at`, `overall`,
`data_available`, `notice`, `components[{key,name,status}]` for Portal, Trading engine, Market data, Exchange
connectivity and Notifications (`OPERATIONAL` / `DEGRADED` / `OUTAGE` / `MAINTENANCE`), and incident notices
(`title, status, impact, components, started/updated/resolved, updates[{status,message,at}]`).

* Only admins declare `OUTAGE` or `MAINTENANCE` (incidents). Alerts can only move a component to `DEGRADED`,
  and only platform-scoped CRITICAL/HIGH alerts do; account-scoped exchange alerts count in aggregate (3+
  distinct accounts) and never reveal how many or whose.
* Notices are refused if they contain secrets, IP addresses, internal host names, e-mail addresses or
  account/tenant/user/worker identifiers.

Admin incident endpoints: `GET/POST /api/admin/operations/status/incidents`,
`POST .../{id}/updates {status, message, impact?, components?}`, `POST .../{id}/resolve {message}`.
