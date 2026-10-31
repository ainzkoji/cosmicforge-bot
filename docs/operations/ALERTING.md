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
| Producers over the runtime database (the 20 conditions below) | `backends/bot-backend/app/ops/alert_producers.py` |
| Tick ownership lease | `backends/shared/shared_lib/operations/tick_lease.py` |
| Tick scheduling | `deploy/systemd/cosmicforge-ops-tick.{service,timer}`; `ops-tick` in `deploy/staging/docker-compose.staging.yml` |
| Public status page payload and admin incidents | `backends/shared/shared_lib/operations/status_page.py` |
| Secret detection (tickets, notices, backups) | `backends/shared/shared_lib/operations/secrets_scan.py` |
| Tables (platform DB migrations 30-34) | `backends/shared/shared_lib/persistence/sql/operations_schema.py` |
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
| KILL_SWITCH_ACTIVATED | HIGH (family switch MEDIUM; stalled/incomplete flatten CRITICAL) | trading-operations | Trading engine |
| EXCHANGE_CONNECTIVITY_LOSS | HIGH | trading-operations | Exchange connectivity (aggregated when account-scoped) |
| RISK_BRAKE_ACTIVATED | MEDIUM (stop / equity exhausted: HIGH) | trading-operations | (never public) |
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
| EXPIRED_EXCHANGE_CERTIFICATION | HIGH (expiring within 14 days: MEDIUM) | research-governance | (never public) |
| EXPIRED_FAMILY_CERTIFICATION | HIGH | research-governance | (never public) |
| GOVERNANCE_VIOLATION_ATTEMPT | HIGH | security | (never public) |
| OPERATIONS_OWNER_REQUIRED | CRITICAL | operations-owner | (never public) |
| ALERT_DELIVERY_FAILURE | HIGH | operations-owner | Notifications |

A caller may override the severity of a single alert (`severity=`); the category's rotation is fixed.

## Deduplication

`fingerprint = sha256(category, key)`. At most one unresolved alert exists per fingerprint (partial unique
index). A repeat signal increments `occurrences`; within the dedup window (default 10 minutes since the last
notification) it is silent, after it an `OPEN` alert is re-notified (`REMINDER`). An acknowledged alert is
never reminded. A resolved alert that fires again is a new alert -- except an *event* alert (`once=True`: a kill
switch activation, an emergency flatten, a risk-brake activation, an expired certification run), keyed by its
durable record: once resolved (by its evaluator or a human) the same record never raises it again. The event log
records the first repeat after each notification (`DEDUPLICATED`); every repeat is still counted in
`occurrences` / `last_seen_at`, so an evaluator confirming a condition every minute does not write a row a minute.

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

Evaluators resolve their own alert when the condition clears (actor `evaluator:<name>`) and send a **RECOVERED**
notification (`[RECOVERED] [SEVERITY] CATEGORY: title`) to the rotation and escalation level the alert reached --
only if someone was notified of it. A human resolution sends none. Every transition, reminder, escalation,
suppression and delivery outcome is a row in `ops_alert_events`, which the database refuses to update or delete.

## Maintenance windows (suppression)

`AlertEngine.add_suppression(category=None|<category>, ends_at_ms, reason, actor, starts_at_ms=None)` /
`cancel_suppression(id, actor)` (table `ops_alert_suppressions`, migration 34; at most 24 h per window). Inside a
window an alert of a suppressed category is still raised and stored, the delivery is replaced by a `SUPPRESSED`
event naming the window, and it does not escalate. After the window the next signal of a still-open alert is
delivered as a `REMINDER` and a due escalation proceeds. **Never suppressible**: UNPROTECTED_POSITION,
EXCHANGE_EXECUTION_UNCERTAINTY, OPERATIONS_OWNER_REQUIRED, ALERT_DELIVERY_FAILURE. There is no admin API endpoint
for windows yet (engine call only).

## Delivery

Sinks (`AlertSink.send(message)`; a raise is a failure):

* `JobQueueSink` -- enqueues a `NOTIFICATION` job on the platform JobQueue, idempotency key
  `ops-alert-delivery:<delivery_id>`; a worker runs `notification_job_handler`, which sends through the
  notify channels and lets the queue retry/dead-letter;
* `NotifyChannelsSink` -- the existing `scripts/notify_failure.py` channels (webhook / Telegram / e-mail from
  `/etc/cosmicforge/alerts.env`, see `deploy/alerts.env.example`), used directly so an alert still reaches a
  channel when the workers are down.

Each (alert, reason, level, sink) is one `ops_alert_deliveries` row. Failures retry after 30 s, 1, 5, 15, 30 min;
after 6 attempts the delivery is `ABANDONED` and `ALERT_DELIVERY_FAILURE` is raised. A sink that does not return
within `send_timeout_s` (default 30 s) counts as a failed attempt and is retried; the tick goes on (the call is
left to finish on a daemon thread: delivery is at-least-once). Title and detail are redacted again
(`redaction.redact_text`, `secrets_scan`, e-mail masking) when the message is built for a sink, whatever wrote the
row. If the platform database itself is down, `evaluate_database` sends the alert straight to the sinks without
storing it. Trading code never calls a sink: every producer below is an evaluator in the operations tick.

## Alert producer matrix (Step 8.5, F1)

Every producer is an **evaluator** in the operations tick: it reads durable state (legacy runtime SQLite database
`DATABASE_URL`, platform database, backup markers), raises through the engine and resolves its own alerts when the
condition clears (`publish` -> `resolve_absent`, RECOVERED notification). None is called from the order path; none
writes a trading table. Customer identifiers never appear in a title or detail (`pseudonym()`; the raw account id
is only in `account_ref`, shown in the admin console); account-scoped alerts count on the public page only in
aggregate (3+ accounts) and never name anyone. A missing table/column is "no evidence"; any other read error is an
evaluator error in the tick result (and nothing is resolved from a database that could not be read).

| # | Condition | Durable source | Evaluator (`source`) | Category / severity | Test |
|---|---|---|---|---|---|
| 1 | Worker crash | `platform_workers.heartbeat_at` (platform); `runtime_ownership.heartbeat_at` (> 150 s, unreleased); `cati_platform_cycles` running with heartbeat > 120 s | `evaluate_worker_heartbeats` (`worker_heartbeats`); `evaluate_runtime_workers` (`runtime_workers`) | WORKER_OUTAGE / CRITICAL | `test_worker_heartbeat_evaluator`; `test_worker_crash_runtime_owner_and_stalled_platform_cycle` |
| 2 | Failed lease takeover | `platform_account_assignments` (unowned > 60 s); `cati_account_leases` expired > 120 s, never taken over | `evaluate_lease_takeovers` (`lease_takeovers`); `evaluate_account_leases` (`account_leases`) | LEASE_TAKEOVER_FAILURE / CRITICAL (runtime leases HIGH) | `test_lease_takeover_failure_evaluator`; `test_failed_lease_takeover_on_runtime_account_leases` |
| 3 | Queue backlog | `JobQueue.backlog()` (`platform_jobs`) | `evaluate_queue_backlog` (`queue_backlog`) | QUEUE_BACKLOG / HIGH | `test_queue_backlog_evaluator_raises_and_resolves` |
| 4 | Database outage | `Database.health()` (platform); read-only open of the runtime SQLite file | `evaluate_database` (`database_health`, keys `platform` / `runtime`) | DATABASE_UNAVAILABLE / CRITICAL | `test_database_evaluator_and_unstored_fallback`; `test_database_outage_of_the_runtime_database` |
| 5 | Exchange connection loss | `cati_production_state` READ_FAILED; `cati_user_stream_state` DISCONNECTED/STALE/owner silent (was connected); `cati_rate_budgets` ban or closure > 2 min (both held 2 min) | `evaluate_exchange_connectivity` (`exchange_connectivity`) | EXCHANGE_CONNECTIVITY_LOSS / HIGH | `test_exchange_connection_loss_accounts_streams_and_venue_bans` |
| 6 | Stale market data | `cati_residual_tracker.heartbeat_at` (> 360 s); `cati_feed_updates` (last FAILED/ABANDONED, RUNNING > 6 h, no success for 36 h) | `evaluate_market_data` (`market_data`) | STALE_MARKET_DATA / HIGH | `test_stale_market_data_collector_and_daily_feed` |
| 7 | Stale instrument rules | `cati_instrument_rule_refreshes` (no success within the rules cache max age 24 h; 3+ failures: MEDIUM; venues refreshed in the last 48 h); `venue_instruments` catalog of connected venues (MEDIUM) | `evaluate_instrument_rules` (`instrument_rules`) | STALE_INSTRUMENT_RULES / HIGH | `test_stale_and_failing_instrument_rules` |
| 8 | Reconciliation mismatch | latest `cati_user_stream_reconciliations` FAILED or differences; latest `broker_transfer_reconciliations` unresolved; `reconciliation_events` / `data_quality_events` POSITION_QTY_MISMATCH in the last hour | `evaluate_reconciliation` (`reconciliation`) | RECONCILIATION_MISMATCH / HIGH | `test_reconciliation_mismatch_streams_transfers_and_position_corrections` |
| 9 | Unknown order outcome | `cati_portfolio_reservations` RESOLUTION_PENDING; latest `cati_execution_attempts` SUBMIT_UNKNOWN/PENDING_SUBMIT (> 60 s); `cati_execution_intents` UNKNOWN (SUBMITTING > 2 min); `cati_reservation_ledger` UNKNOWN; `pending_entries` SUBMIT_UNKNOWN; `platform_order_intents` SUBMITTING > 2 min (platform) | `evaluate_execution_uncertainty` (`execution_uncertainty`), one alert per account | EXCHANGE_EXECUTION_UNCERTAINTY / CRITICAL | `test_unknown_order_outcomes_from_every_execution_ledger` |
| 10 | Missing protective stop | `cati_production_protection_uncertainty` (> 60 s); `cati_protection_state` UNCERTAIN/UNDER_PROTECTED/ABSENT, PENDING > 2 min | `evaluate_protection` (`protection`), per account | UNPROTECTED_POSITION / CRITICAL | `test_positions_without_confirmed_protection` |
| 11 | Emergency flatten | `cati_emergency_requests` (one alert per request id, raised once even if it finished between ticks; stalled > emergency TTL and incomplete outcomes: CRITICAL) | `evaluate_emergency_flattens` (`emergency_flatten`) | KILL_SWITCH_ACTIVATED / HIGH (CRITICAL) | `test_emergency_flatten_is_raised_once_per_request_and_resolves_when_complete` |
| 12 | Platform kill switch | `cati_governance_controls` KILL_NEW_CATI_ENTRIES (GLOBAL or account), `cati_family_events` FAMILY_KILL_SWITCH_ON/OFF; once per control record, resolved on release | `evaluate_kill_switches` (`kill_switch`) | KILL_SWITCH_ACTIVATED / HIGH (family MEDIUM) | `test_kill_switch_activations_global_account_family` |
| 13 | Risk-brake activation | `cati_brake_state` (active brake or latched stop), once per activation | `evaluate_risk_brakes` (`risk_brakes`) | RISK_BRAKE_ACTIVATED / MEDIUM (stop, equity exhausted: HIGH) | `test_risk_brake_activation` |
| 14 | Family certification expiry | `FamilyRegistry` (governance stage from `cati_family_events`, certification re-read from the research register): stage >= M6 without CERTIFIED. Families have no calendar expiry; all are at M0 today, so this is silent | `evaluate_family_certification` (`family_certification`) | EXPIRED_FAMILY_CERTIFICATION / HIGH | `test_family_certification_expiry_only_for_families_with_order_authority`; `..._against_the_real_registry_raises_nothing_at_m0` |
| 15 | Venue certification expiry | `certification_matrix` over `cati_adapter_cert_events` (EXPIRED: HIGH, once per run; expiring within 14 days: MEDIUM; evidence integrity failure: HIGH) | `evaluate_venue_certification` (`venue_certification`) | EXPIRED_EXCHANGE_CERTIFICATION / HIGH | `test_venue_certification_expired_expiring_and_integrity`; `..._against_the_real_register_...` |
| 16 | Billing failures | `billing_events` (`unmatched:*` in 24 h; stored but unprocessed > 10 min; 5+ `invoice.payment_failed` in 24 h: MEDIUM); `billing_payment_reviews` OPEN (MEDIUM); `billing_webhook_rejections` (counted per minute by the user-backend webhook route, reason only): any NOT_CONFIGURED in 1 h, 10+ SIGNATURE_INVALID in 1 h | `evaluate_billing` (`billing`) | BILLING_WEBHOOK_FAILURE / HIGH | `test_billing_failures`; `test_rejected_webhook_deliveries_raise_billing_alerts` |
| 17 | Authentication anomalies | `auth_audit_log` (15 min): refresh-token reuse (admin: CRITICAL), 3+ admin MFA failures, 50+ failed logins; `login_attempts`: an admin key at the lockout threshold, 3+ user lockouts. Counts only | `evaluate_authentication` (`authentication`) | AUTHENTICATION_ANOMALY / HIGH | `test_authentication_anomalies_report_counts_never_identities` |
| 18 | Backup failure | newest verified manifest (none / > 36 h); `backup-status.json` and `offsite-status.json` markers written by the backup scripts on every run (last run failed; no verified off-site copy for 36 h) | `evaluate_backup_age` (`backup_age`); `evaluate_backup_status` (`backup_status`) | BACKUP_FAILURE / HIGH | `test_backup_age_evaluator`; `test_backup_failure_markers`; `test_backup_and_offsite_scripts_record_durable_failure_markers` |
| 19 | Excessive disk growth | volume fill and growth of `--disk-path`; the previous sample persists in the tick lease state | `evaluate_disk_growth` (`disk_growth`) | DISK_GROWTH / MEDIUM (fill: HIGH) | `test_disk_growth_evaluator`; `test_tick_state_survives_a_restart_disk_samples_and_pending_holds` |
| 20 | Unusual execution latency | `cati_execution_events` VENUE_AFTER - VENUE_BEFORE (last 5,000 events, 15 min); `pending_entries` confirmed - submitted. p95 > 5 s (3+ samples) or any > 30 s. (CATI attempts stamp submit and ack with one cycle time: no latency there) | `evaluate_execution_latency` (`execution_latency`) | EXCESSIVE_LATENCY / MEDIUM | `test_unusual_execution_latency` |
| 21 | Governance violation attempts | `cati_execution_events` GATE rows and `cati_execution_evaluations.reason` (1 h) carrying a code only a bypass attempt produces: a non-authoritative family, submission, governance or order-spreading gate, `TEST_ADAPTER_NOT_ALLOWED`, `FAMILY_RULE_HASH_MISMATCH`, a tampered, mismatched or future-dated decision, a LIVE entry at the go-live gate. Routine M0 refusals never alert. One alert per code; accounts counted, never named | `evaluate_governance_violations` (`governance`) | GOVERNANCE_VIOLATION_ATTEMPT / HIGH | `test_governance_bypass_attempts_alert_and_routine_m0_refusals_do_not` |
| 22 | API outage | each `--health-url name=url` (or `OPS_HEALTH_PROBES`) answering anything but 200 for 2 min. The systemd tick probes the user, admin and bot backends on loopback; the staging tick probes user-backend, admin-backend, bot-api and bot-trading | `evaluate_api_health` (`api_health`) | API_OUTAGE / CRITICAL | `test_an_api_that_stops_answering_raises_api_outage_after_the_hold`; `test_the_deployed_ticks_probe_every_api` |

All producer tests run on SQLite and PostgreSQL (`tests/platform_workers/test_operations_alert_producers.py`);
`test_every_producer_runs_against_the_real_runtime_schema_without_error` runs every evaluator against a freshly
migrated runtime database, and `test_secret_shaped_values_in_producer_detail_never_reach_a_sink` proves an
`api_key=` value, a bearer token, an e-mail and the account id in a producer's input reach no sink.

**Closed on 31 October 2026:** rejected Stripe webhook deliveries are counted (`billing_webhook_rejections`), and API_OUTAGE and GOVERNANCE_VIOLATION_ATTEMPT have producers (rows 21 and 22). `scripts/vps_health_check.py` and its systemd/container alert stay as the second, independent health signal.

## The operations tick

One tick = every evaluator above (each isolated: a failing one is reported, the rest run) -> escalation ->
delivery. It is owned by exactly one process at a time through `ops_tick_leases` (migration 33):

* lease by the **database clock**, `fencing_token` incremented on every change of holder, expiry (default
  `3 x interval`, at least 180 s); a holder that died is **taken over after expiry** (`takeovers` counted); a
  holder that was taken over cannot record its result (fenced); a clean stop (SIGTERM) releases the lease;
* `last_tick_started_at` / `last_tick_finished_at` / `last_tick_owner` / `last_tick_outcome` (`OK`, `DEGRADED` =
  some evaluator failed, `ERROR`) / `last_tick_result` are persisted; `state` carries the disk samples and the
  "first seen" times of held conditions to whichever process runs the next tick;
* one-shot runs are paced (`TICK_NOT_DUE` within half an interval of the last tick), so timers on several hosts
  give one tick per interval. Ticks are idempotent anyway (fingerprint dedup, delivery claims).

Topologies:

* **systemd** (VPS): `deploy/systemd/cosmicforge-ops-tick.timer` (every minute, `Persistent=true`) starts
  `cosmicforge-ops-tick.service` (oneshot): `python -m app.ops.alerting --dotenv backends/bot-backend/.env
  --alerts-env /etc/cosmicforge/alerts.env --backup-dir /var/backups/cosmicforge --disk-path ...`. Exit 0: ran
  healthy, or not this host's turn; exit 2: ran and the alerting system reports a CRITICAL finding of its own
  (`OPERATIONS_OWNER_REQUIRED` today) -- `SuccessExitStatus=2`, the finding is already an alert; exit 1: cannot run
  (platform database unreachable, bad configuration) -> unit failure -> `OnFailure=cosmicforge-alert@`, the
  out-of-band channel.
* **containers** (staging): service `ops-tick` in `deploy/staging/docker-compose.staging.yml`:
  `python -m app.ops.alerting --loop --interval 60 --disk-path /var/lib/cosmicforge` (one line of JSON per tick;
  SIGTERM ends it after the current tick and releases the lease). `deploy/staging/lib.sh` restarts it on the new
  image after the trading roll (not gating); `restore.sh` stops it with the other platform-database clients.

Escalation intervals and the dedup window are engineering defaults until the operations owner decides:
`OPS_ESCALATION_MINUTES` (JSON, merged over the defaults, must increase) and `OPS_DEDUP_WINDOW_MINUTES` in the
tick's environment.

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
