# Incident runbooks (Section H, Step 8.5)

One runbook per alert category (`ALERTING.md`). Each lists **detection** (what raises it), **containment**
(stop the damage first), **recovery** and **escalation**. On-call people: **OPERATIONS OWNER REQUIRED** -- no
rotation has an assigned person yet; until one is assigned, "escalate" means the alert reaches the configured
channels only.

General rules for every incident:

* Acknowledge the alert in `/admin/operations` before working on it (stops escalation; records who).
* Never paste credentials, API keys, `.env` content or customer identifiers into alerts, tickets, chat or the
  public status page. Notices with IPs, host names, e-mail addresses or account ids are refused anyway.
* If customers are affected, post a status-page incident (`/admin/operations` -> Public status page incidents)
  within 15 minutes, update it at least every 30 minutes, resolve it when done.
* Resolve the alert with a note saying what was done. The event log is append-only.
* When in doubt about exchange exposure: engage the kill switch first (`/admin` emergency controls), investigate
  second. A kill switch stops new entries; it does not close positions.

---

## EXCHANGE_EXECUTION_UNCERTAINTY (CRITICAL, trading-operations)

* **Detection**: `evaluate_execution_uncertainty`, one alert per account -- reservations RESOLUTION_PENDING,
  attempts SUBMIT_UNKNOWN, CATI intents UNKNOWN / SUBMITTING too long, reservation ledger UNKNOWN, legacy pending
  entries SUBMIT_UNKNOWN, platform `OrderIntents` left `SUBMITTING`.
* **Containment**: do NOT resubmit by hand. Engage the account kill switch if more than one intent is affected.
* **Recovery**: let recovery read the venue by client order id (`OrderIntents._recover`); confirm on the venue
  UI. Only after the venue says the order does not exist and the settle window has passed is a resubmission
  allowed (same client order id).
* **Escalation**: unresolved after 15 min, or any position size unknown -> escalation owner.

## UNPROTECTED_POSITION (CRITICAL, trading-operations)

* **Detection**: `evaluate_protection` -- protection UNKNOWN in `cati_production_protection_uncertainty`, or
  `cati_protection_state` UNCERTAIN / UNDER_PROTECTED / ABSENT (PENDING > 2 min), per account.
* **Containment**: place the protection (or flatten via emergency controls) within minutes; engage the account
  kill switch.
* **Recovery**: find why protection failed (instrument rules, rejected stop, venue error); fix; resume.
* **Escalation**: not protected within 5 min -> secondary; 15 min -> escalation owner.

## RECONCILIATION_MISMATCH (HIGH, trading-operations)

* **Detection**: `evaluate_reconciliation` -- the latest user-stream reconciliation failed or found differences,
  a transfer reconciliation left transfers unresolved, position corrections / quantity mismatches in the last hour.
* **Containment**: kill switch for the affected account; no new entries.
* **Recovery**: re-run reconciliation; the venue is the source of truth for exposure; record any correction as
  an explicit ledger entry, never by editing history.
* **Escalation**: mismatch in more than one account -> treat as platform incident; status page "Trading engine
  DEGRADED".

## KILL_SWITCH_ACTIVATED (HIGH, trading-operations)

* **Detection**: `evaluate_kill_switches` -- global / account (`cati_governance_controls`) or family
  (`cati_family_events`) kill switch engaged, one alert per activation, resolved on release;
  `evaluate_emergency_flattens` -- every emergency flatten request (`cati_emergency_requests`), raised once;
  a stalled or incomplete flatten is CRITICAL (positions may still be open).
* **Containment**: confirm it was intended (emergency request log names the operator). An incomplete flatten:
  check every affected account's positions at the venue now.
* **Recovery**: release only after the cause is fixed and recorded; release is a separate audited action.
* **Escalation**: an unexplained activation -> security rotation as well.

## WORKER_OUTAGE (CRITICAL, platform-infrastructure)

* **Detection**: `evaluate_worker_heartbeats` -- an ACTIVE worker silent > 90 s.
* **Containment**: other workers take over its accounts after the assignment TTL (fencing tokens prevent double
  ownership). Check `LEASE_TAKEOVER_FAILURE` next.
* **Recovery**: `systemctl status`/journal of the worker; restart; if the host is gone, mark it STOPPED.
* **Escalation**: no worker alive -> status page "Trading engine OUTAGE"; escalation owner.

## LEASE_TAKEOVER_FAILURE (CRITICAL, platform-infrastructure)

* **Detection**: `evaluate_lease_takeovers` -- ACTIVE accounts unowned for > 60 s.
* **Containment**: these accounts are not being processed (no protection maintenance by the platform). Check
  each account's exchange exposure manually; kill switch if needed.
* **Recovery**: start a worker / fix the one that cannot claim (database reachable? worker STATE ACTIVE?).
* **Escalation**: immediate to escalation owner if any affected account holds positions.

## DATABASE_UNAVAILABLE (CRITICAL, platform-infrastructure)

* **Detection**: `evaluate_database` -- readiness probe fails. When the alert store itself is down, the alert
  is sent directly to the channels (`RAISED_UNSTORED`).
* **Containment**: workers stop mutating when they cannot verify fencing; engage the kill switch from the
  bot-backend if reachable.
* **Recovery**: disk full? (`DISK_GROWTH`) service down? pool exhausted? Restore from backup only if the data is
  damaged (`BACKUP_AND_RESTORE.md`).
* **Escalation**: > 5 min -> escalation owner; status page.

## QUEUE_BACKLOG (HIGH, platform-infrastructure)

* **Detection**: `evaluate_queue_backlog` -- oldest due job > 5 min, or dead letters.
* **Containment**: identify the category (ORDER backlog is urgent: entries are stale -- engage the kill switch).
* **Recovery**: scale/restart workers; for DEAD jobs read `failure_reason`, fix the cause, then
  `requeue_dead(operator, reason)`.
* **Escalation**: ORDER or RECONCILIATION backlog > 15 min -> escalation owner.

## API_OUTAGE (CRITICAL, platform-infrastructure)

* **Detection**: health checks (`scripts/vps_health_check.py`) fail for a public API.
* **Containment**: trading continues without the portal; post a status-page incident (Portal OUTAGE).
* **Recovery**: journal of the service; nginx; certificates; restart.
* **Escalation**: > 15 min -> escalation owner.

## STALE_INSTRUMENT_RULES (HIGH, market-data)

* **Detection**: `evaluate_instrument_rules` -- no successful rules refresh within the rules cache max age (24 h),
  or 3+ refresh failures in a row (MEDIUM); a stale instrument catalog of a connected venue (MEDIUM).
* **Containment**: entries on affected symbols must be refused (the engine fails closed on stale rules).
* **Recovery**: run the rules refresh; check venue API availability and rate limits.
* **Escalation**: > 1 h -> escalation owner.

## STALE_MARKET_DATA (HIGH, market-data)

* **Detection**: `evaluate_market_data` -- the hourly collector silent > 360 s; the daily-bar feed's last update
  failed / stuck, or no successful update for 36 h.
* **Containment**: no new decisions on stale data (fail closed); status page "Market data DEGRADED" if broad.
* **Recovery**: feed service journal; venue status; backfill only through the governed feed.
* **Escalation**: > 30 min -> escalation owner.

## AUTHENTICATION_ANOMALY (HIGH, security)

* **Detection**: `evaluate_authentication` (15 min window, counts only) -- refresh-token reuse (admin: CRITICAL),
  admin MFA failures, admin lockout threshold reached, several user lockouts, failed-login bursts.
* **Containment**: block the source at nginx if it is an attack; force sign-out of affected sessions.
* **Recovery**: review `auth_audit_log`; notify affected users through the normal channel.
* **Escalation**: suspected account takeover or admin-account involvement -> escalation owner immediately.

## BILLING_WEBHOOK_FAILURE (HIGH, billing)

* **Detection**: `evaluate_billing` -- provider events that could not be matched, events stored but never
  processed, open refund/dispute reviews (MEDIUM), a burst of failed payments (MEDIUM). Signature rejections are
  only logged by the API today (not an alert).
* **Containment**: entitlements fail closed; do not grant plans by hand without an audit record.
* **Recovery**: check the webhook secret configuration (never paste it anywhere); replay events from the
  provider dashboard after the fix.
* **Escalation**: > 4 h -> escalation owner.

## BACKUP_FAILURE (HIGH, platform-infrastructure)

* **Detection**: `evaluate_backup_age` -- no verified backup, or newest > 36 h; `evaluate_backup_status` -- the
  last local or off-site run failed (`backup-status.json` / `offsite-status.json` in the backup directory), or no
  verified off-site copy for 36 h; failed backup units (`OnFailure=`).
* **Containment**: none needed immediately; the risk is data loss on the next failure.
* **Recovery**: `systemctl status cosmicforge-db-backup cosmicforge-offsite-backup`; disk space; rerun; run
  `scripts/restore_drill.py` once fixed.
* **Escalation**: > 48 h without a verified off-site copy -> escalation owner.

## DISK_GROWTH (MEDIUM; fill level HIGH, platform-infrastructure)

* **Detection**: `evaluate_disk_growth` -- volume > 85 % full or growth > 2 GiB/h.
* **Containment**: prevent a full disk (SQLite writes fail at 100 %): pause non-essential jobs (research
  downloads, verbose logging).
* **Recovery**: prune old local backups (keep the newest verified), rotate logs, add space. Never delete
  database files or WAL files by hand.
* **Escalation**: > 95 % -> escalation owner.

## EXCESSIVE_LATENCY (MEDIUM, platform-infrastructure)

* **Detection**: `evaluate_execution_latency` -- venue round trips (venue journal) or entry confirmations with
  p95 > 5 s (3+ samples) or any call > 30 s in 15 min.
* **Containment**: if trading cycles are late, entries are stale -- consider the kill switch.
* **Recovery**: database pool stats (`Database.health()`), slow queries, host load.
* **Escalation**: persistent > 1 h -> secondary.

## EXPIRED_EXCHANGE_CERTIFICATION (HIGH, research-governance)

* **Detection**: `evaluate_venue_certification` -- an adapter certification EXPIRED (90 days, version change),
  expires within 14 days (MEDIUM), or its evidence no longer verifies.
* **Containment**: the venue loses execution authority under governance; confirm no orders are sent.
* **Recovery**: re-run the certification for the adapter; only governance can restore authority.
* **Escalation**: research-governance owner.

## EXCHANGE_CONNECTIVITY_LOSS (HIGH, trading-operations)

* **Detection**: `evaluate_exchange_connectivity` -- account reads READ_FAILED for 2 min, a user stream that was
  connected is down for 2 min, the venue banned this deployment or closed requests for more than 2 min.
* **Containment**: an account that cannot be read cannot be protected or reconciled: account kill switch if it
  holds positions; a venue ban affects every account on that venue -- stop non-essential request load.
* **Recovery**: venue status page, credentials validity (admin console connection test), network/DNS; wait out a
  ban (never retry earlier); confirm READ_FAILED clears and the next reconciliation is clean.
* **Escalation**: 3+ accounts or a venue ban -> platform incident, status page "Exchange connectivity DEGRADED".

## RISK_BRAKE_ACTIVATED (MEDIUM; stop / equity exhausted HIGH, trading-operations)

* **Detection**: `evaluate_risk_brakes` -- `cati_brake_state` shows an active brake (daily-loss pause, drawdown
  halving, drawdown stop, equity exhausted); one alert per activation, resolved when the brake clears.
* **Containment**: none: the brake IS the containment. Do not clear it by hand.
* **Recovery**: a latched stop clears only through an authorized restart record (user or operator), which the
  database enforces; review the drawdown with the customer first.
* **Escalation**: several accounts at once -> look for a strategy or market-data fault (escalation owner).

## EXPIRED_FAMILY_CERTIFICATION (HIGH, research-governance)

* **Detection**: `evaluate_family_certification` -- a family at governance stage M6 or above whose research
  certification (re-read from the register) is no longer CERTIFIED. Today every family is at M0: silent.
* **Containment**: the registry already withholds DEMO/LIVE modes from an uncertified family; confirm no orders.
* **Recovery**: only research governance can restore it (new evidence, a new certification); never edit the
  register by hand.
* **Escalation**: research-governance owner.

## GOVERNANCE_VIOLATION_ATTEMPT (HIGH, security)

* **Detection**: an action refused because it would bypass research or execution governance.
* **Containment**: none required (it was refused); preserve the evidence.
* **Recovery**: identify the actor and why; correct the process or the access.
* **Escalation**: repeated or from an admin account -> escalation owner.

## OPERATIONS_OWNER_REQUIRED (CRITICAL, operations-owner)

* **Detection**: an alert was routed to a rotation with no assigned primary (raised automatically).
* **Containment**: none possible in software: a human must be named.
* **Recovery**: the business assigns named people per rotation in `/admin/operations` (On-call ownership). The
  alert resolves when an admin resolves it after assignment.
* **Escalation**: this IS the escalation gap. It stays CRITICAL until ownership exists.

## ALERT_DELIVERY_FAILURE (HIGH, operations-owner)

* **Detection**: a delivery was abandoned after every retry.
* **Containment**: check `/admin/operations` directly -- alerts may be unseen.
* **Recovery**: `python3 scripts/notify_failure.py --test` on the server; fix the channel configuration in
  `/etc/cosmicforge/alerts.env`.
* **Escalation**: escalation owner, by any working means.
