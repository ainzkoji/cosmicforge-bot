# Step 5 report: persistence, workers, queue, scaling, order spreading, staging

17 October 2026. All measurements were taken on the development workstation (Windows 11). The databases were a
disposable PostgreSQL 18.4 cluster (`initdb` in a scratch directory, port 55432, localhost only) and SQLite files.
The venues were the in-memory `FakeVenue`; the market data was synthetic and replayed. **These are synthetic
measurements**: not exchange-demo throughput, not hosted staging, and not a Linux certification. Linux evidence for
the code comes from CI, which runs the platform tests on a PostgreSQL 16 service.

## 1. PostgreSQL, migrations, pooling (5.1)

**Before this work** the platform used raw `sqlite3` everywhere:

- no SQLAlchemy, no PostgreSQL support and no schema-version table;
- foreign keys were declared but never enforced (`PRAGMA foreign_keys` never set);
- money was mostly `REAL`.

**What was added** is `shared_lib/persistence/sql/`, one layer that runs unchanged on SQLite and PostgreSQL:

- `?` placeholders on both dialects;
- explicit transactions, and a write lock up front on SQLite;
- foreign keys on;
- the **database server's clock** for every lease decision;
- a bounded, health-checked pool;
- `health()` for readiness;
- money as `NUMERIC(38,18)` on PostgreSQL and exact text on SQLite, with floats rejected.

**Migrations** (`sql/migrations.py`) are versioned and checksummed:

- They run under PostgreSQL `pg_advisory_xact_lock` or SQLite `BEGIN IMMEDIATE`.
- Each version is applied in its own transaction.
- An edited applied migration is refused.
- `rollback`, `irreversible` and `compatibility()` serve the rollback scripts.
- Each area has its own module and a reserved version range: Step 5 1–3, go-live 10–12, track record 20–29,
  operations 30–39.

**Migration drill** (`scripts/migration_drill.py`, evidence `evidence/step5/migration_drill.json`). On a
representative database built from the real schema (254 tables, 229 seeded), all 13 checks passed:

- the sanitized copy;
- row counts preserved;
- historical content hashes unchanged;
- integrity ok;
- foreign-key violations not increased;
- a crash inside a version left no partial version;
- a repeated run was a no-op;
- read and write after a restart;
- rollback left the legacy tables untouched and removed only the platform tables;
- restore equal to the baseline;
- the PostgreSQL copy, `pg_dump`, drop, `pg_restore` and counts.

Finding **MIG-F1**: the legacy `migrate()` re-enables the built-in news RSS sources on every run, which resets an
operator's choice. It is low severity (news subsystem) and recorded, not fixed (Section I Batch 10).

**Not done:** the legacy runtime schema still runs on SQLite. The trading process is single-owner and SQLite serves
it. Moving it is EXT-DB-01.

## 2. Workers separated from the API (5.2)

- `PROCESS_ROLE=api` processes start no runner, never request the runtime lease and refuse runtime mutations (409).
  `trading` is the single owner process. `all` (the default) keeps today's single-process deployment.
- `WorkerAssignments` gives each account at most one owner:
  - a fencing token rises on every change of owner;
  - expiry is measured on the database clock;
  - workers take fair-share claims in a deterministic order;
  - takeover history is kept;
  - `needs_recovery` holds until the new owner has reconstructed state.
- Killed-process drill (`scripts/worker_kill_drill.py`, 200 accounts, real OS processes, `taskkill /F` mid-cycle, 3 s
  lease):

| Dialect | Takeover after kill | Drain handover | Venue orders | Duplicates | Intents unresolved after restart |
|---|---|---|---|---|---|
| PostgreSQL | 2,750 ms | 219 ms | 1,984 | 0 | 0 |
| SQLite | 2,640 ms | 359 ms | 1,995 | 0 | 0 |

## 3. Durable job queue (5.3)

`app/platform_workers/jobs.py`:

- **Jobs:** an idempotency key on every job, lease tokens, backoff, a dead letter (`DEAD`) and an operator requeue.
- **Order intents:** each intent is committed as `SUBMITTING` before the venue call, and no transaction stays open
  across that call (tested).
- **Lost acknowledgement:** it is recovered by reading the venue back by client id, never by resubmitting. A
  resubmission happens only after the venue says the order is absent and a settle window has passed, and it uses the
  same client id.

There are 30 scenario tests, on both dialects.

## 4. Signal once, size per account (5.4)

The shared market stage ran once per cycle in **15.9–18.1 ms at 1, 10, 100 and 1,000 accounts** (max/min 1.14,
which is host noise). Sizing grows linearly at about 0.2 ms per account, so 1,000 accounts take about 0.2 s. Eight
threads gave no speed-up: the frozen sizing is CPU-bound Python under the GIL. Concurrency pays for I/O-bound broker
reads, not for this. Isolation is tested: deep-copied snapshots, tamper fingerprint on the shared signal, per-account
errors. Evidence: `evidence/step5/signal_fanout.json`.

## 5. Order spreading (5.5)

**Depth refusal.** Depth is refused when stale (older than 5 s), thin (fewer than 5 levels) or crossed. These
bounds are the venue cost model's own.

**Cap.** The cap is 10% of the visible notional inside a 25 bps band, and it is stored `calibrated=0`. A calibrated
policy needs an approval reference, which the schema enforces.

**Budgets.**

- A budget is shared per (venue, symbol, side) across workers and signals.
- Allocation runs under a row lock plus an advisory lock per symbol on PostgreSQL.
- Grants follow the `sha256(signal|account)` order: reproducible, and different for each signal.
- Slots are bounded, limit prices are tick-safe and never beyond the worst price, and the market fallback is gated.

**Tests.** A concurrent-worker test with 4 database connections and 200 accounts never exceeds the cap.

## 6. Load test at 1 / 10 / 100 / 1,000 accounts (5, closing measurement)

`scripts/step5_load_test.py` produced the evidence in `evidence/step5/load_test.json`. Each run used 4 workers
(threads) and one fake venue per account with a protected position.

| Accounts | Dialect | Sizing ms | Maintenance cycle ms (4 workers) | Queue jobs/s | Duplicate orders | Takeover ms (2 s lease) | Pool peak |
|---|---|---|---|---|---|---|---|
| 1 | PostgreSQL | 0.45 | 12 | 94 | 0 | 9 | 4 |
| 10 | PostgreSQL | 1.55 | 18 | 166 | 0 | 1,909 | 6 |
| 100 | PostgreSQL | 22.8 | 119 | 359 | 0 | 1,274 | 12 |
| 1,000 | PostgreSQL | 207.8 | 825 | 366 | 0 | 1,764 | 16 |
| 1 | SQLite | 0.51 | 50 | 34 | 0 | 33 | 4 |
| 10 | SQLite | 1.55 | 150 | 244 | 0 | 1,909 | 7 |
| 100 | SQLite | 30.9 | 239 | 766 | 0 | 2,221 | 11 |
| 1,000 | SQLite | 176.1 | 497 | 948 | 0 | 586 | 11 |

Every account was maintained in every cycle. Order spreading stayed inside its cap at every scale and used one depth
snapshot per signal, not one per account. The JSON also records CPU, RSS, statement counts and per-account p50/p95
stage latency.

**Closure:** engineering PASS for the measurable parts. EXT-LOAD-01 (exchange demo), EXT-STG-01 (staging) and Linux
resource measurement remain open.

## 7. Staging, containers, deployment and rollback (5.6)

**Images and stack.**

- Container images are non-root and carry no `.env`, database or credential files (CI checks this). They ship the
  research artifacts byte-identical.
- The staging compose stack has a one-shot migrate job, one trading process, API replicas, the portals and a health
  check.

**Scripts.** Runbook: [STAGING_DEPLOYMENT_RUNBOOK.md](../operations/STAGING_DEPLOYMENT_RUNBOOK.md).

- `deploy.sh`:
  1. pre-deploy backups;
  2. migrations;
  3. a check that a rollback stays compatible;
  4. the API tier;
  5. a graceful stop and start of the trading process;
  6. readiness verification, with automatic rollback on failure.
- `rollback.sh` refuses an incompatible binary on an irreversible schema.
- `restore.sh` requires `--confirm`.

**Deployment drill test.**

- Setup: protected positions, and a persisted risk latch.
- Event: the worker is killed with an order outcome unknown.
- Outcome: the successor recovers that intent without a duplicate. Protection never lapses, the latch persists,
  reconciliation runs clean, and emergency control is accepted.
