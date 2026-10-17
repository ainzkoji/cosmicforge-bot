# Staging deployment and rollback runbook (Section H, Step 5.6)

Scope: the staging stack in `deploy/staging/docker-compose.staging.yml`. Staging trades nothing. Its environment
keeps `LIVE_ORDER_SUBMISSION_ENABLED=false` and `DEMO_ORDER_SUBMISSION_ENABLED=false`, and the go-live gate and
family gate refuse regardless. **No hosted staging environment has been authorized yet (EXT-STG-01). The procedure
below has run only as tests in CI and on the workstation.**

## Preconditions

1. A host with Docker Compose v2. The operator holds `deploy/staging/staging.env` (from `staging.env.example`) and
   `secrets/postgres_password` outside version control.
2. Images built from one commit:
   `docker build -f deploy/containers/backend.Dockerfile --build-arg SERVICE=<svc> --build-arg BUILD_REVISION=<sha> -t cosmicforge/<svc>:<tag> .`
   for bot-backend, user-backend and admin-backend, and `frontend.Dockerfile` for the two portals.
3. The exact commit's CI run is green.

## Deploy

```
deploy/staging/deploy.sh <tag>
```

What it does, and where to look if it stops:

| Step | Check | On failure |
|---|---|---|
| preflight | compose valid; the five images exist; schema range read from the image | fix and rerun; nothing has changed |
| backup | `pg_dump` (`backups/platform-<stamp>.dump`, sha256 recorded) and the online SQLite backup | deploy stops; there is no deploy without a restore point |
| migrate | the one-shot `migrate` job of the new image | the version transaction rolled back; investigate; the old stack is still running |
| rollback check | the previous image's schema range against the migrated schema | recorded in `.deploy-state/rollback-mode` (REDEPLOY or RESTORE REQUIRED) |
| API tier | `bot-api`, `user-backend`, `admin-backend`, portals replaced | they hold no trading state |
| trading | graceful stop (SIGTERM, 120 s grace: the cycle finishes, in-flight work joins, the runtime quiesces), then start on the new image | exchange-side stops keep resting at the venue throughout; the new process rebuilds broker truth and resolves unknown submissions before it sends anything |
| verify | `/health?ready=1` within 300 s, then `scripts/vps_health_check.py` | `rollback.sh` runs automatically |

Do **not** treat a restarted API as evidence that trading is safe. Only the trading process's readiness and the
health check cover the runtime.

## Roll back

```
deploy/staging/rollback.sh [<tag>]
```

The script redeploys the earlier image **only** if its schema range accepts the current database. Platform
migrations are additive; an irreversible version makes the check fail, and the script refuses. In that case:

```
deploy/staging/restore.sh deploy/staging/backups/platform-<stamp>.dump <tag> --confirm
```

The restore script:

1. checks the dump's sha256;
2. stops trading and the API tier;
3. replaces the platform database and restarts.

After a restore, verify readiness, reconciliation and protection before reopening anything. The go-live gate
compares the ledger with the off-database revocation anchor (`GOLIVE_REVOCATION_ANCHOR`). A database restored from
before a revocation is refused (`LEDGER_BEHIND_REVOCATION_ANCHOR`) until an operator reconciles it.

## Worker failure during deployment

If the trading process dies without draining, it holds its accounts until their assignments expire (one lease). The
replacement takes over, runs recovery for every account first and only then maintains it. The drill evidence is in
`evidence/step5/worker_kill_drill.json`, and `tests/platform_workers/test_deployment_drill.py` repeats it on every
CI run.

## Emergency controls during a deploy

The emergency routes are served by the trading process only. API replicas answer `409
ROUTE_SERVED_BY_TRADING_PROCESS`. During the short trading restart window, use the platform kill switch first; it is
durable and read by every new entry. Then use the flatten route as soon as the trading process is ready again.
