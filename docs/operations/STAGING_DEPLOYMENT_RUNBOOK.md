# Staging deployment and rollback runbook (Section H, Step 5.6)

Scope: the staging stack in `deploy/staging/docker-compose.staging.yml`. Staging trades nothing. Its environment
keeps `LIVE_ORDER_SUBMISSION_ENABLED=false` and `DEMO_ORDER_SUBMISSION_ENABLED=false`, and the go-live gate and
family gate refuse regardless.

**Status: no hosted staging environment has been authorized (EXT-STG-01, BLOCKED).** Everything below has run only
as (a) tests of the real scripts against a fake `docker` (workstation and Linux CI), (b) worker-level drills on fake
venues and throwaway databases, and (c) a disposable failure drill on the CI runner's Docker. None of these is
evidence of sustained staging or production operation. The one remaining external step is in
[Hosted staging certification](#hosted-staging-certification-the-remaining-external-step).

## Preconditions

1. A host with Docker Compose v2, `bash`, `python3` and `sha256sum`. The operator holds `deploy/staging/staging.env`
   (from `staging.env.example`) and `deploy/staging/secrets/postgres_password` outside version control
   (`deploy/staging/.gitignore` keeps them, the dumps and `.deploy-state/` out of Git).
2. Images built from one commit:
   `docker build -f deploy/containers/backend.Dockerfile --build-arg SERVICE=<svc> --build-arg BUILD_REVISION=<sha> -t cosmicforge/<svc>:<tag> .`
   for bot-backend, user-backend and admin-backend, and `frontend.Dockerfile` for the two portals.
3. The exact commit's CI run is green, including the container job's failure drill.
4. The go-live revocation anchor is on separate storage: set `GOLIVE_ANCHOR_SOURCE=/path/on/another/disk` (a
   directory writable by uid 10001) in the shell that runs the scripts. Without it the anchor uses the named volume
   `golive-anchor`, which is separate from both databases but on the same disk.

## Deploy

```
deploy/staging/deploy.sh <tag>
```

| Step | Check | On failure |
|---|---|---|
| preflight | tag well formed; compose valid; the release's images exist, and so does the previous bot image (the way back); schema range read from the image | exit 10; nothing changed |
| database | PostgreSQL started if needed (never recreated by the script) and answering `pg_isready` | exit 10; nothing changed |
| backup | `pg_dump` (`backups/platform-<stamp>.dump`, sha256 in `.deploy-state/backup-<stamp>.sha256`) and the online SQLite backup in a one-off container | exit 10; there is no deploy without a restore point |
| migrate | the one-shot `migrate` job of the new image | exit 10. Nothing was swapped: the old release keeps serving. `rollback-mode` records whether the old image still accepts the partly migrated schema |
| rollback check | the previous image's schema range against the migrated schema | recorded in `.deploy-state/rollback-mode` (REDEPLOY or RESTORE REQUIRED) |
| API tier | `bot-api`, `user-backend`, `admin-backend`, portals replaced (outgoing logs archived first) | automatic rollback |
| trading | each trading process drained (SIGTERM, 120 s grace: the runtime stops new entries, lets the cycle in flight finish its protection, releases its lease), then started on the new image | automatic rollback |
| verify | the release gate on the trading process (below) within 300 s; then `scripts/vps_health_check.py` (evidence; gating with `DEPLOY_STRICT_HEALTH_CHECK=1`) | automatic rollback |

Exit status: 0 deployed; 10 failed before the swap (old release serving); 11 rejected and rolled back (previous
release serving and accepted by the gate); 12 rejected and the rollback did not complete (page the operations
owner); 13 rejected on a first deploy (no previous release); 9 another deploy/rollback/restore holds the lock;
2 usage or settings.

Every outcome is a JSON line in `.deploy-state/last_result.json` and `history.jsonl` (`status`, `phase`, `from_tag`,
`to_tag`, `serving_tag`, `detail`). Also kept there: the transcript of each run, each release-gate verdict
(`verdict-*.json`), how each trading process stopped (`drain-*.jsonl`: `DRAINED`, `EXITED_ON_SIGTERM`,
`KILLED_AFTER_GRACE`, `OOM_KILLED`, plus whether its log shows the runtime's `[RUNTIME_SHUTDOWN]` quiesce), and the
outgoing containers' logs (`logs/`; a recreated container loses its own). `current_tag` names the last accepted
release; `serving_tag` in the result names what runs now.

Do **not** treat a restarted API as evidence that trading is safe. Only the trading process's release gate covers
the runtime.

### The release gate

`deploy/staging/release_gate.py` reads the trading process's own `/health?ready=1` (through `docker compose exec`,
never an API replica) and an unauthenticated GET of `/api/v1/admin/emergency/status`. It only reads.

| Verdict | Meaning | Accepted |
|---|---|---|
| `READY` | supervised runtime (production profile): supervisor HEALTHY, runtime lease held, every discovered account synced and reconciled, emergency routes served | yes |
| `READY_UNSUPERVISED` | non-production profile: the process answers as the trading role, but no supervisor judges the loop and no broker reconciliation is reported. **Staging runs this profile today** (`APP_ENV=DEVELOPMENT`) | yes, unless `DEPLOY_REQUIRE_SUPERVISED=1` |
| `NOT_READY` | no answer, non-200, wrong role, supervisor not HEALTHY, lease not held, emergency routes not served | no (waits until the deadline) |
| `DEGRADED` | the process is healthy but broker truth is not: an account is not synced/reconciled (exchange unreachable, credentials refused) | no |
| `UNSAFE` | real-money LIVE submission enabled on staging | no, immediately |

A production-profile staging (EXT-STG-01) must run with `DEPLOY_REQUIRE_SUPERVISED=1` and, once accounts are
connected, `DEPLOY_STRICT_HEALTH_CHECK=1`.

## Roll back

```
deploy/staging/rollback.sh [<tag>]
```

Without a tag it targets `previous_tag`. `deploy.sh` runs it automatically with the release it replaced. The
script redeploys the earlier image **only** if that image's own `check-compat` accepts the current database; an
irreversible newer version makes it refuse (exit 3) before it changes anything. Then the API tier is replaced,
each trading process is drained and replaced, and the rolled-back trading process must pass the same release gate
as a deploy, including the post-start broker reconciliation, before the rollback counts as done (exit 0). Exit 4:
the target serves but did not pass the gate; exit 5: the rollback failed part-way. Both page the operations owner.

If the rollback is refused:

```
deploy/staging/restore.sh deploy/staging/backups/platform-<stamp>.dump <tag> --confirm
```

The restore script, under the owner's authorization:

1. refuses unless the dump's sha256 equals the one recorded at backup time;
2. drains the trading process(es) first, then stops every other platform-database client the compose file defines;
3. drops, recreates and restores the platform database, runs the release's migration job and `check-compat`;
4. starts the stack on `<tag>` and waits for the release gate.

It does not restore the runtime SQLite database (its verified online backups are in the `runtime-state` volume,
`/var/lib/cosmicforge/backups`) and never touches the revocation anchor. The go-live gate compares the ledger with
the anchor (`GOLIVE_REVOCATION_ANCHOR`), so a database restored from before a revocation is refused
(`LEDGER_BEHIND_REVOCATION_ANCHOR`) until an operator reconciles it.

## Failure drills

### 1. The real scripts against a fake docker (every CI run)

`backends/bot-backend/tests/deploy/test_staging_deploy_scripts.py` runs `deploy.sh`, `rollback.sh` and
`restore.sh` under bash with `tests/deploy/fake_docker.py` first on `PATH` (and as `DOCKER_BIN`). The fake records
every call and injects failures per release tag. Linux CI runs the whole matrix in the bot job; on the Windows
workstation only the core scenarios run unless `CF_DEPLOY_SCRIPT_TESTS=all` is set (each bash fork costs ~0.3 s
there).

| Scenario | Injected | Asserted |
|---|---|---|
| deploy, then upgrade | — | order: database ready, dump, runtime backup, migrate (new image), compat (old image), API tier, drain, start, gate; dump checksum recorded; drain `DRAINED` with clean quiesce; no mutating request |
| migration fails | `migrate` exits 5 | exit 10, `FAILED_BEFORE_SWAP/migrate`, nothing stopped or started, old release serving, `current_tag` unchanged |
| backup fails | `pg_dump` fails | exit 10 at `backup`; no migration |
| previous image pruned | image missing | exit 10 at `preflight`; nothing run |
| container fails to start | `up` of bot-trading / bot-api fails | exit 11; previous release serving, both histories `ROLLED_BACK` |
| worker crashes after start | the trading container exits at once | gate `NO_ANSWER` until the deadline; exit 11 |
| never ready | 503 `STARTING` | exit 11; rolled back to the release it replaced, never to an older `previous_tag` |
| exchange connection lost | supervisor HEALTHY, accounts `NOT_SYNCED` | verdict `DEGRADED`, never accepted, exit 11; no POST/data request to any process |
| exchange lost for every release | both releases `DEGRADED` | exit 12 (`ROLLED_BACK_NOT_READY` then `ROLLBACK_FAILED`), `serving_tag` truthful; no mutation |
| LIVE gate open | `live_system_gate: true` | `UNSAFE`, rejected without waiting |
| wrong role | the trading service answers as `api` | `NOT_READY PROCESS_ROLE_API` |
| supervision required | unsupervised release, `DEPLOY_REQUIRE_SUPERVISED=1` | `NOT_READY RUNTIME_UNSUPERVISED` |
| strict health check | `vps_health_check.py` exits 2 | gating only with `DEPLOY_STRICT_HEALTH_CHECK=1` (exit 11 at `verify`) |
| first deploy rejected | no previous release | exit 13, nothing recorded as current |
| drain exceeds the grace | outgoing container exits 137 | `KILLED_AFTER_GRACE`, warning, takeover by the replacement |
| grace below the drain budget | `DEPLOY_STOP_GRACE_S=10` | exit 2 before any call |
| concurrent operation | lock held | exit 9 for deploy and rollback |
| manual rollback | target answers 503 twice, then READY and reconciled | exit 0 only after the reconciled verdict (`DEPLOY_REQUIRE_SUPERVISED=1`) |
| irreversible schema | new release marks its version irreversible | `rollback-mode` RESTORE REQUIRED; manual rollback refused (exit 3) with no change; automatic rollback refused → exit 12 |
| restore | tampered dump; no `--confirm`; real dump | refused (3 / 2) with nothing stopped; real restore drains trading before `dropdb`, stops every client, migrates, waits for the gate |
| multi-worker profile | `COMPOSE_PROFILES=multi-worker` | both trading processes drained and replaced one at a time, each through the gate; default topology never touches the optional worker |

### 2. Worker-level drills (every CI run, SQLite and PostgreSQL)

`tests/platform_workers/test_deployment_failure_drills.py` and `test_deployment_drill.py`, on `FakeVenue` and
throwaway databases:

* the replaced release drains through its real `run_forever` stop path; the successor gets every account at once
  (no TTL wait), recovers before it maintains, protection intact;
* the exchange is unreachable during the takeover: recovery fails, nothing is maintained, no venue mutation; the
  next pass recovers first, the resting stop is found, not duplicated;
* a queue consumer is killed mid-deploy (one order accepted, its acknowledgement lost): a new process on the same
  database runs nothing while the leases are held, then finishes both jobs with exactly one venue order each
  (read-back, no resubmission);
* after a rollback the old build takes the accounts back, recovers first, protection intact, REST reconciliation ok;
* a worker killed mid-cycle (`test_deployment_drill.py`): takeover within one lease, unknown submission recovered,
  risk latch persisted, emergency request accepted.

### 3. CI Docker drill (container job, every CI run)

After the image build and the migrate/API smoke, the job runs the real `deploy.sh` in its own compose project on a
network with no route off the runner (`deploy/staging/drill/no-egress.override.yml`):

1. `deploy.sh ci-good` (this commit's image) must pass the release gate;
2. `deploy.sh ci-bad` (`deploy/staging/drill/unready-release.Dockerfile`: the same image serving a process that
   answers 503 forever) must end with exit 11, `ROLLED_BACK`, `current_tag=ci-good`, the running trading container
   on the `ci-good` image answering as the trading role;
3. the replaced `ci-good` trading process must have stopped on SIGTERM (`DRAINED`/`EXITED_ON_SIGTERM`, never
   `KILLED_AFTER_GRACE`) with the runtime's shutdown line in its log.

This proves the scripts, the images and the drain on a real Docker engine for a few minutes. It is not staging.

## Hosted staging certification (the remaining external step)

Needs the owner to provide an authorized staging host (EXT-STG-01). On that host, from one green commit:

1. Build all five images for two tags (`N`, `N+1`) and the drill image
   (`docker build -f deploy/staging/drill/unready-release.Dockerfile --build-arg BASE_IMAGE=cosmicforge/bot-backend:N+1 -t cosmicforge/bot-backend:N+1-unready deploy/staging/drill`).
2. `deploy.sh N`, then `deploy.sh N+1` (exit 0 each).
3. Forced failure: `deploy.sh N+1-unready` → exit 11, `N+1` serving.
4. Manual rollback: `rollback.sh` → exit 0, `N` serving; then `deploy.sh N+1` again.
5. Worker kill: `docker kill -s KILL <bot-trading container>` mid-cycle; confirm the replacement takes the lease over
   and the gate passes again (`docker compose up -d --no-deps bot-trading` if the restart policy has not already).
6. Restore drill (owner's authorization): `restore.sh backups/platform-<stamp>.dump N --confirm` → exit 0.
7. Keep `deploy/staging/.deploy-state/` (results, verdicts, drain records, logs) as the evidence.

For a production-profile staging also set `DEPLOY_REQUIRE_SUPERVISED=1` (and `DEPLOY_STRICT_HEALTH_CHECK=1` once
accounts are connected); that profile change is itself part of EXT-STG-01.

## Worker failure during deployment

A trading process that dies without draining (SIGKILL, OOM) keeps its platform assignments until they expire (one
TTL); the replacement takes over and recovers every account before it maintains it. By code reading (not yet
observed on a container host), its runtime-owner lease is taken over at once by the replacement container: the
lease records the holder's PID and start time, and the new container's process is newer than the lease; the
fallback is the 90 s heartbeat staleness. Step 5 of the hosted certification above observes it. The drill evidence is `evidence/step5/worker_kill_drill.json`;
`tests/platform_workers/test_deployment_drill.py` and `test_deployment_failure_drills.py` repeat it on every CI run.

## Emergency controls during a deploy

The emergency router (`/api/v1/admin/emergency/...`: kill switch, flatten) is served by the trading process only.
API replicas refuse its POSTs with `409 ROUTE_SERVED_BY_TRADING_PROCESS` (the guard used to list
`/api/v1/emergency`, a path where nothing is mounted; it now covers the mounted router and the legacy broker
routes). The user-backend operator proxy calls `bot-trading` directly.

During a deploy the trading process keeps serving the emergency routes through preflight, backup, migration and the
API-tier swap. They are unavailable only between the trading drain and the replacement passing the release gate
(the drain itself is bounded at ~65 s; startup follows). In that window exchange-side protective stops keep resting
at the venue, and the new process reconciles before it submits anything. If an emergency is possible, engage the
kill switch through the portal **before** starting the deploy, or postpone the deploy. There is deliberately no
command-line kill switch.

## Multi-worker topology (optional, off by default)

`bot-trading-assigned` (compose profile `multi-worker`) is a second trading process that works only the accounts
it holds through durable worker assignments (`BROKER_CYCLE_ASSIGNMENT_MODE=assigned`). With
`COMPOSE_PROFILES=multi-worker`, `deploy.sh`/`rollback.sh`/`restore.sh` drain and replace it after `bot-trading`,
one at a time, each through the release gate. Do not enable it before:

1. the owner's rollout decision on the assignment mode (then every trading process runs
   `BROKER_CYCLE_ASSIGNMENT_MODE=assigned`, set in `staging.env`);
2. the runtime-owner lease can judge a holder in another container. Today it decides liveness by PID and process
   start time, and every container has its own PID namespace: a second trading container sees the holder's PID as
   absent (or as one of its own, newer processes) and takes over the live lease of `bot-trading`;
3. a production-profile readiness contract for a trading process that does not hold the runtime-owner lease (the
   gate already accepts that with `--lease-optional` when several trading processes run).
