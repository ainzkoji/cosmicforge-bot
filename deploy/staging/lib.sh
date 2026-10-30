# shellcheck shell=bash
# Shared by deploy.sh, rollback.sh and restore.sh (Section H, Step 5.6). Sourced, never executed.
#
# Every setting below defaults to the production behaviour. They exist so the real scripts can be exercised
# against a fake docker in tests (backends/bot-backend/tests/deploy/) and on CI's Docker with short timeouts;
# none of them removes a step or a check.
#
#   DOCKER_BIN                 docker client                                   (default: docker)
#   PYTHON_BIN                 host python for the release gate and JSON        (default: python3)
#   DEPLOY_STATE_DIR           state, transcripts, verdicts, archived logs      (default: deploy/staging/.deploy-state)
#   DEPLOY_BACKUP_DIR          platform database dumps                          (default: deploy/staging/backups)
#   DEPLOY_COMPOSE_OVERRIDE    an extra compose file layered on the staging one (CI: no-egress network)
#   DEPLOY_API_SERVICES        the API tier, space separated  (default: bot-api user-backend admin-backend
#                              user-frontend admin-frontend)
#   DEPLOY_READY_TIMEOUT_S     how long a started release may take to pass the release gate     (default: 300)
#   DEPLOY_ROLLBACK_READY_TIMEOUT_S  the same for the automatic rollback deploy.sh runs   (default: the above)
#   DEPLOY_POLL_INTERVAL_S     release gate poll interval                                       (default: 5)
#   DEPLOY_STOP_GRACE_S        SIGTERM grace of a trading process; at least 75: the runtime's drain budget
#                              (cycle 15 s + production cycle 30 s + HTTP graceful shutdown 20 s)  (default: 120)
#   DEPLOY_DB_TIMEOUT_S        how long PostgreSQL may take to accept connections               (default: 120)
#   DEPLOY_REQUIRE_SUPERVISED  1: refuse a release whose trading runtime is unsupervised (production profile)
#   DEPLOY_STRICT_HEALTH_CHECK 1: also require scripts/vps_health_check.py to pass (production-profile host)
#   COMPOSE_PROFILES           read by docker compose itself; "multi-worker" adds bot-trading-assigned to the
#                              trading phase (off by default; see the compose file)

STAGING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER="${DOCKER_BIN:-docker}"
PY="${PYTHON_BIN:-python3}"
STATE="${DEPLOY_STATE_DIR:-$STAGING_DIR/.deploy-state}"
BACKUPS="${DEPLOY_BACKUP_DIR:-$STAGING_DIR/backups}"
COMPOSE=("$DOCKER" compose -f "$STAGING_DIR/docker-compose.staging.yml")
if [ -n "${DEPLOY_COMPOSE_OVERRIDE:-}" ]; then COMPOSE+=(-f "$DEPLOY_COMPOSE_OVERRIDE"); fi
read -r -a API_SERVICES <<< "${DEPLOY_API_SERVICES:-bot-api user-backend admin-backend user-frontend admin-frontend}"
TRADING_SERVICES=(bot-trading)
case ",${COMPOSE_PROFILES:-}," in *,multi-worker,*) TRADING_SERVICES+=(bot-trading-assigned) ;; esac
READY_TIMEOUT_S="${DEPLOY_READY_TIMEOUT_S:-300}"
POLL_S="${DEPLOY_POLL_INTERVAL_S:-5}"
GRACE_S="${DEPLOY_STOP_GRACE_S:-120}"
DB_TIMEOUT_S="${DEPLOY_DB_TIMEOUT_S:-120}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"          # names the backup of this run
RUN_ID="$STAMP-p$$"                         # names this run's transcript, verdicts, drain record and logs
LAST_VERDICT='{"verdict": "NOT_EVALUATED"}'
LOCK_OWNED=0

for _v in READY_TIMEOUT_S POLL_S GRACE_S DB_TIMEOUT_S; do
  [[ "${!_v}" =~ ^[0-9]+$ ]] || { echo "invalid $_v=${!_v}: whole seconds expected"; exit 2; }
done
[ "$GRACE_S" -ge 75 ] || { echo "DEPLOY_STOP_GRACE_S=$GRACE_S is below the trading drain budget (75 s)"; exit 2; }
mkdir -p "$STATE" "$STATE/logs" "$BACKUPS"

valid_tag() { [[ "${1:-}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; }

image_of() {  # compose service -> image repository, in REPLY (no subshell)
  case "$1" in
    bot-api|bot-trading|bot-trading-assigned|healthcheck|migrate) REPLY=bot-backend ;;
    *) REPLY="$1" ;;
  esac
}

compose_at() {  # compose_at TAG <compose args...>: the stack file interpolated with IMAGE_TAG=TAG
  local tag="$1"; shift
  IMAGE_TAG="$tag" "${COMPOSE[@]}" "$@"
}

schema_range() {  # the schema range an image understands, read from the image itself
  "$DOCKER" run --rm -w /srv/cosmicforge/backends/shared "cosmicforge/bot-backend:$1" \
    python -m shared_lib.persistence.sql build-range
}

schema_compatible() {  # schema_compatible RANGE_TAG: can RANGE_TAG's image run on the database as it is now?
  local range bounds
  range="$(schema_range "$1")" || return 1
  bounds="$("$PY" -c 'import json, sys; r = json.loads(sys.argv[1]); print(int(r["app_schema_min"]), int(r["app_schema_max"]))' \
    "$range")" || return 1
  compose_at "$1" run --rm migrate python -m shared_lib.persistence.sql check-compat \
    --app-min "${bounds% *}" --app-max "${bounds#* }"
}

record_result() {  # operation status phase from_tag to_tag serving_tag [detail]
  local line
  line="$("$PY" -c 'import json, sys, time
keys = ("operation", "status", "phase", "from_tag", "to_tag", "serving_tag", "detail")
d = dict(zip(keys, sys.argv[1:]))
d["at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
print(json.dumps(d, sort_keys=True))' "$@")"
  printf '%s\n' "$line" > "$STATE/last_result.json"
  printf '%s\n' "$line" >> "$STATE/history.jsonl"
  echo "result: $line"
}

acquire_lock() {  # one deploy / rollback / restore at a time; deploy.sh hands its lock to the rollback it runs
  if [ -n "${DEPLOY_LOCK_HELD:-}" ] && [ "$DEPLOY_LOCK_HELD" = "$STATE/lock" ] && [ -d "$STATE/lock" ]; then
    return 0
  fi
  if ! mkdir "$STATE/lock" 2>/dev/null; then
    echo "REFUSED: another operation holds $STATE/lock ($(cat "$STATE/lock/owner" 2>/dev/null || echo unknown))."
    echo "Remove that directory only after confirming no deploy, rollback or restore is running."
    exit 9
  fi
  echo "$1 pid=$$ at=$STAMP" > "$STATE/lock/owner"
  LOCK_OWNED=1
  export DEPLOY_LOCK_HELD="$STATE/lock"
}

release_lock() { if [ "$LOCK_OWNED" = 1 ]; then rm -rf "$STATE/lock"; LOCK_OWNED=0; fi; }

wait_database_ready() {  # TAG: PostgreSQL started (never recreated here) and accepting connections
  local deadline=$((SECONDS + DB_TIMEOUT_S))
  compose_at "$1" up -d --no-recreate postgres
  until compose_at "$1" exec -T postgres pg_isready -U cosmicforge -d cosmicforge >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then echo "PostgreSQL not accepting connections within ${DB_TIMEOUT_S}s"; return 1; fi
    sleep "$POLL_S"
  done
  echo "database ready"
}

archive_logs() {  # TAG LABEL SERVICE...: keep outgoing containers' logs; a recreated container loses its own
  local tag="$1" label="$2" name; shift 2
  name="$1"; [ "$#" -gt 1 ] && name="api-tier"
  compose_at "$tag" logs --no-color --timestamps "$@" > "$STATE/logs/$RUN_ID-$name-$label.log" 2>&1 || true
}

stop_trading_service() {  # TAG SERVICE OUTGOING: graceful stop (SIGTERM, GRACE_S) and a record of how it ended
  local tag="$1" svc="$2" outgoing="$3" ids id out code oom outcome="NOT_RUNNING" log quiesce=not_logged
  ids="$(compose_at "$tag" ps -q "$svc" 2>/dev/null || true)"
  compose_at "$tag" stop -t "$GRACE_S" "$svc" || true
  for id in $ids; do
    out="$("$DOCKER" inspect -f '{{.State.ExitCode}} {{.State.OOMKilled}}' "$id" 2>/dev/null || echo "unknown false")"
    code="${out%% *}"; oom="${out##* }"
    case "$code" in
      0) outcome="DRAINED" ;;                       # SIGTERM -> uvicorn shutdown -> quiesce -> clean exit
      137) outcome="KILLED_AFTER_GRACE" ;;          # SIGKILL: the drain did not finish within GRACE_S
      143) outcome="EXITED_ON_SIGTERM" ;;           # ended by the signal; "quiesce" says whether it drained first
      *) outcome="EXIT_$code" ;;
    esac
    [ "$oom" = true ] && outcome="OOM_KILLED"
  done
  archive_logs "$tag" "$outgoing" "$svc"           # after the stop: the shutdown lines are in it
  log="$STATE/logs/$RUN_ID-$svc-$outgoing.log"
  if grep -q '\[RUNTIME_SHUTDOWN\] clean=True' "$log" 2>/dev/null; then quiesce=clean
  elif grep -q '\[RUNTIME_SHUTDOWN\] clean=' "$log" 2>/dev/null; then quiesce=unclean; fi
  printf '{"service": "%s", "outgoing_tag": "%s", "outcome": "%s", "quiesce": "%s", "grace_s": %s}\n' \
    "$svc" "$outgoing" "$outcome" "$quiesce" "$GRACE_S" >> "$STATE/drain-$RUN_ID.jsonl"
  echo "stopped $svc ($outgoing): $outcome; runtime quiesce in its log: $quiesce"
  case "$outcome" in DRAINED|NOT_RUNNING) ;; *)
    echo "WARNING: $svc did not drain cleanly ($outcome). Its lease is taken over by the replacement, which"
    echo "recovers unknown submissions by read-back before it submits anything; review $STATE/logs." ;;
  esac
}

release_gate() {  # TAG SERVICE: ask the trading process itself; print the verdict; return the gate's status
  local tag="$1" svc="$2" out code body ecode="" args=()
  out="$(compose_at "$tag" exec -T "$svc" curl -sS -m 10 -w '\n%{http_code}' \
          'http://127.0.0.1:8000/health?ready=1' 2>/dev/null)" || true
  code="${out##*$'\n'}"; body="${out%$'\n'*}"
  [[ "$code" =~ ^[0-9]{3}$ ]] || { code=0; body=""; }
  if [ "$code" = 200 ]; then
    ecode="$(compose_at "$tag" exec -T "$svc" curl -sS -m 10 -o /dev/null -w '%{http_code}' \
              'http://127.0.0.1:8000/api/v1/admin/emergency/status' 2>/dev/null)" || true
    [[ "$ecode" =~ ^[0-9]{3}$ ]] && args+=(--emergency-status "$ecode")
  fi
  [ "${DEPLOY_REQUIRE_SUPERVISED:-0}" = 1 ] && args+=(--require-supervised)
  # several trading processes: exactly one holds the runtime-owner lease, not necessarily this one
  [ "${#TRADING_SERVICES[@]}" -gt 1 ] && args+=(--lease-optional)
  printf '%s' "$body" | "$PY" "$STAGING_DIR/release_gate.py" --http-status "$code" ${args[@]+"${args[@]}"}
}

wait_release_ready() {  # TAG SERVICE: until the release gate accepts, or the deadline; never sends a mutation
  local tag="$1" svc="$2" deadline=$((SECONDS + READY_TIMEOUT_S)) rc
  while :; do
    rc=0; LAST_VERDICT="$(release_gate "$tag" "$svc")" || rc=$?
    printf '%s\n' "$LAST_VERDICT" > "$STATE/verdict-$RUN_ID-$svc.json"
    if [ "$rc" -eq 0 ]; then echo "release gate ($svc, $tag): $LAST_VERDICT"; return 0; fi
    if [ "$rc" -eq 12 ]; then echo "release gate ($svc, $tag): $LAST_VERDICT -- never accepted"; return 1; fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "release gate ($svc, $tag) not passed within ${READY_TIMEOUT_S}s: $LAST_VERDICT"; return 1
    fi
    sleep "$POLL_S"
  done
}

strict_health_check() {  # TAG: the full production health check, gating only when DEPLOY_STRICT_HEALTH_CHECK=1
  local rc=0
  compose_at "$1" exec -T bot-trading python /srv/cosmicforge/scripts/vps_health_check.py \
    --url http://127.0.0.1:8000 --json > "$STATE/health-check-$RUN_ID.json" 2>&1 || rc=$?
  if [ "${DEPLOY_STRICT_HEALTH_CHECK:-0}" = 1 ]; then
    [ "$rc" -eq 0 ] || { echo "strict health check failed (exit $rc): $STATE/health-check-$RUN_ID.json"; return 1; }
  else
    echo "health check report (evidence, not gating unless DEPLOY_STRICT_HEALTH_CHECK=1; exit $rc):"
    echo "  $STATE/health-check-$RUN_ID.json"
  fi
}

roll_trading() {  # TAG OUTGOING: drain and replace each trading process in turn, each through the release gate
  local tag="$1" outgoing="$2" svc
  for svc in "${TRADING_SERVICES[@]}"; do
    stop_trading_service "$tag" "$svc" "$outgoing"
    compose_at "$tag" up -d --no-deps "$svc" || { echo "$svc ($tag) did not start"; return 1; }
    wait_release_ready "$tag" "$svc" || return 1
  done
  compose_at "$tag" up -d --no-deps healthcheck || { echo "healthcheck ($tag) did not start"; return 1; }
}
