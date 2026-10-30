#!/usr/bin/env bash
# Deploy an image revision to the staging stack (Section H, Step 5.6).
#
#   deploy/staging/deploy.sh <image-tag>
#
# Order, and why:
#  1. preflight   tag well formed; compose file valid; the release's images exist, and so does the previous bot
#                 image (the way back must exist before we leave); the schema range is read from the image itself
#  2. database    PostgreSQL started if needed (never recreated here) and accepting connections
#  3. backup      pg_dump of the platform database + online SQLite backup of the runtime database; the deploy
#                 stops if either fails (there is no deploy without a restore point)
#  4. migrate     the one-shot migration job of the NEW image (additive versions only; checksummed). A failure
#                 stops the rollout here: nothing was swapped and the old release keeps serving
#  5. rollback    is the PREVIOUS image still compatible with the migrated schema? recorded before anything
#     check       restarts, so the operator knows whether a rollback can be a redeploy or must be a restore
#  6. API tier    bot-api, user-backend, admin-backend, portals: replaced; they hold no trading state. The
#                 trading process keeps serving the emergency routes until step 7
#  7. trading     each trading process is stopped GRACEFULLY (SIGTERM, 120 s grace: the runtime stops new entries,
#                 lets the cycle in flight finish its protection, releases its lease; how it ended is recorded
#                 in drain-*.jsonl) and started on the new image. Exchange-side protective stops rest at the venue
#                 throughout; the new process reconstructs broker truth and resolves unknown submissions before
#                 it submits anything
#  8. verify      release_gate.py on the trading process's own /health?ready=1 (role, supervisor, runtime lease,
#                 post-start broker reconciliation, emergency routes served, LIVE gate closed) within 300 s; then
#                 scripts/vps_health_check.py (evidence; gating with DEPLOY_STRICT_HEALTH_CHECK=1)
#
# Any failure from step 6 on rolls the stack back to the previous tag automatically (rollback.sh <previous>),
# which must itself pass the release gate. Every outcome is recorded in .deploy-state/last_result.json and
# history.jsonl; transcripts, verdicts, drain records and the outgoing containers' logs stay in .deploy-state.
# Test and CI settings (fake docker, timeouts, state directory): see lib.sh.
#
# Exit status: 0 deployed; 2 usage or settings; 9 another operation holds the lock; 10 failed before the swap
# (nothing changed, the old release serves); 11 the release was rejected and the previous release serves again;
# 12 the release was rejected and the automatic rollback did not complete (page the operations owner); 13 the
# release was rejected and there is no previous release to return to.
set -euo pipefail
NEW_TAG="${1:?usage: deploy.sh <image-tag>}"
# shellcheck source=lib.sh
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
valid_tag "$NEW_TAG" || { echo "invalid image tag: $NEW_TAG"; exit 2; }
PREV_TAG="$(cat "$STATE/current_tag" 2>/dev/null || echo none)"
exec > >(tee -a "$STATE/deploy-$RUN_ID.log") 2>&1
acquire_lock "deploy $PREV_TAG -> $NEW_TAG"
PHASE=preflight
FINISHED=0

reject_release() {  # the release failed after the swap began: back to the release it replaced
  local why="$1" rc=0 serving
  if [ "$PREV_TAG" = none ]; then
    echo "$why. There is no previous release: $NEW_TAG is left as started and NOT accepted (operator decision)."
    record_result deploy REJECTED_NO_PREVIOUS "$PHASE" none "$NEW_TAG" "$NEW_TAG" "$why; verdict $LAST_VERDICT"
    return 13
  fi
  echo "$why: rolling back to $PREV_TAG"
  DEPLOY_READY_TIMEOUT_S="${DEPLOY_ROLLBACK_READY_TIMEOUT_S:-$READY_TIMEOUT_S}" ROLLBACK_FROM_TAG="$NEW_TAG" \
    bash "$STAGING_DIR/rollback.sh" "$PREV_TAG" || rc=$?
  if [ "$rc" -eq 0 ]; then
    record_result deploy ROLLED_BACK "$PHASE" "$PREV_TAG" "$NEW_TAG" "$PREV_TAG" "$why; verdict $LAST_VERDICT"
    return 11
  fi
  echo "AUTOMATIC ROLLBACK DID NOT COMPLETE (rollback.sh exit $rc): page the operations owner"
  case "$rc" in
    2|3|9) serving="$NEW_TAG" ;;                                     # refused before it changed anything
    *) serving="$(cat "$STATE/current_tag" 2>/dev/null || echo unknown)" ;;
  esac
  record_result deploy ROLLBACK_FAILED "$PHASE" "$PREV_TAG" "$NEW_TAG" "$serving" "$why; rollback exit $rc"
  return 12
}

on_exit() {  # every way out of this script records an outcome
  local rc="$1" command="$2" code=0
  trap - EXIT
  set +e
  if [ "$FINISHED" != 1 ]; then
    echo "FAILED in phase $PHASE (exit $rc): $command"
    case "$PHASE" in
      preflight|database|backup|migrate|compat)
        if [ "$PHASE" = migrate ] && [ "$PREV_TAG" != none ]; then
          if schema_compatible "$PREV_TAG" >/dev/null; then
            echo "rollback path: $PREV_TAG (still serving) is compatible with the schema as it is now" \
              | tee "$STATE/rollback-mode"
          else
            echo "rollback path: RESTORE REQUIRED ($PREV_TAG is incompatible with the partly migrated schema)" \
              | tee "$STATE/rollback-mode"
          fi
        fi
        record_result deploy FAILED_BEFORE_SWAP "$PHASE" "$PREV_TAG" "$NEW_TAG" "$PREV_TAG" "exit $rc: $command"
        code=10 ;;
      *) reject_release "phase $PHASE failed (exit $rc: $command)"; code=$? ;;
    esac
  else
    code="$rc"
  fi
  release_lock
  exit "$code"
}
trap 'on_exit $? "$BASH_COMMAND"' EXIT

echo "deploy $PREV_TAG -> $NEW_TAG at $STAMP (run $RUN_ID)"

# 1. preflight
compose_at "$NEW_TAG" config -q
IMAGES=()
for svc in bot-trading "${API_SERVICES[@]}"; do image_of "$svc"; IMAGES+=("cosmicforge/$REPLY:$NEW_TAG"); done
if [ "$PREV_TAG" != none ]; then IMAGES+=("cosmicforge/bot-backend:$PREV_TAG"); fi
"$DOCKER" image inspect "${IMAGES[@]}" >/dev/null
RANGE="$(schema_range "$NEW_TAG")"
echo "new image schema range: $RANGE"

# 2. database
PHASE=database
wait_database_ready "$NEW_TAG"

# 3. backup (restore point)
PHASE=backup
compose_at "$NEW_TAG" exec -T postgres pg_dump -U cosmicforge -Fc cosmicforge > "$BACKUPS/platform-$STAMP.dump"
test -s "$BACKUPS/platform-$STAMP.dump"
if [ "$PREV_TAG" != none ]; then
  # a one-off container on the runtime volume: works whether or not the trading container is up
  compose_at "$PREV_TAG" run --rm --no-deps -T bot-trading python /srv/cosmicforge/scripts/backup_trading_db.py \
    --output-dir /var/lib/cosmicforge/backups --keep 14 --compress
fi
(cd "$BACKUPS" && sha256sum "platform-$STAMP.dump") | tee "$STATE/backup-$STAMP.sha256"

# 4. migrate with the new image
PHASE=migrate
compose_at "$NEW_TAG" run --rm migrate

# 5. can the previous image still run on this schema?
PHASE=compat
if [ "$PREV_TAG" != none ]; then
  if schema_compatible "$PREV_TAG"; then
    echo "rollback path: REDEPLOY of $PREV_TAG is schema-compatible" | tee "$STATE/rollback-mode"
  else
    echo "rollback path: RESTORE REQUIRED (previous image incompatible with migrated schema)" | tee "$STATE/rollback-mode"
  fi
fi

# 6. API tier
PHASE=api
archive_logs "$NEW_TAG" "$PREV_TAG" "${API_SERVICES[@]}"
compose_at "$NEW_TAG" up -d --no-deps "${API_SERVICES[@]}"

# 7 + 8. trading processes, one at a time, each through the release gate; then the health check
PHASE=trading
if ! roll_trading "$NEW_TAG" "$PREV_TAG"; then
  FINISHED=1; code=0
  reject_release "release gate refused $NEW_TAG" || code=$?
  exit "$code"
fi
PHASE=verify
if ! strict_health_check "$NEW_TAG"; then
  FINISHED=1; code=0
  reject_release "post-deploy health check failed" || code=$?
  exit "$code"
fi
echo "$PREV_TAG" > "$STATE/previous_tag"
echo "$NEW_TAG" > "$STATE/current_tag"
PHASE=done
record_result deploy DEPLOYED done "$PREV_TAG" "$NEW_TAG" "$NEW_TAG" "verdict $LAST_VERDICT"
echo "deployed $NEW_TAG"
FINISHED=1
