#!/usr/bin/env bash
# Roll the staging stack back to an earlier image (Section H, Step 5.6).
#
#   deploy/staging/rollback.sh            # to the previous tag recorded by deploy.sh
#   deploy/staging/rollback.sh <tag>      # to a named tag (deploy.sh passes the release it replaced)
#
# A rollback is a REDEPLOY only when the target image's schema range accepts the database as it is now
# (python -m shared_lib.persistence.sql check-compat, run by the target image). Platform migrations are additive,
# so an older image normally runs on a newer schema; a version marked irreversible makes the check fail, and then
# this script REFUSES before it touches anything: an incompatible binary is never started against an irreversible
# schema. The way back is then restore.sh with the pre-deploy backup, under the owner's authorization.
#
# The swap is the deploy's: API tier, then each trading process drained (SIGTERM + grace) and replaced. The
# rolled-back trading process must pass the same release gate as a deploy -- role, supervisor, runtime lease,
# post-start broker reconciliation, emergency routes, LIVE gate closed -- before the rollback counts as done.
#
# Exit status: 0 rolled back and accepted; 2 no target / bad tag; 3 refused (schema incompatible, nothing
# changed); 4 the target serves but did not pass the release gate (page the operations owner); 5 the rollback
# failed part-way (page the operations owner); 9 another operation holds the lock.
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
# deploy.sh names the release it is rolling back FROM (its current_tag still names the accepted one)
CURRENT="${ROLLBACK_FROM_TAG:-$(cat "$STATE/current_tag" 2>/dev/null || true)}"
TARGET="${1:-$(cat "$STATE/previous_tag" 2>/dev/null || true)}"
[ -n "$TARGET" ] && [ "$TARGET" != none ] || { echo "no rollback target recorded"; exit 2; }
valid_tag "$TARGET" || { echo "invalid image tag: $TARGET"; exit 2; }
exec > >(tee -a "$STATE/rollback-$RUN_ID.log") 2>&1
acquire_lock "rollback ${CURRENT:-?} -> $TARGET"
PHASE=compat
FINISHED=0

on_exit() {
  local rc="$1" command="$2"
  trap - EXIT
  set +e
  if [ "$FINISHED" != 1 ]; then
    echo "rollback FAILED in phase $PHASE (exit $rc): $command -- page the operations owner"
    record_result rollback FAILED "$PHASE" "${CURRENT:-unknown}" "$TARGET" \
      "$(cat "$STATE/current_tag" 2>/dev/null || echo unknown)" "exit $rc: $command"
    rc=5
  fi
  release_lock
  exit "$rc"
}
trap 'on_exit $? "$BASH_COMMAND"' EXIT

echo "rollback ${CURRENT:-?} -> $TARGET at $STAMP (run $RUN_ID)"
"$DOCKER" image inspect "cosmicforge/bot-backend:$TARGET" >/dev/null
if ! schema_compatible "$TARGET"; then
  echo "REFUSED: $TARGET cannot run on the current schema. Use restore.sh with the pre-deploy backup."
  record_result rollback REFUSED_SCHEMA compat "${CURRENT:-unknown}" "$TARGET" "${CURRENT:-unknown}" \
    "check-compat refused the target's schema range"
  FINISHED=1; exit 3
fi

PHASE=api
archive_logs "$TARGET" "${CURRENT:-unknown}" "${API_SERVICES[@]}"
compose_at "$TARGET" up -d --no-deps "${API_SERVICES[@]}"
echo "$TARGET" > "$STATE/current_tag"     # from here on the stack runs the target, accepted or not

PHASE=trading
if ! roll_trading "$TARGET" "${CURRENT:-unknown}"; then
  echo "rolled-back trading process did not pass the release gate: page the operations owner"
  record_result rollback ROLLED_BACK_NOT_READY trading "${CURRENT:-unknown}" "$TARGET" "$TARGET" \
    "verdict $LAST_VERDICT"
  FINISHED=1; exit 4
fi
PHASE=done
record_result rollback ROLLED_BACK done "${CURRENT:-unknown}" "$TARGET" "$TARGET" "verdict $LAST_VERDICT"
echo "rolled back to $TARGET"
FINISHED=1
