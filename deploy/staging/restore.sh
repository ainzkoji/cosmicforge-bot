#!/usr/bin/env bash
# Restore the staging platform database from a pre-deploy backup (Section H, Steps 5.6 / 8.5). Destructive: it
# replaces the platform database. Requires the explicit --confirm flag and the owner's authorization.
#
#   deploy/staging/restore.sh <platform-dump> <image-tag> --confirm
#
# Order: the dump's sha256 must equal the one deploy.sh recorded (backup-<stamp>.sha256); every trading process
# is drained (SIGTERM + grace) and every other platform-database client stopped; the database is dropped,
# recreated and restored; the release's migration job brings it to that release's schema and check-compat
# confirms the release can run on it; the stack starts on <image-tag>; each trading process must then pass the
# release gate -- including the post-start broker reconciliation -- before the restore counts as done.
#
# Deliberately NOT restored: the runtime SQLite database (online backups in the runtime-state volume, see the
# runbook) and the go-live revocation anchor (GOLIVE_REVOCATION_ANCHOR, on its own volume). Because the anchor
# survives, a restore cannot resurrect a revoked authorization: the go-live gate refuses
# (LEDGER_BEHIND_REVOCATION_ANCHOR) until an operator reconciles the ledger.
#
# Exit status: 0 restored and accepted; 2 usage or settings; 3 refused (dump or checksum missing or wrong,
# release incompatible with the restored schema); 4 restored but a trading process did not pass the release gate
# (page the operations owner); 5 failed part-way (page the operations owner); 9 another operation holds the lock.
set -euo pipefail
DUMP="${1:?usage: restore.sh <platform-dump> <image-tag> --confirm}"
TAG="${2:?usage: restore.sh <platform-dump> <image-tag> --confirm}"
[ "${3:-}" = "--confirm" ] || { echo "add --confirm"; exit 2; }
# shellcheck source=lib.sh
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
valid_tag "$TAG" || { echo "invalid image tag: $TAG"; exit 2; }
CURRENT="$(cat "$STATE/current_tag" 2>/dev/null || true)"
exec > >(tee -a "$STATE/restore-$RUN_ID.log") 2>&1
acquire_lock "restore $(basename "$DUMP") on $TAG"
PHASE=verify
FINISHED=0

on_exit() {
  local rc="$1" command="$2"
  trap - EXIT
  set +e
  if [ "$FINISHED" != 1 ]; then
    echo "restore FAILED in phase $PHASE (exit $rc): $command -- page the operations owner"
    record_result restore FAILED "$PHASE" "${CURRENT:-unknown}" "$TAG" "none" "exit $rc: $command"
    rc=5
  fi
  release_lock
  exit "$rc"
}
trap 'on_exit $? "$BASH_COMMAND"' EXIT

refuse() { echo "REFUSED: $1"; record_result restore REFUSED "$PHASE" "${CURRENT:-unknown}" "$TAG" \
  "${CURRENT:-unknown}" "$1"; FINISHED=1; exit 3; }

echo "restore $DUMP on $TAG at $STAMP (run $RUN_ID)"
DUMP_STAMP="$(basename "$DUMP" .dump)"; DUMP_STAMP="${DUMP_STAMP#platform-}"
SUMS="$STATE/backup-$DUMP_STAMP.sha256"
[ -s "$DUMP" ] || refuse "$DUMP is missing or empty"
[ -s "$SUMS" ] || refuse "no recorded checksum for this dump ($SUMS)"
EXPECTED="$(awk 'NR == 1 {print $1}' "$SUMS")"
ACTUAL="$(sha256sum "$DUMP" | awk '{print $1}')"
[ "$EXPECTED" = "$ACTUAL" ] || refuse "checksum mismatch for $DUMP (recorded $EXPECTED, found $ACTUAL)"
"$DOCKER" image inspect "cosmicforge/bot-backend:$TAG" >/dev/null

# every platform-database client: what the compose file defines, minus the database, the one-shot job,
# the static portals and the trading processes (drained separately, first)
PHASE=stop
SERVICES="$(compose_at "$TAG" config --services)"
CLIENTS=()
for svc in $SERVICES; do
  case " postgres migrate ${TRADING_SERVICES[*]} " in *" $svc "*) continue ;; esac
  case "$svc" in *-frontend) continue ;; esac
  CLIENTS+=("$svc")
done
for svc in "${TRADING_SERVICES[@]}"; do stop_trading_service "$TAG" "$svc" "${CURRENT:-unknown}"; done
if [ "${#CLIENTS[@]}" -gt 0 ]; then compose_at "$TAG" stop -t 30 "${CLIENTS[@]}"; fi

PHASE=restore
wait_database_ready "$TAG"
compose_at "$TAG" exec -T postgres sh -c 'dropdb -U cosmicforge --force cosmicforge && createdb -U cosmicforge cosmicforge'
compose_at "$TAG" exec -T postgres pg_restore -U cosmicforge -d cosmicforge < "$DUMP"

PHASE=migrate
compose_at "$TAG" run --rm migrate
compose_at "$TAG" run --rm migrate python -m shared_lib.persistence.sql status
schema_compatible "$TAG" || refuse "$TAG cannot run on the restored schema; the stack stays stopped"

PHASE=start
if [ "${#CLIENTS[@]}" -gt 0 ]; then compose_at "$TAG" up -d --no-deps "${CLIENTS[@]}"; fi
echo "$TAG" > "$STATE/current_tag"
for svc in "${TRADING_SERVICES[@]}"; do
  compose_at "$TAG" up -d --no-deps "$svc"
  if ! wait_release_ready "$TAG" "$svc"; then
    echo "restored, but $svc did not pass the release gate: page the operations owner"
    record_result restore RESTORED_NOT_READY start "${CURRENT:-unknown}" "$TAG" "$TAG" "verdict $LAST_VERDICT"
    FINISHED=1; exit 4
  fi
done
PHASE=done
record_result restore RESTORED done "${CURRENT:-unknown}" "$TAG" "$TAG" "from $(basename "$DUMP"); verdict $LAST_VERDICT"
echo "restored from $DUMP on $TAG. The go-live gate re-checks the ledger against the revocation anchor; protection"
echo "and reconciliation were confirmed by the release gate only as far as its verdict says (see last_result.json)."
FINISHED=1
