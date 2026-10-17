#!/usr/bin/env bash
# Roll the staging stack back to the previous image (Section H, Step 5.6).
#
#   deploy/staging/rollback.sh            # to the previous tag recorded by deploy.sh
#   deploy/staging/rollback.sh <tag>      # to a named tag
#
# A rollback is a REDEPLOY only when the target image's schema range accepts the database as it is now
# (python -m shared_lib.persistence.sql check-compat). Platform migrations are additive, so an older image
# normally runs on a newer schema; a version marked irreversible makes the check fail, and then this script
# REFUSES: an incompatible binary is never started against an irreversible schema. The way back is then
# restore.sh with the pre-deploy backup, under the owner's authorization.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.staging.yml")
STATE="$HERE/.deploy-state"
CURRENT="$(cat "$STATE/current_tag" 2>/dev/null || true)"
TARGET="${1:-$(cat "$STATE/previous_tag" 2>/dev/null || true)}"
[ -n "$TARGET" ] && [ "$TARGET" != none ] || { echo "no rollback target recorded"; exit 2; }
echo "rollback ${CURRENT:-?} -> $TARGET"

RANGE="$(docker run --rm -w /srv/cosmicforge/backends/shared "cosmicforge/bot-backend:$TARGET" python -m shared_lib.persistence.sql build-range)"
MIN="$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['app_schema_min'])" "$RANGE")"
MAX="$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['app_schema_max'])" "$RANGE")"
if ! IMAGE_TAG="$TARGET" "${COMPOSE[@]}" run --rm migrate python -m shared_lib.persistence.sql check-compat \
     --app-min "$MIN" --app-max "$MAX"; then
  echo "REFUSED: $TARGET cannot run on the current schema. Use restore.sh with the pre-deploy backup."
  exit 3
fi
IMAGE_TAG="$TARGET" "${COMPOSE[@]}" up -d --no-deps bot-api user-backend admin-backend user-frontend admin-frontend
IMAGE_TAG="$TARGET" "${COMPOSE[@]}" stop -t 120 bot-trading || true
IMAGE_TAG="$TARGET" "${COMPOSE[@]}" up -d --no-deps bot-trading healthcheck
deadline=$((SECONDS + 300))
until IMAGE_TAG="$TARGET" "${COMPOSE[@]}" exec -T bot-trading curl -fsS 'http://127.0.0.1:8000/health?ready=1' >/dev/null; do
  [ $SECONDS -lt $deadline ] || { echo "rolled-back trading process not ready: page the operations owner"; exit 4; }
  sleep 5
done
echo "$TARGET" > "$STATE/current_tag"
echo "rolled back to $TARGET"
