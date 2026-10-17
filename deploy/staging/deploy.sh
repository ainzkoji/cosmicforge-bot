#!/usr/bin/env bash
# Deploy an image revision to the staging stack (Section H, Step 5.6).
#
#   deploy/staging/deploy.sh <image-tag>
#
# Order, and why:
#  1. preflight   compose file valid; new images exist; their schema range is read from the image itself
#  2. backup      pg_dump of the platform database + online SQLite backup of the runtime database; deploy stops
#                 if either fails (there is no deploy without a restore point)
#  3. migrate     the one-shot migration job of the NEW image (additive versions only; checksummed)
#  4. rollback    is the PREVIOUS image still compatible with the migrated schema? recorded before anything
#     check       restarts, so the operator knows whether a rollback can be a redeploy or must be a restore
#  5. API tier    bot-api, user-backend, admin-backend, portals: replaced; they hold no trading state
#  6. trading     the single trading process is stopped GRACEFULLY (SIGTERM, 120 s grace: the runtime finishes
#                 its cycle, joins in-flight work and quiesces) and started on the new image. Exchange-side
#                 protective stops rest at the venue throughout; the new process reconstructs broker truth and
#                 resolves unknown submissions before it submits anything
#  7. verify      trading readiness (/health?ready=1) and the health check must pass within the deadline;
#                 otherwise rollback.sh runs automatically
set -euo pipefail
NEW_TAG="${1:?usage: deploy.sh <image-tag>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.staging.yml")
STATE="$HERE/.deploy-state"; mkdir -p "$STATE" "$HERE/backups"
PREV_TAG="$(cat "$STATE/current_tag" 2>/dev/null || echo none)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="$STATE/deploy-$STAMP.log"
exec > >(tee -a "$LOG") 2>&1
echo "deploy $PREV_TAG -> $NEW_TAG at $STAMP"

# 1. preflight
IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" config -q
for svc in bot-backend user-backend admin-backend user-frontend admin-frontend; do
  docker image inspect "cosmicforge/$svc:$NEW_TAG" >/dev/null
done
RANGE="$(docker run --rm -w /srv/cosmicforge/backends/shared "cosmicforge/bot-backend:$NEW_TAG" python -m shared_lib.persistence.sql build-range)"
echo "new image schema range: $RANGE"

# 2. backup (restore point)
IMAGE_TAG="${PREV_TAG/none/$NEW_TAG}" "${COMPOSE[@]}" exec -T postgres pg_dump -U cosmicforge -Fc cosmicforge > "$HERE/backups/platform-$STAMP.dump"
test -s "$HERE/backups/platform-$STAMP.dump"
if [ "$PREV_TAG" != none ]; then
  IMAGE_TAG="$PREV_TAG" "${COMPOSE[@]}" exec -T bot-trading python /srv/cosmicforge/scripts/backup_trading_db.py \
    --output-dir /var/lib/cosmicforge/backups --keep 14 --compress
fi
sha256sum "$HERE/backups/platform-$STAMP.dump" | tee "$STATE/backup-$STAMP.sha256"

# 3. migrate with the new image
IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" run --rm migrate

# 4. can the previous image still run on this schema?
if [ "$PREV_TAG" != none ]; then
  PREV_RANGE="$(docker run --rm -w /srv/cosmicforge/backends/shared "cosmicforge/bot-backend:$PREV_TAG" python -m shared_lib.persistence.sql build-range)"
  PMIN="$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['app_schema_min'])" "$PREV_RANGE")"
  PMAX="$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['app_schema_max'])" "$PREV_RANGE")"
  if IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" run --rm migrate python -m shared_lib.persistence.sql check-compat \
       --app-min "$PMIN" --app-max "$PMAX"; then
    echo "rollback path: REDEPLOY of $PREV_TAG is schema-compatible" | tee "$STATE/rollback-mode"
  else
    echo "rollback path: RESTORE REQUIRED (previous image incompatible with migrated schema)" | tee "$STATE/rollback-mode"
  fi
fi

# 5. API tier
IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" up -d --no-deps bot-api user-backend admin-backend user-frontend admin-frontend

# 6. trading process: graceful stop, then start on the new image
IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" stop -t 120 bot-trading || true
IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" up -d --no-deps bot-trading healthcheck

# 7. verify
deadline=$((SECONDS + 300))
until IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" exec -T bot-trading curl -fsS 'http://127.0.0.1:8000/health?ready=1' >/dev/null; do
  if [ $SECONDS -ge $deadline ]; then
    echo "trading process not ready within 300 s: rolling back"
    exec "$HERE/rollback.sh"
  fi
  sleep 5
done
IMAGE_TAG="$NEW_TAG" "${COMPOSE[@]}" exec -T bot-trading python /srv/cosmicforge/scripts/vps_health_check.py \
  --url http://127.0.0.1:8000 || { echo "post-deploy health check failed: rolling back"; exec "$HERE/rollback.sh"; }
echo "$PREV_TAG" > "$STATE/previous_tag"
echo "$NEW_TAG" > "$STATE/current_tag"
echo "deployed $NEW_TAG"
