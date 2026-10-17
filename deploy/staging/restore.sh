#!/usr/bin/env bash
# Restore the staging databases from a pre-deploy backup (Section H, Steps 5.6 / 8.5). Destructive: it replaces
# the platform database. Requires the explicit --confirm flag and stops trading first.
#
#   deploy/staging/restore.sh <platform-dump> <image-tag> --confirm
#
# After a restore, trading authorizations are NOT trusted from the restored rows alone: the go-live gate checks
# the append-only revocation journal (see app.golive) so a restore cannot resurrect a revoked authorization.
set -euo pipefail
DUMP="${1:?dump file}"; TAG="${2:?image tag}"; [ "${3:-}" = "--confirm" ] || { echo "add --confirm"; exit 2; }
HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.staging.yml")
sha256sum -c "$HERE/.deploy-state/backup-$(basename "$DUMP" .dump | sed 's/^platform-//').sha256"
IMAGE_TAG="$TAG" "${COMPOSE[@]}" stop -t 120 bot-trading bot-api
IMAGE_TAG="$TAG" "${COMPOSE[@]}" exec -T postgres sh -c 'dropdb -U cosmicforge --force cosmicforge && createdb -U cosmicforge cosmicforge'
IMAGE_TAG="$TAG" "${COMPOSE[@]}" exec -T postgres pg_restore -U cosmicforge -d cosmicforge < "$DUMP"
IMAGE_TAG="$TAG" "${COMPOSE[@]}" run --rm migrate python -m shared_lib.persistence.sql status
IMAGE_TAG="$TAG" "${COMPOSE[@]}" up -d bot-trading bot-api
echo "restored from $DUMP; verify readiness, reconciliation and protection before reopening anything"
