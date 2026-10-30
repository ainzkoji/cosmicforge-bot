#!/usr/bin/env bash
# Run the legacy trading-runtime modules on PostgreSQL (Step 5.1, EXT-DB-01), exactly as CI does.
#   LEGACY_TEST_POSTGRES_URL=postgresql://user@host:port/postgres bash tests/run_legacy_postgres.sh [pytest args...]
# Run from backends/bot-backend. Modules: tests/legacy_postgres_modules.txt; deselected SQLite-only tests (with
# their reasons): tests/legacy_postgres_test_side.txt.
set -euo pipefail
set -f   # node ids contain [..]; never glob them
: "${LEGACY_TEST_POSTGRES_URL:?LEGACY_TEST_POSTGRES_URL must name a server where throwaway databases may be created}"
PYTHON="${PYTHON:-python}"
here="$(cd "$(dirname "$0")" && pwd)"
mapfile -t modules < <(sed -e 's/#.*//' -e 's/[[:space:]]*$//' "$here/legacy_postgres_modules.txt" | grep -v '^$')
deselect=()
while IFS= read -r id; do
  deselect+=(--deselect "$id")
done < <(sed -e 's/[[:space:]]*#.*//' -e 's/[[:space:]]*$//' "$here/legacy_postgres_test_side.txt" | grep -v '^$')
sep=":"; case "${OSTYPE:-}" in msys*|cygwin*) sep=";" ;; esac   # Git Bash runs a Windows Python
export PYTHONPATH="tests${PYTHONPATH:+$sep$PYTHONPATH}"
exec "$PYTHON" -m pytest -p legacy_pg_plugin "${modules[@]}" "${deselect[@]}" -q --tb=short -p no:cacheprovider "$@"
