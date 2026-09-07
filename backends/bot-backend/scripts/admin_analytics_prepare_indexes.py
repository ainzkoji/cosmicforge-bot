from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path


BACKENDS_ROOT = Path(__file__).resolve().parents[2]
BOT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BACKENDS_ROOT / "shared"))

from shared_lib.persistence.admin_analytics import ensure_admin_analytics_foundation


DEFAULT_DB_PATH = REPO_ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"


def resolve_db_path(value: str | None = None) -> Path:
    raw = value or os.environ.get("DATABASE_URL") or str(DEFAULT_DB_PATH)
    if raw.startswith("sqlite:///"):
        raw = raw[len("sqlite:///") :]
    path = Path(raw)
    if path.is_absolute():
        return path
    if value:
        return (Path.cwd() / path).resolve()
    return (BOT_ROOT / path).resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare Admin Analytics Profitability/ML indexes and snapshot tables."
    )
    parser.add_argument("--db-path", help="SQLite DB path or sqlite:/// URL. Defaults to DATABASE_URL.")
    parser.add_argument("--skip-analyze", action="store_true", help="Create indexes without running ANALYZE.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.db_path)
    started = time.perf_counter()
    print(f"[admin-analytics-indexes] db={db_path}")

    with sqlite3.connect(str(db_path), timeout=60) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=60000")
        indexes = ensure_admin_analytics_foundation(conn, create_indexes=True, analyze=not args.skip_analyze)

    elapsed_ms = (time.perf_counter() - started) * 1000
    print(f"[admin-analytics-indexes] indexes_verified={len(indexes)}")
    for index_name in indexes:
        print(f"[admin-analytics-indexes] index={index_name}")
    print(f"[admin-analytics-indexes] elapsed_ms={elapsed_ms:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
