from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BOT_BACKEND = ROOT / "backends" / "bot-backend"
SHARED = ROOT / "backends" / "shared"
sys.path.insert(0, str(BOT_BACKEND))
sys.path.insert(0, str(SHARED))

from app.signals.signal_expiry import SignalExpiryUpdater  # noqa: E402
from app.signals.signal_performance import SignalPerformanceUpdater  # noqa: E402
from app.signals.signal_scheduler_config import (  # noqa: E402
    LOCK_STATUS_UPDATE,
    STATUS_UPDATE_LOCK_TTL_SECONDS,
)
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    acquire_signal_operation_lock,
    is_status_updater_paused,
    release_signal_operation_lock,
)


def load_dotenv(path: Path = BOT_BACKEND / ".env") -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def run_update(
    db_path: str | None = None,
    *,
    scheduled: bool = False,
    ignore_pause: bool = False,
    lock_ttl_seconds: int = STATUS_UPDATE_LOCK_TTL_SECONDS,
) -> dict:
    if db_path:
        migrate(db_path)
        db = DB(path=db_path)
    else:
        migrate()
        db = DB()
    if scheduled and is_status_updater_paused(db=db) and not ignore_pause:
        return {"checked": 0, "expired": 0, "entry_triggered": 0, "tp1_hit": 0, "tp2_hit": 0, "tp3_hit": 0, "sl_hit": 0, "ambiguous": 0, "invalidated": 0, "paused": True, "errors": []}
    lock_acquired = False
    if scheduled:
        lock_acquired = acquire_signal_operation_lock(
            LOCK_STATUS_UPDATE,
            lock_ttl_seconds,
            metadata={"script": "update_signal_statuses"},
            db=db,
        )
        if not lock_acquired:
            return {"checked": 0, "expired": 0, "entry_triggered": 0, "tp1_hit": 0, "tp2_hit": 0, "tp3_hit": 0, "sl_hit": 0, "ambiguous": 0, "invalidated": 0, "lock_not_acquired": True, "errors": []}
    try:
        performance_summary = SignalPerformanceUpdater(db=db).update_signal_performance()
        expiry_summary = SignalExpiryUpdater(db=db).expire_due_signals()
        return {
            "checked": int(performance_summary.get("checked", 0)) + int(expiry_summary.get("checked", 0)),
            "expired": int(performance_summary.get("expired", 0)) + int(expiry_summary.get("expired", 0)),
            "entry_triggered": int(performance_summary.get("entry_triggered", 0)),
            "tp1_hit": int(performance_summary.get("tp1_hit", 0)),
            "tp2_hit": int(performance_summary.get("tp2_hit", 0)),
            "tp3_hit": int(performance_summary.get("tp3_hit", 0)),
            "sl_hit": int(performance_summary.get("sl_hit", 0)),
            "ambiguous": int(performance_summary.get("ambiguous", 0)),
            "invalidated": int(performance_summary.get("invalidated", 0)),
            "scheduled": scheduled,
            "errors": list(performance_summary.get("errors", [])) + list(expiry_summary.get("errors", [])),
        }
    finally:
        if lock_acquired:
            release_signal_operation_lock(LOCK_STATUS_UPDATE, db=db)


def main() -> int:
    parser = argparse.ArgumentParser(description="Update Crypto Signal Center signal statuses and performance.")
    parser.add_argument("--db-path", default=None, help="Optional SQLite database path for tests/manual runs.")
    parser.add_argument("--scheduled", action="store_true", help="Respect pause switch and prevent overlapping status updates.")
    parser.add_argument("--ignore-pause", action="store_true", help="Explicitly bypass status_updater_paused.")
    parser.add_argument("--lock-ttl-seconds", type=int, default=STATUS_UPDATE_LOCK_TTL_SECONDS)
    args = parser.parse_args()
    load_dotenv()
    summary = run_update(
        db_path=args.db_path,
        scheduled=args.scheduled,
        ignore_pause=args.ignore_pause,
        lock_ttl_seconds=args.lock_ttl_seconds,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if summary.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
