from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args()

    if not args.confirm:
        print("Refusing to reset Phase 6 counters without --confirm")
        return 2

    conn = sqlite3.connect(args.db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    try:
        lockout = conn.execute(
            "SELECT * FROM tradingview_safety_lockouts WHERE bot_instance_id = ? AND is_locked = 1",
            (args.bot_id,),
        ).fetchone()
        if lockout:
            print("Refusing to reset counters: active TradingView safety lockout exists")
            print(json.dumps(dict(lockout), indent=2))
            return 3

        pending = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM external_signal_queue
            WHERE bot_id = ? AND status IN ('PENDING','CLAIMED')
            """,
            (args.bot_id,),
        ).fetchone()["c"]
        if int(pending):
            print(f"Refusing to reset counters: {pending} pending/claimed queue rows exist")
            return 4

        unprotected = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM position_lifecycle_state
            WHERE bot_instance_id = ?
              AND COALESCE(exchange_position_active, 0) = 1
              AND (
                sl_order_id IS NULL OR tp_order_id IS NULL
                OR sl_order_id LIKE 'DUPLICATE_%'
                OR tp_order_id LIKE 'DUPLICATE_%'
              )
            """,
            (args.bot_id,),
        ).fetchone()["c"]
        if int(unprotected):
            print(f"Refusing to reset counters: {unprotected} unprotected active positions exist")
            return 5

        now = utc_now()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tradingview_phase6_rate_limit_resets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_instance_id TEXT NOT NULL,
                reset_at TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO tradingview_phase6_rate_limit_resets (
                bot_instance_id, reset_at, reason, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (args.bot_id, now, args.reason, now),
        )
        conn.commit()
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "reset": True,
                "bot_id": args.bot_id,
                "reset_at": now,
                "reason": args.reason,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
