from __future__ import annotations

import argparse
import json
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_runtime_fingerprint(runtime_url: str) -> dict:
    try:
        with urllib.request.urlopen(runtime_url, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return {"reachable": False, "error": str(exc)}
    fp = data.get("tradingview_runtime_fingerprint") if isinstance(data, dict) else None
    if not isinstance(fp, dict):
        return {"reachable": True, "fingerprint_present": False}
    fp["reachable"] = True
    fp["fingerprint_present"] = True
    return fp


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--runtime-url", default="http://127.0.0.1:9000/health")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args()

    if not args.confirm:
        print("Refusing to clear lockout without --confirm")
        return 2

    runtime = fetch_runtime_fingerprint(args.runtime_url)
    if not runtime.get("reachable") or not runtime.get("fingerprint_present"):
        print("Refusing to clear lockout: runtime fingerprint unavailable")
        print(json.dumps(runtime, indent=2, default=str))
        return 3
    if not runtime.get("phase6_gate_available"):
        print("Refusing to clear lockout: runtime phase6_gate_available is false")
        print(json.dumps(runtime, indent=2, default=str))
        return 3
    if runtime.get("active_safety_lockout") is False:
        print("Runtime already reports no active safety lockout")

    conn = sqlite3.connect(args.db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    try:
        pending = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM external_signal_queue
            WHERE bot_id = ? AND status IN ('PENDING','CLAIMED')
            """,
            (args.bot_id,),
        ).fetchone()["c"]
        if int(pending):
            print(f"Refusing to clear lockout: {pending} pending/claimed TradingView queue rows exist")
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
            print(f"Refusing to clear lockout: {unprotected} unprotected active positions exist")
            return 5

        now = utc_now()
        conn.execute(
            """
            INSERT INTO tradingview_safety_lockouts (
                bot_instance_id, is_locked, reason, created_at, updated_at
            ) VALUES (?, 0, ?, ?, ?)
            ON CONFLICT(bot_instance_id) DO UPDATE SET
                is_locked = 0,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (args.bot_id, f"CLEARED: {args.reason}", now, now),
        )
        conn.commit()
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "cleared": True,
                "bot_id": args.bot_id,
                "reason": args.reason,
                "runtime_pid": runtime.get("pid"),
                "phase6_gate_code_version": runtime.get("phase6_gate_code_version"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
