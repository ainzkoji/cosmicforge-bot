"""One JSON answer about the canonical trading runtime. Read-only.

Kept as a real file rather than inlined into PowerShell: Windows PowerShell 5.1
mangles multi-line here-strings passed to ``python -c``, which is what silently
broke the launcher's database and lease checks once already.

    python scripts/runtime_status.py            # full status
    python scripts/runtime_status.py --port 9000

Prints a single JSON object. Never raises: a failure is reported as
``{"status": "ERROR", ...}`` so the caller decides rather than crashing
mid-check.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED = os.path.join(REPO_ROOT, "backends", "shared")


def bootstrap() -> None:
    for path in (BACKEND, SHARED):
        if path not in sys.path:
            sys.path.insert(0, path)
    os.chdir(BACKEND)
    # This process inspects the runtime; it must never be refused by the very
    # preflight it is reporting on.
    os.environ["COSMICFORGE_SKIP_RUNTIME_PREFLIGHT"] = "1"
    env = os.path.join(BACKEND, ".env")
    if os.path.exists(env):
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env, override=True)


def process_tree(pid: int | None) -> dict:
    """The listener, its parent, and how long it has been up."""
    if not pid:
        return {}
    try:
        import psutil

        proc = psutil.Process(pid)
        parent = proc.parent()
        return {
            "pid": proc.pid,
            "parent_pid": parent.pid if parent else None,
            "parent_name": parent.name() if parent else None,
            "created_at": proc.create_time(),
            "cmdline": " ".join(proc.cmdline()),
            "uptime_seconds": None,
        }
    except Exception:
        return {"pid": pid}


def bot_health(db) -> list[dict]:
    try:
        with db.connect() as conn:
            rows = conn.execute(
                "SELECT id, status, mode, bot_health_status, bot_health_reason_code, "
                "last_run_at FROM bot_instances WHERE status='active'"
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    bootstrap()
    payload: dict = {}
    try:
        from app.ops.runtime_preflight import preflight
        from shared_lib.persistence.db import DB

        result = preflight(port=args.port)
        payload = result.to_dict()

        database = DB()
        payload["running"] = result.status.value in {
            "ALREADY_RUNNING", "PORT_OCCUPIED_BY_CANONICAL_RUNTIME",
        }
        payload["process"] = process_tree(result.port_pid or result.lease_pid)
        payload["bots"] = bot_health(database)

        with database.connect() as conn:
            payload["sessions_marked_running"] = int(conn.execute(
                "SELECT COUNT(*) FROM runtime_sessions WHERE status='RUNNING'"
            ).fetchone()[0])
    except Exception as exc:
        payload = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}",
                   "running": False}

    print(json.dumps(payload, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
