"""Pre-flight probe for the always-on launcher.

Kept as a real file rather than inlined into the PowerShell script: Windows
PowerShell 5.1 mangles multi-line here-strings passed to ``python -c``, which
silently broke the launcher's database and lease checks.

Prints a single JSON object. Never raises — a failure is reported as
``{"error": ...}`` so the launcher can decide, rather than crashing mid-check.

Usage:
    python scripts/runtime_probe.py database
    python scripts/runtime_probe.py lease
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone


def _bootstrap() -> None:
    """Make the backend importable regardless of the caller's cwd."""
    here = os.path.dirname(os.path.abspath(__file__))
    backend = os.path.join(os.path.dirname(here), "backends", "bot-backend")
    shared = os.path.join(os.path.dirname(here), "backends", "shared")
    for path in (backend, shared):
        if path not in sys.path:
            sys.path.insert(0, path)
    os.chdir(backend)

    # Load the backend's own .env explicitly. Relying on find_dotenv() would
    # walk up from THIS file (scripts/), miss the backend .env, and silently
    # resolve the legacy data/bot.db instead of the canonical database.
    env_path = os.path.join(backend, ".env")
    if os.path.exists(env_path):
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_path, override=True)


def probe_database() -> dict:
    from app.core.config import settings
    from shared_lib.persistence.db import DB

    path = DB().path
    exists = os.path.exists(path)
    return {
        "path": path,
        "exists": exists,
        "size": os.path.getsize(path) if exists else 0,
        "role": str(getattr(settings, "DATABASE_ROLE", "development")),
    }


def probe_lease() -> dict:
    from app.ops.runtime_ownership import (
        RuntimeOwnership,
        current_owner,
        ensure_ownership_schema,
    )
    from shared_lib.persistence.db import DB

    db = DB()
    ensure_ownership_schema(db)
    owner = current_owner(db, db.path)
    if not owner:
        return {"held": False}

    probe = RuntimeOwnership(db, database_path=db.path)
    return {
        "held": True,
        "pid": owner.get("pid"),
        "hostname": owner.get("hostname"),
        "heartbeat_at": owner.get("heartbeat_at"),
        "runtime_session_id": owner.get("runtime_session_id"),
        "stale": bool(probe._is_stale(owner.get("heartbeat_at"), datetime.now(timezone.utc))),
        "pid_alive": bool(RuntimeOwnership._pid_alive(int(owner.get("pid") or -1))),
    }


def main() -> int:
    what = sys.argv[1] if len(sys.argv) > 1 else "database"
    try:
        _bootstrap()
        result = probe_database() if what == "database" else probe_lease()
    except Exception as exc:
        result = {"error": f"{type(exc).__name__}: {exc}"}
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
