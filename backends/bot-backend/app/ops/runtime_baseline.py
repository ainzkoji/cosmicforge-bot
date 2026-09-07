"""[RUNTIME_BASELINE] — prove which code, interpreter and database are running.

The audit found that a local HEAD did not represent the running application.
This module reports the identity of the process at startup so that trading
evidence can always be tied back to a specific revision and database file.

It never prints secrets: only paths, sizes, versions and identifiers.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_APP_ROOT = Path(__file__).resolve().parents[2]


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(
            ["git", *args],
            cwd=str(_APP_ROOT),
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    value = (out.stdout or "").strip()
    return value or None


def collect_runtime_baseline(
    *,
    db_path: str | Path | None = None,
    execution_mode: str | None = None,
    broker_environment: str | None = None,
    environment_name: str | None = None,
) -> dict[str, Any]:
    """Return the identity of this process. Safe to call more than once."""
    revision = _git("rev-parse", "HEAD")
    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    dirty = _git("status", "--porcelain")

    db_size = 0
    db_exists = False
    schema_version: int | None = None
    resolved_db: str | None = None
    if db_path:
        resolved = Path(db_path).resolve()
        resolved_db = str(resolved)
        db_exists = resolved.exists()
        if db_exists:
            db_size = resolved.stat().st_size
            try:
                import sqlite3

                with sqlite3.connect(str(resolved), timeout=5) as conn:
                    schema_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            except Exception:
                schema_version = None

    return {
        "code_revision": revision,
        "branch": branch,
        "working_tree_dirty": bool(dirty) if dirty is not None else None,
        "process_id": os.getpid(),
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "application_root": str(_APP_ROOT),
        "resolved_db_path": resolved_db,
        "db_exists": db_exists,
        "db_size": db_size,
        "db_schema_version": schema_version,
        "execution_mode": execution_mode,
        "broker_environment": broker_environment,
        "environment_name": environment_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def format_runtime_baseline(baseline: dict[str, Any]) -> str:
    fields = " ".join(f"{k}={v}" for k, v in baseline.items())
    return f"[RUNTIME_BASELINE] {fields}"


def emit_runtime_baseline(**kwargs: Any) -> dict[str, Any]:
    baseline = collect_runtime_baseline(**kwargs)
    print(format_runtime_baseline(baseline))
    return baseline
