#!/usr/bin/env python3
"""Scheduled, verified backup of the PLATFORM PostgreSQL database (master plan Section J follow-up).

The platform database holds what every worker shares: account assignments and their fencing history, the durable
job queue and order intents, order budgets, launch records, the alerting state. ``deploy/staging/deploy.sh`` dumps
it before a deploy; this script is the scheduled copy (``cosmicforge-platform-db-backup.timer``):

    python scripts/backup_platform_db.py --output-dir /var/backups/cosmicforge --keep 14

1. the source is ``--url``, else ``PLATFORM_DATABASE_URL``, else that line of the backend's ``.env`` (``--dotenv``);
   only ``postgresql://`` is accepted (a SQLite platform file is a plain file: ``backup_trading_db.py --database``);
2. ``pg_dump -Fc`` into ``platform-<stamp>.dump.partial``. The password goes to pg_dump through ``PGPASSWORD`` in
   its environment -- never on a command line, where any local user could read it from the process list;
3. the dump is verified with ``pg_restore --list`` (it must parse and contain table data);
4. atomic rename, ``platform-<stamp>.dump.json`` manifest (size, SHA-256, table-data entries);
5. retention: all but the newest ``--keep`` dumps are deleted.

Every run that knows its output directory writes ``<output-dir>/platform-backup-status.json`` (same schema as
``backup-status.json``); the operations tick raises BACKUP_FAILURE from it (``evaluate_platform_backup_status``).

Exit status: 0 success; 1 bad arguments / no source; 2 the dump failed; 3 the dump failed verification; 4 the dump
is good but retention failed. Standard library only (plus the PostgreSQL client tools).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backup_trading_db import BACKEND, remove, sha256_of, write_status  # noqa: E402

PREFIX = "platform"
NAME = re.compile(rf"^{PREFIX}-\d{{8}}T\d{{6}}Z\.dump$")
STATUS_FILE = "platform-backup-status.json"
EXIT_OK, EXIT_USAGE, EXIT_BACKUP_FAILED, EXIT_INTEGRITY_FAILED, EXIT_RETENTION_FAILED = 0, 1, 2, 3, 4
DUMP_TIMEOUT_S = 3 * 3600
_RUN: dict = {"output_dir": None, "file": None, "failure": None}


def log(message: str) -> None:
    print(f"[PLATFORM_DB_BACKUP] {message}", flush=True)
    if message.startswith(("FAILED", "WARNING")):
        _RUN["failure"] = message[:300]


def resolve_url(explicit: str | None, dotenv: str | None) -> str | None:
    if explicit:
        return explicit
    if os.environ.get("PLATFORM_DATABASE_URL"):
        return os.environ["PLATFORM_DATABASE_URL"]
    env_file = Path(dotenv) if dotenv else BACKEND / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.strip().startswith("PLATFORM_DATABASE_URL="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def libpq_environment(url: str) -> dict[str, str]:
    """``PG*`` variables for a ``postgresql://user:password@host:port/db?sslmode=...`` URL. Raises ValueError."""
    parts = urlsplit(url)
    if parts.scheme not in ("postgresql", "postgres") or not parts.path.strip("/"):
        raise ValueError("PLATFORM_DATABASE_URL must be postgresql://user@host[:port]/database")
    env = {"PGDATABASE": unquote(parts.path.lstrip("/")), "PGHOST": parts.hostname or "localhost",
           "PGPORT": str(parts.port or 5432)}
    if parts.username:
        env["PGUSER"] = unquote(parts.username)
    if parts.password:
        env["PGPASSWORD"] = unquote(parts.password)
    query = parse_qs(parts.query)
    if query.get("sslmode"):
        env["PGSSLMODE"] = query["sslmode"][0]
    return env


def redacted(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.username or ''}@{parts.hostname or ''}:{parts.port or 5432}{parts.path}"


def tool(name: str, explicit: str | None) -> str | None:
    return explicit or shutil.which(name)


def prune(output_dir: Path, keep: int) -> list[str]:
    dumps = sorted((p for p in output_dir.iterdir() if p.is_file() and NAME.match(p.name)),
                   key=lambda p: p.name, reverse=True)
    removed = []
    for old in dumps[keep:]:
        old.unlink()
        manifest = old.with_name(old.name + ".json")
        if manifest.exists():
            manifest.unlink()
        removed.append(old.name)
    return removed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", help="source (default: PLATFORM_DATABASE_URL, then the backend .env)")
    parser.add_argument("--dotenv", help="read PLATFORM_DATABASE_URL from this file when it is not in the environment")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--keep", type=int, default=14)
    parser.add_argument("--pg-dump", help="pg_dump executable (default: on PATH)")
    parser.add_argument("--pg-restore", help="pg_restore executable (default: on PATH)")
    args = parser.parse_args(argv)
    _RUN.update(output_dir=None, file=None, failure=None)
    try:
        code = _backup(args)
    except Exception as exc:  # noqa: BLE001 -- an unexpected error is a failed backup, recorded like the others
        log(f"FAILED unexpected: {type(exc).__name__}")
        code = EXIT_BACKUP_FAILED
    if _RUN["output_dir"] is not None and Path(_RUN["output_dir"]).is_dir():
        write_status(Path(_RUN["output_dir"]), tool="backup_platform_db", code=code, file=_RUN["file"],
                     message=_RUN["failure"], status_file=STATUS_FILE)
    return code


def _backup(args: argparse.Namespace) -> int:
    _RUN["output_dir"] = Path(args.output_dir).expanduser()
    if args.keep < 1:
        log("FAILED --keep must be at least 1")
        return EXIT_USAGE
    url = resolve_url(args.url, args.dotenv)
    if not url:
        log("FAILED no PLATFORM_DATABASE_URL (environment, --url or the backend .env)")
        return EXIT_USAGE
    try:
        pg_env = libpq_environment(url)
    except ValueError as exc:
        log(f"FAILED {exc}")
        return EXIT_USAGE
    pg_dump, pg_restore = tool("pg_dump", args.pg_dump), tool("pg_restore", args.pg_restore)
    if not pg_dump or not pg_restore:
        log("FAILED pg_dump / pg_restore not found (install the PostgreSQL client tools)")
        return EXIT_USAGE
    output_dir = _RUN["output_dir"].resolve()
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            output_dir.chmod(0o700)
    except OSError as exc:
        log(f"FAILED cannot use output directory {output_dir}: {type(exc).__name__}")
        return EXIT_USAGE
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    final = output_dir / f"{PREFIX}-{stamp}.dump"
    partial = final.with_name(final.name + ".partial")
    env = {**os.environ, **pg_env}
    log(f"source={redacted(url)} output={output_dir}")
    started = time.monotonic()
    remove(partial)
    try:
        done = subprocess.run([pg_dump, "-Fc", "--no-owner", "--no-privileges", "-f", str(partial)], env=env,
                              capture_output=True, text=True, timeout=DUMP_TIMEOUT_S, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        remove(partial)
        log(f"FAILED pg_dump: {type(exc).__name__}")
        return EXIT_BACKUP_FAILED
    if done.returncode != 0 or not partial.is_file():
        remove(partial)
        secret = pg_env.get("PGPASSWORD")
        reason = (done.stderr or "").strip().splitlines()[-1:] or ["no output"]
        text = reason[0].replace(secret, "***") if secret else reason[0]
        log(f"FAILED pg_dump exit {done.returncode}: {text[:200]}")
        return EXIT_BACKUP_FAILED
    try:
        listed = subprocess.run([pg_restore, "--list", str(partial)], capture_output=True, text=True,
                                timeout=600, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        listed = None
        log(f"WARNING pg_restore --list could not run: {type(exc).__name__}")
    entries = [l for l in (listed.stdout if listed else "").splitlines() if l and not l.startswith(";")]
    table_data = sum(1 for l in entries if " TABLE DATA " in l)
    if listed is None or listed.returncode != 0 or not table_data:
        remove(partial)
        log(f"FAILED verification: pg_restore --list exit {getattr(listed, 'returncode', None)}, "
            f"{table_data} table-data entries")
        return EXIT_INTEGRITY_FAILED
    os.replace(partial, final)
    if os.name != "nt":
        final.chmod(0o600)
    manifest = {"file": final.name, "created_at": datetime.now(timezone.utc).isoformat(), "format": "pg_dump custom",
                "source": redacted(url), "size_bytes": final.stat().st_size, "sha256": sha256_of(final),
                "toc_entries": len(entries), "table_data_entries": table_data, "verified": "pg_restore --list"}
    manifest_path = final.with_name(final.name + ".json")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if os.name != "nt":
        manifest_path.chmod(0o600)
    _RUN["file"] = final.name
    log(f"OK backup={final} size_bytes={manifest['size_bytes']} table_data={table_data} "
        f"seconds={time.monotonic() - started:.1f}")
    try:
        removed = prune(output_dir, args.keep)
    except Exception as exc:  # noqa: BLE001
        log(f"WARNING dump written, but retention failed: {type(exc).__name__}")
        return EXIT_RETENTION_FAILED
    log(f"retention keep={args.keep} removed={len(removed)}")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
