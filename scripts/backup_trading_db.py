#!/usr/bin/env python3
"""Consistent, verified backup of the production trading database.

The database is SQLite in WAL mode and the runtime writes to it continuously.
Copying ``cosmicforge.db`` with ``cp`` while it is open captures the main file
without the commits still sitting in ``cosmicforge.db-wal`` -- at best stale,
at worst torn. This script uses SQLite's online backup API instead: it reads
one transactionally consistent snapshot, including everything in the WAL,
while the runtime keeps running. The source is opened read-only and its
contents are never modified. Run it as the service user: like any reader of a
WAL database it needs to be able to create the ``-shm`` file beside the source.

    python scripts/backup_trading_db.py --output-dir /var/backups/cosmicforge --keep 14

What it does, in order:

1. resolve the source (``--database``, else ``DATABASE_URL``, else the
   backend's ``.env``) and open it READ-ONLY;
2. check there is room for the copy;
3. take the snapshot into ``<name>.partial``;
4. run ``PRAGMA quick_check`` on the COPY -- a backup that has not been
   verified is not a backup;
5. convert the copy to a single self-contained file (no ``-wal`` sidecar),
   optionally gzip it, and rename it into place atomically;
6. write ``<name>.json`` (size, SHA-256, check result);
7. delete all but the newest ``--keep`` backups.

Exit status: 0 success; 1 bad arguments or source missing; 2 the backup could
not be taken; 3 the copy failed its integrity check; 4 the backup succeeded but
pruning old ones did not. Standard library only.

Every run that got as far as knowing its output directory also writes
``<output-dir>/backup-status.json`` (outcome, exit code, time, the failure
message, and the last successful run carried forward). A failed run therefore
leaves a durable marker even though it wrote no backup and no manifest; the
operations tick (``app.ops.alerting.evaluate_backup_status``) raises
BACKUP_FAILURE from it. Writing the marker never changes the exit status.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BACKEND = REPO_ROOT / "backends" / "bot-backend"
PREFIX = "cosmicforge"
NAME = re.compile(rf"^{PREFIX}-\d{{8}}T\d{{6}}Z\.db(\.gz)?$")

EXIT_OK, EXIT_USAGE, EXIT_BACKUP_FAILED, EXIT_INTEGRITY_FAILED, EXIT_RETENTION_FAILED = 0, 1, 2, 3, 4
STATUS_FILE = "backup-status.json"
STATUS_SCHEMA = "cosmicforge-backup-status-v1"

#: What this run knows for its status marker (set as the run goes).
_RUN: dict = {"output_dir": None, "file": None, "failure": None}


def log(message: str) -> None:
    print(f"[DB_BACKUP] {message}", flush=True)
    if message.startswith(("FAILED", "WARNING")):
        _RUN["failure"] = message[:300]


def write_status(output_dir: Path, *, tool: str, code: int, file: str | None, message: str | None,
                 status_file: str = STATUS_FILE) -> None:
    """Record this run's outcome beside the backups (atomic replace). Never raises: a marker that cannot be
    written is logged, and the run's own exit status still reports the failure to systemd."""
    try:
        path = Path(output_dir) / status_file
        previous = {}
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass
        now = datetime.now(timezone.utc).isoformat()
        ok = code == 0
        status = {"schema": STATUS_SCHEMA, "tool": tool, "outcome": "OK" if ok else "FAILED", "exit_code": code,
                  "finished_at": now, "file": file, "message": None if ok else (message or f"exit {code}")[:300],
                  "last_ok_at": now if ok else (previous.get("last_ok_at") if isinstance(previous, dict) else None)}
        staged = path.with_name(path.name + ".partial")
        staged.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
        if os.name != "nt":
            staged.chmod(0o600)
        os.replace(staged, path)
    except Exception as exc:  # noqa: BLE001 -- the marker is evidence, never a reason to fail differently
        print(f"[DB_BACKUP] WARNING status marker not written: {type(exc).__name__}: {exc}", flush=True)


def database_from_url(url: str) -> Path | None:
    """Resolve a ``sqlite:///`` URL exactly as the backend does."""
    if not url.startswith("sqlite:///"):
        return None
    target = url[len("sqlite:///"):]
    path = Path(target)
    return path if path.is_absolute() else (BACKEND / target).resolve()


def resolve_source(explicit: str | None) -> Path | None:
    if explicit:
        return Path(explicit).expanduser().resolve()
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        env_file = BACKEND / ".env"
        if env_file.is_file():
            for line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.strip().startswith("DATABASE_URL="):
                    url = line.split("=", 1)[1].strip().strip('"').strip("'")
    return database_from_url(url) if url else None


def quick_check(path: Path) -> str:
    """``ok`` when the file is a sound database; otherwise the first findings."""
    conn = sqlite3.connect(str(path), timeout=60)
    try:
        rows = [str(r[0]) for r in conn.execute("PRAGMA quick_check").fetchall()]
    finally:
        conn.close()
    return "ok" if rows == ["ok"] else "; ".join(rows[:5]) or "no result"


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def remove(path: Path) -> None:
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm"), Path(str(path) + "-journal")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def take_snapshot(source: Path, partial: Path) -> int:
    """Copy one consistent snapshot of ``source`` to ``partial``. Returns pages."""
    src = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True, timeout=60)
    try:
        src.execute("PRAGMA busy_timeout=60000")
        dst = sqlite3.connect(str(partial), timeout=60)
        try:
            # One step: the whole copy reads a single snapshot. Stepping in
            # slices restarts whenever the runtime writes, which is always.
            src.backup(dst, pages=-1)
            dst.commit()
            # A self-contained artifact: no WAL sidecar to lose in transit.
            mode = dst.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            if str(mode).lower() != "delete":
                raise RuntimeError(f"could not finalize the copy (journal_mode={mode})")
            return int(dst.execute("PRAGMA page_count").fetchone()[0])
        finally:
            dst.close()
    finally:
        src.close()


def prune(output_dir: Path, keep: int) -> list[str]:
    backups = sorted((p for p in output_dir.iterdir() if p.is_file() and NAME.match(p.name)),
                     key=lambda p: p.name, reverse=True)
    removed = []
    for old in backups[keep:]:
        old.unlink()
        manifest = old.with_name(old.name + ".json")
        if manifest.exists():
            manifest.unlink()
        removed.append(old.name)
    return removed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", help="source database (default: DATABASE_URL, then the backend .env)")
    parser.add_argument("--output-dir", required=True, help="directory that receives the backups")
    parser.add_argument("--keep", type=int, default=14, help="backups to retain (default 14)")
    parser.add_argument("--compress", action="store_true", help="gzip the verified copy")
    args = parser.parse_args(argv)
    _RUN.update(output_dir=None, file=None, failure=None)
    try:
        code = _backup(args)
    except Exception as exc:  # noqa: BLE001 -- an unexpected error is a failed backup, recorded like the others
        log(f"FAILED unexpected: {type(exc).__name__}: {exc}")
        code = EXIT_BACKUP_FAILED
    if _RUN["output_dir"] is not None and Path(_RUN["output_dir"]).is_dir():
        write_status(Path(_RUN["output_dir"]), tool="backup_trading_db", code=code, file=_RUN["file"],
                     message=_RUN["failure"])
    return code


def _backup(args: argparse.Namespace) -> int:
    _RUN["output_dir"] = Path(args.output_dir).expanduser()
    if args.keep < 1:
        log("FAILED --keep must be at least 1")
        return EXIT_USAGE
    source = resolve_source(args.database)
    if source is None or not source.is_file():
        log(f"FAILED source database not found: {source}")
        return EXIT_USAGE
    output_dir = Path(args.output_dir).expanduser().resolve()
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            output_dir.chmod(0o700)
    except OSError as exc:
        log(f"FAILED cannot use output directory {output_dir}: {exc}")
        return EXIT_USAGE

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    final = output_dir / f"{PREFIX}-{stamp}.db"
    partial = output_dir / f"{PREFIX}-{stamp}.db.partial"
    size = source.stat().st_size
    free = shutil.disk_usage(output_dir).free
    # The copy, plus the gzip written beside it before the copy is removed.
    needed = int(size * (1.6 if args.compress else 1.05)) + 64 * 1024 * 1024
    log(f"source={source} size_bytes={size} output={output_dir} free_bytes={free}")
    if free < needed:
        log(f"FAILED not enough free space: need about {needed} bytes, have {free}")
        return EXIT_BACKUP_FAILED

    started = time.monotonic()
    try:
        remove(partial)
        pages = take_snapshot(source, partial)
    except Exception as exc:
        remove(partial)
        log(f"FAILED snapshot: {type(exc).__name__}: {exc}")
        return EXIT_BACKUP_FAILED
    log(f"snapshot_complete pages={pages} seconds={time.monotonic() - started:.1f}")

    try:
        verdict = quick_check(partial)
    except Exception as exc:
        verdict = f"{type(exc).__name__}: {exc}"
    if verdict != "ok":
        remove(partial)
        log(f"FAILED quick_check={verdict}")
        return EXIT_INTEGRITY_FAILED
    log("quick_check=ok")

    try:
        if args.compress:
            final = final.with_name(final.name + ".gz")
            staged = final.with_name(final.name + ".partial")
            with partial.open("rb") as raw, gzip.open(staged, "wb", compresslevel=6) as packed:
                shutil.copyfileobj(raw, packed, 4 * 1024 * 1024)
            partial.unlink()
            os.replace(staged, final)
        else:
            os.replace(partial, final)
        if os.name != "nt":
            final.chmod(0o600)
        manifest = {"file": final.name, "created_at": datetime.now(timezone.utc).isoformat(),
                    "source": str(source), "source_size_bytes": size, "size_bytes": final.stat().st_size,
                    "pages": pages, "quick_check": "ok", "compressed": bool(args.compress),
                    "sha256": sha256_of(final), "sqlite_version": sqlite3.sqlite_version}
        manifest_path = final.with_name(final.name + ".json")
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        if os.name != "nt":
            manifest_path.chmod(0o600)
    except Exception as exc:
        remove(partial)
        log(f"FAILED finalize: {type(exc).__name__}: {exc}")
        return EXIT_BACKUP_FAILED
    _RUN["file"] = final.name
    log(f"OK backup={final} size_bytes={manifest['size_bytes']} sha256={manifest['sha256']} "
        f"seconds={time.monotonic() - started:.1f}")

    try:
        removed = prune(output_dir, args.keep)
    except Exception as exc:
        log(f"WARNING backup written, but retention failed: {type(exc).__name__}: {exc}")
        return EXIT_RETENTION_FAILED
    log(f"retention keep={args.keep} removed={len(removed)}")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
