#!/usr/bin/env python3
"""Copy the newest verified database backup off this server.

``backup_trading_db.py`` leaves verified backups in a directory on the same
disk as the database. That protects against a damaged database file, not
against losing the disk, the server or the account it is rented from. This
script takes the newest backup that script produced and verified, encrypts it,
uploads it somewhere else and checks the copy that arrived.

    python3 scripts/offsite_backup.py --dry-run     # show what would be done
    python3 scripts/offsite_backup.py

What it does, in order:

1. find the newest ``cosmicforge-<UTC timestamp>.db[.gz]`` whose manifest
   (``<name>.json``) says ``quick_check: ok``; a file without a manifest is
   still being written and is skipped;
2. refuse a backup older than ``--max-age-hours`` (default 36): uploading
   last week's file every night would hide that local backups have stopped;
3. recompute the file's SHA-256 and compare it with the manifest;
4. encrypt it with ``age`` (``BACKUP_ENCRYPTION_RECIPIENT``) or ``gpg``
   (``BACKUP_GPG_RECIPIENT``) into a private staging directory. Without a
   recipient the upload is REFUSED unless ``BACKUP_ALLOW_PLAINTEXT_OFFSITE=true``:
   the database holds broker credentials and identity data;
5. upload the staging directory (the artefact and its manifest) with
   ``rclone copy`` to ``BACKUP_RCLONE_REMOTE`` and/or ``rsync -a`` to
   ``BACKUP_RSYNC_TARGET``;
6. verify what arrived: ``rclone check`` (hashes, or sizes where the remote
   has no hashes) / an ``rsync --checksum --dry-run`` that must find nothing
   left to send.

Remote files are never deleted: set retention with the storage provider's
lifecycle rules. Settings are read from ``--env-file`` (default
``/etc/cosmicforge/offsite-backup.env``) and the process environment, which
wins; see ``deploy/offsite-backup.env.example``.

Exit status: 0 uploaded and verified (or a clean ``--dry-run``); 1 not
configured, or plaintext refused, or a tool is missing; 2 no usable backup;
3 the backup does not match its manifest; 4 encryption failed; 5 an upload
failed; 6 the remote copy could not be verified. Standard library only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ENV_FILE = "/etc/cosmicforge/offsite-backup.env"
DEFAULT_BACKUP_DIR = "/var/backups/cosmicforge"
# Must stay identical to backup_trading_db.NAME (a test compares the two).
NAME = re.compile(r"^cosmicforge-\d{8}T\d{6}Z\.db(\.gz)?$")
STAGING_PREFIX = ".offsite-staging-"
CONFIG_KEYS = ("BACKUP_DIR", "BACKUP_RCLONE_REMOTE", "BACKUP_RSYNC_TARGET", "BACKUP_ENCRYPTION_RECIPIENT",
               "BACKUP_GPG_RECIPIENT", "BACKUP_ALLOW_PLAINTEXT_OFFSITE", "BACKUP_MAX_AGE_HOURS")
ENCRYPT_TIMEOUT, TRANSFER_TIMEOUT = 2 * 3600, 6 * 3600

(EXIT_OK, EXIT_CONFIG, EXIT_NO_BACKUP, EXIT_INTEGRITY, EXIT_ENCRYPT, EXIT_UPLOAD,
 EXIT_REMOTE_VERIFY) = 0, 1, 2, 3, 4, 5, 6


class OffsiteError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


def log(message: str) -> None:
    print(f"[OFFSITE_BACKUP] {message}", flush=True)


def parse_env_file(path: Path) -> dict[str, str]:
    """``KEY=value`` lines as systemd's EnvironmentFile= reads them (no expansion)."""
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line[0] in "#;" or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def load_config(env_file: str | None, environ) -> dict[str, str]:
    config: dict[str, str] = {}
    if env_file:
        try:
            config.update(parse_env_file(Path(env_file)))
        except FileNotFoundError:
            pass
        except OSError as exc:
            log(f"WARNING cannot read {env_file}: {type(exc).__name__}")
    for key in CONFIG_KEYS:
        if environ.get(key):
            config[key] = environ[key]
    return {key: value.strip() for key, value in config.items() if key in CONFIG_KEYS and value.strip()}


def truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def backup_time(name: str) -> datetime:
    return datetime.strptime(name.split("-", 1)[1][:16], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def read_manifest(backup: Path) -> dict | None:
    """The manifest backup_trading_db.py wrote for ``backup``; None if absent or unusable."""
    try:
        manifest = json.loads(backup.with_name(backup.name + ".json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return manifest if isinstance(manifest, dict) else None


def newest_verified_backup(backup_dir: Path, max_age_hours: float, now: datetime) -> tuple[Path, dict]:
    """The newest backup with a passing manifest, proven to still match it."""
    try:
        names = sorted((p.name for p in backup_dir.iterdir() if p.is_file() and NAME.match(p.name)), reverse=True)
    except OSError as exc:
        raise OffsiteError(EXIT_NO_BACKUP, f"cannot read backup directory {backup_dir}: {type(exc).__name__}") from exc
    for name in names:
        backup = backup_dir / name
        manifest = read_manifest(backup)
        if not manifest or manifest.get("quick_check") != "ok" or manifest.get("file") != name:
            log(f"skipped {name}: no manifest with quick_check=ok (unfinished or unverified)")
            continue
        try:
            age_hours = (now - backup_time(name)).total_seconds() / 3600
        except ValueError:
            log(f"skipped {name}: its name is not a valid UTC timestamp")
            continue
        if age_hours > max_age_hours:
            raise OffsiteError(EXIT_NO_BACKUP, f"newest verified backup {name} is {age_hours:.1f}h old "
                               f"(limit {max_age_hours:g}h): is cosmicforge-db-backup still running?")
        size = backup.stat().st_size
        if size != manifest.get("size_bytes"):
            raise OffsiteError(EXIT_INTEGRITY, f"{name}: size {size} differs from its manifest "
                               f"({manifest.get('size_bytes')})")
        if sha256_of(backup) != manifest.get("sha256"):
            raise OffsiteError(EXIT_INTEGRITY, f"{name}: SHA-256 differs from its manifest")
        return backup, manifest
    raise OffsiteError(EXIT_NO_BACKUP, f"no verified backup in {backup_dir}")


def encryption_plan(config: dict[str, str]) -> tuple[str, str] | None:
    """(tool, recipient), or None for plaintext -- which must be allowed explicitly."""
    if config.get("BACKUP_ENCRYPTION_RECIPIENT"):
        return "age", config["BACKUP_ENCRYPTION_RECIPIENT"]
    if config.get("BACKUP_GPG_RECIPIENT"):
        return "gpg", config["BACKUP_GPG_RECIPIENT"]
    if truthy(config.get("BACKUP_ALLOW_PLAINTEXT_OFFSITE")):
        return None
    raise OffsiteError(EXIT_CONFIG, "REFUSED: no BACKUP_ENCRYPTION_RECIPIENT (age) or BACKUP_GPG_RECIPIENT is set. "
                       "The database holds broker credentials and identity data and is not uploaded unencrypted. "
                       "(BACKUP_ALLOW_PLAINTEXT_OFFSITE=true overrides this for storage you already encrypt.)")


def encrypt_command(tool: str, recipient: str, source: Path, target: Path) -> list[str]:
    if tool == "age":
        # A path names a recipients file (one public key per line); anything
        # else is one or more public keys separated by commas or spaces.
        if Path(recipient).is_file():
            to = ["-R", recipient]
        else:
            to = [arg for key in re.split(r"[,\s]+", recipient) if key for arg in ("-r", key)]
        return ["age", "--encrypt", *to, "-o", str(target), str(source)]
    return ["gpg", "--batch", "--yes", "--trust-model", "always", "--recipient", recipient,
            "--output", str(target), "--encrypt", str(source)]


def transfer_commands(staging: Path, config: dict[str, str]) -> list[tuple[str, list[str], list[str]]]:
    """(destination, upload command, verify command) for every configured destination."""
    source = str(staging)
    plans = []
    remote = config.get("BACKUP_RCLONE_REMOTE")
    if remote:
        plans.append((f"rclone:{remote}", ["rclone", "copy", source, remote],
                      ["rclone", "check", source, remote, "--one-way"]))
    target = config.get("BACKUP_RSYNC_TARGET")
    if target:
        target = target.rstrip("/") + "/"
        plans.append((f"rsync:{target}", ["rsync", "-a", source + "/", target],
                      ["rsync", "-a", "--checksum", "--dry-run", "--itemize-changes", source + "/", target]))
    return plans


def run(command: list[str], timeout: float) -> subprocess.CompletedProcess:
    """Run one external tool. Kept as a single seam so tests can replace it."""
    return subprocess.run(command, capture_output=True, text=True, errors="replace", timeout=timeout, check=False)


def _run_step(command: list[str], timeout: float, code: int, what: str) -> subprocess.CompletedProcess:
    log("$ " + shlex.join(command))
    try:
        done = run(command, timeout)
    except (OSError, subprocess.SubprocessError) as exc:
        raise OffsiteError(code, f"{what}: {type(exc).__name__}: {exc}") from exc
    if done.returncode != 0:
        detail = " | ".join((done.stderr or done.stdout or "").strip().splitlines()[-5:])
        raise OffsiteError(code, f"{what}: exit status {done.returncode}: {detail}")
    return done


def rsync_differences(itemized: str) -> list[str]:
    """Files an ``rsync --dry-run --itemize-changes`` would still transfer."""
    return [line for line in itemized.splitlines() if len(line) > 1 and line[0] in "<>c" and line[1] == "f"]


def remove_stale_staging(backup_dir: Path) -> None:
    for leftover in backup_dir.glob(STAGING_PREFIX + "*"):
        if leftover.is_dir() and not leftover.is_symlink():
            shutil.rmtree(leftover, ignore_errors=True)


def stage(backup: Path, plan: tuple[str, str] | None, staging: Path) -> Path:
    """Put exactly what will be uploaded into ``staging``; returns the artefact."""
    shutil.copy2(backup.with_name(backup.name + ".json"), staging / (backup.name + ".json"))
    if plan is None:
        artefact = staging / backup.name
        try:
            os.link(backup, artefact)                     # same filesystem: no second copy
        except OSError:
            shutil.copy2(backup, artefact)
        return artefact
    tool, recipient = plan
    artefact = staging / f"{backup.name}.{tool}"
    free, size = shutil.disk_usage(staging).free, backup.stat().st_size
    if free < size + 64 * 1024 * 1024:
        raise OffsiteError(EXIT_ENCRYPT, f"not enough free space to encrypt: need about {size} bytes, have {free}")
    _run_step(encrypt_command(tool, recipient, backup, artefact), ENCRYPT_TIMEOUT, EXIT_ENCRYPT, f"{tool} encryption")
    if not artefact.is_file() or artefact.stat().st_size == 0:
        raise OffsiteError(EXIT_ENCRYPT, f"{tool} reported success but wrote no output")
    if os.name != "nt":
        artefact.chmod(0o600)
    return artefact


def offsite(config: dict[str, str], backup_dir: Path, max_age_hours: float, dry_run: bool) -> None:
    if not config.get("BACKUP_RCLONE_REMOTE") and not config.get("BACKUP_RSYNC_TARGET"):
        raise OffsiteError(EXIT_CONFIG, "no destination: set BACKUP_RCLONE_REMOTE and/or BACKUP_RSYNC_TARGET")
    plan = encryption_plan(config)                        # before any work: refuse plaintext early
    tools = ([plan[0]] if plan else []) + [name for name, key in (("rclone", "BACKUP_RCLONE_REMOTE"),
                                                                 ("rsync", "BACKUP_RSYNC_TARGET")) if config.get(key)]
    missing = [tool for tool in tools if shutil.which(tool) is None]
    if missing:
        raise OffsiteError(EXIT_CONFIG, f"not installed or not on PATH: {', '.join(missing)}")
    if plan is None:
        log("WARNING uploading UNENCRYPTED (BACKUP_ALLOW_PLAINTEXT_OFFSITE=true)")

    backup, manifest = newest_verified_backup(backup_dir, max_age_hours, datetime.now(timezone.utc))
    log(f"selected backup={backup} size_bytes={manifest['size_bytes']} sha256={manifest['sha256']} "
        f"encryption={plan[0] if plan else 'NONE'}")

    if dry_run:
        staging = backup_dir / (STAGING_PREFIX + "DRYRUN")
        if plan:
            log("DRY-RUN would run: " + shlex.join(encrypt_command(*plan, backup, staging / f"{backup.name}.{plan[0]}")))
        for destination, upload, verify in transfer_commands(staging, config):
            log("DRY-RUN would run: " + shlex.join(upload))
            log("DRY-RUN would run: " + shlex.join(verify))
        log("DRY-RUN complete: nothing was encrypted or uploaded")
        return

    remove_stale_staging(backup_dir)
    staging = Path(tempfile.mkdtemp(prefix=STAGING_PREFIX, dir=backup_dir))    # mode 0700
    try:
        artefact = stage(backup, plan, staging)
        for destination, upload, verify in transfer_commands(staging, config):
            _run_step(upload, TRANSFER_TIMEOUT, EXIT_UPLOAD, f"upload to {destination}")
            checked = _run_step(verify, TRANSFER_TIMEOUT, EXIT_REMOTE_VERIFY, f"verification of {destination}")
            if verify[0] == "rsync" and rsync_differences(checked.stdout):
                raise OffsiteError(EXIT_REMOTE_VERIFY, f"verification of {destination}: the remote copy differs")
            log(f"OK destination={destination} file={artefact.name} size_bytes={artefact.stat().st_size} verified=yes")
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main(argv: list[str] | None = None, environ=None) -> int:
    environ = os.environ if environ is None else environ
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE, help=f"settings file (default {DEFAULT_ENV_FILE})")
    parser.add_argument("--backup-dir", help=f"where backup_trading_db.py writes (default BACKUP_DIR, "
                                             f"then {DEFAULT_BACKUP_DIR})")
    parser.add_argument("--max-age-hours", type=float, help="refuse a newest backup older than this (default 36)")
    parser.add_argument("--dry-run", action="store_true",
                        help="select and verify the backup, print the commands, encrypt and upload nothing")
    args = parser.parse_args(argv)

    config = load_config(args.env_file, environ)
    backup_dir = Path(args.backup_dir or config.get("BACKUP_DIR") or DEFAULT_BACKUP_DIR).expanduser()
    try:
        max_age = args.max_age_hours if args.max_age_hours is not None \
            else float(config.get("BACKUP_MAX_AGE_HOURS") or 36)
        offsite(config, backup_dir, max_age, args.dry_run)
    except OffsiteError as exc:
        log(f"FAILED {exc}")
        return exc.code
    except ValueError as exc:
        log(f"FAILED bad setting: {exc}")
        return EXIT_CONFIG
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
