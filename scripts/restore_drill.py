#!/usr/bin/env python3
"""Backup -> encrypt -> off-site -> restore -> verify drill (Section H Step 8.5).

    python scripts/restore_drill.py --workdir <scratch> --out evidence/step8/restore_drill.json \
        [--source <sqlite file>] [--rows 5] [--remote-dir <local dir standing in for off-site storage>] \
        [--postgres-url postgresql://user@host:port/postgres]

The drill never writes to a ``--source`` database (it is read through SQLite's online backup API, read-only).
Without ``--source`` it builds a representative database from the real schema (legacy ``migrate()``, CATI
production schema, platform migrations) with synthetic rows and REALISTIC credential storage: broker credentials
Fernet-encrypted with a drill key, TOTP secrets in the ``enc:v2:`` envelope, KYC PII base64(Fernet), password and
token columns hashed.

Steps (each recorded in the evidence JSON):

 a. back up the database with ``scripts/backup_trading_db.py`` (online backup, ``quick_check``, SHA-256 manifest);
 g. SCAN the backup before anything leaves the machine: every credential column must hold ciphertext, a hash, a
    masked value or nothing -- a plaintext value REFUSES the drill (exit 2); every text cell is scanned for
    high-confidence secret patterns (private keys, cloud/payment/platform tokens, bot tokens, env-file lines,
    passwords in connection strings, ``api_key=...``); any non-database file in the backup set (an ``.env``) is
    refused;
 b. encrypt with the production off-site path when it is configured (``age`` with
    ``BACKUP_ENCRYPTION_RECIPIENT`` and ``RESTORE_DRILL_AGE_IDENTITY`` for decryption, or ``gpg`` with
    ``BACKUP_GPG_RECIPIENT``) -- otherwise with a **TEST-ONLY** Fernet key (``RESTORE_DRILL_TEST_FERNET_KEY``, or an
    ephemeral key generated for this run and never written anywhere). Also proves the production uploader still
    refuses plaintext (``offsite_backup.encryption_plan`` with no recipient);
 c. "upload": copy the encrypted artifact and its manifest to a LOCAL directory standing in for off-site storage
    (no network, no credentials) and verify the copy's SHA-256; the remote must hold no plaintext database;
 d. restore from the remote copy into a fresh directory (decrypt, gunzip if needed);
 e. verify: ``integrity_check``, foreign-key violations not increased, row counts and content hashes of EVERY
    table equal to the backup snapshot and to the source at backup time, with the research-governance,
    account-configuration and trading-intent tables reported by name; credential columns still ciphertext after
    restore and decryptable only with the key restored from the secret manager (simulated by the drill key --
    secrets are never restored from the database backup);
    PostgreSQL leg with ``--postgres-url``: platform schema on a throwaway database, platform rows written by the
    platform code, ``pg_dump -Fc`` -> scan -> encrypt -> remote -> ``pg_restore`` into a second throwaway
    database -> per-table counts and content hashes equal (``PG_BIN`` names the PostgreSQL bin directory);
 f. measure RPO (newest data in the source vs newest data in the restored backup; a synthetic source receives
    writes after the backup so the measurement is not trivially zero) and RTO (remote fetch -> verified,
    usable database).

Exit status: 0 every check passed; 1 bad arguments; 2 refused by the secret scan (nothing was encrypted or
uploaded); 3 a verification check failed; 4 the drill could not run.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT / "backends" / "shared", ROOT / "backends" / "bot-backend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

EXIT_OK, EXIT_USAGE, EXIT_SECRET_REFUSED, EXIT_VERIFY_FAILED, EXIT_FAILED = 0, 1, 2, 3, 4
SCHEMA = "cosmicforge-restore-drill-v1"

#: Columns whose stored value must be ciphertext / hash / masked / empty. Matched on column name.
CREDENTIAL_COLUMN = re.compile(r"(secret|password|passwd|api_?key|apikey|private|encrypted|mnemonic|"
                               r"(^|_)token$|(token|key)_hash$|passphrase)", re.I)
#: Secret-looking names that are NOT secrets (metadata, lease tokens, idempotency/dedupe keys).
NOT_CREDENTIAL = {"key_metadata", "key_fingerprint", "processing_token", "fencing_token", "lease_token",
                  "credential_version", "active_credential_version", "content_hash", "config_hash", "payload_hash",
                  "dataset_hash", "manifest_hash", "evidence_hash", "record_hash", "prev_hash", "row_hash",
                  "chain_hash", "event_hash", "depth_hash", "file_hash", "input_hash", "output_hash", "params_hash",
                  "decision_hash", "feature_hash", "model_hash", "source_hash", "universe_hash", "plan_hash",
                  "previous_hash", "entry_hash", "snapshot_hash", "spec_hash", "code_hash", "data_hash",
                  "schema_hash", "rules_hash", "config_sha256"}
#: Free-text findings that are high-confidence in a database cell (Fernet blobs and ids are high-entropy, so the
#: generic HIGH_ENTROPY_TOKEN and JSON_WEB_TOKEN detectors are not used on cells).
CELL_FINDINGS = {"API_KEY_ASSIGNMENT", "PRIVATE_KEY_BLOCK", "CLOUD_ACCESS_KEY", "PAYMENT_PROVIDER_KEY",
                 "PLATFORM_TOKEN", "BOT_TOKEN", "ENV_FILE_SECRET", "CONNECTION_STRING_PASSWORD"}
GOVERNANCE_TABLE = re.compile(r"^cati_(family_|governance_|promotion_|holdout_|experiment_|certification_|"
                              r"adapter_cert_)")
ACCOUNT_CONFIG_TABLES = ("bot_instances", "broker_accounts", "broker_credentials", "broker_credentials_v2")
INTENT_TABLES = ("cati_execution_intents", "platform_order_intents")
PLATFORM_SNAPSHOT_TABLES = ("platform_workers", "platform_accounts", "platform_account_assignments",
                            "platform_assignment_history", "platform_jobs", "platform_job_attempts",
                            "platform_order_intents", "ops_alerts", "ops_alert_events", "ops_alert_deliveries",
                            "ops_status_incidents", "ops_status_incident_updates")


def log(msg: str) -> None:
    print(f"[RESTORE_DRILL] {msg}", flush=True)


_MODULES: Dict[str, Any] = {}


def _load(name: str):
    if name in _MODULES:
        return _MODULES[name]
    spec = importlib.util.spec_from_file_location(f"cf_{name}", ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    _MODULES[name] = module
    return module


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── synthetic source ────────────────────────────────────────────────────────────────────────────────────
def _check_values(conn: sqlite3.Connection, table: str) -> Dict[str, str]:
    sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0] or ""
    out = {}
    for col, values in re.findall(r"\b(\w+)\s+IN\s*\(([^)]*)\)", sql, re.I):
        first = re.findall(r"'([^']*)'", values)
        if first and col.lower() not in out:
            out[col] = first[0]
    return out


def _seed_refused(conn: sqlite3.Connection, tables: Sequence[str], rows: int) -> Dict[str, int]:
    """Second pass for tables the generic seeder could not fill: use the first allowed value of every
    ``col IN (...)`` CHECK. Best effort; what still refuses is reported, not forced."""
    seeded = {}
    for t in tables:
        checks = {k.lower(): v for k, v in _check_values(conn, t).items()}
        cols = [c for c in conn.execute(f'PRAGMA table_info("{t}")').fetchall() if not (c[5] and c[2].upper() == "INTEGER")]
        n = 0
        for i in range(rows):
            values = []
            for _, col, ctype, *_ in cols:
                ct = (ctype or "").upper()
                if col.lower() in checks:
                    values.append(checks[col.lower()])
                elif "INT" in ct:
                    values.append(1_700_000_000_000 + i if col.endswith(("_at", "_ms", "time")) else i + 1)
                elif any(k in ct for k in ("REAL", "FLOA", "DOUB", "NUM", "DEC")):
                    values.append(round(1000 + i * 1.25, 8))
                else:
                    values.append(f"{col}-{i}")
            try:
                conn.execute(f'INSERT INTO "{t}" (' + ",".join(f'"{c[1]}"' for c in cols) + ") VALUES ("
                             + ",".join("?" * len(cols)) + ")", values)
                n += 1
            except sqlite3.DatabaseError:
                break
        if n:
            seeded[t] = n
    conn.commit()
    return seeded


def _credential_columns(conn: sqlite3.Connection) -> List[tuple]:
    out = []
    for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                             "ORDER BY name").fetchall():
        for c in conn.execute(f'PRAGMA table_info("{t}")').fetchall():
            name, ctype = c[1], (c[2] or "").upper()
            if c[5] or name.lower() in NOT_CREDENTIAL or name.lower().endswith("_version"):
                continue
            if any(k in ctype for k in ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC", "BOOL")):
                continue                     # numbers and flags are not secrets
            if CREDENTIAL_COLUMN.search(name):
                out.append((t, name))
    return out


def _realistic_secret(table: str, column: str, broker_fernet, i: int) -> Optional[str]:
    col = column.lower()
    if col == "masked_key":
        return f"ab{i:02d}****{i:02d}yz"
    if col == "hashed_password":
        return "$2b$12$" + hashlib.sha256(f"pw{i}".encode()).hexdigest()[:53]
    if col.endswith("_hash") or col.endswith("token_hash"):
        return hashlib.sha256(f"{table}.{column}.{i}".encode()).hexdigest()
    payload = json.dumps({"api_key": f"SYNTHETIC-KEY-{i}", "api_secret": f"SYNTHETIC-SECRET-{i}"}).encode()
    token = broker_fernet.encrypt(payload).decode()
    if col in ("totp_secret", "mfa_secret"):
        return "enc:v2:" + token
    if table.startswith("kyc_"):
        return base64.urlsafe_b64encode(token.encode()).decode()
    return token


def realistic_credentials(path: Path, broker_key: bytes) -> Dict[str, Any]:
    """Overwrite every credential column with what production stores there (never plaintext)."""
    from cryptography.fernet import Fernet
    f = Fernet(broker_key)
    conn = sqlite3.connect(str(path))
    touched: Dict[str, int] = {}
    try:
        for table, column in _credential_columns(conn):
            triggers = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
                                    (table,)).fetchall()
            for name, _ in triggers:
                conn.execute(f'DROP TRIGGER "{name}"')
            rowids = [r[0] for r in conn.execute(f'SELECT rowid FROM "{table}" WHERE "{column}" IS NOT NULL')]
            for i, rowid in enumerate(rowids):
                conn.execute(f'UPDATE "{table}" SET "{column}"=? WHERE rowid=?',
                             (_realistic_secret(table, column, f, i), rowid))
            for _, sql in triggers:
                conn.execute(sql)
            if rowids:
                touched[f"{table}.{column}"] = len(rowids)
        conn.commit()
    finally:
        conn.close()
    return touched


def build_synthetic(path: Path, rows: int, broker_key: bytes) -> Dict[str, Any]:
    md = _load("migration_drill")
    info = md.build_representative(path, rows)
    conn = sqlite3.connect(str(path))
    try:
        empty = [t for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                            "AND name NOT LIKE 'sqlite_%'").fetchall()
                 if conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] == 0]
        info["second_pass_seeded"] = _seed_refused(conn, empty, rows)
    finally:
        conn.close()
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    from app.platform_workers.assignments import WorkerAssignments
    from app.platform_workers.jobs import NOTIFICATION, JobQueue, OrderIntents
    pdb = Database(f"sqlite:///{path.as_posix()}")
    try:
        migrate(pdb, all_migrations())
        _platform_rows(pdb, rows, WorkerAssignments, JobQueue, OrderIntents, NOTIFICATION)
    finally:
        pdb.close()
    info["credential_columns_made_realistic"] = realistic_credentials(path, broker_key)
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE IF NOT EXISTS restore_drill_heartbeat (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                 "written_at_ms INTEGER NOT NULL, phase TEXT NOT NULL)")
    conn.execute("INSERT INTO restore_drill_heartbeat (written_at_ms, phase) VALUES (?, 'BEFORE_BACKUP')",
                 (int(time.time() * 1000),))
    conn.commit()
    conn.close()
    return info


def _platform_rows(pdb, rows, WorkerAssignments, JobQueue, OrderIntents, NOTIFICATION) -> None:
    asg = WorkerAssignments(pdb)
    asg.upsert_accounts([(f"drill-acct-{i}", f"drill-tenant-{i}", "binance", "DEMO") for i in range(rows)])
    w = asg.register_worker("drill-worker-1")
    asg.claim(w)
    q = JobQueue(pdb)
    intents = OrderIntents(pdb)
    for i in range(rows):
        q.enqueue(NOTIFICATION, f"drill-job-{i}", {"n": i})
        intents.prepare(idempotency_key=f"drill-intent-{i}", account_id=f"drill-acct-{i}", tenant_id=f"drill-tenant-{i}",
                        venue="binance", symbol="BTCUSDT", side="BUY", quantity="0.001", limit_price="65000.5")
    from shared_lib.operations.alerting import AlertEngine
    from shared_lib.operations.status_page import StatusIncidents
    AlertEngine(pdb, sinks=[]).raise_alert("BACKUP_FAILURE", key="drill", title="drill alert", source="restore_drill")
    StatusIncidents(pdb).create(title="Drill notice", components=["PORTAL"], impact="MAINTENANCE",
                                message="Restore drill test notice.", actor="restore_drill")


# ── snapshots ───────────────────────────────────────────────────────────────────────────────────────────
def snapshot(path: Path) -> Dict[str, Any]:
    md = _load("migration_drill")
    return md.snapshot(path)


_EPOCH_MS = (1_500_000_000_000, 4_000_000_000_000)


def newest_data_ms(path: Path) -> Optional[int]:
    """The newest timestamp in any ``*_at`` / ``*_ms`` column (epoch ms integers or ISO-8601 text)."""
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    best: Optional[int] = None
    try:
        for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
            for c in conn.execute(f'PRAGMA table_info("{t}")').fetchall():
                col = c[1]
                if not col.endswith(("_at", "_at_ms", "_ms")) or any(
                        k in col.lower() for k in ("expire", "next_", "eligible", "until", "deadline", "due", "valid")):
                    continue                 # scheduled times, not data: they may lie in the future
                v = conn.execute(f'SELECT MAX("{col}") FROM "{t}"').fetchone()[0]
                ms = None
                if isinstance(v, int) and _EPOCH_MS[0] <= v <= _EPOCH_MS[1]:
                    ms = v
                elif isinstance(v, str) and re.match(r"^\d{4}-\d\d-\d\dT", v):
                    try:
                        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                        dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                        ms = int(dt.timestamp() * 1000)
                    except ValueError:
                        ms = None
                if ms is not None and ms <= int(time.time() * 1000) and (best is None or ms > best):
                    best = ms
    finally:
        conn.close()
    return best


# ── secret scan ─────────────────────────────────────────────────────────────────────────────────────────
def scan_database(path: Path) -> Dict[str, Any]:
    from shared_lib.operations.secrets_scan import PLAINTEXT, classify_stored_secret, find_secrets
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    findings: List[Dict[str, Any]] = []
    classes: Dict[str, Dict[str, int]] = {}
    try:
        cred = set(_credential_columns(conn))
        for table, column in sorted(cred):
            for rowid, value in conn.execute(f'SELECT rowid, "{column}" FROM "{table}"'):
                if isinstance(value, (int, float)):
                    continue
                kind = classify_stored_secret(value)
                classes.setdefault(f"{table}.{column}", {}).setdefault(kind, 0)
                classes[f"{table}.{column}"][kind] += 1
                if kind == PLAINTEXT:
                    findings.append({"table": table, "column": column, "rowid": rowid, "kind": "PLAINTEXT_CREDENTIAL"})
        for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall():
            text_cols = [c[1] for c in conn.execute(f'PRAGMA table_info("{t}")').fetchall()
                         if (t, c[1]) not in cred]
            if not text_cols:
                continue
            for row in conn.execute(f'SELECT rowid, {", ".join(chr(34) + c + chr(34) for c in text_cols)} FROM "{t}"'):
                for col, value in zip(text_cols, row[1:]):
                    if not isinstance(value, str) or len(value) < 8:
                        continue
                    kinds = sorted(set(find_secrets(value)) & CELL_FINDINGS)
                    if kinds:
                        findings.append({"table": t, "column": col, "rowid": row[0], "kind": ",".join(kinds)})
    finally:
        conn.close()
    return {"ok": not findings, "findings": findings[:200], "finding_count": len(findings),
            "credential_columns": classes}


def scan_backup_set(directory: Path, database_file: Path) -> Dict[str, Any]:
    """Everything that would be shipped: the database (by column) and any other file (refused if env-like)."""
    from shared_lib.operations.secrets_scan import find_secrets
    out = scan_database(database_file)
    extra = []
    for p in sorted(Path(directory).iterdir()):
        if p == database_file or p.name == database_file.name + ".json":
            continue
        if p.name.startswith(".env") or p.suffix == ".env":
            extra.append({"file": p.name, "kind": "ENV_FILE"})
            continue
        try:
            text = p.read_text(encoding="utf-8", errors="replace")[:2_000_000]
        except OSError:
            continue
        kinds = find_secrets(text)
        if kinds:
            extra.append({"file": p.name, "kind": ",".join(kinds)})
    out["foreign_files"] = extra
    out["ok"] = out["ok"] and not extra
    return out


# ── encryption ──────────────────────────────────────────────────────────────────────────────────────────
class Encryptor:
    """Production path (age / gpg through offsite_backup) when configured; otherwise TEST-ONLY Fernet."""

    def __init__(self, environ=None) -> None:
        self.environ = os.environ if environ is None else environ
        self.offsite = _load("offsite_backup")
        config = {k: v for k, v in self.environ.items() if k in self.offsite.CONFIG_KEYS and v}
        self.method = None
        if config.get("BACKUP_ENCRYPTION_RECIPIENT") and shutil.which("age") and self.environ.get("RESTORE_DRILL_AGE_IDENTITY"):
            self.method, self.recipient = "age", config["BACKUP_ENCRYPTION_RECIPIENT"]
        elif config.get("BACKUP_GPG_RECIPIENT") and shutil.which("gpg"):
            self.method, self.recipient = "gpg", config["BACKUP_GPG_RECIPIENT"]
        else:
            from cryptography.fernet import Fernet
            key = self.environ.get("RESTORE_DRILL_TEST_FERNET_KEY")
            self.key_source = ("env:RESTORE_DRILL_TEST_FERNET_KEY (TEST ONLY)" if key
                               else "ephemeral key generated for this run, held in memory only (TEST ONLY)")
            self._key = key.encode() if key else Fernet.generate_key()
            self._fernet = Fernet(self._key)
            self.method = "fernet-TEST-ONLY"

    def describe(self) -> Dict[str, Any]:
        d = {"method": self.method, "age_available": bool(shutil.which("age")), "gpg_available": bool(shutil.which("gpg"))}
        if self.method == "fernet-TEST-ONLY":
            d.update(key_source=self.key_source, key_fingerprint=hashlib.sha256(self._key).hexdigest()[:16],
                     note="TEST-ONLY symmetric encryption: production off-site copies use age/gpg public-key "
                          "encryption (scripts/offsite_backup.py); the private key is never on the server.")
        else:
            d["recipient_configured"] = True
        return d

    def encrypt(self, source: Path, target: Path) -> Path:
        if self.method == "fernet-TEST-ONLY":
            target.write_bytes(self._fernet.encrypt(source.read_bytes()))
            return target
        cmd = self.offsite.encrypt_command(self.method, self.recipient, source, target)
        subprocess.run(cmd, check=True, capture_output=True)
        return target

    def decrypt(self, source: Path, target: Path) -> Path:
        if self.method == "fernet-TEST-ONLY":
            target.write_bytes(self._fernet.decrypt(source.read_bytes()))
        elif self.method == "age":
            subprocess.run(["age", "--decrypt", "-i", self.environ["RESTORE_DRILL_AGE_IDENTITY"], "-o", str(target),
                            str(source)], check=True, capture_output=True)
        else:
            subprocess.run(["gpg", "--batch", "--yes", "--output", str(target), "--decrypt", str(source)], check=True,
                           capture_output=True)
        return target


def offsite_refuses_plaintext() -> bool:
    off = _load("offsite_backup")
    try:
        off.encryption_plan({})
    except off.OffsiteError as exc:
        return "REFUSED" in str(exc)
    return False


# ── upload (local "remote") ─────────────────────────────────────────────────────────────────────────────
def upload(files: Sequence[Path], remote_dir: Path, stamp: str) -> Dict[str, Any]:
    dest = Path(remote_dir) / stamp
    dest.mkdir(parents=True, exist_ok=False)
    copied = {}
    for f in files:
        shutil.copy2(f, dest / f.name)
        copied[f.name] = {"bytes": f.stat().st_size, "sha256_matches": sha256_file(dest / f.name) == sha256_file(f)}
    plaintext_db = [p.name for p in dest.iterdir() if p.is_file() and p.read_bytes()[:16] == b"SQLite format 3\x00"]
    return {"remote_path_kind": "LOCAL_DIRECTORY_STANDING_IN_FOR_OFFSITE", "objects": copied,
            "verified": all(v["sha256_matches"] for v in copied.values()), "plaintext_databases_on_remote": plaintext_db,
            "dir": dest}


# ── verification ────────────────────────────────────────────────────────────────────────────────────────
def compare(reference: Dict[str, Any], restored: Dict[str, Any], tables: Sequence[str]) -> Dict[str, Any]:
    out = {}
    for t in tables:
        if t not in reference["counts"]:
            out[t] = {"present_in_source": False}
            continue
        out[t] = {"rows_source": reference["counts"][t], "rows_restored": restored["counts"].get(t),
                  "hash_equal": reference["hashes"][t] == restored["hashes"].get(t)}
    return out


def credentials_usable_with_secret_manager_key(path: Path, broker_key: Optional[bytes]) -> Optional[bool]:
    if broker_key is None:
        return None
    from cryptography.fernet import Fernet, InvalidToken
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT encrypted_blob FROM broker_credentials WHERE encrypted_blob IS NOT NULL LIMIT 1").fetchone()
    except sqlite3.DatabaseError:
        return None
    finally:
        conn.close()
    if row is None:
        return None
    try:
        json.loads(Fernet(broker_key).decrypt(row[0].encode()))
        wrong = Fernet(Fernet.generate_key())
        try:
            wrong.decrypt(row[0].encode())
            return False          # decrypts with ANY key: it is not protected
        except InvalidToken:
            return True
    except (InvalidToken, ValueError):
        return False


# ── PostgreSQL leg ──────────────────────────────────────────────────────────────────────────────────────
def _pg_tool(name: str) -> str:
    bin_dir = os.environ.get("PG_BIN") or (r"C:\Program Files\PostgreSQL\18\bin" if os.name == "nt" and
                                           Path(r"C:\Program Files\PostgreSQL\18\bin").is_dir() else "")
    return str(Path(bin_dir) / name) if bin_dir else name


def _pg_snapshot(db) -> Dict[str, Any]:
    counts, hashes = {}, {}
    with db.transaction(write=False) as conn:
        for t in PLATFORM_SNAPSHOT_TABLES:
            cols = [r[0] for r in conn.all("SELECT column_name FROM information_schema.columns WHERE table_schema='public' "
                                           "AND table_name=? ORDER BY ordinal_position", (t,))]
            rows = conn.all(f"SELECT {', '.join(cols)} FROM {t} ORDER BY {', '.join(str(i + 1) for i in range(len(cols)))}")
            counts[t] = len(rows)
            h = hashlib.sha256()
            for r in rows:
                h.update(repr(tuple(str(v) for v in r)).encode())
            hashes[t] = h.hexdigest()
    return {"counts": counts, "hashes": hashes}


def postgres_leg(admin_url: str, workdir: Path, encryptor: Encryptor, remote_dir: Path, rows: int) -> Dict[str, Any]:
    import psycopg
    from shared_lib.operations.secrets_scan import find_secrets
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    from app.platform_workers.assignments import WorkerAssignments
    from app.platform_workers.jobs import NOTIFICATION, JobQueue, OrderIntents
    base = admin_url.rpartition("/")[0]
    src_name, dst_name = f"cfrestore_src_{uuid.uuid4().hex[:8]}", f"cfrestore_dst_{uuid.uuid4().hex[:8]}"
    out: Dict[str, Any] = {"source_database": src_name, "restored_database": dst_name}
    with psycopg.connect(admin_url, autocommit=True) as c:
        c.execute(f"CREATE DATABASE {src_name}")
        c.execute(f"CREATE DATABASE {dst_name}")
    try:
        src = Database(f"{base}/{src_name}")
        out["schema_version"] = migrate(src, all_migrations()).final_version
        _platform_rows(src, rows, WorkerAssignments, JobQueue, OrderIntents, NOTIFICATION)
        before = _pg_snapshot(src)
        src.close()
        dump = workdir / f"{src_name}.dump"
        t0 = time.perf_counter()
        subprocess.run([_pg_tool("pg_dump"), "-Fc", "-f", str(dump), f"{base}/{src_name}"], check=True, capture_output=True)
        out["dump_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        plain = subprocess.run([_pg_tool("pg_restore"), "-f", "-", str(dump)], check=True, capture_output=True).stdout
        kinds = sorted(set(find_secrets(plain.decode("utf-8", errors="replace"))) & CELL_FINDINGS)
        out["secret_scan"] = {"ok": not kinds, "findings": kinds}
        if kinds:
            out["status"] = "REFUSED_SECRET_SCAN"
            return out
        enc = encryptor.encrypt(dump, workdir / (dump.name + ".enc"))
        up = upload([enc], remote_dir, f"pg-{src_name}")
        out["upload_verified"] = up["verified"]
        t_restore = time.perf_counter()
        fetched = workdir / "pg-fetched.dump"
        encryptor.decrypt(up["dir"] / enc.name, fetched)
        subprocess.run([_pg_tool("pg_restore"), "--no-owner", "-d", f"{base}/{dst_name}", str(fetched)], check=True,
                       capture_output=True)
        dst = Database(f"{base}/{dst_name}")
        ready = dst.health()["ready"]
        after = _pg_snapshot(dst)
        dst.close()
        out["rto_ms"] = round((time.perf_counter() - t_restore) * 1000, 1)
        out["tables"] = {t: {"rows_source": before["counts"][t], "rows_restored": after["counts"][t],
                             "hash_equal": before["hashes"][t] == after["hashes"][t]} for t in PLATFORM_SNAPSHOT_TABLES}
        out["restore_equal"] = before == after
        out["health_after_restore"] = ready
        out["status"] = "PASSED" if (before == after and ready and up["verified"]) else "FAILED"
    finally:
        with psycopg.connect(admin_url, autocommit=True) as c:
            c.execute(f"DROP DATABASE IF EXISTS {src_name} WITH (FORCE)")
            c.execute(f"DROP DATABASE IF EXISTS {dst_name} WITH (FORCE)")
    return out


# ── the drill ───────────────────────────────────────────────────────────────────────────────────────────
class DrillRefused(Exception):
    pass


def run(workdir: Path, *, source: Optional[Path] = None, rows: int = 5, remote_dir: Optional[Path] = None,
        postgres_url: Optional[str] = None, environ=None,
        tamper=None) -> Dict[str, Any]:
    """``tamper(path)`` is a test hook applied to the BACKUP before the secret scan (inject plaintext)."""
    environ = os.environ if environ is None else environ
    workdir = Path(workdir)
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    remote_dir = Path(remote_dir or workdir / "remote")
    result: Dict[str, Any] = {"schema": SCHEMA, "started_at": _now_iso(), "checks": {}}
    checks = result["checks"]
    broker_key: Optional[bytes] = None
    if source is None:
        from cryptography.fernet import Fernet
        src = workdir / "source" / "drill-source.db"
        src.parent.mkdir()
        os.environ["DATABASE_URL"] = f"sqlite:///{src.as_posix()}"
        os.environ["COSMICFORGE_TEST_MODE"] = "1"
        broker_key = Fernet.generate_key()   # stands in for the key held by the secret manager
        result["source"] = {"kind": "REPRESENTATIVE_SYNTHETIC", **build_synthetic(src, rows, broker_key)}
    else:
        src = Path(source).resolve()
        if not src.is_file():
            raise FileNotFoundError(str(src))
        result["source"] = {"kind": "PROVIDED_FILE", "sha256_before": sha256_file(src)}
        env_key = environ.get("BROKER_SECRET_KEY")
        broker_key = env_key.encode() if env_key else None
    source_snapshot = snapshot(src)
    source_newest_at_backup = newest_data_ms(src)

    # (a) back up with the production script
    backup_dir = workdir / "backups"
    bt = _load("backup_trading_db")
    code = bt.main(["--database", str(src), "--output-dir", str(backup_dir), "--keep", "3"])
    if code != 0:
        raise RuntimeError(f"backup_trading_db exited {code}")
    backup_file = sorted(p for p in backup_dir.iterdir() if bt.NAME.match(p.name))[-1]
    manifest_file = backup_file.with_name(backup_file.name + ".json")
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    backup_taken_ms = int(time.time() * 1000)
    result["backup"] = {"file": backup_file.name, "bytes": manifest["size_bytes"], "sha256": manifest["sha256"],
                        "quick_check": manifest["quick_check"]}
    checks["backup_manifest_matches_file"] = sha256_file(backup_file) == manifest["sha256"]
    if tamper is not None:
        tamper(backup_file, backup_dir)
    backup_snapshot = snapshot(backup_file)

    if source is None:   # the primary keeps working after the backup: what a restore would lose
        time.sleep(0.05)
        conn = sqlite3.connect(str(src))
        conn.execute("INSERT INTO restore_drill_heartbeat (written_at_ms, phase) VALUES (?, 'AFTER_BACKUP')",
                     (int(time.time() * 1000),))
        conn.commit()
        conn.close()

    # (g) refuse to ship secrets
    scan = scan_backup_set(backup_dir, backup_file)
    result["secret_scan"] = {k: v for k, v in scan.items() if k != "findings"}
    result["secret_scan"]["findings"] = scan["findings"][:50]
    checks["secret_scan_clean"] = scan["ok"]
    if not scan["ok"]:
        result["outcome"] = "REFUSED_SECRET_SCAN"
        result["ok"] = False
        result["finished_at"] = _now_iso()
        raise DrillRefused(json.dumps(result, default=str))
    checks["credential_columns_not_plaintext"] = all("PLAINTEXT" not in v for v in scan["credential_columns"].values())

    # (b) encrypt
    enc = Encryptor(environ)
    result["encryption"] = enc.describe()
    checks["offsite_uploader_refuses_plaintext"] = offsite_refuses_plaintext()
    artifact = enc.encrypt(backup_file, workdir / (backup_file.name + ".enc"))
    checks["artifact_is_not_plaintext_sqlite"] = artifact.read_bytes()[:16] != b"SQLite format 3\x00"

    # (c) "upload"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    up = upload([artifact, manifest_file], remote_dir, stamp)
    result["upload"] = {k: v for k, v in up.items() if k != "dir"}
    checks["remote_copy_verified"] = up["verified"]
    checks["remote_holds_no_plaintext_database"] = not up["plaintext_databases_on_remote"]

    # (d) restore from the remote copy
    t0 = time.perf_counter()
    restore_dir = workdir / "restored"
    restore_dir.mkdir()
    fetched = restore_dir / artifact.name
    shutil.copy2(up["dir"] / artifact.name, fetched)
    decrypted = enc.decrypt(fetched, restore_dir / backup_file.name)
    checks["decrypted_matches_manifest"] = sha256_file(decrypted) == json.loads(
        (up["dir"] / manifest_file.name).read_text(encoding="utf-8"))["sha256"]
    restored = restore_dir / "restored.db"
    if decrypted.suffix == ".gz":
        with gzip.open(decrypted, "rb") as zin, restored.open("wb") as zout:
            shutil.copyfileobj(zin, zout)
    else:
        decrypted.rename(restored)
    fetched.unlink()

    # (e) verify
    restored_snapshot = snapshot(restored)
    rto_ms = round((time.perf_counter() - t0) * 1000, 1)
    tables = sorted(source_snapshot["counts"])
    governance = [t for t in tables if GOVERNANCE_TABLE.match(t)]
    checks["integrity_check_ok"] = restored_snapshot["integrity_check"] == "ok"
    checks["foreign_key_violations_not_increased"] = (restored_snapshot["foreign_key_violations"]
                                                     <= source_snapshot["foreign_key_violations"])
    checks["all_tables_equal_backup_snapshot"] = (restored_snapshot["counts"] == backup_snapshot["counts"]
                                                  and restored_snapshot["hashes"] == backup_snapshot["hashes"])
    checks["all_tables_equal_source_at_backup"] = (restored_snapshot["counts"] == source_snapshot["counts"]
                                                   and restored_snapshot["hashes"] == source_snapshot["hashes"])
    result["verification"] = {
        "tables": len(tables), "rows": sum(restored_snapshot["counts"].values()),
        "integrity_check": restored_snapshot["integrity_check"],
        "research_governance": compare(source_snapshot, restored_snapshot, governance),
        "account_configuration": compare(source_snapshot, restored_snapshot, ACCOUNT_CONFIG_TABLES),
        "trading_intents": compare(source_snapshot, restored_snapshot, INTENT_TABLES),
    }
    for group in ("research_governance", "account_configuration", "trading_intents"):
        present = {t: v for t, v in result["verification"][group].items() if v.get("present_in_source", True)}
        checks[f"{group}_equal"] = bool(present) and all(v["hash_equal"] and v["rows_source"] == v["rows_restored"]
                                                         for v in present.values())
    checks["governance_tables_have_rows"] = any(v.get("rows_source", 0) > 0
                                                for v in result["verification"]["research_governance"].values())
    rescan = scan_database(restored)
    checks["restored_credentials_still_ciphertext"] = rescan["ok"]
    usable = credentials_usable_with_secret_manager_key(restored, broker_key)
    result["verification"]["credentials_decrypt_only_with_secret_manager_key"] = usable
    if usable is not None:
        checks["credentials_decrypt_only_with_secret_manager_key"] = usable

    # (f) RPO / RTO
    source_newest_now = newest_data_ms(src)
    restored_newest = newest_data_ms(restored)
    result["rpo"] = {"definition": "newest data in the source now minus newest data in the restored backup",
                     "source_newest_at_backup_ms": source_newest_at_backup, "source_newest_now_ms": source_newest_now,
                     "restored_newest_ms": restored_newest,
                     "rpo_ms": (source_newest_now - restored_newest) if source_newest_now and restored_newest else None,
                     "simulated_post_backup_writes": source is None,
                     "backup_age_at_restore_ms": int(time.time() * 1000) - backup_taken_ms,
                     "target": "UNSET (pending the operations owner's decision)"}
    result["rto"] = {"definition": "fetch from remote + decrypt + verify until the database is usable",
                     "rto_ms": rto_ms, "target": "UNSET (pending the operations owner's decision)"}
    if source is None:
        checks["rpo_measured_nonzero_for_post_backup_writes"] = bool(result["rpo"]["rpo_ms"] and result["rpo"]["rpo_ms"] > 0)

    if postgres_url:
        result["postgres"] = postgres_leg(postgres_url, workdir, enc, remote_dir, rows)
        checks["postgres_leg_passed"] = result["postgres"].get("status") == "PASSED"
    else:
        result["postgres"] = {"status": "NOT_RUN", "reason": "no --postgres-url given"}

    if source is not None:
        result["source"]["sha256_after"] = sha256_file(src)
        checks["source_untouched"] = result["source"]["sha256_after"] == result["source"]["sha256_before"]
    result["secret_restoration"] = (
        "Broker credential, TOTP and KYC encryption keys are NOT in the backup and are never restored from it. "
        "They are restored from the secret manager (BROKER_SECRET_KEY, CREDENTIAL_KEY, KYC encryption key) into "
        "the service environment; see docs/operations/BACKUP_AND_RESTORE.md.")
    result["ok"] = all(v is True for v in checks.values())
    result["outcome"] = "PASSED" if result["ok"] else "FAILED"
    result["finished_at"] = _now_iso()
    result["limits"] = ("Run on a workstation against a disposable database; the 'remote' is a local directory and "
                        "the encryption is TEST-ONLY unless age/gpg recipients are configured. It rehearses the "
                        "procedure; it does not prove the production off-site copy can be restored.")
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--source")
    ap.add_argument("--rows", type=int, default=5)
    ap.add_argument("--remote-dir")
    ap.add_argument("--postgres-url")
    a = ap.parse_args(argv)
    if a.rows < 1:
        log("FAILED --rows must be >= 1")
        return EXIT_USAGE
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = run(Path(a.workdir), source=Path(a.source) if a.source else None, rows=a.rows,
                     remote_dir=Path(a.remote_dir) if a.remote_dir else None, postgres_url=a.postgres_url)
    except DrillRefused as refused:
        out.write_text(json.dumps(json.loads(str(refused)), indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
        log("REFUSED: the backup contains plaintext secrets; nothing was encrypted or uploaded")
        return EXIT_SECRET_REFUSED
    except FileNotFoundError as exc:
        log(f"FAILED source not found: {exc}")
        return EXIT_USAGE
    except Exception as exc:  # noqa: BLE001
        log(f"FAILED {type(exc).__name__}: {exc}")
        return EXIT_FAILED
    out.write_text(json.dumps(result, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    for k, v in result["checks"].items():
        print(f"{'PASS' if v else 'FAIL'}  {k}")
    log(f"RPO_ms={result['rpo']['rpo_ms']} RTO_ms={result['rto']['rto_ms']} postgres={result['postgres'].get('status')}")
    log("DRILL " + result["outcome"])
    return EXIT_OK if result["ok"] else EXIT_VERIFY_FAILED


if __name__ == "__main__":
    sys.exit(main())
