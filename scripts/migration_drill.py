#!/usr/bin/env python3
"""Repeatable database migration drill (Section H, Step 5.1).

    python scripts/migration_drill.py --workdir <scratch> --out evidence/step5/migration_drill.json \
        [--source <sqlite file to copy>] [--postgres-url postgresql://user@host:port/postgres]

The drill never writes to its source. With ``--source`` the file is copied through SQLite's online backup API
(opened read-only) into the work directory and the COPY is sanitized; without it a representative database is
built from the real schema (legacy ``migrate()`` + CATI production schema) and filled with synthetic rows.

Steps, each recorded in the output:

 1. capture and sanitize (secret-looking columns blanked, e-mail addresses replaced);
 2. record the schema versions (``PRAGMA user_version``, platform version), table counts, ``integrity_check``,
    ``foreign_key_check`` violations and a content hash of every table;
 3. back up the source (online backup API, SHA-256 of the file);
 4. apply the deploy's migrations: legacy ``migrate()``, CATI ``production_schema.ensure`` and the versioned
    platform migrations -- once with a simulated crash in the middle of a version, then for real;
 5. recount and re-hash: every pre-existing table must keep its rows and its content hash (historical and
    financial evidence unchanged), integrity must hold, foreign-key violations must not increase;
 6. repeat the migration: nothing may change;
 7. "restart" (fresh connections and pools) and exercise read and write paths of both layers;
 8. roll the platform migrations back (legacy data untouched), re-apply, then restore the backup and verify it
    equals the baseline;
 9. PostgreSQL leg (optional): apply the platform schema to a throwaway database, copy the platform rows,
    verify counts and foreign keys, ``pg_dump`` it, drop it, ``pg_restore`` it and verify again.

Exit status 0 only if every check passed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "backends" / "shared", ROOT / "backends" / "bot-backend"):
    sys.path.insert(0, str(p))

SECRET_COLUMN = re.compile(r"(secret|password|passwd|api_key|apikey|token|credential|private|encrypted|totp|mnemonic)",
                           re.I)
EMAIL_COLUMN = re.compile(r"e?mail", re.I)


def _tables(conn: sqlite3.Connection) -> List[str]:
    return [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                                       "ORDER BY name")]


def _columns(conn, table):
    return conn.execute(f'PRAGMA table_info("{table}")').fetchall()


def table_hash(conn, table) -> str:
    h = hashlib.sha256()
    cols = [c[1] for c in _columns(conn, table)]
    for row in conn.execute(f'SELECT * FROM "{table}" ORDER BY ' + ", ".join(f'"{c}"' for c in cols)):
        h.update(repr(tuple(row)).encode())
    return h.hexdigest()


def snapshot(path: Path) -> Dict[str, Any]:
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        tables = _tables(conn)
        return {
            "user_version": conn.execute("PRAGMA user_version").fetchone()[0],
            "platform_version": (conn.execute("SELECT MAX(version) FROM platform_schema_migrations").fetchone()[0]
                                 if "platform_schema_migrations" in tables else 0),
            "integrity_check": conn.execute("PRAGMA integrity_check").fetchone()[0],
            "foreign_key_violations": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "counts": {t: conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables},
            "hashes": {t: table_hash(conn, t) for t in tables},
        }
    finally:
        conn.close()


def online_copy(src: Path, dst: Path) -> None:
    s = sqlite3.connect(f"file:{src.as_posix()}?mode=ro", uri=True)
    d = sqlite3.connect(str(dst))
    try:
        s.backup(d)
    finally:
        d.close()
        s.close()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sanitize(path: Path) -> Dict[str, int]:
    conn = sqlite3.connect(str(path))
    blanked = emails = 0
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        for t in _tables(conn):
            trig = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (t,)).fetchall()
            for name, _ in trig:   # append-only triggers would refuse the scrub; re-created afterwards
                conn.execute(f'DROP TRIGGER "{name}"')
            for cid, col, ctype, notnull, default, pk in _columns(conn, t):
                if pk:
                    continue
                if SECRET_COLUMN.search(col):
                    blanked += conn.execute(f'UPDATE "{t}" SET "{col}" = ? WHERE "{col}" IS NOT NULL',
                                            ("REDACTED" if notnull else None,)).rowcount
                elif EMAIL_COLUMN.fullmatch(col) or col.endswith("_email"):
                    emails += conn.execute(f'UPDATE "{t}" SET "{col}" = \'user\' || rowid || \'@example.invalid\' '
                                           f'WHERE "{col}" IS NOT NULL').rowcount
            for _, sql in trig:
                conn.execute(sql)
        conn.commit()
    finally:
        conn.close()
    return {"secret_values_blanked": blanked, "emails_replaced": emails}


def build_representative(path: Path, rows: int) -> Dict[str, Any]:
    """The real schema, filled with synthetic rows by declared column type. Tables whose constraints refuse
    generic values are listed, not forced."""
    os.environ.setdefault("COSMICFORGE_TEST_MODE", "1")
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    from app.execution import production_schema
    db = DB(path=str(path))
    legacy_migrate(db)
    production_schema.ensure(db, force=True)
    conn = sqlite3.connect(str(path))
    seeded, refused = {}, {}
    for t in _tables(conn):
        cols = [c for c in _columns(conn, t) if not (c[5] and c[2].upper() == "INTEGER")]
        n = 0
        for i in range(rows):
            values = []
            for _, col, ctype, *_ in cols:
                ct = (ctype or "").upper()
                if "INT" in ct:
                    values.append(1_700_000_000_000 + i if col.endswith(("_at", "_ms", "time")) else i + 1)
                elif any(k in ct for k in ("REAL", "FLOA", "DOUB", "NUM", "DEC")):
                    values.append(round(1000 + i * 1.25, 8))
                else:
                    values.append(f"{col}-{i}")
            try:
                conn.execute(f'INSERT INTO "{t}" (' + ",".join(f'"{c[1]}"' for c in cols) + ") VALUES ("
                             + ",".join("?" * len(cols)) + ")", values)
                n += 1
            except sqlite3.DatabaseError as exc:
                refused[t] = str(exc)[:120]
                break
        if n:
            seeded[t] = n
    conn.commit()
    conn.close()
    return {"seeded_tables": len(seeded), "rows_per_table": rows, "refused_tables": len(refused),
            "refused_examples": dict(list(refused.items())[:10])}


def apply_deploy_migrations(path: Path, *, crash_in_platform: bool = False) -> Dict[str, Any]:
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    from app.execution import production_schema
    started = time.perf_counter()
    db = DB(path=str(path))
    legacy_migrate(db)
    production_schema.ensure(db, force=True)
    pdb = Database(f"sqlite:///{path.as_posix()}")
    out: Dict[str, Any] = {}
    if crash_in_platform:
        def crash(version, index):
            if version == 2 and index == 1:
                raise RuntimeError("simulated crash inside platform migration 2")
        try:
            migrate(pdb, all_migrations(), fail_after=crash)
        except RuntimeError as exc:
            out["crash"] = str(exc)
        with pdb.transaction(write=False) as conn:
            out["version_after_crash"] = conn.scalar("SELECT COALESCE(MAX(version),0) FROM platform_schema_migrations")
            out["partial_tables_after_crash"] = conn.scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('platform_jobs','platform_job_attempts')")
    report = migrate(pdb, all_migrations())
    pdb.close()
    out.update({"platform_applied": report.applied, "platform_version": report.final_version,
                "duration_ms": round((time.perf_counter() - started) * 1000, 1)})
    return out


def exercise(path: Path) -> Dict[str, Any]:
    """Fresh connections (a restarted service) reading and writing both layers."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.sql import Database
    from app.platform_workers.assignments import WorkerAssignments
    from app.platform_workers.jobs import JobQueue, NOTIFICATION
    legacy = DB(path=str(path))
    with legacy.connect() as conn:
        users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    pdb = Database(f"sqlite:///{path.as_posix()}")
    asg = WorkerAssignments(pdb)
    asg.upsert_accounts([("drill-acct", "drill-tenant", "binance", "DEMO")])
    w = asg.register_worker("drill-worker")
    claimed = asg.claim(w)
    job, created = JobQueue(pdb).enqueue(NOTIFICATION, f"drill-{uuid.uuid4().hex}", {"drill": True})
    health = pdb.health()
    pdb.close()
    return {"legacy_users_read": users, "assignment_claimed": len(claimed), "job_enqueued": created,
            "platform_health_ready": health["ready"], "platform_schema_version": health["schema_version"]}


#: Configuration tables the legacy ``migrate()`` deliberately re-seeds on every run. Their row counts must still be
#: preserved; their content drift is reported (``reseeded_tables``), not counted as evidence loss.
RESEEDED_CONFIG_TABLES = {
    "news_sources": "legacy migrate() upserts the built-in RSS sources and resets is_enabled, rss_url, "
                    "fetch_interval_seconds and updated_at on every run (finding MIG-F1: an operator's disabled "
                    "source is re-enabled by each deploy)",
}

PLATFORM_TABLES = ["platform_workers", "platform_accounts", "platform_account_assignments", "platform_assignment_history",
                   "platform_jobs", "platform_job_attempts", "platform_order_intents", "platform_symbol_budgets",
                   "platform_order_slots"]


def postgres_leg(sqlite_path: Path, admin_url: str, workdir: Path) -> Dict[str, Any]:
    import psycopg
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    name = f"cfdrill_{uuid.uuid4().hex[:10]}"
    base = admin_url.rpartition("/")[0]
    url = f"{base}/{name}"
    bin_dir = os.environ.get("PG_BIN", "")
    tool = lambda t: str(Path(bin_dir) / t) if bin_dir else t  # noqa: E731
    with psycopg.connect(admin_url, autocommit=True) as c:
        c.execute(f"CREATE DATABASE {name}")
    out: Dict[str, Any] = {"database": name}
    try:
        db = Database(url)
        out["applied"] = migrate(db, all_migrations()).applied
        src = sqlite3.connect(str(sqlite_path))
        copied = {}
        with db.transaction() as conn:
            for t in PLATFORM_TABLES:
                cols = [c[1] for c in _columns(src, t)]
                rows = src.execute(f'SELECT {",".join(cols)} FROM "{t}"').fetchall()
                if rows:
                    conn.executemany(f'INSERT INTO {t} ({",".join(cols)}) VALUES ({",".join("?" * len(cols))})', rows)
                copied[t] = len(rows)
            for t in ("platform_assignment_history", "platform_job_attempts"):   # keep BIGSERIAL ahead of copied ids
                conn.execute(f"SELECT setval(pg_get_serial_sequence('{t}','id'), COALESCE((SELECT MAX(id) FROM {t}),0)+1, false)")
        src.close()

        def counts():
            with db.transaction(write=False) as conn:
                return {t: int(conn.scalar(f"SELECT COUNT(*) FROM {t}")) for t in PLATFORM_TABLES}
        out["counts_after_copy"] = counts()
        out["copy_matches_sqlite"] = out["counts_after_copy"] == copied
        with db.transaction(write=False) as conn:
            out["fk_constraints"] = int(conn.scalar("SELECT COUNT(*) FROM information_schema.table_constraints "
                                                    "WHERE constraint_type='FOREIGN KEY' AND table_schema='public'"))
            out["numeric_money_columns"] = int(conn.scalar(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND data_type='numeric'"))
        db.close()
        dump = workdir / f"{name}.dump"
        t0 = time.perf_counter()
        subprocess.run([tool("pg_dump"), "-Fc", "-f", str(dump), url], check=True)
        out["dump_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        out["dump_sha256"] = sha256_file(dump)
        with psycopg.connect(admin_url, autocommit=True) as c:
            c.execute(f"DROP DATABASE {name} WITH (FORCE)")
            c.execute(f"CREATE DATABASE {name}")
        t0 = time.perf_counter()
        subprocess.run([tool("pg_restore"), "-d", url, str(dump)], check=True)
        out["restore_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        db = Database(url)
        out["counts_after_restore"] = counts()
        out["restore_matches"] = out["counts_after_restore"] == out["counts_after_copy"]
        out["health_after_restore"] = db.health()["ready"]
        db.close()
    finally:
        with psycopg.connect(admin_url, autocommit=True) as c:
            c.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    return out


def run(workdir: Path, *, source: Optional[Path], rows: int, postgres_url: Optional[str]) -> Dict[str, Any]:
    workdir.mkdir(parents=True, exist_ok=True)
    copy = workdir / "drill.db"
    # Every legacy module resolves its database from DATABASE_URL (and may load an operator .env): pin it to the
    # drill copy before anything is imported, so no step can reach another database.
    os.environ["DATABASE_URL"] = f"sqlite:///{copy.as_posix()}"
    os.environ["COSMICFORGE_TEST_MODE"] = "1"
    for suffix in ("", "-wal", "-shm"):
        Path(str(copy) + suffix).unlink(missing_ok=True)
    result: Dict[str, Any] = {"schema": "cosmicforge-migration-drill-v1",
                              "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "checks": {}}
    if source is not None:
        online_copy(source, copy)
        result["source"] = {"kind": "SANITIZED_COPY", "source_sha256_unchanged": None}
        before_src = sha256_file(source)
    else:
        result["source"] = {"kind": "REPRESENTATIVE_SYNTHETIC", **build_representative(copy, rows)}
    result["sanitize"] = sanitize(copy)
    base = snapshot(copy)
    result["baseline"] = {k: base[k] for k in ("user_version", "platform_version", "integrity_check",
                                               "foreign_key_violations")}
    result["baseline"]["tables"] = len(base["counts"])
    result["baseline"]["rows"] = sum(base["counts"].values())
    backup = workdir / "drill.backup.db"
    backup.unlink(missing_ok=True)
    online_copy(copy, backup)
    result["backup"] = {"sha256": sha256_file(backup), "bytes": backup.stat().st_size}

    result["migration"] = apply_deploy_migrations(copy, crash_in_platform=True)
    after = snapshot(copy)
    pre = set(base["counts"])
    checks = result["checks"]
    checks["crash_left_no_partial_version"] = (result["migration"].get("version_after_crash") == 1
                                               and result["migration"].get("partial_tables_after_crash") == 0)
    checks["row_counts_preserved"] = all(after["counts"].get(t) == n for t, n in base["counts"].items())
    changed = sorted(t for t in pre if after["hashes"].get(t) != base["hashes"][t])
    checks["historical_content_unchanged"] = not [t for t in changed if t not in RESEEDED_CONFIG_TABLES]
    result["changed_tables"] = changed
    result["reseeded_tables"] = {t: RESEEDED_CONFIG_TABLES[t] for t in changed if t in RESEEDED_CONFIG_TABLES}
    checks["integrity_ok"] = after["integrity_check"] == "ok"
    checks["fk_violations_not_increased"] = after["foreign_key_violations"] <= base["foreign_key_violations"]
    result["new_tables"] = sorted(set(after["counts"]) - pre)

    again = apply_deploy_migrations(copy)
    second = snapshot(copy)
    stable = lambda a, b: all(a[t] == b.get(t) for t in a if t not in RESEEDED_CONFIG_TABLES)  # noqa: E731
    checks["repeat_is_noop"] = (again["platform_applied"] == [] and stable(after["hashes"], second["hashes"])
                                and second["counts"] == after["counts"])

    result["exercise"] = exercise(copy)
    checks["read_write_after_restart"] = (result["exercise"]["platform_health_ready"]
                                          and result["exercise"]["assignment_claimed"] == 1)

    from shared_lib.persistence.sql import Database, migrate, rollback
    from shared_lib.persistence.sql.platform_schema import all_migrations
    pdb = Database(f"sqlite:///{copy.as_posix()}")
    undone = rollback(pdb, all_migrations(), to_version=0)
    pdb.close()
    rolled = snapshot(copy)
    checks["rollback_leaves_legacy_untouched"] = all(rolled["hashes"].get(t) == second["hashes"][t] for t in pre
                                                     if t not in RESEEDED_CONFIG_TABLES)
    checks["rollback_removed_platform_tables"] = not (set(PLATFORM_TABLES) & set(rolled["counts"]))
    result["rollback"] = {"undone": undone}
    pdb = Database(f"sqlite:///{copy.as_posix()}")
    result["reapply"] = migrate(pdb, all_migrations()).applied
    result["exercise_after_reapply"] = exercise(copy)
    pdb.close()

    restored = workdir / "drill.restored.db"
    restored.unlink(missing_ok=True)
    t0 = time.perf_counter()
    online_copy(backup, restored)
    result["restore"] = {"ms": round((time.perf_counter() - t0) * 1000, 1)}
    checks["restore_equals_baseline"] = snapshot(restored)["hashes"] == base["hashes"]

    if source is not None:
        result["source"]["source_sha256_unchanged"] = sha256_file(source) == before_src
        checks["source_untouched"] = result["source"]["source_sha256_unchanged"]
    if postgres_url:
        result["postgresql"] = postgres_leg(copy, postgres_url, workdir)
        pg = result["postgresql"]
        checks["postgres_copy_counts_match"] = pg["copy_matches_sqlite"]
        checks["postgres_restore_counts_match"] = pg["restore_matches"] and pg["health_after_restore"]
        checks["postgres_foreign_keys_present"] = pg["fk_constraints"] >= 8
    result["passed"] = all(checks.values())
    result["finished_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    result["limits"] = ("A disposable copy on a workstation. It rehearses the procedure; it is not a migration of "
                        "the production database, which needs the owner's authorization and a maintenance window.")
    return result


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
# LEGACY MODE (EXT-DB-01): the legacy SQLite schema -> PostgreSQL
#
#     python scripts/migration_drill.py --legacy --workdir <scratch> --out evidence/step5/legacy_migration_drill.json \
#         --target-url postgresql://user@host:port/postgres [--source <sanitized sqlite file> --owner-authorization REF]
#         [--rows 5] [--batch-size 500]
#
# ``--target-url`` names a PostgreSQL SERVER (its maintenance database): the drill creates its own uniquely named
# throwaway target and restore databases there and drops them afterwards -- it never migrates into an existing
# database. The source is read through SQLite's online backup API into the work directory; only that copy is read.
# Without ``--source`` the source is built from the REAL legacy schema (``legacy_persistence_inventory``) and filled
# with synthetic rows carrying realistic financial values. See ``run_legacy`` for the 13 checks.
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
LEGACY_SCHEMA_ID = "cosmicforge-legacy-migration-drill-v1"
LEGACY_EXIT_REFUSED = 2
#: Directory names whose content is the operator's (runtime databases, backups, logs).
PROTECTED_DIR_NAMES = frozenset({"data", "backups", "backup", "logs"})
PROTECTED_DB_NAME = re.compile(r"^(cosmicforge.*|phase12_paper_validation.*|bot|app|cosmicforge_trading)\.db$", re.I)
_OPERATOR_DATABASE_URL = os.environ.get("DATABASE_URL", "")          # captured before the drill re-points it
CHECKPOINT_SCHEMA = "legacy_migration"
#: The compatibility layer's rowid column (``legacy_dialect``: every translated table carries it).
ROWID_COLUMN = "cf_rowid"
#: Tables of the compatibility layer itself (catalog mirrors), not legacy data.
COMPAT_INTERNAL_TABLE = re.compile(r"^cf_")
GOVERNANCE_TABLE = re.compile(r"^cati_(family_|governance_|promotion_|holdout_|experiment_|certification_|adapter_cert_)")
INTENT_TABLES = ("cati_execution_intents", "cati_execution_attempts", "execution_attempts", "pending_entries",
                 "cati_reservation_ledger", "cati_portfolio_reservations", "cati_reservation_ledger_events",
                 "platform_order_intents", "positions", "trade_fills", "position_events", "cati_user_stream_fills")
IDENTITY_COLUMN = re.compile(r"(^id$|_id$|^client_order|idempotency|^client_id$)", re.I)
#: (child table, child column, parent table, parent column): position / protection / execution associations that
#: are NOT declared foreign keys in the legacy schema. Their join counts must survive the move.
ASSOCIATIONS = (
    ("position_events", "position_id", "positions", "position_id"),
    ("position_lifecycle_state", "position_id", "positions", "position_id"),
    ("trade_fills", "position_id", "positions", "position_id"),
    ("execution_attempts", "position_id", "positions", "position_id"),
    ("positions", "execution_attempt_id", "execution_attempts", "execution_attempt_id"),
    ("cati_execution_attempts", "position_id", "positions", "position_id"),
    ("cati_execution_intents", "reservation_id", "cati_reservation_ledger", "reservation_id"),
    ("cati_reservation_ledger", "client_order_id", "cati_execution_intents", "client_order_id"),
    ("cati_protection_state", "client_order_id", "cati_execution_intents", "client_order_id"),
    ("cati_protection_replacements", "old_client_order_id", "cati_protection_state", "client_order_id"),
    ("cati_portfolio_reservations", "execution_attempt_id", "cati_execution_attempts", "execution_attempt_id"),
    ("entry_protection_events", "client_order_id", "pending_entries", "client_order_id"),
    ("cati_production_protection", "account_id", "cati_production_state", "account_id"),
)
#: Money-shaped values for the synthetic source: fractional quantities, tiny fees, large notionals, the classic
#: binary-float traps. Every one fits NUMERIC(38,18) exactly (``repr`` has at most 17 significant digits).
SYNTHETIC_MONEY = (0.1 + 0.2, 0.00012345, 123456789.12345679, 0.001, 65000.5, 1e-08, 2.675, 99999.99999999, 0.07,
                   1234567890123.4568, 3.3333333333333335, 4.35, 0.000001, 1.005, 27000.123456789)
SYNTHETIC_SIGNED = (-12.75, -0.00012345, 0.0)
SYNTHETIC_RATIO = (0.123456789, 0.5, 1.0e-05, 0.95, 2.5, 0.333333333333)


class LegacySourceRefused(Exception):
    """``--source`` names a location that holds the operator's databases."""


class SimulatedCrash(RuntimeError):
    pass


def _load_script(name: str):
    import importlib.util
    spec = importlib.util.spec_from_file_location(f"cf_md_{name}", ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def legacy_source_refusal(path: Path) -> Optional[str]:
    """Why ``path`` may not be read without the owner's explicit authorization. None when it may."""
    p = Path(path).resolve()
    try:
        p.relative_to(ROOT.resolve())
        return "it is inside the repository, where the operator's runtime databases live"
    except ValueError:
        pass
    lowered = {part.lower() for part in p.parts[:-1]}
    hit = sorted(lowered & PROTECTED_DIR_NAMES)
    if hit:
        return f"it is under a '{hit[0]}' directory (operator data, backups or logs)"
    if PROTECTED_DB_NAME.match(p.name):
        return f"{p.name} is an operator runtime database name"
    if _OPERATOR_DATABASE_URL.startswith("sqlite:///"):
        configured = _OPERATOR_DATABASE_URL[len("sqlite:///"):]
        if configured and configured != ":memory:" and os.path.isabs(configured) \
                and os.path.normcase(str(Path(configured).resolve())) == os.path.normcase(str(p)):
            return "it is the DATABASE_URL of this environment"
    return None


def _pg_tool(name: str) -> str:
    bin_dir = os.environ.get("PG_BIN") or (r"C:\Program Files\PostgreSQL\18\bin" if os.name == "nt" and
                                           Path(r"C:\Program Files\PostgreSQL\18\bin").is_dir() else "")
    return str(Path(bin_dir) / name) if bin_dir else name


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ── synthetic source ───────────────────────────────────────────────────────────────────────────────────────
def _fk_order(conn: sqlite3.Connection, tables: List[str]) -> Tuple[List[str], List[str]]:
    """Parents before children (Kahn). Returns (order, tables left in a cycle -- appended, reported)."""
    deps = {t: set() for t in tables}
    for t in tables:
        for row in conn.execute(f'PRAGMA foreign_key_list("{t}")'):
            if row[2] in deps and row[2] != t:
                deps[t].add(row[2])
    order, ready = [], sorted(t for t, d in deps.items() if not d)
    remaining = {t: set(d) for t, d in deps.items() if d}
    while ready:
        t = ready.pop(0)
        order.append(t)
        for child in sorted(remaining):
            remaining[child].discard(t)
            if not remaining[child]:
                del remaining[child]
                ready.append(child)
        ready.sort()
    cyclic = sorted(remaining)
    return order + cyclic, cyclic


def _check_in_values(sql: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for col, values in re.findall(r"\b(\w+)\s+IN\s*\(([^)]*)\)", sql or "", re.I):
        first = re.findall(r"'([^']*)'", values)
        if first and col.lower() not in out:
            out[col.lower()] = first[0]
    return out


def seed_legacy_source(path: Path, rows: int) -> Dict[str, Any]:
    """Fill every legacy table with ``rows`` synthetic rows where its constraints allow, parents first, foreign
    keys pointing at real parent rows, identity columns shared across tables (so associations join), monetary
    columns carrying realistic financial values. Refusals are reported, never forced."""
    from shared_lib.persistence import legacy_money as LM
    inv = _load_script("legacy_persistence_inventory")
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys=ON")
    tables = inv.legacy_tables(conn)
    order, cyclic = _fk_order(conn, tables)
    seeded, refused = {}, {}
    for t in order:
        sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()[0] or ""
        checks = _check_in_values(sql)
        info = conn.execute(f'PRAGMA table_info("{t}")').fetchall()
        cols = [c for c in info if not (c[5] and (c[2] or "").upper() == "INTEGER" and sum(1 for x in info if x[5]) == 1)]
        fks = {}
        for row in conn.execute(f'PRAGMA foreign_key_list("{t}")'):
            parent_vals = [r[0] for r in conn.execute(f'SELECT "{row[4] or "rowid"}" FROM "{row[2]}" ORDER BY rowid')]
            fks[row[3]] = parent_vals
        n = 0
        for i in range(rows):
            inserted = False
            for variant in ("rich", "positive", "plain"):
                values = []
                for idx, (_, col, ctype, notnull, default, pk) in enumerate(cols):
                    ct = (ctype or "").upper()
                    if col in fks:
                        pv = fks[col]
                        values.append(pv[i % len(pv)] if pv else None)
                    elif col.lower() in checks:
                        values.append(checks[col.lower()])
                    elif "INT" in ct or ct == "BOOLEAN":
                        if ct == "BOOLEAN" or col.startswith(("is_", "has_")) or col.endswith(("_enabled", "_flag")):
                            values.append(i % 2)
                        else:
                            values.append(1_760_000_000_000 + i * 86_400_000 if col.endswith(("_at", "_ms", "time"))
                                          else i + 1)
                    elif LM.is_real_type(ct):
                        try:
                            money = LM.is_monetary(t, col)
                        except LM.UnclassifiedRealColumn:
                            money = False
                        if money:
                            pool = SYNTHETIC_MONEY if variant != "rich" else SYNTHETIC_MONEY + SYNTHETIC_SIGNED
                            values.append(pool[(i * 7 + idx) % len(pool)])
                        else:
                            values.append(SYNTHETIC_RATIO[(i + idx) % len(SYNTHETIC_RATIO)])
                    elif col.lower().endswith("_id") or col.lower() == "id":
                        values.append(f"{re.sub(r'^(old|new)_', '', col.lower())}-{i}")
                    elif inv.JSON_NAME.search(col) or ct == "JSON":
                        values.append(json.dumps({"synthetic": i, "column": col, "price": 0.1 + 0.2,
                                                  "note": "quote ' and \\ backslash"}, sort_keys=True))
                    elif (rep := inv.time_representation(col, ctype, default)) and rep["representation"] == "ISO_TEXT":
                        values.append(f"2026-10-{(i % 28) + 1:02d}T12:{i % 60:02d}:00.123456+00:00")
                    elif rep and rep["representation"] == "DATE_TEXT":
                        values.append(f"2026-10-{(i % 28) + 1:02d}")
                    elif variant == "rich" and i == 1:
                        values.append(f"{col}-{i} O'Brien \\ é ✓")
                    else:
                        values.append(f"{col}-{i}")
                try:
                    conn.execute(f'INSERT INTO "{t}" (' + ",".join(f'"{c[1]}"' for c in cols) + ") VALUES ("
                                 + ",".join("?" * len(cols)) + ")", values)
                    inserted = True
                    break
                except sqlite3.DatabaseError as exc:
                    refused[t] = str(exc)[:160]
            if not inserted:
                break
            n += 1
        if n:
            seeded[t] = n
            refused.pop(t, None)
        conn.commit()
    conn.close()
    return {"tables": len(tables), "seeded_tables": len(seeded), "rows_per_table": rows,
            "rows": sum(seeded.values()), "refused_tables": dict(sorted(refused.items())),
            "fk_cycles": cyclic, "money_values": [repr(v) for v in SYNTHETIC_MONEY + SYNTHETIC_SIGNED]}


def single_file(path: Path) -> None:
    """Fold a WAL into the main file so the file alone is the database (and its SHA-256 means something)."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
    finally:
        conn.close()


def build_legacy_source(path: Path, rows: int) -> Dict[str, Any]:
    inv = _load_script("legacy_persistence_inventory")
    steps = inv.build_legacy_schema(path)
    info = seed_legacy_source(path, rows)
    info["schema_steps"] = [s["step"] for s in steps]
    return info


# ── source audit (no target needed) ───────────────────────────────────────────────────────────────────────
def audit_legacy_source(path: Path, *, limit: int = 500,
                        target_types: Optional[Dict[Tuple[str, str], str]] = None) -> Dict[str, Any]:
    """Every value that would not survive the move, by table / column / rowid: non-finite or non-numeric money,
    money the target column cannot hold exactly (``target_types``: (table, column) lower-cased -> the drill's type
    tag; without it ``legacy_money.PG_MONEY_TYPE``, unconstrained NUMERIC, is assumed), type-affinity mismatches (text in REAL/INTEGER columns, fractional
    numbers in INTEGER columns), unclassified REAL columns, and foreign-key orphans. Nothing is fixed here."""
    from shared_lib.persistence import legacy_money as LM
    inv = _load_script("legacy_persistence_inventory")
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    default_money = "numeric" if LM.PG_MONEY_TYPE.upper() == "NUMERIC" else "numeric:38:18"
    issues: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}

    def add(kind, table, column, rowid, detail):
        counts[kind] = counts.get(kind, 0) + 1
        if len(issues) < limit:
            issues.append({"kind": kind, "table": table, "column": column, "rowid": rowid, "detail": detail})
    try:
        tables = inv.legacy_tables(conn)
        for t in tables:
            for _, col, ctype, *_ in conn.execute(f'PRAGMA table_info("{t}")').fetchall():
                ct = (ctype or "").upper()
                if LM.is_real_type(ct):
                    try:
                        money = LM.is_monetary(t, col)
                    except LM.UnclassifiedRealColumn:
                        add("UNCLASSIFIED_REAL_COLUMN", t, col, None, "classify it in legacy_money")
                        money = False
                    for rowid, v in conn.execute(f'SELECT rowid, "{col}" FROM "{t}" WHERE "{col}" IS NOT NULL'):
                        if money:
                            try:
                                tag = (target_types or {}).get((t.lower(), col.lower()), default_money)
                                _to_target(v, tag if tag.startswith("numeric") else default_money)
                            except LM.LegacyMoneyError as exc:
                                add(exc.code, t, col, rowid, str(exc)[:120])
                        elif not isinstance(v, (int, float)):
                            add("TYPE_AFFINITY_MISMATCH", t, col, rowid, f"{type(v).__name__} in REAL column")
                elif "INT" not in ct:
                    for (rowid,) in conn.execute(f'SELECT rowid FROM "{t}" WHERE typeof("{col}")=? '
                                                 f'AND instr("{col}", char(0)) > 0', ("text",)):
                        add("TEXT_WITH_NUL_CHARACTER", t, col, rowid, "PostgreSQL text cannot hold NUL")
                if "INT" in ct:
                    for rowid, v in conn.execute(f'SELECT rowid, "{col}" FROM "{t}" WHERE "{col}" IS NOT NULL '
                                                 f'AND typeof("{col}") <> \'integer\''):
                        add("TYPE_AFFINITY_MISMATCH", t, col, rowid, f"{type(v).__name__} {str(v)[:40]!r} in INTEGER column")
        orphans = conn.execute("PRAGMA foreign_key_check").fetchall()
        for table, rowid, parent, fkid in orphans:
            if table in tables:
                add("FOREIGN_KEY_ORPHAN", table, None, rowid, f"references a missing {parent} row (fk {fkid})")
    finally:
        conn.close()
    return {"ok": not counts, "issue_counts": dict(sorted(counts.items())), "issues": issues,
            "issues_truncated": sum(counts.values()) > len(issues)}


# ── PostgreSQL side ────────────────────────────────────────────────────────────────────────────────────────
def _pg_columns(pg) -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for table, col, dtype, nullable, default, is_identity, generation, prec, scale, generated in pg.execute(
            "SELECT c.table_name, c.column_name, c.data_type, c.is_nullable, c.column_default, c.is_identity, "
            "c.identity_generation, c.numeric_precision, c.numeric_scale, c.is_generated FROM information_schema.columns c JOIN information_schema.tables t "
            "ON t.table_schema=c.table_schema AND t.table_name=c.table_name WHERE c.table_schema='public' "
            "AND t.table_type='BASE TABLE' ORDER BY c.table_name, c.ordinal_position").fetchall():
        if dtype == "numeric":                         # carry the declared limits: "numeric" = unconstrained
            dtype = f"numeric:{prec}:{scale}" if prec is not None else "numeric"
        out.setdefault(table, {})[col] = {"type": dtype, "nullable": nullable == "YES", "default": default,
                                          "identity": is_identity == "YES", "generation": generation,
                                          "generated": generated == "ALWAYS"}
    return out


def _dec_canon(d: Decimal) -> str:
    sign, digits, exponent = d.as_tuple()
    digits = list(digits)
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    if digits == [0]:
        return "0"
    return format(Decimal((sign, tuple(digits), exponent)), "f")


def _to_target(value: Any, pg_type: str):
    """The value bound to a PostgreSQL column of ``pg_type``. Raises on anything that would change it."""
    from shared_lib.persistence import legacy_money as LM
    if value is None:
        return None
    if pg_type.startswith("numeric"):
        if pg_type == "numeric":                       # unconstrained NUMERIC holds every finite binary64 exactly
            return LM.to_decimal(value)
        _, prec, scale = pg_type.split(":")
        return LM.to_numeric_exact(value, int(prec), int(scale))
    if pg_type in ("double precision", "real"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise LM.MoneyTypeMismatch(f"{type(value).__name__} for {pg_type}")
        return float(value)
    if pg_type in ("bigint", "integer", "smallint"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise LM.MoneyTypeMismatch(f"{type(value).__name__} for {pg_type}")
        return value
    if pg_type == "boolean":
        if value in (0, 1):
            return bool(value)
        raise LM.MoneyTypeMismatch(f"{value!r} for boolean")
    if pg_type == "bytea":
        return bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else str(value).encode()
    if isinstance(value, (bytes, bytearray)):
        raise LM.MoneyTypeMismatch(f"bytes for {pg_type}")
    return value if isinstance(value, str) else (repr(value) if isinstance(value, float) else str(value))


def _canon(value: Any, pg_type: str):
    """One comparable form for a value on either side (source values go through ``_to_target`` first)."""
    if value is None:
        return None
    if pg_type.startswith("numeric"):
        return "N" + _dec_canon(value if isinstance(value, Decimal) else Decimal(str(value)))
    if pg_type in ("double precision", "real"):
        return "F" + repr(float(value))
    if pg_type in ("bigint", "integer", "smallint"):
        return "I" + str(int(value))
    if pg_type == "boolean":
        return "B" + str(bool(value))
    if pg_type in ("json", "jsonb"):
        obj = json.loads(value) if isinstance(value, (str, bytes)) else value
        return "J" + json.dumps(obj, sort_keys=True, separators=(",", ":"))
    if pg_type == "bytea":
        return "X" + bytes(value).hex()
    if hasattr(value, "isoformat"):                       # a time type in the target (text in the source)
        return "T" + value.isoformat()
    return "T" + (value if isinstance(value, str) else str(value))


class LegacyCopier:
    """Copies the legacy tables from a SQLite file into a PostgreSQL database that already holds the schema,
    in foreign-key order, in batches, with a checkpoint per table written in the SAME transaction as the batch
    (so a crash at any point leaves the checkpoint and the rows consistent and a rerun resumes from it)."""

    def __init__(self, source: Path, url: str, *, batch_size: int = 500) -> None:
        import psycopg
        self.source, self.url, self.batch_size = Path(source), url, int(batch_size)
        self.pg = psycopg.connect(url)
        self.src = sqlite3.connect(f"file:{self.source.as_posix()}?mode=ro", uri=True)
        inv = _load_script("legacy_persistence_inventory")
        self.tables = inv.legacy_tables(self.src)
        self.order, self.cycles = _fk_order(self.src, self.tables)
        self.columns = _pg_columns(self.pg)
        self.pg.commit()
        try:
            self.pg.execute("SET session_replication_role = replica")
            self.pg.execute("RESET session_replication_role")
            self.pg.commit()
            self.replica = True
        except Exception:  # noqa: BLE001 - not a superuser: FK order alone keeps parents first
            self.pg.rollback()
            self.replica = False

    def close(self) -> None:
        self.src.close()
        self.pg.close()

    # source / target column mapping (PostgreSQL folds unquoted identifiers to lower case)
    def mapping(self, table: str) -> List[Tuple[str, str, str]]:
        target = self.columns.get(table.lower(), {})
        lower = {c.lower(): c for c in target}
        out = []
        for c in self.src.execute(f'PRAGMA table_info("{table}")').fetchall():
            tc = lower.get(c[1].lower())
            if tc is not None:
                out.append((c[1], tc, target[tc]["type"]))
        return out

    def rowid_column(self, table: str) -> Optional[str]:
        """The target's rowid column (``cf_rowid`` of the compatibility layer) when it is a writable column
        (identity or plain); None when absent or generated from the INTEGER PRIMARY KEY (equal by construction)."""
        meta = self.columns.get(table.lower(), {}).get(ROWID_COLUMN)
        if meta is None or meta["generated"]:
            return None
        return ROWID_COLUMN

    def ensure_checkpoints(self, source_sha256: str) -> Dict[str, Any]:
        """Create the checkpoint schema. On a FRESH target (no checkpoint yet) the rows the schema creation
        seeded are recorded and removed: the source is authoritative (an operator may have changed a seeded row).
        A target that already holds checkpoints of a DIFFERENT source is refused."""
        cur = self.pg.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CHECKPOINT_SCHEMA}")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {CHECKPOINT_SCHEMA}.meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {CHECKPOINT_SCHEMA}.checkpoints (table_name TEXT PRIMARY KEY, "
                    "last_rowid BIGINT NOT NULL, rows_copied BIGINT NOT NULL, done BOOLEAN NOT NULL, "
                    "updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
        row = cur.execute(f"SELECT value FROM {CHECKPOINT_SCHEMA}.meta WHERE key='source_sha256'").fetchone()
        out: Dict[str, Any] = {"fresh": row is None, "seed_rows_replaced": {}}
        if row is not None:
            if row[0] != source_sha256:
                self.pg.rollback()
                raise RuntimeError("TARGET_HOLDS_ANOTHER_SOURCES_CHECKPOINTS")
            self.pg.commit()
            return out
        if self.replica:
            cur.execute("SET LOCAL session_replication_role = replica")
        for t in reversed(self.order):
            if t.lower() in self.columns:
                n = cur.execute(f"SELECT COUNT(*) FROM {_q(t.lower())}").fetchone()[0]
                if n:
                    cur.execute(f"DELETE FROM {_q(t.lower())}")
                    out["seed_rows_replaced"][t] = n
        cur.execute(f"INSERT INTO {CHECKPOINT_SCHEMA}.meta VALUES ('source_sha256', %s)", (source_sha256,))
        self.pg.commit()
        return out

    def checkpoint(self, table: str) -> Tuple[int, int, bool]:
        row = self.pg.execute(f"SELECT last_rowid, rows_copied, done FROM {CHECKPOINT_SCHEMA}.checkpoints "
                              "WHERE table_name=%s", (table,)).fetchone()
        self.pg.commit()
        return (row[0], row[1], row[2]) if row else (0, 0, False)

    def _sequences(self, cur, table: str) -> None:
        for col, meta in self.columns.get(table.lower(), {}).items():
            if meta["identity"] or "nextval(" in (meta["default"] or ""):
                seq = cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (_q(table.lower()), col)).fetchone()[0]
                if seq:
                    top = cur.execute(f"SELECT MAX({_q(col)}) FROM {_q(table.lower())}").fetchone()[0]
                    if top is None:
                        cur.execute("SELECT setval(%s, 1, false)", (seq,))
                    else:
                        cur.execute("SELECT setval(%s, %s, true)", (seq, int(top)))

    def copy(self, *, crash: Optional[Tuple[str, int]] = None) -> Dict[str, Any]:
        """Copy every table not yet done. ``crash=(table, k)`` raises ``SimulatedCrash`` while batch k+1 of
        ``table`` is inserted but not committed (the drill's interruption test)."""
        stats = {"tables_copied": 0, "rows_inserted": 0, "tables_skipped_done": 0, "batches": 0, "resumed": {}}
        for t in self.order:
            if t.lower() not in self.columns:
                continue                                      # reported by the coverage check
            last, copied, done = self.checkpoint(t)
            if done:
                stats["tables_skipped_done"] += 1
                continue
            if last:
                stats["resumed"][t] = {"from_rowid": last, "rows_already_copied": copied}
            cols = self.mapping(t)
            rowid_col = self.rowid_column(t)             # the source rowid is kept, not renumbered
            names = [tc for _, tc, _ in cols] + ([rowid_col] if rowid_col else [])
            overriding = any(self.columns[t.lower()][n]["generation"] == "ALWAYS" for n in names)
            insert = (f"INSERT INTO {_q(t.lower())} ({', '.join(_q(n) for n in names)}) "
                      f"{'OVERRIDING SYSTEM VALUE ' if overriding else ''}VALUES ({', '.join(['%s'] * len(names))})")
            select = (f'SELECT rowid, {", ".join(_q(sc) for sc, _, _ in cols)} FROM "{t}" WHERE rowid > ? '
                      f"ORDER BY rowid LIMIT ?")
            batch_no = 0
            while True:
                rows = self.src.execute(select, (last, self.batch_size)).fetchall()
                cur = self.pg.cursor()
                if self.replica:
                    cur.execute("SET LOCAL session_replication_role = replica")
                if rows:
                    payload = [tuple(_to_target(v, pt) for v, (_, _, pt) in zip(r[1:], cols))
                               + ((r[0],) if rowid_col else ()) for r in rows]
                    cur.executemany(insert, payload)
                    last, copied = rows[-1][0], copied + len(rows)
                    if crash and crash[0] == t and batch_no == crash[1]:
                        self.pg.rollback()
                        raise SimulatedCrash(f"simulated crash in {t} during batch {batch_no + 1} (not committed)")
                finished = len(rows) < self.batch_size
                if finished:
                    self._sequences(cur, t)
                cur.execute(f"INSERT INTO {CHECKPOINT_SCHEMA}.checkpoints (table_name, last_rowid, rows_copied, done) "
                            "VALUES (%s,%s,%s,%s) ON CONFLICT (table_name) DO UPDATE SET last_rowid=excluded.last_rowid, "
                            "rows_copied=excluded.rows_copied, done=excluded.done, updated_at=clock_timestamp()",
                            (t, last, copied, finished))
                self.pg.commit()
                stats["rows_inserted"] += len(rows)
                stats["batches"] += 1 if rows else 0
                batch_no += 1
                if finished:
                    break
            stats["tables_copied"] += 1
        return stats


# ── verification ───────────────────────────────────────────────────────────────────────────────────────────
def _source_rows(src: sqlite3.Connection, table: str, cols: List[Tuple[str, str, str]]) -> List[tuple]:
    rows = src.execute(f'SELECT {", ".join(_q(sc) for sc, _, _ in cols)} FROM "{table}"').fetchall()
    return [tuple(_canon(_to_target(v, pt), pt) for v, (_, _, pt) in zip(r, cols)) for r in rows]


def _target_rows(pg, table: str, cols: List[Tuple[str, str, str]]) -> List[tuple]:
    rows = pg.execute(f"SELECT {', '.join(_q(tc) for _, tc, _ in cols)} FROM {_q(table.lower())}").fetchall()
    pg.commit()
    return [tuple(_canon(v, pt) for v, (_, _, pt) in zip(r, cols)) for r in rows]


def _digest(rows: List[tuple]) -> str:
    h = hashlib.sha256()
    for r in sorted(rows, key=repr):
        h.update(repr(r).encode())
    return h.hexdigest()


def target_snapshot(url: str, copier_like: "LegacyCopier") -> Dict[str, Any]:
    """Per-table counts and canonical content hashes of a PostgreSQL database (legacy tables of the source)."""
    import psycopg
    out = {"counts": {}, "hashes": {}}
    with psycopg.connect(url) as pg:
        for t in copier_like.tables:
            if t.lower() not in copier_like.columns:
                continue
            rows = _target_rows(pg, t, copier_like.mapping(t))
            out["counts"][t] = len(rows)
            out["hashes"][t] = _digest(rows)
    return out


def source_snapshot(copier: "LegacyCopier") -> Dict[str, Any]:
    out = {"counts": {}, "hashes": {}}
    for t in copier.tables:
        if t.lower() not in copier.columns:
            continue
        rows = _source_rows(copier.src, t, copier.mapping(t))
        out["counts"][t] = len(rows)
        out["hashes"][t] = _digest(rows)
    return out


def _pg_indexes(pg) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for table, name, unique, primary, partial, has_expr, cols in pg.execute(
            "SELECT t.relname, i.relname, ix.indisunique, ix.indisprimary, ix.indpred IS NOT NULL, "
            "ix.indexprs IS NOT NULL, ARRAY(SELECT a.attname FROM unnest(ix.indkey) WITH ORDINALITY k(attnum, ord) "
            "JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum ORDER BY k.ord) "
            "FROM pg_index ix JOIN pg_class t ON t.oid=ix.indrelid JOIN pg_class i ON i.oid=ix.indexrelid "
            "JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public'").fetchall():
        out.setdefault(table, []).append({"name": name, "unique": unique, "primary": primary, "partial": partial,
                                          "expression": has_expr, "columns": [c.lower() for c in cols]})
    pg.commit()
    return out


def _pg_foreign_keys(pg) -> List[Dict[str, Any]]:
    rows = pg.execute(
        "SELECT c.conname, cl.relname, ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(n, o) "
        "JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.n ORDER BY k.o), pcl.relname, "
        "ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY k(n, o) JOIN pg_attribute a "
        "ON a.attrelid=c.confrelid AND a.attnum=k.n ORDER BY k.o) "
        "FROM pg_constraint c JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_class pcl ON pcl.oid=c.confrelid "
        "JOIN pg_namespace n ON n.oid=cl.relnamespace WHERE c.contype='f' AND n.nspname='public' ORDER BY 1").fetchall()
    pg.commit()
    return [{"name": r[0], "table": r[1], "columns": list(r[2]), "references": r[3], "to": list(r[4])} for r in rows]


def verify_legacy(copier: LegacyCopier, src_snap: Dict[str, Any], tgt_snap: Dict[str, Any]) -> Dict[str, Any]:
    """Checks 1-9 (see ``run_legacy``)."""
    from shared_lib.persistence import legacy_money as LM
    pg, src = copier.pg, copier.src
    checks: Dict[str, Dict[str, Any]] = {}
    src_tables = set(copier.tables)
    compat_internal = sorted(t for t in copier.columns if COMPAT_INTERNAL_TABLE.match(t))
    tgt_tables = set(copier.columns) - {t for t in copier.columns if PLATFORM_TABLE_RX.match(t)} - set(compat_internal)
    src_counts = {t: src.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in sorted(src_tables)}

    # (1) source row counts
    checks["01_source_row_counts"] = {"passed": True, "evidence": {
        "tables": len(src_counts), "rows": sum(src_counts.values()),
        "tables_with_rows": sum(1 for v in src_counts.values() if v)}}

    # (2) destination row counts
    diff = {t: {"source": n, "target": tgt_snap["counts"].get(t)} for t, n in src_counts.items()
            if tgt_snap["counts"].get(t) != n}
    rowid_tables, rowid_diff = 0, []
    for t in sorted(src_tables):
        if ROWID_COLUMN not in copier.columns.get(t.lower(), {}):
            continue
        rowid_tables += 1
        s_ids = [r[0] for r in src.execute(f'SELECT rowid FROM "{t}" ORDER BY rowid')]
        t_ids = [r[0] for r in pg.execute(f"SELECT {ROWID_COLUMN} FROM {_q(t.lower())} ORDER BY 1")]
        if s_ids != t_ids:
            rowid_diff.append(t)
    pg.commit()
    checks["02_destination_row_counts"] = {"passed": not diff and not rowid_diff, "evidence": {
        "rows": sum(tgt_snap["counts"].values()), "tables_compared": len(tgt_snap["counts"]), "mismatches": diff,
        "rowid_preserved_tables": rowid_tables - len(rowid_diff), "rowid_differs": rowid_diff}}

    # (3) coverage both ways (tables and columns); target-only tables must be empty
    lower_src = {t.lower(): t for t in src_tables}
    missing_tables = sorted(t for t in src_tables if t.lower() not in copier.columns)
    target_only = sorted(t for t in tgt_tables if t not in lower_src)
    target_only_rows = {}
    for t in target_only:
        n = pg.execute(f"SELECT COUNT(*) FROM {_q(t)}").fetchone()[0]
        if n:
            target_only_rows[t] = n
    pg.commit()
    missing_cols, extra_required = {}, {}
    for t in sorted(src_tables - set(missing_tables)):
        scols = [c[1] for c in src.execute(f'PRAGMA table_info("{t}")').fetchall()]
        mapped = {sc.lower() for sc, _, _ in copier.mapping(t)}
        miss = [c for c in scols if c.lower() not in mapped]
        if miss:
            missing_cols[t] = miss
        extra = [c for c, m in copier.columns[t.lower()].items() if c not in {s.lower() for s in scols}
                 and not m["nullable"] and m["default"] is None and not m["identity"] and not m["generated"]]
        if extra:
            extra_required[t] = extra
    src_triggers = sum(1 for (tbl,) in src.execute("SELECT tbl_name FROM sqlite_master WHERE type='trigger'")
                       if tbl in src_tables)
    tgt_triggers = pg.execute("SELECT COUNT(*) FROM pg_trigger g JOIN pg_class c ON c.oid=g.tgrelid JOIN pg_namespace n "
                              "ON n.oid=c.relnamespace WHERE NOT g.tgisinternal AND n.nspname='public'").fetchone()[0]
    pg.commit()
    checks["03_table_coverage_both_ways"] = {
        "passed": not missing_tables and not missing_cols and not target_only_rows and not extra_required,
        "evidence": {"source_tables": len(src_tables), "target_tables": len(tgt_tables),
                     "source_triggers": src_triggers, "target_triggers_informational": tgt_triggers,
                     "compatibility_layer_tables": compat_internal,
                     "source_tables_missing_in_target": missing_tables, "source_columns_missing_in_target": missing_cols,
                     "target_only_tables": target_only, "target_only_tables_with_rows": target_only_rows,
                     "target_only_required_columns": extra_required}}

    # (4) foreign keys. The legacy SQLite connections never enable ``foreign_keys``, so the compatibility layer may
    # keep REFERENCES declarative (not enforced) to preserve runtime behaviour. Integrity is therefore checked for
    # EVERY FK the source declares, evaluated on the target data, whether or not the target enforces it.
    tfks = _pg_foreign_keys(pg)
    enforced = {(f["table"], tuple(c.lower() for c in f["columns"]), f["references"]) for f in tfks}
    declared, orphans = [], {}
    for t in sorted(src_tables):
        groups: Dict[int, Dict[str, Any]] = {}
        for fid, _, ref, frm, to, *_ in src.execute(f'PRAGMA foreign_key_list("{t}")'):
            g = groups.setdefault(fid, {"ref": ref, "from": [], "to": []})
            g["from"].append(frm.lower())
            g["to"].append(to.lower() if to else None)
        for g in groups.values():
            if None in g["to"]:
                pk = [c[1].lower() for c in sorted((c for c in src.execute(f'PRAGMA table_info("{g["ref"]}")')
                                                    if c[5]), key=lambda c: c[5])]
                g["to"] = pk
            label = f"{t}({','.join(g['from'])}) -> {g['ref']}({','.join(g['to'])})"
            declared.append({"fk": label, "enforced_in_target": (t.lower(), tuple(g["from"]), g["ref"].lower()) in enforced})
            if t.lower() not in copier.columns or g["ref"].lower() not in copier.columns:
                orphans[label] = "TABLE_MISSING_IN_TARGET"
                continue
            cond = " AND ".join(f"c.{_q(a)} IS NOT NULL" for a in g["from"])
            join = " AND ".join(f"p.{_q(b)} = c.{_q(a)}" for a, b in zip(g["from"], g["to"]))
            n = pg.execute(f"SELECT COUNT(*) FROM {_q(t.lower())} c WHERE {cond} AND NOT EXISTS "
                           f"(SELECT 1 FROM {_q(g['ref'].lower())} p WHERE {join})").fetchone()[0]
            if n:
                orphans[label] = n
    pg.commit()
    checks["04_foreign_key_integrity"] = {"passed": bool(declared) and not orphans, "evidence": {
        "source_declared_foreign_keys": len(declared), "target_enforced_foreign_keys": len(tfks),
        "declared_but_not_enforced_in_target": sum(1 for d in declared if not d["enforced_in_target"]),
        "target_orphans": orphans,
        "note": "orphans are counted for every FK the source declares, on the migrated rows"}}

    # (5) unique constraints present and unviolated
    tidx = _pg_indexes(pg)
    src_uniques, missing_u, violated = 0, [], {}
    for t in sorted(src_tables):
        tset = [(sorted(i["columns"]) + (["<expr>"] if i["expression"] else []), i["partial"])
                for i in tidx.get(t.lower(), []) if i["unique"]]
        for _, name, unique, origin, partial in src.execute(f'PRAGMA index_list("{t}")').fetchall():
            if not unique:
                continue
            src_uniques += 1
            keys = [(r[2] or "<expr>").lower() for r in src.execute(f'PRAGMA index_xinfo("{name}")').fetchall() if r[5]]
            want = sorted(k for k in keys if k != "<expr>") + (["<expr>"] if "<expr>" in keys else [])
            if (want, bool(partial)) not in tset:
                missing_u.append(f"{t}({','.join(keys)}){' partial' if partial else ''}")
            elif not partial and "<expr>" not in keys:
                cols = ", ".join(_q(k) for k in keys)
                notnull = " AND ".join(f"{_q(k)} IS NOT NULL" for k in keys)
                dup = pg.execute(f"SELECT COUNT(*) FROM (SELECT {cols} FROM {_q(t.lower())} WHERE {notnull} "
                                 f"GROUP BY {cols} HAVING COUNT(*) > 1) d").fetchone()[0]
                if dup:
                    violated[f"{t}({','.join(keys)})"] = dup
    pg.commit()
    checks["05_unique_constraints"] = {"passed": not missing_u and not violated, "evidence": {
        "source_unique_indexes": src_uniques,
        "target_unique_indexes": sum(1 for v in tidx.values() for i in v if i["unique"]),
        "missing_in_target": missing_u, "violated": violated}}

    # (6) financial values: every monetary value exactly equal as Decimal, per-column sums in NUMERIC
    compared, mismatches, sums, wrong_type, non_money_compared = 0, [], {}, [], 0
    for t in sorted(src_tables - set(missing_tables)):
        cols = copier.mapping(t)
        info = {c[1]: c for c in src.execute(f'PRAGMA table_info("{t}")').fetchall()}
        pk = [c[1] for c in sorted((c for c in info.values() if c[5]), key=lambda c: c[5])] or ["rowid"]
        for sc, tc, pt in cols:
            if not LM.is_real_type(info[sc][2]):
                continue
            try:
                money = LM.is_monetary(t, sc)
            except LM.UnclassifiedRealColumn:
                money = False
            if money and not pt.startswith("numeric"):
                wrong_type.append(f"{t}.{sc}:{pt}")
                continue
            pk_src = ", ".join(_q(k) for k in pk)
            svals = {tuple(r[:-1]): r[-1] for r in src.execute(f'SELECT {pk_src}, {_q(sc)} FROM "{t}"')}
            pk_tgt = ", ".join(_q(k.lower()) for k in pk) if pk != ["rowid"] else None
            if pk_tgt is None:
                continue
            tvals = {tuple(r[:-1]): r[-1] for r in pg.execute(f"SELECT {pk_tgt}, {_q(tc)} FROM {_q(t.lower())}")}
            if money:
                total_src = Decimal(0)
                for key, v in svals.items():
                    d = LM.to_decimal(v)
                    got = tvals.get(key)
                    compared += 1 if d is not None else 0
                    if (d is None) != (got is None) or (d is not None and Decimal(got) != d):
                        mismatches.append({"table": t, "column": sc, "key": repr(key)[:80], "source": str(d),
                                           "target": str(got)})
                    total_src += d or 0
                total_tgt = pg.execute(f"SELECT COALESCE(SUM({_q(tc)}), 0) FROM {_q(t.lower())}").fetchone()[0]
                if svals:
                    sums[f"{t}.{sc}"] = {"source_decimal_sum": _dec_canon(total_src),
                                         "target_numeric_sum": _dec_canon(Decimal(total_tgt)),
                                         "equal": Decimal(total_tgt) == total_src}
            else:
                for key, v in svals.items():
                    got = tvals.get(key)
                    non_money_compared += 1 if v is not None else 0
                    if (v is None) != (got is None) or (v is not None and float(got) != float(v)):
                        mismatches.append({"table": t, "column": sc, "key": repr(key)[:80], "source": repr(v),
                                           "target": repr(got)})
        pg.commit()
    sum_failures = sorted(k for k, v in sums.items() if not v["equal"])
    checks["06_financial_value_preservation"] = {
        "passed": not mismatches and not wrong_type and not sum_failures and compared > 0,
        "evidence": {"monetary_values_compared": compared, "non_monetary_real_values_compared": non_money_compared,
                     "mismatches": mismatches[:50], "mismatch_count": len(mismatches),
                     "monetary_columns_not_numeric_in_target": wrong_type,
                     "monetary_columns_summed": len(sums), "sum_mismatches": sum_failures,
                     "sample_sums": dict(list(sorted(sums.items()))[:12])}}

    # (7) trading-intent identity
    ident = {}
    for t in INTENT_TABLES:
        if t not in src_tables or t.lower() not in copier.columns:
            continue
        cols = [c for c in copier.mapping(t) if IDENTITY_COLUMN.search(c[0])]
        s, g = _source_rows(src, t, cols), _target_rows(pg, t, cols)
        ident[t] = {"identity_columns": [c[0] for c in cols], "rows": len(s), "equal": _digest(s) == _digest(g),
                    "distinct_source_ids": len(set(s))}
    checks["07_trading_intent_identity"] = {"passed": bool(ident) and all(v["equal"] for v in ident.values()),
                                            "evidence": ident}

    # (8) position / protection / execution associations still join
    joins = {}
    for child, ccol, parent, pcol in ASSOCIATIONS:
        if not {child, parent} <= src_tables or child.lower() not in copier.columns or parent.lower() not in copier.columns:
            continue
        sq = (f'SELECT COUNT(*) FROM "{child}" c JOIN "{parent}" p ON p."{pcol}" = c."{ccol}"')
        tq = (f"SELECT COUNT(*) FROM {_q(child)} c JOIN {_q(parent)} p ON p.{_q(pcol)} = c.{_q(ccol)}")
        s, g = src.execute(sq).fetchone()[0], pg.execute(tq).fetchone()[0]
        joins[f"{child}.{ccol} -> {parent}.{pcol}"] = {"source": s, "target": g}
    pg.commit()
    checks["08_position_protection_associations"] = {
        "passed": bool(joins) and all(v["source"] == v["target"] for v in joins.values()),
        "evidence": {"joins": joins, "joining_pairs_in_source": sum(v["source"] for v in joins.values())}}

    # (9) research governance: content hashes equal
    gov = {t: {"rows": src_snap["counts"].get(t), "hash_equal": src_snap["hashes"].get(t) == tgt_snap["hashes"].get(t)}
           for t in sorted(src_tables) if GOVERNANCE_TABLE.match(t)}
    checks["09_research_governance_preservation"] = {
        "passed": bool(gov) and all(v["hash_equal"] for v in gov.values()),
        "evidence": {"tables": gov, "rows": sum(v["rows"] or 0 for v in gov.values())}}
    return checks


PLATFORM_TABLE_RX = re.compile(r"^(platform_|ops_|track_record_|golive_events$|incident_records$|beta_reviews$|"
                               r"drill_venue_orders$)")


def float8_cast_loss(url: str, values: List[float]) -> Dict[str, Any]:
    """How many of ``values`` PostgreSQL's own ``float8 -> numeric`` cast would change (versus ``to_decimal``)."""
    import psycopg
    from shared_lib.persistence import legacy_money as LM
    vals = sorted({v for v in values if isinstance(v, float)})[:5000]
    if not vals:
        return {"values": 0, "changed_by_float8_cast": 0}
    with psycopg.connect(url) as pg:
        cast = pg.execute("SELECT v::numeric FROM unnest(%s::float8[]) WITH ORDINALITY u(v, o) ORDER BY o",
                          (vals,)).fetchall()
    changed = [(v, c[0]) for v, c in zip(vals, cast) if Decimal(c[0]) != LM.to_decimal(v)]
    return {"values": len(vals), "changed_by_float8_cast": len(changed),
            "examples": [{"stored": repr(v), "to_decimal": str(LM.to_decimal(v)), "float8_cast": str(c)}
                         for v, c in changed[:5]]}


# ── application startup on the target (A1's PostgreSQL compatibility layer) ────────────────────────────────
def startup_smoke(url: str, before: Dict[str, Any], copier: LegacyCopier) -> Dict[str, Any]:
    """Open the migrated target with the legacy ``DB`` in PostgreSQL mode and exercise: schema ensure idempotent,
    broker accounts read, runtime-ownership lease acquire/release, a reservation round trip."""
    out: Dict[str, Any] = {}
    try:
        from shared_lib.persistence import legacy_postgres as LP
    except ImportError as exc:
        return {"passed": False, "status": "BLOCKED", "reason": f"legacy_postgres not importable: {exc}"}
    try:
        LP.create_schema(url)
        after = target_snapshot(url, copier)
        changed = sorted(t for t in before["hashes"] if after["hashes"].get(t) != before["hashes"][t])
        out["schema_ensure_idempotent"] = {"changed_tables": changed,
                                           "unexpected": [t for t in changed if t not in RESEEDED_CONFIG_TABLES],
                                           "counts_equal": after["counts"] == before["counts"]}
        from shared_lib.persistence.db import DB
        db = DB(url=url)
        with db.connect() as conn:
            rows = conn.execute("SELECT id, broker_id, environment FROM broker_accounts ORDER BY id").fetchall()
        out["broker_accounts_read"] = len(rows)
        from app.ops.runtime_ownership import RuntimeOwnership, current_owner
        own = RuntimeOwnership(db, database_path="legacy-migration-drill", database_role="development")
        acquired = own.acquire()
        holder = current_owner(db, own.database_path)
        own.release(reason="LEGACY_MIGRATION_DRILL")
        after_release = current_owner(db, own.database_path)
        out["runtime_ownership"] = {"acquired": acquired.acquired, "reason": acquired.reason,
                                    "holder_seen": bool(holder) and holder.get("runtime_owner_id") == own.runtime_owner_id,
                                    "held_after_release": bool(after_release)}
        out["reservation_round_trip"] = _reservation_round_trip(db)
        ok = (not out["schema_ensure_idempotent"]["unexpected"] and out["schema_ensure_idempotent"]["counts_equal"]
              and out["broker_accounts_read"] == before["counts"].get("broker_accounts")
              and acquired.acquired and out["runtime_ownership"]["holder_seen"]
              and not out["runtime_ownership"]["held_after_release"]
              and out["reservation_round_trip"].get("ok") is True)
        out["passed"] = bool(ok)
        out["status"] = "PASSED" if ok else "FAILED"
    except Exception as exc:  # noqa: BLE001 - every failure of the compatibility layer is evidence
        out.update(passed=False, status="FAILED", error=f"{type(exc).__name__}: {str(exc)[:400]}")
    return out


def _reservation_round_trip(db) -> Dict[str, Any]:
    """Reserve one entry through the canonical ledger, read it back, release it, read the event history."""
    import dataclasses
    from app.exchange.platform_adapter.contract import InstrumentRules
    from app.trading_intelligence.pipeline import portfolio as P
    from app.trading_intelligence.pipeline import reservations as R
    from app.trading_intelligence.pipeline.brakes import NO_BRAKE
    from app.trading_intelligence.pipeline.contracts import LONG, EntryIntent
    now = 1_780_000_000_000
    account, user, symbol = "drill-acct", "drill-user", "DRILLUSDT"
    rules = {symbol: InstrumentRules(venue="binance", environment="DEMO", symbol=symbol, venue_symbol=symbol,
                                     status="TRADING", venue_status="TRADING", base_asset="DRILL", quote_asset="USDT",
                                     margin_asset="USDT", price_tick=0.01, quantity_step=0.001, min_quantity=0.001,
                                     max_quantity=1_000_000.0, min_notional=5.0, max_leverage=20.0, contract_size=1.0,
                                     fetched_at_ms=1)}
    equity, price = 10_000.0, 100.0
    intent = EntryIntent(symbol=symbol, direction=LONG, quantity=0.001 * equity / 0.10 / price, reference_price=price,
                         stop_price=price * 0.9, risk_fraction=0.001)
    snap = P.AccountSnapshot(account_id=account, equity=equity, wallet_balance=equity, available_margin=equity,
                             positions=(), reservations=(), as_of_ms=now - 1_000)
    scope = R.ReservationScope(account_id=account, user_id=user, family_id="daily_trend", decision_id="drill-dec",
                               decision_hash="drill-hash", mode="DEMO", expires_at_ms=now + 86_400_000)
    ledger = R.ReservationLedger(db)
    outcome = ledger.reserve([intent], snapshot=snap, limits=dataclasses.replace(P.AccountLimits.from_profile("conservative")),
                             rules=rules, brake_state=NO_BRAKE, now_ms=now, scope=scope)
    res = outcome.reservation(symbol)
    if res is None:
        return {"ok": False, "reason": "NOT_RESERVED", "detail": repr(outcome)[:300]}
    read = ledger.get(account, res.reservation_id)
    transition = ledger.release(account_id=account, reservation_id=res.reservation_id, now_ms=now + 1, reason="DRILL")
    final = ledger.get(account, res.reservation_id)
    history = ledger.history(account, res.reservation_id)
    exact = read is not None and read.quantity == intent.quantity and read.notional is not None
    return {"ok": (read is not None and read.state == R.RESERVED and exact and final is not None
                   and final.state == R.RELEASED and len(history) >= 2),
            "reserved_state": read.state if read else None, "reserved_quantity": repr(read.quantity) if read else None,
            "quantity_round_trip_exact": exact, "release": repr(transition)[:160],
            "final_state": final.state if final else None, "events": len(history)}


# ── the drill ──────────────────────────────────────────────────────────────────────────────────────────────
def _create_db(admin_url: str, name: str) -> str:
    import psycopg
    with psycopg.connect(admin_url, autocommit=True) as c:
        c.execute(f"CREATE DATABASE {name}")
    return f"{admin_url.rpartition('/')[0]}/{name}"


def _drop_db(admin_url: str, name: str) -> None:
    import psycopg
    with psycopg.connect(admin_url, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def run_legacy(workdir: Path, *, target_url: str, source: Optional[Path] = None,
               owner_authorization: Optional[str] = None, rows: int = 5, batch_size: int = 500,
               crash_table: Optional[str] = None, keep_databases: bool = False) -> Dict[str, Any]:
    """The 13 checks:

     1 source row counts; 2 destination row counts; 3 table (and column) coverage both ways;
     4 FK integrity in the target (declared FKs present, no orphans); 5 unique constraints present and unviolated;
     6 financial-value preservation (every monetary value equal as Decimal, per-column NUMERIC sums equal);
     7 trading-intent identity; 8 position / protection associations still join;
     9 research-governance content hashes equal; 10 interrupted-migration recovery (a crash inside a batch of a
       table, rerun resumes from the checkpoint, final state identical); 11 rerun safety (a second full run copies
       nothing and changes nothing); 12 ``pg_dump -Fc`` / ``pg_restore`` into a second database, counts and hashes
       equal; 13 application startup on the target through the PostgreSQL compatibility layer.

    Source inconsistencies (non-finite money, type-affinity mismatches, FK orphans in the source) are reported by
    table / column / rowid and stop the drill before anything is copied: nothing is fixed or dropped silently."""
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    result: Dict[str, Any] = {"schema": LEGACY_SCHEMA_ID, "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              "checks": {}, "limits": (
        "A disposable rehearsal on throwaway databases. It is not the migration of the production database, which "
        "needs the owner's authorization, a sanitized copy and a maintenance window (EXT-DB-01).")}
    copy = workdir / "legacy_source.db"
    for suffix in ("", "-wal", "-shm"):
        Path(str(copy) + suffix).unlink(missing_ok=True)
    if source is not None:
        reason = legacy_source_refusal(Path(source))
        if reason and not owner_authorization:
            raise LegacySourceRefused(f"--source refused: {reason}. Pass --owner-authorization <reference> only with the "
                                      "owner's written authorization to read that database.")
        result["source"] = {"kind": "PROVIDED_FILE", "refusal_overridden_by_owner_authorization": bool(reason),
                            "owner_authorization": owner_authorization, "sha256_before": sha256_file(Path(source))}
        # Pin every database resolution to the work directory before any application import.
        os.environ["DATABASE_URL"] = f"sqlite:///{(workdir / 'unused-runtime.db').as_posix()}"
        os.environ["COSMICFORGE_TEST_MODE"] = "1"
        online_copy(Path(source), copy)
    else:
        inv = _load_script("legacy_persistence_inventory")
        inv.isolated_environment(workdir / "unused-runtime.db")
        t0 = time.perf_counter()
        result["source"] = {"kind": "REPRESENTATIVE_SYNTHETIC", **build_legacy_source(copy, rows),
                            "build_ms": round((time.perf_counter() - t0) * 1000, 1)}
    single_file(copy)
    source_sha = sha256_file(copy)
    result["source"]["copy_sha256"] = source_sha

    def refused(audit: Dict[str, Any]) -> Dict[str, Any]:
        result["source_audit"] = audit
        result["checks"] = {"00_source_audit_clean": False}
        result["passed"] = False
        result["outcome"] = "REFUSED_SOURCE_INCONSISTENT"
        result["finished_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return result

    try:
        from shared_lib.persistence import legacy_postgres as LP
    except ImportError as exc:
        audit = audit_legacy_source(copy)
        if not audit["ok"]:
            return refused(audit)
        result["source_audit"] = audit
        result["passed"] = False
        result["outcome"] = "BLOCKED_NO_POSTGRES_COMPATIBILITY_LAYER"
        result["blocked_reason"] = f"shared_lib.persistence.legacy_postgres is not importable: {exc}"
        return result

    tag = uuid.uuid4().hex[:10]
    # cftest_*: the only PostgreSQL names the test-isolation guard lets a test-mode process open
    tname, rname = f"cftest_legacy_{tag}", f"cftest_legacy_restore_{tag}"
    target = _create_db(target_url, tname)
    result["target"] = {"database": tname, "restore_database": rname, "server": target_url.rpartition("/")[0]
                        .split("@")[-1]}
    detail: Dict[str, Any] = {}
    checks: Dict[str, Dict[str, Any]] = {}
    try:
        t0 = time.perf_counter()
        LP.create_schema(target)
        detail["create_schema_ms"] = round((time.perf_counter() - t0) * 1000, 1)

        copier = LegacyCopier(copy, target, batch_size=batch_size)
        detail["target_money_types"] = sorted({m["type"] for t in copier.columns.values() for m in t.values()
                                               if m["type"].startswith("numeric")})
        audit = audit_legacy_source(copy, target_types={(t, c): m["type"] for t, cols in copier.columns.items()
                                                        for c, m in cols.items()})
        if not audit["ok"]:
            copier.close()
            return refused(audit)
        result["source_audit"] = audit
        detail["copy_order_fk_cycles"] = copier.cycles
        detail["triggers_and_fk_checks_disabled_during_copy"] = copier.replica
        detail["checkpoints"] = copier.ensure_checkpoints(source_sha)
        src_snap = source_snapshot(copier)

        # (10) interrupted migration: crash inside a batch of a table with several batches, then resume
        # The crashing run uses batches small enough that the victim table needs at least two; the resuming run
        # uses the configured batch size (a checkpoint is a rowid, so the two need not agree).
        eligible = [t for t in copier.order if src_snap["counts"].get(t, 0) >= 2 and t.lower() in copier.columns]
        victim = crash_table if crash_table in eligible else ("trade_fills" if "trade_fills" in eligible
                                                               else (eligible[0] if eligible else None))
        t0 = time.perf_counter()
        crash_info: Dict[str, Any] = {"table": victim}
        if victim:
            copier.batch_size = max(1, min(batch_size, src_snap["counts"][victim] // 2))
            crash_info["crash_run_batch_size"] = copier.batch_size
            try:
                copier.copy(crash=(victim, 1))
            except SimulatedCrash as exc:
                crash_info["crash"] = str(exc)
            copier.close()
            copier = LegacyCopier(copy, target, batch_size=batch_size)
            copier.ensure_checkpoints(source_sha)
            last, copied, done = copier.checkpoint(victim)
            n = copier.pg.execute(f"SELECT COUNT(*) FROM {_q(victim.lower())}").fetchone()[0]
            copier.pg.commit()
            crash_info.update(checkpoint_after_crash={"last_rowid": last, "rows_copied": copied, "done": done},
                              target_rows_after_crash=n, rows_consistent_with_checkpoint=(n == copied and not done))
        stats = copier.copy()
        detail["copy"] = {**stats, "ms": round((time.perf_counter() - t0) * 1000, 1), "batch_size": batch_size}
        tgt_snap = target_snapshot(target, copier)
        crash_info["resumed_from"] = stats["resumed"].get(victim)
        identical = tgt_snap == src_snap
        checks["10_interrupted_migration_recovery"] = {
            "passed": bool(victim) and "crash" in crash_info and crash_info.get("rows_consistent_with_checkpoint", False)
                      and crash_info["resumed_from"] is not None and identical,
            "evidence": {**crash_info, "final_state_identical_to_source": identical}}

        checks.update(verify_legacy(copier, src_snap, tgt_snap))
        money_values = []
        for t in copier.tables:
            for sc, _, pt in copier.mapping(t) if t.lower() in copier.columns else ():
                if pt.startswith("numeric"):
                    money_values += [r[0] for r in copier.src.execute(f'SELECT "{sc}" FROM "{t}" WHERE "{sc}" IS NOT NULL')]
        detail["postgres_float8_cast_comparison"] = float8_cast_loss(target, money_values)

        # (11) rerun safety
        again = copier.copy()
        rerun_snap = target_snapshot(target, copier)
        checks["11_rerun_safety"] = {"passed": again["rows_inserted"] == 0 and rerun_snap == tgt_snap, "evidence": {
            "rows_inserted": again["rows_inserted"], "tables_skipped_done": again["tables_skipped_done"],
            "state_unchanged": rerun_snap == tgt_snap}}

        # (12) backup and restore
        dump = workdir / f"{tname}.dump"
        bk: Dict[str, Any] = {}
        try:
            t0 = time.perf_counter()
            subprocess.run([_pg_tool("pg_dump"), "-Fc", "-f", str(dump), target], check=True, capture_output=True)
            bk["dump_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            bk["dump_bytes"] = dump.stat().st_size
            restored = _create_db(target_url, rname)
            t0 = time.perf_counter()
            subprocess.run([_pg_tool("pg_restore"), "--no-owner", "-d", restored, str(dump)], check=True,
                           capture_output=True)
            bk["restore_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            restored_snap = target_snapshot(restored, copier)
            bk["counts_equal"] = restored_snap["counts"] == tgt_snap["counts"]
            bk["hashes_equal"] = restored_snap["hashes"] == tgt_snap["hashes"]
            bk["tables"] = len(restored_snap["counts"])
            ok = bk["counts_equal"] and bk["hashes_equal"]
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            bk["error"] = (getattr(exc, "stderr", b"") or str(exc).encode())[:400].decode("utf-8", "replace")
            ok = False
        finally:
            dump.unlink(missing_ok=True)
        checks["12_backup_and_restore"] = {"passed": ok, "evidence": bk}

        # (13) application startup on the target
        copier.pg.close()                                  # the application opens its own connections
        smoke = startup_smoke(target, tgt_snap, copier)
        copier.src.close()
        if hasattr(LP, "close_pools"):
            LP.close_pools(target)
        checks["13_application_startup_on_target"] = {"passed": smoke.get("passed") is True, "evidence": smoke}
    finally:
        if not keep_databases:
            for name in (tname, rname):
                try:
                    _drop_db(target_url, name)
                except Exception:  # noqa: BLE001
                    pass
    ordered = dict(sorted(checks.items()))
    result["checks"] = {k: v["passed"] for k, v in ordered.items()}
    result["evidence"] = {k: v["evidence"] for k, v in ordered.items()}
    result["detail"] = detail
    if source is not None:
        result["source"]["sha256_after"] = sha256_file(Path(source))
        result["checks"]["source_untouched"] = result["source"]["sha256_after"] == result["source"]["sha256_before"]
    result["passed"] = len(ordered) == 13 and all(result["checks"].values())
    result["outcome"] = "PASSED" if result["passed"] else "FAILED"
    result["finished_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return result


def main_legacy(a) -> int:
    if not a.target_url:
        print("--legacy needs --target-url postgresql://user@host:port/<maintenance db>")
        return 1
    try:
        result = run_legacy(Path(a.workdir), target_url=a.target_url, source=Path(a.source) if a.source else None,
                            owner_authorization=a.owner_authorization, rows=a.rows, batch_size=a.batch_size,
                            keep_databases=a.keep_databases)
    except LegacySourceRefused as exc:
        print(f"REFUSED {exc}")
        return LEGACY_EXIT_REFUSED
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(result, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    for k, v in result.get("checks", {}).items():
        print(f"{'PASS' if v else 'FAIL'}  {k}")
    print("LEGACY DRILL", result.get("outcome"))
    return 0 if result.get("passed") else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--source")
    ap.add_argument("--rows", type=int, default=None)
    ap.add_argument("--postgres-url")
    ap.add_argument("--legacy", action="store_true", help="migrate the legacy SQLite schema to PostgreSQL (EXT-DB-01)")
    ap.add_argument("--target-url", help="--legacy: PostgreSQL server (maintenance database URL) for throwaway DBs")
    ap.add_argument("--owner-authorization", help="--legacy: reference to the owner's written authorization; "
                                                  "required to read a --source in an operator location")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--keep-databases", action="store_true")
    a = ap.parse_args(argv)
    if a.legacy:
        a.rows = 5 if a.rows is None else a.rows
        return main_legacy(a)
    a.rows = 25 if a.rows is None else a.rows
    result = run(Path(a.workdir), source=Path(a.source) if a.source else None, rows=a.rows,
                 postgres_url=a.postgres_url)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(result, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    for k, v in result["checks"].items():
        print(f"{'PASS' if v else 'FAIL'}  {k}")
    print("DRILL", "PASSED" if result["passed"] else "FAILED")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
