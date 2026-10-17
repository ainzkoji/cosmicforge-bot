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
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--source")
    ap.add_argument("--rows", type=int, default=25)
    ap.add_argument("--postgres-url")
    a = ap.parse_args(argv)
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
