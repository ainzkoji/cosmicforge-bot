#!/usr/bin/env python3
"""Inventory of the legacy SQLite persistence (Section H Step 5.1, EXT-DB-01).

    python scripts/legacy_persistence_inventory.py [--out evidence/step5/legacy_persistence_inventory.json]
                                                   [--summary evidence/step5/legacy_persistence_inventory.md]

Builds a FRESH legacy schema in a temporary SQLite file by running the real creation path -- ``DB()`` init, the
legacy ``migrate()`` (with every ``ensure_*`` it calls), ``production_schema.ensure``, the runtime-ownership lease
table and every table the runtime creates on first use (``FIRST_USE_CREATORS``) -- and records, without any row
data and without secrets:

* per table: columns (name, declared type, notnull, default, pk position), primary key, foreign keys, unique
  constraints, indexes (unique / partial / expression), triggers, CHECK count, AUTOINCREMENT / WITHOUT ROWID;
* every REAL column with its monetary classification (``shared_lib.persistence.legacy_money``);
* date/time representation and JSON-serialized columns (by declared type, name and default);
* reservation / execution-state tables;
* the legacy migration history (``migrate()``'s numbered steps, ``ensure_*`` calls, ``ADD COLUMN`` upgrades);
* a static scan of non-test source code: direct ``sqlite3`` importers, per-file counts of SQLite dialect
  constructs, transaction-boundary sites and background-maintenance dependencies (VACUUM, checkpoints, backup API);
* the difference to the schema ``scripts/restore_drill.py`` (and ``migration_drill.py``) builds.

The output is deterministic (sorted keys, no timestamps): the same tree gives the same file. The operator's
databases are never opened; everything is built in a temporary directory.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
BACKENDS = ROOT / "backends"
for _p in (BACKENDS / "shared", BACKENDS / "bot-backend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

SCHEMA = "cosmicforge-legacy-persistence-inventory-v1"
DEFAULT_OUT = ROOT / "evidence" / "step5" / "legacy_persistence_inventory.json"
DEFAULT_SUMMARY = ROOT / "evidence" / "step5" / "legacy_persistence_inventory.md"

#: Tables of the dialect-neutral platform layer (``shared_lib.persistence.sql``): already PostgreSQL-ready,
#: migrated by their own versioned migrations, and therefore NOT part of the legacy schema.
PLATFORM_TABLE = re.compile(r"^(platform_|ops_|track_record_|golive_events$|incident_records$|beta_reviews$|"
                            r"drill_venue_orders$)")
#: Transient tables that exist only inside one migration step (a rebuild's copy target).
TRANSIENT_TABLES = frozenset({"news_narratives_new"})

RESERVATION_OR_EXECUTION_STATE = re.compile(
    r"(reservation|intent|attempt|execution|pending_entr|protection|order|position|fill|closes|production_state|"
    r"runtime_ownership|lease|brake_state|daily_risk|period_risk|rate_budget)")


# ═══════════════════════════════════════════════════════════════════════════════════════ schema build
# The first-use creators live in ``shared_lib.persistence.legacy_first_use`` so that this inventory and
# ``legacy_postgres.create_schema`` (the PostgreSQL schema) run the one same list.
from shared_lib.persistence.legacy_first_use import (  # noqa: E402
    USER_SQL_MIGRATION_ERRORS,
    ddl_from_source as _ddl_from_source,
    first_use_creators as _first_use_creators,
    function_from_source as _function_from_source,
)


def _table_names(path: Path) -> List[str]:
    conn = sqlite3.connect(str(path))
    try:
        return [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                           "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    finally:
        conn.close()


def build_legacy_schema(path: Path, *, first_use: bool = True) -> List[Dict[str, Any]]:
    """Create the complete legacy schema in the (new) SQLite file ``path`` through the real creation path.
    Returns the steps in order with the tables each one added. The caller must have pointed ``DATABASE_URL``
    away from any operator database (``isolated_environment``)."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from app.execution import production_schema
    from app.ops.runtime_ownership import ensure_ownership_schema

    steps: List[Dict[str, Any]] = []
    before: set = set()

    def columns() -> set:
        conn = sqlite3.connect(str(path))
        try:
            return {(t, r[1]) for t in _table_names(path) for r in conn.execute(f'PRAGMA table_info("{t}")')}
        finally:
            conn.close()

    cols_before: set = set()

    def record(name: str, method: str) -> None:
        nonlocal before, cols_before
        now, cols_now = set(_table_names(path)), columns()
        added_cols = sorted(f"{t}.{c}" for t, c in cols_now - cols_before if t in before)
        steps.append({"step": name, "method": method, "tables_added": sorted(now - before),
                      "columns_added_to_existing_tables": added_cols})
        before, cols_before = now, cols_now

    db = DB(path=str(path))
    record("shared_lib.persistence.db.DB._init", "FUNCTION")
    migrate(db)
    record("shared_lib.persistence.migrations.migrate", "FUNCTION")
    production_schema.ensure(db, force=True)
    record("app.execution.production_schema.ensure", "FUNCTION")
    ensure_ownership_schema(db)
    record("app.ops.runtime_ownership.ensure_ownership_schema", "FUNCTION")
    if first_use:
        for name, method, creator in _first_use_creators():
            with db.connect() as conn:
                creator(db, conn)
            record(name, method)
    return steps


def isolated_environment(db_path: Path) -> None:
    """Pin every database resolution to ``db_path`` before any application import (``DB()`` without a path
    resolves ``DATABASE_URL``; several modules open one at import)."""
    os.environ["DATABASE_URL"] = f"sqlite:///{Path(db_path).as_posix()}"
    os.environ["COSMICFORGE_TEST_MODE"] = "1"
    os.environ.setdefault("APP_ENV", "TEST")


def legacy_tables(conn: sqlite3.Connection) -> List[str]:
    return [t for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                                       "ORDER BY name")
            if not PLATFORM_TABLE.match(t) and t not in TRANSIENT_TABLES]


NOT_STATE = re.compile(r"^(login_|pricing_)")


# ═══════════════════════════════════════════════════════════════════════════════════════ schema facts
def _norm_sql(sql: Optional[str]) -> Optional[str]:
    return " ".join(sql.split()) if sql else sql


TIME_NAME = re.compile(r"(^|_)(at|time|timestamp|ts|date|day|ms|until|since|expires|expiry|deadline|created|updated|"
                       r"opened|closed|start|end|month|week)(_ms|_utc|_s)?$", re.I)
NOT_TIME = re.compile(r"(duration|latency|elapsed|age_|_age|interval|ttl|timeout|window|lookback|minutes|seconds|"
                      r"_sec$|count|days$|hours$|bars$|_r$)", re.I)
JSON_NAME = re.compile(r"(_json$|^json$|^document$|^payload$|^response$|^request$|^metadata$|^details?$|^context$|"
                       r"^snapshot$|^evidence$|^extra$|^tags$|^attributes$|^body$|^raw$|^config$)", re.I)


def time_representation(name: str, declared: str, default: Optional[str]) -> Optional[Dict[str, Any]]:
    if not TIME_NAME.search(name) or NOT_TIME.search(name):
        return None
    t = (declared or "").upper()
    d = (default or "").upper()
    if "INT" in t:
        unit = "ms" if re.search(r"_ms$|_at_ms$|^ms$", name, re.I) else "UNSTATED_BY_NAME"
        rep = "EPOCH_INTEGER"
    elif any(k in t for k in ("REAL", "FLOA", "DOUB")):
        rep, unit = "EPOCH_OR_JULIAN_REAL", "UNSTATED_BY_NAME"
    elif "TEXT" in t or "CHAR" in t or "DATE" in t or "TIME" in t:
        rep = "DATE_TEXT" if re.search(r"(^|_)(date|day|month|week)$", name, re.I) else "ISO_TEXT"
        unit = None
    else:
        rep, unit = "UNTYPED", None
    out: Dict[str, Any] = {"representation": rep}
    if unit:
        out["unit"] = unit
    if "CURRENT_TIMESTAMP" in d or "DATETIME(" in d.replace(" ", "") or "STRFTIME(" in d.replace(" ", ""):
        out["sqlite_default"] = default
    return out


def table_facts(conn: sqlite3.Connection, table: str) -> Dict[str, Any]:
    from shared_lib.persistence import legacy_money as LM
    sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0] or ""
    cols = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    columns = [{"name": c[1], "type": c[2] or "", "notnull": bool(c[3]), "default": c[4], "pk": c[5]} for c in cols]
    pk = [c[1] for c in sorted((c for c in cols if c[5]), key=lambda c: c[5])]
    fks: Dict[int, Dict[str, Any]] = {}
    for fid, seq, ref, frm, to, on_update, on_delete, match in conn.execute(f'PRAGMA foreign_key_list("{table}")'):
        fk = fks.setdefault(fid, {"references": ref, "from": [], "to": [], "on_update": on_update,
                                  "on_delete": on_delete})
        fk["from"].append(frm)
        fk["to"].append(to)
    indexes, uniques = [], []
    for _, name, unique, origin, partial in conn.execute(f'PRAGMA index_list("{table}")'):
        info = conn.execute(f'PRAGMA index_xinfo("{name}")').fetchall()
        keys = [(r[2] if r[2] is not None else ("<expr>" if r[1] == -2 else "<rowid>")) for r in info if r[5]]
        isql = conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)).fetchone()
        entry = {"name": name, "unique": bool(unique), "origin": {"c": "CREATE_INDEX", "u": "UNIQUE_CONSTRAINT",
                                                                    "pk": "PRIMARY_KEY"}.get(origin, origin),
                 "partial": bool(partial), "columns": keys}
        if isql and isql[0]:
            entry["sql"] = _norm_sql(isql[0])
        indexes.append(entry)
        if unique:
            uniques.append({"name": name, "columns": keys, "origin": entry["origin"], "partial": bool(partial)})
    triggers = []
    for name, tsql in conn.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? "
                                   "ORDER BY name", (table,)):
        m = re.search(r"\b(BEFORE|AFTER|INSTEAD\s+OF)\s+(INSERT|UPDATE|DELETE)", tsql or "", re.I)
        triggers.append({"name": name, "timing": m.group(1).upper() if m else None,
                         "event": m.group(2).upper() if m else None,
                         "raises": bool(re.search(r"RAISE\s*\(", tsql or "", re.I)),
                         "sql_sha256": hashlib.sha256((_norm_sql(tsql) or "").encode()).hexdigest()})
    real = {}
    for c in columns:
        if LM.is_real_type(c["type"]):
            try:
                real[c["name"]] = LM.classify_real_column(table, c["name"])
            except LM.UnclassifiedRealColumn:
                real[c["name"]] = "UNCLASSIFIED"
    times = {c["name"]: rep for c in columns if (rep := time_representation(c["name"], c["type"], c["default"]))}
    json_cols = sorted(c["name"] for c in columns if (c["type"] or "").upper() == "JSON" or JSON_NAME.search(c["name"]))
    return {
        "columns": columns,
        "primary_key": pk,
        "rowid_alias_autoincrement": bool(re.search(r"\bAUTOINCREMENT\b", sql, re.I)),
        "without_rowid": bool(re.search(r"\bWITHOUT\s+ROWID\b", sql, re.I)),
        "check_constraints": len(re.findall(r"\bCHECK\s*\(", sql, re.I)),
        "foreign_keys": [fks[k] for k in sorted(fks)],
        "unique_constraints": uniques,
        "indexes": indexes,
        "triggers": triggers,
        "real_columns": real,
        "time_columns": times,
        "json_columns": json_cols,
        "create_sql_sha256": hashlib.sha256(_norm_sql(sql).encode()).hexdigest(),
    }


# ═══════════════════════════════════════════════════════════════════════════════════════ migration history
STEP_COMMENT = re.compile(r"^\s*#\s*(\d+[a-z]?(?:-[a-z]+)?)\)\s*(.+?)\s*$")


def migration_history() -> Dict[str, Any]:
    """The legacy ``migrate()`` is not versioned: it is one idempotent function re-run on every start. Its
    history is the ordered list of numbered steps (source comments), the ``ensure_*`` schema modules it calls and
    the ``ADD COLUMN`` upgrades it applies."""
    path = BACKENDS / "shared" / "shared_lib" / "persistence" / "migrations.py"
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "migrate")
    lines = text.splitlines()[fn.lineno - 1:fn.end_lineno]
    steps = [{"step": m.group(1), "title": m.group(2)} for line in lines if (m := STEP_COMMENT.match(line))]
    ensure_calls, add_columns, statements = [], [], Counter()
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name and name.startswith("ensure") and name not in ensure_calls:
                ensure_calls.append(name)
            if name == "_add_column_if_missing" and len(node.args) >= 4 and all(
                    isinstance(a, ast.Constant) for a in node.args[1:4]):
                add_columns.append({"table": node.args[1].value, "column": node.args[2].value,
                                    "type": node.args[3].value})
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for verb in ("CREATE TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX", "CREATE TRIGGER", "ALTER TABLE",
                         "DROP TABLE", "DROP INDEX", "DROP TRIGGER", "UPDATE ", "INSERT ", "DELETE FROM"):
                statements[verb.strip()] += len(re.findall(rf"\b{verb}", node.value, re.I))
    return {
        "versioned": False,
        "user_version_used": "user_version" in text,
        "mechanism": ("one idempotent migrate() re-run at every start: CREATE ... IF NOT EXISTS, guarded ALTER TABLE "
                      "ADD COLUMN (PRAGMA table_info), in-place rebuilds and data backfills; no version table"),
        "numbered_steps": steps,
        "ensure_calls_in_order": ensure_calls,
        "add_column_upgrades": len(add_columns),
        "add_column_upgrades_by_table": dict(sorted(Counter(a["table"] for a in add_columns).items())),
        "statement_literals_in_migrate": dict(sorted(statements.items())),
    }


# ═══════════════════════════════════════════════════════════════════════════════════════ source scan
SCAN_ROOTS = ("backends/bot-backend/app", "backends/shared/shared_lib", "backends/user-backend/app",
              "backends/admin-backend/app", "scripts")
SELF = Path(__file__).resolve()

CONSTRUCTS = {
    "insert_or_ignore": r"\bINSERT\s+OR\s+IGNORE\b",
    "insert_or_replace": r"\bINSERT\s+OR\s+REPLACE\b",
    "replace_into": r"(?<!OR )\bREPLACE\s+INTO\b",
    "on_conflict": r"\bON\s+CONFLICT\b",
    "pragma": r"\bPRAGMA\s+\w+",
    "sqlite_master": r"\bsqlite_(?:master|schema|sequence)\b",
    "datetime_now": r"\bdatetime\(\s*['\"]now['\"]",
    "current_timestamp": r"\bCURRENT_TIMESTAMP\b",
    "sql_strftime": r"(?<![.\w])strftime\(\s*'",
    "julianday": r"\bjulianday\(",
    "autoincrement": r"\bAUTOINCREMENT\b",
    "begin_immediate": r"\bBEGIN\s+IMMEDIATE\b",
    "begin_exclusive": r"\bBEGIN\s+EXCLUSIVE\b",
    "lastrowid": r"\.lastrowid\b",
    "last_insert_rowid": r"\blast_insert_rowid\(",
    "json_extract": r"\bjson_extract\(",
    "json_each": r"\bjson_(?:each|tree)\(",
    "group_concat": r"\bgroup_concat\(",
    "ifnull": r"\bIFNULL\(",
    "iif": r"\bIIF\(",
    "rowid": r"\browid\b",
    "executescript": r"\.executescript\(",
    "commit": r"\.commit\(\)",
    "rollback": r"\.rollback\(\)",
    "vacuum": r"\bVACUUM\b",
    "wal_checkpoint": r"\bwal_checkpoint\b",
    "backup_api": r"\.backup\(",
    "integrity_check": r"\b(?:integrity_check|quick_check)\b",
}
_COMPILED = {k: re.compile(v, re.I if k not in ("commit", "rollback", "backup_api", "lastrowid", "executescript")
                           else 0) for k, v in CONSTRUCTS.items()}
SQLITE_IMPORT = re.compile(r"^\s*(?:import\s+sqlite3\b|from\s+sqlite3\s+import\b)", re.M)
SQL_STRFTIME_FORMAT = re.compile(r"(?<![.\w])strftime\(\s*'([^']*)'", re.I)
PRAGMA_NAME = re.compile(r"\bPRAGMA\s+(\w+)", re.I)
TRANSACTION_SITE = ("begin_immediate", "begin_exclusive")
MAINTENANCE_SITE = ("vacuum", "wal_checkpoint", "backup_api", "integrity_check")


def _scan_files() -> List[Path]:
    out = []
    for root in SCAN_ROOTS:
        base = ROOT / root
        for p in sorted(base.rglob("*.py")):
            parts = set(p.relative_to(ROOT).parts)
            if "__pycache__" in parts or "tests" in parts or "test" in parts or p.name.startswith("test_") \
                    or p.name == "conftest.py" or p.resolve() == SELF:
                continue
            out.append(p)
    return out


def source_scan() -> Dict[str, Any]:
    per_file: Dict[str, Dict[str, int]] = {}
    importers, transaction_sites, maintenance_sites = [], [], []
    strftime_formats: Counter = Counter()
    pragmas: Counter = Counter()
    totals: Counter = Counter()
    for p in _scan_files():
        rel = p.relative_to(ROOT).as_posix()
        text = p.read_text(encoding="utf-8", errors="replace")
        if SQLITE_IMPORT.search(text):
            importers.append(rel)
        counts = {}
        for key, rx in _COMPILED.items():
            n = len(rx.findall(text))
            if n:
                counts[key] = n
                totals[key] += n
        if counts and (rel in importers or set(counts) - {"commit", "rollback", "rowid", "backup_api"}):
            per_file[rel] = dict(sorted(counts.items()))
        strftime_formats.update(SQL_STRFTIME_FORMAT.findall(text))
        pragmas.update(m.lower() for m in PRAGMA_NAME.findall(text))
        if any(k in counts for k in TRANSACTION_SITE + MAINTENANCE_SITE):
            for lineno, line in enumerate(text.splitlines(), 1):
                for key in TRANSACTION_SITE:
                    if _COMPILED[key].search(line):
                        transaction_sites.append({"file": rel, "line": lineno, "kind": key})
                for key in MAINTENANCE_SITE:
                    if _COMPILED[key].search(line):
                        maintenance_sites.append({"file": rel, "line": lineno, "kind": key})
    commit_files = {f: c["commit"] for f, c in per_file.items() if "commit" in c}
    return {
        "roots": list(SCAN_ROOTS),
        "excluded": "tests/ and test/ directories, test_*.py, conftest.py, __pycache__, this script",
        "files_scanned": len(_scan_files()),
        "direct_sqlite3_importers": importers,
        "construct_totals": dict(sorted(totals.items())),
        "per_file_constructs": dict(sorted(per_file.items())),
        "sql_strftime_formats": dict(sorted(strftime_formats.items())),
        "pragmas_used": dict(sorted(pragmas.items())),
        "transaction_boundary_sites": transaction_sites,
        "explicit_commit_sites_per_file": dict(sorted(commit_files.items())),
        "background_maintenance_sites": maintenance_sites,
    }


# ═══════════════════════════════════════════════════════════════════════════════════════ inventory
def inventory(workdir: Path) -> Dict[str, Any]:
    from shared_lib.persistence import legacy_money as LM
    workdir.mkdir(parents=True, exist_ok=True)
    full = workdir / "legacy_full.db"
    drill = workdir / "restore_drill_path.db"
    for p in (full, drill):
        for suffix in ("", "-wal", "-shm"):
            Path(str(p) + suffix).unlink(missing_ok=True)
    isolated_environment(full)
    steps = build_legacy_schema(full)
    # The restore / migration drills build DB + migrate() + production_schema.ensure only.
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from app.execution import production_schema
    dd = DB(path=str(drill))
    migrate(dd)
    production_schema.ensure(dd, force=True)
    drill_tables = set(_table_names(drill))

    conn = sqlite3.connect(str(full))
    try:
        tables = legacy_tables(conn)
        facts = {t: table_facts(conn, t) for t in tables}
        all_tables = set(_table_names(full))
    finally:
        conn.close()

    real_cols = {f"{t}.{c}": k for t, f in facts.items() for c, k in f["real_columns"].items()}
    monetary = sorted(k for k, v in real_cols.items() if v == LM.MONETARY)
    unclassified = sorted(k for k, v in real_cols.items() if v == "UNCLASSIFIED")
    stale = sorted(f"{t}.{c}" for m in (LM.MONETARY_COLUMNS, LM.NON_MONETARY_REAL_COLUMNS) for t, cs in m.items()
                   for c in cs if f"{t}.{c}" not in real_cols)
    time_reps = Counter(rep["representation"] for f in facts.values() for rep in f["time_columns"].values())
    first_use = sorted({t for s in steps if s["method"] == "SOURCE_LITERAL" or s["step"] not in (
        "shared_lib.persistence.db.DB._init", "shared_lib.persistence.migrations.migrate",
        "app.execution.production_schema.ensure", "app.ops.runtime_ownership.ensure_ownership_schema")
        for t in s["tables_added"]})
    state_tables = sorted(t for t in tables if RESERVATION_OR_EXECUTION_STATE.search(t) and not NOT_STATE.match(t))
    headline = {
        "tables": len(tables),
        "columns": sum(len(f["columns"]) for f in facts.values()),
        "real_columns": len(real_cols),
        "monetary_columns": len(monetary),
        "non_monetary_real_columns": len(real_cols) - len(monetary) - len(unclassified),
        "unclassified_real_columns": len(unclassified),
        "foreign_keys": sum(len(f["foreign_keys"]) for f in facts.values()),
        "tables_with_foreign_keys": sum(1 for f in facts.values() if f["foreign_keys"]),
        "unique_constraints": sum(len(f["unique_constraints"]) for f in facts.values()),
        "partial_unique_indexes": sum(1 for f in facts.values() for u in f["unique_constraints"] if u["partial"]),
        "indexes": sum(len(f["indexes"]) for f in facts.values()),
        "triggers": sum(len(f["triggers"]) for f in facts.values()),
        "check_constraints": sum(f["check_constraints"] for f in facts.values()),
        "autoincrement_tables": sum(1 for f in facts.values() if f["rowid_alias_autoincrement"]),
        "tables_without_primary_key": sum(1 for f in facts.values() if not f["primary_key"]),
        "json_columns": sum(len(f["json_columns"]) for f in facts.values()),
        "time_columns": sum(len(f["time_columns"]) for f in facts.values()),
        "first_use_tables": len(first_use),
    }
    scan = source_scan()
    headline["direct_sqlite3_modules"] = len(scan["direct_sqlite3_importers"])
    result = {
        "schema": SCHEMA,
        "build": {
            "steps": steps,
            "platform_layer_tables_excluded": sorted(t for t in all_tables if PLATFORM_TABLE.match(t)),
            "transient_tables_excluded": sorted(TRANSIENT_TABLES & all_tables),
            "first_use_tables": first_use,
            "user_backend_sql_migration_errors_on_fresh_schema": dict(sorted(USER_SQL_MIGRATION_ERRORS.items())),
        },
        "headline": headline,
        "restore_drill_comparison": {
            "restore_drill_path": "migration_drill.build_representative: DB() + migrate() + production_schema.ensure "
                                  "(restore_drill.py adds the platform migrations and restore_drill_heartbeat)",
            "legacy_tables_missing_from_restore_drill_build": sorted(set(tables) - drill_tables),
            "restore_drill_tables_not_in_inventory": sorted(t for t in drill_tables - set(tables)
                                                            if not PLATFORM_TABLE.match(t)),
        },
        "money": {
            "rule": LM.__doc__.split("\n\n")[1].strip() if LM.__doc__ else "",
            "postgres_types": {"MONETARY": LM.PG_MONEY_TYPE, "NON_MONETARY": LM.PG_NON_MONEY_TYPE},
            "monetary_columns": monetary,
            "unclassified_real_columns": unclassified,
            "classified_but_absent": stale,
        },
        "time_representation_counts": dict(sorted(time_reps.items())),
        "reservation_and_execution_state_tables": {
            t: {"primary_key": facts[t]["primary_key"],
                "unique": [u["columns"] for u in facts[t]["unique_constraints"]],
                "triggers": len(facts[t]["triggers"]),
                "monetary_columns": sorted(c for c, k in facts[t]["real_columns"].items() if k == LM.MONETARY)}
            for t in state_tables},
        "trades_table_conflicting_definitions": {
            "tables": ["trades"],
            "detail": ("shared_lib.persistence.trade_tracker.TradeTracker and analytics_service.AnalyticsService both "
                       "CREATE TABLE IF NOT EXISTS trades with different NOT NULL constraints (TradeTracker: NOT NULL on "
                       "run_id..entry_confidence and status, plus idx_trades_run). Whichever runs first on a database "
                       "decides the schema; the inventory uses TradeTracker's."),
        },
        "migration_history": migration_history(),
        "source_scan": scan,
        "tables": facts,
    }
    return result


# ═══════════════════════════════════════════════════════════════════════════════════════ summary
def summary_markdown(inv: Dict[str, Any]) -> str:
    h = inv["headline"]
    scan = inv["source_scan"]
    tot = scan["construct_totals"]
    lines = [
        "# Legacy persistence inventory (Step 5.1, EXT-DB-01)",
        "",
        "Generated by `scripts/legacy_persistence_inventory.py` from a fresh schema built through the real creation path",
        "(`DB()` init, legacy `migrate()`, `production_schema.ensure`, the runtime-ownership lease table and every",
        "first-use table). No row data, no secrets. Full detail: `legacy_persistence_inventory.json`.",
        "",
        "## Headline",
        "",
        "| Item | Count |",
        "|---|---:|",
    ]
    for key in ("tables", "columns", "real_columns", "monetary_columns", "non_monetary_real_columns",
                "unclassified_real_columns", "foreign_keys", "tables_with_foreign_keys", "unique_constraints",
                "partial_unique_indexes", "indexes", "triggers", "check_constraints", "autoincrement_tables",
                "tables_without_primary_key", "json_columns", "time_columns", "first_use_tables",
                "direct_sqlite3_modules"):
        lines.append(f"| {key.replace('_', ' ')} | {h[key]} |")
    lines += ["", "## Schema creation path", ""]
    for s in inv["build"]["steps"]:
        lines.append(f"- `{s['step']}` ({s['method']}): {len(s['tables_added'])} tables")
    lines += ["", "The platform-layer tables (`platform_*`, `ops_*`, `track_record_*`, go-live/launch records) are "
                  "excluded: they belong to the dialect-neutral layer and already run on PostgreSQL.", ""]
    cmp_ = inv["restore_drill_comparison"]
    lines += ["## Difference to the restore / migration drill build", "",
              f"The drills build `DB()` + `migrate()` + `production_schema.ensure` only. Legacy tables they miss "
              f"({len(cmp_['legacy_tables_missing_from_restore_drill_build'])}):", "",
              ", ".join(f"`{t}`" for t in cmp_["legacy_tables_missing_from_restore_drill_build"]) or "none", ""]
    lines += ["## Money", "",
              f"{h['monetary_columns']} of {h['real_columns']} REAL columns hold money or exchange quantities/prices "
              f"(-> `{inv['money']['postgres_types']['MONETARY']}`); the rest are ratios, scores, indicators, "
              f"statistics or durations (-> `{inv['money']['postgres_types']['NON_MONETARY']}`). "
              f"Unclassified: {h['unclassified_real_columns']}.", ""]
    by_table: Dict[str, List[str]] = {}
    for col in inv["money"]["monetary_columns"]:
        t, c = col.split(".", 1)
        by_table.setdefault(t, []).append(c)
    state = inv["reservation_and_execution_state_tables"]
    lines += ["Monetary columns in reservation / execution-state tables:", ""]
    for t in sorted(state):
        if state[t]["monetary_columns"]:
            lines.append(f"- `{t}`: {', '.join(state[t]['monetary_columns'])}")
    lines += ["", "## Reservation and execution-state tables", "",
              ", ".join(f"`{t}`" for t in sorted(state)), ""]
    lines += ["## Time and JSON", "",
              "Time columns by representation: " + ", ".join(f"{k} {v}" for k, v in
                                                            inv["time_representation_counts"].items()) + ".",
              f"JSON-serialized columns: {h['json_columns']} (declared JSON, `*_json`, `document`, `payload`, ...).",
              "SQL `strftime` formats in source: " + (", ".join(f"`{k}` x{v}" for k, v in
                                                               scan["sql_strftime_formats"].items()) or "none") + ".",
              ""]
    mh = inv["migration_history"]
    lines += ["## Legacy migration history", "",
              f"Versioned: {mh['versioned']} ({mh['mechanism']}).",
              f"Numbered steps: {len(mh['numbered_steps'])}; `ensure_*` schema modules called: "
              f"{len(mh['ensure_calls_in_order'])}; guarded `ADD COLUMN` upgrades: {mh['add_column_upgrades']}.", ""]
    lines += ["## Source scan (non-test code)", "",
              f"Files scanned: {scan['files_scanned']}; modules importing `sqlite3` directly: "
              f"{len(scan['direct_sqlite3_importers'])}.", "",
              "| Construct | Occurrences |", "|---|---:|"]
    for k, v in tot.items():
        lines.append(f"| {k} | {v} |")
    lines += ["", f"Transaction-boundary sites (BEGIN IMMEDIATE/EXCLUSIVE): {len(scan['transaction_boundary_sites'])}; "
                  f"files with explicit `commit()`: {len(scan['explicit_commit_sites_per_file'])}.",
              f"Background-maintenance sites (VACUUM, wal_checkpoint, backup API, integrity checks): "
              f"{len(scan['background_maintenance_sites'])}:", ""]
    for s in scan["background_maintenance_sites"][:25]:
        lines.append(f"- `{s['file']}:{s['line']}` {s['kind']}")
    if len(scan["background_maintenance_sites"]) > 25:
        lines.append(f"- ... {len(scan['background_maintenance_sites']) - 25} more in the JSON")
    lines += ["", "Direct `sqlite3` importers by area: " + ", ".join(
        f"{area} {n}" for area, n in sorted(Counter(p.split('/')[1] if p.startswith('backends/') else p.split('/')[0]
                                                    for p in scan["direct_sqlite3_importers"]).items())) + ".", ""]
    lazy = [c for st in inv["build"]["steps"][4:] for c in st["columns_added_to_existing_tables"]]
    lines += ["Columns added lazily to existing tables outside `migrate()` (" + str(len(lazy)) + "): "
              + ", ".join(f"`{c}`" for c in lazy) + ".", ""]
    errs = inv["build"]["user_backend_sql_migration_errors_on_fresh_schema"]
    if errs:
        lines += ["User-backend SQL migration files failing on a fresh schema (their runner prints and continues): "
                  + "; ".join(f"`{k}`: {v}" for k, v in errs.items()) + ".", ""]
    lines += ["## Findings", "",
              "- **NUMERIC(38,18) cannot hold every legacy float.** Money is computed in binary floats, and a float "
              "result carries up to 17 significant digits at any magnitude: a fee `notional * 0.0004` such as "
              "`0.0062978382397085135` has 19 fractional digits (about 1.5 % of such fees). NUMERIC(38,18) would round "
              "them at the 18th place. Decision (coordinator): legacy monetary columns use unconstrained `NUMERIC`, which "
              "holds the shortest decimal (`repr`) of every finite binary64 exactly, subnormals included "
              "(`test_legacy_money_postgres.py`); the platform layer keeps NUMERIC(38,18) for exact decimal input. "
              "The migration drill still reports, never rounds, any value a target column with a declared scale cannot "
              "hold.",
              "- PostgreSQL's own `float8 -> numeric` cast rounds to 15 significant digits (`0.30000000000000004` -> "
              "`0.3`): values must be bound as `Decimal(repr(x))`, never cast on the server.",
              "- `trades` has two divergent `CREATE TABLE IF NOT EXISTS` definitions (trade_tracker.py vs "
              "analytics_service.py); the first to run decides NOT NULL constraints.",
              "- The legacy schema has no version table: PostgreSQL needs its own versioned baseline for it.",
              "- Time is stored three ways (ISO text from `isoformat()`, SQLite `CURRENT_TIMESTAMP` text without `T`/zone, "
              "epoch integers); a port must keep each column's representation, not normalise it silently.",
              ""]
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--summary", default=str(DEFAULT_SUMMARY))
    ap.add_argument("--workdir", help="where the temporary schema databases are built (default: a temp dir)")
    a = ap.parse_args(argv)
    if a.workdir:
        result = inventory(Path(a.workdir))
    else:
        with tempfile.TemporaryDirectory(prefix="cf_legacy_inventory_") as tmp:
            result = inventory(Path(tmp))
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    Path(a.summary).write_text(summary_markdown(result), encoding="utf-8")
    h = result["headline"]
    print(json.dumps(h, sort_keys=True))
    if h["unclassified_real_columns"]:
        print("UNCLASSIFIED REAL COLUMNS:", ", ".join(result["money"]["unclassified_real_columns"]))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
