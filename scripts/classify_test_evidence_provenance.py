#!/usr/bin/env python3
"""Classify known test evidence in the canonical database -- provenance only.

The live-paper audit of 2026-09-10 found test-suite rows in the canonical paper
database: 9,339 in ``canonical_trade_decisions`` and 9,339 in
``decision_traces``, written by ``tests/test_replay_production_parity.py``
across eleven full-suite runs (849 of them while the live runtime was running).
Neither table had a provenance column, so the rows were indistinguishable from
organic evidence except by bot name.

This script identifies ONLY unambiguous, known test families and, on an explicit
``--apply``, sets ``provenance='TEST_FIXTURE'`` on rows whose provenance is
still NULL. Nothing else is written:

* no row is deleted;
* no economic or value field is modified -- the UPDATE sets one column;
* rows that already carry a provenance are left alone;
* a family whose bot id exists in ``bot_instances`` (i.e. could be a real bot)
  is reported with LOW confidence and never modified.

It runs as a DRY RUN by default and opens the database read-only unless
``--apply`` is given. ``--apply`` requires the ``provenance`` column, which the
runtime migration adds on its next start; the script never alters the schema.

Usage::

    python scripts/classify_test_evidence_provenance.py            # dry run
    python scripts/classify_test_evidence_provenance.py --json     # dry run, JSON
    python scripts/classify_test_evidence_provenance.py --apply    # label rows
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
BOT_BACKEND = REPO_ROOT / "backends" / "bot-backend"

TEST_PROVENANCE = "TEST_FIXTURE"

#: (family, match kind, pattern, where it comes from)
KNOWN_TEST_FAMILIES: tuple[tuple[str, str, str, str], ...] = (
    ("bot_replay_*", "LIKE", "bot_replay\\_%", "tests/test_replay_production_parity.py"),
    ("bot_determinism_*", "LIKE", "bot_determinism\\_%", "tests/test_replay_production_parity.py"),
    ("bot_sensitivity", "EQ", "bot_sensitivity", "tests/test_replay_production_parity.py"),
    ("test-bot", "EQ", "test-bot", "unit-test fixture bot id"),
)

#: Tables whose provenance column this script may set, and their timestamp column.
LABELLABLE_TABLES: dict[str, str] = {
    "canonical_trade_decisions": "decision_timestamp",
    "decision_traces": "ts",
}

#: Tables with a bot id but no provenance column: counted and reported, never touched.
REPORT_ONLY_TABLES: dict[str, str] = {
    "bot_daily_state": "last_updated_at",
}


def resolve_database(explicit: str | None) -> Path:
    """The canonical database: --db, else the DATABASE_URL in bot-backend/.env."""
    if explicit:
        return Path(explicit).resolve()
    env_file = BOT_BACKEND / ".env"
    url = None
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            key, sep, value = line.strip().partition("=")
            if sep and key.strip() == "DATABASE_URL":
                url = value.strip().strip('"').strip("'")
    if not url or not url.startswith("sqlite:///"):
        raise SystemExit("DATABASE_URL not found in bot-backend/.env; pass --db")
    rel = url[len("sqlite:///"):]
    return Path(rel if os.path.isabs(rel) else BOT_BACKEND / rel).resolve()


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _family_clause(kind: str, pattern: str) -> tuple[str, tuple[Any, ...]]:
    if kind == "LIKE":
        return "bot_instance_id LIKE ? ESCAPE '\\'", (pattern,)
    return "bot_instance_id = ?", (pattern,)


def _known_bot(conn: sqlite3.Connection, kind: str, pattern: str) -> bool:
    """True if any real bot row matches the family -- then it is not unambiguous."""
    if "bot_instances" not in {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}:
        return False
    clause, params = _family_clause(kind, pattern)
    clause = clause.replace("bot_instance_id", "id")
    return bool(conn.execute(f"SELECT 1 FROM bot_instances WHERE {clause} LIMIT 1", params).fetchone())


def scan(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """One report row per (table, family). Read-only."""
    existing = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    report: list[dict[str, Any]] = []
    for table, ts_col in {**LABELLABLE_TABLES, **REPORT_ONLY_TABLES}.items():
        if table not in existing:
            continue
        columns = _columns(conn, table)
        has_provenance = "provenance" in columns
        has_run_id = "run_id" in columns
        for family, kind, pattern, origin in KNOWN_TEST_FAMILIES:
            clause, params = _family_clause(kind, pattern)
            row = conn.execute(
                f"SELECT COUNT(*), MIN({ts_col}), MAX({ts_col}) FROM {table} WHERE {clause}",
                params,
            ).fetchone()
            count = int(row[0] or 0)
            if not count:
                continue
            run_ids: list[str] = []
            if has_run_id:
                run_ids = [r[0] for r in conn.execute(
                    f"SELECT DISTINCT run_id FROM {table} WHERE {clause} ORDER BY run_id",
                    params,
                ).fetchall() if r[0]]
            unlabelled = count
            if has_provenance:
                unlabelled = int(conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {clause} AND provenance IS NULL",
                    params,
                ).fetchone()[0])
            known_bot = _known_bot(conn, kind, pattern)
            report.append({
                "table": table,
                "family": family,
                "origin": origin,
                "row_count": count,
                "first_timestamp": row[1],
                "last_timestamp": row[2],
                "run_id_count": len(run_ids),
                "run_ids": run_ids,
                "provenance_column": has_provenance,
                "unlabelled_rows": unlabelled,
                "labellable": table in LABELLABLE_TABLES,
                "classification_confidence": (
                    "LOW: a bot_instances row matches this family" if known_bot
                    else "HIGH: known test family, no matching bot_instances row"
                ),
                "will_apply": bool(
                    table in LABELLABLE_TABLES and has_provenance and not known_bot and unlabelled
                ),
            })
    return report


def apply(conn: sqlite3.Connection, report: list[dict[str, Any]]) -> dict[str, int]:
    """Label the HIGH-confidence rows. One transaction; provenance column only."""
    changed: dict[str, int] = {}
    with conn:
        for item in report:
            if not item["will_apply"]:
                continue
            kind, pattern = next(
                (k, p) for f, k, p, _ in KNOWN_TEST_FAMILIES if f == item["family"]
            )
            clause, params = _family_clause(kind, pattern)
            cursor = conn.execute(
                f"UPDATE {item['table']} SET provenance = ? "
                f"WHERE {clause} AND provenance IS NULL",
                (TEST_PROVENANCE, *params),
            )
            changed[f"{item['table']}::{item['family']}"] = int(cursor.rowcount or 0)
    return changed


def _print_human(db: Path, report: list[dict[str, Any]], mode: str) -> None:
    print(f"database : {db}")
    print(f"mode     : {mode}")
    print(f"label    : provenance='{TEST_PROVENANCE}' (only where provenance IS NULL)")
    print()
    if not report:
        print("no known test-family rows found")
        return
    for item in report:
        print(
            f"{item['table']:<27} {item['family']:<18} rows={item['row_count']:<6} "
            f"unlabelled={item['unlabelled_rows']:<6} runs={item['run_id_count']:<4} "
            f"{item['first_timestamp']} -> {item['last_timestamp']}"
        )
        print(f"    confidence={item['classification_confidence']}  "
              f"provenance_column={item['provenance_column']}  will_apply={item['will_apply']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", help="database path (default: bot-backend/.env DATABASE_URL)")
    parser.add_argument("--apply", action="store_true",
                        help="label HIGH-confidence rows; without it nothing is written")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)

    db = resolve_database(args.db)
    if not db.exists():
        raise SystemExit(f"database not found: {db}")

    if not args.apply:
        conn = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
        try:
            report = scan(conn)
        finally:
            conn.close()
        if args.json:
            print(json.dumps({"mode": "DRY_RUN", "database": str(db), "report": report}, indent=2))
        else:
            _print_human(db, report, "DRY RUN (read-only; nothing written)")
        return 0

    conn = sqlite3.connect(str(db), timeout=30)
    try:
        report = scan(conn)
        missing = [i["table"] for i in report if i["labellable"] and not i["provenance_column"]]
        if missing:
            raise SystemExit(
                "provenance column missing on " + ", ".join(sorted(set(missing))) +
                "; it is added by the runtime migration on the next start. Nothing written."
            )
        changed = apply(conn, report)
        after = scan(conn)
    finally:
        conn.close()
    if args.json:
        print(json.dumps({"mode": "APPLY", "database": str(db), "changed": changed,
                          "report_after": after}, indent=2))
    else:
        _print_human(db, after, "APPLY")
        print()
        for key, value in changed.items():
            print(f"labelled {value} row(s): {key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
