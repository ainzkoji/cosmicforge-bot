#!/usr/bin/env python3
"""Read-only duplicate check for the prepared additive unique indexes (master plan Section K2).

    python scripts/check_unique_index_candidates.py --sqlite <file> [--out report.json]
    python scripts/check_unique_index_candidates.py --postgres-url postgresql://user@host:port/<db> [--out report.json]
        [--owner-authorization <reference>]

K2 lists four unique indexes that would turn an application-level idempotency rule into a schema rule: deploy
request, consent request, one active operator grant, job attempt. They are prepared, NOT applied: an existing
database must first be checked for rows that already break them, or ``CREATE UNIQUE INDEX`` fails half-way through a
deploy. This script is that check. It:

* opens the database READ-ONLY (SQLite ``mode=ro`` URI; PostgreSQL ``default_transaction_read_only``) and never
  executes DDL or DML;
* reports, per candidate: whether the table and an equivalent index exist, the rows in scope, the duplicate groups
  and rows, and a verdict (``READY``, ``DUPLICATES_FOUND``, ``ALREADY_PRESENT``, ``TABLE_ABSENT``);
* prints the exact DDL that a later, owner-approved migration would run (the same text on both dialects: partial
  unique indexes exist on SQLite >= 3.8 and on PostgreSQL);
* never prints a key: a duplicate group is identified by a short SHA-256 of its key, so a report on a copy of real
  data carries no user ids or request ids.

Like ``migration_drill.py``, it refuses an operator database -- a file inside the repository or under ``data``,
``backups`` or ``logs``, an operator database file name, this environment's ``DATABASE_URL``, or a PostgreSQL
database not named ``cftest_*`` -- unless ``--owner-authorization <reference>`` is given. Exit status: 0 when every
present candidate is ``READY`` or ``ALREADY_PRESENT``, 1 when duplicates exist, 2 when the source is refused.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

EXIT_OK, EXIT_DUPLICATES, EXIT_REFUSED = 0, 1, 2


@dataclass(frozen=True)
class Candidate:
    name: str
    table: str
    columns: Sequence[str]
    where: Optional[str]
    rule: str
    note: str = ""

    def ddl(self) -> str:
        cols = ", ".join(self.columns)
        tail = f" WHERE {self.where}" if self.where else ""
        return f"CREATE UNIQUE INDEX IF NOT EXISTS {self.name} ON {self.table}({cols}){tail}"


#: The four K2 candidates. Keys follow how the code already looks rows up, so the index enforces exactly the rule
#: the code relies on (``deployment_service._existing_for_request`` reads by user and deploy request id; the job
#: queue numbers attempts per job; ``operator_grants.active_grant`` reads the user's unrevoked grant).
CANDIDATES = (
    Candidate("uq_bot_instances_user_deploy_request", "bot_instances", ("user_id", "deploy_request_id"),
              "deploy_request_id IS NOT NULL",
              "one bot instance per deploy request of a user (deployment idempotency)"),
    Candidate("uq_deployment_consents_user_request", "deployment_consents", ("user_id", "request_id"), None,
              "one consent record per deploy request of a user"),
    Candidate("uq_billing_operator_grants_user_unrevoked", "billing_operator_grants", ("user_id",),
              "revoked_at IS NULL",
              "at most one unrevoked operator grant per user",
              "An expired grant is not revoked (operator_grants.active_grant ignores it by time), so a user whose "
              "grant expired and who was granted again has two unrevoked rows. Those groups are counted separately "
              "as expired_unrevoked: they are legitimate history, and the index as written would refuse them. "
              "Applying it needs expiry to revoke, or a different key, which is an owner decision."),
    Candidate("uq_platform_job_attempts_job_attempt", "platform_job_attempts", ("job_id", "attempt"), None,
              "one row per attempt number of a durable job"),
)


class SourceRefused(RuntimeError):
    pass


# ── connections (read-only) ─────────────────────────────────────────────────────────────────────────────────
class _Reader:
    dialect = ""

    def rows(self, sql: str, params: Sequence[Any] = ()) -> List[tuple]:
        raise NotImplementedError

    def table_exists(self, table: str) -> bool:
        raise NotImplementedError

    def columns(self, table: str) -> List[str]:
        raise NotImplementedError

    def unique_indexes(self, table: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class _SQLiteReader(_Reader):
    dialect = "sqlite"

    def __init__(self, path: Path) -> None:
        if not Path(path).is_file():
            raise FileNotFoundError(str(path))
        self.conn = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)

    def rows(self, sql, params=()):
        return list(self.conn.execute(sql, tuple(params)).fetchall())

    def table_exists(self, table):
        return bool(self.rows("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)))

    def columns(self, table):
        return [r[1] for r in self.rows(f'PRAGMA table_info("{table}")')]

    def unique_indexes(self, table):
        out = []
        for _, name, unique, *_rest in self.rows(f'PRAGMA index_list("{table}")'):
            if not unique:
                continue
            cols = [r[2] for r in self.rows(f'PRAGMA index_info("{name}")')]
            sql = self.rows("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,))
            out.append({"name": name, "columns": cols, "partial": bool(sql and sql[0][0] and " WHERE " in sql[0][0].upper())})
        return out

    def close(self):
        self.conn.close()


class _PostgresReader(_Reader):
    dialect = "postgresql"

    def __init__(self, url: str) -> None:
        import psycopg
        self.conn = psycopg.connect(url, autocommit=True)
        self.conn.execute("SET default_transaction_read_only = on")

    def rows(self, sql, params=()):
        return list(self.conn.execute(sql.replace("?", "%s"), tuple(params)).fetchall())

    def table_exists(self, table):
        return bool(self.rows("SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() "
                              "AND table_name = ?", (table,)))

    def columns(self, table):
        return [r[0] for r in self.rows("SELECT column_name FROM information_schema.columns WHERE "
                                        "table_schema = current_schema() AND table_name = ? ORDER BY ordinal_position",
                                        (table,))]

    def unique_indexes(self, table):
        out = []
        for name, cols, partial in self.rows(
                "SELECT i.relname, array_agg(a.attname ORDER BY k.ord), x.indpred IS NOT NULL "
                "FROM pg_index x JOIN pg_class t ON t.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid "
                "JOIN pg_namespace n ON n.oid = t.relnamespace "
                "CROSS JOIN LATERAL unnest(x.indkey) WITH ORDINALITY AS k(attnum, ord) "
                "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum "
                "WHERE x.indisunique AND t.relname = ? AND n.nspname = current_schema() "
                "GROUP BY i.relname, x.indpred", (table,)):
            out.append({"name": name, "columns": list(cols), "partial": bool(partial)})
        return out

    def close(self):
        self.conn.close()


# ── refusal (same rules as migration_drill.py) ──────────────────────────────────────────────────────────────
def sqlite_refusal(path: Path) -> Optional[str]:
    from migration_drill import legacy_source_refusal
    return legacy_source_refusal(Path(path))


def postgres_refusal(url: str) -> Optional[str]:
    database = url.rpartition("/")[2].split("?")[0]
    if os.environ.get("DATABASE_URL", "").strip() == url.strip():
        return "it is the DATABASE_URL of this environment"
    if not database.startswith("cftest_"):
        return f"database {database!r} is not a throwaway cftest_* database"
    return None


# ── the check ───────────────────────────────────────────────────────────────────────────────────────────────
def _key_digest(key: Sequence[Any]) -> str:
    return hashlib.sha256(json.dumps([str(k) for k in key]).encode()).hexdigest()[:12]


def check_candidate(reader: _Reader, c: Candidate, *, now_iso: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"index": c.name, "table": c.table, "columns": list(c.columns), "where": c.where,
                           "rule": c.rule, "ddl": c.ddl()}
    if c.note:
        out["note"] = c.note
    if not reader.table_exists(c.table):
        out.update(verdict="TABLE_ABSENT", rows_in_scope=0, duplicate_groups=0, duplicate_rows=0)
        return out
    missing = [col for col in c.columns if col not in reader.columns(c.table)]
    if missing:
        out.update(verdict="COLUMNS_ABSENT", missing_columns=missing)
        return out
    present = [i for i in reader.unique_indexes(c.table)
               if [x.lower() for x in i["columns"]] == [x.lower() for x in c.columns]
               and i["partial"] == bool(c.where)]
    cols = ", ".join(c.columns)
    where = f" WHERE {c.where}" if c.where else ""
    out["rows_in_scope"] = reader.rows(f"SELECT COUNT(*) FROM {c.table}{where}")[0][0]
    groups = reader.rows(f"SELECT {cols}, COUNT(*) FROM {c.table}{where} GROUP BY {cols} HAVING COUNT(*) > 1 "
                         f"ORDER BY COUNT(*) DESC, {cols}")
    out["duplicate_groups"] = len(groups)
    out["duplicate_rows"] = sum(int(g[-1]) for g in groups)
    out["duplicate_group_digests"] = [{"key_sha256_12": _key_digest(g[:-1]), "rows": int(g[-1])} for g in groups[:20]]
    if c.table == "billing_operator_grants" and groups:
        # Split the groups: those that are only there because an expired grant was never revoked.
        expired_only = 0
        for g in groups:
            live = reader.rows("SELECT COUNT(*) FROM billing_operator_grants WHERE user_id = ? AND revoked_at IS NULL "
                               "AND expires_at > ?", (g[0], now_iso))[0][0]
            if live <= 1:
                expired_only += 1
        out["expired_unrevoked_groups"] = expired_only
        out["live_duplicate_groups"] = len(groups) - expired_only
    if present:
        out["existing_index"] = [i["name"] for i in present]
        out["verdict"] = "ALREADY_PRESENT"
    else:
        out["verdict"] = "DUPLICATES_FOUND" if groups else "READY"
    return out


def run_check(*, sqlite_path: Optional[Path] = None, postgres_url: Optional[str] = None,
              owner_authorization: Optional[str] = None, now_iso: Optional[str] = None) -> Dict[str, Any]:
    if (sqlite_path is None) == (postgres_url is None):
        raise ValueError("give exactly one of --sqlite or --postgres-url")
    reason = sqlite_refusal(sqlite_path) if sqlite_path is not None else postgres_refusal(postgres_url)
    if reason and not owner_authorization:
        raise SourceRefused(f"source refused: {reason}. Pass --owner-authorization <reference> only with the "
                            "owner's written authorization to read that database.")
    now_iso = now_iso or time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
    reader = _SQLiteReader(sqlite_path) if sqlite_path is not None else _PostgresReader(postgres_url)
    try:
        results = [check_candidate(reader, c, now_iso=now_iso) for c in CANDIDATES]
        dialect = reader.dialect
    finally:
        reader.close()
    return {
        "schema": "cosmicforge-k2-unique-index-check-v1", "dialect": dialect, "checked_at_utc": now_iso,
        "read_only": True, "ddl_executed": False,
        "refusal_overridden_by_owner_authorization": bool(reason), "owner_authorization": owner_authorization,
        "candidates": results,
        "duplicates_found": any(r.get("verdict") == "DUPLICATES_FOUND" for r in results),
        "limits": ("A read-only check. It applies nothing; the indexes are applied only by an owner-approved "
                   "migration after this check is clean on the database in question."),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sqlite")
    ap.add_argument("--postgres-url")
    ap.add_argument("--owner-authorization")
    ap.add_argument("--out")
    a = ap.parse_args(argv)
    try:
        report = run_check(sqlite_path=Path(a.sqlite) if a.sqlite else None, postgres_url=a.postgres_url,
                           owner_authorization=a.owner_authorization)
    except SourceRefused as exc:
        print(f"REFUSED {exc}")
        return EXIT_REFUSED
    text = json.dumps(report, indent=1, sort_keys=True)
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text(text + "\n", encoding="utf-8")
    for r in report["candidates"]:
        print(f"{r['verdict']:<17} {r['index']}  rows={r.get('rows_in_scope', 0)} "
              f"duplicate_groups={r.get('duplicate_groups', 0)}")
    return EXIT_DUPLICATES if report["duplicates_found"] else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
