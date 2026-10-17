"""Versioned, checksummed, crash-safe schema migrations for the platform layer (Section H, Step 5.1).

Each migration is an ordered ``Migration(version, name, statements, down=..., irreversible=...)`` with
per-dialect DDL. ``migrate()``:

1. takes the migration lock (PostgreSQL ``pg_advisory_xact_lock``; SQLite ``BEGIN IMMEDIATE``), so two
   migration jobs started at once apply each version exactly once;
2. refuses to run if an applied version's checksum differs from the code (an applied migration was edited --
   the database no longer matches what the code believes it is);
3. applies every pending version **in its own transaction** together with its ``platform_schema_migrations``
   row. Both dialects have transactional DDL, so an interrupted migration leaves either the whole version or
   none of it: a rerun resumes at the first version not recorded.

``rollback(to_version)`` runs ``down`` statements newest-first and refuses an ``irreversible`` version.
``compatibility()`` tells a deploy or rollback script whether a binary built for one schema range can run
against the database as it is (``app_min_schema``/``app_max_schema``), so a rollback cannot start an
incompatible binary against an irreversible schema.
"""
from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from .database import POSTGRESQL, SQLITE, Connection, Database

MIGRATION_TABLE = "platform_schema_migrations"
_ADVISORY_LOCK_KEY = 0x0C05_F0_5E  # stable application-wide key for the migration lock

Statements = Union[Sequence[str], Mapping[str, Sequence[str]]]


class MigrationError(RuntimeError):
    code = "MIGRATION_FAILED"


class ChecksumMismatch(MigrationError):
    code = "MIGRATION_CHECKSUM_MISMATCH"


class IrreversibleMigration(MigrationError):
    code = "MIGRATION_IRREVERSIBLE"


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    up: Statements
    down: Statements = ()
    irreversible: bool = False

    def statements(self, dialect: str, *, direction: str = "up") -> Tuple[str, ...]:
        body = self.up if direction == "up" else self.down
        if isinstance(body, Mapping):
            return tuple(body.get(dialect, ()))
        return tuple(body)

    def checksum(self) -> str:
        h = hashlib.sha256()
        h.update(f"{self.version}:{self.name}:{self.irreversible}".encode())
        for dialect in (SQLITE, POSTGRESQL):
            for s in self.statements(dialect):
                h.update(b"\x00" + dialect.encode() + b"\x00" + " ".join(s.split()).encode())
        return h.hexdigest()


def _ensure_table(conn: Connection) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (version INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        "checksum TEXT NOT NULL, applied_at BIGINT NOT NULL, duration_ms BIGINT NOT NULL, irreversible INTEGER NOT NULL)")


def _lock(conn: Connection) -> None:
    if conn.dialect == POSTGRESQL:
        conn.execute("SELECT pg_advisory_xact_lock(?)", (_ADVISORY_LOCK_KEY,))
    # SQLite: the surrounding BEGIN IMMEDIATE already excludes every other writer.


def _validate(migrations: Sequence[Migration]) -> List[Migration]:
    ordered = sorted(migrations, key=lambda m: m.version)
    seen = set()
    for m in ordered:
        if m.version <= 0 or m.version in seen:
            raise MigrationError(f"MIGRATION_VERSION_INVALID:{m.version}")
        seen.add(m.version)
    return ordered


def applied(db: Database) -> Dict[int, Tuple[str, str, bool]]:
    with db.transaction() as conn:
        _lock(conn)
        _ensure_table(conn)
        return {int(v): (n, c, bool(i)) for v, n, c, i in
                conn.all(f"SELECT version, name, checksum, irreversible FROM {MIGRATION_TABLE}")}


def current_version(db: Database) -> int:
    with db.transaction(write=False) as conn:
        try:
            return int(conn.scalar(f"SELECT COALESCE(MAX(version), 0) FROM {MIGRATION_TABLE}") or 0)
        except Exception:
            return 0


@dataclass
class MigrationReport:
    applied: List[int] = field(default_factory=list)
    skipped: List[int] = field(default_factory=list)
    final_version: int = 0
    durations_ms: Dict[int, float] = field(default_factory=dict)


def migrate(db: Database, migrations: Sequence[Migration], *, target: Optional[int] = None,
            fail_after: Optional[Callable[[int, int], None]] = None) -> MigrationReport:
    """Apply pending migrations up to ``target`` (default: all). ``fail_after(version, statement_index)`` is a
    test hook that raises to simulate a crash in the middle of a version."""
    ordered = _validate(migrations)
    report = MigrationReport()
    for m in ordered:
        if target is not None and m.version > target:
            break
        for attempt in range(_LOCK_RETRIES):
            try:
                _apply_one(db, m, ordered, report, fail_after)
                break
            except sqlite3.OperationalError as exc:
                # Another migration job holds SQLite's write lock beyond the busy timeout. The whole version
                # rolled back; its "already applied?" check runs again inside the retried transaction.
                if "locked" not in str(exc) or attempt == _LOCK_RETRIES - 1:
                    raise
                time.sleep(0.2 * (attempt + 1))
    report.final_version = current_version(db)
    return report


_LOCK_RETRIES = 5


def _apply_one(db: Database, m: Migration, ordered, report: "MigrationReport", fail_after) -> None:
    with db.transaction() as conn:
        _lock(conn)                 # before the table exists: concurrent CREATE ... IF NOT EXISTS races on PG
        _ensure_table(conn)
        rows = {int(v): c for v, c in conn.all(f"SELECT version, checksum FROM {MIGRATION_TABLE}")}
        for v, c in rows.items():
            code = next((x for x in ordered if x.version == v), None)
            if code is not None and code.checksum() != c:
                raise ChecksumMismatch(f"version {v} ({code.name}) was changed after it was applied")
        if m.version in rows:
            report.skipped.append(m.version)
            return
        started = time.perf_counter()
        for i, sql in enumerate(m.statements(db.dialect)):
            conn.execute(sql)
            if fail_after is not None:
                fail_after(m.version, i)
        ms = (time.perf_counter() - started) * 1000
        conn.execute(f"INSERT INTO {MIGRATION_TABLE} (version, name, checksum, applied_at, duration_ms, irreversible) "
                     "VALUES (?,?,?,?,?,?)", (m.version, m.name, m.checksum(), conn.now_ms(), int(ms),
                                              1 if m.irreversible else 0))
        report.applied.append(m.version)
        report.durations_ms[m.version] = round(ms, 3)


def rollback(db: Database, migrations: Sequence[Migration], *, to_version: int) -> List[int]:
    """Undo applied versions above ``to_version``, newest first, each in its own transaction. Refuses before
    changing anything if any of them is irreversible."""
    ordered = {m.version: m for m in _validate(migrations)}
    done = applied(db)
    victims = sorted((v for v in done if v > to_version), reverse=True)
    for v in victims:
        if done[v][2] or (v in ordered and ordered[v].irreversible):
            raise IrreversibleMigration(f"version {v} cannot be rolled back; restore from backup instead")
        if v not in ordered:
            raise MigrationError(f"MIGRATION_UNKNOWN_TO_CODE:{v}")
    undone = []
    for v in victims:
        with db.transaction() as conn:
            _lock(conn)
            for sql in ordered[v].statements(db.dialect, direction="down"):
                conn.execute(sql)
            conn.execute(f"DELETE FROM {MIGRATION_TABLE} WHERE version=?", (v,))
        undone.append(v)
    return undone


@dataclass(frozen=True)
class Compatibility:
    compatible: bool
    database_version: int
    reason: str


def compatibility(db: Database, *, app_min_schema: int, app_max_schema: int,
                  migrations: Sequence[Migration] = ()) -> Compatibility:
    """Can a binary that understands schemas ``[app_min_schema, app_max_schema]`` run on this database?

    A database newer than the binary is acceptable only when every newer version is additive (not marked
    irreversible): additive tables and columns are ignored by older code. A newer irreversible version means
    the old binary would misread the data -- the rollback must restore a backup instead."""
    version = current_version(db)
    if version < app_min_schema:
        return Compatibility(False, version, f"DATABASE_TOO_OLD: {version} < {app_min_schema}; run migrations first")
    if version <= app_max_schema:
        return Compatibility(True, version, "WITHIN_RANGE")
    newer = {v: meta for v, meta in applied(db).items() if v > app_max_schema}
    blocking = sorted(v for v, (_, _, irreversible) in newer.items() if irreversible)
    if blocking:
        return Compatibility(False, version, f"IRREVERSIBLE_NEWER_SCHEMA:{blocking}")
    return Compatibility(True, version, f"ADDITIVE_NEWER_SCHEMA:{sorted(newer)}")
