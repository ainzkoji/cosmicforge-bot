"""The legacy persistence inventory (``scripts/legacy_persistence_inventory.py``) and the money classification of
every REAL column of the legacy schema (``shared_lib.persistence.legacy_money``), Step 5.1 / EXT-DB-01.

These fail the day the legacy schema gains a REAL column nobody classified as money or not, or a table created on
first use that the inventory (and therefore the PostgreSQL port and the migration drill) does not build.
"""
from __future__ import annotations

import importlib.util
import re
import sqlite3
from pathlib import Path

import pytest

from shared_lib.persistence import legacy_money as LM

ROOT = Path(__file__).resolve().parents[4]
BACKENDS = ROOT / "backends"


def _inventory():
    spec = importlib.util.spec_from_file_location("legacy_inventory_under_test",
                                                  ROOT / "scripts" / "legacy_persistence_inventory.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    inv = _inventory()
    path = tmp_path_factory.mktemp("legacy_inventory") / "legacy.db"
    steps = inv.build_legacy_schema(path)
    conn = sqlite3.connect(str(path))
    yield inv, conn, steps
    conn.close()


def test_every_real_column_of_the_legacy_schema_is_classified(built):
    inv, conn, _ = built
    real, unclassified = set(), []
    for table in inv.legacy_tables(conn):
        for _, col, ctype, *_ in conn.execute(f'PRAGMA table_info("{table}")').fetchall():
            if LM.is_real_type(ctype):
                real.add((table, col))
                try:
                    LM.classify_real_column(table, col)
                except LM.UnclassifiedRealColumn:
                    unclassified.append(f"{table}.{col}")
    assert len(real) > 600
    assert not unclassified, ("classify these REAL columns in shared_lib/persistence/legacy_money.py "
                              f"(MONETARY_COLUMNS or NON_MONETARY_REAL_COLUMNS): {unclassified}")
    listed = {(t, c) for m in (LM.MONETARY_COLUMNS, LM.NON_MONETARY_REAL_COLUMNS) for t, cs in m.items() for c in cs}
    assert not listed - real, f"classified columns that no longer exist: {sorted(listed - real)}"
    money = {(t, c) for t, cs in LM.MONETARY_COLUMNS.items() for c in cs}
    for must in (("trade_fills", "fee"), ("trade_fills", "funding_fees"), ("positions", "committed_margin"),
                 ("cati_reservation_ledger", "notional"), ("cati_reservation_ledger", "risk_amount"),
                 ("cati_execution_intents", "quantity"), ("cati_instrument_rules", "price_tick"),
                 ("cati_instrument_rules", "quantity_step"), ("account_equity_snapshots", "equity"),
                 ("cati_user_stream_fills", "realized_pnl"), ("execution_attempts", "requested_notional")):
        assert must in money, must


def test_the_inventory_builds_every_legacy_table_the_source_can_create(built):
    """Tables created on first use are part of the legacy schema too: every CREATE TABLE literal of the
    application (outside the dialect-neutral platform layer) is built by ``build_legacy_schema``."""
    inv, conn, steps = built
    have = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    literal = re.compile(r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-z][a-z0-9_]*)\s*\(", re.IGNORECASE)
    templated = re.compile(r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+\{([A-Za-z_][A-Za-z0-9_]*)\}", re.IGNORECASE)
    found = {}
    for root in (BACKENDS / "bot-backend" / "app", BACKENDS / "shared" / "shared_lib", BACKENDS / "user-backend" / "app"):
        for path in root.rglob("*.py"):
            if "persistence" in path.parts and "sql" in path.parts:
                continue                                   # the platform layer: versioned, already PostgreSQL
            text = path.read_text(encoding="utf-8", errors="replace")
            names = {m.lower() for m in literal.findall(text) if m.islower()}
            for constant in set(templated.findall(text)):
                value = re.search(rf"^{constant}\s*=\s*[\"']([a-z][a-z0-9_]*)[\"']", text, re.MULTILINE)
                if value:
                    names.add(value.group(1))
            for name in names:
                found.setdefault(name, path.relative_to(BACKENDS).as_posix())
    missing = {t: where for t, where in found.items()
               if t not in have and not inv.PLATFORM_TABLE.match(t) and t not in inv.TRANSIENT_TABLES}
    assert len(found) > 250
    assert not missing, f"add their creator to legacy_persistence_inventory._first_use_creators: {missing}"
    assert [s["step"] for s in steps][:4] == ["shared_lib.persistence.db.DB._init",
                                              "shared_lib.persistence.migrations.migrate",
                                              "app.execution.production_schema.ensure",
                                              "app.ops.runtime_ownership.ensure_ownership_schema"]


def test_table_facts_are_deterministic_and_hold_no_row_data(built):
    inv, conn, _ = built
    conn.execute("INSERT INTO users (id, email, hashed_password, created_at, updated_at) "
                 "VALUES ('u-secret', 'someone@example.invalid', 'not-a-real-hash', 'x', 'x')")
    a = {t: inv.table_facts(conn, t) for t in ("users", "trade_fills", "cati_reservation_ledger")}
    b = {t: inv.table_facts(conn, t) for t in ("users", "trade_fills", "cati_reservation_ledger")}
    assert a == b
    text = repr(a)
    assert "someone@example.invalid" not in text and "not-a-real-hash" not in text and "u-secret" not in text
    conn.rollback()
    ledger = a["cati_reservation_ledger"]
    assert ledger["real_columns"]["notional"] == LM.MONETARY and ledger["real_columns"]["leverage"] == LM.NON_MONETARY
    assert ledger["triggers"] and any(u["partial"] for u in ledger["unique_constraints"])


def test_migration_history_and_source_scan_shape():
    inv = _inventory()
    history = inv.migration_history()
    assert history["versioned"] is False and history["add_column_upgrades"] > 100
    assert "ensure_cati_schema" in history["ensure_calls_in_order"] and len(history["numbered_steps"]) > 20
    scan = inv.source_scan()
    assert "backends/shared/shared_lib/persistence/db.py" in scan["direct_sqlite3_importers"]
    assert scan["construct_totals"]["begin_immediate"] > 0 and scan["construct_totals"]["insert_or_ignore"] > 0
    assert all(not ("/tests/" in f or f.split("/")[-1].startswith("test_")) for f in scan["per_file_constructs"])
