from __future__ import annotations

import os
import tempfile

import pytest


def _make_db(path: str):
    from shared_lib.persistence.migrations import migrate
    migrate(path)
    from shared_lib.persistence.db import DB
    return DB(path)


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def db(db_path):
    return _make_db(db_path)


def test_record_fill_derives_user_id_from_bot_instance(db):
    """
    New fills should persist trade_fills.user_id when bot_instance_id is known.
    This is best-effort enrichment and must not affect execution behaviour.
    """
    bot_instance_id = "bot-test-1"
    user_id = "user-abc"

    with db.connect() as conn:
        conn.execute(
            "INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, mode, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
            (bot_instance_id, user_id, "brk-1", "futures", "test_strategy", "live"),
        )

    from shared_lib.persistence.trade_fills import record_fill

    record_fill(
        db,
        symbol="BTCUSDT",
        side="LONG",
        action="OPEN",
        qty=0.01,
        price=50000.0,
        bot_instance_id=bot_instance_id,
        broker_account_id="brk-1",
        position_id="pos-1",
        trace_id="trace-1",
        run_id="run-1",
        cycle_id="cycle-1",
        # user_id intentionally omitted
    )

    with db.connect() as conn:
        row = conn.execute(
            "SELECT user_id, bot_instance_id FROM trade_fills WHERE position_id='pos-1' AND action='OPEN' LIMIT 1"
        ).fetchone()

    assert row is not None
    assert row["bot_instance_id"] == bot_instance_id
    assert row["user_id"] == user_id


def test_backfill_script_updates_only_linkable_rows(tmp_path):
    """
    Backfill must only set user_id when bot_instance_id maps to a bot_instances.user_id.
    Ambiguous/unmatched rows must remain unchanged.
    """
    db_path = str(tmp_path / "bf.db")

    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE bot_instances (id TEXT PRIMARY KEY, user_id TEXT);
            CREATE TABLE trade_fills (id INTEGER PRIMARY KEY, user_id TEXT, bot_instance_id TEXT);
            """
        )
        conn.execute("INSERT INTO bot_instances (id, user_id) VALUES ('bot-1', 'user-1')")
        conn.execute("INSERT INTO bot_instances (id, user_id) VALUES ('bot-2', '')")

        # linkable
        conn.execute("INSERT INTO trade_fills (id, user_id, bot_instance_id) VALUES (1, NULL, 'bot-1')")
        # already set
        conn.execute("INSERT INTO trade_fills (id, user_id, bot_instance_id) VALUES (2, 'user-x', 'bot-1')")
        # bot_instance missing
        conn.execute("INSERT INTO trade_fills (id, user_id, bot_instance_id) VALUES (3, NULL, 'bot-missing')")
        # bot_instance has empty user_id
        conn.execute("INSERT INTO trade_fills (id, user_id, bot_instance_id) VALUES (4, NULL, 'bot-2')")
        conn.commit()
    finally:
        conn.close()

    import importlib.util
    from pathlib import Path

    script_path = Path(__file__).resolve().parents[1] / "scripts" / "backfill_trade_fills_user_id.py"
    spec = importlib.util.spec_from_file_location("backfill_trade_fills_user_id", script_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    backfill_trade_fills_user_id = mod.backfill_trade_fills_user_id

    res = backfill_trade_fills_user_id(db_path, dry_run=False)
    assert res.would_update == 1
    assert res.updated == 1
    assert res.skipped_no_bot_instance == 1
    assert res.skipped_no_bot_instance_user == 1
    assert res.already_set == 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = {r["id"]: dict(r) for r in conn.execute("SELECT id, user_id, bot_instance_id FROM trade_fills").fetchall()}
        assert rows[1]["user_id"] == "user-1"
        assert rows[2]["user_id"] == "user-x"
        assert rows[3]["user_id"] is None
        assert rows[4]["user_id"] is None
    finally:
        conn.close()
