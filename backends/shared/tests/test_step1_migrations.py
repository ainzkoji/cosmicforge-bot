"""Step 1 schema changes: additive, repeatable, safe on a populated database.

What Step 1 added (all through ``shared_lib.persistence.migrations.migrate``):

    bot_instances            + risk_profile_version, max_position_usdt,
                               risk_acknowledged_at, deploy_request_id,
                               stopped_reason, index on deploy_request_id
    deployment_consents      new (consent identity and version per deployment)
    account_equity_snapshots new (equity history)        account_equity_daily new
    user_events              new (customer events)       notification_outbox  new

Nothing is dropped or rewritten, so the recovery path for a failed deployment
is to run the previous code against the same file: it ignores the new columns
and tables. These tests cover a clean database, a populated one from before
Step 1, a repeated and an interrupted migration, the constraints, and that a
bot carries no environment of its own.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))

from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

BOT_COLUMNS = {"risk_profile_version", "max_position_usdt", "risk_acknowledged_at", "deploy_request_id", "stopped_reason"}
NEW_TABLES = {"deployment_consents", "account_equity_snapshots", "account_equity_daily", "user_events", "notification_outbox"}
NOW = "2026-10-08T12:00:00+00:00"


def columns(db, table):
    with db.connect() as c:
        return {r[1] for r in c.execute(f"PRAGMA table_info({table})")}


def tables(db):
    with db.connect() as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def schema(db):
    with db.connect() as c:
        return sorted((r[0], r[1], r[2] or "") for r in c.execute("SELECT type, name, sql FROM sqlite_master"))


def seed(db):
    """Two customers with an account, a bot, a fill and a subscription each."""
    with db.connect() as c:
        for uid in ("alice", "bob"):
            c.execute("INSERT INTO users (id,email,hashed_password,status,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", NOW, NOW))
            c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,updated_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?)", (f"acct-{uid}", uid, "binance", "crypto", uid, "connected", "demo", NOW, NOW))
            c.execute("INSERT INTO bot_instances (id,user_id,broker_account_id,market_type,strategy_id,strategy_version,config_id,"
                      "risk_profile_id,symbols_json,timeframes_json,allocation_type,allocation_value,mode,status,created_at,updated_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (f"bot-{uid}", uid, f"acct-{uid}", "crypto", "cati", "1", "cfg", "rp", "[]", "[]", "fixed_amount", 120.0,
                       "paper", "active", NOW, NOW))


def rows(db, table, order):
    with db.connect() as c:
        return [tuple(r) for r in c.execute(f"SELECT * FROM {table} ORDER BY {order}")]


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "migrate.db"))
    migrate(database)
    return database


def to_pre_step1(db):
    """Turn a migrated database back into the shape it had before Step 1."""
    with db.connect() as c:
        c.execute("DROP INDEX IF EXISTS idx_bot_instances_deploy_request")
        for column in BOT_COLUMNS:
            c.execute(f"ALTER TABLE bot_instances DROP COLUMN {column}")
        for table in NEW_TABLES:
            c.execute(f"DROP TABLE {table}")


def test_a_clean_database_gets_every_step_one_object(db):
    assert BOT_COLUMNS <= columns(db, "bot_instances")
    assert NEW_TABLES <= tables(db)
    with db.connect() as c:
        indexes = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_bot_instances_deploy_request", "idx_deployment_consents_bot", "idx_account_equity_snapshots_account_time",
            "idx_user_events_user_at", "idx_user_events_bot_at", "idx_notification_outbox_due"} <= indexes


def test_a_repeated_migration_changes_nothing(db):
    seed(db)
    migrate(db)        # an older backfill (universe_mode) settles new rows once; that predates Step 1
    before_schema, before_rows = schema(db), rows(db, "bot_instances", "id")
    for _ in range(3):
        migrate(db)
    assert schema(db) == before_schema and rows(db, "bot_instances", "id") == before_rows


def test_a_populated_database_from_before_step_one_keeps_every_row(db):
    seed(db)
    migrate(db)        # settle the pre-Step-1 backfills first, so only Step 1 is measured
    to_pre_step1(db)
    assert not (BOT_COLUMNS & columns(db, "bot_instances")) and not (NEW_TABLES & tables(db))
    legacy_columns = sorted(columns(db, "bot_instances"))
    with db.connect() as c:
        before = {t: [tuple(r) for r in c.execute(f"SELECT * FROM {t} ORDER BY id")] for t in ("users", "broker_accounts")}
        bots = [tuple(r) for r in c.execute(f"SELECT {','.join(legacy_columns)} FROM bot_instances ORDER BY id")]
    migrate(db)
    assert BOT_COLUMNS <= columns(db, "bot_instances") and NEW_TABLES <= tables(db)
    with db.connect() as c:
        assert {t: [tuple(r) for r in c.execute(f"SELECT * FROM {t} ORDER BY id")] for t in ("users", "broker_accounts")} == before
        # Every pre-existing bot column still reads exactly as before (a legacy reader is unaffected) ...
        assert [tuple(r) for r in c.execute(f"SELECT {','.join(legacy_columns)} FROM bot_instances ORDER BY id")] == bots
        # ... and the new ones are NULL, never an invented value.
        new = [tuple(r) for r in c.execute(f"SELECT {','.join(sorted(BOT_COLUMNS))} FROM bot_instances")]
    assert new == [(None,) * len(BOT_COLUMNS)] * 2


def test_an_interrupted_migration_is_completed_by_the_next_run(db):
    seed(db)
    with db.connect() as c:                                 # some of the objects exist, some do not
        c.execute("ALTER TABLE bot_instances DROP COLUMN stopped_reason")
        c.execute("DROP INDEX IF EXISTS idx_bot_instances_deploy_request")
        c.execute("ALTER TABLE bot_instances DROP COLUMN deploy_request_id")
        c.execute("DROP TABLE notification_outbox")
        c.execute("DROP TABLE account_equity_daily")
        c.execute("DROP INDEX idx_user_events_bot_at")
    migrate(db)
    assert BOT_COLUMNS <= columns(db, "bot_instances") and NEW_TABLES <= tables(db)
    with db.connect() as c:
        assert c.execute("SELECT 1 FROM sqlite_master WHERE name='idx_user_events_bot_at'").fetchone()
        assert c.execute("SELECT COUNT(*) FROM bot_instances").fetchone()[0] == 2


def test_new_writes_and_their_constraints(db):
    seed(db)
    with db.connect() as c:
        c.execute("UPDATE bot_instances SET allocation_type='risk_based', allocation_value=0.5, risk_profile_version='2026-10-08.v1', "
                  "max_position_usdt=200, risk_acknowledged_at=?, deploy_request_id='req-1' WHERE id='bot-alice'", (NOW,))
        c.execute("INSERT INTO deployment_consents VALUES ('c1','alice','bot-alice','acct-alice','balanced','2026-10-08.v1','v1',?,'req-1','{}')", (NOW,))
        c.execute("INSERT INTO account_equity_snapshots (snapshot_id,user_id,broker_account_id,observed_at,equity,source,reason,dedupe_key) "
                  "VALUES ('s1','alice','acct-alice',1,1000.0,'ENGINE_CYCLE','HOURLY','k1')")
        c.execute("INSERT INTO user_events VALUES ('e1','BOT_PAUSED',1,'alice','bot-alice','acct-alice','DEMO','{}',?)", (NOW,))
        c.execute("INSERT INTO notification_outbox (outbox_id,event_id,user_id,channel,next_attempt_at,created_at,dedupe_key) "
                  "VALUES ('o1','e1','alice','email',1,1,'e1:email')")
    duplicates = [
        "INSERT INTO deployment_consents VALUES ('c1','alice','bot-alice','acct-alice','balanced','v','v','t','r','{}')",
        "INSERT INTO account_equity_snapshots (snapshot_id,broker_account_id,observed_at,equity,source,reason,dedupe_key) "
        "VALUES ('s2','acct-alice',2,1.0,'X','Y','k1')",
        "INSERT INTO user_events VALUES ('e1','BOT_PAUSED',2,'alice',NULL,NULL,NULL,'{}','t')",
        "INSERT INTO notification_outbox (outbox_id,event_id,user_id,channel,next_attempt_at,created_at,dedupe_key) "
        "VALUES ('o2','e1','alice','email',1,1,'e1:email')",
        "INSERT INTO user_events VALUES ('e9','BOT_PAUSED',2,NULL,NULL,NULL,NULL,'{}','t')",          # an event always has an owner
        "INSERT INTO account_equity_snapshots (snapshot_id,broker_account_id,observed_at,source,reason,dedupe_key) "
        "VALUES ('s3','acct-alice',2,'X','Y','k3')",                                                   # equity is never NULL
    ]
    for statement in duplicates:
        with pytest.raises(sqlite3.IntegrityError):
            with db.connect() as c:
                c.execute(statement)
    migrate(db)                                             # a later migration keeps the new rows
    with db.connect() as c:
        assert c.execute("SELECT deploy_request_id, risk_profile_version FROM bot_instances WHERE id='bot-alice'").fetchone()[:] == ("req-1", "2026-10-08.v1")
        assert c.execute("SELECT COUNT(*) FROM notification_outbox").fetchone()[0] == 1
        assert c.execute("PRAGMA foreign_key_check").fetchall() == []
        assert c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_rows_stay_with_their_owner(db):
    seed(db)
    with db.connect() as c:
        c.execute("INSERT INTO user_events VALUES ('e-a','BOT_PAUSED',1,'alice','bot-alice','acct-alice','DEMO','{}',?)", (NOW,))
        c.execute("INSERT INTO user_events VALUES ('e-b','BOT_PAUSED',1,'bob','bot-bob','acct-bob','DEMO','{}',?)", (NOW,))
    migrate(db)
    with db.connect() as c:
        assert [r[0] for r in c.execute("SELECT event_id FROM user_events WHERE user_id='alice'")] == ["e-a"]
        assert [r[0] for r in c.execute("SELECT user_id FROM bot_instances WHERE broker_account_id='acct-bob'")] == ["bob"]


def test_a_bot_has_no_environment_column_and_the_short_lived_unique_index_is_gone(db):
    assert "environment" not in columns(db, "bot_instances")       # always the broker account's
    assert "environment" in columns(db, "broker_accounts")
    seed(db)
    with db.connect() as c:                                        # a build of this step briefly created the index
        c.execute("CREATE UNIQUE INDEX ux_bot_instances_one_occupying_per_account ON bot_instances(broker_account_id) "
                  "WHERE status IN ('active','paused')")
    migrate(db)
    with db.connect() as c:
        assert c.execute("SELECT 1 FROM sqlite_master WHERE name='ux_bot_instances_one_occupying_per_account'").fetchone() is None
        # Two occupying bots on one account stay a representable (and reported) state.
        c.execute("INSERT INTO bot_instances (id,user_id,broker_account_id,market_type,strategy_id,strategy_version,config_id,"
                  "risk_profile_id,symbols_json,timeframes_json,allocation_type,allocation_value,mode,status,created_at,updated_at) "
                  "VALUES ('bot-alice-2','alice','acct-alice','crypto','cati','1','cfg','rp','[]','[]','fixed_amount',1,'paper','paused',?,?)",
                  (NOW, NOW))
    migrate(db)
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM bot_instances WHERE broker_account_id='acct-alice'").fetchone()[0] == 2
