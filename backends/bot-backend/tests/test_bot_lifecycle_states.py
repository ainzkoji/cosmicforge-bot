import pytest
from app.core.bot_instance_service import BotInstanceService
from shared_lib.persistence.db import DB
from datetime import datetime, timezone
import tempfile
import uuid
import os

def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def _make_db() -> DB:
    """SQLite DB with bot_instances and broker_accounts tables."""
    db_path = os.path.join(tempfile.gettempdir(), f"test_lifecycle_{uuid.uuid4().hex}.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    db = DB(path=db_path)
    with db.connect() as conn:
        conn.executescript(f"""
            DROP TABLE IF EXISTS broker_accounts;
            DROP TABLE IF EXISTS bot_instances;
            
            CREATE TABLE broker_accounts (
                id           TEXT PRIMARY KEY,
                user_id      TEXT NOT NULL,
                broker_id    TEXT NOT NULL DEFAULT 'binance',
                environment  TEXT NOT NULL DEFAULT 'live',
                status       TEXT NOT NULL DEFAULT 'connected',
                validation_error TEXT,
                updated_at   TEXT,
                active_credential_version INTEGER
            );

            CREATE TABLE bot_instances (
                id                   TEXT PRIMARY KEY,
                user_id              TEXT NOT NULL,
                broker_account_id    TEXT NOT NULL,
                market_type          TEXT NOT NULL DEFAULT 'spot',
                strategy_id          TEXT NOT NULL DEFAULT 'test_strat',
                strategy_version     TEXT NOT NULL DEFAULT '1.0',
                allocation_type      TEXT NOT NULL DEFAULT 'percent',
                allocation_value     REAL NOT NULL DEFAULT 100.0,
                mode                 TEXT NOT NULL DEFAULT 'live',
                status               TEXT NOT NULL DEFAULT 'active',
                last_error           TEXT,
                last_run_at          TEXT,
                created_at           TEXT NOT NULL DEFAULT '2023-01-01',
                updated_at           TEXT,
                broker_health_status TEXT DEFAULT 'ok',
                broker_error_code    TEXT,
                broker_blocked_at    TEXT,
                block_category       TEXT,
                block_reason_code    TEXT,
                block_reason_detail  TEXT,
                blocked_since        TEXT,
                last_validated_at    TEXT,
                last_validation_error TEXT
            );
        """)
    return db

def _insert_bot(conn, bot_id: str, user_id: str, broker_account_id: str, status: str = "active") -> None:
    conn.execute(
        """INSERT INTO bot_instances 
           (id, user_id, broker_account_id, status, market_type, strategy_id, strategy_version, allocation_type, allocation_value, mode, created_at, updated_at) 
           VALUES (?,?,?,?, 'spot', 'strat', '1.0', 'pct', 100.0, 'live', '2023-01-01', '2023-01-01')""",
        (bot_id, user_id, broker_account_id, status),
    )

def test_deleted_bot_is_not_resurrected_by_runtime_error():
    db = _make_db()
    svc = BotInstanceService(db=db)
    
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)", ("brk_1", "user_A", "binance", "connected"))
        _insert_bot(conn, "bot_1", "user_A", "brk_1", status="deleted")
        
    # The runner might try to update an instance if it failed while caching it
    svc.update_instance_runtime_state(
        "bot_1", 
        error_message="some unexpected runtime crash"
    )
    
    with db.connect() as conn:
        row = conn.execute("SELECT status, last_error FROM bot_instances WHERE id='bot_1'").fetchone()
        
    # Status MUST remain deleted, it must not turn to 'error'
    assert row["status"] == "deleted"
    assert row["last_error"] == "some unexpected runtime crash"

def test_stopped_bot_is_not_resurrected():
    db = _make_db()
    svc = BotInstanceService(db=db)
    
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)", ("brk_2", "user_A", "binance", "connected"))
        _insert_bot(conn, "bot_2", "user_A", "brk_2", status="stopped")
        
    svc.update_instance_runtime_state(
        "bot_2", 
        error_message="stop requested but script crashed"
    )
    
    with db.connect() as conn:
        row = conn.execute("SELECT status FROM bot_instances WHERE id='bot_2'").fetchone()
        
    assert row["status"] == "stopped"


def test_archived_bot_is_not_resurrected():
    db = _make_db()
    svc = BotInstanceService(db=db)
    
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)", ("brk_3", "user_A", "binance", "connected"))
        _insert_bot(conn, "bot_3", "user_A", "brk_3", status="archived")
        
    svc.update_instance_runtime_state(
        "bot_3", 
        error_message="some unexpected runtime crash"
    )
    
    with db.connect() as conn:
        row = conn.execute("SELECT status FROM bot_instances WHERE id='bot_3'").fetchone()
        
    assert row["status"] == "archived"


def test_archived_bot_excluded_from_active_query():
    db = _make_db()
    svc = BotInstanceService(db=db)
    
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)", ("brk_4", "user_A", "binance", "connected"))
        _insert_bot(conn, "bot_4_active", "user_A", "brk_4", status="active")
        _insert_bot(conn, "bot_4_archived", "user_A", "brk_4", status="archived")
        
    active_bots = svc.get_active_bot_instances()
    
    # Needs a broker to return, but the test DB has limited schema logic, so we just verify what gets returned
    active_ids = [b.id for b in active_bots]
    assert "bot_4_active" in active_ids
    assert "bot_4_archived" not in active_ids


def test_archived_bot_excluded_from_blocked_query():
    db = _make_db()
    svc = BotInstanceService(db=db)
    
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)", ("brk_5", "user_A", "binance", "connected"))
        # Insert a blocked active bot
        _insert_bot(conn, "bot_5_blocked", "user_A", "brk_5", status="error")
        conn.execute("UPDATE bot_instances SET broker_health_status='broker_blocked' WHERE id='bot_5_blocked'")
        
        # Insert a blocked archived bot
        _insert_bot(conn, "bot_5_archived", "user_A", "brk_5", status="archived")
        conn.execute("UPDATE bot_instances SET broker_health_status='broker_blocked' WHERE id='bot_5_archived'")
        
    blocked_bots = svc.get_broker_blocked_instances()
    
    blocked_ids = [b.id for b in blocked_bots]
    assert "bot_5_blocked" in blocked_ids
    assert "bot_5_archived" not in blocked_ids
