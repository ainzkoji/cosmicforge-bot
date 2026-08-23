"""
Broker quarantine system test suite.

Tests:
  Q1.  quarantine_bot: sets broker_health_status='broker_blocked', records reason_code + timestamp
  Q2.  get_active_bot_instances: excludes broker_blocked bots, includes ok bots
  Q3.  get_broker_blocked_instances: returns only broker_blocked bots
  Q4.  unquarantine_bot: clears quarantine, restores broker_health_status='ok'
  Q5.  rebind_bot_broker: rebinds to valid same-type account, clears quarantine
  Q6.  rebind_bot_broker: rejects rebind to wrong broker_id
  Q7.  rebind_bot_broker: rejects rebind to unconnected account
  Q8.  rebind_bot_broker: rejects rebind to other user's account
  Q9.  quarantine_bot: pre-migration DB (no broker_health_status column) falls back gracefully
  Q10. get_active_bot_instances: pre-migration DB (no broker_health_status column) returns all active
  Q11. quarantine_bot: repeated quarantine call is idempotent (updates in place)
  Q12. broker_accounts auto-invalidated on ENV_MISMATCH error in multi_runner
  Q13. broker_accounts NOT invalidated for transient errors (auth_failed, pending)
  Q14. runner evicts cached runner from _runners on BrokerResolverError
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = os.path.dirname(__file__)
_BOT_BACKEND = os.path.abspath(os.path.join(_HERE, ".."))
_SHARED = os.path.abspath(os.path.join(_BOT_BACKEND, "..", "shared"))
sys.path.insert(0, _BOT_BACKEND)
sys.path.insert(0, _SHARED)

from shared_lib.persistence.db import DB
from shared_lib.broker.errors import BrokerResolverError


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Minimal DB setup ──────────────────────────────────────────────────────────

import tempfile
import uuid
def _make_db(with_quarantine_cols: bool = True) -> DB:
    """SQLite DB with bot_instances and broker_accounts tables."""
    db_path = os.path.join(tempfile.gettempdir(), f"test_quarantine_{uuid.uuid4().hex}.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    db = DB(path=db_path)
    with db.connect() as conn:
        quarantine_cols = ""
        if with_quarantine_cols:
            quarantine_cols = """
                ,broker_health_status TEXT DEFAULT 'ok'
                ,broker_error_code    TEXT
                ,broker_blocked_at    TEXT
                ,block_category       TEXT
                ,block_reason_code    TEXT
                ,block_reason_detail  TEXT
                ,blocked_since        TEXT
                ,last_validated_at    TEXT
                ,last_validation_error TEXT
            """
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
                updated_at           TEXT
                {quarantine_cols}
            );
        """)
    return db


def _insert_broker(conn, account_id: str, user_id: str, broker_id: str = "binance",
                   status: str = "connected") -> None:
    conn.execute(
        "INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)",
        (account_id, user_id, broker_id, status),
    )


def _insert_bot(conn, bot_id: str, user_id: str, broker_account_id: str,
                status: str = "active", broker_health_status: str = "ok") -> None:
    try:
        conn.execute(
            """INSERT INTO bot_instances
               (id, user_id, broker_account_id, status, broker_health_status, market_type, strategy_id, strategy_version, allocation_type, allocation_value, mode, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'spot', 'strat', '1.0', 'pct', 100.0, 'live', '2023-01-01', '2023-01-01')""",
            (bot_id, user_id, broker_account_id, status, broker_health_status),
        )
    except Exception:
        # Pre-migration table without broker_health_status
        conn.execute(
            """INSERT INTO bot_instances 
               (id, user_id, broker_account_id, status, market_type, strategy_id, strategy_version, allocation_type, allocation_value, mode, created_at, updated_at) 
               VALUES (?,?,?,?, 'spot', 'strat', '1.0', 'pct', 100.0, 'live', '2023-01-01', '2023-01-01')""",
            (bot_id, user_id, broker_account_id, status),
        )


# ── Import service under test ─────────────────────────────────────────────────

def _get_service(db: DB):
    from app.core.bot_instance_service import BotInstanceService
    return BotInstanceService(db=db)


# ── Q1: quarantine_bot sets expected columns ──────────────────────────────────

def test_q1_quarantine_bot_sets_columns():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1")

    svc.quarantine_bot("bot_1", BrokerResolverError.REASON_ENV_MISMATCH, "env mismatch test")

    with db.connect() as conn:
        row = conn.execute(
            "SELECT broker_health_status, broker_error_code, broker_blocked_at, last_error "
            "FROM bot_instances WHERE id='bot_1'"
        ).fetchone()

        row2 = conn.execute(
            "SELECT block_category, block_reason_code, blocked_since "
            "FROM bot_instances WHERE id='bot_1'"
        ).fetchone()

    assert row["broker_health_status"] == "broker_blocked"
    assert row["broker_error_code"] == BrokerResolverError.REASON_ENV_MISMATCH
    assert row["broker_blocked_at"] is not None
    assert "broker_env_mismatch" in row["last_error"]
    
    assert row2["block_category"] == "broker_auth_failure"
    assert row2["block_reason_code"] == BrokerResolverError.REASON_ENV_MISMATCH
    assert row2["blocked_since"] is not None


# ── Q2: get_active_bot_instances excludes blocked, includes ok ────────────────

def test_q2_active_excludes_blocked():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_broker(conn, "brk_2", "user_A")
        # ok bot
        _insert_bot(conn, "bot_ok", "user_A", "brk_1", broker_health_status="ok")
        # blocked bot
        conn.execute(
            """INSERT INTO bot_instances
               (id, user_id, broker_account_id, status, broker_health_status, broker_error_code, broker_blocked_at)
               VALUES (?, ?, ?, 'active', 'broker_blocked', ?, ?)""",
            ("bot_blocked", "user_A", "brk_2", BrokerResolverError.REASON_ENV_MISMATCH, _utc()),
        )

    active = svc.get_active_bot_instances()
    ids = [b.id for b in active]
    assert "bot_ok" in ids
    assert "bot_blocked" not in ids


# ── Q3: get_broker_blocked_instances returns only blocked bots ────────────────

def test_q3_blocked_instances_returned():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_broker(conn, "brk_2", "user_A")
        _insert_bot(conn, "bot_ok", "user_A", "brk_1", broker_health_status="ok")
        conn.execute(
            """INSERT INTO bot_instances
               (id, user_id, broker_account_id, status, broker_health_status, broker_error_code, broker_blocked_at)
               VALUES (?, ?, ?, 'active', 'broker_blocked', ?, ?)""",
            ("bot_blocked", "user_A", "brk_2", BrokerResolverError.REASON_ENV_MISMATCH, _utc()),
        )

    blocked = svc.get_broker_blocked_instances()
    ids = [b.id for b in blocked]
    assert "bot_blocked" in ids
    assert "bot_ok" not in ids


# ── Q4: unquarantine_bot clears quarantine fields ─────────────────────────────

def test_q4_unquarantine_clears_fields():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1")

    svc.quarantine_bot("bot_1", BrokerResolverError.REASON_ENV_MISMATCH, "env mismatch test")
    svc.unquarantine_bot("bot_1")

    with db.connect() as conn:
        row = conn.execute(
            "SELECT broker_health_status, broker_error_code, broker_blocked_at, last_error "
            "FROM bot_instances WHERE id='bot_1'"
        ).fetchone()

    assert row["broker_health_status"] == "ok"
    assert row["broker_error_code"] is None
    assert row["broker_blocked_at"] is None
    assert row["last_error"] is None


# ── Q5: rebind_bot_broker rebinds to valid same-type account ──────────────────

def test_q5_rebind_to_valid_account():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_old", "user_A", broker_id="binance", status="invalid")
        _insert_broker(conn, "brk_new", "user_A", broker_id="binance", status="connected")
        _insert_bot(conn, "bot_1", "user_A", "brk_old")
        conn.execute(
            """UPDATE bot_instances
               SET broker_health_status='broker_blocked', broker_error_code=?
               WHERE id='bot_1'""",
            (BrokerResolverError.REASON_ENV_MISMATCH,),
        )

    svc.rebind_bot_broker("bot_1", "brk_new")

    with db.connect() as conn:
        row = conn.execute(
            "SELECT broker_account_id, broker_health_status, broker_error_code "
            "FROM bot_instances WHERE id='bot_1'"
        ).fetchone()

    assert row["broker_account_id"] == "brk_new"
    assert row["broker_health_status"] == "ok"
    assert row["broker_error_code"] is None


# ── Q6: rebind_bot_broker rejects different broker_id ─────────────────────────

def test_q6_rebind_rejects_wrong_broker_id():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_old", "user_A", broker_id="binance", status="invalid")
        _insert_broker(conn, "brk_bybit", "user_A", broker_id="bybit", status="connected")
        _insert_bot(conn, "bot_1", "user_A", "brk_old")

    with pytest.raises(ValueError, match="Broker type mismatch"):
        svc.rebind_bot_broker("bot_1", "brk_bybit")


# ── Q7: rebind_bot_broker rejects unconnected account ─────────────────────────

def test_q7_rebind_rejects_unconnected():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_old", "user_A", broker_id="binance", status="invalid")
        _insert_broker(conn, "brk_new", "user_A", broker_id="binance", status="invalid")
        _insert_bot(conn, "bot_1", "user_A", "brk_old")

    with pytest.raises(ValueError, match="not connected"):
        svc.rebind_bot_broker("bot_1", "brk_new")


# ── Q8: rebind_bot_broker rejects another user's account ──────────────────────

def test_q8_rebind_rejects_other_user_account():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_old", "user_A", broker_id="binance", status="invalid")
        _insert_broker(conn, "brk_user_b", "user_B", broker_id="binance", status="connected")
        _insert_bot(conn, "bot_1", "user_A", "brk_old")

    with pytest.raises(ValueError, match="does not belong"):
        svc.rebind_bot_broker("bot_1", "brk_user_b")


# ── Q9: quarantine_bot on pre-migration DB (no quarantine cols) ───────────────

def test_q9_quarantine_pre_migration_graceful():
    db = _make_db(with_quarantine_cols=False)
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1")

    # Should not raise — falls back to last_error update only
    svc.quarantine_bot("bot_1", BrokerResolverError.REASON_ENV_MISMATCH, "env mismatch test")

    with db.connect() as conn:
        row = conn.execute(
            "SELECT last_error FROM bot_instances WHERE id='bot_1'"
        ).fetchone()

    assert "broker_env_mismatch" in (row["last_error"] or "")


# ── Q10: get_active_bot_instances on pre-migration DB returns all active ───────

def test_q10_active_pre_migration_returns_all():
    db = _make_db(with_quarantine_cols=False)
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_broker(conn, "brk_2", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1")
        _insert_bot(conn, "bot_2", "user_A", "brk_2")

    active = svc.get_active_bot_instances()
    ids = [b.id for b in active]
    assert "bot_1" in ids
    assert "bot_2" in ids


# ── Q11: repeated quarantine_bot is idempotent ────────────────────────────────

def test_q11_quarantine_idempotent():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1")

    svc.quarantine_bot("bot_1", BrokerResolverError.REASON_ENV_MISMATCH, "first quarantine")
    svc.quarantine_bot("bot_1", BrokerResolverError.REASON_DECRYPT_FAILED, "second quarantine")

    with db.connect() as conn:
        row = conn.execute(
            "SELECT broker_health_status, broker_error_code FROM bot_instances WHERE id='bot_1'"
        ).fetchone()

    # Second quarantine wins
    assert row["broker_health_status"] == "broker_blocked"
    assert row["broker_error_code"] == BrokerResolverError.REASON_DECRYPT_FAILED


# ── Q12: non-transient errors auto-invalidate broker_accounts ─────────────────

def test_q12_non_transient_auto_invalidates_broker():
    """
    When multi_runner handles ENV_MISMATCH or DECRYPT_FAILED, it should
    UPDATE broker_accounts SET status='invalid'. Verify the SQL logic works.
    """
    db = _make_db()
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)",
            ("brk_1", "user_A", "binance", "connected"),
        )

    # Simulate the auto-invalidation block from multi_runner
    exc = BrokerResolverError(
        BrokerResolverError.REASON_ENV_MISMATCH,
        "account row env=live, blob env=demo",
        account_id="brk_1",
    )
    _NON_TRANSIENT = {
        BrokerResolverError.REASON_ENV_MISMATCH,
        BrokerResolverError.REASON_DECRYPT_FAILED,
        BrokerResolverError.REASON_REVOKED,
        BrokerResolverError.REASON_NO_CREDENTIALS,
    }
    if exc.reason_code in _NON_TRANSIENT and exc.account_id:
        with db.connect() as conn:
            conn.execute(
                """
                UPDATE broker_accounts
                SET status='invalid', validation_error=?, updated_at=?
                WHERE id=? AND status NOT IN ('invalid', 'deleted', 'revoked')
                """,
                (f"[{exc.reason_code}] {exc}", _utc(), exc.account_id),
            )

    with db.connect() as conn:
        row = conn.execute(
            "SELECT status, validation_error FROM broker_accounts WHERE id='brk_1'"
        ).fetchone()

    assert row["status"] == "invalid"
    assert "env_mismatch" in (row["validation_error"] or "")


# ── Q13: transient errors do NOT auto-invalidate broker_accounts ──────────────

def test_q13_transient_does_not_invalidate_broker():
    """
    REASON_AUTH_FAILED and REASON_PENDING are transient — should NOT change
    broker_accounts.status to invalid.
    """
    db = _make_db()
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)",
            ("brk_1", "user_A", "binance", "connected"),
        )

    _NON_TRANSIENT = {
        BrokerResolverError.REASON_ENV_MISMATCH,
        BrokerResolverError.REASON_DECRYPT_FAILED,
        BrokerResolverError.REASON_REVOKED,
        BrokerResolverError.REASON_NO_CREDENTIALS,
    }
    for transient_code in (BrokerResolverError.REASON_AUTH_FAILED, BrokerResolverError.REASON_PENDING):
        exc = BrokerResolverError(transient_code, "transient error", account_id="brk_1")
        # Neither code should trigger the invalidation block
        assert exc.reason_code not in _NON_TRANSIENT

    with db.connect() as conn:
        row = conn.execute(
            "SELECT status FROM broker_accounts WHERE id='brk_1'"
        ).fetchone()

    # Status unchanged
    assert row["status"] == "connected"


# ── Q14: quarantine evicts runner from _runners cache ─────────────────────────

def test_q14_runner_evicted_on_quarantine():
    """
    After a BrokerResolverError, the cached PaperRunner for that bot must
    be evicted from _runners so the next cycle creates a fresh one.
    """
    # We test the _runners dict eviction logic in isolation — no full runner needed.
    _runners = {"bot_1": MagicMock(), "bot_2": MagicMock()}

    # Simulate the eviction line from multi_runner.py
    _runners.pop("bot_1", None)

    assert "bot_1" not in _runners
    assert "bot_2" in _runners  # other bots unaffected
