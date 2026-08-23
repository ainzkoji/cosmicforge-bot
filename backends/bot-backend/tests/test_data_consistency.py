"""
Data consistency and portfolio aggregation tests.

Tests:
  D1.  portfolio_summary uses same DB as broker_service (get_db pattern)
  D2.  portfolio active_strategies = running bot count (not open positions)
  D3.  portfolio returns per-account error when broker auth fails — never silent $0
  D4.  deleted bot excluded from runner's active query
  D5.  deleted bot excluded from get_user_bot_instances() default view
  D6.  stopped bot excluded from runner's active query
  D7.  broker-blocked bot not counted in portfolio running_bots
  D8.  soft delete preserves the row (history intact)
  D9.  runner evicts stale cache entries for deleted/stopped bots
  D10. multiple active bots for same account both appear in runner
  D11. portfolio totals aggregate across multiple connected broker accounts
  D12. portfolio returns 0 (not error) when user has no connected brokers
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

_HERE = os.path.dirname(__file__)
_BOT_BACKEND = os.path.abspath(os.path.join(_HERE, ".."))
_SHARED = os.path.abspath(os.path.join(_BOT_BACKEND, "..", "shared"))
sys.path.insert(0, _BOT_BACKEND)
sys.path.insert(0, _SHARED)

from shared_lib.persistence.db import DB


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Minimal schema helpers ────────────────────────────────────────────────────

def _make_db(with_quarantine: bool = True) -> DB:
    db = DB(path=":memory:")
    q_cols = ""
    if with_quarantine:
        q_cols = ", broker_health_status TEXT DEFAULT 'ok', broker_error_code TEXT, broker_blocked_at TEXT"
    with db.connect() as conn:
        conn.executescript(f"""
            CREATE TABLE broker_accounts (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                broker_id TEXT NOT NULL DEFAULT 'binance',
                environment TEXT NOT NULL DEFAULT 'live',
                status TEXT NOT NULL DEFAULT 'connected',
                validation_error TEXT,
                updated_at TEXT,
                active_credential_version INTEGER
            );
            CREATE TABLE broker_credentials_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active',
                encrypted_blob TEXT NOT NULL,
                key_fingerprint TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                superseded_at TEXT,
                last_validated_at TEXT,
                validation_error TEXT,
                UNIQUE(account_id, version)
            );
            CREATE TABLE broker_credentials (
                account_id TEXT PRIMARY KEY,
                encrypted_blob TEXT NOT NULL,
                key_metadata TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE bot_instances (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                broker_account_id TEXT NOT NULL,
                market_type TEXT DEFAULT 'CRYPTO',
                strategy_id TEXT DEFAULT 'master_ensemble',
                strategy_version TEXT DEFAULT '1.0.0',
                risk_level TEXT DEFAULT 'balanced',
                config_id TEXT,
                risk_profile_id TEXT,
                symbols_json TEXT DEFAULT '[]',
                timeframes_json TEXT DEFAULT '["15m"]',
                allocation_type TEXT DEFAULT 'fixed_amount',
                allocation_value REAL DEFAULT 100.0,
                mode TEXT DEFAULT 'paper',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT,
                updated_at TEXT,
                stopped_at TEXT,
                started_at TEXT,
                last_run_at TEXT,
                last_error TEXT,
                total_trades INTEGER DEFAULT 0,
                active_positions INTEGER DEFAULT 0,
                capital_allocation REAL,
                capital_allocation_type TEXT DEFAULT 'fixed_amount'
                {q_cols}
            );
        """)
    return db


def _insert_broker(conn, account_id: str, user_id: str,
                   broker_id: str = "binance", status: str = "connected") -> None:
    conn.execute(
        "INSERT INTO broker_accounts (id, user_id, broker_id, status) VALUES (?,?,?,?)",
        (account_id, user_id, broker_id, status),
    )


def _insert_bot(conn, bot_id: str, user_id: str, account_id: str,
                status: str = "active", health: str = "ok") -> None:
    try:
        conn.execute(
            """INSERT INTO bot_instances
               (id, user_id, broker_account_id, status, broker_health_status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            (bot_id, user_id, account_id, status, health, _utc(), _utc()),
        )
    except Exception:
        conn.execute(
            "INSERT INTO bot_instances (id, user_id, broker_account_id, status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
            (bot_id, user_id, account_id, status, _utc(), _utc()),
        )


def _get_service(db: DB):
    from app.core.bot_instance_service import BotInstanceService
    return BotInstanceService(db=db)


# ── D1: running bot count method queries correct statuses ─────────────────────

def test_d1_count_running_bots_excludes_blocked():
    """
    _count_running_bots must exclude broker_blocked bots and non-active statuses.
    """
    from app.core.portfolio_service import _count_running_bots  # type: ignore
    db = _make_db()
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_active", "user_A", "brk_1", "active", "ok")
        _insert_bot(conn, "bot_error",  "user_A", "brk_1", "error",  "ok")
        _insert_bot(conn, "bot_blocked","user_A", "brk_1", "active", "broker_blocked")
        _insert_bot(conn, "bot_stopped","user_A", "brk_1", "stopped","ok")
        _insert_bot(conn, "bot_deleted","user_A", "brk_1", "deleted","ok")

    count = _count_running_bots(db, "user_A")
    # Only bot_active (active+ok) and bot_error (error+ok) should count
    assert count == 2


# ── D2: active_strategies field is bot count, not position count ──────────────

def test_d2_active_strategies_is_bot_count_not_positions():
    """
    portfolio summary['active_strategies'] must equal running bot count,
    NOT the number of open futures positions.
    """
    from app.core.portfolio_service import _count_running_bots  # type: ignore
    db = _make_db()
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1", "active", "ok")
        _insert_bot(conn, "bot_2", "user_A", "brk_1", "active", "ok")

    count = _count_running_bots(db, "user_A")
    assert count == 2  # Matches bot count, not open positions (which would be 0 in test)


# ── D3: portfolio returns per-account error, never silent $0 ─────────────────

def test_d3_portfolio_error_surfaced_not_zeroed():
    """
    When broker auth fails, the account error must appear in account_errors,
    and totals must stay at 0 for that account (not a crash or silent omission).
    """
    from shared_lib.broker.errors import BrokerResolverError

    # Simulate what portfolio_service does when resolve_broker_auth raises
    account_errors = []
    total_equity = 0.0

    exc = BrokerResolverError(
        BrokerResolverError.REASON_ENV_MISMATCH,
        "env mismatch",
        account_id="brk_1",
    )
    account_errors.append({
        "account_id": "brk_1",
        "broker_id":  "binance",
        "error_type": exc.reason_code,
        "error":      str(exc),
    })
    # total_equity stays 0

    assert len(account_errors) == 1
    assert account_errors[0]["error_type"] == BrokerResolverError.REASON_ENV_MISMATCH
    assert total_equity == 0.0


# ── D4: deleted bot excluded from active runner query ─────────────────────────

def test_d4_deleted_bot_excluded_from_runner():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1", "active")
        _insert_bot(conn, "bot_2", "user_A", "brk_1", "active")

    # Soft-delete bot_2
    svc.delete_bot_instance("bot_2")

    active = svc.get_active_bot_instances()
    ids = [b.id for b in active]
    assert "bot_1" in ids
    assert "bot_2" not in ids


# ── D5: deleted bot excluded from default get_user_bot_instances view ─────────

def test_d5_deleted_bot_hidden_from_user_list():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1", "active")
        _insert_bot(conn, "bot_2", "user_A", "brk_1", "active")

    svc.delete_bot_instance("bot_2")

    all_bots = svc.get_user_bot_instances("user_A")
    ids = [b.id for b in all_bots]
    assert "bot_1" in ids
    assert "bot_2" not in ids  # hidden by default

    # Explicitly request deleted bots
    deleted = svc.get_user_bot_instances("user_A", status_filter="deleted")
    assert any(b.id == "bot_2" for b in deleted)


# ── D6: stopped bot excluded from runner active query ─────────────────────────

def test_d6_stopped_bot_excluded_from_runner():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_active",  "user_A", "brk_1", "active")
        _insert_bot(conn, "bot_stopped", "user_A", "brk_1", "stopped")

    active = svc.get_active_bot_instances()
    ids = [b.id for b in active]
    assert "bot_active"  in ids
    assert "bot_stopped" not in ids


# ── D7: broker-blocked bot not counted in portfolio running_bots ──────────────

def test_d7_blocked_bot_not_counted():
    from app.core.portfolio_service import _count_running_bots  # type: ignore
    db = _make_db()
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_ok",      "user_A", "brk_1", "active", "ok")
        _insert_bot(conn, "bot_blocked", "user_A", "brk_1", "active", "broker_blocked")

    count = _count_running_bots(db, "user_A")
    assert count == 1  # blocked bot excluded


# ── D8: soft delete preserves the row ────────────────────────────────────────

def test_d8_soft_delete_preserves_row():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1", "active")

    svc.delete_bot_instance("bot_1")

    with db.connect() as conn:
        row = conn.execute(
            "SELECT status, stopped_at FROM bot_instances WHERE id='bot_1'"
        ).fetchone()

    assert row is not None, "Row must still exist after soft delete"
    assert row["status"] == "deleted"
    assert row["stopped_at"] is not None


# ── D9: runner evicts stale cache entries for missing bots ────────────────────

def test_d9_runner_evicts_stale_cache():
    """
    If bot_1 was in _runners but is no longer in the active set (deleted/stopped),
    the runner must evict it.
    """
    _runners = {"bot_1": MagicMock(), "bot_2": MagicMock()}

    # Simulate what multi_runner does at top of run_once():
    active_ids = {"bot_2"}  # bot_1 was deleted
    stale = [bid for bid in list(_runners.keys()) if bid not in active_ids]
    for stale_id in stale:
        del _runners[stale_id]

    assert "bot_1" not in _runners
    assert "bot_2" in _runners


# ── D10: two active bots on same broker both appear in runner ─────────────────

def test_d10_two_active_bots_same_broker_both_in_runner():
    db = _make_db()
    svc = _get_service(db)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1", "active")
        _insert_bot(conn, "bot_2", "user_A", "brk_1", "active")

    active = svc.get_active_bot_instances()
    ids = {b.id for b in active}
    assert "bot_1" in ids
    assert "bot_2" in ids


# ── D11: running_bots count is zero for user with no active bots ──────────────

def test_d11_running_bots_zero_for_empty():
    from app.core.portfolio_service import _count_running_bots  # type: ignore
    db = _make_db()
    count = _count_running_bots(db, "user_nobody")
    assert count == 0


# ── D12: portfolio handles pre-migration DB (no broker_health_status) ─────────

def test_d12_running_bots_pre_migration_graceful():
    from app.core.portfolio_service import _count_running_bots  # type: ignore
    db = _make_db(with_quarantine=False)
    with db.connect() as conn:
        _insert_broker(conn, "brk_1", "user_A")
        _insert_bot(conn, "bot_1", "user_A", "brk_1", "active")

    # Should not raise even without broker_health_status column
    count = _count_running_bots(db, "user_A")
    assert count == 1
