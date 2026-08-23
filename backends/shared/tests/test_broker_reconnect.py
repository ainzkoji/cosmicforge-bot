"""
Broker reconnect + resolver test suite — SEV-1 regression coverage.

Tests:
  T1.  resolve_broker_auth: owner mismatch → REASON_ACCESS_DENIED
  T2.  resolve_broker_auth: account not found → REASON_NOT_FOUND
  T3.  resolve_broker_auth: status=invalid → REASON_INVALID
  T4.  resolve_broker_auth: status=connected + mismatched blob env → REASON_ENV_MISMATCH
  T5.  resolve_broker_auth: decrypt failure → REASON_DECRYPT_FAILED
  T6.  resolve_broker_auth: happy path → BrokerAuth fields correct
  T7.  resolve_broker_auth: credential version pointer respected (v1 superseded, v2 active)
  T8.  BrokerHealthCache: mark_unhealthy → get returns unhealthy; mark_healthy → get returns healthy
  T9.  BrokerHealthCache: expired entry → get returns None
  T10. reconnect_broker_account: validate succeeds → active_credential_version incremented
  T11. reconnect_broker_account: validate fails → account status=invalid, old creds intact
  T12. environment mismatch in submitted credentials → ValueError raised before encrypt
  T13. normalize_environment: all known aliases round-trip
  T14. resolve_base_url: unknown broker → raises ValueError
  T15. BrokerHealthCache: invalidate(account_id) clears all versions
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# ── Path setup ────────────────────────────────────────────────────────────────
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(_ROOT, "shared"))
sys.path.insert(0, os.path.join(_ROOT, "user-backend"))

from shared_lib.broker.errors import BrokerResolverError
from shared_lib.broker.environment import (
    BrokerEnvironment,
    normalize_environment,
    resolve_base_url,
)
from shared_lib.broker.health_cache import BrokerHealthCache
from shared_lib.broker.resolver import resolve_broker_auth, BrokerAuth
from shared_lib.persistence.db import DB


# ── Helpers ───────────────────────────────────────────────────────────────────

def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_db() -> DB:
    """Create an in-memory SQLite DB with the broker tables needed for tests."""
    db = DB(path=":memory:")
    with db.connect() as conn:
        conn.executescript("""
            CREATE TABLE broker_accounts (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                broker_id TEXT NOT NULL,
                environment TEXT NOT NULL DEFAULT 'live',
                status TEXT NOT NULL DEFAULT 'connected',
                active_credential_version INTEGER,
                last_error_message TEXT
            );

            CREATE TABLE broker_credentials_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active',
                encrypted_blob TEXT NOT NULL,
                key_metadata TEXT,
                key_fingerprint TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                superseded_at TEXT,
                last_validated_at TEXT,
                validation_error TEXT,
                UNIQUE (account_id, version)
            );

            -- Legacy table — present in real DB, not used by resolver v2
            CREATE TABLE broker_credentials (
                account_id TEXT PRIMARY KEY,
                encrypted_blob TEXT NOT NULL,
                key_metadata TEXT,
                updated_at TEXT NOT NULL
            );
        """)
    return db


def _insert_account(conn, account_id: str, user_id: str, broker_id: str = "binance",
                    environment: str = "live", status: str = "connected",
                    active_version: int | None = 1) -> None:
    conn.execute(
        "INSERT INTO broker_accounts (id, user_id, broker_id, environment, status, active_credential_version) VALUES (?,?,?,?,?,?)",
        (account_id, user_id, broker_id, environment, status, active_version),
    )


def _make_blob(api_key: str = "key_abc123", api_secret: str = "sec_xyz456",
               extra_env: str | None = None) -> str:
    """Return a Fernet-encrypted blob containing test credentials."""
    from shared_lib.core.security.broker_security import encrypt_credentials
    creds: dict = {"api_key": api_key, "api_secret": api_secret}
    if extra_env:
        creds["environment"] = extra_env
    return encrypt_credentials(creds)


def _insert_cred_v2(conn, account_id: str, version: int = 1, status: str = "active",
                    blob: str | None = None) -> None:
    if blob is None:
        blob = _make_blob()
    now = _utc()
    conn.execute(
        """INSERT INTO broker_credentials_v2
           (account_id, version, status, encrypted_blob, key_fingerprint, created_at, updated_at)
           VALUES (?, ?, ?, ?, '...abc1', ?, ?)""",
        (account_id, version, status, blob, now, now),
    )


# ── T1: owner mismatch → REASON_ACCESS_DENIED ─────────────────────────────────

def test_t1_owner_mismatch():
    db = _make_db()
    with db.connect() as conn:
        _insert_account(conn, "brk_1", "user_A")
        _insert_cred_v2(conn, "brk_1")

    with pytest.raises(BrokerResolverError) as exc_info:
        resolve_broker_auth("brk_1", "user_B", db)
    assert exc_info.value.reason_code == BrokerResolverError.REASON_ACCESS_DENIED


# ── T2: account not found → REASON_NOT_FOUND ──────────────────────────────────

def test_t2_account_not_found():
    db = _make_db()
    with pytest.raises(BrokerResolverError) as exc_info:
        resolve_broker_auth("brk_nonexistent", "user_A", db)
    assert exc_info.value.reason_code == BrokerResolverError.REASON_NOT_FOUND


# ── T3: status=invalid → REASON_INVALID ───────────────────────────────────────

def test_t3_invalid_status():
    db = _make_db()
    with db.connect() as conn:
        _insert_account(conn, "brk_3", "user_A", status="invalid")
        _insert_cred_v2(conn, "brk_3")

    with pytest.raises(BrokerResolverError) as exc_info:
        resolve_broker_auth("brk_3", "user_A", db)
    assert exc_info.value.reason_code == BrokerResolverError.REASON_INVALID


# ── T4: env mismatch → REASON_ENV_MISMATCH ────────────────────────────────────

def test_t4_env_mismatch():
    db = _make_db()
    # Account says "live", blob says "demo"
    bad_blob = _make_blob(extra_env="demo")
    with db.connect() as conn:
        _insert_account(conn, "brk_4", "user_A", environment="live")
        _insert_cred_v2(conn, "brk_4", blob=bad_blob)

    with pytest.raises(BrokerResolverError) as exc_info:
        resolve_broker_auth("brk_4", "user_A", db)
    assert exc_info.value.reason_code == BrokerResolverError.REASON_ENV_MISMATCH


# ── T5: decrypt failure → REASON_DECRYPT_FAILED ───────────────────────────────

def test_t5_decrypt_failed():
    db = _make_db()
    with db.connect() as conn:
        _insert_account(conn, "brk_5", "user_A")
        # Insert garbage blob that can't be decrypted
        now = _utc()
        conn.execute(
            "INSERT INTO broker_credentials_v2 (account_id, version, status, encrypted_blob, created_at, updated_at) VALUES (?,1,'active','garbage_blob_not_fernet',?,?)",
            ("brk_5", now, now),
        )

    with pytest.raises(BrokerResolverError) as exc_info:
        resolve_broker_auth("brk_5", "user_A", db)
    assert exc_info.value.reason_code == BrokerResolverError.REASON_DECRYPT_FAILED


# ── T6: happy path → correct BrokerAuth ───────────────────────────────────────

def test_t6_happy_path():
    db = _make_db()
    with db.connect() as conn:
        _insert_account(conn, "brk_6", "user_A", environment="demo")
        _insert_cred_v2(conn, "brk_6", blob=_make_blob("testkey_1234", "testsecret_5678"))

    auth = resolve_broker_auth("brk_6", "user_A", db)

    assert isinstance(auth, BrokerAuth)
    assert auth.account_id == "brk_6"
    assert auth.user_id == "user_A"
    assert auth.broker_type == "binance"
    assert auth.environment == BrokerEnvironment.DEMO
    assert auth.base_url == "https://demo-fapi.binance.com"
    assert auth.api_key == "testkey_1234"
    assert auth.api_secret == "testsecret_5678"
    assert auth.credential_version == 1
    assert auth.key_fingerprint == "...1234"


# ── T7: version pointer respected ─────────────────────────────────────────────

def test_t7_version_pointer():
    """Resolver should use active_credential_version=2, not the superseded version 1."""
    db = _make_db()
    v1_blob = _make_blob("oldkey_0000", "oldsec_0000")
    v2_blob = _make_blob("newkey_9999", "newsec_9999")
    with db.connect() as conn:
        _insert_account(conn, "brk_7", "user_A", active_version=2)
        _insert_cred_v2(conn, "brk_7", version=1, status="superseded", blob=v1_blob)
        _insert_cred_v2(conn, "brk_7", version=2, status="active",    blob=v2_blob)

    auth = resolve_broker_auth("brk_7", "user_A", db)
    assert auth.api_key == "newkey_9999"
    assert auth.credential_version == 2


# ── T8: BrokerHealthCache mark/get ────────────────────────────────────────────

def test_t8_health_cache_basic():
    cache = BrokerHealthCache(healthy_ttl=300, unhealthy_ttl=300)

    cache.mark_healthy("brk_x", 1)
    entry = cache.get("brk_x", 1)
    assert entry is not None
    assert entry.is_healthy is True

    cache.mark_unhealthy("brk_x", 2, BrokerResolverError.REASON_AUTH_FAILED, "401")
    entry2 = cache.get("brk_x", 2)
    assert entry2 is not None
    assert entry2.is_healthy is False
    assert entry2.reason_code == BrokerResolverError.REASON_AUTH_FAILED

    # Different version — not affected
    assert cache.get("brk_x", 1).is_healthy is True


# ── T9: BrokerHealthCache expiry ──────────────────────────────────────────────

def test_t9_health_cache_expiry():
    cache = BrokerHealthCache(healthy_ttl=0.05, unhealthy_ttl=0.05)
    cache.mark_healthy("brk_exp", 1)
    time.sleep(0.1)
    assert cache.get("brk_exp", 1) is None


# ── T10: reconnect happy path → version incremented ───────────────────────────

def test_t10_reconnect_success():
    """validate_and_activate_credential_v2 promotes new version atomically."""
    db = _make_db()
    with db.connect() as conn:
        _insert_account(conn, "brk_10", "user_A", environment="live", active_version=1)
        _insert_cred_v2(conn, "brk_10", version=1, status="active",     blob=_make_blob("oldkey", "oldsec"))
        _insert_cred_v2(conn, "brk_10", version=2, status="validating", blob=_make_blob("newkey", "newsec"))

    # Mock the exchange test so we don't hit real Binance
    sys.path.insert(0, os.path.join(_ROOT, "user-backend"))
    import app.core.broker_service as bsvc

    original_test = bsvc._test_broker_connection

    def mock_test(broker_id, credentials, environment):
        return {"success": True, "capabilities": ["read", "trade"]}

    bsvc._test_broker_connection = mock_test

    try:
        from app.core.broker_service import validate_and_activate_credential_v2

        # Must use the same db path — inject a custom get_db
        def _mock_get_db():
            return db
        bsvc.get_db = _mock_get_db

        result = validate_and_activate_credential_v2("user_A", "brk_10", version=2)
        assert result["success"] is True
        assert result["version"] == 2
        assert result["status"] == "connected"

        # Verify DB state
        with db.connect() as conn:
            acct = conn.execute("SELECT active_credential_version, status FROM broker_accounts WHERE id='brk_10'").fetchone()
            assert acct["active_credential_version"] == 2
            assert acct["status"] == "connected"

            v1 = conn.execute("SELECT status FROM broker_credentials_v2 WHERE account_id='brk_10' AND version=1").fetchone()
            assert v1["status"] == "superseded"

            v2 = conn.execute("SELECT status FROM broker_credentials_v2 WHERE account_id='brk_10' AND version=2").fetchone()
            assert v2["status"] == "active"
    finally:
        bsvc._test_broker_connection = original_test


# ── T11: reconnect failure → account=invalid, old creds intact ─────────────────

def test_t11_reconnect_failure():
    db = _make_db()
    with db.connect() as conn:
        _insert_account(conn, "brk_11", "user_A", environment="live", active_version=1)
        _insert_cred_v2(conn, "brk_11", version=1, status="active",     blob=_make_blob("goodkey", "goodsec"))
        _insert_cred_v2(conn, "brk_11", version=2, status="validating", blob=_make_blob("badkey",  "badsec"))

    sys.path.insert(0, os.path.join(_ROOT, "user-backend"))
    import app.core.broker_service as bsvc

    original_test = bsvc._test_broker_connection

    def mock_fail(broker_id, credentials, environment):
        return {"success": False, "error": "Invalid API key"}

    bsvc._test_broker_connection = mock_fail

    def _mock_get_db():
        return db
    bsvc.get_db = _mock_get_db

    try:
        from app.core.broker_service import validate_and_activate_credential_v2
        result = validate_and_activate_credential_v2("user_A", "brk_11", version=2)
        assert result["success"] is False
        assert result["status"] == "invalid"

        # Old version must still be active
        with db.connect() as conn:
            v1 = conn.execute("SELECT status FROM broker_credentials_v2 WHERE account_id='brk_11' AND version=1").fetchone()
            assert v1["status"] == "active"

            acct = conn.execute("SELECT active_credential_version FROM broker_accounts WHERE id='brk_11'").fetchone()
            assert acct["active_credential_version"] == 1  # Not changed
    finally:
        bsvc._test_broker_connection = original_test


# ── T12: env mismatch on submit → ValueError ──────────────────────────────────

def test_t12_env_mismatch_on_submit():
    db = _make_db()
    with db.connect() as conn:
        _insert_account(conn, "brk_12", "user_A", environment="live")
        _insert_cred_v2(conn, "brk_12")

    sys.path.insert(0, os.path.join(_ROOT, "user-backend"))
    import app.core.broker_service as bsvc

    def _mock_get_db():
        return db
    bsvc.get_db = _mock_get_db

    from app.core.broker_service import submit_broker_credentials_v2
    with pytest.raises(ValueError, match="Environment mismatch"):
        submit_broker_credentials_v2(
            user_id="user_A",
            account_id="brk_12",
            credentials={"api_key": "key", "api_secret": "sec", "environment": "demo"},
        )


# ── T13: normalize_environment aliases ────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("live",       BrokerEnvironment.LIVE),
    ("mainnet",    BrokerEnvironment.LIVE),
    ("production", BrokerEnvironment.LIVE),
    ("demo",       BrokerEnvironment.DEMO),
    ("testnet",    BrokerEnvironment.DEMO),
    ("sandbox",    BrokerEnvironment.DEMO),
    ("paper",      BrokerEnvironment.DEMO),
    ("test",       BrokerEnvironment.DEMO),
    ("LIVE",       BrokerEnvironment.LIVE),
    ("DEMO",       BrokerEnvironment.DEMO),
])
def test_t13_normalize_environment(raw, expected):
    assert normalize_environment(raw) == expected


def test_t13_normalize_unrecognized():
    with pytest.raises(ValueError):
        normalize_environment("unknown_env")


# ── T14: resolve_base_url unknown broker ──────────────────────────────────────

def test_t14_resolve_base_url_unknown():
    with pytest.raises(ValueError):
        resolve_base_url("unknown_broker", BrokerEnvironment.LIVE)


def test_t14_resolve_base_url_known():
    url = resolve_base_url("binance", BrokerEnvironment.DEMO)
    assert url == "https://demo-fapi.binance.com"

    url = resolve_base_url("binance", BrokerEnvironment.LIVE)
    assert url == "https://fapi.binance.com"


# ── T15: BrokerHealthCache.invalidate clears all versions ─────────────────────

def test_t15_health_cache_invalidate():
    cache = BrokerHealthCache()
    cache.mark_unhealthy("brk_y", 1, "reason_1", "err1")
    cache.mark_unhealthy("brk_y", 2, "reason_2", "err2")
    cache.mark_healthy("brk_z", 1)

    evicted = cache.invalidate("brk_y")
    assert evicted == 2
    assert cache.get("brk_y", 1) is None
    assert cache.get("brk_y", 2) is None
    # Other account unaffected
    assert cache.get("brk_z", 1) is not None
