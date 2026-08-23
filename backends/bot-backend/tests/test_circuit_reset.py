import pytest
import sqlite3
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone
from app.risk.safety_engine import SafetyEngine
from shared_lib.persistence.db import DB

@pytest.fixture
def mock_db():
    db = MagicMock(spec=DB)
    # Create an in-memory SQLite DB for testing actual queries
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    
    # Setup schema
    conn.execute("CREATE TABLE order_failures (config_id TEXT, symbol TEXT, consecutive_failures INTEGER, last_failure_at TEXT, last_failure_reason TEXT, paused_until TEXT, PRIMARY KEY(config_id, symbol))")
    
    # Mock connect to return this connection context manager
    db.connect.return_value.__enter__.return_value = conn
    return db, conn

@pytest.fixture
def safety_engine(mock_db):
    db, conn = mock_db
    engine = SafetyEngine(db, MagicMock(), MagicMock(), MagicMock())
    return engine

def test_reset_symbol_circuit_breaker_specific(safety_engine, mock_db):
    db, conn = mock_db
    config_id = "bot_test"
    symbol = "BTCUSDT"
    
    # Setup initial tripped state
    conn.execute(
        "INSERT INTO order_failures VALUES (?, ?, ?, ?, ?, ?)",
        (config_id, symbol, 5, "2024-01-01T10:00:00", "Simulated Error", "2024-01-01T11:00:00")
    )
    
    # Call reset
    result = safety_engine.reset_symbol_circuit_breaker(config_id, symbol)
    
    # Verify DB state
    row = conn.execute("SELECT * FROM order_failures WHERE config_id = ? AND symbol = ?", (config_id, symbol)).fetchone()
    assert row["consecutive_failures"] == 0
    assert row["paused_until"] is None
    assert row["last_failure_reason"] is None
    
    # Verify return details
    key = f"{config_id}:{symbol}"
    assert key in result
    assert result[key]["old_failures"] == 5
    assert result[key]["old_paused_until"] == "2024-01-01T11:00:00"
    assert result[key]["new_failures"] == 0

def test_reset_symbol_circuit_breaker_all(safety_engine, mock_db):
    db, conn = mock_db
    config_id = "bot_test"
    
    # Setup multiple symbols
    conn.execute("INSERT INTO order_failures VALUES (?, ?, ?, ?, ?, ?)", (config_id, "BTCUSDT", 3, None, "Err", "2024-01-01T12:00:00"))
    conn.execute("INSERT INTO order_failures VALUES (?, ?, ?, ?, ?, ?)", (config_id, "ETHUSDT", 2, None, "Err2", "2024-01-01T12:00:00"))
    
    # Call reset all
    result = safety_engine.reset_symbol_circuit_breaker(config_id, None)
    
    # Verify DB state
    rows = conn.execute("SELECT * FROM order_failures WHERE config_id = ?", (config_id,)).fetchall()
    for row in rows:
        assert row["consecutive_failures"] == 0
        assert row["paused_until"] is None
        
    # Verify details
    assert len(result) == 2
    assert result[f"{config_id}:BTCUSDT"]["old_failures"] == 3
    assert result[f"{config_id}:ETHUSDT"]["old_failures"] == 2

def test_db_write_retry_logic(safety_engine):
    # Mock operation that fails with 'database is locked' then succeeds
    operation = MagicMock(side_effect=[
        sqlite3.OperationalError("database is locked"),
        sqlite3.OperationalError("database is locked"),
        "success"
    ])
    
    # Should succeed on 3rd attempt
    with patch("time.sleep") as mock_sleep: # Don't actually sleep
        result = safety_engine._db_write_with_retry(operation, max_retries=3)
        assert result == "success"
        assert operation.call_count == 3

def test_db_write_retry_fail(safety_engine):
    # Mock operation that fails permanently
    operation = MagicMock(side_effect=sqlite3.OperationalError("database is locked"))
    
    with pytest.raises(sqlite3.OperationalError):
         with patch("time.sleep"):
            safety_engine._db_write_with_retry(operation, max_retries=3)
    
    assert operation.call_count == 3
