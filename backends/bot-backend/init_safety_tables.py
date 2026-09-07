"""
Initialize SafetyEngine database tables manually.
Run this once to create the monitoring tables if they don't exist.
"""
import sqlite3
from pathlib import Path

db_path = Path(__file__).parent / "data" / "bot.db"

with sqlite3.connect(db_path) as conn:
    # Trade counters
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_trade_counts (
            config_id TEXT NOT NULL,
            date TEXT NOT NULL,
            trade_count INTEGER DEFAULT 0,
            PRIMARY KEY(config_id, date)
        )
    """)
    
    # Order failure tracking (circuit breaker)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_failures (
            config_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            consecutive_failures INTEGER DEFAULT 0,
            last_failure_at TEXT,
            last_failure_reason TEXT,
            paused_until TEXT,
            PRIMARY KEY(config_id, symbol)
        )
    """)
    
    # Slippage tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS slippage_monitoring (
            config_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            avg_slippage_pct REAL DEFAULT 0.0,
            max_slippage_pct REAL DEFAULT 0.0,
            sample_count INTEGER DEFAULT 0,
            last_updated TEXT,
            PRIMARY KEY(config_id, symbol)
        )
    """)
    
    # Daily activity tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_activity_tracking (
            config_id TEXT PRIMARY KEY,
            last_trade_timestamp TEXT,
            last_fallback_trade_timestamp TEXT,
            total_trades INTEGER DEFAULT 0,
            total_fallback_trades INTEGER DEFAULT 0
        )
    """)
    
    conn.commit()
    print("✅ Created all SafetyEngine monitoring tables!")
    print("✅ Circuit breaker system is now active!")
