"""
Enhanced Admin Monitoring Migration
- Creates comprehensive tracking tables
- Enables full system monitoring
"""
import sqlite3
from pathlib import Path
import uuid
from datetime import datetime

DB_PATH = Path("data/bot.db")

def utc_now_iso():
    return datetime.utcnow().isoformat() + "Z"

def migrate():
    print(f"Migrating enhanced monitoring tables at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. System Metrics Table
    print("Creating system_metrics table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_metrics (
            id TEXT PRIMARY KEY,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            metric_unit TEXT,
            recorded_at TEXT NOT NULL,
            metadata TEXT
        )
    """)

    # 2. Bot Executions Table (Enhanced Trade Tracking)
    print("Creating bot_executions table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bot_executions (
            id TEXT PRIMARY KEY,
            bot_id TEXT NOT NULL,
            strategy_id TEXT,
            action TEXT NOT NULL,
            symbol TEXT,
            quantity REAL,
            price REAL,
            pnl REAL,
            execution_time_ms INTEGER,
            status TEXT NOT NULL,
            error_message TEXT,
            executed_at TEXT NOT NULL
        )
    """)

    # 3. Transactions Table
    print("Creating transactions table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            type TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT DEFAULT 'USD',
            status TEXT DEFAULT 'pending',
            payment_method TEXT,
            reference_id TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    # 4. User Sessions Table (Enhanced Session Tracking)
    print("Creating user_sessions table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            ip_address TEXT,
            user_agent TEXT,
            device_type TEXT,
            location TEXT,
            started_at TEXT NOT NULL,
            last_activity_at TEXT,
            ended_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    # 5. API Requests Table (API Usage Tracking)
    print("Creating api_requests table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_requests (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            endpoint TEXT NOT NULL,
            method TEXT NOT NULL,
            status_code INTEGER,
            response_time_ms INTEGER,
            ip_address TEXT,
            user_agent TEXT,
            created_at TEXT NOT NULL
        )
    """)

    # 6. Feature Flags Table
    print("Creating feature_flags table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS feature_flags (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            is_enabled BOOLEAN DEFAULT 0,
            rollout_percentage INTEGER DEFAULT 0,
            enabled_for_users TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # 7. Activity Events Table (Real-time Activity Feed)
    print("Creating activity_events table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS activity_events (
            id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            event_category TEXT NOT NULL,
            user_id TEXT,
            bot_id TEXT,
            description TEXT NOT NULL,
            severity TEXT DEFAULT 'info',
            metadata TEXT,
            created_at TEXT NOT NULL
        )
    """)

    # Seed some sample system metrics
    print("Seeding sample system metrics...")
    metrics = [
        ("cpu_usage", 45.2, "%"),
        ("memory_usage", 62.8, "%"),
        ("disk_usage", 38.5, "%"),
        ("api_response_time_p50", 125, "ms"),
        ("api_response_time_p95", 380, "ms"),
        ("error_rate", 0.8, "%"),
    ]
    
    for name, value, unit in metrics:
        cursor.execute("""
            INSERT OR IGNORE INTO system_metrics (id, metric_name, metric_value, metric_unit, recorded_at, metadata)
            VALUES (?, ?, ?, ?, ?, NULL)
        """, (str(uuid.uuid4()), name, value, unit, utc_now_iso()))
    print(f"  Added {len(metrics)} sample metrics")

    # Seed feature flags
    print("Seeding feature flags...")
    flags = [
        ("advanced_charts", "Enable advanced charting features", 1, 100),
        ("social_trading", "Enable social trading functionality", 1, 100),
        ("api_v2", "Enable API v2 endpoints", 0, 0),
        ("dark_mode", "Enable dark mode UI", 1, 100),
    ]
    
    for name, desc, enabled, rollout in flags:
        flag_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT OR IGNORE INTO feature_flags (id, name, description, is_enabled, rollout_percentage, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (flag_id, name, desc, enabled, rollout, utc_now_iso(), utc_now_iso()))
    print(f"  Added {len(flags)} feature flags")

    # Create indexes for performance
    print("Creating indexes...")
    indexes = [
        ("idx_bot_executions_bot_id", "bot_executions", "bot_id"),
        ("idx_bot_executions_executed_at", "bot_executions", "executed_at"),
        ("idx_transactions_user_id", "transactions", "user_id"),
        ("idx_transactions_created_at", "transactions", "created_at"),
        ("idx_user_sessions_user_id", "user_sessions", "user_id"),
        ("idx_api_requests_endpoint", "api_requests", "endpoint"),
        ("idx_api_requests_created_at", "api_requests", "created_at"),
        ("idx_activity_events_created_at", "activity_events", "created_at"),
        ("idx_activity_events_event_type", "activity_events", "event_type"),
    ]
    
    for idx_name, table, column in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")
            print(f"  Created index: {idx_name}")
        except Exception as e:
            print(f"  Skipped {idx_name}: {e}")

    conn.commit()
    conn.close()
    print("Enhanced monitoring migration complete! ✅")

if __name__ == "__main__":
    migrate()
