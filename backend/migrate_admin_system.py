"""
Admin System Migration
- Creates admin tables
- Adds commission tiers
- Creates revenue tracking tables
- Seeds initial data
"""
import sqlite3
from pathlib import Path
import uuid
from datetime import datetime

DB_PATH = Path("data/bot.db")

def utc_now_iso():
    return datetime.utcnow().isoformat() + "Z"

def migrate():
    print(f"Migrating admin database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Admin Roles Table
    print("Creating admin_roles table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_roles (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            role TEXT NOT NULL,
            granted_by TEXT,
            granted_at TEXT NOT NULL,
            revoked_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    # 2. Commission Tiers Table
    print("Creating commission_tiers table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commission_tiers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            min_volume REAL NOT NULL,
            max_volume REAL,
            rate REAL NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # 3. Revenue Snapshots Table
    print("Creating revenue_snapshots table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS revenue_snapshots (
            id TEXT PRIMARY KEY,
            date TEXT NOT NULL,
            subscription_revenue REAL NOT NULL,
            commission_revenue REAL NOT NULL,
            other_revenue REAL DEFAULT 0,
            total_revenue REAL NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # 4. KYC Submissions Table
    print("Creating kyc_submissions table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kyc_submissions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            document_type TEXT NOT NULL,
            document_url TEXT NOT NULL,
            risk_level TEXT DEFAULT 'medium',
            status TEXT DEFAULT 'pending',
            reviewed_by TEXT,
            reviewed_at TEXT,
            submitted_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    # 5. AML Alerts Table
    print("Creating aml_alerts table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aml_alerts (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            description TEXT,
            status TEXT DEFAULT 'open',
            investigated_by TEXT,
            resolved_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    # 6. Update users table with admin tracking columns
    print("Adding tracking columns to users table...")
    new_columns = [
        ("last_login_at", "TEXT"),
        ("login_count", "INTEGER DEFAULT 0"),
        ("total_trades", "INTEGER DEFAULT 0"),
        ("total_commission", "REAL DEFAULT 0"),
    ]
    
    for col_name, col_def in new_columns:
        try:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
            print(f"  Added {col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"  {col_name} already exists")
            else:
                raise

    # Seed commission tiers
    print("Seeding commission tiers...")
    tiers = [
        ("Tier 1 (Free)", 0, 10000, 0.005),
        ("Tier 2 (Pro)", 10000, 100000, 0.0035),
        ("Tier 3 (Enterprise)", 100000, None, 0.002),
        ("VIP Tier", 1000000, None, 0.001),
    ]
    
    for name, min_vol, max_vol, rate in tiers:
        tier_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT OR IGNORE INTO commission_tiers (id, name, min_volume, max_volume, rate, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        """, (tier_id, name, min_vol, max_vol, rate, utc_now_iso(), utc_now_iso()))
        print(f"  Added tier: {name}")

    # Seed sample revenue data (last 12 months)
    print("Seeding sample revenue data...")
    from datetime import datetime, timedelta
    for i in range(12):
        date = (datetime.now() - timedelta(days=30 * i)).strftime("%Y-%m")
        sub_revenue = 800000 + (i * 10000)
        comm_revenue = 300000 + (i * 5000)
        total = sub_revenue + comm_revenue
        
        snapshot_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT OR IGNORE INTO revenue_snapshots (id, date, subscription_revenue, commission_revenue, other_revenue, total_revenue, created_at)
            VALUES (?, ?, ?, ?, 0, ?, ?)
        """, (snapshot_id, date, sub_revenue, comm_revenue, total, utc_now_iso()))
    print("  Added 12 months of revenue data")

    conn.commit()
    conn.close()
    print("Admin migration complete! ✅")

if __name__ == "__main__":
    migrate()
