"""
Quick script to create the trades table in the database.
This table is required by AnalyticsService but isn't created by the main DB._init() method.
"""

import sys
from pathlib import Path

# Add shared package to path
shared_path = Path(__file__).parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from shared_lib.persistence.db import DB

def create_trades_table():
    """Create the trades table if it doesn't exist."""
    db = DB()
    print(f"Using database: {db.path}")
    
    with db.connect() as conn:
        # Create trades table (from AnalyticsService._ensure_tables)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                run_id TEXT,
                symbol TEXT,
                side TEXT,
                strategy TEXT,
                mode TEXT,
                timeframe TEXT,
                entry_time TEXT,
                entry_price REAL,
                entry_qty REAL,
                entry_confidence REAL,
                exit_time TEXT,
                exit_price REAL,
                exit_reason TEXT,
                realized_pnl REAL DEFAULT 0,
                fees REAL DEFAULT 0,
                r_multiple REAL,
                initial_stop REAL,
                tp1_hit INTEGER DEFAULT 0,
                tp1_time TEXT,
                add_count INTEGER DEFAULT 0,
                status TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Verify the table was created
        cursor = conn.execute("SELECT count(*) FROM trades")
        count = cursor.fetchone()[0]
        print(f"✅ trades table created/verified successfully with {count} rows.")
        
        # Also create indexes for better performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_exit_time ON trades(exit_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
        print("✅ Indexes created.")

if __name__ == "__main__":
    try:
        create_trades_table()
        print("\n✅ All done! Your analytics endpoint should work now.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
