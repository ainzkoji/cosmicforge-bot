import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from shared_lib.persistence.db import DB

db = DB()
with db.connect() as conn:
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    print("Tables in database:")
    for table in tables:
        print(f"  - {table}")
        
    # Check if strategies table has data
    if 'strategies' in tables:
        cursor = conn.execute("SELECT COUNT(*) FROM strategies")
        count = cursor.fetchone()[0]
        print(f"\nstrategies table: {count} rows")
