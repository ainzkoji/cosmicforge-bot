"""List all tables in the database"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from shared_lib.persistence.db import DB

db = DB()
with db.connect() as conn:
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
print("\n📊 DATABASE TABLES:\n")
for table in tables:
    print(f"  - {table[0]}")
    
    # Get row count
    try:
        with db.connect() as conn:
            count_cursor = conn.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = count_cursor.fetchone()[0]
            print(f"    ({count} rows)")
    except:
        pass

print("\n" + "=" * 80)
