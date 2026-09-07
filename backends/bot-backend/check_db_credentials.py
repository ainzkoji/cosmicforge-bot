"""Check if broker credentials are stored in database"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

db = DB()
with db.connect() as conn:
    # List all tables
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [t[0] for t in cursor.fetchall()]
    
    print("Looking for broker/credential tables...\n")
    cred_tables = [t for t in tables if 'broker' in t.lower() or 'api' in t.lower() or 'cred' in t.lower() or 'key' in t.lower()]
    
    if cred_tables:
        print(f"Found tables: {cred_tables}\n")
        for table in cred_tables:
            cursor = conn.execute(f"SELECT * FROM {table} LIMIT 5")
            rows = cursor.fetchall()
            print(f"\n{table}: {len(rows)} rows")
            if rows:
                # Get column names
                cursor2 = conn.execute(f"PRAGMA table_info({table})")
                cols = [c[1] for c in cursor2.fetchall()]
                print(f"Columns: {cols}")
                for row in rows[:2]:
                    print(f"  {row}")
    else:
        print("No credential tables found in database")
        print("\nChecking bot_instances table for broker info...")
        cursor = conn.execute("SELECT * FROM bot_instances LIMIT 1")
        rows = cursor.fetchall()
        if rows:
            cursor2 = conn.execute("PRAGMA table_info(bot_instances)")
            cols = [c[1] for c in cursor2.fetchall()]
            print(f"Columns: {cols}")
            print(f"Row: {rows[0]}")
