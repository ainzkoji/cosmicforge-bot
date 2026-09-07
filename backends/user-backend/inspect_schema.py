import sqlite3
import os

db_path = '../shared/shared_lib/persistence/cosmicforge.db'

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"--- Schema for broker_accounts in {db_path} ---")
    cursor.execute("PRAGMA table_info(broker_accounts)")
    columns = cursor.fetchall()
    
    if not columns:
         print("Table broker_accounts not found.")
    else:
        for col in columns:
            print(col) # cid, name, type, notnull, dflt_value, pk

    conn.close()

except Exception as e:
    print(f"Database error: {e}")
