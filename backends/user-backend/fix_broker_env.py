import sqlite3
import os

db_path = '../shared/shared_lib/persistence/cosmicforge.db'

if not os.path.exists(db_path):
    print(f"Error: {db_path} not found.")
    exit(1)

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Check current state - Schema has 'label', not 'name'
    cursor.execute("SELECT id, label, environment FROM broker_accounts WHERE broker_id='binance'")
    row = cursor.fetchone()
    
    if not row:
        print("No binance account found.")
    else:
        print(f"Current State: ID={row[0]}, Label={row[1]}, Env={row[2]}")
        
        # 2. Update to live
        cursor.execute("UPDATE broker_accounts SET environment='live' WHERE id=?", (row[0],))
        conn.commit()
        print("✅ Updated environment to 'live'.")
        
        # 3. Verify
        cursor.execute("SELECT environment FROM broker_accounts WHERE id=?", (row[0],))
        new_env = cursor.fetchone()[0]
        print(f"New State: Env={new_env}")

    conn.close()

except Exception as e:
    print(f"Database error: {e}")
