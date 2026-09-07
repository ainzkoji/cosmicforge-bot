import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def check_equity():
    db = DB()
    with db.connect() as conn:
        print("=== EQUITY AUDIT ===")
        # Look for bot_8c4e186bed16 equity
        cursor = conn.execute("SELECT equity, timestamp FROM equity_snapshots WHERE bot_instance_id = 'bot_8c4e186bed16' ORDER BY timestamp DESC LIMIT 5")
        for r in cursor.fetchall():
            print(dict(r))
            
        print("\nChecking for any bots with 10 positions:")
        cursor = conn.execute("SELECT bot_instance_id, count(*) as cnt FROM bot_symbol_state WHERE position NOT IN ('NONE', 'flat', 'FLAT') GROUP BY bot_instance_id")
        for r in cursor.fetchall():
            print(dict(r))

if __name__ == "__main__":
    check_equity()
