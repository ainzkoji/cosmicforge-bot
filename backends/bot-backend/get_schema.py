import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def get_schema():
    db = DB()
    with db.connect() as conn:
        for table in ['bot_instances', 'bot_symbol_state', 'decision_logs', 'order_failures', 'equity_snapshots']:
            print(f"--- Schema for {table} ---")
            cursor = conn.execute(f"PRAGMA table_info({table})")
            for row in cursor.fetchall():
                print(dict(row))
            print("\n")

if __name__ == "__main__":
    get_schema()
