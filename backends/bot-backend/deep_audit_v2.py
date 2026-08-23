import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def audit():
    db = DB()
    with db.connect() as conn:
        print("=== ORDER FAILURES (Audit) ===")
        cursor = conn.execute("SELECT symbol, last_failure_reason, last_failure_at FROM order_failures WHERE config_id = 'bot_8c4e186bed16'")
        for r in cursor.fetchall():
            print(dict(r))
            
        print("\n=== RECENT DECISIONS (FULL) ===")
        # Search for any recent 'hold' or 'error' in bot_8c4e186bed16
        cursor = conn.execute("""
            SELECT created_at, symbol, final_action, risk_gate_decision_json, sizing_decision_json, execution_result_json 
            FROM decision_logs 
            WHERE config_id = 'bot_8c4e186bed16' 
            ORDER BY created_at DESC LIMIT 50
        """)
        for r in cursor.fetchall():
            d = dict(r)
            if d['final_action'] == 'hold':
                # Only print interesting ones
                print(f"Time: {d['created_at']}, Symbol: {d['symbol']}, Action: {d['final_action']}")
                print(f"  Gate: {d['risk_gate_decision_json']}")
                print(f"  Sizing: {d['sizing_decision_json']}")
                print(f"  Result: {d['execution_result_json']}")

if __name__ == "__main__":
    audit()
