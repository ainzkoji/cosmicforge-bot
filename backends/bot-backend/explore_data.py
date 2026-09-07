import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def explore_data():
    db = DB()
    with db.connect() as conn:
        print("=== BOT INSTANCES CONTENT ===")
        cursor = conn.execute("SELECT * FROM bot_instances")
        for r in cursor.fetchall():
            print(dict(r))
            
        print("\n=== BOT SYMBOL STATE (COUNT BY BOT) ===")
        cursor = conn.execute("SELECT bot_instance_id, count(*) as cnt FROM bot_symbol_state WHERE position NOT IN ('NONE', 'flat', 'FLAT') GROUP BY bot_instance_id")
        for r in cursor.fetchall():
            print(dict(r))

        print("\n=== RECENT DECISION LOGS FOR MAX POSITIONS ===")
        # Search for ReasonCode.MAX_POSITIONS_REACHED or similar
        cursor = conn.execute("""
            SELECT created_at, symbol, final_action, execution_result_json 
            FROM decision_logs 
            WHERE final_action = 'hold' 
            ORDER BY created_at DESC LIMIT 50
        """)
        for r in cursor.fetchall():
            d = dict(r)
            if d['execution_result_json'] and 'MAX_POSITIONS_REACHED' in d['execution_result_json']:
                print(f"Time: {d['created_at']}, Symbol: {d['symbol']}, Action: {d['final_action']}, Result: {d['execution_result_json']}")
            elif d['risk_gate_decision_json'] and 'MAX_POSITIONS_REACHED' in d['risk_gate_decision_json']:
                print(f"Time: {d['created_at']}, Symbol: {d['symbol']}, Action: {d['final_action']}, Gate: {d['risk_gate_decision_json']}")

if __name__ == "__main__":
    explore_data()
