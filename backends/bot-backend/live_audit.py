import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def live_audit():
    db = DB()
    with db.connect() as conn:
        print("=== LIVE DECISION LOGS (LAST 20) ===")
        cursor = conn.execute("""
            SELECT created_at, symbol, final_action, risk_gate_decision_json, sizing_decision_json, execution_result_json 
            FROM decision_logs 
            WHERE config_id = 'bot_8c4e186bed16' 
            ORDER BY created_at DESC LIMIT 20
        """)
        for r in cursor.fetchall():
            d = dict(r)
            time = d['created_at']
            sym = d['symbol']
            action = d['final_action']
            
            # Extract gating reason
            gate = json.loads(d['risk_gate_decision_json']) if d['risk_gate_decision_json'] else {}
            sizing = json.loads(d['sizing_decision_json']) if d['sizing_decision_json'] else {}
            exec_res = json.loads(d['execution_result_json']) if d['execution_result_json'] else {}
            
            reason = "OK"
            if action == 'hold':
                # Find why it's a hold
                if 'gate_reason' in gate:
                    reason = gate['gate_reason']
                elif 'error' in sizing:
                    reason = sizing['error']
                elif 'error' in exec_res:
                    reason = exec_res['error']
                elif 'reason' in sizing:
                    reason = sizing['reason']
                    
            print(f"Time: {time} | Sym: {sym:8} | Action: {action:5} | Reason: {reason}")
            # If reason is LOW_CONFIDENCE, print confidence
            # if 'confidence' in d: # Not in this table?
            #    pass

if __name__ == "__main__":
    live_audit()
