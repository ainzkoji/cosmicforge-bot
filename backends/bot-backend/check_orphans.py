import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def check_orphans():
    db = DB()
    with db.connect() as conn:
        print("=== ORPHANED SYMBOLS AUDIT ===")
        # Count all unique bot_instance_ids in state
        cursor = conn.execute("SELECT bot_instance_id, count(*) as cnt FROM bot_symbol_state GROUP BY bot_instance_id")
        bots_in_state = cursor.fetchall()
        
        # Get active bot_instance_ids from bot_instances
        cursor = conn.execute("SELECT id FROM bot_instances")
        active_bots = [r['id'] for r in cursor.fetchall()]
        
        print(f"Active bots in 'bot_instances' table: {active_bots}")
        
        for b in bots_in_state:
            bid = b['bot_instance_id']
            if bid not in active_bots:
                print(f"⚠️ FOUND ORPHANED BOT DATA: '{bid}' has {b['cnt']} symbols in state but is NOT in 'bot_instances' table.")
                
                # Check for actual open positions for this orphaned bot
                cursor = conn.execute("SELECT symbol FROM bot_symbol_state WHERE bot_instance_id = ? AND position NOT IN ('NONE', 'flat', 'FLAT')", (bid,))
                open_pos = [r['symbol'] for r in cursor.fetchall()]
                if open_pos:
                    print(f"  🛑 ACTIVE OPEN POSITIONS for orphaned bot {bid}: {open_pos}")

if __name__ == "__main__":
    check_orphans()
