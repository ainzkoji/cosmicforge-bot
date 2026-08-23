import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def final_audit():
    db = DB()
    with db.connect() as conn:
        print("=== FINAL EQUITY AUDIT ===")
        # Use correct column names: equity, margin_used, created_at, bot_instance_id
        cursor = conn.execute("""
            SELECT equity, margin_used, created_at 
            FROM equity_snapshots 
            WHERE bot_instance_id = 'bot_8c4e186bed16' 
            ORDER BY created_at DESC LIMIT 1
        """)
        row = cursor.fetchone()
        if not row:
            print("No equity snapshots found for bot_8c4e186bed16 specifically. Checking by broker_account_id if possible.")
            # Let's find the broker_account_id first
            cursor = conn.execute("SELECT broker_account_id FROM bot_instances LIMIT 1")
            broker = cursor.fetchone()
            if broker:
                baid = broker['broker_account_id']
                cursor = conn.execute("""
                    SELECT equity, margin_used, created_at 
                    FROM equity_snapshots 
                    WHERE broker_account_id = ? 
                    ORDER BY created_at DESC LIMIT 1
                """, (baid,))
                row = cursor.fetchone()
        
        if row:
            equity = float(row['equity'])
            margin = float(row['margin_used'])
            print(f"Latest Equity: ${equity:.2f}")
            print(f"Margin Used: ${margin:.2f}")
            
            # Risk Budget Calculation
            base_slots = 5
            equity_bonus = int(equity / 1000.0)
            margin_usage = margin / equity if equity > 0 else 0
            margin_bonus = 2 if margin_usage < 0.25 else (-2 if margin_usage > 0.50 else 0)
            
            # Assume 0 drawdown penalty for now (best case)
            drawdown_penalty = 0
            
            calculated_slots = base_slots + equity_bonus + margin_bonus - drawdown_penalty
            print(f"\nCalculated Risk Budget Slots:")
            print(f"  Base Slots:     {base_slots}")
            print(f"  Equity Bonus:  +{equity_bonus} (for ${equity:.0f})")
            print(f"  Margin Bonus:  {margin_bonus:+} (usage: {margin_usage:.1%})")
            print(f"  --------------------")
            print(f"  TOTAL SLOTS:    {calculated_slots}")
            
            print(f"\nResult: If TOTAL SLOTS is {calculated_slots}, that is why only {calculated_slots} positions are open.")
        else:
            print("Could not find equity data for calculation.")

if __name__ == "__main__":
    final_audit()
