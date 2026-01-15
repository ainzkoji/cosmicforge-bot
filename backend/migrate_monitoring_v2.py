import sqlite3
import os

DB_PATH = "data/bot.db"

def migrate():
    print(f"Checking {DB_PATH} for Decision Traces v2 updates...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        c.execute("PRAGMA table_info(decision_traces)")
        columns = [row[1] for row in c.fetchall()]
        print(f"Existing columns: {columns}")
        
        new_cols = {
            "regime_state": "TEXT",
            "regime_confidence": "REAL",
            "exposure_freeze": "INTEGER", # boolean 0/1
            "kill_switch_state": "TEXT",
            "portfolio_risk_budget": "REAL",
            "portfolio_risk_used": "REAL",
            "reason_codes": "TEXT", # JSON or comma-separated
            "start_time_ms": "INTEGER", # Ensure these exist too
            "end_time_ms": "INTEGER"
        }
        
        for col, dtype in new_cols.items():
            if col not in columns:
                print(f"Adding column {col} ({dtype})...")
                conn.execute(f"ALTER TABLE decision_traces ADD COLUMN {col} {dtype}")
            else:
                print(f"Column {col} already exists.")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.commit()
        conn.close()
        print("Migration v2 complete.")

if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        migrate()
