import sqlite3
import datetime
from pathlib import Path
import json

def get_db_path():
    # Find the db relative to this script or shared path
    db_path = Path(__file__).parent.parent / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
    return db_path

def archive_legacy_bots():
    db_path = get_db_path()
    print(f"Connecting to database: {db_path}")
    
    if not db_path.exists():
        print("Database not found!")
        return
        
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Target specific bot that causes noise
    target_id = "bot_0064a4b6dd86"
    now = datetime.datetime.utcnow().isoformat()
    
    # Check if bot exists
    row = conn.execute("SELECT id, status, broker_health_status FROM bot_instances WHERE id = ?", (target_id,)).fetchone()
    
    if row:
        print(f"Found bot {target_id}: status={row['status']}, health={row['broker_health_status']}")
        
        # Archive it
        conn.execute(
            "UPDATE bot_instances SET status = 'archived', updated_at = ? WHERE id = ?",
            (now, target_id)
        )
        conn.commit()
        print(f"Successfully archived legacy bot {target_id}")
    else:
        print(f"Bot {target_id} not found in database.")
    
    # Show current inventory summary
    rows = conn.execute("SELECT status, COUNT(*) as cnt FROM bot_instances GROUP BY status").fetchall()
    print("\nCurrent Inventory Summary:")
    for r in rows:
        print(f" - {r['status']}: {r['cnt']}")
        
    conn.close()

if __name__ == "__main__":
    archive_legacy_bots()
