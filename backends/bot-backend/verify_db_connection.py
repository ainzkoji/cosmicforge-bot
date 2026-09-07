import sys
import os
import traceback
from pathlib import Path

# Add shared to path so we can import DB
shared_path = Path(__file__).parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from shared_lib.persistence.db import DB

# Explictly point to cosmicforge.db
# Relative from bot-backend needs to go up one level to 'backends' then into 'shared'
# backends/bot-backend -> backends -> shared -> shared_lib -> persistence -> cosmicforge.db
# So: ../../shared/shared_lib/persistence/cosmicforge.db ?
# No, run form bot-backend cwd (backends/bot-backend)
# ../shared/shared_lib/persistence/cosmicforge.db

db_path_rel = "../shared/shared_lib/persistence/cosmicforge.db"
db_path_abs = os.path.abspath(db_path_rel)

def test_connection():
    print(f"Testing DB connection to: {db_path_abs}")
    if not os.path.exists(db_path_abs):
        print(f"ERROR: Database file not found at {db_path_abs}")
        # List dir to see what's there
        parent = os.path.dirname(db_path_abs)
        print(f"Listing {parent}:")
        try:
            for f in os.listdir(parent):
                print(f" - {f}")
        except Exception as e:
            print(f"Count not list dir: {e}")
        return

    try:
        db = DB(path=db_path_abs)
        with db.connect() as conn:
            print("[DB] Connection successful!")
            
            # Check for users table (crucial for Auth)
            try:
                count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                print(f"[DB] 'users' table found. Row count: {count}")
                if count == 0:
                     print("[DB] WARNING: Users table is empty! 401 will persist until a user is created.")
            except Exception as e:
                print(f"[DB] WARNING: 'users' table access failed: {e}")
                
            # Check for bot_instances
            try:
                # It might accept bot_instances if migration ran previously on this DB?
                # or if user-backend created it.
                count = conn.execute("SELECT COUNT(*) FROM bot_instances").fetchone()[0]
                print(f"[DB] 'bot_instances' table found. Row count: {count}")
            except Exception as e:
                print(f"[DB] 'bot_instances' table not found (Expected if migration hasn't run on this DB yet).")
                
    except Exception as e:
        print(f"[DB] Connection failed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_connection()
