import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from shared_lib.persistence.db import DB

db = DB()

with db.connect() as conn:
    cursor = conn.execute("SELECT id, name, visibility, status FROM strategies")
    rows = cursor.fetchall()
    
    print(f"\nFound {len(rows)} strategies in database:")
    for row in rows:
        print(f"  - {row['name']} ({row['visibility']}, {row['status']})")
    
    if len(rows) == 0:
        print("\n❌ No strategies found! Database is empty.")
    else:
        print(f"\n✅ {len(rows)} strategies ready to be displayed!")
