import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.persistence.db import DB

print("Testing row factory in DB class:")
db = DB()

with db.connect() as conn:
    print(f"Connection type: {type(conn)}")
    print(f"Row factory: {conn.row_factory}")
    
    # Try to fetch a user
    cursor = conn.execute("SELECT * FROM users LIMIT 1")
    row = cursor.fetchone()
    
    if row:
        print(f"\nRow type: {type(row)}")
        print(f"Row keys: {row.keys() if hasattr(row, 'keys') else 'NO KEYS METHOD'}")
        
        # Try to convert to dict
        try:
            user_dict = dict(row)
            print(f"dict(row) SUCCESS: {list(user_dict.keys())}")
        except Exception as e:
            print(f"dict(row) FAILED: {e}")
            
        # Try alternative method
        try:
            user_dict2 = {key: row[key] for key in row.keys()}
            print(f"Comprehension SUCCESS: {list(user_dict2.keys())}")
        except Exception as e:
            print(f"Comprehension FAILED: {e}")
    else:
        print("No users in database!")
