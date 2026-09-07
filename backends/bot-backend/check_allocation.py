"""Check bot instance allocation settings"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

db = DB()
with db.connect() as conn:
    cursor = conn.execute("""
        SELECT 
            id, user_id, allocation_type, allocation_value,
            capital_allocation_type, capital_allocation
        FROM bot_instances
        LIMIT 5
    """)
    rows = cursor.fetchall()
    
    print("\n📊 BOT INSTANCE ALLOCATION SETTINGS:\n")
    for row in rows:
        print(f"Bot ID: {row[0]}")
        print(f"  User: {row[1]}")
        print(f"  Allocation Type: {row[2]}")
        print(f"  Allocation Value: {row[3]}")
        print(f"  Capital Alloc Type: {row[4]}")
        print(f"  Capital Allocation: {row[5]}")
        print()
