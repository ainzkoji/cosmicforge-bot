"""Check risk profile and its default sizing"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB
import json

db = DB()
with db.connect() as conn:
    # Get bot instance with risk details
    cursor = conn.execute("""
        SELECT 
            id, risk_level, allocation_type, allocation_value,
            capital_allocation, risk_profile_id, config
        FROM bot_instances  
        WHERE id = 'bot_062f90be64b3'
    """)
    row = cursor.fetchone()
    
    if row:
        print("=" * 80)
        print("BOT INSTANCE CONFIG")
        print("=" * 80)
        print(f"Bot ID: {row[0]}")
        print(f"Risk Level: {row[1]}")
        print(f"Allocation Type: {row[2]}")
        print(f"Allocation Value: {row[3]}")
        print(f"Capital Allocation: {row[4]}")
        print(f"Risk Profile ID: {row[5]}")
        
        if row[6]:
            config = json.loads(row[6])
            print(f"\nConfig JSON:")
            print(json.dumps(config, indent=2))
        
        print("\n" + "=" * 80)
        
        # Check if there's a risk_profiles table
        try:
            cursor2 = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='risk_profiles'")
            if cursor2.fetchone():
                print("RISK PROFILES TABLE EXISTS")
                print("=" * 80)
                
                if row[5]:  # has risk_profile_id
                    cursor3 = conn.execute(f"""
                        SELECT * FROM risk_profiles WHERE id = '{row[5]}'
                    """)
                    profile = cursor3.fetchone()
                    if profile:
                        col_names = [desc[0] for desc in cursor3.description]
                        for i, col in enumerate(col_names):
                            print(f"{col}: {profile[i]}")
        except Exception as e:
            print(f"⚠️  No risk_profiles table: {e}")
