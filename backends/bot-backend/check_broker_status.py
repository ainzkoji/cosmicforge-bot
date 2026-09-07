"""Query broker accounts and credentials status"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

db = DB()
with db.connect() as conn:
    print("=" * 80)
    print("BROKER ACCOUNTS IN DATABASE")
    print("=" * 80 + "\n")
    
    cursor = conn.execute("""
        SELECT 
            id, user_id, broker_id, market_type, label, status, 
            environment, masked_key, last_validated_at, 
            last_error_code, last_error_message
        FROM broker_accounts
    """)
    accounts = cursor.fetchall()
    
    for acc in accounts:
        print(f"Account ID: {acc[0]}")
        print(f"  User: {acc[1]}")
        print(f"  Broker: {acc[2]}")
        print(f"  Market: {acc[3]}")
        print(f"  Label: {acc[4]}")
        print(f"  Status: {acc[5]}")
        print(f"  Environment: {acc[6]}")
        print(f"  Masked Key: {acc[7]}")
        print(f"  Last Validated: {acc[8]}")
        print(f"  Last Error Code: {acc[9]}")
        print(f"  Last Error: {acc[10]}")
        print()
    
    print("=" * 80)
    print("BROKER CREDENTIALS")
    print("=" * 80 + "\n")
    
    cursor2 = conn.execute("SELECT account_id, key_metadata, updated_at FROM broker_credentials")
    creds = cursor2.fetchall()
    
    for cred in creds:
        print(f"Account ID: {cred[0]}")
        print(f"  Key Metadata: {cred[1]}")
        print(f"  Updated: {cred[2]}")
        print()
