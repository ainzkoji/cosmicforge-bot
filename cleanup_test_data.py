"""Quick script to clean up test broker accounts"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from app.persistence.db import DB

db = DB()
with db.connect() as conn:
    # Delete credentials first (foreign key constraint)
    conn.execute("DELETE FROM broker_credentials WHERE account_id IN (SELECT id FROM broker_accounts WHERE user_id = 'test_user_123')")
    conn.execute("DELETE FROM broker_audit_log WHERE user_id = 'test_user_123'")
    conn.execute("DELETE FROM broker_accounts WHERE user_id = 'test_user_123'")
    
    # Check what's left
    remaining = conn.execute("SELECT COUNT(*) as count FROM broker_accounts WHERE user_id = 'test_user_123'").fetchone()
    print(f"✓ Cleaned up test data. Remaining accounts for test_user_123: {remaining['count']}")
