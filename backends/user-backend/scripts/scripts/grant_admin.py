#!/usr/bin/env python3
"""Grant admin role to a user"""
import sys
import uuid
sys.path.insert(0, '.')

from shared_lib.persistence.db import DB, utc_now_iso

db = DB()

# Grant admin to the main user
user_email = "favourdan027@gmail.com"
user_id = "ac205d5b-7cc9-4639-8d7c-46788c483419"

with db.connect() as conn:
    # Check if already has admin role
    existing = conn.execute(
        'SELECT * FROM admin_roles WHERE user_id = ? AND revoked_at IS NULL',
        (user_id,)
    ).fetchone()
    
    if existing:
        print(f"✅ User {user_email} already has admin role")
    else:
        # Grant admin role
        role_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO admin_roles (id, user_id, role, granted_by, granted_at)
            VALUES (?, ?, 'admin', 'system', ?)
        """, (role_id, user_id, utc_now_iso()))
        
        print(f"✅ Admin role granted to {user_email}")
        print(f"   User ID: {user_id}")
        print(f"   Role ID: {role_id}")
        
    # Verify
    admin_count = conn.execute(
        'SELECT COUNT(*) as cnt FROM admin_roles WHERE revoked_at IS NULL'
    ).fetchone()['cnt']
    
    print(f"\n📊 Total admin users: {admin_count}")
