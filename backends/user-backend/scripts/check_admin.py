#!/usr/bin/env python3
"""Check users and admin roles in the database"""
import sys
sys.path.insert(0, '.')

from shared_lib.persistence.db import DB

db = DB()

with db.connect() as conn:
    # Get all users
    users = conn.execute('SELECT id, email, status FROM users').fetchall()
    print(f"\n📊 Total Users: {len(users)}\n")
    
    if users:
        print("Users:")
        for u in users:
            print(f"  - ID: {u['id']}")
            print(f"    Email: {u['email']}")
            print(f"    Status: {u['status']}")
            print()
    else:
        print("No users found in database.\n")
    
    # Get admin roles
    admin_roles = conn.execute(
        'SELECT user_id FROM admin_roles WHERE revoked_at IS NULL'
    ).fetchall()
    
    print(f"🔐 Admin Roles: {len(admin_roles)} users have admin access\n")
    
    if admin_roles:
        print("Admin users:")
        for r in admin_roles:
            user = conn.execute(
                'SELECT email FROM users WHERE id = ?', 
                (r['user_id'],)
            ).fetchone()
            if user:
                print(f"  - User ID: {r['user_id']}")
                print(f"    Email: {user['email']}")
                print()
