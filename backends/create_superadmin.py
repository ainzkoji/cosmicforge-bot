import sys
import os
import uuid
import asyncio
from datetime import datetime, timezone

# Add path
sys.path.append(os.path.join(os.getcwd(), 'user-backend'))
sys.path.append(os.path.join(os.getcwd(), 'shared'))

from app.core.security import get_password_hash
from shared_lib.persistence.db import DB

def create_superadmin():
    email = "favourdan027@gmail.com"
    password = "ainzkoji"
    name = "Super Admin"
    
    print(f"Creating Superadmin: {email}")
    
    db = DB(path="bot.db")
    with db.connect() as conn:
        # 1. Check if user exists
        existing = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        
        now = datetime.now(timezone.utc).isoformat()
        hashed = get_password_hash(password)
        uid = str(uuid.uuid4())
        
        if existing:
            print(f"User already exists (ID: {existing['id']}). Updating password and role...")
            uid = existing['id']
            conn.execute(
                "UPDATE users SET hashed_password = ?, status = 'active', is_verified = 1, role = 'admin' WHERE id = ?",
                (hashed, uid)
            )
        else:
            print("Creating new user...")
            conn.execute("""
                INSERT INTO users (id, email, hashed_password, full_name, status, role, is_verified, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'active', 'admin', 1, ?, ?)""",
                (uid, email, hashed, name, now, now)
            )
            
        # 2. Grant Admin Role in admin_roles table
        # Check if role exists
        role_existing = conn.execute(
            "SELECT * FROM admin_roles WHERE user_id = ? AND role = 'admin' AND revoked_at IS NULL", 
            (uid,)
        ).fetchone()
        
        if not role_existing:
            print("Granting admin_roles entry...")
            role_id = str(uuid.uuid4())
            conn.execute("""
                INSERT INTO admin_roles (id, user_id, role, granted_by, granted_at)
                VALUES (?, ?, 'admin', 'system_init', ?)
            """, (role_id, uid, now))
        else:
            print("Admin role already exists.")
            
        print("✅ Superadmin created successfully!")

if __name__ == "__main__":
    create_superadmin()
