"""
Seed initial super admin user directly into the database
"""
import hashlib
import uuid
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Ensure we can import from shared_lib
# Add project root to sys.path if needed, or rely on installed package
try:
    from shared_lib.persistence.db import DB, utc_now_iso
except ImportError:
    # If shared_lib is not found, try adding path relative to script
    sys.path.append(str(Path(__file__).parent.parent.parent.parent))
    from backends.shared.shared_lib.persistence.db import DB, utc_now_iso

ADMIN_EMAIL = "admin@example.com"
ADMIN_PASSWORD = "admin123"

# Password hashing matches app/core/security.py
from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt", "argon2"], deprecated="auto")

def hash_password(password: str) -> str:
    """Hash password using bcrypt/argon2"""
    return pwd_context.hash(password)

def seed_admin():
    print("Initializing database connection...")
    # Use default path from DB class (../../data/bot.db) or ensure pointing to root
    db = DB() 
    
    print(f"Target DB: {db.path}")

    # Ensure tables exist (running init effectively)
    db._init()
    
    with db.connect() as conn:
        try:
            # Check if user exists
            cursor = conn.execute("SELECT id FROM users WHERE email = ?", (ADMIN_EMAIL,))
            existing = cursor.fetchone()
            
            if existing:
                print(f"✓ User already exists: {ADMIN_EMAIL}")
                return

            # Create the user
            user_id = str(uuid.uuid4())
            hashed_pw = hash_password(ADMIN_PASSWORD)
            now = utc_now_iso()
            
            # Insert matching db.py schema
            conn.execute("""
                INSERT INTO users (
                    id, 
                    email, 
                    hashed_password, 
                    full_name,
                    is_active, 
                    is_superuser, 
                    status, 
                    created_at, 
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, 
                ADMIN_EMAIL, 
                hashed_pw, 
                "Super Admin",
                1,  # is_active
                1,  # is_superuser
                'active', 
                now, 
                now
            ))
            
            print(f"✓ Created user: {ADMIN_EMAIL}")
            
            conn.commit()
            print(f"\n✅ SUPER ADMIN CREATED!")
            print(f"   Email: {ADMIN_EMAIL}")
            print(f"   Password: {ADMIN_PASSWORD}")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    seed_admin()
