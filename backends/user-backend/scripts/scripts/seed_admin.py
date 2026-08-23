"""
Seed initial super admin user directly into the database
"""
import sqlite3
import hashlib
import uuid
from datetime import datetime

DB_PATH = "data/bot.db"
ADMIN_EMAIL = "favourdan27@gmail.com"
ADMIN_PASSWORD = "Password123!"

def hash_password(password: str) -> str:
    """Simple password hashing (matches the backend auth)"""
    return hashlib.sha256(password.encode()).hexdigest()

def seed_admin():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check if user exists
        cursor.execute("SELECT id FROM users WHERE email = ?", (ADMIN_EMAIL,))
        existing = cursor.fetchone()
        
        if existing:
            user_id = existing[0]
            print(f"✓ User already exists: {ADMIN_EMAIL}")
        else:
            # Create the user
            user_id = str(uuid.uuid4())
            hashed_pw = hash_password(ADMIN_PASSWORD)
            now = datetime.utcnow().isoformat()
            
            cursor.execute("""
                INSERT INTO users (id, email, password_hash, created_at, is_verified, status, role)
                VALUES (?, ?, ?, ?, 1, 'active', 'admin')
            """, (user_id, ADMIN_EMAIL, hashed_pw, now))
            
            print(f"✓ Created user: {ADMIN_EMAIL}")
        
        # Grant admin role in admin_roles table
        cursor.execute("""
            INSERT OR IGNORE INTO admin_roles (user_id, role, granted_at, granted_by)
            VALUES (?, 'admin', ?, 'super_admin')
        """, (user_id, datetime.utcnow().isoformat()))
        
        conn.commit()
        print(f"\n✅ SUPER ADMIN CREATED!")
        print(f"   Email: {ADMIN_EMAIL}")
        print(f"   Password: {ADMIN_PASSWORD}")
        print(f"\n🌐 Access admin panel:")
        print(f"   1. Go to: http://localhost:5173/login")
        print(f"   2. Login with credentials above")
        print(f"   3. Navigate to: http://localhost:5173/admin")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    seed_admin()
