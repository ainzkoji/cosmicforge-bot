"""
Create Admin User Script

Grant admin role to a specific user by email.
Usage: python scripts/create_admin.py --email user@example.com
"""
import sys
import sqlite3
import uuid
from pathlib import Path
from datetime import datetime
import argparse

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = Path("data/bot.db")

def utc_now_iso():
    return datetime.utcnow().isoformat() + "Z"

def grant_admin_role(email: str):
    """Grant admin role to user by email"""
    if not DB_PATH.exists():
        print(f"❌ Database not found at {DB_PATH}")
        print("   Run migrations first: python migrate_admin_system.py")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Find user by email
        user = cursor.execute(
            "SELECT id, email, status FROM users WHERE email = ?",
            (email,)
        ).fetchone()
        
        if not user:
            print(f"❌ User not found with email: {email}")
            print("   Make sure the user has registered first.")
            return False
        
        user_id = user["id"]
        user_email = user["email"]
        user_status = user["status"]
        
        print(f"✅ Found user: {user_email} (ID: {user_id}, Status: {user_status})")
        
        # Check if already admin
        existing = cursor.execute(
            "SELECT * FROM admin_roles WHERE user_id = ? AND revoked_at IS NULL",
            (user_id,)
        ).fetchone()
        
        if existing:
            print(f"⚠️  User already has admin role (granted at {existing['granted_at']})")
            return True
        
        # Grant admin role
        role_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO admin_roles (id, user_id, role, granted_by, granted_at)
            VALUES (?, ?, 'admin', NULL, ?)
        """, (role_id, user_id, utc_now_iso()))
        
        # Log audit event
        audit_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO auth_audit_log (id, event_type, user_id, email, details, created_at)
            VALUES (?, 'admin_role_granted', ?, ?, ?, ?)
        """, (audit_id, user_id, user_email, "Admin role granted via script", utc_now_iso()))
        
        conn.commit()
        
        print(f"✅ Admin role granted successfully!")
        print(f"   User: {user_email}")
        print(f"   Role ID: {role_id}")
        print(f"   The user can now access admin endpoints at /api/admin/*")
        
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        return False
    finally:
        conn.close()

def list_admins():
    """List all current admin users"""
    if not DB_PATH.exists():
        print(f"❌ Database not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        admins = cursor.execute("""
            SELECT u.id, u.email, u.status, ar.granted_at, ar.revoked_at
            FROM admin_roles ar
            JOIN users u ON ar.user_id = u.id
            WHERE ar.revoked_at IS NULL
            ORDER BY ar.granted_at DESC
        """).fetchall()
        
        if not admins:
            print("No admin users found.")
            return
        
        print(f"\n{'='*60}")
        print(f"Current Admin Users ({len(admins)})")
        print(f"{'='*60}")
        for admin in admins:
            print(f"Email: {admin['email']}")
            print(f"  User ID: {admin['id']}")
            print(f"  Status: {admin['status']}")
            print(f"  Granted: {admin['granted_at']}")
            print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Manage admin users")
    parser.add_argument("--email", help="Email of user to grant admin role")
    parser.add_argument("--list", action="store_true", help="List all admin users")
    
    args = parser.parse_args()
    
    if args.list:
        list_admins()
    elif args.email:
        grant_admin_role(args.email)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
