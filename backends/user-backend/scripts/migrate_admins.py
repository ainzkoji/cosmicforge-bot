import sys
import os
from datetime import datetime, timezone

# Add the backends root directory to sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
backends_dir = os.path.abspath(os.path.join(script_dir, "../.."))
sys.path.insert(0, backends_dir)

from shared.shared_lib.persistence.db import DB
import dotenv
dotenv.load_dotenv(os.path.join(backends_dir, "bot-backend", ".env"))

def migrate_admins():
    print("=" * 60)
    print(" COSMICFORGE - ADMIN IDENTITY MIGRATION UTILITY (STAGE 1)")
    print("=" * 60)
    
    db = DB()
    migrated_count = 0
    skipped_count = 0
    
    with db.connect() as conn:
        # Find all users with active admin roles
        cursor = conn.execute(
            """
            SELECT u.*, ar.role as admin_role
            FROM users u
            JOIN admin_roles ar ON u.id = ar.user_id
            WHERE ar.revoked_at IS NULL AND u.is_active = 1
            """
        )
        
        users_to_migrate = cursor.fetchall()
        print(f"[*] Found {len(users_to_migrate)} active admin(s) in legacy tables.")
        
        for user in users_to_migrate:
            email = user["email"]
            print(f"  -> Migrating {email}...")
            
            # Check if already in admins table
            existing = conn.execute("SELECT id FROM admins WHERE email = ?", (email,)).fetchone()
            if existing:
                print(f"     [SKIPPED] Admin {email} already exists in admins table.")
                skipped_count += 1
                continue
                
            now = datetime.now(timezone.utc).isoformat()
            
            # Insert into admins
            conn.execute(
                """
                INSERT INTO admins (
                    id, email, hashed_password, full_name, role, 
                    is_active, is_superuser, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user["id"],  # Keep the same ID for consistency (optional, but safe since it's a UUID string)
                    user["email"],
                    user["hashed_password"],
                    user["full_name"],
                    user["admin_role"],
                    1,
                    user["is_superuser"] if user["is_superuser"] is not None else 1,
                    user["created_at"],
                    now
                )
            )
            migrated_count += 1
            print(f"     [SUCCESS] {email} copied to admins table.")
            
    print("-" * 60)
    print(" MIGRATION REPORT")
    print("-" * 60)
    print(f" Total candidates : {len(users_to_migrate)}")
    print(f" Successfully Migrated : {migrated_count}")
    print(f" Skipped (Exists) : {skipped_count}")
    print("-" * 60)
    print(" NOTE: Legacy users/admin_roles records were NOT deleted.")
    print(" This is an additive, non-destructive migration.")
    print("=" * 60)

if __name__ == "__main__":
    migrate_admins()
