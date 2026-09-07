import sys
import os
import sqlite3

# Set up path to import app modules
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
# Also add shared
sys.path.append(os.path.join(current_dir, '../../shared'))

from app.core.security import get_password_hash, verify_password
from shared_lib.persistence.db import DB

TARGET_EMAIL = "favourdan027@gmail.com"
NEW_PASSWORD = "admin123"

def reset_password():
    print(f"Resetting password for {TARGET_EMAIL} to '{NEW_PASSWORD}'")
    
    # Use the DB class logic to find the file
    db = DB()
    print(f"Using DB at: {db.path}")
    
    with db.connect() as conn:
        # Check if user exists
        row = conn.execute("SELECT id, email, hashed_password FROM users WHERE email = ?", (TARGET_EMAIL,)).fetchone()
        
        if not row:
            print(f"ERROR: User {TARGET_EMAIL} not found!")
            return
            
        print(f"Found user {row['email']} (ID: {row['id']})")
        print(f"Current hash: {row['hashed_password']}")
        
        # Hash new password
        new_hash = get_password_hash(NEW_PASSWORD)
        print(f"New hash: {new_hash}")
        
        # Verify immediately before saving (sanity check)
        if verify_password(NEW_PASSWORD, new_hash):
            print("Sanity check passed: New hash verifies against new password.")
        else:
            print("CRITICAL: Sanity check FAILED. Hashing/Verification broken in this environment.")
            return

        # Update DB
        conn.execute("UPDATE users SET hashed_password = ? WHERE id = ?", (new_hash, row['id']))
        print("Database updated.")
        
    print("Password reset successful.")

if __name__ == "__main__":
    reset_password()
