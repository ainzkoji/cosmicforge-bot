"""
Migration: Auth & Security Features
- Adds 2FA columns to users table
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data/bot.db")

def migrate():
    print(f"Migrating database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Add 2FA columns to users table
    print("Adding 2FA columns to users...")
    new_columns = [
        ("totp_secret", "TEXT"),
        ("is_2fa_enabled", "BOOLEAN DEFAULT 0"),
    ]
    for col_name, col_def in new_columns:
        try:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
            print(f"  Added {col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"  {col_name} already exists")
            else:
                raise

    conn.commit()
    conn.close()
    print("Migration complete! ✅")

if __name__ == "__main__":
    migrate()
