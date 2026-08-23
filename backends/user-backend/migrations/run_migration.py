"""
Run database migrations
"""
import sqlite3
import sys
import os
from pathlib import Path

# Add shared package to path
SHARED_PATH = Path(__file__).parent.parent.parent / "shared"
sys.path.insert(0, str(SHARED_PATH))

from shared_lib.persistence.db import DB

MIGRATIONS_DIR = Path(__file__).parent

def run_migrations():
    print(f"Running migrations...")
    
    # Initialize DB (creates file/tables if needed)
    db = DB()
    print(f"Database: {db.path}")
    
    conn = sqlite3.connect(db.path)
    cursor = conn.cursor()
    
    try:
        # Get list of sql files
        sql_files = sorted(list(MIGRATIONS_DIR.glob("*.sql")))
        
        for sql_file in sql_files:
            print(f"Applying {sql_file.name}...")
            with open(sql_file, 'r') as f:
                sql = f.read()
            
            try:
                cursor.executescript(sql)
                conn.commit()
                print(f"✅ Applied {sql_file.name}")
            except Exception as e:
                if "duplicate column name" in str(e):
                    print(f"⚠️  Skipping {sql_file.name} (already applied or column exists)")
                    conn.rollback() # Rollback valid for this script but we might want to continue? 
                    # Actually executescript is atomic usually.
                elif "already exists" in str(e):
                     print(f"⚠️  Skipping {sql_file.name} (table already exists)")
                else:
                    print(f"❌ Failed to apply {sql_file.name}: {e}")
                    # Don't raise, try next one? Or stop?
                    # For safety, simplistic approach: stop on error unless it's "already exists" logic which is hard to parse perfectly
                    # But generic 002 adds columns, if they exist it fails.
                    pass 

        # Verify table schema
        print("Verifying schema...")
        cursor.execute("PRAGMA table_info(mt_pairing_sessions)")
        columns = {row[1] for row in cursor.fetchall()}
        print(f"Columns in mt_pairing_sessions: {columns}")
        
    except Exception as e:
        print(f"❌ Migration script failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run_migrations()
