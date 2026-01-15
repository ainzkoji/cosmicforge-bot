"""
Migration: Enhanced User Identity System
- Adds status, role to users table
- Creates email_verifications table
- Creates auth_sessions table (replaces refresh_tokens)
- Creates password_resets table
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data/bot.db")

def migrate():
    print(f"Migrating database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Add status and role to users table
    print("Adding status and role columns to users...")
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'pending_verification'")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("  status column already exists")
        else:
            raise
    
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user'")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("  role column already exists")
        else:
            raise

    # Update existing users to active status
    cursor.execute("UPDATE users SET status = 'active' WHERE status IS NULL OR status = 'pending_verification'")
    cursor.execute("UPDATE users SET role = 'user' WHERE role IS NULL")

    # 2. Create email_verifications table
    print("Creating email_verifications table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_verifications (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            used_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_email_verifications_user_id ON email_verifications(user_id)")

    # 3. Create auth_sessions table
    print("Creating auth_sessions table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS auth_sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            refresh_token_hash TEXT NOT NULL,
            device TEXT,
            ip TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            revoked_at TEXT,
            rotated_from TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_id ON auth_sessions(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_refresh_hash ON auth_sessions(refresh_token_hash)")

    # 4. Create password_resets table
    print("Creating password_resets table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS password_resets (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            used_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_password_resets_user_id ON password_resets(user_id)")

    # 5. Create login_attempts table for rate limiting
    print("Creating login_attempts table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS login_attempts (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            ip TEXT,
            success INTEGER DEFAULT 0,
            attempted_at TEXT NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_login_attempts_email ON login_attempts(email)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_login_attempts_time ON login_attempts(attempted_at)")

    # 6. Create auth_audit_log table for security events
    print("Creating auth_audit_log table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS auth_audit_log (
            id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            user_id TEXT,
            email TEXT,
            ip TEXT,
            details TEXT,
            created_at TEXT NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_auth_audit_event_type ON auth_audit_log(event_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_auth_audit_user_id ON auth_audit_log(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_auth_audit_created_at ON auth_audit_log(created_at)")

    conn.commit()
    conn.close()
    print("Migration complete! ✅")

if __name__ == "__main__":
    migrate()
