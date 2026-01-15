import sqlite3
import os

DB_PATH = "data/bot.db"

def migrate():
    print(f"Migrating database at {DB_PATH}...")
    
    if not os.path.exists(DB_PATH):
        print("Database not found. Initializing new one...")
        os.makedirs("data", exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    try:
        # 1. Users Table
        print("Creating users table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_verified BOOLEAN DEFAULT 0,
                created_at TEXT NOT NULL,
                last_login_at TEXT
            )
        """)

        # 2. Broker Accounts Table (Multi-broker support)
        print("Creating broker_accounts table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS broker_accounts (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                exchange TEXT NOT NULL,
                name TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)

        # 3. Broker Credentials Table (Encrypted)
        print("Creating broker_credentials table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS broker_credentials (
                account_id TEXT PRIMARY KEY,
                api_key_enc TEXT,
                api_secret_enc TEXT,
                passphrase_enc TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
            )
        """)

        # 4. Refresh Tokens Table (Long-lived auth)
        print("Creating refresh_tokens table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                token_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked BOOLEAN DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)

        conn.commit()
        print("Migration complete! ✅")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
