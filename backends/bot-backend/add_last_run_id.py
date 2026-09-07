import sqlite3
import os

DB_PATH = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\data\bot.db"

def migrate():
    if not os.path.exists(DB_PATH):
        print("DB not found!")
        return

    conn = sqlite3.connect(DB_PATH)
    
    print("Migrating: Adding last_run_id to bot_instances...")
    try:
        conn.execute("ALTER TABLE bot_instances ADD COLUMN last_run_id TEXT")
        conn.commit()
        print("Success: Column added.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("Column already exists.")
        else:
            print(f"Error: {e}")
            
    # Reset bot status while we are here
    bot_id = "bot_4a21f293f52b"
    print(f"Resetting {bot_id} to active...")
    conn.execute("UPDATE bot_instances SET status = 'active', last_error = NULL WHERE id = ?", (bot_id,))
    conn.commit()
    print("Done.")

if __name__ == "__main__":
    migrate()
