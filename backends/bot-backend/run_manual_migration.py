import sys
import os
from pathlib import Path

shared_path = Path('C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/shared')
sys.path.insert(0, str(shared_path))

from shared_lib.persistence.migrations import migrate

if __name__ == '__main__':
    db_path = '../shared/shared_lib/persistence/cosmicforge.db'
    print(f"Running migrations on {db_path}...")
    try:
        migrate(db_path=db_path)
        print(f"Migrations complete.")
    except Exception as e:
        print(f"Migration failed: {e}")
