import os

def check_path(base_dir, db_url):
    print(f"\n--- Checking from {base_dir} ---")
    try:
        os.chdir(base_dir)
    except FileNotFoundError:
        print(f"Directory not found: {base_dir}")
        return

    print(f"CWD: {os.getcwd()}")
    
    if db_url and db_url.startswith("sqlite:///"):
        db_path = db_url.replace("sqlite:///", "")
    else:
        db_path = db_url
        
    print(f"Extracted Path: {db_path}")
    
    abs_path = os.path.abspath(db_path)
    print(f"Absolute Path: {abs_path}")
    
    folder = os.path.dirname(abs_path)
    print(f"Folder: {folder}")
    
    if os.path.exists(folder):
        print("Folder EXISTS")
    else:
        print("Folder DOES NOT EXIST (would be created)")

# Simulating bot-backend
check_path(
    r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot-backend",
    "sqlite:///../shared/shared_lib/persistence/cosmicforge.db"
)

# Simulating user-backend
check_path(
    r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\user-backend",
    "sqlite:///../shared/shared_lib/persistence/cosmicforge.db"
)
