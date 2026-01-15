import sys
sys.path.insert(0, 'c:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backend')

from app.persistence.db import DB
from app.auth.crypto import hash_password

# Initialize DB
db = DB()

# Check existing users
with db.connect() as conn:
    users = conn.execute("SELECT id, email, username, is_verified FROM users LIMIT 10").fetchall()
    print(f"\nExisting users ({len(users)}):")
    for user in users:
        print(f"  - {user[1]} (username: {user[2]}, verified: {user[3]})")
    
    # Create test user if none exist
    if len(users) == 0:
        print("\n Creating test user...")
        test_email = "test@example.com"
        test_password = "password123"
        hashed_pw = hash_password(test_password)
        
        conn.execute("""
            INSERT INTO users (id, email, username, hashed_password, is_verified, created_at, plan_id)
            VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
        """, (
            "test-user-001",
            test_email,
            test_email,
            hashed_pw,
            1,  # is_verified
            "plan_free"
        ))
        conn.commit()
        print(f"✓ Test user created:")
        print(f"  Email: {test_email}")
        print(f"  Password: {test_password}")
    else:
        print("\n✓ Users already exist in database")
