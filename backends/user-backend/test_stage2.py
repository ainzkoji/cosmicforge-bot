import os
import sys
from datetime import timedelta

# Add the backends root directory to sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
backends_dir = os.path.abspath(os.path.join(script_dir, ".."))
user_backend_dir = os.path.join(backends_dir, "user-backend")
sys.path.insert(0, backends_dir)
sys.path.insert(0, user_backend_dir)

import dotenv
dotenv.load_dotenv(os.path.join(backends_dir, "bot-backend", ".env"))

from app.core.security import (
    create_access_token,
    decode_token,
    create_admin_access_token,
    decode_admin_token
)

def test_token_isolation():
    print("="*50)
    print(" TOKEN ISOLATION TEST")
    print("="*50)
    
    user_id = "user-1234"
    admin_id = "admin-1234"
    
    print("[1] Generating standard User Access Token...")
    user_token = create_access_token(user_id, expires_delta=timedelta(minutes=15))
    
    print("[2] Generating Admin Access Token...")
    admin_token = create_admin_access_token(admin_id, expires_delta=timedelta(minutes=15))
    
    print("\n--- Testing Decoder Isolation ---")
    
    print("\n[A] decode_admin_token(user_token) -> Should fail")
    admin_payload_with_user_token = decode_admin_token(user_token)
    print(f"Result: {admin_payload_with_user_token}")
    if admin_payload_with_user_token is None:
        print("✅ SUCCESS: Admin decoder successfully rejected User token.")
    else:
        print("❌ FAIL: Admin decoder accepted User token!")
        
    print("\n[B] decode_admin_token(admin_token) -> Should succeed")
    admin_payload = decode_admin_token(admin_token)
    print(f"Result (sub): {admin_payload.get('sub') if admin_payload else None}")
    if admin_payload is not None:
        print("✅ SUCCESS: Admin decoder accepted Admin token.")
    else:
        print("❌ FAIL: Admin decoder rejected Admin token!")

    print("\n[C] decode_token(admin_token) -> Should fail (User decoder rejecting Admin token)")
    user_payload_with_admin_token = decode_token(admin_token)
    print(f"Result: {user_payload_with_admin_token}")
    if user_payload_with_admin_token is None:
        print("✅ SUCCESS: User decoder successfully rejected Admin token.")
    else:
        print("❌ FAIL: User decoder accepted Admin token!")

if __name__ == "__main__":
    test_token_isolation()
