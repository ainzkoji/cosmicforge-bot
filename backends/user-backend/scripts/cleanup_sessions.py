#!/usr/bin/env python3
"""Clean up expired and test pairing sessions"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared_lib.persistence.db import DB

db = DB()
with db.connect() as conn:
    # Delete expired sessions
    result1 = conn.execute(
        "DELETE FROM mt_pairing_sessions WHERE status = 'pending' AND datetime(expires_at) < datetime('now')"
    )
    
    # Delete test sessions
    result2 = conn.execute(
        "DELETE FROM mt_pairing_sessions WHERE user_id LIKE 'user_e2e_%'"
    )
    
    conn.commit()
    print(f"Cleaned up {result1.rowcount + result2.rowcount} sessions")
