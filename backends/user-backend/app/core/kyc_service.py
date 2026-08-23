"""
KYC (Know Your Customer) Service

Manages KYC verification status for users.
Required for IBKR live trading compliance.
"""
from shared_lib.persistence.db import DB


def check_kyc_status(user_id: str) -> bool:
    """
    Check if user has completed KYC verification.
    
    Args:
        user_id: User ID to check
        
    Returns:
        True if KYC verified, False otherwise
    """
    db = DB()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT is_kyc_verified FROM users WHERE id = ?",
            (user_id,)
        ).fetchone()
        
        if not row:
            return False
        
        # Handle both boolean and integer (SQLite may store as 0/1)
        return bool(row["is_kyc_verified"])


def set_kyc_status(user_id: str, verified: bool) -> bool:
    """
    Set KYC verification status for a user.
    
    This is an admin-only function to be called after manual verification.
    
    Args:
        user_id: User ID
        verified: True to mark as verified, False to revoke
        
    Returns:
        True if successful, False if user not found
    """
    db = DB()
    with db.connect() as conn:
        # Check user exists
        row = conn.execute("SELECT id FROM users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return False
        
        # Update KYC status
        conn.execute(
            "UPDATE users SET is_kyc_verified = ? WHERE id = ?",
            (verified, user_id)
        )
        
        return True


def get_kyc_info(user_id: str) -> dict:
    """
    Get KYC information for a user.
    
    Args:
        user_id: User ID
        
    Returns:
        Dictionary with KYC info or None if user not found
    """
    db = DB()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT is_kyc_verified FROM users WHERE id = ?",
            (user_id,)
        ).fetchone()
        
        if not row:
            return None
        
        return {
            "user_id": user_id,
            "is_verified": bool(row["is_kyc_verified"]),
            "required_for": ["ibkr_live"]  # List of features requiring KYC
        }
