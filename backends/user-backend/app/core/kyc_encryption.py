"""
KYC Encryption Utilities
Handles field-level encryption for PII data
"""
import os
import base64
from typing import Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# In production, this should come from environment variable
# For dev, we generate a consistent key from a secret
_KYC_ENCRYPTION_SECRET = os.getenv("KYC_ENCRYPTION_KEY", "cosmicforge-kyc-dev-secret-key-2024")

def _get_fernet_key() -> bytes:
    """Derive a Fernet key from the secret"""
    # Use PBKDF2 to derive a proper key from our secret
    salt = b"cosmicforge_kyc_salt"  # In production, use a proper random salt stored securely
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(_KYC_ENCRYPTION_SECRET.encode()))
    return key

# Singleton Fernet instance
_fernet: Optional[Fernet] = None

def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = Fernet(_get_fernet_key())
    return _fernet


def encrypt_pii(value: Optional[str]) -> Optional[str]:
    """
    Encrypt a PII value for storage.
    Returns base64-encoded encrypted string.
    """
    if value is None or value == "":
        return None
    
    fernet = _get_fernet()
    encrypted = fernet.encrypt(value.encode("utf-8"))
    return base64.urlsafe_b64encode(encrypted).decode("utf-8")


def decrypt_pii(encrypted_value: Optional[str]) -> Optional[str]:
    """
    Decrypt a PII value from storage.
    Returns the original plaintext string.
    """
    if encrypted_value is None or encrypted_value == "":
        return None
    
    try:
        fernet = _get_fernet()
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_value.encode("utf-8"))
        decrypted = fernet.decrypt(encrypted_bytes)
        return decrypted.decode("utf-8")
    except Exception as e:
        # Log error but don't expose details
        print(f"[KYC Encryption] Decryption failed: {type(e).__name__}")
        return None


def hash_document_number(doc_number: str) -> str:
    """
    Hash a document number for storage.
    Uses SHA256 - one-way, cannot be reversed.
    """
    import hashlib
    salted = f"kyc_doc_{doc_number}_salt"
    return hashlib.sha256(salted.encode()).hexdigest()


def mask_pii(value: Optional[str], visible_chars: int = 4) -> str:
    """
    Mask a PII value for display (e.g., showing last 4 chars).
    """
    if value is None or len(value) <= visible_chars:
        return "****"
    
    masked_length = len(value) - visible_chars
    return "*" * masked_length + value[-visible_chars:]


def mask_name(full_name: Optional[str]) -> str:
    """
    Mask a name for display (e.g., "John Doe" -> "J*** D**")
    """
    if not full_name:
        return "***"
    
    parts = full_name.split()
    masked_parts = []
    for part in parts:
        if len(part) > 1:
            masked_parts.append(part[0] + "*" * (len(part) - 1))
        else:
            masked_parts.append("*")
    
    return " ".join(masked_parts)
