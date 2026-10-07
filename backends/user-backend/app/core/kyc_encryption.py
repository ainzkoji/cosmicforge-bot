"""
KYC Encryption Utilities
Handles field-level encryption for PII data
"""
import os
import base64
import logging
from typing import Dict, List, Optional
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

ENV_KEY = "KYC_ENCRYPTION_KEY"

# Development-only fallback. It is public (it is in the repository), so it is
# refused in production -- see ``_configured_secret``.
_DEV_DEFAULT_SECRET = "cosmicforge-kyc-dev-secret-key-2024"

# Values that are documentation placeholders, never real keys.
_PLACEHOLDER_SECRETS = frozenset({
    _DEV_DEFAULT_SECRET,
    "your-kyc-encryption-key-32-chars-min",
    "CHANGE_ME_BEFORE_PRODUCTION",
    "changeme",
})

# Key-derivation salts. The PII salt is the one every existing ciphertext was
# produced with: changing it would make stored PII unreadable, so it stays.
# (With a mandatory high-entropy key the salt is domain separation, not the
# secret.) Document files use their own salt so the two keys are independent.
_PII_SALT = b"cosmicforge_kyc_salt"
_FILE_SALT = b"cosmicforge_kyc_file_v1"
_KDF_ITERATIONS = 100000

# Every Fernet token starts with the version byte 0x80, i.e. "gAAAA" in base64.
FERNET_TOKEN_PREFIX = b"gAAAA"


class KYCConfigError(RuntimeError):
    """KYC secrets are missing or unsafe for the current environment."""


def _is_production() -> bool:
    try:
        from shared_lib.core.security.broker_security import is_production
        return bool(is_production())
    except Exception:
        # If the environment cannot be determined, behave as production.
        return True


def _configured_secret() -> str:
    """The active KYC encryption secret.

    Production: ``KYC_ENCRYPTION_KEY`` is mandatory and must not be a
    placeholder/default. Elsewhere the historical development default is kept
    so local data stays readable.
    """
    secret = (os.getenv(ENV_KEY) or "").strip()
    if _is_production():
        if not secret or secret in _PLACEHOLDER_SECRETS:
            raise KYCConfigError(
                f"{ENV_KEY} is required in production and must not be a default/placeholder value. "
                "Set a long random value (e.g. `openssl rand -hex 32`) and keep it stable: "
                "it decrypts stored KYC personal data and identity documents."
            )
        if len(secret) < 32:
            logger.warning("%s is shorter than 32 characters; use a longer random value", ENV_KEY)
        return secret
    return secret or _DEV_DEFAULT_SECRET


def assert_kyc_encryption_configured() -> None:
    """Startup/request check. Raises ``KYCConfigError`` in production without a real key."""
    _configured_secret()


def _derive_key(secret: str, salt: bytes) -> bytes:
    """Derive a Fernet key from the secret"""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=_KDF_ITERATIONS,
    )
    return base64.urlsafe_b64encode(kdf.derive(secret.encode()))


# Derived-key cache (PBKDF2 is deliberately slow), keyed by (secret, salt).
_fernet_cache: Dict[tuple, Fernet] = {}


def _fernet_for(secret: str, salt: bytes) -> Fernet:
    cache_key = (secret, salt)
    fernet = _fernet_cache.get(cache_key)
    if fernet is None:
        fernet = Fernet(_derive_key(secret, salt))
        _fernet_cache[cache_key] = fernet
    return fernet


def _get_fernet() -> Fernet:
    """Fernet used to encrypt PII (current key, historical derivation)."""
    return _fernet_for(_configured_secret(), _PII_SALT)


def _decrypt_fernets(salt: bytes) -> List[Fernet]:
    """Keys to try when reading: the current key first, then the historical
    development default, so data written before a real key was configured
    remains readable. New data is never written with the fallback key."""
    primary = _configured_secret()
    fernets = [_fernet_for(primary, salt)]
    if primary != _DEV_DEFAULT_SECRET:
        fernets.append(_fernet_for(_DEV_DEFAULT_SECRET, salt))
    return fernets


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
    
    # A missing production key is a configuration error, not "no data".
    fernets = _decrypt_fernets(_PII_SALT)
    try:
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_value.encode("utf-8"))
    except Exception as e:
        logger.warning("[KYC Encryption] Decryption failed: %s", type(e).__name__)
        return None
    for fernet in fernets:
        try:
            return fernet.decrypt(encrypted_bytes).decode("utf-8")
        except Exception:
            continue
    # Log error but don't expose details
    logger.warning("[KYC Encryption] Decryption failed: no configured key matches")
    return None


# ----------------------------------------------------------------------------
# Document (file) encryption at rest
# ----------------------------------------------------------------------------

def is_encrypted_blob(blob: bytes) -> bool:
    """True when the bytes look like a Fernet token (vs. a legacy plaintext file)."""
    return bytes(blob[:len(FERNET_TOKEN_PREFIX)]) == FERNET_TOKEN_PREFIX


def encrypt_file_bytes(content: bytes) -> bytes:
    """Encrypt a document for storage on disk. Returns a Fernet token."""
    return _fernet_for(_configured_secret(), _FILE_SALT).encrypt(content)


def decrypt_file_bytes(blob: bytes) -> bytes:
    """Decrypt a stored document.

    Files written before encryption at rest was introduced are plaintext
    (JPEG/PNG/PDF never start with the Fernet prefix) and are returned as-is.
    """
    if not is_encrypted_blob(blob):
        return blob
    for fernet in _decrypt_fernets(_FILE_SALT):
        try:
            return fernet.decrypt(blob)
        except InvalidToken:
            continue
    raise KYCConfigError(
        f"Stored KYC document cannot be decrypted with the configured {ENV_KEY}"
    )


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
