"""
Enhanced Security Module
- Password hashing (Argon2id)
- JWT generation with role
- Credential encryption (Fernet)
- OTP code generation and hashing
- Rate limiting helpers
"""
from datetime import datetime, timedelta, timezone
from typing import Union, Any, Optional
import hashlib
import hmac
import secrets
import time
import uuid

from jose import jwt, JWTError
from passlib.context import CryptContext
from cryptography.fernet import Fernet
import base64

from app.core.config import settings


# --- Password Hashing ---
pwd_context = CryptContext(schemes=["bcrypt", "argon2"], deprecated="auto")


# bcrypt only reads the first 72 bytes of its input. Rather than silently
# truncating (two different long passwords would then be interchangeable),
# anything longer is rejected at hashing time and never matches at login.
MAX_PASSWORD_BYTES = 72

_DUMMY_PASSWORD_HASH: Optional[str] = None


def password_too_long(password: str) -> bool:
    return len((password or "").encode("utf-8")) > MAX_PASSWORD_BYTES


def get_password_hash(password: str) -> str:
    if password_too_long(password):
        raise ValueError(f"Password must be at most {MAX_PASSWORD_BYTES} bytes")
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    if not plain_password or not hashed_password or password_too_long(plain_password):
        return False
    try:
        return bool(pwd_context.verify(plain_password, hashed_password))
    except (ValueError, TypeError):
        # Unrecognised / deliberately unusable stored hash: never a match.
        return False


def dummy_verify_password(plain_password: str) -> None:
    """Spend the same hashing work as a real check when no account exists,
    so response time does not reveal whether an email is registered."""
    global _DUMMY_PASSWORD_HASH
    if _DUMMY_PASSWORD_HASH is None:
        _DUMMY_PASSWORD_HASH = pwd_context.hash(secrets.token_urlsafe(24))
    verify_password(plain_password or "x", _DUMMY_PASSWORD_HASH)


# --- JWT Tokens ---
# --- Constants ---
ISSUER = "cosmicforge-user-backend"
AUDIENCE = "cosmicforge-services"


# --- JWT Tokens ---
from app.core.rbac import resolve_permissions
try:
    from app.core.billing_service import get_user_subscription
except ImportError:
    # Circular import fallback or mock
    def get_user_subscription(uid):
        return {"entitlements": {}}

# ...

def create_access_token(subject: Union[str, Any], role: str = "user", expires_delta: timedelta = None) -> str:
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    permissions = resolve_permissions(role)
    
    # Fetch entitlements
    entitlements = {}
    try:
        sub_data = get_user_subscription(str(subject))
        entitlements = sub_data.get("entitlements", {})
    except Exception as e:
        logger.error(f"Failed to fetch entitlements for user {subject}: {e}")
    
    to_encode = {
        "exp": expire,
        "nbf": now,
        "iat": now,
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": str(subject),
        "jti": str(uuid.uuid4()),
        "type": "access",
        "role": role,
        "permissions": permissions,
        "entitlements": entitlements
    }
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def create_refresh_token(subject: Union[str, Any], expires_delta: timedelta = None) -> str:
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    
    to_encode = {
        "exp": expire,
        "nbf": now,
        "iat": now,
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": str(subject),
        "jti": str(uuid.uuid4()),
        "type": "refresh"
    }
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


import logging
from jose import jwt, JWTError, ExpiredSignatureError
from jose.exceptions import JWTClaimsError

logger = logging.getLogger(__name__)

# ...

def decode_token(token: str) -> Optional[dict]:
    """Decode and validate a JWT token. Returns payload or None if invalid."""
    try:
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=[settings.ALGORITHM],
            issuer=ISSUER,
            audience=AUDIENCE,
            options={
                "require": ["iss", "aud", "exp", "sub"]
            }
        )
        return payload
    except ExpiredSignatureError:
        logger.warning("Token validation failed: Signature has expired.")
        return None
    except JWTClaimsError as e:
        logger.warning(f"Token validation failed: Invalid claims. {str(e)}")
        return None
    except JWTError as e:
        logger.warning(f"Token validation failed: Invalid token. {str(e)}")
        return None


# --- Admin Tokens ---
ADMIN_ISSUER = "cosmicforge-admin-backend"
ADMIN_AUDIENCE = "admin-portal"

def create_admin_access_token(subject: Union[str, Any], role: str = "admin", expires_delta: timedelta = None) -> str:
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode = {
        "exp": expire,
        "nbf": now,
        "iat": now,
        "iss": ADMIN_ISSUER,
        "aud": ADMIN_AUDIENCE,
        "sub": str(subject),
        "jti": str(uuid.uuid4()),
        "type": "admin_access",
        "role": role,
    }
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt

def create_admin_refresh_token(subject: Union[str, Any], expires_delta: timedelta = None) -> str:
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    
    to_encode = {
        "exp": expire,
        "nbf": now,
        "iat": now,
        "iss": ADMIN_ISSUER,
        "aud": ADMIN_AUDIENCE,
        "sub": str(subject),
        "jti": str(uuid.uuid4()),
        "type": "admin_refresh"
    }
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt

def decode_admin_token(token: str) -> Optional[dict]:
    """Decode and validate an Admin JWT token. Returns payload or None if invalid."""
    try:
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=[settings.ALGORITHM],
            issuer=ADMIN_ISSUER,
            audience=ADMIN_AUDIENCE,
            options={
                "require": ["iss", "aud", "exp", "sub"]
            }
        )
        return payload
    except ExpiredSignatureError:
        logger.warning("Admin Token validation failed: Signature has expired.")
        return None
    except JWTClaimsError as e:
        logger.warning(f"Admin Token validation failed: Invalid claims. {str(e)}")
        return None
    except JWTError as e:
        logger.warning(f"Admin Token validation failed: Invalid token. {str(e)}")
        return None


# --- Admin MFA step tokens ---
# Issued after a correct password, before the second factor. Their own
# audience means decode_admin_token() (and every admin route) rejects them
# outright; the type says which single step they allow.
ADMIN_MFA_AUDIENCE = "admin-mfa"
ADMIN_MFA_CHALLENGE = "admin_mfa_challenge"   # password ok, TOTP / recovery code still due
ADMIN_MFA_ENROLL = "admin_mfa_enroll"         # password ok, MFA required but not set up: may only enroll
ADMIN_MFA_CHALLENGE_MINUTES = 5
ADMIN_MFA_ENROLL_MINUTES = 15


def password_fingerprint(hashed_password: Optional[str]) -> str:
    """Short digest of the stored hash: a step token dies when the password changes."""
    return hashlib.sha256((hashed_password or "").encode("utf-8")).hexdigest()[:16]


def create_admin_mfa_token(subject: Union[str, Any], purpose: str, hashed_password: Optional[str]) -> str:
    if purpose not in (ADMIN_MFA_CHALLENGE, ADMIN_MFA_ENROLL):
        raise ValueError("unknown admin MFA token purpose")
    minutes = ADMIN_MFA_CHALLENGE_MINUTES if purpose == ADMIN_MFA_CHALLENGE else ADMIN_MFA_ENROLL_MINUTES
    now = datetime.now(timezone.utc)
    claims = {
        "exp": now + timedelta(minutes=minutes),
        "nbf": now,
        "iat": now,
        "iss": ADMIN_ISSUER,
        "aud": ADMIN_MFA_AUDIENCE,
        "sub": str(subject),
        "jti": str(uuid.uuid4()),
        "type": purpose,
        "pwf": password_fingerprint(hashed_password),
    }
    return jwt.encode(claims, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_admin_mfa_token(token: Optional[str], purpose: str) -> Optional[dict]:
    """Payload of a valid step token of exactly this purpose, else None."""
    if not token:
        return None
    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM],
            issuer=ADMIN_ISSUER, audience=ADMIN_MFA_AUDIENCE,
            options={"require": ["iss", "aud", "exp", "sub"]},
        )
    except (ExpiredSignatureError, JWTClaimsError, JWTError):
        logger.warning("Admin MFA step token rejected")
        return None
    if payload.get("type") != purpose:
        return None
    return payload


# --- Admin MFA recovery codes ---
RECOVERY_CODE_COUNT = 10
_RECOVERY_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"  # no 0/o, 1/l/i


def generate_recovery_codes(count: int = RECOVERY_CODE_COUNT) -> list:
    """``xxxxx-xxxxx`` codes (~49 bits each), shown to the admin exactly once."""
    codes = []
    for _ in range(count):
        raw = "".join(secrets.choice(_RECOVERY_ALPHABET) for _ in range(10))
        codes.append(f"{raw[:5]}-{raw[5:]}")
    return codes


def normalise_recovery_code(code: Optional[str]) -> str:
    return "".join(ch for ch in (code or "").lower() if ch.isalnum())


def hash_recovery_code(code: str, salt: Optional[str] = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.sha256(f"{salt}:{normalise_recovery_code(code)}".encode("utf-8")).hexdigest()
    return f"{salt}${digest}"


def verify_recovery_code(code: Optional[str], stored: Optional[str]) -> bool:
    if not code or not stored or "$" not in stored:
        return False
    salt = stored.split("$", 1)[0]
    return hmac.compare_digest(hash_recovery_code(code, salt), stored)

# --- Credential Encryption (Fernet) ---
# Ciphertext format, versioned by prefix:
#   "v2:<fernet token>"  key = HKDF-SHA256(CREDENTIAL_KEY) -- what is written now
#   "v1:<fernet token>"  legacy: CREDENTIAL_KEY truncated / zero-padded to 32 bytes
#   "<fernet token>"     legacy, unprefixed (same key as v1)
# Legacy values are decrypt-only; reencrypt_credential() rewrites them as v2.
CIPHERTEXT_V2_PREFIX = "v2:"
CIPHERTEXT_V1_PREFIX = "v1:"
_HKDF_SALT = b"cosmicforge/credential-encryption/v2"
_HKDF_INFO = b"user-backend credential fernet key"


def _hkdf_fernet_key(secret: str) -> bytes:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF

    if not secret:
        raise ValueError("CREDENTIAL_KEY is empty")
    derived = HKDF(algorithm=hashes.SHA256(), length=32, salt=_HKDF_SALT, info=_HKDF_INFO).derive(
        secret.encode("utf-8"))
    return base64.urlsafe_b64encode(derived)


def _get_fernet() -> Fernet:
    """The current (v2, HKDF-derived) key."""
    return Fernet(_hkdf_fernet_key(settings.CREDENTIAL_KEY))


def _get_legacy_fernet() -> Fernet:
    """The pre-8.4 derivation (truncate / zero-pad). Decryption of old values only."""
    key_bytes = settings.CREDENTIAL_KEY.encode()[:32].ljust(32, b'\0')
    return Fernet(base64.urlsafe_b64encode(key_bytes))


def encrypt_credential(text: str) -> str:
    if not text:
        return ""
    return CIPHERTEXT_V2_PREFIX + _get_fernet().encrypt(text.encode()).decode()


def credential_needs_reencryption(encrypted: Optional[str]) -> bool:
    return bool(encrypted) and not encrypted.startswith(CIPHERTEXT_V2_PREFIX)


def decrypt_credential(encrypted: str) -> str:
    """Decrypt any supported version. Raises cryptography's InvalidToken on a
    wrong key or tampered value; the error never contains the plaintext."""
    if not encrypted:
        return ""
    if encrypted.startswith(CIPHERTEXT_V2_PREFIX):
        return _get_fernet().decrypt(encrypted[len(CIPHERTEXT_V2_PREFIX):].encode()).decode()
    if encrypted.startswith(CIPHERTEXT_V1_PREFIX):
        encrypted = encrypted[len(CIPHERTEXT_V1_PREFIX):]
    return _get_legacy_fernet().decrypt(encrypted.encode()).decode()


def reencrypt_credential(encrypted: Optional[str]) -> Optional[str]:
    """The v2 form of a stored ciphertext, or None when it is already current."""
    if not credential_needs_reencryption(encrypted):
        return None
    return encrypt_credential(decrypt_credential(encrypted))


# --- TOTP (2FA) secrets ---
# Stored encrypted with CREDENTIAL_KEY under the prefix "enc:" followed by the
# versioned credential ciphertext: "enc:v2:..." now, "enc:v1:<fernet>" for
# values written before 8.4 (legacy key). A value without "enc:" is a legacy
# plaintext base32 secret, which stays readable (and is re-encrypted by the sweep).
_TOTP_ENC_PREFIX = "enc:"
_TOTP_SECRET_PREFIX = "enc:v1:"  # legacy prefix, kept for readers of old rows


def encrypt_totp_secret(secret: str) -> str:
    return _TOTP_ENC_PREFIX + encrypt_credential(secret)


def decrypt_totp_secret(stored: Optional[str]) -> str:
    if not stored:
        return ""
    if stored.startswith(_TOTP_ENC_PREFIX):
        return decrypt_credential(stored[len(_TOTP_ENC_PREFIX):])
    return stored


def reencrypt_totp_secret(stored: Optional[str]) -> Optional[str]:
    """The current form of a stored TOTP secret, or None when nothing changes."""
    if not stored:
        return None
    if stored.startswith(_TOTP_ENC_PREFIX + CIPHERTEXT_V2_PREFIX):
        return None
    return encrypt_totp_secret(decrypt_totp_secret(stored))


def reencrypt_stored_totp_secrets(conn) -> dict:
    """Rewrite every legacy TOTP secret (users.totp_secret, admins.mfa_secret)
    in the v2 format. Idempotent; returns counts per table, never values. A
    value that cannot be decrypted (wrong key) is left untouched and counted."""
    result = {}
    for table, column in (("users", "totp_secret"), ("admins", "mfa_secret")):
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in cols:
            continue
        updated = failed = 0
        rows = conn.execute(f"SELECT id, {column} FROM {table} WHERE {column} IS NOT NULL").fetchall()
        for row in rows:
            try:
                new_value = reencrypt_totp_secret(row[1])
            except Exception:
                failed += 1
                logger.error("TOTP secret re-encryption failed for one %s row (value not logged)", table)
                continue
            if new_value is not None:
                conn.execute(f"UPDATE {table} SET {column} = ? WHERE id = ? AND {column} = ?",
                             (new_value, row[0], row[1]))
                updated += 1
        result[table] = {"updated": updated, "failed": failed}
    return result


# One 30s step of clock drift is tolerated either way.
TOTP_VALID_WINDOW = 1


def _totp_time() -> float:
    return time.time()


def match_totp_counter(
    stored_secret: Optional[str],
    code: Optional[str],
    last_counter: Optional[int] = None,
) -> Optional[int]:
    """The time-step counter a 6-digit authenticator code is valid for, or None.

    A code stays mathematically valid for the whole drift window (~90 s), so a
    code that was observed once could be replayed. Callers therefore store the
    counter of the last code they accepted and pass it as ``last_counter``:
    only a code for a LATER time step is accepted (RFC 6238, section 5.2).
    """
    code = (code or "").replace(" ", "")
    if len(code) != 6 or not (code.isascii() and code.isdigit()):
        return None
    try:
        secret = decrypt_totp_secret(stored_secret)
        if not secret:
            return None
        import pyotp
        totp = pyotp.TOTP(secret)
        current = int(_totp_time()) // int(totp.interval)
        matched: Optional[int] = None
        for counter in range(current - TOTP_VALID_WINDOW, current + TOTP_VALID_WINDOW + 1):
            # No early exit: every step of the window is compared in constant time.
            if hmac.compare_digest(str(totp.generate_otp(counter)), code):
                matched = counter
    except Exception:
        logger.error("TOTP verification failed: stored secret could not be read")
        return None
    if matched is None:
        return None
    if last_counter is not None and matched <= int(last_counter):
        return None  # this code (or a newer one) was already used
    return matched


def verify_totp_code(stored_secret: Optional[str], code: Optional[str]) -> bool:
    """Check a 6-digit authenticator code against a stored (encrypted or legacy) secret.

    Stateless: it cannot detect a replayed code. Anything that authenticates a
    user must use :func:`match_totp_counter` with the stored last counter.
    """
    return match_totp_counter(stored_secret, code) is not None


# --- OTP Generation ---
def generate_otp(length: int = 6) -> str:
    """Generate a random numeric OTP code."""
    return ''.join([str(secrets.randbelow(10)) for _ in range(length)])


def hash_otp(code: str) -> str:
    """Hash OTP for secure storage."""
    return hashlib.sha256(code.encode()).hexdigest()


def verify_otp(plain_code: str, hashed_code: str) -> bool:
    """Verify OTP code against hash."""
    return hmac.compare_digest(hash_otp(plain_code or ""), hashed_code or "")


# --- Token Hashing ---
def hash_token(token: str) -> str:
    """Hash a token (refresh token) for secure storage."""
    return hashlib.sha256(token.encode()).hexdigest()


# --- Rate Limiting Helpers ---
def is_rate_limited(attempts: list, max_attempts: int = 5, window_minutes: int = 15) -> bool:
    """
    Check if rate limit is exceeded.
    attempts: list of datetime objects for recent attempts
    """
    if not attempts:
        return False
    
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    recent = [a for a in attempts if a > cutoff]
    return len(recent) >= max_attempts
