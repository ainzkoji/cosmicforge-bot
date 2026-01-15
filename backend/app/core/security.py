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
import secrets

from jose import jwt, JWTError
from passlib.context import CryptContext
from cryptography.fernet import Fernet
import base64

from app.core.config import settings


# --- Password Hashing ---
pwd_context = CryptContext(schemes=["bcrypt", "argon2"], deprecated="auto")


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


# --- JWT Tokens ---
def create_access_token(subject: Union[str, Any], role: str = "user", expires_delta: timedelta = None) -> str:
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode = {
        "exp": expire,
        "sub": str(subject),
        "type": "access",
        "role": role
    }
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def create_refresh_token(subject: Union[str, Any], expires_delta: timedelta = None) -> str:
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    
    to_encode = {
        "exp": expire,
        "sub": str(subject),
        "type": "refresh"
    }
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def decode_token(token: str) -> Optional[dict]:
    """Decode and validate a JWT token. Returns payload or None if invalid."""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError:
        return None


# --- Credential Encryption (Fernet) ---
def _get_fernet() -> Fernet:
    key_bytes = settings.CREDENTIAL_KEY.encode()[:32].ljust(32, b'\0')
    key_b64 = base64.urlsafe_b64encode(key_bytes)
    return Fernet(key_b64)


def encrypt_credential(text: str) -> str:
    if not text:
        return ""
    f = _get_fernet()
    return f.encrypt(text.encode()).decode()


def decrypt_credential(encrypted: str) -> str:
    if not encrypted:
        return ""
    f = _get_fernet()
    return f.decrypt(encrypted.encode()).decode()


# --- OTP Generation ---
def generate_otp(length: int = 6) -> str:
    """Generate a random numeric OTP code."""
    return ''.join([str(secrets.randbelow(10)) for _ in range(length)])


def hash_otp(code: str) -> str:
    """Hash OTP for secure storage."""
    return hashlib.sha256(code.encode()).hexdigest()


def verify_otp(plain_code: str, hashed_code: str) -> bool:
    """Verify OTP code against hash."""
    return hash_otp(plain_code) == hashed_code


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
