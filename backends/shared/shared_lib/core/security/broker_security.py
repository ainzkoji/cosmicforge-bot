"""Broker credential encryption (Fernet) with a fail-closed production key.

Key policy
----------
* ``BROKER_SECRET_KEY`` is the ONLY source of the broker-credential key. A
  valid Fernet key is used as-is; any other non-empty value is stretched
  with SHA-256 (the secret is still BROKER_SECRET_KEY, never a generic
  application secret).
* Production (``APP_ENV``/``ENVIRONMENT_NAME`` in {production, prod, live}
  or ``DATABASE_ROLE=live``) without ``BROKER_SECRET_KEY`` is a configuration
  error: ``assert_broker_encryption_configured()`` raises (call it at
  startup) and every encrypt/decrypt raises ``BrokerEncryptionConfigError``.
  There is no silent fallback key in production.
* Outside production, a missing ``BROKER_SECRET_KEY`` falls back to the
  historical development derivation so existing local databases still open.
  A warning is logged once.

Migration compatibility
-----------------------
Earlier revisions encrypted with ``sha256(settings.SECRET_KEY)`` or the
constant ``b"0" * 32`` when ``BROKER_SECRET_KEY`` was absent or not a Fernet
key. Those keys are kept as DECRYPT-ONLY legacy keys:

* outside production they are always tried after the primary key;
* in production only while ``BROKER_LEGACY_KEY_DECRYPT=1`` is set (the
  migration window). ``scripts/rotate_broker_credential_encryption.py``
  re-encrypts every stored blob under the primary key, after which the flag
  is removed.

New blobs are ALWAYS written with the primary key.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

ENV_KEY = "BROKER_SECRET_KEY"
ENV_LEGACY_DECRYPT = "BROKER_LEGACY_KEY_DECRYPT"
_PRODUCTION_NAMES = {"production", "prod", "live"}
_TRUE = {"1", "true", "yes", "on"}
_ZERO_KEY = base64.urlsafe_b64encode(b"0" * 32)
_warned_dev_fallback = False


class BrokerEncryptionConfigError(RuntimeError):
    """Broker-credential encryption is not safely configured."""


def _settings_value(name: str) -> Optional[str]:
    try:
        from app.core.config import settings  # type: ignore[import]

        value = getattr(settings, name, None)
        return str(value) if value not in (None, "") else None
    except Exception:
        return None


def is_production() -> bool:
    for name in ("APP_ENV", "ENVIRONMENT_NAME"):
        value = os.getenv(name) or _settings_value(name)
        if value and value.strip().lower() in _PRODUCTION_NAMES:
            return True
    role = os.getenv("DATABASE_ROLE") or _settings_value("DATABASE_ROLE")
    return bool(role and role.strip().lower() == "live")


def _as_fernet_key(secret: str) -> bytes:
    raw = secret.encode() if isinstance(secret, str) else secret
    try:
        Fernet(raw)
        return raw
    except Exception:
        return base64.urlsafe_b64encode(hashlib.sha256(raw).digest())


def _legacy_keys() -> List[bytes]:
    keys: List[bytes] = []
    app_secret = _settings_value("SECRET_KEY")
    if app_secret:
        keys.append(base64.urlsafe_b64encode(hashlib.sha256(app_secret.encode()).digest()))
    keys.append(_ZERO_KEY)
    return keys


def _primary_key() -> bytes:
    global _warned_dev_fallback
    secret = os.getenv(ENV_KEY)
    if secret:
        return _as_fernet_key(secret)
    if is_production():
        raise BrokerEncryptionConfigError(
            f"{ENV_KEY} is required in production; refusing to use a fallback broker-credential key")
    if not _warned_dev_fallback:
        logger.warning("%s not set: using the DEVELOPMENT credential key derivation (never in production)", ENV_KEY)
        _warned_dev_fallback = True
    return _legacy_keys()[0]


def _decrypt_keys() -> List[bytes]:
    keys = [_primary_key()]
    legacy_allowed = (not is_production()) or (os.getenv(ENV_LEGACY_DECRYPT, "").strip().lower() in _TRUE)
    if legacy_allowed:
        keys.extend(k for k in _legacy_keys() if k not in keys)
    return keys


def assert_broker_encryption_configured() -> None:
    """Startup check. Raises in production without a dedicated key."""
    _primary_key()


def encryption_status() -> Dict[str, Any]:
    """Non-secret description of the active key configuration (for health checks)."""
    production = is_production()
    return {
        "production": production,
        "dedicated_key_configured": bool(os.getenv(ENV_KEY)),
        "legacy_decrypt_enabled": (not production) or (os.getenv(ENV_LEGACY_DECRYPT, "").strip().lower() in _TRUE),
    }


def encrypt_credentials(credentials: Dict[str, Any]) -> str:
    """Encrypt a credentials dict into a Fernet token (always the primary key)."""
    f = Fernet(_primary_key())
    return f.encrypt(json.dumps(credentials).encode("utf-8")).decode("utf-8")


def decrypt_credentials_strict(encrypted_blob: str) -> Dict[str, Any]:
    """Decrypt or raise. Configuration errors propagate; a blob no allowed
    key can open raises ``InvalidToken``."""
    token = encrypted_blob.encode("utf-8") if isinstance(encrypted_blob, str) else encrypted_blob
    last: Optional[Exception] = None
    for key in _decrypt_keys():
        try:
            return json.loads(Fernet(key).decrypt(token).decode("utf-8"))
        except InvalidToken as exc:
            last = exc
    raise last or InvalidToken()


def decrypt_credentials(encrypted_blob: str) -> Dict[str, Any]:
    """Decrypt into a dict; ``{}`` when no allowed key opens the blob.

    A production configuration error is NOT swallowed: it raises
    ``BrokerEncryptionConfigError`` so callers cannot mistake a missing key
    for a corrupt credential.
    """
    try:
        return decrypt_credentials_strict(encrypted_blob)
    except BrokerEncryptionConfigError:
        raise
    except Exception:
        return {}


def needs_reencryption(encrypted_blob: str) -> bool:
    """True when the blob opens only under a legacy key."""
    try:
        Fernet(_primary_key()).decrypt(encrypted_blob.encode("utf-8"))
        return False
    except InvalidToken:
        return True


def mask_credentials(credentials: Dict[str, Any]) -> str:
    """A masked display form of the primary key (e.g. ``...a1b2``)."""
    login = credentials.get("login")
    if login:
        return str(login)  # MT login is a public account number
    key = credentials.get("api_key") or credentials.get("public_key") or credentials.get("key") or credentials.get("ClientId")
    if key and isinstance(key, str) and len(key) > 8:
        return f"...{key[-4:]}"
    elif key:
        return "***"
    return "..."


__all__ = [
    "BrokerEncryptionConfigError",
    "assert_broker_encryption_configured",
    "decrypt_credentials",
    "decrypt_credentials_strict",
    "encrypt_credentials",
    "encryption_status",
    "is_production",
    "mask_credentials",
    "needs_reencryption",
]
