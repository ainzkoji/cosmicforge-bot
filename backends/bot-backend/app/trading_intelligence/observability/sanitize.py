"""Secret sanitization for CATI evidence payloads and structured logs (21.16,
21.17, 21.20). Reuses the canonical message sanitizer
(``integration/errors.sanitize_message``) for free-form text and adds a
STRUCTURAL pass for payloads:

* any key that names a credential (api key, secret, password, token,
  authorization, JWT, private key, signature, listen key, cookie ...) has
  its value replaced by ``[REDACTED]`` -- whatever the value looks like;
* every string value is scrubbed of bearer headers, JWTs, ``key=value``
  credential fragments and mixed-case credential-shaped blobs.

Analytical content hashes (lowercase hex) and CATI ids (``prefix_<hex>``)
are deliberately NOT treated as secrets, so sanitizing evidence never
changes lineage identity.
"""
from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"

_SECRET_KEY = re.compile(
    r"(?i)(api[_-]?key|apikey|api[_-]?secret|secret|passw(or)?d|pwd|token|authoriz|auth[_-]?header|jwt|bearer|"
    r"private[_-]?key|signature|listen[_-]?key|credential|cookie|session[_-]?key|x-mbx-apikey)")

_VALUE_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+\S+"),
    re.compile(r"\beyJ[\w-]+\.[\w-]+\.[\w-]+"),
    re.compile(r"(?i)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(-----END [A-Z ]*PRIVATE KEY-----|$)", re.S),
    re.compile(r"(?i)\b(api[_-]?key|api[_-]?secret|apikey|secret|token|password|passwd|signature|authorization|"
               r"private[_-]?key|listenkey|x-mbx-apikey)\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)(signature|apikey|api_key|recvwindow)=[^&\s]+"),
)
_MIXED_BLOB = re.compile(r"\b[A-Za-z0-9+/_-]{32,}={0,2}")


def _is_credential_blob(token: str) -> bool:
    """A long token with BOTH upper- and lower-case letters (API keys,
    base64 secrets). Lowercase hex hashes and CATI ids never match."""
    return any(c.isupper() for c in token) and any(c.islower() for c in token)


def sanitize_text(value: str) -> str:
    text = str(value)
    for pattern in _VALUE_PATTERNS:
        text = pattern.sub(REDACTED, text)
    return _MIXED_BLOB.sub(lambda m: REDACTED if _is_credential_blob(m.group(0)) else m.group(0), text)


def is_secret_key(key: Any) -> bool:
    k = str(key)
    if k.endswith("_hash") or k.endswith("_id") or k in ("hash", "data_hash", "payload_hash"):
        return False
    return bool(_SECRET_KEY.search(k))


def sanitize_payload(value: Any) -> Any:
    """Recursively redact a JSON-able payload. Never raises."""
    try:
        if isinstance(value, dict):
            return {k: (REDACTED if is_secret_key(k) else sanitize_payload(v)) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [sanitize_payload(v) for v in value]
        if isinstance(value, str):
            return sanitize_text(value)
        return value
    except Exception:  # pragma: no cover - sanitization must never break evidence
        return REDACTED


__all__ = ["REDACTED", "sanitize_text", "sanitize_payload", "is_secret_key"]
