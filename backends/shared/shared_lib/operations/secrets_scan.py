"""Secret detection shared by the support desk, the status page and the backup/restore pipeline (Step 8.5).

Two questions, answered without ever returning the secret itself:

* ``find_secrets(text)`` -- does free text (a support ticket, a public incident notice, a file in a backup
  staging directory) contain something that looks like a credential? Returns finding *kinds*
  (``API_KEY_ASSIGNMENT``, ``PRIVATE_KEY_BLOCK`` ...) and never the matched value.
* ``classify_stored_secret(value)`` -- is a value stored in a credential column ciphertext (Fernet, the
  ``v1:``/``v2:``/``enc:`` envelopes used by the user-backend, the base64-wrapped Fernet of KYC PII), a one-way
  hash, a masked display string, empty -- or PLAINTEXT?

High-entropy detection is deliberately conservative for free text: a 32+ character token mixing upper case,
lower case and digits (the shape of exchange API keys and secrets) is flagged; a lower-case hex transaction hash,
a UUID or an order number is not.
"""
from __future__ import annotations

import base64
import binascii
import re
from typing import List, Optional

API_KEY_ASSIGNMENT = "API_KEY_ASSIGNMENT"
PRIVATE_KEY_BLOCK = "PRIVATE_KEY_BLOCK"
CLOUD_ACCESS_KEY = "CLOUD_ACCESS_KEY"
PAYMENT_PROVIDER_KEY = "PAYMENT_PROVIDER_KEY"
PLATFORM_TOKEN = "PLATFORM_TOKEN"
JSON_WEB_TOKEN = "JSON_WEB_TOKEN"
BOT_TOKEN = "BOT_TOKEN"
HIGH_ENTROPY_TOKEN = "HIGH_ENTROPY_TOKEN"
ENV_FILE_SECRET = "ENV_FILE_SECRET"
CONNECTION_STRING_PASSWORD = "CONNECTION_STRING_PASSWORD"

_SECRET_NAME = (r"(?:api[\s_\-]?key|api[\s_\-]?secret|secret[\s_\-]?key|client[\s_\-]?secret|private[\s_\-]?key|"
                r"pass(?:word|phrase)|passwd|access[\s_\-]?token|refresh[\s_\-]?token|auth[\s_\-]?token|"
                r"bearer|token|secret|seed[\s_\-]?phrase|mnemonic|2fa[\s_\-]?code|totp|otp)")

_TEXT_PATTERNS = (
    (API_KEY_ASSIGNMENT, re.compile(rf"(?i)\b{_SECRET_NAME}\b[\"']?\s*(?:[:=]|\bis\b)\s*[\"']?[^\s\"',;]{{8,}}")),
    (PRIVATE_KEY_BLOCK, re.compile(r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----")),
    (CLOUD_ACCESS_KEY, re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    (PAYMENT_PROVIDER_KEY, re.compile(r"\b(?:sk|rk|pk)_(?:live|test)_[0-9A-Za-z]{16,}\b|\bwhsec_[0-9A-Za-z]{16,}\b")),
    (PLATFORM_TOKEN, re.compile(r"\b(?:ghp|gho|ghs|github_pat|glpat|xox[abprs])[_-][0-9A-Za-z_\-]{16,}\b")),
    (JSON_WEB_TOKEN, re.compile(r"\beyJ[A-Za-z0-9_\-]{8,}\.eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}")),
    (BOT_TOKEN, re.compile(r"\b\d{6,}:[A-Za-z0-9_\-]{30,}\b")),
    (CONNECTION_STRING_PASSWORD, re.compile(r"(?i)\b[a-z][a-z0-9+.\-]*://[^\s/@:]+:[^\s/@]+@")),
)
_TOKEN = re.compile(r"[A-Za-z0-9_\-]{32,}")
#: ``NAME=value`` lines of an env file whose NAME says it is a secret.
_ENV_LINE = re.compile(r"(?im)^\s*(?:export\s+)?[A-Z][A-Z0-9_]*(?:KEY|SECRET|TOKEN|PASSWORD|PASS|PASSPHRASE|DSN|"
                       r"WEBHOOK_URL|DATABASE_URL)\s*=\s*\S{6,}")


def _high_entropy(token: str) -> bool:
    return (any(c.isupper() for c in token) and any(c.islower() for c in token) and any(c.isdigit() for c in token))


def find_secrets(text: Optional[str], *, include_env_lines: bool = True) -> List[str]:
    """Finding kinds (sorted, unique) for anything in ``text`` that looks like a credential. Never values."""
    if not text:
        return []
    found = set()
    for kind, pattern in _TEXT_PATTERNS:
        if pattern.search(text):
            found.add(kind)
    if include_env_lines and _ENV_LINE.search(text):
        found.add(ENV_FILE_SECRET)
    for token in _TOKEN.findall(text):
        if _high_entropy(token):
            found.add(HIGH_ENTROPY_TOKEN)
            break
    return sorted(found)


# ── stored values ─────────────────────────────────────────────────────────────────────────────────────────
EMPTY, CIPHERTEXT, HASH, MASKED, PLAINTEXT = "EMPTY", "CIPHERTEXT", "HASH", "MASKED", "PLAINTEXT"
_HEX_DIGEST = re.compile(r"^[0-9a-f]{40}$|^[0-9a-f]{64}$|^[0-9a-f]{128}$", re.I)
_PASSWORD_HASH = re.compile(r"^\$(?:2[abxy]|argon2(?:id|i|d)|pbkdf2(?:-sha\d+)?|scrypt|5|6)\$|^pbkdf2_sha\d+\$")


def is_fernet_token(value: str) -> bool:
    """Structure of a Fernet token: version byte 0x80, timestamp, IV, >=1 AES block, HMAC (57 + 16n bytes)."""
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii") + b"=" * (-len(value) % 4))
    except (binascii.Error, ValueError, UnicodeEncodeError):
        return False
    return len(raw) >= 73 and raw[0] == 0x80 and (len(raw) - 57) % 16 == 0


def classify_stored_secret(value) -> str:
    if value is None:
        return EMPTY
    if isinstance(value, (bytes, bytearray)):
        try:
            value = bytes(value).decode("ascii")
        except UnicodeDecodeError:
            return PLAINTEXT
    text = str(value).strip()
    if text in ("", "REDACTED"):
        return EMPTY
    body = text
    if body.startswith("enc:"):
        body = body[4:]
    if body[:3] in ("v1:", "v2:"):
        body = body[3:]
    if is_fernet_token(body):
        return CIPHERTEXT
    try:   # KYC PII: base64(Fernet token text)
        inner = base64.urlsafe_b64decode(body.encode("ascii") + b"=" * (-len(body) % 4)).decode("ascii")
        if is_fernet_token(inner):
            return CIPHERTEXT
    except (binascii.Error, ValueError, UnicodeError):
        pass
    digest = text.split(":", 1)[1] if text.lower().startswith(("sha256:", "sha512:", "sha1:")) else text
    if _PASSWORD_HASH.match(text) or _HEX_DIGEST.match(digest):
        return HASH
    if "*" in text or "•" in text:
        visible = sum(1 for c in text if c not in "*•.")
        if visible <= 8:
            return MASKED
    return PLAINTEXT
