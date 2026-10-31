"""The JWT library every CosmicForge service uses: PyJWT (F6, replaces python-jose).

python-jose has an advisory with no fixed release (PYSEC-2026-4298 / CVE-2026-85394, HMAC
key confusion) and pulls in ``ecdsa``, which has another (CVE-2024-23342, Minerva timing). Our
tokens are HS256 only, so PyJWT -- already installed as a firebase-admin dependency -- does the
same job with neither package.

The names below keep the call sites as they were (``jwt.encode`` / ``jwt.decode`` and the three
exception classes the decoders catch). One behaviour is intentionally STRICTER than before:
python-jose silently ignored ``options={"require": [...]}``, so a correctly signed token without
``exp`` (or ``iss``/``aud``/``sub``) used to pass ``decode_token``. PyJWT enforces the list.
"""
from __future__ import annotations

import jwt
from jwt import ExpiredSignatureError
from jwt import PyJWTError as JWTError
from jwt.exceptions import (
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidIssuerError,
    InvalidJTIError,
    InvalidSubjectError,
    MissingRequiredClaimError,
)

#: Everything python-jose reported as ``JWTClaimsError``: a well-signed token whose claims are wrong.
JWTClaimsError = (
    InvalidAudienceError,
    InvalidIssuerError,
    MissingRequiredClaimError,
    ImmatureSignatureError,
    InvalidIssuedAtError,
    InvalidSubjectError,
    InvalidJTIError,
)


def unverified_claims(token: str) -> dict:
    """The claims of a token WITHOUT checking its signature: for logging and tests, never for trust."""
    return jwt.decode(token, options={"verify_signature": False})


__all__ = ["ExpiredSignatureError", "JWTClaimsError", "JWTError", "jwt", "unverified_claims"]
