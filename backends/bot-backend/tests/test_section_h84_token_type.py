"""Section H 8.4: the bot-backend accepts ACCESS tokens only.

``app.api.deps.get_current_active_user`` (used by the analytics-reporting and
backtesting routers) checked issuer, audience and expiry but not the token
type, so a user's 14-day REFRESH token -- same issuer and audience -- was
accepted as if it were a 15-minute access token.
"""
from __future__ import annotations

import time

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
import jwt  # PyJWT (F6)


def _token(token_type: str) -> str:
    from app.core.config import settings
    from app.core.security import AUDIENCE, ISSUER

    now = int(time.time())
    return jwt.encode(
        {"sub": "h84-user", "type": token_type, "iss": ISSUER, "aud": AUDIENCE, "iat": now, "exp": now + 600},
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


def _creds(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def test_access_token_is_accepted():
    from app.api.deps import get_current_active_user

    assert get_current_active_user(_creds(_token("access")))["id"] == "h84-user"


@pytest.mark.parametrize("token_type", ["refresh", "admin_access", "admin_mfa_challenge", None])
def test_non_access_tokens_are_rejected(token_type):
    from app.api.deps import get_current_active_user

    with pytest.raises(HTTPException) as exc:
        get_current_active_user(_creds(_token(token_type)))
    assert exc.value.status_code == 401


# --------------------------------------------------------------------------- F6: PyJWT replaces python-jose

def _claims(**overrides) -> dict:
    from app.core.security import AUDIENCE, ISSUER

    now = int(time.time())
    claims = {"sub": "h84-user", "type": "access", "iss": ISSUER, "aud": AUDIENCE, "iat": now, "exp": now + 600}
    claims.update(overrides)
    return {k: v for k, v in claims.items() if v is not None}


@pytest.mark.parametrize("missing", ["exp", "iss", "aud", "sub"])
def test_a_token_missing_a_required_claim_is_refused(missing):
    """python-jose silently ignored ``options={"require": [...]}``: a well-signed token without ``exp`` never
    expired. PyJWT enforces the list."""
    from app.core.config import settings
    from app.core.security import decode_token

    token = jwt.encode(_claims(**{missing: None}), settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    assert decode_token(token) is None


def test_foreign_keys_unsigned_tokens_and_other_algorithms_are_refused():
    import base64
    import json
    import warnings

    from app.core.config import settings
    from app.core.security import decode_token

    assert decode_token(jwt.encode(_claims(), settings.SECRET_KEY, algorithm=settings.ALGORITHM)) is not None
    assert decode_token(jwt.encode(_claims(), "x" * 48, algorithm="HS256")) is None  # another secret

    def _b64(part: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()

    assert decode_token(f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64(_claims())}.") is None
    # Same secret, other HMAC algorithm: only the configured one is accepted.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # PyJWT warns that the test secret is short for SHA-512
        assert decode_token(jwt.encode(_claims(), settings.SECRET_KEY, algorithm="HS512")) is None
    assert decode_token(jwt.encode(_claims(exp=int(time.time()) - 5), settings.SECRET_KEY,
                                   algorithm=settings.ALGORITHM)) is None  # expired
