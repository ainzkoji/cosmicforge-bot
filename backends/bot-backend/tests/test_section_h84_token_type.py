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
from jose import jwt


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
