"""A production admin API must not verify tokens with the built-in / placeholder SECRET_KEY."""
from __future__ import annotations

import secrets
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "admin-backend"))
sys.path.insert(0, str(ROOT / "backends" / "shared"))

from app.core.config import Settings, weak_secret_reason  # noqa: E402


def _strong() -> str:
    return secrets.token_urlsafe(48)


@pytest.mark.parametrize("value", [
    "", None, "changeme_in_production_secret_key", "CHANGE_ME_BEFORE_PRODUCTION", "short", "k" * 44,
])
def test_weak_secrets_are_recognised(value):
    assert weak_secret_reason(value)


@pytest.mark.parametrize("secret_key", [None, "", "CHANGE_ME_BEFORE_PRODUCTION", "too-short"])
def test_production_refuses_to_start_without_a_real_secret_key(secret_key):
    values = {} if secret_key is None else {"SECRET_KEY": secret_key}  # None -> built-in default
    settings = Settings.model_construct(APP_ENV="PRODUCTION", **values)

    with pytest.raises(ValueError, match="SECRET_KEY") as raised:
        settings.assert_production_secrets()
    assert "secrets.token_urlsafe" in str(raised.value)


def test_production_starts_with_a_strong_secret_key():
    settings = Settings.model_construct(APP_ENV="PRODUCTION", SECRET_KEY=_strong())

    assert settings.production_secret_errors() == []
    settings.assert_production_secrets()


@pytest.mark.parametrize("app_env", ["TEST", "DEVELOPMENT"])
def test_non_production_keeps_working_with_defaults(app_env):
    settings = Settings.model_construct(APP_ENV=app_env)

    assert settings.production_secret_errors() == []
    settings.assert_production_secrets()
