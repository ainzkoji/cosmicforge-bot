"""A production engine must not start on the built-in / placeholder SECRET_KEY.

The engine verifies user JWTs with SECRET_KEY; with the public default anyone
can forge a session. Non-production environments keep working with defaults.
"""
import secrets

import pytest

from app.core.config import Settings, weak_secret_reason


def _strong() -> str:
    return secrets.token_urlsafe(48)


@pytest.mark.parametrize("value", [
    "", None, "changeme_in_production_secret_key", "CHANGE_ME_BEFORE_PRODUCTION",
    "default-engine-key", "short", "k" * 44,
])
def test_weak_secrets_are_recognised(value):
    assert weak_secret_reason(value)


def test_random_secrets_are_accepted():
    assert weak_secret_reason(_strong()) is None
    assert weak_secret_reason(secrets.token_hex(32)) is None


@pytest.mark.parametrize("secret_key", [None, "", "CHANGE_ME_BEFORE_PRODUCTION", "too-short"])
def test_production_refuses_to_start_without_a_real_secret_key(secret_key):
    values = {} if secret_key is None else {"SECRET_KEY": secret_key}  # None -> built-in default
    settings = Settings.model_construct(APP_ENV="PRODUCTION", **values)

    with pytest.raises(ValueError, match="SECRET_KEY") as raised:
        settings.assert_production_secrets()
    assert "secrets.token_urlsafe" in str(raised.value)
    # validate_runtime (called by the API startup hook) is fatal too, not a warning.
    with pytest.raises(ValueError, match="INSECURE_PRODUCTION_SECRETS"):
        settings.validate_runtime()


def test_production_starts_with_a_strong_secret_key():
    settings = Settings.model_construct(APP_ENV="PRODUCTION", SECRET_KEY=_strong())

    assert settings.production_secret_errors() == []
    settings.assert_production_secrets()
    assert isinstance(settings.validate_runtime(), list)


@pytest.mark.parametrize("app_env", ["TEST", "DEVELOPMENT"])
def test_non_production_keeps_working_with_defaults(app_env):
    settings = Settings.model_construct(APP_ENV=app_env)

    assert settings.production_secret_errors() == []
    settings.assert_production_secrets()
    assert isinstance(settings.validate_runtime(), list)
