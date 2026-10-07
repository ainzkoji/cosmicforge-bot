from __future__ import annotations
from pathlib import Path
from shared_lib.core.production import ProductionSettings


import json
from functools import lru_cache
from typing import Any

from pydantic_settings import SettingsConfigDict


# --- Production secret validation ---
# Built-in defaults and the placeholders shipped in the .env.example files are
# public: anyone can forge a session with them. A production process refuses
# to start with one. Non-production (APP_ENV=TEST / DEVELOPMENT) is unaffected.
MIN_SECRET_LENGTH = 32
_SECRET_PLACEHOLDER_MARKERS = (
    "changeme", "change_me", "change-me", "replace_me", "replace-me",
    "placeholder", "example", "default-",
)
_SECRET_PLACEHOLDER_PREFIXES = ("your-", "your_", "<")
_SECRET_HOWTO = 'python -c "import secrets; print(secrets.token_urlsafe(48))"'
# Secrets this service signs or encrypts with.
REQUIRED_PRODUCTION_SECRETS = ("SECRET_KEY",)


def weak_secret_reason(value):
    """Why ``value`` is unacceptable as a production secret, or None if it is fine."""
    text = str(value or "").strip()
    if not text:
        return "is not set"
    lowered = text.lower()
    if lowered.startswith(_SECRET_PLACEHOLDER_PREFIXES) or any(
            marker in lowered for marker in _SECRET_PLACEHOLDER_MARKERS):
        return "is a known default/placeholder value"
    if len(text) < MIN_SECRET_LENGTH:
        return f"is shorter than {MIN_SECRET_LENGTH} characters"
    if len(set(text)) < 8:
        return "is not random (too few distinct characters)"
    return None


def _secret_errors(production: bool, secrets_by_name: dict) -> list:
    if not production:
        return []
    errors = []
    for name, value in secrets_by_name.items():
        reason = weak_secret_reason(value)
        if reason:
            errors.append(f"{name} {reason}. Generate one with: {_SECRET_HOWTO}")
    return errors



def _parse_origins(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    raw = str(value).strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except Exception:
            pass
    return [item.strip() for item in raw.split(",") if item.strip()]


class Settings(ProductionSettings):
    """Admin Backend shell configuration.

    The default database URL intentionally matches the existing local shared DB.
    Production secrets must be supplied by the deployment environment.
    """

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
        enable_decoding=False,
    )

    ADMIN_BACKEND_PORT: int = 8100
    DATABASE_URL: str = "sqlite:///../shared/shared_lib/persistence/cosmicforge.db"
    SECRET_KEY: str = "changeme_in_production_secret_key"
    ALGORITHM: str = "HS256"
    ADMIN_CORS_ORIGINS: str = (
        "http://localhost:4173,http://127.0.0.1:4173,"
        "http://localhost:5173,http://127.0.0.1:5173"
    )
    USER_BACKEND_URL: str = "http://localhost:8000"
    BOT_BACKEND_URL: str = "http://localhost:9000"
    SERVICE_AUTH_TOKEN: str = ""
    SQLITE_BUSY_TIMEOUT_MS: int = 10000
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15

    @property
    def cors_origins(self) -> list[str]:
        return _parse_origins(self.ADMIN_CORS_ORIGINS)

    def production_secret_errors(self) -> list:
        return _secret_errors(
            self.production, {name: getattr(self, name, "") for name in REQUIRED_PRODUCTION_SECRETS})

    def assert_production_secrets(self) -> None:
        """Refuse to run a production process on default, placeholder or short secrets."""
        errors = self.production_secret_errors()
        if errors:
            raise ValueError(
                "INSECURE_PRODUCTION_SECRETS: refusing to start with APP_ENV=PRODUCTION:\n"
                + "\n".join(f"- {e}" for e in errors)
                + "\nAll CosmicForge services must be given the SAME SECRET_KEY "
                  "(they verify each other's tokens)."
            )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
# Import-time: a production admin API never verifies tokens with the built-in
# default SECRET_KEY.
settings.assert_production_secrets()
