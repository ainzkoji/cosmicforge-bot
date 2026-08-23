from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


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


class Settings(BaseSettings):
    """Admin Backend shell configuration.

    The default database URL intentionally matches the existing local shared DB.
    Production secrets must be supplied by the deployment environment.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
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


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
