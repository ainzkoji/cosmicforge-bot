"""Two small decisions every HTTP service makes the same way.

``api_docs_enabled``: whether ``/docs``, ``/redoc`` and ``/openapi.json`` are
served. They enumerate the whole API, so a production process serves them only
when ``API_DOCS_ENABLED`` is set explicitly. Fails closed: a settings object
without a ``production`` flag counts as production.

``browser_origins``: which browser origins may call the API with credentials.
The configured public address of the portal (``FRONTEND_URL``,
``PUBLIC_APP_URL``) and any ``CORS_ALLOWED_ORIGINS`` are always honoured; the
development origins are added only outside production. A wildcard is never
accepted, and an entry that is not a bare ``http(s)://host[:port]`` origin is
dropped rather than guessed at.
"""
from __future__ import annotations

import os
from typing import Any, Iterable, List, Mapping, Optional
from urllib.parse import urlsplit

DEVELOPMENT_ORIGINS = ("http://localhost:5173", "http://127.0.0.1:5173", "http://localhost:4173", "http://127.0.0.1:4173")
_TRUE = {"1", "true", "yes", "on"}


def _flag(value: Any) -> bool:
    return value is True or str(value or "").strip().lower() in _TRUE


def api_docs_enabled(settings: Any, environ: Optional[Mapping[str, str]] = None) -> bool:
    env = os.environ if environ is None else environ
    explicit = getattr(settings, "API_DOCS_ENABLED", None)
    if explicit is None:
        explicit = env.get("API_DOCS_ENABLED")
    return (not getattr(settings, "production", True)) or _flag(explicit)


def docs_kwargs(settings: Any, environ: Optional[Mapping[str, str]] = None) -> dict:
    """Keyword arguments for ``FastAPI(...)``."""
    enabled = api_docs_enabled(settings, environ)
    return {"docs_url": "/docs" if enabled else None, "redoc_url": "/redoc" if enabled else None,
            "openapi_url": "/openapi.json" if enabled else None}


def normalise_origin(value: Any) -> Optional[str]:
    """``scheme://host[:port]`` of ``value``, or None when it is not a usable origin."""
    text = str(value or "").strip().rstrip("/")
    if not text or "*" in text:
        return None
    parts = urlsplit(text)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
        return None
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        return None
    try:
        port = parts.port
    except ValueError:
        return None
    return f"{parts.scheme}://{parts.hostname}" + (f":{port}" if port else "")


def _values(*sources: Any) -> Iterable[str]:
    for source in sources:
        for item in str(source or "").split(","):
            if item.strip():
                yield item.strip()


def browser_origins(settings: Any, environ: Optional[Mapping[str, str]] = None) -> List[str]:
    env = os.environ if environ is None else environ

    def read(name: str) -> str:
        return str(getattr(settings, name, None) or env.get(name) or "")

    configured = [o for o in (normalise_origin(v) for v in _values(read("FRONTEND_URL"), read("PUBLIC_APP_URL"),
                                                                  read("CORS_ALLOWED_ORIGINS"))) if o]
    production = bool(getattr(settings, "production", True))
    origins = list(configured)
    if not production or not configured:
        # Outside production, and for an installation that configured no public
        # address at all (the behaviour before this function existed).
        origins += list(DEVELOPMENT_ORIGINS)
    return list(dict.fromkeys(origins))


__all__ = ["DEVELOPMENT_ORIGINS", "api_docs_enabled", "docs_kwargs", "normalise_origin", "browser_origins"]
