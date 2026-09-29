"""CATI runtime configuration (env-driven, mirroring ``app/shadow/config.py``).

Library identity is EXPLICIT: a path plus an expected hash. Nothing ever
searches the disk for "any available" library, and an unset/invalid
configuration yields ``OUTCOME_LIBRARY_UNAVAILABLE`` downstream (fail
closed) -- never a partial or best-effort library.

``CATI_LIBRARY_MODE`` (default ``RUNTIME``):

* ``RUNTIME``      -- only REAL_MARKET / REPLAY_CAPTURE libraries load, and
                      ``CATI_OUTCOME_LIBRARY_EXPECTED_HASH`` is REQUIRED, so a
                      different artifact copied into the configured path can
                      never be picked up silently.
* ``TEST`` / ``DEVELOPMENT`` -- explicit override: synthetic/fixture/unknown
                      libraries may load and the expected hash is optional.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Tuple

from app.trading_intelligence.forecast.source import LibraryLoadMode

logger = logging.getLogger(__name__)

ENV_LIBRARY_PATH = "CATI_OUTCOME_LIBRARY_PATH"
ENV_LIBRARY_EXPECTED_HASH = "CATI_OUTCOME_LIBRARY_EXPECTED_HASH"
ENV_LIBRARY_MODE = "CATI_LIBRARY_MODE"

LIBRARY_NOT_CONFIGURED = "OUTCOME_LIBRARY_NOT_CONFIGURED"
LIBRARY_EXPECTED_HASH_REQUIRED = "OUTCOME_LIBRARY_EXPECTED_HASH_REQUIRED"
LIBRARY_REFUSED = "OUTCOME_LIBRARY_REFUSED"
LIBRARY_MODE_INVALID = "OUTCOME_LIBRARY_MODE_INVALID"


@dataclass(frozen=True)
class CATIConfig:
    outcome_library_path: Optional[str] = None
    outcome_library_expected_hash: Optional[str] = None
    library_mode: str = LibraryLoadMode.RUNTIME.value

    @classmethod
    def from_env(cls) -> "CATIConfig":
        path = os.environ.get(ENV_LIBRARY_PATH, "").strip() or None
        expected = os.environ.get(ENV_LIBRARY_EXPECTED_HASH, "").strip() or None
        mode = os.environ.get(ENV_LIBRARY_MODE, "").strip().upper() or LibraryLoadMode.RUNTIME.value
        return cls(outcome_library_path=path, outcome_library_expected_hash=expected, library_mode=mode)


def library_failure_code(reason: Optional[str]) -> Optional[str]:
    """The specific ForecastReasonCode behind a library that could not be used (None when it loaded)."""
    if not reason:
        return None
    r = str(reason)
    low = r.lower()
    if r in (LIBRARY_NOT_CONFIGURED, LIBRARY_EXPECTED_HASH_REQUIRED, LIBRARY_MODE_INVALID):
        return "OUTCOME_LIBRARY_NOT_CONFIGURED"
    if "hash" in low and ("mismatch" in low or "does not match" in low):
        return "OUTCOME_LIBRARY_IDENTITY_MISMATCH"
    if "incompatible" in low:
        return "OUTCOME_LIBRARY_VERSION_UNSUPPORTED"
    if "SOURCE_KIND_NOT_TRUSTED" in r:
        return "OUTCOME_LIBRARY_SOURCE_NOT_TRUSTED"
    return "OUTCOME_LIBRARY_UNAVAILABLE"


_MARKET_TYPE_TO_ASSET_CLASS = {"crypto": "CRYPTO", "forex": "FX", "fx": "FX", "futures": "FUTURES",
                               "equity": "EQUITY"}


def library_scope(manifest: Optional[dict]) -> Optional[Tuple[str, ...]]:
    """The asset classes a loaded library's evidence was built from (its manifest ``market_type``).
    A manifest that does not state it yields an EMPTY scope: such a library forecasts nothing."""
    if manifest is None:
        return None
    kind = _MARKET_TYPE_TO_ASSET_CLASS.get(str(manifest.get("market_type") or "").strip().lower())
    return (kind,) if kind else ()


def load_configured_library_with_reason(
    config: Optional[CATIConfig] = None,
) -> Tuple[Optional[object], Optional[dict], Optional[str]]:
    """(library, manifest, None) or (None, None, reason). Never raises."""
    config = config or CATIConfig.from_env()
    if not config.outcome_library_path:
        return None, None, LIBRARY_NOT_CONFIGURED
    if config.library_mode not in {m.value for m in LibraryLoadMode}:
        logger.error("[CATI] invalid %s=%r (fail closed)", ENV_LIBRARY_MODE, config.library_mode)
        return None, None, LIBRARY_MODE_INVALID
    if config.library_mode == LibraryLoadMode.RUNTIME.value and not config.outcome_library_expected_hash:
        logger.error("[CATI] %s is required in RUNTIME mode; library REFUSED (fail closed)", ENV_LIBRARY_EXPECTED_HASH)
        return None, None, LIBRARY_EXPECTED_HASH_REQUIRED
    from app.trading_intelligence.forecast.artifact import LibraryArtifactError, load_library_artifact

    try:
        library, manifest = load_library_artifact(
            config.outcome_library_path, expected_hash=config.outcome_library_expected_hash, mode=config.library_mode)
        return library, manifest, None
    except LibraryArtifactError as exc:
        logger.error("[CATI] configured outcome library REFUSED (fail closed): %s", exc)
        return None, None, f"{LIBRARY_REFUSED}: {exc}"


def load_configured_library(config: Optional[CATIConfig] = None) -> Tuple[Optional[object], Optional[dict]]:
    """(library, manifest) or (None, None): fail closed."""
    library, manifest, _reason = load_configured_library_with_reason(config)
    return library, manifest


__all__ = [
    "ENV_LIBRARY_PATH", "ENV_LIBRARY_EXPECTED_HASH", "ENV_LIBRARY_MODE", "CATIConfig",
    "LIBRARY_NOT_CONFIGURED", "LIBRARY_EXPECTED_HASH_REQUIRED", "LIBRARY_REFUSED", "LIBRARY_MODE_INVALID",
    "load_configured_library", "load_configured_library_with_reason", "library_failure_code", "library_scope",
]
