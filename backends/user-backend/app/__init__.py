# CosmicForge User Backend - Public API Service

# Every entry point (server, `python -m app...`, scripts, subprocess tests) resolves the canonical
# backends/shared package from this checkout, never from a stale editable install elsewhere.
import sys as _sys
from pathlib import Path as _Path

_SHARED = _Path(__file__).resolve().parents[2] / "shared"
if _SHARED.is_dir() and str(_SHARED) not in _sys.path:
    _sys.path.insert(0, str(_SHARED))
