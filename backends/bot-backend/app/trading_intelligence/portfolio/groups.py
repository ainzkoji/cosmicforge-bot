"""Instrument-group metadata (Sections 12.5 cohort dimension, 16.9 sector/
theme). Reuses the repo's existing static correlation groups from
``app/risk/correlation_filter.py`` -- no second taxonomy is invented. Unknown
symbols stay ``UNKNOWN`` (never guessed)."""
from __future__ import annotations

from typing import Optional

UNKNOWN_GROUP = "UNKNOWN"


def static_group_for(venue_symbol: Optional[str]) -> str:
    if not venue_symbol:
        return UNKNOWN_GROUP
    try:
        from app.risk.correlation_filter import _SYMBOL_TO_GROUP
    except Exception:  # pragma: no cover - the module is part of the repo
        return UNKNOWN_GROUP
    group = _SYMBOL_TO_GROUP.get(str(venue_symbol).upper())
    return UNKNOWN_GROUP if group is None else f"STATIC_GROUP_{group}"


__all__ = ["UNKNOWN_GROUP", "static_group_for"]
