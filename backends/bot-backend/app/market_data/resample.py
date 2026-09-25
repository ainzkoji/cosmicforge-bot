"""Deterministic, causal resampling of 1m bars (Phase 4H).

Wraps ``app.research.dataset.derive`` (the canonical rule: only complete,
aligned, gapless windows) and adds the causal cut: a derived bar exists only
once its window has CLOSED at or before ``as_of_ms``. A bar can therefore
never contain information from after the decision time, and re-running on
the same 1m input always yields the same bars and the same hash.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Sequence

from app.research.dataset import DERIVABLE, DatasetError, close_time, derive

RESAMPLE_RULE_VERSION = "resample-1m-v1"


def derive_causal(rows_1m: Sequence[Any], timeframe: str, *, as_of_ms: Optional[int] = None) -> List[list]:
    if timeframe == "1m":
        bars = [list(r) for r in rows_1m]
    else:
        bars = derive(rows_1m, timeframe)
    if as_of_ms is not None:
        bars = [b for b in bars if close_time(b) <= as_of_ms]
    return bars


def series_hash(rows: Sequence[Any]) -> str:
    h = hashlib.sha256()
    for r in rows:
        h.update(json.dumps([str(x) for x in list(r)[:7]]).encode())
    return h.hexdigest()


def resample_all(rows_1m: Sequence[Any], timeframes: Sequence[str], *, as_of_ms: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """{timeframe: {"rows": [...], "hash": ..., "derived_from_hash": ..., "rule": ...}}."""
    base_hash = series_hash(rows_1m)
    out: Dict[str, Dict[str, Any]] = {}
    for tf in timeframes:
        if tf != "1m" and tf not in DERIVABLE:
            raise DatasetError(f"{tf} is not derivable from 1m")
        rows = derive_causal(rows_1m, tf, as_of_ms=as_of_ms)
        out[tf] = {"rows": rows, "hash": series_hash(rows), "derived_from_hash": base_hash,
                   "rule": RESAMPLE_RULE_VERSION}
    return out


__all__ = ["RESAMPLE_RULE_VERSION", "derive_causal", "resample_all", "series_hash"]
