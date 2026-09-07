"""
News Latency & Timeliness Engine.

Evaluates how fresh/relevant a cluster is and penalises stale or delayed news.

Returns:
    latency_score  : 0.0 (stale) → 1.0 (real-time)
    latency_flag   : None | "STALE_NEWS" | "DELAYED_REACTION"
"""
from __future__ import annotations

import math
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple


# Thresholds
_STALE_HOURS = 24           # older than this → STALE_NEWS
_DELAYED_HOURS = 4          # older than this but < _STALE_HOURS → DELAYED_REACTION
_REAL_TIME_MINUTES = 15     # within this window → full freshness score


def _parse_utc(ts: str) -> Optional[datetime]:
    """Parse ISO datetime string to aware UTC datetime."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def compute_latency_score(
    first_seen_utc: str,
    now_utc: Optional[datetime] = None,
) -> Tuple[float, Optional[str]]:
    """
    Compute a latency_score and optional timeliness flag.

    Parameters
    ----------
    first_seen_utc : ISO string of when the cluster was first observed.
    now_utc        : Inject for testing; defaults to datetime.now(UTC).

    Returns
    -------
    (latency_score, flag)
        latency_score in [0.0, 1.0]
        flag: None | "STALE_NEWS" | "DELAYED_REACTION"
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    first_seen = _parse_utc(first_seen_utc)
    if first_seen is None:
        return 0.5, None  # unknown age → neutral

    age_hours = (now_utc - first_seen).total_seconds() / 3600.0
    age_minutes = age_hours * 60.0

    if age_hours >= _STALE_HOURS:
        # Exponential decay: score asymptotically approaches 0
        score = round(math.exp(-0.2 * (age_hours - _STALE_HOURS + 1)), 4)
        return max(0.01, score), "STALE_NEWS"

    if age_hours >= _DELAYED_HOURS:
        # Linear decay from 0.75 → 0.30 in the delayed zone
        ratio = (age_hours - _DELAYED_HOURS) / (_STALE_HOURS - _DELAYED_HOURS)
        score = round(0.75 - ratio * 0.45, 4)
        return max(0.30, score), "DELAYED_REACTION"

    if age_minutes <= _REAL_TIME_MINUTES:
        return 1.0, None

    # Between real-time window and delayed threshold
    # Smooth decay from 1.0 → 0.75
    ratio = (age_hours - _REAL_TIME_MINUTES / 60) / (_DELAYED_HOURS - _REAL_TIME_MINUTES / 60)
    score = round(1.0 - ratio * 0.25, 4)
    return max(0.75, score), None


def is_stale(latency_flag: Optional[str]) -> bool:
    return latency_flag == "STALE_NEWS"


def is_delayed(latency_flag: Optional[str]) -> bool:
    return latency_flag == "DELAYED_REACTION"
