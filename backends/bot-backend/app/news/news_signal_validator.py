"""
Signal Validity Engine — Final decision layer.

Aggregates all engine outputs to determine:
  - is_valid_signal  : bool
  - data_quality_status : DataQualityStatus enum string

DataQualityStatus enum:
    HIGH_CONFIDENCE   – trusted sources, clean, fresh, non-manipulated
    MEDIUM_CONFIDENCE – decent sources, minor freshness/spam issues
    LOW_CONFIDENCE    – weak signals, low source reliability
    SPAM              – classified as spam cluster
    MANIPULATED       – manipulation flag fired
    STALE             – news is too old to be actionable

Priority (descending): MANIPULATED > SPAM > STALE > HIGH/MEDIUM/LOW_CONFIDENCE
"""
from __future__ import annotations

from typing import Optional, Tuple

from app.news.news_manipulation_detector import flag_severity


# Minimum thresholds to be considered a valid signal
_MIN_CLUSTER_CONFIDENCE = 0.40
_MIN_RELIABILITY        = 0.45
_MAX_SPAM_SCORE         = 0.45
_MIN_LATENCY_SCORE      = 0.30


class DataQualityStatus:
    HIGH_CONFIDENCE   = "HIGH_CONFIDENCE"
    MEDIUM_CONFIDENCE = "MEDIUM_CONFIDENCE"
    LOW_CONFIDENCE    = "LOW_CONFIDENCE"
    SPAM              = "SPAM"
    MANIPULATED       = "MANIPULATED"
    STALE             = "STALE"


def evaluate_signal_validity(
    *,
    cluster_confidence: float,
    reliability_score: float,
    spam_score: float,
    latency_score: float,
    manipulation_flag: Optional[str],
    latency_flag: Optional[str],
    sentiment_confidence: float = 0.0,
    is_blocked_source: bool = False,
) -> Tuple[bool, str]:
    """
    Determine is_valid_signal and data_quality_status.

    Returns
    -------
    (is_valid, data_quality_status)
    """

    # ── Hard disqualifiers ──────────────────────────────────────────────────
    if is_blocked_source:
        return False, DataQualityStatus.SPAM

    if manipulation_flag in ("POSSIBLE_MANIPULATION", "BOT_AMPLIFICATION"):
        return False, DataQualityStatus.MANIPULATED

    if spam_score >= _MAX_SPAM_SCORE:
        return False, DataQualityStatus.SPAM

    if latency_flag == "STALE_NEWS":
        return False, DataQualityStatus.STALE

    # ── Soft scoring ────────────────────────────────────────────────────────
    # Combined quality score (weighted)
    quality = (
        cluster_confidence  * 0.35
        + reliability_score * 0.30
        + latency_score     * 0.20
        + (1 - spam_score)  * 0.15
    )

    # Downgrade if rumour or low-confidence manipulation flag is present
    if manipulation_flag in ("RUMOR_ONLY", "LOW_CONFIDENCE_EVENT"):
        quality *= 0.65

    if latency_flag == "DELAYED_REACTION":
        quality *= 0.85

    # ── Classify ────────────────────────────────────────────────────────────
    if (
        quality >= 0.70
        and reliability_score >= _MIN_RELIABILITY
        and cluster_confidence >= _MIN_CLUSTER_CONFIDENCE
        and spam_score < 0.20
        and latency_flag is None
    ):
        return True, DataQualityStatus.HIGH_CONFIDENCE

    if quality >= 0.50 and reliability_score >= 0.35:
        return True, DataQualityStatus.MEDIUM_CONFIDENCE

    # Soft medium gate — valid but flagged (e.g. RUMOR_ONLY degraded signals)
    if quality >= 0.35 and reliability_score >= 0.30:
        return False, DataQualityStatus.MEDIUM_CONFIDENCE

    if quality >= 0.30:
        return False, DataQualityStatus.LOW_CONFIDENCE

    return False, DataQualityStatus.LOW_CONFIDENCE


def is_signal_reportable(
    data_quality_status: str,
    manipulation_flag: Optional[str],
) -> bool:
    """
    Should this signal be surfaced in the admin dashboard?
    We always surface signals for observability — even bad ones —
    but mark them clearly so analysts know to ignore them.
    """
    return True  # Always show — admins need full visibility
