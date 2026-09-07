"""
News Signal Effectiveness Scorer + False Signal Detector.

Aggregates all engine outputs into a final effectiveness score
and classifies false/no-impact signals.

Formula:
  signal_effectiveness_score =
      impact_score            * 0.35
    + sentiment_accuracy_score * 0.30
    + data_quality_score       * 0.20
    + reliability_score        * 0.15

False signal taxonomy:
  FALSE_SIGNAL     : sentiment strong (≥0.60) but impact_score < 0.10
  NO_IMPACT_EVENT  : impact_score < 0.05 and reaction_type = NO_REACTION
  MISLEADING_NEWS  : sentiment_accuracy = INCORRECT and impact_score > 0.20

None → clean signal (may still be low effectiveness)
"""
from __future__ import annotations

from typing import Optional, Tuple


# Thresholds
_STRONG_SENTIMENT  = 0.60    # sentiment_accuracy_score ≥ this → "strong"
_LOW_IMPACT        = 0.10    # below this + strong sentiment = FALSE_SIGNAL
_ZERO_IMPACT       = 0.05    # effectively no market move
_MISLEAD_IMPACT    = 0.20    # market moved hard but sentiment was wrong


def compute_effectiveness_score(
    *,
    impact_score: float,
    sentiment_accuracy_score: float,
    data_quality_score: float,
    reliability_score: float,
) -> float:
    """Return signal_effectiveness_score in [0.0, 1.0]."""
    score = (
        impact_score             * 0.35
        + sentiment_accuracy_score * 0.30
        + data_quality_score       * 0.20
        + reliability_score        * 0.15
    )
    return round(min(1.0, max(0.0, score)), 4)


def detect_false_signal(
    *,
    impact_score: float,
    sentiment_accuracy_score: float,
    sentiment_accuracy: str,
    reaction_type: str,
) -> Optional[str]:
    """
    Detect false/misleading signal patterns.

    Returns a flag string or None (clean).
    """
    # 1. Strong sentiment but market didn't move
    if sentiment_accuracy_score >= _STRONG_SENTIMENT and impact_score < _LOW_IMPACT:
        return "FALSE_SIGNAL"

    # 2. Completely flat reaction (noise event)
    if impact_score < _ZERO_IMPACT and reaction_type in ("NO_REACTION", ""):
        return "NO_IMPACT_EVENT"

    # 3. Sentiment was wrong AND market moved hard (misleading)
    if sentiment_accuracy == "INCORRECT" and impact_score > _MISLEAD_IMPACT:
        return "MISLEADING_NEWS"

    return None


def classify_signal(
    *,
    impact_score: float,
    sentiment_accuracy_score: float,
    sentiment_accuracy: str,
    data_quality_score: float,
    reliability_score: float,
    reaction_type: str,
) -> Tuple[float, Optional[str]]:
    """
    Full classification — returns (effectiveness_score, false_signal_reason).
    """
    effectiveness = compute_effectiveness_score(
        impact_score=impact_score,
        sentiment_accuracy_score=sentiment_accuracy_score,
        data_quality_score=data_quality_score,
        reliability_score=reliability_score,
    )
    false_reason = detect_false_signal(
        impact_score=impact_score,
        sentiment_accuracy_score=sentiment_accuracy_score,
        sentiment_accuracy=sentiment_accuracy,
        reaction_type=reaction_type,
    )
    return effectiveness, false_reason
