"""
News Sentiment Validator.

Compares the news cluster's sentiment direction against the
actual price direction recorded in the market_event_reactions row.

Outputs:
  sentiment_accuracy        : CORRECT | INCORRECT | NEUTRAL | MIXED
  sentiment_accuracy_score  : 1.0 | 0.0 | 0.3 | 0.5

Mapping:
  sentiment_direction  actual_direction  → accuracy
  ─────────────────────────────────────────────────
  BULLISH              UP                → CORRECT    (1.0)
  BEARISH              DOWN              → CORRECT    (1.0)
  BULLISH              DOWN              → INCORRECT  (0.0)
  BEARISH              UP                → INCORRECT  (0.0)
  NEUTRAL              *                 → NEUTRAL    (0.3)
  *                    MIXED/SIDEWAYS    → MIXED      (0.5)
  BULLISH/BEARISH      SIDEWAYS          → MIXED      (0.5)

actual_direction is derived from direction_after_event (Phase 2):
  UP, DOWN, SIDEWAYS, MIXED
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple


# accuracy → score
_ACCURACY_SCORES: Dict[str, float] = {
    "CORRECT":   1.0,
    "INCORRECT": 0.0,
    "MIXED":     0.5,
    "NEUTRAL":   0.3,
}

# Sentiment score ranges → direction label
_BULLISH_THRESHOLD =  0.15
_BEARISH_THRESHOLD = -0.15


def sentiment_to_direction(sentiment_score: Optional[float]) -> str:
    """Convert raw compound sentiment score to BULLISH/BEARISH/NEUTRAL."""
    if sentiment_score is None:
        return "NEUTRAL"
    if sentiment_score >= _BULLISH_THRESHOLD:
        return "BULLISH"
    if sentiment_score <= _BEARISH_THRESHOLD:
        return "BEARISH"
    return "NEUTRAL"


def price_direction(reaction_row: Dict) -> str:
    """
    Extract actual market direction from Phase 2 reaction row.
    Returns UP | DOWN | SIDEWAYS | MIXED.
    """
    direction = (reaction_row.get("direction_after_event") or "").upper()
    if direction in ("UP", "DOWN", "SIDEWAYS", "MIXED"):
        return direction

    # Fall back: derive from net_move_pct
    net = reaction_row.get("net_move_pct")
    if net is None:
        return "SIDEWAYS"
    if net > 0.005:
        return "UP"
    if net < -0.005:
        return "DOWN"
    return "SIDEWAYS"


def validate_sentiment(
    sentiment_score: Optional[float],
    reaction_row: Dict,
) -> Tuple[str, str, str, float]:
    """
    Compare sentiment vs market direction.

    Parameters
    ----------
    sentiment_score : compound sentiment float (e.g. from VADER)
    reaction_row    : market_event_reactions dict

    Returns
    -------
    (sentiment_direction, actual_direction, sentiment_accuracy, accuracy_score)
    """
    sent_dir   = sentiment_to_direction(sentiment_score)
    actual_dir = price_direction(reaction_row)

    # ── Classification ───────────────────────────────────────────────────────
    if sent_dir == "NEUTRAL":
        accuracy = "NEUTRAL"

    elif actual_dir in ("SIDEWAYS", "MIXED"):
        accuracy = "MIXED"

    elif (
        (sent_dir == "BULLISH" and actual_dir == "UP") or
        (sent_dir == "BEARISH" and actual_dir == "DOWN")
    ):
        accuracy = "CORRECT"

    else:
        accuracy = "INCORRECT"

    return sent_dir, actual_dir, accuracy, _ACCURACY_SCORES[accuracy]
