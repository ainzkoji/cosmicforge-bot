"""
VADER-based sentiment scoring for news items.

Pure Python, no GPU, no external API calls.
pip install vaderSentiment
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_intelligence import upsert_sentiment


_MODEL_VERSION = "vader-1.0"


def _load_vader():
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        return SentimentIntensityAnalyzer()
    except ImportError:
        return None


class NewsSentimentService:
    def __init__(self, db: DB) -> None:
        self._db = db
        self._analyzer = _load_vader()

    def _fallback_score(self, text: str) -> Dict[str, float]:
        """Keyword fallback when VADER is not installed."""
        text_lower = text.lower()
        bullish = sum(1 for w in ["surge", "rally", "gain", "bull", "pump", "rise", "high", "record"] if w in text_lower)
        bearish = sum(1 for w in ["crash", "drop", "fall", "bear", "dump", "plunge", "low", "risk"] if w in text_lower)
        if bullish > bearish:
            compound = min(0.5, bullish * 0.15)
        elif bearish > bullish:
            compound = max(-0.5, -bearish * 0.15)
        else:
            compound = 0.0
        return {"compound": compound, "pos": 0.0, "neg": 0.0, "neu": 1.0}

    def score_text(self, text: str) -> Tuple[str, float, float, Dict[str, float]]:
        """
        Returns (label, score, confidence, raw_scores).
        label: BULLISH | BEARISH | NEUTRAL
        score: 0.0-1.0 directional strength
        confidence: 0.0-1.0
        """
        if self._analyzer:
            raw = self._analyzer.polarity_scores(text)
        else:
            raw = self._fallback_score(text)

        compound = raw["compound"]
        abs_compound = abs(compound)

        if compound >= 0.05:
            label = "BULLISH"
            score = compound
        elif compound <= -0.05:
            label = "BEARISH"
            score = abs_compound
        else:
            label = "NEUTRAL"
            score = 0.0

        # Confidence scales with |compound|, floored at 0.3 for non-neutral
        if label == "NEUTRAL":
            confidence = max(0.3, 1.0 - abs_compound * 2)
        else:
            confidence = min(0.95, 0.4 + abs_compound * 0.8)

        return label, score, confidence, raw

    def score_and_store(
        self,
        cluster_id: int,
        title: str,
        body: str = "",
    ) -> float:
        text = f"{title}. {body}".strip()
        if not text:
            return 0.0

        label, score, confidence, raw = self.score_text(text)

        upsert_sentiment(
            self._db,
            cluster_id=cluster_id,
            sentiment_label=label,
            sentiment_score=score,
            confidence_score=confidence,
            compound_raw=raw.get("compound", 0.0),
            pos_raw=raw.get("pos", 0.0),
            neg_raw=raw.get("neg", 0.0),
            neu_raw=raw.get("neu", 1.0),
            model_version=_MODEL_VERSION,
        )

        return raw.get("compound", 0.0)
