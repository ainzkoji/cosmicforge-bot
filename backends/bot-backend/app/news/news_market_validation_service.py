"""
News Market Validation Service — Orchestrator.

Full pipeline per news cluster:
  1. Link cluster → market_event_reactions (Phase 2)
  2. Score impact
  3. Validate sentiment direction vs actual price
  4. Compute signal effectiveness + false signal flag
  5. Persist news_market_reactions row
  6. Update narrative_effectiveness_scores aggregate

SHADOW MODE INVARIANTS (hard-enforced):
  - should_affect_trading NEVER set to True
  - No trading logic touched
  - No ML layer modified

Config flags (set at class level or pass via constructor):
  MIN_IMPACT_THRESHOLD      = 0.02
  MIN_CONFIDENCE_THRESHOLD  = 0.70
  LINK_WINDOW_BEFORE_MIN    = 30
  LINK_WINDOW_AFTER_MIN     = 90
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_intelligence import (
    insert_news_market_reaction,
    get_cluster_row,
    update_signal_validation_outcome,
)
from app.news.news_market_linker import find_market_reaction
from app.news.news_impact_scorer import compute_impact_score
from app.news.news_sentiment_validator import validate_sentiment
from app.news.news_signal_effectiveness import classify_signal
from app.news.news_narrative_tracker import update_narrative_effectiveness

logger = logging.getLogger(__name__)


class NewsMarketValidationService:
    """
    Connects a news cluster to its real market reaction and
    produces a validated, effectiveness-scored result.

    All outputs are shadow-only — nothing feeds into live trading.
    """

    def __init__(
        self,
        db: DB,
        *,
        min_impact_threshold:    float = 0.02,
        min_confidence_threshold: float = 0.70,
        link_window_before_min:  int   = 30,
        link_window_after_min:   int   = 90,
    ) -> None:
        self._db = db
        self._min_impact        = min_impact_threshold
        self._min_confidence    = min_confidence_threshold
        self._window_before     = link_window_before_min
        self._window_after      = link_window_after_min

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def validate_cluster(
        self,
        cluster_id: int,
        *,
        first_seen_utc: Optional[str] = None,
        sentiment_score: Optional[float] = None,
        data_quality_score: float = 0.5,
        reliability_score: float = 0.5,
        top_narrative: Optional[str] = None,
    ) -> List[Dict]:
        """
        Run the full validation pipeline for a single cluster.

        Returns a list of result dicts (one per matched symbol/reaction).
        Empty list if no market reaction was found in the time window.
        """
        # Resolve first_seen_utc from DB if not provided
        if first_seen_utc is None:
            cluster_row = get_cluster_row(self._db, cluster_id)
            if not cluster_row:
                logger.warning("validate_cluster: cluster %s not found", cluster_id)
                return []
            first_seen_utc = cluster_row.get("first_seen_utc", "")
            if not sentiment_score:
                sentiment_score = None

        # ── Step 1: Link to Phase 2 reactions ────────────────────────────────
        matched_reactions = find_market_reaction(
            self._db,
            cluster_id=cluster_id,
            first_seen_utc=first_seen_utc,
            before_minutes=self._window_before,
            after_minutes=self._window_after,
        )

        if not matched_reactions:
            logger.debug("validate_cluster: no market reaction found for cluster %s", cluster_id)
            # Still store a NO_REACTION record per mapped symbol
            return self._store_no_reaction(
                cluster_id=cluster_id,
                sentiment_score=sentiment_score,
                data_quality_score=data_quality_score,
                reliability_score=reliability_score,
                top_narrative=top_narrative,
            )

        results: List[Dict] = []

        for reaction_row in matched_reactions:
            result = self._process_reaction(
                cluster_id=cluster_id,
                reaction_row=reaction_row,
                sentiment_score=sentiment_score,
                data_quality_score=data_quality_score,
                reliability_score=reliability_score,
                top_narrative=top_narrative,
            )
            results.append(result)

        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _process_reaction(
        self,
        *,
        cluster_id: int,
        reaction_row: Dict,
        sentiment_score: Optional[float],
        data_quality_score: float,
        reliability_score: float,
        top_narrative: Optional[str],
    ) -> Dict:
        symbol          = reaction_row.get("symbol", "")
        latency_minutes = reaction_row.get("latency_minutes", 0.0)
        event_reaction_id = reaction_row.get("id")

        # ── Step 2: Impact ────────────────────────────────────────────────────
        impact_score, latency_cat = compute_impact_score(reaction_row, latency_minutes)

        # ── Step 3: Sentiment validation ─────────────────────────────────────
        sent_dir, actual_dir, accuracy, accuracy_score = validate_sentiment(
            sentiment_score, reaction_row
        )

        # ── Step 4: Effectiveness + false signal ──────────────────────────────
        effectiveness, false_reason = classify_signal(
            impact_score=impact_score,
            sentiment_accuracy_score=accuracy_score,
            sentiment_accuracy=accuracy,
            data_quality_score=data_quality_score,
            reliability_score=reliability_score,
            reaction_type=reaction_row.get("reaction_type", "NO_REACTION"),
        )

        # ── Step 5: Persist ───────────────────────────────────────────────────
        reaction_id = insert_news_market_reaction(
            self._db,
            cluster_id=cluster_id,
            symbol=symbol,
            event_reaction_id=event_reaction_id,
            sentiment_score=sentiment_score,
            sentiment_direction=sent_dir,
            actual_direction=actual_dir,
            sentiment_accuracy=accuracy,
            sentiment_accuracy_score=accuracy_score,
            impact_score=impact_score,
            max_price_move_pct=reaction_row.get("max_move_pct"),
            volatility_expansion=reaction_row.get("volatility_expansion_ratio"),
            volume_spike=reaction_row.get("volume_spike_ratio"),
            reaction_type=reaction_row.get("reaction_type", "NO_REACTION"),
            reaction_latency_minutes=latency_minutes,
            reaction_latency_category=latency_cat,
            signal_effectiveness_score=effectiveness,
            is_false_signal=false_reason is not None,
            false_signal_reason=false_reason,
            data_quality_score=data_quality_score,
            reliability_score=reliability_score,
        )
        market_status = "NO_MARKET_REACTION"
        if reaction_row.get("reaction_type", "NO_REACTION") != "NO_REACTION" and impact_score >= self._min_impact:
            market_status = "DELAYED_REACTION" if latency_cat == "DELAYED" else "MARKET_CONFIRMED"
        market_validation_passed = (
            market_status in ("MARKET_CONFIRMED", "DELAYED_REACTION")
            and false_reason is None
        )
        update_signal_validation_outcome(
            self._db,
            cluster_id=cluster_id,
            symbol=symbol,
            market_validation_passed=market_validation_passed,
            market_confirmation_status=market_status,
            sentiment_accuracy=accuracy,
            validation_status="VALIDATED" if market_validation_passed else "INVALIDATED",
            suppression_reason=(None if market_validation_passed else (false_reason or market_status)),
        )

        # ── Step 6: Narrative tracking ─────────────────────────────────────────
        if top_narrative:
            try:
                update_narrative_effectiveness(
                    self._db,
                    narrative_type=top_narrative,
                    impact_score=impact_score,
                    price_move_pct=reaction_row.get("net_move_pct") or 0.0,
                    sentiment_accuracy=accuracy,
                    is_false_signal=false_reason is not None,
                    effectiveness_score=effectiveness,
                )
            except Exception as e:
                logger.warning("narrative tracker error: %s", e)

        result = {
            "reaction_id": reaction_id,
            "cluster_id": cluster_id,
            "symbol": symbol,
            "event_reaction_id": event_reaction_id,
            "sentiment_score": sentiment_score,
            "sentiment_direction": sent_dir,
            "actual_direction": actual_dir,
            "sentiment_accuracy": accuracy,
            "sentiment_accuracy_score": accuracy_score,
            "impact_score": impact_score,
            "reaction_latency_minutes": latency_minutes,
            "reaction_latency_category": latency_cat,
            "signal_effectiveness_score": effectiveness,
            "is_false_signal": false_reason is not None,
            "false_signal_reason": false_reason,
        }
        logger.info(
            "validate_cluster[%s/%s]: impact=%.2f accuracy=%s effectiveness=%.2f false=%s",
            cluster_id, symbol, impact_score, accuracy, effectiveness, false_reason,
        )
        return result

    def _store_no_reaction(
        self,
        *,
        cluster_id: int,
        sentiment_score: Optional[float],
        data_quality_score: float,
        reliability_score: float,
        top_narrative: Optional[str],
    ) -> List[Dict]:
        """Persist NO_REACTION records for clusters with no market match."""
        from app.news.news_market_linker import get_symbols_for_cluster
        from app.news.news_sentiment_validator import sentiment_to_direction

        symbols = get_symbols_for_cluster(self._db, cluster_id)
        if not symbols:
            symbols = ["UNKNOWN"]

        results = []
        sent_dir = sentiment_to_direction(sentiment_score)

        for symbol in symbols:
            reaction_id = insert_news_market_reaction(
                self._db,
                cluster_id=cluster_id,
                symbol=symbol,
                event_reaction_id=None,
                sentiment_score=sentiment_score,
                sentiment_direction=sent_dir,
                actual_direction=None,
                sentiment_accuracy="NEUTRAL",
                sentiment_accuracy_score=0.3,
                impact_score=0.0,
                reaction_type="NO_REACTION",
                reaction_latency_category="NO_REACTION",
                signal_effectiveness_score=0.0,
                is_false_signal=False,
                data_quality_score=data_quality_score,
                reliability_score=reliability_score,
            )
            update_signal_validation_outcome(
                self._db,
                cluster_id=cluster_id,
                symbol=symbol,
                market_validation_passed=False,
                market_confirmation_status="NO_MARKET_REACTION",
                sentiment_accuracy="NEUTRAL",
                validation_status="INVALIDATED",
                suppression_reason="NO_MARKET_REACTION",
            )
            if top_narrative:
                try:
                    update_narrative_effectiveness(
                        self._db,
                        narrative_type=top_narrative,
                        impact_score=0.0,
                        price_move_pct=0.0,
                        sentiment_accuracy="NEUTRAL",
                        is_false_signal=False,
                        effectiveness_score=0.0,
                    )
                except Exception:
                    pass

            results.append({
                "reaction_id": reaction_id,
                "cluster_id": cluster_id,
                "symbol": symbol,
                "impact_score": 0.0,
                "reaction_latency_category": "NO_REACTION",
                "sentiment_accuracy": "NEUTRAL",
                "signal_effectiveness_score": 0.0,
                "is_false_signal": False,
                "false_signal_reason": None,
            })
        return results
