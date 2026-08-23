"""
MarketReactionTracker — orchestrates snapshot collection and metric computation
for one (event_id, symbol) pair after all post-event windows have been collected.

Responsibilities:
  1. Called by EventReactionWorker after all snapshots for a window are done.
  2. Loads snapshots from DB, computes all metrics, runs classifier, persists result.
  3. Does NOT open trades — pure observer.

Design invariants:
  - Never raises; all errors are caught and logged.
  - If data is insufficient, stores the reaction with degraded data_quality.
  - Shadow mode: computes metrics but result is flagged; does not affect risk.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.market_reactions import (
    get_snapshots,
    upsert_reaction,
)
from app.events.reaction_metrics import (
    compute_atr,
    compute_candle_range,
    compute_confidence_and_quality,
    compute_continuation_or_reversal,
    compute_price_moves,
    compute_realized_vol,
    compute_spread_metrics,
    compute_volatility_expansion,
    compute_volume_metrics,
)
from app.events.reaction_classifier import classify_reaction

logger = logging.getLogger(__name__)

# Window labels in the order they are collected
PRE_WINDOWS = ["PRE_60", "PRE_30"]
EVENT_WINDOWS = ["EVENT"]
POST_WINDOWS = ["POST_5", "POST_15", "POST_30", "POST_60"]
ALL_WINDOWS = PRE_WINDOWS + EVENT_WINDOWS + POST_WINDOWS


class MarketReactionTracker:
    """
    Computes and persists a market_event_reactions row from collected snapshots.
    """

    def __init__(self, db: DB, snapshot_interval_minutes: float = 1.0):
        self._db = db
        self._interval_min = snapshot_interval_minutes

    def compute_and_save(
        self,
        event_id: str,
        symbol: str,
        exchange: str,
        event_time_utc: str,
        pre_window_start_utc: str,
        post_window_end_utc: str,
        *,
        shadow_mode: bool = True,
        vol_spike_threshold: float = 2.5,
        volume_spike_threshold: float = 3.0,
        spread_widening_threshold: float = 2.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Load all snapshots for this event+symbol, compute reaction metrics,
        classify the reaction, and persist the result.

        Returns the reaction dict on success, None on total failure.
        """
        try:
            return self._compute(
                event_id=event_id,
                symbol=symbol,
                exchange=exchange,
                event_time_utc=event_time_utc,
                pre_window_start_utc=pre_window_start_utc,
                post_window_end_utc=post_window_end_utc,
                shadow_mode=shadow_mode,
                vol_spike_threshold=vol_spike_threshold,
                volume_spike_threshold=volume_spike_threshold,
                spread_widening_threshold=spread_widening_threshold,
            )
        except Exception as exc:
            logger.error(
                "[ReactionTracker] Unhandled error event=%s symbol=%s: %s",
                event_id, symbol, exc, exc_info=True,
            )
            return None

    def _compute(
        self,
        event_id: str,
        symbol: str,
        exchange: str,
        event_time_utc: str,
        pre_window_start_utc: str,
        post_window_end_utc: str,
        shadow_mode: bool,
        vol_spike_threshold: float,
        volume_spike_threshold: float,
        spread_widening_threshold: float,
    ) -> Dict[str, Any]:
        all_snaps = get_snapshots(self._db, event_id, symbol)
        exchange_error = len(all_snaps) == 0

        def _by_window(labels: List[str]) -> List[Dict]:
            return [s for s in all_snaps if s.get("window_label") in labels]

        pre_snaps = _by_window(PRE_WINDOWS)
        event_snaps = _by_window(EVENT_WINDOWS)
        post_snaps = _by_window(POST_WINDOWS)

        has_pre60 = any(s["window_label"] == "PRE_60" for s in all_snaps)
        has_event = bool(event_snaps)
        has_post30 = any(s["window_label"] == "POST_30" for s in all_snaps)
        has_post60 = any(s["window_label"] == "POST_60" for s in all_snaps)

        confidence, data_quality = compute_confidence_and_quality(
            has_pre60=has_pre60,
            has_event=has_event,
            has_post30=has_post30,
            has_post60=has_post60,
            pre60_count=len(pre_snaps),
            event_count=len(event_snaps),
            post60_count=len([s for s in all_snaps if s["window_label"] == "POST_60"]),
            exchange_error=exchange_error,
        )

        # --- Price before event (last PRE snapshot price) ---
        price_before = _last_price(pre_snaps) if pre_snaps else None
        price_at_event = _last_price(event_snaps) if event_snaps else None

        def _post_price(label: str) -> Optional[float]:
            snaps = [s for s in all_snaps if s["window_label"] == label]
            return _last_price(snaps) if snaps else None

        p5  = _post_price("POST_5")
        p15 = _post_price("POST_15")
        p30 = _post_price("POST_30")
        p60 = _post_price("POST_60")

        prices_during = [price_at_event] if price_at_event else []
        move_metrics: Dict[str, Any] = {}
        cont = "NEUTRAL"
        if price_before:
            move_metrics = compute_price_moves(
                price_before=price_before,
                prices_during=prices_during,
                price_after_5m=p5,
                price_after_15m=p15,
                price_after_30m=p30,
                price_after_60m=p60,
            )
            cont = compute_continuation_or_reversal(price_before, price_at_event, p60)

        # --- ATR ---
        atr_before = compute_atr(pre_snaps) if len(pre_snaps) >= 2 else None
        atr_after = compute_atr(post_snaps) if len(post_snaps) >= 2 else None
        vol_expansion = compute_volatility_expansion(atr_before, atr_after)
        candle_range_before = compute_candle_range(pre_snaps)
        candle_range_during = compute_candle_range(event_snaps)

        pre_prices = [s["price"] for s in pre_snaps if s.get("price")]
        post_prices = [s["price"] for s in post_snaps if s.get("price")]
        rv_before = compute_realized_vol(pre_prices, self._interval_min)
        rv_after  = compute_realized_vol(post_prices, self._interval_min)

        # --- Volume ---
        pre_volumes = [s["volume"] for s in pre_snaps if s.get("volume") is not None]
        event_volume = _last_volume(event_snaps) if event_snaps else None
        vol_metrics = compute_volume_metrics(
            pre_volumes, event_volume, volume_spike_threshold
        )

        # --- Spread ---
        spread_metrics = compute_spread_metrics(
            spread_before=_mean_spread(pre_snaps),
            spread_during=_mean_spread(event_snaps),
            spread_after=_mean_spread(post_snaps),
            threshold=spread_widening_threshold,
        )

        # --- Classification ---
        reaction_type = classify_reaction(
            data_quality=data_quality,
            volatility_expansion_ratio=vol_expansion,
            volume_spike_ratio=vol_metrics.get("volume_spike_ratio"),
            spread_widening_ratio=spread_metrics.get("spread_widening_ratio"),
            net_move_pct=move_metrics.get("net_move_pct"),
            max_move_pct=move_metrics.get("max_move_pct"),
            min_move_pct=move_metrics.get("min_move_pct"),
            direction_after_event=move_metrics.get("direction_after_event"),
            continuation_or_reversal=cont,
            price_before_event=price_before,
            price_after_5m=p5,
            price_after_15m=p15,
            price_after_30m=p30,
            price_after_60m=p60,
            atr_before=atr_before,
            atr_after=atr_after,
            vol_spike_threshold=vol_spike_threshold,
            volume_spike_threshold=volume_spike_threshold,
            spread_widening_threshold=spread_widening_threshold,
        )

        reaction: Dict[str, Any] = {
            "exchange": exchange,
            "pre_window_start_utc": pre_window_start_utc,
            "event_time_utc": event_time_utc,
            "post_window_end_utc": post_window_end_utc,
            "price_before_event": price_before,
            "price_at_event": price_at_event,
            "price_after_event": p60 if p60 is not None else (p30 if p30 is not None else (p15 if p15 is not None else p5)),
            "price_after_5m": p5,
            "price_after_15m": p15,
            "price_after_30m": p30,
            "price_after_60m": p60,
            "max_move_pct": move_metrics.get("max_move_pct"),
            "min_move_pct": move_metrics.get("min_move_pct"),
            "net_move_pct": move_metrics.get("net_move_pct"),
            "direction_after_event": move_metrics.get("direction_after_event", "FLAT"),
            "continuation_or_reversal": cont,
            "atr_before": atr_before,
            "atr_after": atr_after,
            "volatility_expansion_ratio": vol_expansion,
            "candle_range_before": candle_range_before,
            "candle_range_during": candle_range_during,
            "realized_vol_before": rv_before,
            "realized_vol_after": rv_after,
            "average_volume_before": vol_metrics.get("average_volume_before"),
            "event_volume": vol_metrics.get("event_volume"),
            "volume_spike_ratio": vol_metrics.get("volume_spike_ratio"),
            "abnormal_volume_score": vol_metrics.get("abnormal_volume_score"),
            "spread_before": spread_metrics.get("spread_before"),
            "spread_during": spread_metrics.get("spread_during"),
            "spread_after": spread_metrics.get("spread_after"),
            "spread_widening_ratio": spread_metrics.get("spread_widening_ratio"),
            "slippage_estimate": None,
            "order_book_depth_change": None,
            "reaction_type": reaction_type,
            "confidence_score": confidence,
            "data_quality": data_quality,
        }

        upsert_reaction(self._db, event_id=event_id, symbol=symbol, **reaction)

        mode_tag = "[SHADOW]" if shadow_mode else "[LIVE]"
        logger.info(
            "[ReactionTracker] %s event=%s symbol=%s type=%s quality=%s confidence=%.2f",
            mode_tag, event_id, symbol, reaction_type, data_quality, confidence,
        )
        return reaction


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _last_price(snaps: List[Dict]) -> Optional[float]:
    for s in reversed(snaps):
        if s.get("price") is not None:
            return s["price"]
    return None


def _last_volume(snaps: List[Dict]) -> Optional[float]:
    for s in reversed(snaps):
        if s.get("volume") is not None:
            return s["volume"]
    return None


def _mean_spread(snaps: List[Dict]) -> Optional[float]:
    vals = [s["spread"] for s in snaps if s.get("spread") is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)
