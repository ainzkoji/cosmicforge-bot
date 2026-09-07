"""
Shadow Trading System — Analytics & Reporting

ShadowAnalytics provides read-only research queries:
  - Category summary (win rate, PnL by rejection stage)
  - ML gate analysis (would ML-blocked trades have been profitable?)
  - Threshold gap analysis (missed profit per threshold gap bucket)
  - Shadow vs real trade comparison

All methods return plain dicts/lists for JSON serialization.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)


class ShadowAnalytics:
    """
    Read-only research analytics for the shadow trading system.
    """

    def __init__(self, db: Optional[DB] = None):
        self._db = db or DB()

    # =====================================================================
    # CATEGORY SUMMARY
    # =====================================================================

    def get_category_summary(
        self,
        days: int = 30,
        bot_instance_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Per-rejection-stage summary of evaluated shadow trades.

        Returns a list of dicts with:
          rejection_stage, total, evaluated, tp_hit, sl_hit, expired,
          data_missing, win_rate, avg_pnl_pct, total_pnl_net,
          avg_mfe, avg_mae
        """
        bot_filter = "AND s.bot_instance_id = :bot_id" if bot_instance_id else ""
        query = f"""
            SELECT
                s.rejection_stage,
                COUNT(s.id)                                         AS total,
                SUM(CASE WHEN s.status = 'EVALUATED' THEN 1 ELSE 0 END) AS evaluated,
                SUM(CASE WHEN o.outcome = 'TP_HIT'   THEN 1 ELSE 0 END) AS tp_hit,
                SUM(CASE WHEN o.outcome = 'SL_HIT'   THEN 1 ELSE 0 END) AS sl_hit,
                SUM(CASE WHEN o.outcome = 'EXPIRED'  THEN 1 ELSE 0 END) AS expired,
                SUM(CASE WHEN o.outcome = 'DATA_MISSING' THEN 1 ELSE 0 END) AS data_missing,
                ROUND(
                    100.0 * SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN o.outcome IN ('TP_HIT','SL_HIT') THEN 1.0 ELSE 0.0 END), 0),
                2)                                                  AS win_rate_pct,
                ROUND(AVG(o.pnl_pct), 4)                          AS avg_pnl_pct,
                ROUND(SUM(o.pnl_net), 4)                          AS total_pnl_net,
                ROUND(AVG(o.mfe), 4)                              AS avg_mfe,
                ROUND(AVG(o.mae), 4)                              AS avg_mae,
                ROUND(AVG(o.bars_elapsed), 1)                     AS avg_bars_to_exit
            FROM shadow_trades s
            LEFT JOIN shadow_trade_outcomes o ON o.shadow_trade_id = s.id
            WHERE s.created_at >= datetime('now', :days || ' days')
              {bot_filter}
            GROUP BY s.rejection_stage
            ORDER BY total DESC
        """
        params: Dict[str, Any] = {"days": f"-{days}"}
        if bot_instance_id:
            params["bot_id"] = bot_instance_id

        return self._query(query, params)

    # =====================================================================
    # ML GATE ANALYSIS
    # =====================================================================

    def get_ml_gate_analysis(
        self,
        days: int = 30,
        bot_instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyse hypothetical performance of ML-blocked trades.

        Answers: "If the ML gate hadn't blocked these entries,
                  would they have been profitable?"

        Returns summary + per-score-bucket breakdown.
        """
        bot_filter = "AND s.bot_instance_id = :bot_id" if bot_instance_id else ""

        # Overall summary for ML_BLOCKED
        summary_query = f"""
            SELECT
                COUNT(s.id)                                                 AS total_blocked,
                SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1 ELSE 0 END)   AS would_have_won,
                SUM(CASE WHEN o.outcome = 'SL_HIT' THEN 1 ELSE 0 END)   AS would_have_lost,
                SUM(CASE WHEN o.outcome = 'EXPIRED' THEN 1 ELSE 0 END)  AS would_have_expired,
                ROUND(
                    100.0 * SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN o.outcome IN ('TP_HIT','SL_HIT') THEN 1.0 ELSE 0.0 END),0)
                , 2)                                                        AS hypothetical_win_rate_pct,
                ROUND(SUM(o.pnl_net), 4)                                  AS total_missed_pnl_net,
                ROUND(AVG(o.pnl_pct), 4)                                  AS avg_missed_pnl_pct,
                ROUND(AVG(s.ml_score), 4)                                 AS avg_ml_score_at_block,
                ROUND(MIN(s.ml_score), 4)                                 AS min_ml_score,
                ROUND(MAX(s.ml_score), 4)                                 AS max_ml_score
            FROM shadow_trades s
            LEFT JOIN shadow_trade_outcomes o ON o.shadow_trade_id = s.id
            WHERE s.rejection_stage = 'ML_BLOCKED'
              AND s.created_at >= datetime('now', :days || ' days')
              {bot_filter}
        """

        # Score bucket breakdown (0.0-0.1, 0.1-0.2, ..., 0.9-1.0)
        bucket_query = f"""
            SELECT
                ROUND(CAST(CAST(s.ml_score * 10 AS INTEGER) AS REAL) / 10.0, 1) AS score_bucket,
                COUNT(s.id)                                               AS count,
                ROUND(
                    100.0 * SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN o.outcome IN ('TP_HIT','SL_HIT') THEN 1.0 ELSE 0.0 END),0)
                , 1)                                                       AS win_rate_pct,
                ROUND(AVG(o.pnl_pct), 4)                                AS avg_pnl_pct
            FROM shadow_trades s
            LEFT JOIN shadow_trade_outcomes o ON o.shadow_trade_id = s.id
            WHERE s.rejection_stage = 'ML_BLOCKED'
              AND s.ml_score IS NOT NULL
              AND s.created_at >= datetime('now', :days || ' days')
              {bot_filter}
            GROUP BY score_bucket
            ORDER BY score_bucket ASC
        """

        params: Dict[str, Any] = {"days": f"-{days}"}
        if bot_instance_id:
            params["bot_id"] = bot_instance_id

        summary_rows = self._query(summary_query, params)
        bucket_rows = self._query(bucket_query, params)

        summary = summary_rows[0] if summary_rows else {}
        return {
            "summary": summary,
            "score_buckets": bucket_rows,
            "interpretation": (
                "positive total_missed_pnl_net → ML blocking was correct. "
                "Negative → ML over-filtered profitable entries."
            ),
        }

    # =====================================================================
    # THRESHOLD GAP ANALYSIS
    # =====================================================================

    def get_threshold_gap_analysis(
        self,
        days: int = 30,
        bot_instance_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Analyse trades blocked by the confidence threshold, bucketed by
        confidence_gap (confidence - threshold).

        Answers: "How profitable were the near-misses vs far-below-threshold trades?"

        Gap buckets: < -0.20, -0.20 to -0.10, -0.10 to -0.05, -0.05 to 0.00
        """
        bot_filter = "AND s.bot_instance_id = :bot_id" if bot_instance_id else ""

        query = f"""
            SELECT
                CASE
                    WHEN s.confidence_gap >= -0.05 THEN 'near_miss (-5% to 0)'
                    WHEN s.confidence_gap >= -0.10 THEN 'moderate_miss (-10% to -5%)'
                    WHEN s.confidence_gap >= -0.20 THEN 'large_miss (-20% to -10%)'
                    ELSE 'far_miss (below -20%)'
                END                                                         AS gap_bucket,
                COUNT(s.id)                                                 AS count,
                ROUND(AVG(s.confidence_gap), 4)                            AS avg_gap,
                ROUND(
                    100.0 * SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN o.outcome IN ('TP_HIT','SL_HIT') THEN 1.0 ELSE 0.0 END),0)
                , 1)                                                         AS win_rate_pct,
                ROUND(AVG(o.pnl_pct), 4)                                   AS avg_pnl_pct,
                ROUND(SUM(o.pnl_net), 4)                                   AS total_pnl_net
            FROM shadow_trades s
            LEFT JOIN shadow_trade_outcomes o ON o.shadow_trade_id = s.id
            WHERE s.rejection_stage = 'THRESHOLD_BLOCKED'
              AND s.confidence_gap IS NOT NULL
              AND s.created_at >= datetime('now', :days || ' days')
              {bot_filter}
            GROUP BY gap_bucket
            ORDER BY avg_gap DESC
        """

        params: Dict[str, Any] = {"days": f"-{days}"}
        if bot_instance_id:
            params["bot_id"] = bot_instance_id

        return self._query(query, params)

    # =====================================================================
    # SHADOW VS REAL COMPARISON
    # =====================================================================

    def compare_vs_real_trades(
        self,
        days: int = 30,
        bot_instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compare shadow (hypothetical) trade metrics against real executed trades.

        Answers: "Are the trades we're taking better or worse than the ones we're blocking?"
        """
        bot_filter_shadow = "AND s.bot_instance_id = :bot_id" if bot_instance_id else ""

        shadow_query = f"""
            SELECT
                'shadow'                                                    AS source,
                COUNT(s.id)                                                 AS trade_count,
                ROUND(
                    100.0 * SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN o.outcome IN ('TP_HIT','SL_HIT') THEN 1.0 ELSE 0.0 END),0)
                , 2)                                                         AS win_rate_pct,
                ROUND(AVG(o.pnl_pct), 4)                                   AS avg_pnl_pct,
                ROUND(SUM(o.pnl_net), 4)                                   AS total_pnl_net,
                ROUND(AVG(o.mfe), 4)                                       AS avg_mfe,
                ROUND(AVG(o.mae), 4)                                       AS avg_mae
            FROM shadow_trades s
            LEFT JOIN shadow_trade_outcomes o ON o.shadow_trade_id = s.id
            WHERE s.status = 'EVALUATED'
              AND s.created_at >= datetime('now', :days || ' days')
              {bot_filter_shadow}
        """

        real_query = """
            SELECT
                'real'                              AS source,
                COUNT(*) / 2                        AS trade_count,
                ROUND(
                    100.0 * SUM(CASE WHEN action='CLOSE' AND realized_pnl > 0 THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN action='CLOSE' THEN 1.0 ELSE 0.0 END), 0)
                , 2)                                AS win_rate_pct,
                ROUND(AVG(CASE WHEN action='CLOSE' AND price > 0
                          THEN realized_pnl / price * 100 ELSE NULL END), 4) AS avg_pnl_pct,
                ROUND(SUM(CASE WHEN action='CLOSE' THEN realized_pnl ELSE 0 END), 4) AS total_pnl_net,
                NULL                                AS avg_mfe,
                NULL                                AS avg_mae
            FROM trade_fills
            WHERE timestamp_utc >= datetime('now', :days || ' days')
        """

        params: Dict[str, Any] = {"days": f"-{days}"}
        if bot_instance_id:
            params["bot_id"] = bot_instance_id

        shadow_rows = self._query(shadow_query, params)
        real_rows = self._query(real_query, {"days": f"-{days}"})

        return {
            "shadow": shadow_rows[0] if shadow_rows else {},
            "real": real_rows[0] if real_rows else {},
            "period_days": days,
            "note": (
                "Shadow metrics are hypothetical (no slippage, normalized size). "
                "Real metrics reflect actual execution with all fees."
            ),
        }

    # =====================================================================
    # REGIME BREAKDOWN
    # =====================================================================

    def get_regime_breakdown(
        self,
        days: int = 30,
    ) -> List[Dict[str, Any]]:
        """Win rate and PnL of shadow trades grouped by market regime."""
        query = """
            SELECT
                s.regime,
                s.rejection_stage,
                COUNT(s.id)                                                 AS count,
                ROUND(
                    100.0 * SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN o.outcome IN ('TP_HIT','SL_HIT') THEN 1.0 ELSE 0.0 END),0)
                , 1)                                                         AS win_rate_pct,
                ROUND(AVG(o.pnl_pct), 4)                                   AS avg_pnl_pct
            FROM shadow_trades s
            LEFT JOIN shadow_trade_outcomes o ON o.shadow_trade_id = s.id
            WHERE s.status = 'EVALUATED'
              AND s.regime IS NOT NULL
              AND s.created_at >= datetime('now', :days || ' days')
            GROUP BY s.regime, s.rejection_stage
            ORDER BY s.regime, s.rejection_stage
        """
        return self._query(query, {"days": f"-{days}"})

    # =====================================================================
    # SYMBOL BREAKDOWN
    # =====================================================================

    def get_symbol_breakdown(
        self,
        days: int = 30,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Per-symbol shadow trade summary."""
        query = """
            SELECT
                s.symbol,
                COUNT(s.id)                                                 AS total,
                SUM(CASE WHEN s.status = 'EVALUATED' THEN 1 ELSE 0 END)   AS evaluated,
                ROUND(
                    100.0 * SUM(CASE WHEN o.outcome = 'TP_HIT' THEN 1.0 ELSE 0.0 END)
                    / NULLIF(SUM(CASE WHEN o.outcome IN ('TP_HIT','SL_HIT') THEN 1.0 ELSE 0.0 END),0)
                , 1)                                                         AS win_rate_pct,
                ROUND(SUM(o.pnl_net), 4)                                   AS total_pnl_net
            FROM shadow_trades s
            LEFT JOIN shadow_trade_outcomes o ON o.shadow_trade_id = s.id
            WHERE s.created_at >= datetime('now', :days || ' days')
            GROUP BY s.symbol
            ORDER BY total DESC
            LIMIT :limit
        """
        return self._query(query, {"days": f"-{days}", "limit": limit})

    # =====================================================================
    # STATUS COUNTS
    # =====================================================================

    def get_status_counts(self) -> Dict[str, int]:
        """Quick status count for health dashboard."""
        query = "SELECT status, COUNT(*) AS cnt FROM shadow_trades GROUP BY status"
        rows = self._query(query, {})
        return {r["status"]: r["cnt"] for r in rows}

    # =====================================================================
    # INTERNAL
    # =====================================================================

    def _query(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return list of row dicts."""
        try:
            import sqlite3
            with self._db.connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(sql, params).fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("[ShadowAnalytics] query failed: %s | sql=%s", exc, sql[:120])
            return []


# ── Singleton ─────────────────────────────────────────────────────────────────
_analytics: Optional[ShadowAnalytics] = None


def get_shadow_analytics(db: Optional[DB] = None) -> ShadowAnalytics:
    """Return the singleton ShadowAnalytics."""
    global _analytics
    if _analytics is None:
        _analytics = ShadowAnalytics(db=db)
    return _analytics
