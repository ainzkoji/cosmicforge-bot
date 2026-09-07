"""
Narrative Effectiveness Tracker.

Maintains rolling aggregates in `narrative_effectiveness_scores`
so we know which narrative types actually move markets over time.

After each validation run, call update_narrative_effectiveness()
with the cluster's top narrative and the validation results.
The table uses exponential moving averages (EMA) to weight recent
observations more than old ones.

EMA α = 0.2 (last ~5 samples dominate)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from shared_lib.persistence.db import DB


_EMA_ALPHA = 0.20   # smoothing factor


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ema(old: float, new: float, alpha: float = _EMA_ALPHA) -> float:
    return round(old * (1 - alpha) + new * alpha, 4)


def update_narrative_effectiveness(
    db: DB,
    *,
    narrative_type: str,
    impact_score: float,
    price_move_pct: float,
    sentiment_accuracy: str,
    is_false_signal: bool,
    effectiveness_score: float,
) -> None:
    """
    Upsert rolling aggregate for a narrative type.
    Uses INSERT OR REPLACE with EMA blending when a row already exists.
    """
    now = _now()
    correct = 1.0 if sentiment_accuracy == "CORRECT" else 0.0

    with db.connect() as conn:
        existing = conn.execute(
            "SELECT * FROM narrative_effectiveness_scores WHERE narrative_type = ?",
            (narrative_type,),
        ).fetchone()

        if existing is None:
            conn.execute(
                """
                INSERT INTO narrative_effectiveness_scores
                    (narrative_type, sample_count, avg_impact_score,
                     avg_price_move_pct, correct_sentiment_ratio,
                     false_signal_ratio, avg_effectiveness_score, last_updated)
                VALUES (?, 1, ?, ?, ?, ?, ?, ?)
                """,
                (
                    narrative_type,
                    round(impact_score, 4),
                    round(abs(price_move_pct), 4),
                    correct,
                    1.0 if is_false_signal else 0.0,
                    round(effectiveness_score, 4),
                    now,
                ),
            )
        else:
            row = dict(existing)
            new_count = row["sample_count"] + 1

            # EMA for smoothed metrics; simple increment for count
            conn.execute(
                """
                UPDATE narrative_effectiveness_scores SET
                    sample_count            = ?,
                    avg_impact_score        = ?,
                    avg_price_move_pct      = ?,
                    correct_sentiment_ratio = ?,
                    false_signal_ratio      = ?,
                    avg_effectiveness_score = ?,
                    last_updated            = ?
                WHERE narrative_type = ?
                """,
                (
                    new_count,
                    _ema(row["avg_impact_score"],        impact_score),
                    _ema(row["avg_price_move_pct"],      abs(price_move_pct)),
                    _ema(row["correct_sentiment_ratio"], correct),
                    _ema(row["false_signal_ratio"],      1.0 if is_false_signal else 0.0),
                    _ema(row["avg_effectiveness_score"], effectiveness_score),
                    now,
                    narrative_type,
                ),
            )


def get_all_narrative_effectiveness(db: DB, limit: int = 50) -> List[Dict]:
    """Return all rows ordered by avg_impact_score descending."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT * FROM narrative_effectiveness_scores
            ORDER BY avg_impact_score DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_narrative_effectiveness(db: DB, narrative_type: str) -> Optional[Dict]:
    """Return a single narrative row or None."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute(
            "SELECT * FROM narrative_effectiveness_scores WHERE narrative_type = ?",
            (narrative_type,),
        ).fetchone()
        return dict(row) if row else None
