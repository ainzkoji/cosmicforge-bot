"""
DB helpers for news_asset_mappings, news_sentiment_scores,
news_narratives, news_intelligence_signals, and cluster quality updates.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# news_clusters — quality update
# ---------------------------------------------------------------------------

def update_cluster_quality(
    db: DB,
    cluster_id: int,
    *,
    cluster_confidence: float,
    spam_score: float,
    latency_score: float,
    is_valid_signal: bool,
    manipulation_flag: Optional[str],
    data_quality_status: str,
    is_manipulation_suspect: bool = False,
    manipulation_reason: Optional[str] = None,
) -> None:
    """Persist all quality/hardening scores back to the news_clusters row."""
    now = _now()
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE news_clusters SET
                cluster_confidence      = ?,
                spam_score              = ?,
                latency_score           = ?,
                is_valid_signal         = ?,
                manipulation_flag       = ?,
                data_quality_status     = ?,
                is_manipulation_suspect = ?,
                manipulation_reason     = ?,
                updated_at              = ?
            WHERE id = ?
            """,
            (
                cluster_confidence,
                spam_score,
                latency_score,
                1 if is_valid_signal else 0,
                manipulation_flag,
                data_quality_status,
                1 if is_manipulation_suspect else 0,
                manipulation_reason,
                now,
                cluster_id,
            ),
        )


# ---------------------------------------------------------------------------
# news_asset_mappings
# ---------------------------------------------------------------------------

def upsert_asset_mapping(
    db: DB,
    *,
    cluster_id: int,
    symbol: str,
    asset: str = "crypto",
    mapping_confidence: float = 1.0,
    mapping_reason: str = "keyword",
) -> None:
    now = _now()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO news_asset_mappings
                (cluster_id, symbol, asset,
                 mapping_confidence, mapping_reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (cluster_id, symbol, asset,
             mapping_confidence, mapping_reason, now),
        )


def get_mappings_for_item(db: DB, raw_news_item_id: int) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            "SELECT * FROM news_asset_mappings WHERE raw_news_item_id=?",
            (raw_news_item_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_recent_items_for_symbol(
    db: DB,
    symbol: str,
    since_utc: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT rni.*, nam.match_confidence, nam.match_source
            FROM news_asset_mappings nam
            JOIN raw_news_items rni ON rni.id = nam.raw_news_item_id
            WHERE nam.symbol = ?
              AND rni.published_utc >= ?
            ORDER BY rni.published_utc DESC
            LIMIT ?
            """,
            (symbol, since_utc, limit),
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# news_sentiment_scores
# ---------------------------------------------------------------------------

def upsert_sentiment(
    db: DB,
    *,
    cluster_id: int,
    sentiment_label: str,
    sentiment_score: float,
    confidence_score: float,
    compound_raw: float = 0.0,
    pos_raw: float = 0.0,
    neg_raw: float = 0.0,
    neu_raw: float = 0.0,
    model_version: str = "vader-1.0",
) -> None:
    now = _now()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO news_sentiment_scores
                (cluster_id, sentiment_label, sentiment_score,
                 confidence_score, compound_raw, pos_raw, neg_raw, neu_raw,
                 model_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (cluster_id, sentiment_label, sentiment_score,
             confidence_score, compound_raw, pos_raw, neg_raw, neu_raw,
             model_version, now),
        )


def get_sentiment_for_item(
    db: DB, raw_news_item_id: int, symbol: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        if symbol:
            row = conn.execute(
                "SELECT * FROM news_sentiment_scores WHERE raw_news_item_id=? AND symbol=?",
                (raw_news_item_id, symbol),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM news_sentiment_scores WHERE raw_news_item_id=? AND symbol IS NULL",
                (raw_news_item_id,),
            ).fetchone()
        return dict(row) if row else None


def get_sentiment_trend(
    db: DB,
    symbol: str,
    since_utc: str,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Returns time-ordered sentiment scores for a symbol since since_utc."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT nss.*, rni.published_utc, rni.title
            FROM news_sentiment_scores nss
            JOIN raw_news_items rni ON rni.id = nss.raw_news_item_id
            WHERE nss.symbol = ?
              AND rni.published_utc >= ?
            ORDER BY rni.published_utc ASC
            LIMIT ?
            """,
            (symbol, since_utc, limit),
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# news_narratives
# ---------------------------------------------------------------------------

def upsert_narrative(
    db: DB,
    *,
    cluster_id: int,
    narrative_type: str,
    narrative_confidence: float,
    matched_keywords: Optional[str] = None,
    severity_level: Optional[str] = None,
) -> None:
    now = _now()
    severity = severity_level or (
        "HIGH" if narrative_confidence >= 0.75 else
        "MEDIUM" if narrative_confidence >= 0.45 else
        "LOW"
    )
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO news_narratives
                (cluster_id, narrative_type, narrative_confidence,
                 severity_level, matched_keywords, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cluster_id, narrative_type) DO UPDATE SET
                narrative_confidence=CASE
                    WHEN excluded.narrative_confidence > news_narratives.narrative_confidence
                    THEN excluded.narrative_confidence
                    ELSE news_narratives.narrative_confidence
                END,
                severity_level=CASE
                    WHEN excluded.narrative_confidence >= news_narratives.narrative_confidence
                    THEN excluded.severity_level
                    ELSE news_narratives.severity_level
                END,
                matched_keywords=CASE
                    WHEN excluded.narrative_confidence >= news_narratives.narrative_confidence
                    THEN excluded.matched_keywords
                    ELSE news_narratives.matched_keywords
                END,
                updated_at=excluded.updated_at
            """,
            (cluster_id, narrative_type, narrative_confidence,
             severity, matched_keywords, now, now),
        )


def get_narratives_for_cluster(
    db: DB, cluster_id: int
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """SELECT * FROM news_narratives
               WHERE cluster_id=?
               ORDER BY narrative_confidence DESC""",
            (cluster_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_active_narratives(
    db: DB,
    since_utc: str,
    narrative_type: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    query = """
        SELECT nn.*, nc.canonical_title, nc.first_seen_utc, nc.last_seen_utc,
               nc.source_count, nc.is_manipulation_suspect
        FROM news_narratives nn
        JOIN news_clusters nc ON nc.id = nn.cluster_id
        WHERE nc.first_seen_utc >= ?
    """
    params: list = [since_utc]
    if narrative_type:
        query += " AND nn.narrative_type = ?"
        params.append(narrative_type)
    query += " ORDER BY nc.first_seen_utc DESC LIMIT ?"
    params.append(limit)
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# news_intelligence_signals
# ---------------------------------------------------------------------------

def insert_signal(
    db: DB,
    *,
    cluster_id: int,
    symbol: str,
    signal_type: str,
    sentiment_label: Optional[str] = None,
    confidence_score: float,
    reliability_score: float,
    narrative_type: Optional[str] = None,
    sentiment_score: Optional[float] = None,
    spam_score: float = 0.0,
    latency_score: float = 1.0,
    source_validation_passed: bool = False,
    market_validation_passed: bool = False,
    is_valid_signal: bool = False,
    manipulation_flag: Optional[str] = None,
    data_quality_status: str = "LOW_CONFIDENCE",
    sentiment_accuracy: Optional[str] = None,
    validation_status: str = "PENDING_MARKET_VALIDATION",
    market_confirmation_status: str = "PENDING_MARKET_VALIDATION",
    shadow_only: bool = True,
    should_affect_trading: bool = False,
    validated_at: Optional[str] = None,
    suppression_reason: Optional[str] = None,
) -> int:
    now = _now()
    # HARD ENFORCEMENT: news signals NEVER affect trading in Phase 3
    should_affect_trading = False
    shadow_only = True
    is_valid_signal = bool(source_validation_passed and market_validation_passed)
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO news_intelligence_signals
                (cluster_id, symbol, signal_type,
                 sentiment_label, confidence_score, reliability_score, narrative_type,
                 sentiment_score, spam_score, latency_score,
                 source_validation_passed, market_validation_passed,
                 is_valid_signal, manipulation_flag, data_quality_status,
                 sentiment_accuracy, validation_status, market_confirmation_status,
                 shadow_only, should_affect_trading,
                 validated_at, suppression_reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (cluster_id, symbol, signal_type,
             sentiment_label,
             confidence_score, reliability_score, narrative_type,
             sentiment_score, spam_score, latency_score,
             1 if source_validation_passed else 0,
             1 if market_validation_passed else 0,
             1 if is_valid_signal else 0, manipulation_flag, data_quality_status,
             sentiment_accuracy, validation_status, market_confirmation_status,
             1, 0,
             validated_at, suppression_reason, now),
        )
        return cur.lastrowid  # type: ignore[return-value]


def update_signal_validation_outcome(
    db: DB,
    *,
    cluster_id: int,
    symbol: str,
    market_validation_passed: bool,
    market_confirmation_status: str,
    sentiment_accuracy: Optional[str] = None,
    validation_status: Optional[str] = None,
    suppression_reason: Optional[str] = None,
) -> int:
    now = _now()
    final_validation_status = validation_status or (
        "VALIDATED" if market_validation_passed else "INVALIDATED"
    )
    with db.connect() as conn:
        cur = conn.execute(
            """
            UPDATE news_intelligence_signals
            SET market_validation_passed = ?,
                market_confirmation_status = ?,
                sentiment_accuracy = COALESCE(?, sentiment_accuracy),
                validation_status = ?,
                validated_at = ?,
                is_valid_signal = CASE
                    WHEN source_validation_passed = 1 AND ? = 1 THEN 1
                    ELSE 0
                END,
                suppression_reason = CASE
                    WHEN ? IS NOT NULL THEN ?
                    ELSE suppression_reason
                END
            WHERE cluster_id = ?
              AND symbol = ?
              AND shadow_only = 1
            """,
            (
                1 if market_validation_passed else 0,
                market_confirmation_status,
                sentiment_accuracy,
                final_validation_status,
                now,
                1 if market_validation_passed else 0,
                suppression_reason,
                suppression_reason,
                cluster_id,
                symbol,
            ),
        )
    return cur.rowcount


def get_active_signals(
    db: DB,
    symbol: Optional[str] = None,
    since_utc: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    query = """
        SELECT * FROM news_intelligence_signals
        WHERE should_affect_trading = 0
    """
    params: list = []
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    if since_utc:
        query += " AND created_at >= ?"
        params.append(since_utc)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_signals_for_cluster(db: DB, cluster_id: int) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            "SELECT * FROM news_intelligence_signals WHERE cluster_id=? ORDER BY created_at DESC",
            (cluster_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_signal_stats(db: DB, since_utc: str) -> List[Dict[str, Any]]:
    """Count signals grouped by symbol + signal_type since since_utc."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT symbol, signal_type,
                   COUNT(*) as signal_count,
                   AVG(confidence_score) as avg_confidence,
                   AVG(is_valid_signal) as valid_ratio,
                   SUM(CASE WHEN validation_status = 'PENDING_MARKET_VALIDATION' THEN 1 ELSE 0 END) as pending_validation_count,
                   SUM(CASE WHEN validation_status = 'VALIDATED' THEN 1 ELSE 0 END) as validated_count,
                   SUM(CASE WHEN validation_status = 'INVALIDATED' THEN 1 ELSE 0 END) as invalidated_count,
                   MAX(created_at) as last_signal_utc
            FROM news_intelligence_signals
            WHERE created_at >= ?
              AND should_affect_trading = 0
            GROUP BY symbol, signal_type
            ORDER BY last_signal_utc DESC
            """,
            (since_utc,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_data_quality_summary(db: DB) -> Dict[str, Any]:
    """Aggregate data quality breakdown across all clusters."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT data_quality_status, COUNT(*) as count
            FROM news_clusters
            GROUP BY data_quality_status
            """
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) FROM news_clusters").fetchone()[0]
        valid = conn.execute(
            "SELECT COUNT(*) FROM news_clusters WHERE is_valid_signal = 1"
        ).fetchone()[0]
        spam = conn.execute(
            "SELECT COUNT(*) FROM news_clusters WHERE spam_score >= 0.45"
        ).fetchone()[0]
        manip = conn.execute(
            "SELECT COUNT(*) FROM news_clusters WHERE manipulation_flag IS NOT NULL"
        ).fetchone()[0]
    return {
        "total_clusters": total,
        "valid_clusters": valid,
        "spam_clusters": spam,
        "manipulation_suspects": manip,
        "by_status": [dict(r) for r in rows],
    }


# ---------------------------------------------------------------------------
# news_clusters — raw row helper (for validation service)
# ---------------------------------------------------------------------------

def get_cluster_row(db: DB, cluster_id: int) -> Optional[Dict[str, Any]]:
    """Return the full news_clusters row or None."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute(
            "SELECT * FROM news_clusters WHERE id = ?", (cluster_id,)
        ).fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# news_market_reactions
# ---------------------------------------------------------------------------

def insert_news_market_reaction(
    db: DB,
    *,
    cluster_id: int,
    symbol: str,
    event_reaction_id: Optional[int] = None,
    sentiment_score: Optional[float] = None,
    sentiment_direction: Optional[str] = None,
    actual_direction: Optional[str] = None,
    sentiment_accuracy: str = "NEUTRAL",
    sentiment_accuracy_score: float = 0.0,
    impact_score: float = 0.0,
    max_price_move_pct: Optional[float] = None,
    volatility_expansion: Optional[float] = None,
    volume_spike: Optional[float] = None,
    reaction_type: str = "NO_REACTION",
    reaction_latency_minutes: Optional[float] = None,
    reaction_latency_category: str = "NO_REACTION",
    signal_effectiveness_score: float = 0.0,
    is_false_signal: bool = False,
    false_signal_reason: Optional[str] = None,
    data_quality_score: float = 0.0,
    reliability_score: float = 0.0,
) -> int:
    now = _now()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO news_market_reactions (
                cluster_id, symbol, event_reaction_id,
                sentiment_score, sentiment_direction, actual_direction,
                sentiment_accuracy, sentiment_accuracy_score,
                impact_score, max_price_move_pct,
                volatility_expansion, volume_spike,
                reaction_type, reaction_latency_minutes,
                reaction_latency_category, signal_effectiveness_score,
                is_false_signal, false_signal_reason,
                data_quality_score, reliability_score,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                cluster_id, symbol, event_reaction_id,
                sentiment_score, sentiment_direction, actual_direction,
                sentiment_accuracy, sentiment_accuracy_score,
                impact_score, max_price_move_pct,
                volatility_expansion, volume_spike,
                reaction_type, reaction_latency_minutes,
                reaction_latency_category, signal_effectiveness_score,
                1 if is_false_signal else 0, false_signal_reason,
                data_quality_score, reliability_score,
                now, now,
            ),
        )
        return cur.lastrowid  # type: ignore[return-value]


def get_market_reactions_for_cluster(
    db: DB, cluster_id: int
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            "SELECT * FROM news_market_reactions WHERE cluster_id = ? "
            "ORDER BY created_at DESC",
            (cluster_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_recent_validations(
    db: DB,
    *,
    since_utc: Optional[str] = None,
    symbol: Optional[str] = None,
    false_only: bool = False,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Join news_market_reactions with news_clusters for dashboard display."""
    from datetime import timedelta
    if since_utc is None:
        since_utc = (
            datetime.now(timezone.utc) - timedelta(hours=24)
        ).isoformat()

    conditions = ["nmr.created_at >= ?"]
    params: List[Any] = [since_utc]

    if symbol:
        conditions.append("nmr.symbol = ?")
        params.append(symbol)
    if false_only:
        conditions.append("nmr.is_false_signal = 1")

    where = " AND ".join(conditions)
    params.append(limit)

    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            f"""
            SELECT
                nmr.*,
                nc.canonical_title,
                nc.first_seen_utc,
                nc.data_quality_status,
                nc.manipulation_flag
            FROM news_market_reactions nmr
            LEFT JOIN news_clusters nc ON nc.id = nmr.cluster_id
            WHERE {where}
            ORDER BY nmr.created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]


def get_validation_summary(db: DB) -> Dict[str, Any]:
    """Aggregate accuracy + false-signal statistics across all validations."""
    with db.connect() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions"
        ).fetchone()[0]
        correct = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions "
            "WHERE sentiment_accuracy = 'CORRECT'"
        ).fetchone()[0]
        incorrect = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions "
            "WHERE sentiment_accuracy = 'INCORRECT'"
        ).fetchone()[0]
        false_ct = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions WHERE is_false_signal = 1"
        ).fetchone()[0]
        avg_impact = conn.execute(
            "SELECT AVG(impact_score) FROM news_market_reactions"
        ).fetchone()[0] or 0.0
        avg_eff = conn.execute(
            "SELECT AVG(signal_effectiveness_score) FROM news_market_reactions"
        ).fetchone()[0] or 0.0

        # Impact distribution buckets
        conn.row_factory = __import__("sqlite3").Row
        by_latency = conn.execute(
            """
            SELECT reaction_latency_category, COUNT(*) as count
            FROM news_market_reactions
            GROUP BY reaction_latency_category
            """
        ).fetchall()
        by_false = conn.execute(
            """
            SELECT false_signal_reason, COUNT(*) as count
            FROM news_market_reactions
            WHERE is_false_signal = 1
            GROUP BY false_signal_reason
            """
        ).fetchall()

    n = max(1, total)
    return {
        "total_validations": total,
        "correct_count": correct,
        "incorrect_count": incorrect,
        "false_signal_count": false_ct,
        "correct_pct": round(correct / n * 100, 1),
        "incorrect_pct": round(incorrect / n * 100, 1),
        "false_signal_pct": round(false_ct / n * 100, 1),
        "avg_impact_score": round(avg_impact, 4),
        "avg_effectiveness_score": round(avg_eff, 4),
        "by_latency_category": [dict(r) for r in by_latency],
        "by_false_reason": [dict(r) for r in by_false],
    }


def purge_expired_signals(db: DB, max_age_hours: int = 168) -> int:
    """
    Delete shadow signals older than max_age_hours.
    Returns the number of rows deleted.
    """
    from datetime import datetime, timezone, timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
    with db.connect() as conn:
        cur = conn.execute(
            "DELETE FROM news_intelligence_signals WHERE created_at < ?",
            (cutoff,),
        )
    return cur.rowcount
