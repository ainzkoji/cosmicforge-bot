"""
Shadow Trading System — DB Persistence Layer

ShadowStore owns all reads and writes to shadow_trades and shadow_trade_outcomes.
It follows the same DB access pattern as the rest of the codebase (DB() singleton,
context-manager connections).

SAFETY CONTRACT:
  - Never writes to trade_fills
  - Never writes to decision_traces
  - Never calls any exchange API
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

# ── Rejection stage constants (used as tag values) ───────────────────────────
STAGE_ML_BLOCKED = "ML_BLOCKED"
STAGE_THRESHOLD_BLOCKED = "THRESHOLD_BLOCKED"
STAGE_CORRELATION_BLOCKED = "CORRELATION_BLOCKED"
STAGE_ALREADY_OPEN = "ALREADY_OPEN_NOT_TAKEN"
STAGE_EXECUTOR_REJECTED = "EXECUTOR_REJECTED"
STAGE_SAFETY_BLOCKED = "SAFETY_BLOCKED"
STAGE_NEAR_MISS = "NEAR_MISS_HOLD"

# ── Outcome constants ─────────────────────────────────────────────────────────
OUTCOME_TP_HIT = "TP_HIT"
OUTCOME_SL_HIT = "SL_HIT"
OUTCOME_EXPIRED = "EXPIRED"
OUTCOME_INVALIDATED = "INVALIDATED"
OUTCOME_CANCELLED = "CANCELLED"
OUTCOME_DATA_MISSING = "DATA_MISSING"

# ── Status constants ──────────────────────────────────────────────────────────
STATUS_PENDING = "PENDING"
STATUS_EVALUATING = "EVALUATING"
STATUS_EVALUATED = "EVALUATED"
STATUS_DATA_MISSING = "DATA_MISSING"
STATUS_CANCELLED = "CANCELLED"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ShadowStore:
    """
    All DB persistence for the shadow trading research system.

    Uses DB() singleton from shared_lib — same pattern as the rest of the codebase.
    """

    def __init__(self, db: Optional[DB] = None):
        self._db = db or DB()

    # ======================================================================
    # SHADOW TRADE WRITE
    # ======================================================================

    def insert_shadow_trade(
        self,
        *,
        bot_instance_id: str,
        trace_id: str,
        symbol: str,
        side: Optional[str],
        regime: Optional[str],
        strategy: Optional[str],
        confidence: Optional[float],
        threshold: Optional[float],
        confidence_gap: Optional[float],
        ml_score: Optional[float],
        ml_action: Optional[str],
        gate_reason: Optional[str],
        rejection_stage: str,
        rejection_reason: Optional[str],
        entry_time: str,
        entry_price: Optional[float],
        stop_loss: Optional[float],
        take_profit: Optional[float],
        expiry_time: Optional[str],
    ) -> str:
        """Insert a new shadow trade candidate. Returns the new shadow_trade_id."""
        shadow_id = str(uuid.uuid4())
        now = _utc_now()

        try:
            with self._db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO shadow_trades (
                        id, bot_instance_id, trace_id, symbol, side, regime,
                        strategy, confidence, threshold, confidence_gap,
                        ml_score, ml_action, gate_reason,
                        rejection_stage, rejection_reason,
                        entry_time, entry_price, stop_loss, take_profit,
                        expiry_time, status, created_at, updated_at
                    ) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    (
                        shadow_id, bot_instance_id, trace_id, symbol,
                        side, regime, strategy,
                        confidence, threshold, confidence_gap,
                        ml_score, ml_action, gate_reason,
                        rejection_stage, rejection_reason,
                        entry_time, entry_price, stop_loss, take_profit,
                        expiry_time, STATUS_PENDING, now, now,
                    ),
                )
        except Exception as exc:
            db_path = getattr(self._db, "path", "unknown")
            logger.error(
                "[ShadowStore] INSERT FAILED | db_path=%s | table=shadow_trades | op=insert_shadow_trade | trace_id=%s | error=%s",
                db_path, trace_id, exc
            )
            raise

        return shadow_id

    def update_status(
        self,
        shadow_trade_id: str,
        status: str,
    ) -> None:
        """Update the status of a shadow trade."""
        try:
            with self._db.connect() as conn:
                conn.execute(
                    "UPDATE shadow_trades SET status=?, updated_at=? WHERE id=?",
                    (status, _utc_now(), shadow_trade_id),
                )
        except Exception as exc:
            logger.error("[ShadowStore] update_status failed for %s: %s", shadow_trade_id, exc)

    # ======================================================================
    # SHADOW OUTCOME WRITE
    # ======================================================================

    def insert_outcome(
        self,
        *,
        shadow_trade_id: str,
        outcome: str,
        exit_time: Optional[str],
        exit_price: Optional[float],
        pnl_abs: Optional[float],
        pnl_pct: Optional[float],
        pnl_net: Optional[float],
        mfe: Optional[float],
        mae: Optional[float],
        bars_elapsed: Optional[int],
        minutes_elapsed: Optional[float],
        evaluation_notes: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Insert a shadow trade outcome. Returns outcome id."""
        outcome_id = str(uuid.uuid4())
        now = _utc_now()

        notes_json = json.dumps(evaluation_notes or {})

        try:
            with self._db.connect() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO shadow_trade_outcomes (
                        id, shadow_trade_id, outcome, exit_time, exit_price,
                        pnl_abs, pnl_pct, pnl_net, mfe, mae,
                        bars_elapsed, minutes_elapsed,
                        evaluation_notes, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        outcome_id, shadow_trade_id, outcome, exit_time, exit_price,
                        pnl_abs, pnl_pct, pnl_net, mfe, mae,
                        bars_elapsed, minutes_elapsed,
                        notes_json, now, now,
                    ),
                )
                # Also update parent shadow_trades status
                conn.execute(
                    "UPDATE shadow_trades SET status=?, updated_at=? WHERE id=?",
                    (STATUS_EVALUATED, now, shadow_trade_id),
                )
        except Exception as exc:
            db_path = getattr(self._db, "path", "unknown")
            logger.error(
                "[ShadowStore] INSERT FAILED | db_path=%s | table=shadow_trade_outcomes | op=insert_outcome | shadow_trade_id=%s | error=%s",
                db_path, shadow_trade_id, exc
            )
            raise

        return outcome_id

    # ======================================================================
    # SHADOW TRADE READS
    # ======================================================================

    def get_pending_trades(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Fetch shadow trades that need evaluation (status=PENDING)."""
        try:
            with self._db.connect() as conn:
                conn.row_factory = __import__("sqlite3").Row
                rows = conn.execute(
                    """
                    SELECT * FROM shadow_trades
                    WHERE status = ?
                    ORDER BY created_at ASC
                    LIMIT ?
                    """,
                    (STATUS_PENDING, limit),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("[ShadowStore] get_pending_trades failed: %s", exc)
            return []

    def get_shadow_trade(self, shadow_trade_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single shadow trade by id."""
        try:
            with self._db.connect() as conn:
                conn.row_factory = __import__("sqlite3").Row
                row = conn.execute(
                    "SELECT * FROM shadow_trades WHERE id=?",
                    (shadow_trade_id,),
                ).fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logger.error("[ShadowStore] get_shadow_trade failed: %s", exc)
            return None

    def get_outcome(self, shadow_trade_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the outcome for a shadow trade."""
        try:
            with self._db.connect() as conn:
                conn.row_factory = __import__("sqlite3").Row
                row = conn.execute(
                    "SELECT * FROM shadow_trade_outcomes WHERE shadow_trade_id=?",
                    (shadow_trade_id,),
                ).fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logger.error("[ShadowStore] get_outcome failed: %s", exc)
            return None

    def list_trades(
        self,
        *,
        rejection_stage: Optional[str] = None,
        status: Optional[str] = None,
        symbol: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List shadow trades with optional filters."""
        query = "SELECT * FROM shadow_trades WHERE 1=1"
        params: List[Any] = []

        if rejection_stage:
            query += " AND rejection_stage=?"
            params.append(rejection_stage)
        if status:
            query += " AND status=?"
            params.append(status)
        if symbol:
            query += " AND symbol=?"
            params.append(symbol)
        if bot_instance_id:
            query += " AND bot_instance_id=?"
            params.append(bot_instance_id)

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]

        try:
            with self._db.connect() as conn:
                conn.row_factory = __import__("sqlite3").Row
                rows = conn.execute(query, params).fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("[ShadowStore] list_trades failed: %s", exc)
            return []

    def list_outcomes(
        self,
        *,
        outcome: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List shadow trade outcomes with optional filters."""
        query = """
            SELECT o.*, s.symbol, s.side, s.rejection_stage,
                   s.confidence, s.threshold, s.ml_score, s.regime
            FROM shadow_trade_outcomes o
            JOIN shadow_trades s ON o.shadow_trade_id = s.id
            WHERE 1=1
        """
        params: List[Any] = []

        if outcome:
            query += " AND o.outcome=?"
            params.append(outcome)

        query += " ORDER BY o.created_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]

        try:
            with self._db.connect() as conn:
                conn.row_factory = __import__("sqlite3").Row
                rows = conn.execute(query, params).fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("[ShadowStore] list_outcomes failed: %s", exc)
            return []

    def count_by_status(self) -> Dict[str, int]:
        """Count shadow trades grouped by status."""
        try:
            with self._db.connect() as conn:
                rows = conn.execute(
                    "SELECT status, COUNT(*) as cnt FROM shadow_trades GROUP BY status"
                ).fetchall()
                return {r[0]: r[1] for r in rows}
        except Exception as exc:
            logger.error("[ShadowStore] count_by_status failed: %s", exc)
            return {}

    def count_by_stage(self) -> Dict[str, int]:
        """Count evaluated shadow trades grouped by rejection_stage."""
        try:
            with self._db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT rejection_stage, COUNT(*) as cnt
                    FROM shadow_trades
                    GROUP BY rejection_stage
                    """
                ).fetchall()
                return {r[0]: r[1] for r in rows}
        except Exception as exc:
            logger.error("[ShadowStore] count_by_stage failed: %s", exc)
            return {}

    def cleanup_old_evaluated(self, days: int = 30) -> int:
        """Remove evaluated shadow records older than N days. Returns deleted count."""
        try:
            with self._db.connect() as conn:
                result = conn.execute(
                    """
                    DELETE FROM shadow_trades
                    WHERE status = 'EVALUATED'
                      AND created_at < datetime('now', ? || ' days')
                    """,
                    (f"-{days}",),
                )
                return result.rowcount
        except Exception as exc:
            logger.error("[ShadowStore] cleanup_old_evaluated failed: %s", exc)
            return 0


# ── Singleton ─────────────────────────────────────────────────────────────────
_store: Optional[ShadowStore] = None


def get_shadow_store(db: Optional[DB] = None) -> ShadowStore:
    """Return the singleton ShadowStore."""
    global _store
    if _store is None:
        _store = ShadowStore(db)
    return _store
