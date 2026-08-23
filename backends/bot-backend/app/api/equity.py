"""
Equity Reporting API Endpoints

Provides equity curve, latest snapshots, and historical equity data
from the equity_snapshots table.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timezone, timedelta
from shared_lib.persistence.db import DB
from app.core.config import settings
from app.core.auth import get_current_active_user
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


def _trade_fills_user_scope_sql() -> str:
    """
    Match analytics_reporting.py scoping for trade_fills.

    Primary scope: trade_fills.user_id == current user.
    Fallback scope: legacy fills may have NULL/empty user_id; scope via bot_instances.user_id
    joined on trade_fills.bot_instance_id.
    """
    return "(f.user_id = ? OR ((f.user_id IS NULL OR f.user_id = '') AND bi.user_id = ?))"

def get_db():
    """Dependency to get DB instance."""
    # Use the configured database path (cosmicforge.db) — NOT the fallback bot.db
    db_path = settings.DATABASE_URL.replace("sqlite:///", "")
    return DB(db_path)


@router.get("/equity-curve")
async def get_equity_curve(
    broker_account_id: Optional[str] = Query(None, description="Filter by broker account"),
    bot_instance_id: Optional[str] = Query(None, description="Filter by bot instance"),
    days: int = Query(30, ge=1, le=365, description="Number of days to fetch"),
    interval: str = Query("1h", description="Data interval: 1h, 6h, 1d"),
    current_user: dict = Depends(get_current_active_user),
    db: DB = Depends(get_db)
):
    """
    Get equity curve time series data.
    
    Returns equity snapshots over time, optionally filtered by broker account or bot.
    Supports different aggregation intervals.
    """
    try:
        user_id = current_user.get("id")
        
        # Build query based on filters
        where_clauses = ["user_id = ?"]
        params = [user_id]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        if bot_instance_id:
            where_clauses.append("bot_instance_id = ?")
            params.append(bot_instance_id)
        
        # Time filter
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        where_clauses.append("timestamp_utc >= ?")
        params.append(cutoff_time.isoformat())
        
        where_sql = " AND ".join(where_clauses)
        
        # For now, return all snapshots (can add downsampling later)
        with db.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT 
                    id,
                    broker_account_id,
                    bot_instance_id,
                    broker_id,
                    timestamp_utc,
                    wallet_balance,
                    equity,
                    available_balance,
                    unrealized_pnl,
                    margin_used,
                    currency,
                    source
                FROM equity_snapshots
                WHERE {where_sql}
                ORDER BY timestamp_utc ASC
                """,
                params
            ).fetchall()
        
        # Convert to list of dicts
        data_points = []
        for row in rows:
            data_points.append({
                "timestamp": row["timestamp_utc"],
                "equity": float(row["equity"] or 0.0),
                "wallet_balance": float(row["wallet_balance"] or 0.0),
                "available_balance": float(row["available_balance"] or 0.0),
                "unrealized_pnl": float(row["unrealized_pnl"] or 0.0),
                "margin_used": float(row["margin_used"] or 0.0),
                "broker_id": row["broker_id"],
                "broker_account_id": row["broker_account_id"],
                "bot_instance_id": row["bot_instance_id"],
                "source": row["source"]
            })
        
        return {
            "data": data_points,
            "count": len(data_points),
            "broker_account_id": broker_account_id,
            "bot_instance_id": bot_instance_id,
            "period_days": days,
            "currency": "USDT"
        }
    
    except Exception as e:
        logger.error(f"Failed to fetch equity curve: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/equity-latest")
async def get_latest_equity(
    current_user: dict = Depends(get_current_active_user),
    db: DB = Depends(get_db)
):
    """
    Get the latest equity snapshot for each broker account owned by the user.
    
    Returns the most recent snapshot per broker account.
    """
    try:
        user_id = current_user.get("id")
        
        with db.connect() as conn:
            # Get latest snapshot per broker_account_id
            rows = conn.execute(
                """
                WITH ranked_snapshots AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY broker_account_id 
                            ORDER BY timestamp_utc DESC
                        ) as rn
                    FROM equity_snapshots
                    WHERE user_id = ?
                )
                SELECT 
                    broker_account_id,
                    broker_id,
                    timestamp_utc,
                    wallet_balance,
                    equity,
                    available_balance,
                    unrealized_pnl,
                    margin_used,
                    currency,
                    source
                FROM ranked_snapshots
                WHERE rn = 1
                ORDER BY broker_id, broker_account_id
                """,
                (user_id,)
            ).fetchall()
        
        accounts = []
        for row in rows:
            accounts.append({
                "broker_account_id": row["broker_account_id"],
                "broker_id": row["broker_id"],
                "timestamp": row["timestamp_utc"],
                "equity": float(row["equity"] or 0.0),
                "wallet_balance": float(row["wallet_balance"] or 0.0),
                "available_balance": float(row["available_balance"] or 0.0),
                "unrealized_pnl": float(row["unrealized_pnl"] or 0.0),
                "margin_used": float(row["margin_used"] or 0.0),
                "currency": row["currency"] or "USDT",
                "source": row["source"]
            })
        
        # Calculate total across all accounts
        total_equity = sum(a["equity"] for a in accounts)
        total_unrealized_pnl = sum(a["unrealized_pnl"] for a in accounts)
        
        return {
            "accounts": accounts,
            "total_equity": total_equity,
            "total_unrealized_pnl": total_unrealized_pnl,
            "currency": "USDT",
            "account_count": len(accounts)
        }
    
    except Exception as e:
        logger.error(f"Failed to fetch latest equity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reconciliation")
async def get_reconciliation(
    days: int = Query(30, ge=1, le=365, description="Reconciliation window in days"),
    broker_account_id: Optional[str] = Query(None, description="Optional broker account filter"),
    bot_instance_id: Optional[str] = Query(None, description="Optional bot instance filter"),
    current_user: dict = Depends(get_current_active_user),
    db: DB = Depends(get_db),
):
    """
    Reconcile broker equity movement vs closed-trade PnL derived from trade_fills.

    Read-only diagnostic endpoint:
    - Uses equity_snapshots for balance/equity/unrealized.
    - Uses trade_fills grouped by (position_id OR synthetic fill_<id>) for closed PnL totals.
    - Does NOT place/cancel orders and does NOT modify execution logic.
    """
    user_id = current_user.get("id")
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
    scope_sql = _trade_fills_user_scope_sql()

    try:
        with db.connect() as conn:
            # -----------------------------
            # Equity snapshots (balance source)
            # -----------------------------
            eq_where = ["user_id = ?", "timestamp_utc >= ?"]
            eq_params: list = [user_id, cutoff_time.isoformat()]
            if broker_account_id:
                eq_where.append("broker_account_id = ?")
                eq_params.append(broker_account_id)
            if bot_instance_id:
                eq_where.append("bot_instance_id = ?")
                eq_params.append(bot_instance_id)

            eq_where_sql = " AND ".join(eq_where)

            latest_eq = conn.execute(
                f"""
                SELECT broker_account_id, broker_id, timestamp_utc, wallet_balance, equity,
                       available_balance, unrealized_pnl, margin_used, currency, source
                FROM equity_snapshots
                WHERE {eq_where_sql}
                ORDER BY timestamp_utc DESC
                LIMIT 1
                """,
                eq_params,
            ).fetchone()

            start_eq = conn.execute(
                f"""
                SELECT broker_account_id, broker_id, timestamp_utc, wallet_balance, equity,
                       available_balance, unrealized_pnl, margin_used, currency, source
                FROM equity_snapshots
                WHERE {eq_where_sql}
                ORDER BY timestamp_utc ASC
                LIMIT 1
                """,
                eq_params,
            ).fetchone()

            account = None
            balance_change = None
            if latest_eq is not None:
                account = {
                    "broker_account_id": latest_eq["broker_account_id"],
                    "broker_id": latest_eq["broker_id"],
                    "timestamp": latest_eq["timestamp_utc"],
                    "wallet_balance": float(latest_eq["wallet_balance"] or 0.0),
                    "equity": float(latest_eq["equity"] or 0.0),
                    "available_balance": float(latest_eq["available_balance"] or 0.0),
                    "unrealized_pnl": float(latest_eq["unrealized_pnl"] or 0.0),
                    "margin_used": float(latest_eq["margin_used"] or 0.0),
                    "currency": latest_eq["currency"] or "USDT",
                    "source": latest_eq["source"],
                }
                if start_eq is not None:
                    balance_change = {
                        "from_ts": start_eq["timestamp_utc"],
                        "to_ts": latest_eq["timestamp_utc"],
                        "wallet_balance_change": float(latest_eq["wallet_balance"] or 0.0) - float(start_eq["wallet_balance"] or 0.0),
                        "equity_change": float(latest_eq["equity"] or 0.0) - float(start_eq["equity"] or 0.0),
                    }

            # -----------------------------
            # trade_fills closed PnL (read-model truth)
            # -----------------------------
            tf_where = [scope_sql]
            tf_params: list = [user_id, user_id]

            if broker_account_id:
                tf_where.append(
                    "(f.broker_account_id = ? OR ((f.broker_account_id IS NULL OR f.broker_account_id = '') AND bi.broker_account_id = ?))"
                )
                tf_params.extend([broker_account_id, broker_account_id])
            if bot_instance_id:
                tf_where.append("f.bot_instance_id = ?")
                tf_params.append(bot_instance_id)

            tf_where.append("f.timestamp_utc >= ?")
            tf_params.append(cutoff_time.isoformat())

            tf_where_sql = " AND ".join(tf_where)

            # Grouped-by-position totals (includes synthetic fill_<id> when position_id missing)
            grouped_row = conn.execute(
                f"""
                WITH fill_data AS (
                    SELECT
                        CASE
                            WHEN f.position_id IS NULL OR f.position_id = '' THEN ('fill_' || CAST(f.id AS TEXT))
                            ELSE f.position_id
                        END AS pos_id,
                        f.action,
                        COALESCE(f.realized_pnl, 0) AS realized_pnl,
                        COALESCE(f.fee, 0) AS fee
                    FROM trade_fills f
                    LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
                    WHERE {tf_where_sql}
                ),
                grouped AS (
                    SELECT
                        pos_id,
                        SUM(CASE WHEN action = 'CLOSE' THEN realized_pnl ELSE 0 END) AS realized_sum,
                        SUM(fee) AS fees_sum,
                        SUM(CASE WHEN action = 'CLOSE' THEN 1 ELSE 0 END) AS close_count
                    FROM fill_data
                    GROUP BY pos_id
                )
                SELECT
                    SUM(CASE WHEN close_count > 0 THEN 1 ELSE 0 END) AS closed_positions,
                    SUM(CASE WHEN close_count > 0 THEN realized_sum ELSE 0 END) AS gross_realized_pnl,
                    SUM(CASE WHEN close_count > 0 THEN fees_sum ELSE 0 END) AS fees
                FROM grouped
                """,
                tf_params,
            ).fetchone()

            closed_positions = int(grouped_row["closed_positions"] or 0) if grouped_row else 0
            gross_realized = float(grouped_row["gross_realized_pnl"] or 0.0) if grouped_row else 0.0
            fees = float(grouped_row["fees"] or 0.0) if grouped_row else 0.0
            net_closed = gross_realized - fees

            trade_fills_summary = {
                "closed_positions": closed_positions,
                "gross_realized_pnl": gross_realized,
                "fees": fees,
                "net_pnl": net_closed,
            }

            # -----------------------------
            # positions/history-equivalent totals
            # (group by position_id OR synthetic fill_<id>)
            # -----------------------------
            positions_row = conn.execute(
                f"""
                WITH fill_data AS (
                    SELECT
                        CASE
                            WHEN f.position_id IS NULL OR f.position_id = '' THEN ('fill_' || CAST(f.id AS TEXT))
                            ELSE f.position_id
                        END AS pos_id,
                        f.action,
                        COALESCE(f.realized_pnl, 0) AS realized_pnl,
                        COALESCE(f.fee, 0) AS fee,
                        f.timestamp_utc
                    FROM trade_fills f
                    LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
                    WHERE {tf_where_sql}
                ),
                grouped AS (
                    SELECT
                        pos_id,
                        MIN(timestamp_utc) AS opened_at,
                        MAX(CASE WHEN action = 'CLOSE' THEN timestamp_utc END) AS closed_at,
                        SUM(CASE WHEN action = 'OPEN' THEN 1 ELSE 0 END) AS open_count,
                        SUM(CASE WHEN action = 'CLOSE' THEN 1 ELSE 0 END) AS close_count,
                        SUM(fee) AS total_fees,
                        SUM(CASE WHEN action = 'CLOSE' THEN realized_pnl ELSE 0 END) AS realized_sum
                    FROM fill_data
                    GROUP BY pos_id
                ),
                computed AS (
                    SELECT
                        pos_id,
                        CASE WHEN close_count > 0 THEN 'CLOSED' ELSE 'OPEN' END AS status,
                        realized_sum AS realized_pnl,
                        total_fees,
                        CASE WHEN close_count > 0 THEN (realized_sum - total_fees) ELSE NULL END AS net_pnl,
                        close_count
                    FROM grouped
                )
                SELECT
                    COUNT(*) AS total_positions,
                    SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_positions,
                    SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_positions,
                    SUM(CASE WHEN status = 'CLOSED' THEN realized_pnl ELSE 0 END) AS total_realized_pnl,
                    SUM(CASE WHEN status = 'CLOSED' THEN total_fees ELSE 0 END) AS total_fees,
                    SUM(CASE WHEN net_pnl IS NOT NULL THEN net_pnl ELSE 0 END) AS total_net_pnl,
                    SUM(CASE WHEN status = 'CLOSED' AND pos_id LIKE 'fill_%' THEN 1 ELSE 0 END) AS synthetic_closed_positions
                FROM computed
                """,
                tf_params,
            ).fetchone()

            positions_history_totals = {
                "total": int(positions_row["total_positions"] or 0) if positions_row else 0,
                "open_count": int(positions_row["open_positions"] or 0) if positions_row else 0,
                "closed_count": int(positions_row["closed_positions"] or 0) if positions_row else 0,
                "synthetic_closed_count": int(positions_row["synthetic_closed_positions"] or 0) if positions_row else 0,
                "total_realized_pnl": float(positions_row["total_realized_pnl"] or 0.0) if positions_row else 0.0,
                "total_fees": float(positions_row["total_fees"] or 0.0) if positions_row else 0.0,
                "total_net_pnl": float(positions_row["total_net_pnl"] or 0.0) if positions_row else 0.0,
            }

            # -----------------------------
            # Transfers (if cached)
            # -----------------------------
            transfers_where = ["user_id = ?", "ts_utc >= ?"]
            transfers_params: list = [user_id, cutoff_time.isoformat()]
            if broker_account_id:
                transfers_where.append("broker_account_id = ?")
                transfers_params.append(broker_account_id)
            transfers_where_sql = " AND ".join(transfers_where)

            transfers_row = conn.execute(
                f"""
                SELECT
                    SUM(CASE WHEN type IN ('deposit','DEPOSIT') THEN amount ELSE 0 END) AS deposits,
                    SUM(CASE WHEN type IN ('withdrawal','WITHDRAWAL') THEN amount ELSE 0 END) AS withdrawals,
                    COUNT(*) AS count
                FROM broker_transfers_cache
                WHERE {transfers_where_sql}
                """,
                transfers_params,
            ).fetchone()

            transfers = {
                "count": int(transfers_row["count"] or 0) if transfers_row else 0,
                "deposits": float(transfers_row["deposits"] or 0.0) if transfers_row else 0.0,
                "withdrawals": float(transfers_row["withdrawals"] or 0.0) if transfers_row else 0.0,
            }

            # -----------------------------
            # Difference + heuristics
            # -----------------------------
            warnings: list[str] = []
            likely = "unknown"

            if account is None or balance_change is None:
                warnings.append("No equity_snapshots found in window; balance source unknown.")
            else:
                diff = balance_change["equity_change"] - net_closed
                if abs(diff) < 1e-6:
                    likely = "closed_realized_pnl_matches_equity_change"
                elif abs(account["unrealized_pnl"]) > abs(diff) * 0.6:
                    likely = "open_unrealized_pnl"
                elif transfers["count"] > 0:
                    likely = "deposits_withdrawals"
                else:
                    likely = "missing_persistence_or_other_balance_components"

            response = {
                "account": {
                    **(account or {}),
                    "window_days": days,
                    "balance_change": balance_change,
                },
                "trade_fills": trade_fills_summary,
                "positions_history": positions_history_totals,
                "transfers": transfers,
                "difference": {
                    "equity_change_vs_net_closed_pnl": (balance_change["equity_change"] - net_closed) if balance_change else None,
                    "likely_explanation": likely,
                },
                "metadata": {
                    "scope_mode": "trade_fills.user_id OR bot_instances.user_id",
                    "warnings": warnings,
                },
            }

            return response

    except Exception as e:
        logger.error(f"Failed reconciliation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brokers/{broker_account_id}/equity-history")
async def get_broker_equity_history(
    broker_account_id: str,
    days: int = Query(7, ge=1, le=90, description="Number of days"),
    current_user: dict = Depends(get_current_active_user),
    db: DB = Depends(get_db)
):
    """
    Get equity history for a specific broker account.
    
    Returns time series of equity snapshots for the specified broker account.
    """
    try:
        user_id = current_user.get("id")
        
        # Verify user owns this broker account
        with db.connect() as conn:
            owner_check = conn.execute(
                "SELECT user_id FROM broker_accounts WHERE id = ?",
                (broker_account_id,)
            ).fetchone()
            
            if not owner_check or owner_check["user_id"] != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            
            # Get snapshots
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
            rows = conn.execute(
                """
                SELECT 
                    timestamp_utc,
                    equity,
                    wallet_balance,
                    available_balance,
                    unrealized_pnl,
                    margin_used,
                    source
                FROM equity_snapshots
                WHERE broker_account_id = ? AND timestamp_utc >= ?
                ORDER BY timestamp_utc ASC
                """,
                (broker_account_id, cutoff_time.isoformat())
            ).fetchall()
        
        snapshots = []
        for row in rows:
            snapshots.append({
                "timestamp": row["timestamp_utc"],
                "equity": row["equity"],
                "wallet_balance": row["wallet_balance"],
                "available_balance": row["available_balance"],
                "unrealized_pnl": row["unrealized_pnl"],
                "margin_used": row["margin_used"],
                "source": row["source"]
            })
        
        return {
            "broker_account_id": broker_account_id,
            "snapshots": snapshots,
            "count": len(snapshots),
            "period_days": days,
            "currency": "USDT"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch broker equity history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
