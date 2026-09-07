"""
PnL Service - Realized and Unrealized Profit/Loss Calculations

Provides multi-user, multi-broker P&L reporting using:
- Realized P&L from trade_fills (execution truth)
- Unrealized P&L from equity_snapshots (broker truth)
"""
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone, timedelta
from shared_lib.persistence.db import DB
import logging

logger = logging.getLogger(__name__)


class PnLService:
    """Calculate realized and unrealized P&L across users, bots, and brokers"""
    
    def __init__(self, db: DB):
        self.db = db
    
    def get_realized_pnl(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calculate total realized P&L from closed positions.
        
        Args:
            user_id: User ID (required)
            broker_account_id: Filter by broker account
            bot_instance_id: Filter by bot instance
            symbol: Filter by symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            {
                "total_pnl": float,
                "total_fees": float,
                "net_pnl": float (pnl - fees),
                "trade_count": int,
                "profitable_trades": int,
                "losing_trades": int,
                "currency": str,
                "filters": dict
            }
        """
        where_clauses = ["user_id = ?", "action = 'CLOSE'"]
        params = [user_id]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        if bot_instance_id:
            where_clauses.append("bot_instance_id = ?")
            params.append(bot_instance_id)
        
        if symbol:
            where_clauses.append("symbol = ?")
            params.append(symbol)
        
        if start_date:
            where_clauses.append("timestamp_utc >= ?")
            params.append(start_date.isoformat())
        
        if end_date:
            where_clauses.append("timestamp_utc <= ?")
            params.append(end_date.isoformat())
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            result = conn.execute(
                f"""
                SELECT 
                    COALESCE(SUM(realized_pnl), 0.0) as total_pnl,
                    COALESCE(SUM(fee), 0.0) as total_fees,
                    COUNT(*) as trade_count,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as profitable_trades,
                    SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                    quote_currency
                FROM trade_fills
                WHERE {where_sql}
                GROUP BY quote_currency
                """,
                params
            ).fetchone()
        
        if not result or result["trade_count"] == 0:
            return {
                "total_pnl": 0.0,
                "total_fees": 0.0,
                "net_pnl": 0.0,
                "trade_count": 0,
                "profitable_trades": 0,
                "losing_trades": 0,
                "currency": "USDT",
                "filters": {
                    "broker_account_id": broker_account_id,
                    "bot_instance_id": bot_instance_id,
                    "symbol": symbol,
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None
                }
            }
        
        total_pnl = float(result["total_pnl"] or 0.0)
        total_fees = float(result["total_fees"] or 0.0)
        
        return {
            "total_pnl": total_pnl,
            "total_fees": total_fees,
            "net_pnl": total_pnl - total_fees,
            "trade_count": result["trade_count"],
            "profitable_trades": result["profitable_trades"] or 0,
            "losing_trades": result["losing_trades"] or 0,
            "currency": result["quote_currency"] or "USDT",
            "filters": {
                "broker_account_id": broker_account_id,
                "bot_instance_id": bot_instance_id,
                "symbol": symbol,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            }
        }
    
    def get_unrealized_pnl(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get current unrealized P&L from latest equity snapshots.
        
        Args:
            user_id: User ID (required)
            broker_account_id: Filter by specific broker account
            
        Returns:
            {
                "total_unrealized_pnl": float,
                "accounts": List[dict],  # Per-account breakdown
                "currency": str,
                "timestamp": str
            }
        """
        where_clauses = ["user_id = ?"]
        params = [user_id]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            # Get latest snapshot per broker account
            rows = conn.execute(
                f"""
                WITH ranked_snapshots AS (
                    SELECT 
                        broker_account_id,
                        broker_id,
                        unrealized_pnl,
                        currency,
                        timestamp_utc,
                        ROW_NUMBER() OVER (
                            PARTITION BY broker_account_id 
                            ORDER BY timestamp_utc DESC
                        ) as rn
                    FROM equity_snapshots
                    WHERE {where_sql}
                )
                SELECT 
                    broker_account_id,
                    broker_id,
                    unrealized_pnl,
                    currency,
                    timestamp_utc
                FROM ranked_snapshots
                WHERE rn = 1
                """,
                params
            ).fetchall()
        
        accounts = []
        total_unrealized = 0.0
        latest_timestamp = None
        
        for row in rows:
            upnl = float(row["unrealized_pnl"] or 0.0)
            total_unrealized += upnl
            
            accounts.append({
                "broker_account_id": row["broker_account_id"],
                "broker_id": row["broker_id"],
                "unrealized_pnl": upnl,
                "currency": row["currency"] or "USDT",
                "timestamp": row["timestamp_utc"]
            })
            
            if not latest_timestamp or row["timestamp_utc"] > latest_timestamp:
                latest_timestamp = row["timestamp_utc"]
        
        return {
            "total_unrealized_pnl": total_unrealized,
            "accounts": accounts,
            "currency": "USDT",  # Assuming all in USDT for now
            "timestamp": latest_timestamp
        }
    
    def get_total_pnl(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get total P&L (realized + unrealized).
        
        Args:
            user_id: User ID (required)
            broker_account_id: Filter by broker account
            bot_instance_id: Filter by bot instance
            start_date: Start date for realized P&L
            end_date: End date for realized P&L
            
        Returns:
            {
                "realized_pnl": float,
                "unrealized_pnl": float,
                "total_pnl": float,
                "total_fees": float,
                "net_total_pnl": float,
                "currency": str
            }
        """
        realized = self.get_realized_pnl(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            start_date=start_date,
            end_date=end_date
        )
        
        unrealized = self.get_unrealized_pnl(
            user_id=user_id,
            broker_account_id=broker_account_id
        )
        
        return {
            "realized_pnl": realized["total_pnl"],
            "unrealized_pnl": unrealized["total_unrealized_pnl"],
            "total_pnl": realized["total_pnl"] + unrealized["total_unrealized_pnl"],
            "total_fees": realized["total_fees"],
            "net_total_pnl": realized["net_pnl"] + unrealized["total_unrealized_pnl"],
            "trade_count": realized["trade_count"],
            "profitable_trades": realized["profitable_trades"],
            "losing_trades": realized["losing_trades"],
            "currency": "USDT"
        }
    
    def get_pnl_breakdown(
        self,
        user_id: str,
        group_by: str = "broker_account",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get P&L breakdown by account, bot, or symbol.
        
        Args:
            user_id: User ID (required)
            group_by: 'broker_account', 'bot_instance', or 'symbol'
            start_date: Start date
            end_date: End date
            
        Returns:
            {
                "group_by": str,
                "breakdown": List[dict],
                "total_pnl": float,
                "currency": str
            }
        """
        group_column_map = {
            "broker_account": "broker_account_id",
            "bot_instance": "bot_instance_id",
            "symbol": "symbol"
        }
        
        if group_by not in group_column_map:
            raise ValueError(f"Invalid group_by: {group_by}. Must be one of {list(group_column_map.keys())}")
        
        group_column = group_column_map[group_by]
        
        where_clauses = ["user_id = ?", "action = 'CLOSE'"]
        params = [user_id]
        
        if start_date:
            where_clauses.append("timestamp_utc >= ?")
            params.append(start_date.isoformat())
        
        if end_date:
            where_clauses.append("timestamp_utc <= ?")
            params.append(end_date.isoformat())
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT 
                    {group_column} as group_key,
                    COALESCE(SUM(realized_pnl), 0.0) as total_pnl,
                    COALESCE(SUM(fee), 0.0) as total_fees,
                    COUNT(*) as trade_count,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as profitable_trades,
                    SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades
                FROM trade_fills
                WHERE {where_sql} AND {group_column} IS NOT NULL
                GROUP BY {group_column}
                ORDER BY total_pnl DESC
                """,
                params
            ).fetchall()
        
        breakdown = []
        total_pnl = 0.0
        
        for row in rows:
            pnl = float(row["total_pnl"] or 0.0)
            fees = float(row["total_fees"] or 0.0)
            total_pnl += pnl
            
            breakdown.append({
                group_by: row["group_key"],
                "total_pnl": pnl,
                "total_fees": fees,
                "net_pnl": pnl - fees,
                "trade_count": row["trade_count"],
                "profitable_trades": row["profitable_trades"] or 0,
                "losing_trades": row["losing_trades"] or 0,
                "win_rate": (row["profitable_trades"] or 0) / row["trade_count"] * 100 if row["trade_count"] > 0 else 0
            })
        
        return {
            "group_by": group_by,
            "breakdown": breakdown,
            "total_pnl": total_pnl,
            "currency": "USDT"
        }


# Singleton instance
_pnl_service_instance = None

def get_pnl_service() -> PnLService:
    """Get singleton PnL service instance"""
    global _pnl_service_instance
    if _pnl_service_instance is None:
        _pnl_service_instance = PnLService(DB())
    return _pnl_service_instance
