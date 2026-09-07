"""
Trade Statistics Service - Win Rate, Performance Metrics, Trade Analysis

Provides comprehensive trade statistics including:
- Win rate and profit factor
- Average win/loss calculations
- Best/worst trades
- Symbol and time-based performance
"""
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone, timedelta
from shared_lib.persistence.db import DB
import logging

logger = logging.getLogger(__name__)


class TradeStatsService:
    """Calculate trade statistics and performance metrics"""
    
    def __init__(self, db: DB):
        self.db = db
    
    def get_win_rate(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calculate win rate (% of profitable closed trades).
        
        Returns:
            {
                "win_rate": float (0-100),
                "total_trades": int,
                "winning_trades": int,
                "losing_trades": int,
                "breakeven_trades": int
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
                WITH grouped_trades AS (
                    SELECT 
                        COALESCE(position_id, CAST(id AS TEXT)) as pos_id,
                        SUM(realized_pnl) as net_pnl
                    FROM trade_fills
                    WHERE {where_sql}
                    GROUP BY pos_id
                )
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                    SUM(CASE WHEN net_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                    SUM(CASE WHEN net_pnl = 0 THEN 1 ELSE 0 END) as breakeven_trades
                FROM grouped_trades
                """,
                params
            ).fetchone()
        
        if not result or result["total_trades"] == 0:
            return {
                "win_rate": 0.0,
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "breakeven_trades": 0
            }
        
        winning = result["winning_trades"] or 0
        total = result["total_trades"]
        
        return {
            "win_rate": (winning / total * 100) if total > 0 else 0.0,
            "total_trades": total,
            "winning_trades": winning,
            "losing_trades": result["losing_trades"] or 0,
            "breakeven_trades": result["breakeven_trades"] or 0
        }
    
    def get_trade_summary(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive trade summary with win/loss statistics.
        
        Returns:
            {
                "total_trades": int,
                "winning_trades": int,
                "losing_trades": int,
                "win_rate": float,
                "avg_win": float,
                "avg_loss": float,
                "largest_win": float,
                "largest_loss": float,
                "profit_factor": float,
                "total_pnl": float,
                "total_fees": float,
                "currency": str
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
                WITH grouped_trades AS (
                    SELECT 
                        COALESCE(position_id, CAST(id AS TEXT)) as pos_id,
                        SUM(realized_pnl) as net_pnl,
                        SUM(fee) as pos_fee
                    FROM trade_fills
                    WHERE {where_sql}
                    GROUP BY pos_id
                )
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                    SUM(CASE WHEN net_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                    AVG(CASE WHEN net_pnl > 0 THEN net_pnl END) as avg_win,
                    AVG(CASE WHEN net_pnl < 0 THEN net_pnl END) as avg_loss,
                    MAX(net_pnl) as largest_win,
                    MIN(net_pnl) as largest_loss,
                    SUM(CASE WHEN net_pnl > 0 THEN net_pnl ELSE 0 END) as total_wins,
                    SUM(CASE WHEN net_pnl < 0 THEN net_pnl ELSE 0 END) as total_losses,
                    SUM(net_pnl) as total_pnl,
                    SUM(pos_fee) as total_fees
                FROM grouped_trades
                """,
                params
            ).fetchone()
        
        if not result or result["total_trades"] == 0:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "largest_win": 0.0,
                "largest_loss": 0.0,
                "profit_factor": 0.0,
                "expectancy": 0.0,
                "total_pnl": 0.0,
                "total_fees": 0.0,
                "currency": "USDT"
            }
        
        winning = result["winning_trades"] or 0
        total = result["total_trades"]
        total_wins = abs(float(result["total_wins"] or 0.0))
        total_losses = abs(float(result["total_losses"] or 0.0))
        
        profit_factor = 999.0
        if total_losses > 0:
            profit_factor = total_wins / total_losses
            
        win_rate_decimal = (winning / total) if total > 0 else 0.0
        avg_win = float(result["avg_win"] or 0.0)
        avg_loss = float(result["avg_loss"] or 0.0)
        
        # Expectancy = (Win Rate * Avg Win) - (Loss Rate * Abs(Avg Loss))
        loss_rate_decimal = 1.0 - win_rate_decimal
        expectancy = (win_rate_decimal * avg_win) - (loss_rate_decimal * abs(avg_loss))
        
        return {
            "total_trades": total,
            "winning_trades": winning,
            "losing_trades": result["losing_trades"] or 0,
            "win_rate": (winning / total * 100) if total > 0 else 0.0,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "largest_win": float(result["largest_win"] or 0.0),
            "largest_loss": float(result["largest_loss"] or 0.0),
            "profit_factor": profit_factor,
            "expectancy": expectancy,
            "total_pnl": float(result["total_pnl"] or 0.0),
            "total_fees": float(result["total_fees"] or 0.0),
            "currency": "USDT"
        }
    
    def get_best_worst_trades(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get top and bottom performing trades.
        
        Returns:
            {
                "best_trades": List[dict],
                "worst_trades": List[dict]
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
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            # Best trades
            best_rows = conn.execute(
                f"""
                SELECT 
                    symbol,
                    side,
                    qty,
                    price,
                    realized_pnl,
                    fee,
                    timestamp_utc,
                    broker_id
                FROM trade_fills
                WHERE {where_sql}
                ORDER BY realized_pnl DESC
                LIMIT ?
                """,
                params + [limit]
            ).fetchall()
            
            # Worst trades
            worst_rows = conn.execute(
                f"""
                SELECT 
                    symbol,
                    side,
                    qty,
                    price,
                    realized_pnl,
                    fee,
                    timestamp_utc,
                    broker_id
                FROM trade_fills
                WHERE {where_sql}
                ORDER BY realized_pnl ASC
                LIMIT ?
                """,
                params + [limit]
            ).fetchall()
        
        def format_trade(row):
            return {
                "symbol": row["symbol"],
                "side": row["side"],
                "qty": float(row["qty"]),
                "price": float(row["price"]),
                "realized_pnl": float(row["realized_pnl"] or 0.0),
                "fee": float(row["fee"] or 0.0),
                "timestamp": row["timestamp_utc"],
                "broker_id": row["broker_id"]
            }
        
        return {
            "best_trades": [format_trade(row) for row in best_rows],
            "worst_trades": [format_trade(row) for row in worst_rows]
        }
    
    def get_symbol_performance(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get performance breakdown by symbol.
        
        Returns:
            {
                "symbols": List[dict],
                "total_symbols": int
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
                    symbol,
                    COUNT(*) as trade_count,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                    SUM(realized_pnl) as total_pnl,
                    AVG(realized_pnl) as avg_pnl,
                    SUM(fee) as total_fees
                FROM trade_fills
                WHERE {where_sql}
                GROUP BY symbol
                ORDER BY total_pnl DESC
                """,
                params
            ).fetchall()
        
        symbols = []
        for row in rows:
            trade_count = row["trade_count"]
            winning = row["winning_trades"] or 0
            
            symbols.append({
                "symbol": row["symbol"],
                "trade_count": trade_count,
                "winning_trades": winning,
                "win_rate": (winning / trade_count * 100) if trade_count > 0 else 0.0,
                "total_pnl": float(row["total_pnl"] or 0.0),
                "avg_pnl": float(row["avg_pnl"] or 0.0),
                "total_fees": float(row["total_fees"] or 0.0)
            })
        
        return {
            "symbols": symbols,
            "total_symbols": len(symbols)
        }
    
    def get_time_series_performance(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        interval: str = "daily",
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get cumulative P&L over time.
        
        Args:
            interval: 'daily', 'weekly', or 'monthly'
            days: Number of days to look back
            
        Returns:
            {
                "interval": str,
                "data": List[dict],
                "total_pnl": float
            }
        """
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        where_clauses = ["user_id = ?", "action = 'CLOSE'", "timestamp_utc >= ?"]
        params = [user_id, start_date.isoformat()]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        if bot_instance_id:
            where_clauses.append("bot_instance_id = ?")
            params.append(bot_instance_id)
        
        where_sql = " AND ".join(where_clauses)
        
        # Determine grouping format
        date_format = {
            "daily": "%Y-%m-%d",
            "weekly": "%Y-W%W",
            "monthly": "%Y-%m"
        }.get(interval, "%Y-%m-%d")
        
        with self.db.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT 
                    strftime(?, timestamp_utc) as period,
                    SUM(realized_pnl) as period_pnl,
                    COUNT(*) as trade_count
                FROM trade_fills
                WHERE {where_sql}
                GROUP BY period
                ORDER BY period ASC
                """,
                [date_format] + params
            ).fetchall()
        
        cumulative_pnl = 0.0
        data = []
        
        for row in rows:
            period_pnl = float(row["period_pnl"] or 0.0)
            cumulative_pnl += period_pnl
            
            data.append({
                "period": row["period"],
                "period_pnl": period_pnl,
                "cumulative_pnl": cumulative_pnl,
                "trade_count": row["trade_count"]
            })
        
        return {
            "interval": interval,
            "data": data,
            "total_pnl": cumulative_pnl,
            "currency": "USDT"
        }


# Singleton instance
_trade_stats_service_instance = None

def get_trade_stats_service() -> TradeStatsService:
    """Get singleton trade stats service instance"""
    global _trade_stats_service_instance
    if _trade_stats_service_instance is None:
        _trade_stats_service_instance = TradeStatsService(DB())
    return _trade_stats_service_instance
