from typing import List, Optional
from datetime import datetime

# Placeholder for SQL query builders.
# In Phase 2, this might abstract SQLAlchemy queries or raw SQL string generation.

class AnalyticsQueries:
    
    @staticmethod
    def aggregated_pnl_by_user(user_id: str, start_date: datetime, end_date: datetime) -> str:
        """
        Returns SQL to aggregate PnL from trade_fills.
        """
        return f"""
        SELECT 
            SUM(realized_pnl) as total_pnl,
            COUNT(*) as trade_count,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) as losses
        FROM trade_fills
        WHERE user_id = '{user_id}'
          AND timestamp_utc BETWEEN '{start_date}' AND '{end_date}'
          AND action = 'CLOSE'
        """

    @staticmethod
    def equity_curve_points(user_id: str, limit: int = 100) -> str:
        """
        Returns SQL for equity curve.
        """
        return f"""
        SELECT timestamp_utc, equity, currency
        FROM equity_snapshots
        WHERE user_id = '{user_id}'
        ORDER BY timestamp_utc DESC
        LIMIT {limit}
        """
