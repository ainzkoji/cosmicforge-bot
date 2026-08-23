"""
Drawdown Calculator - Equity Curve Analysis and Drawdown Metrics

Calculates drawdown metrics from equity snapshots (broker truth):
- Maximum drawdown
- Current drawdown from peak
- Drawdown periods with recovery times
- Equity curve for charting
"""
from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime, timezone, timedelta
from shared_lib.persistence.db import DB
import logging

logger = logging.getLogger(__name__)


class DrawdownService:
    """Calculate drawdown metrics from equity snapshots"""
    
    def __init__(self, db: DB):
        self.db = db
    
    def get_equity_curve(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get equity curve time series.
        
        Returns:
            {
                "data": List[dict],
                "start_equity": float,
                "end_equity": float,
                "peak_equity": float,
                "low_equity": float,
                "currency": str
            }
        """
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        where_clauses = ["user_id = ?", "timestamp_utc >= ?"]
        params = [user_id, start_date.isoformat()]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        if bot_instance_id:
            where_clauses.append("bot_instance_id = ?")
            params.append(bot_instance_id)
        
        where_sql = " AND ".join(where_clauses)
        
        if days <= 7:
            # Raw data (no grouping)
            group_sql = ""
            select_sql = """
                timestamp_utc,
                equity,
                unrealized_pnl,
                broker_account_id,
                broker_id
            """
        elif days <= 90:
            # Hourly grouping
            group_sql = "GROUP BY strftime('%Y-%m-%d %H', timestamp_utc)"
            select_sql = """
                MAX(timestamp_utc) as timestamp_utc,
                equity,
                unrealized_pnl,
                broker_account_id,
                broker_id
            """
        else:
            # Daily grouping
            group_sql = "GROUP BY strftime('%Y-%m-%d', timestamp_utc)"
            select_sql = """
                MAX(timestamp_utc) as timestamp_utc,
                equity,
                unrealized_pnl,
                broker_account_id,
                broker_id
            """
        
        with self.db.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT 
                    {select_sql}
                FROM equity_snapshots
                WHERE {where_sql}
                {group_sql}
                ORDER BY timestamp_utc ASC
                """,
                params
            ).fetchall()
        
        if not rows:
            return {
                "data": [],
                "start_equity": 0.0,
                "end_equity": 0.0,
                "peak_equity": 0.0,
                "low_equity": 0.0,
                "currency": "USDT"
            }
        
        data = []
        peak = 0.0
        low = float('inf')
        
        for row in rows:
            equity = float(row["equity"] or 0.0)
            if equity > peak:
                peak = equity
            if equity < low:
                low = equity
            
            data.append({
                "timestamp": row["timestamp_utc"],
                "equity": equity,
                "unrealized_pnl": float(row["unrealized_pnl"] or 0.0),
                "broker_account_id": row["broker_account_id"],
                "broker_id": row["broker_id"]
            })
        
        return {
            "data": data,
            "start_equity": float(data[0]["equity"] if data else 0.0),
            "end_equity": float(data[-1]["equity"] if data else 0.0),
            "peak_equity": peak,
            "low_equity": low if low != float('inf') else 0.0,
            "currency": "USDT"
        }
    
    def get_max_drawdown(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        bot_instance_id: Optional[str] = None,
        days: int = 90
    ) -> Dict[str, Any]:
        """
        Calculate maximum drawdown percentage over period.
        
        Returns:
            {
                "max_drawdown_pct": float,
                "max_drawdown_value": float,
                "peak_equity": float,
                "trough_equity": float,
                "peak_timestamp": str,
                "trough_timestamp": str,
                "recovery_timestamp": str | None,
                "recovery_days": int | None,
                "currency": str
            }
        """
        equity_curve = self.get_equity_curve(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            days=days
        )
        
        if not equity_curve["data"]:
            return {
                "max_drawdown_pct": 0.0,
                "max_drawdown_value": 0.0,
                "peak_equity": 0.0,
                "trough_equity": 0.0,
                "peak_timestamp": None,
                "trough_timestamp": None,
                "recovery_timestamp": None,
                "recovery_days": None,
                "currency": "USDT"
            }
        
        data = equity_curve["data"]
        
        max_dd_pct = 0.0
        max_dd_value = 0.0
        peak = 0.0
        peak_ts = None
        trough = 0.0
        trough_ts = None
        recovery_ts = None
        
        current_peak = 0.0
        current_peak_ts = None
        in_drawdown = False
        
        for point in data:
            equity = point["equity"]
            timestamp = point["timestamp"]
            
            if equity > current_peak:
                current_peak = equity
                current_peak_ts = timestamp
                in_drawdown = False
            else:
                drawdown_value = current_peak - equity
                drawdown_pct = (drawdown_value / current_peak * 100) if current_peak > 0 else 0.0
                
                if drawdown_pct > max_dd_pct:
                    max_dd_pct = drawdown_pct
                    max_dd_value = drawdown_value
                    peak = current_peak
                    peak_ts = current_peak_ts
                    trough = equity
                    trough_ts = timestamp
                    in_drawdown = True
                    recovery_ts = None
                elif in_drawdown and equity >= peak:
                    recovery_ts = timestamp
                    in_drawdown = False
        
        recovery_days = None
        if peak_ts and recovery_ts:
            try:
                peak_dt = datetime.fromisoformat(peak_ts.replace('Z', '+00:00'))
                recovery_dt = datetime.fromisoformat(recovery_ts.replace('Z', '+00:00'))
                recovery_days = (recovery_dt - peak_dt).days
            except:
                pass
        
        return {
            "max_drawdown_pct": max_dd_pct,
            "max_drawdown_value": max_dd_value,
            "peak_equity": peak,
            "trough_equity": trough,
            "peak_timestamp": peak_ts,
            "trough_timestamp": trough_ts,
            "recovery_timestamp": recovery_ts,
            "recovery_days": recovery_days,
            "currency": "USDT"
        }
    
    def get_current_drawdown(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Calculate current drawdown from all-time peak.
        
        Returns:
            {
                "current_drawdown_pct": float,
                "current_drawdown_value": float,
                "current_equity": float,
                "peak_equity": float,
                "peak_timestamp": str,
                "days_in_drawdown": int,
                "currency": str
            }
        """
        where_clauses = ["user_id = ?"]
        params = [user_id]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            # Get peak equity
            peak_row = conn.execute(
                f"""
                SELECT MAX(equity) as peak_equity, timestamp_utc
                FROM equity_snapshots
                WHERE {where_sql}
                GROUP BY timestamp_utc
                ORDER BY equity DESC
                LIMIT 1
                """,
                params
            ).fetchone()
            
            # Get latest equity
            latest_row = conn.execute(
                f"""
                SELECT equity, timestamp_utc
                FROM equity_snapshots
                WHERE {where_sql}
                ORDER BY timestamp_utc DESC
                LIMIT 1
                """,
                params
            ).fetchone()
        
        if not peak_row or not latest_row:
            return {
                "current_drawdown_pct": 0.0,
                "current_drawdown_value": 0.0,
                "current_equity": 0.0,
                "peak_equity": 0.0,
                "peak_timestamp": None,
                "days_in_drawdown": 0,
                "currency": "USDT"
            }
        
        peak = float(peak_row["peak_equity"] or 0.0)
        current = float(latest_row["equity"] or 0.0)
        dd_value = peak - current
        dd_pct = (dd_value / peak * 100) if peak > 0 else 0.0
        
        days_in_dd = 0
        if peak_row["timestamp_utc"] and latest_row["timestamp_utc"]:
            try:
                peak_dt = datetime.fromisoformat(peak_row["timestamp_utc"].replace('Z', '+00:00'))
                current_dt = datetime.fromisoformat(latest_row["timestamp_utc"].replace('Z', '+00:00'))
                days_in_dd = (current_dt - peak_dt).days
            except:
                pass
        
        return {
            "current_drawdown_pct": dd_pct,
            "current_drawdown_value": dd_value,
            "current_equity": current,
            "peak_equity": peak,
            "peak_timestamp": peak_row["timestamp_utc"],
            "days_in_drawdown": days_in_dd,
            "currency": "USDT"
        }
    
    def get_drawdown_periods(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        days: int = 90
    ) -> Dict[str, Any]:
        """
        Get all drawdown periods with recovery times.
        
        Returns:
            {
                "periods": List[dict],
                "total_periods": int,
                "avg_recovery_days": float
            }
        """
        equity_curve = self.get_equity_curve(
            user_id=user_id,
            broker_account_id=broker_account_id,
            days=days
        )
        
        if not equity_curve["data"]:
            return {
                "periods": [],
                "total_periods": 0,
                "avg_recovery_days": 0.0
            }
        
        data = equity_curve["data"]
        periods = []
        
        current_peak = 0.0
        current_peak_ts = None
        in_drawdown = False
        dd_start_ts = None
        max_dd_in_period = 0.0
        
        for point in data:
            equity = point["equity"]
            timestamp = point["timestamp"]
            
            if equity > current_peak:
                # New peak
                if in_drawdown:
                    # Recovered - close period
                    periods.append({
                        "peak_equity": current_peak,
                        "peak_timestamp": current_peak_ts,
                        "trough_timestamp": dd_start_ts,
                        "recovery_timestamp": timestamp,
                        "max_drawdown_pct": max_dd_in_period,
                        "recovered": True
                    })
                    in_drawdown = False
                
                current_peak = equity
                current_peak_ts = timestamp
                max_dd_in_period = 0.0
            else:
                # In drawdown
                dd_pct = ((current_peak - equity) / current_peak * 100) if current_peak > 0 else 0.0
                
                if not in_drawdown and dd_pct > 0:
                    in_drawdown = True
                    dd_start_ts = timestamp
                
                if dd_pct > max_dd_in_period:
                    max_dd_in_period = dd_pct
        
        # If still in drawdown at end
        if in_drawdown:
            periods.append({
                "peak_equity": current_peak,
                "peak_timestamp": current_peak_ts,
                "trough_timestamp": dd_start_ts,
                "recovery_timestamp": None,
                "max_drawdown_pct": max_dd_in_period,
                "recovered": False
            })
        
        # Calculate recovery days
        recovery_days = []
        for period in periods:
            if period["recovered"] and period["peak_timestamp"] and period["recovery_timestamp"]:
                try:
                    peak_dt = datetime.fromisoformat(period["peak_timestamp"].replace('Z', '+00:00'))
                    recovery_dt = datetime.fromisoformat(period["recovery_timestamp"].replace('Z', '+00:00'))
                    days = (recovery_dt - peak_dt).days
                    period["recovery_days"] = days
                    recovery_days.append(days)
                except:
                    period["recovery_days"] = None
        
        avg_recovery = sum(recovery_days) / len(recovery_days) if recovery_days else 0.0
        
        return {
            "periods": periods,
            "total_periods": len(periods),
            "avg_recovery_days": avg_recovery
        }


# Singleton instance
_drawdown_service_instance = None

def get_drawdown_service() -> DrawdownService:
    """Get singleton drawdown service instance"""
    global _drawdown_service_instance
    if _drawdown_service_instance is None:
        _drawdown_service_instance = DrawdownService(DB())
    return _drawdown_service_instance
