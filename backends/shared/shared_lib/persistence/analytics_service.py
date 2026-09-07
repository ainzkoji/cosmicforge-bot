from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any
import sqlite3
import math
import logging

from shared_lib.persistence.db import DB
from shared_lib.persistence.trade_tracker import TradeStatus
from shared_lib.persistence.analytics_cache import cache_analytics

logger = logging.getLogger(__name__)

class AnalyticsService:
    def __init__(self, db: Optional[DB] = None):
        self.db = db or DB()
        self._ensure_tables()
    
    def _get_timeframe_date(self, timeframe: str) -> Optional[str]:
        """
        Convert timeframe string to ISO date string for filtering.
        
        **Timezone**: Uses UTC (datetime.utcnow()).
        
        **Supported Timeframes**:
        - "1M": 30 days ago from now
        - "3M": 90 days ago from now
        - "YTD": January 1 of current year
        - "ALL": None (no filter)
        
        Args:
            timeframe: One of "1M", "3M", "YTD", "ALL" (case-insensitive)
            
        Returns:
            ISO format datetime string (UTC) or None for ALL
            
        Raises:
            ValueError: If timeframe is not recognized
        """
        timeframe = timeframe.upper()
        now = datetime.utcnow()
        
        if timeframe == "1M":
            since = now - timedelta(days=30)
        elif timeframe == "3M":
            since = now - timedelta(days=90)
        elif timeframe == "YTD":
            # January 1 of current year at 00:00:00 UTC
            since = datetime(now.year, 1, 1)
        elif timeframe == "ALL":
            return None
        else:
            raise ValueError(
                f"Invalid timeframe '{timeframe}'. Must be one of: 1M, 3M, YTD, ALL"
            )
        
        return since.isoformat()
    
    def _ensure_tables(self):
        """Ensure required tables exist."""
        with self.db.connect() as conn:
            # Create trades table if it doesn't exist
            # This duplicates TradeTracker logic but ensures the service can work standalone
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    symbol TEXT,
                    side TEXT,
                    strategy TEXT,
                    mode TEXT,
                    timeframe TEXT,
                    entry_time TEXT,
                    entry_price REAL,
                    entry_qty REAL,
                    entry_confidence REAL,
                    exit_time TEXT,
                    exit_price REAL,
                    exit_reason TEXT,
                    realized_pnl REAL DEFAULT 0,
                    fees REAL DEFAULT 0,
                    r_multiple REAL,
                    initial_stop REAL,
                    tp1_hit INTEGER DEFAULT 0,
                    tp1_time TEXT,
                    add_count INTEGER DEFAULT 0,
                    status TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def get_monthly_pnl(self, user_id: str, timeframe: str = "ALL", months: int = 12) -> List[Dict[str, float]]:
        """
        Aggregate realized PnL by month with timeframe filtering.
        Returns list of dicts: [{"month": "Jan", "value": 150.0}, ...]
        
        Note: Currently assumes single-tenant DB (all trades belong to user_id).
        For multi-tenant, would need to join through runs -> bot_instances.
        """
        # Calculate date filter based on timeframe
        since_date = self._get_timeframe_date(timeframe)
        
        monthly_map = {}
        
        with self.db.connect() as conn:
            if since_date:
                cursor = conn.execute("""
                    SELECT strftime('%Y-%m', timestamp_utc) as m, SUM(realized_pnl) as val
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                      AND timestamp_utc IS NOT NULL
                      AND timestamp_utc >= ?
                    GROUP BY 1
                    ORDER BY 1 ASC
                """, (since_date,))
            else:
                cursor = conn.execute("""
                    SELECT strftime('%Y-%m', timestamp_utc) as m, SUM(realized_pnl) as val
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                      AND timestamp_utc IS NOT NULL
                    GROUP BY 1
                    ORDER BY 1 ASC
                    LIMIT ?
                """, (months,))
            
            rows = cursor.fetchall()
            for r in rows:
                if r["m"]:
                    monthly_map[r["m"]] = r["val"] or 0.0
        
        # Format output with readable month names
        result = []
        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        sorted_months = sorted(monthly_map.keys())
        for m in sorted_months:
            # Convert "2026-01" to "Jan" or "Jan 2026" if spanning years
            year, month_num = m.split("-")
            month_name = month_names[int(month_num) - 1]
            result.append({"month": month_name, "value": round(monthly_map[m], 2)})
            
        return result

    def get_asset_allocation(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Derive from current open positions notional value.
        Returns list of dicts: [{"symbol": "BTC", "value_usdt": 1000, "percent": 50.0}, ...]
        """
        allocations = []
        total_value = 0.0
        
        with self.db.connect() as conn:
            # Open trades: status = OPEN, TP1_HIT, RUNNER, etc.
            # We can just check status != 'CLOSED' or use NOT IN
            cursor = conn.execute("""
                SELECT symbol, SUM(entry_price * entry_qty) as notional
                FROM trades
                WHERE status != 'CLOSED'
                GROUP BY symbol
            """)
            
            rows = cursor.fetchall()
            for r in rows:
                symbol = r["symbol"]
                val = r["notional"] or 0.0
                allocations.append({"symbol": symbol, "value_usdt": val})
                total_value += val
        
        # Calculate percentages
        result = []
        for item in allocations:
            pct = (item["value_usdt"] / total_value * 100) if total_value > 0 else 0.0
            result.append({
                "label": item["symbol"],  # Backward compatibility - deprecated field
                "symbol": item["symbol"],
                "value_usdt": round(item["value_usdt"], 2),
                "value": round(item["value_usdt"], 2),  # Backward compatibility - deprecated field
                "percent": round(pct, 1),
                "color": "#888888"  # client can color map this
            })
            
        return result

    def get_risk_metrics(self, user_id: str, timeframe: str = "ALL", window_days: int = 30) -> Dict[str, float]:
        """
        Compute risk metrics: max_drawdown, volatility_30d, sortino_ratio, sharpe_ratio, alpha.
        """
        metrics = {
            "max_drawdown": 0.0,
            "volatility_30d": 0.0,
            "sortino_ratio": 0.0,
            "sharpe_ratio": 0.0,
            "alpha": 0.0
        }
        
        # Get timeframe date filter
        since_date = self._get_timeframe_date(timeframe)
        # Note: If since_date is None (ALL), we fetch all history.
        
        with self.db.connect() as conn:
            # Fetch daily PnL for volatility/Sortino/Sharpe
            if since_date:
                cursor = conn.execute("""
                    SELECT date(timestamp_utc) as d, SUM(realized_pnl) as daily_pnl
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                      AND timestamp_utc >= ?
                    GROUP BY 1
                    ORDER BY 1 ASC
                """, (since_date,))
            else:
                cursor = conn.execute("""
                    SELECT date(timestamp_utc) as d, SUM(realized_pnl) as daily_pnl
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                    GROUP BY 1
                    ORDER BY 1 ASC
                """)
            
            daily_pnls = [r["daily_pnl"] or 0.0 for r in cursor.fetchall()]
            
            # Fetch all trades for equity curve and max drawdown
            if since_date:
                cursor_all = conn.execute("""
                    SELECT realized_pnl
                    FROM trade_fills
                    WHERE action = 'CLOSE' AND timestamp_utc >= ?
                    ORDER BY timestamp_utc ASC
                """, (since_date,))
            else:
                cursor_all = conn.execute("""
                    SELECT realized_pnl
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                    ORDER BY timestamp_utc ASC
                """)
            all_pnls = [r["realized_pnl"] or 0.0 for r in cursor_all.fetchall()]

        # Calculate metrics
        if len(daily_pnls) > 1:
            try:
                mean_pnl = sum(daily_pnls) / len(daily_pnls)
                variance = sum((x - mean_pnl) ** 2 for x in daily_pnls) / (len(daily_pnls) - 1)
                std_dev = math.sqrt(variance)
                
                # 1. Volatility (annualized)
                # Annualize: std_dev * sqrt(252) for daily data
                metrics["volatility_30d"] = round(std_dev * math.sqrt(252), 2)
                
                # 2. Sharpe Ratio (annualized) - Delegated to canonical BenchmarkService
                try:
                    from app.analytics.benchmark_service import get_benchmark_service
                    days_lookback = 30 if timeframe == "1M" else (90 if timeframe == "3M" else 365)
                    sharpe_data = get_benchmark_service().get_sharpe_ratio(user_id=user_id, days=days_lookback)
                    metrics["sharpe_ratio"] = round(sharpe_data.get("sharpe_ratio", 0.0), 2)
                except Exception as e:
                    logger.warning(f"Failed to fetch canonical Sharpe ratio: {e}")
                
                # 3. Sortino Ratio (annualized) - Delegated to canonical BenchmarkService
                try:
                    from app.analytics.benchmark_service import get_benchmark_service
                    sortino_data = get_benchmark_service().get_sortino_ratio(user_id=user_id, days=days_lookback)
                    metrics["sortino_ratio"] = round(sortino_data.get("sortino_ratio", 0.0), 2)
                except Exception as e:
                    logger.warning(f"Failed to fetch canonical Sortino ratio: {e}")
            except Exception as e:
                logger.warning(f"Failed to calculate risk metrics: {e}. Returning defaults.")
        elif len(daily_pnls) == 0:
            logger.info(f"No trades found for timeframe {timeframe}. Returning zero metrics.")
        
        # 4. Max Drawdown (absolute USD)
        curr_equity = 0.0
        peak = 0.0
        max_dd = 0.0
        
        for pnl in all_pnls:
            curr_equity += pnl
            if curr_equity > peak:
                peak = curr_equity
            dd = peak - curr_equity
            if dd > max_dd:
                max_dd = dd
                
        metrics["max_drawdown"] = round(max_dd, 2)
        
        # 5. Alpha (simplified: excess return vs benchmark)
        # For now, return 0.0 as we don't have benchmark data
        # In production, would compare against BTC/market return
        metrics["alpha"] = 0.0
        
        return metrics

    @cache_analytics(ttl_seconds=300)
    def get_total_stats(self, user_id: str, timeframe: str = "ALL") -> Dict[str, Any]:
        """
        Get aggregate stats with timeframe filtering and profit change %.
        Returns: total_profit, total_trades, wins, losses, win_rate, profit_factor, 
                 sharpe_ratio, profit_change_pct
        """
        stats = {
            "total_profit": 0.0,
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "breakevens": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "sharpe_ratio": 0.0,
            "profit_change_pct": 0.0,
        }
        
        # Get date filter
        since_date = self._get_timeframe_date(timeframe)
        
        with self.db.connect() as conn:
                # Import TradeStatsService dynamically
                from app.analytics.trade_stats_service import get_trade_stats_service
                trade_stats_svc = get_trade_stats_service()
                
                start_dt = None
                if since_date:
                    start_dt = datetime.fromisoformat(since_date.replace('Z', '+00:00'))
                
                trade_summary = trade_stats_svc.get_trade_summary(user_id=user_id, start_date=start_dt)
                
                stats["total_trades"] = trade_summary["total_trades"]
                stats["total_profit"] = trade_summary["total_pnl"]
                stats["wins"] = trade_summary["winning_trades"]
                stats["losses"] = trade_summary["losing_trades"]
                stats["breakevens"] = trade_summary["total_trades"] - trade_summary["winning_trades"] - trade_summary["losing_trades"] if trade_summary["total_trades"] > 0 else 0
                stats["win_rate"] = trade_summary["win_rate"]
                stats["profit_factor"] = trade_summary["profit_factor"]
                
                # Profit change pct (compare to previous period)
                if start_dt and stats.get("total_profit", 0) != 0:
                    period_start = start_dt
                    period_end = datetime.now(timezone.utc)
                    period_length = (period_end - period_start).days
                    prev_period_start = period_start - timedelta(days=period_length)
                    
                    prev_row = conn.execute("""
                        SELECT SUM(realized_pnl) as prev_pnl
                        FROM trade_fills
                        WHERE action = 'CLOSE' 
                          AND timestamp_utc >= ? 
                          AND timestamp_utc < ?
                    """, (prev_period_start.isoformat(), since_date)).fetchone()
                    
                    prev_pnl = prev_row["prev_pnl"] if prev_row and prev_row["prev_pnl"] else 0.0
                    
                    if prev_pnl != 0:
                        stats["profit_change_pct"] = ((stats["total_profit"] - prev_pnl) / abs(prev_pnl)) * 100
        
        # Get Sharpe ratio from risk metrics
        risk_metrics = self.get_risk_metrics(user_id, timeframe)
        stats["sharpe_ratio"] = risk_metrics.get("sharpe_ratio", 0.0)
        
        return stats
    
    @cache_analytics(ttl_seconds=300)
    def get_equity_curve(self, user_id: str, timeframe: str = "ALL") -> List[Dict[str, Any]]:
        """
        Calculate equity curve from equity snapshots (primary) or trade history (fallback).
        Returns: [{"timestamp": "2026-01-15T10:30:00", "equity": 10150.0}, ...]
        """
        # Get date filter
        since_date = self._get_timeframe_date(timeframe)
        
        curve = []
        
        with self.db.connect() as conn:
            # 1. Try fetching from equity_snapshots
            query = "SELECT timestamp_utc, equity FROM equity_snapshots WHERE 1=1"
            params = []
            
            if since_date:
                query += " AND timestamp_utc >= ?"
                params.append(since_date)
                
            # Filter by user's bot instances (TODO: join with bot_instances table for strict user isolation)
            # For now, we return all or global if user_id is provided?
            # Ideally: query += " AND bot_instance_id IN (SELECT id FROM bot_instances WHERE user_id = ?)" 
            # But specific mapping might be tricky without duplicating logic.
            # Assuming single-user for now as per context (user_id used mainly for future)
            
            query += " ORDER BY timestamp_utc ASC"
            
            rows = conn.execute(query, tuple(params)).fetchall()
            
            if rows:
                for r in rows:
                    curve.append({
                        "timestamp": r["timestamp_utc"],
                        "equity": r["equity"]
                    })
                return curve
                
            # 2. Fallback: Reconstruct from trades (if no snapshots)
            logger.info("No equity snapshots found, reconstructing from trade history")
            
            if since_date:
                cursor = conn.execute("""
                    SELECT timestamp_utc, realized_pnl
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                      AND timestamp_utc IS NOT NULL
                      AND timestamp_utc >= ?
                    ORDER BY timestamp_utc ASC
                """, (since_date,))
            else:
                cursor = conn.execute("""
                    SELECT timestamp_utc, realized_pnl
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                      AND timestamp_utc IS NOT NULL
                    ORDER BY timestamp_utc ASC
                """)
            
            cumulative_pnl = 0.0
            for row in cursor.fetchall():
                cumulative_pnl += row["realized_pnl"] or 0.0
                curve.append({
                    "timestamp": row["timestamp_utc"],
                    "equity": round(cumulative_pnl, 2)
                })
        
        return curve

    def get_raw_trades(self, user_id: str, timeframe: str = "ALL") -> List[Dict[str, Any]]:
        """
        Fetch raw trade data for export.
        Returns list of trade dictionaries.
        """
        since_date = self._get_timeframe_date(timeframe)
        
        with self.db.connect() as conn:
            query = "SELECT * FROM trade_fills WHERE action = 'CLOSE'"
            params = []
            
            if since_date:
                query += " AND timestamp_utc >= ?"
                params.append(since_date)
                
            query += " ORDER BY timestamp_utc DESC"
            
            cursor = conn.execute(query, tuple(params))
            
            # Convert rows to dicts
            result = []
            for row in cursor.fetchall():
                # sqlite3.Row provides dict-like access but we want a real dict for pandas
                result.append(dict(row))
                
            return result
