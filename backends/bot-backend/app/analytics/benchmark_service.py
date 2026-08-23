"""
Benchmarking Service - Compare Bot Performance Against Market Benchmarks

Compare equity curve performance against:
- Crypto benchmarks (BTC, ETH)
- Future: FOREX indices (DXY, EUR/USD)
- Future: Stock indices (SPY, QQQ)
"""
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone, timedelta
from shared_lib.persistence.db import DB
import logging
import math

logger = logging.getLogger(__name__)


class BenchmarkService:
    """Compare bot performance against market benchmarks"""
    
    def __init__(self, db: DB):
        self.db = db
        self.available_benchmarks = {
            "BTCUSDT": {"name": "Bitcoin", "category": "crypto"},
            "ETHUSDT": {"name": "Ethereum", "category": "crypto"},
            # Future: "DXY": {"name": "US Dollar Index", "category": "forex"},
            # Future: "EURUSD": {"name": "EUR/USD", "category": "forex"}
        }
    
    def get_available_benchmarks(self) -> List[Dict[str, Any]]:
        """
        Get list of supported benchmarks.
        
        Returns:
            List of {symbol, name, category}
        """
        return [
            {"symbol": symbol, **details}
            for symbol, details in self.available_benchmarks.items()
        ]
    
    async def update_benchmark_prices(self):
        """
        Fetch and update benchmark prices (daily close) for supported assets.
        Triggered by scheduled job (e.g., daily at 00:05 UTC).
        """
        import httpx
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            for symbol, details in self.available_benchmarks.items():
                if details["category"] == "crypto":
                    try:
                        # Use Binance Public API for crypto benchmarks as a reliable source
                        # GET /api/v3/klines?symbol=BTCUSDT&interval=1d&limit=30
                        # [timestamp, open, high, low, close, volume, ...]
                        response = await client.get(
                            "https://api.binance.com/api/v3/klines",
                            params={
                                "symbol": symbol,
                                "interval": "1d",
                                "limit": 90  # Update last 90 days to fill gaps
                            }
                        )
                        
                        if response.status_code == 200:
                            klines = response.json()
                            self._store_benchmark_prices(symbol, klines, details)
                            logger.info(f"Updated benchmark prices for {symbol}")
                        else:
                            logger.error(f"Failed to fetch benchmark {symbol}: {response.status_code}")
                            
                    except Exception as e:
                        logger.error(f"Error updating benchmark {symbol}: {e}")
    
    def _store_benchmark_prices(self, symbol: str, klines: list, details: dict):
        """Store kline data into benchmark_prices table"""
        upsert_data = []
        for k in klines:
            # Binance TS is ms, divide by 1000
            ts = datetime.fromtimestamp(k[0]/1000, tz=timezone.utc)
            date_str = ts.strftime('%Y-%m-%d')
            close_price = float(k[4])
            
            upsert_data.append((
                symbol,
                date_str,
                close_price,
                "USD",
                details.get("category"),
                "binance",
                datetime.now(timezone.utc).isoformat()
            ))
        
        with self.db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO benchmark_prices (symbol, date, close_price, quote_currency, asset_class, provider, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    close_price=excluded.close_price,
                    updated_at=excluded.updated_at
                """,
                upsert_data
            )

    def get_benchmark_comparison(
        self,
        user_id: str,
        benchmark_symbol: str = "BTCUSDT",
        broker_account_id: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Compare user's equity curve against benchmark.
        Uses benchmark_prices table with trade_fills fallback.
        """
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        # Get user's equity curve
        where_clauses = ["user_id = ?", "timestamp_utc >= ?"]
        params = [user_id, start_date.isoformat()]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            # Bot equity snapshots
            equity_rows = conn.execute(
                f"""
                SELECT timestamp_utc, equity
                FROM equity_snapshots
                WHERE {where_sql}
                ORDER BY timestamp_utc ASC
                """,
                params
            ).fetchall()
            
            # 1. Try fetching from benchmark_prices
            benchmark_rows = conn.execute(
                """
                SELECT date, close_price as price
                FROM benchmark_prices
                WHERE symbol = ? AND date >= ?
                ORDER BY date ASC
                """,
                [benchmark_symbol, start_date_str]
            ).fetchall()
            
            # 2. Fallback to trade_fills if missing
            if not benchmark_rows:
                logger.warning(f"Missing benchmark data for {benchmark_symbol}, using user trades proxy.")
                benchmark_rows = conn.execute(
                    """
                    SELECT timestamp_utc, price
                    FROM trade_fills
                    WHERE user_id = ? AND symbol = ? AND timestamp_utc >= ?
                    ORDER BY timestamp_utc ASC
                    """,
                    [user_id, benchmark_symbol, start_date.isoformat()]
                ).fetchall()
        
        if not equity_rows or len(equity_rows) < 2:
            return {
                "benchmark_symbol": benchmark_symbol,
                "bot_return_pct": 0.0,
                "benchmark_return_pct": 0.0,
                "outperformance_pct": 0.0,
                "correlation": 0.0,
                "sharpe_ratio": 0.0,
                "period_days": days,
                "warning": "Insufficient equity data for comparison"
            }
        
        # Calculate bot returns
        start_equity = float(equity_rows[0]["equity"] or 0.0)
        end_equity = float(equity_rows[-1]["equity"] or 0.0)
        bot_return_pct = ((end_equity - start_equity) / start_equity * 100) if start_equity > 0 else 0.0
        
        # Calculate benchmark returns
        benchmark_return_pct = 0.0
        warning = None
        
        if benchmark_rows and len(benchmark_rows) >= 2:
            # Handle different wrappers (sqlite3.Row)
            start_price = float(benchmark_rows[0][1]) # price
            end_price = float(benchmark_rows[-1][1])
            benchmark_return_pct = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0.0
        else:
            warning = f"No data for {benchmark_symbol}. Returns set to 0."
        
        # Calculate stats
        correlation = self.get_correlation(user_id, benchmark_symbol, broker_account_id, days)["value"]
        sharpe_ratio = self.get_sharpe_ratio(user_id, broker_account_id, days)["value"]
        
        return {
            "benchmark_symbol": benchmark_symbol,
            "bot_return_pct": bot_return_pct,
            "benchmark_return_pct": benchmark_return_pct,
            "outperformance_pct": bot_return_pct - benchmark_return_pct,
            "correlation": correlation,
            "sharpe_ratio": sharpe_ratio,
            "period_days": days,
            "warning": warning
        }
    
    def get_sharpe_ratio(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        days: int = 30,
        risk_free_rate: float = 0.0
    ) -> Dict[str, Any]:
        """
        Calculate Sharpe ratio (risk-adjusted return) using daily return resolution.
        Sharpe = (Return - Risk-Free Rate) / StdDev of Returns
        """
        daily_data = self.get_daily_returns(user_id, broker_account_id, days)
        returns_list = daily_data.get("returns", [])
        returns = [r["daily_return_pct"] / 100.0 for r in returns_list]
        
        sample_size = len(returns)
        
        base_result = {
            "metric_name": "sharpe_ratio",
            "value": 0.0,
            "calculation_basis": "daily_returns",
            "formula_description": "(Return - Risk-Free Rate) / StdDev of Returns",
            "source_tables": ["equity_snapshots"],
            "source_module": "benchmark_service",
            "lookback_window": days,
            "annualization_factor": 365.0,
            "sample_size": sample_size,
            "data_quality_flag": "insufficient_data" if sample_size < 2 else "ok",
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "annualized_return_pct": 0.0,
                "annualized_volatility_pct": 0.0,
                "risk_free_rate_assumed": risk_free_rate
            }
        }
        
        if sample_size < 2:
            return base_result
            
        avg_return = sum(returns) / sample_size
        variance = sum((r - avg_return) ** 2 for r in returns) / sample_size
        std_dev = math.sqrt(variance)
        
        annualized_return_pct = avg_return * 365 * 100
        annualized_volatility_pct = std_dev * math.sqrt(365) * 100
        
        sharpe = 0.0
        if std_dev > 0:
            sharpe = (avg_return - (risk_free_rate / 365)) / std_dev * math.sqrt(365)
            
        base_result["value"] = sharpe
        base_result["metadata"]["annualized_return_pct"] = annualized_return_pct
        base_result["metadata"]["annualized_volatility_pct"] = annualized_volatility_pct
        
        return base_result
    
    def get_sortino_ratio(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        days: int = 30,
        risk_free_rate: float = 0.0,
        target_return: float = 0.0
    ) -> Dict[str, Any]:
        """
        Calculate Sortino ratio (risk-adjusted return relative to downside risk).
        Sortino = (Return - Risk-Free Rate) / Downside Deviation
        """
        daily_data = self.get_daily_returns(user_id, broker_account_id, days)
        returns_list = daily_data.get("returns", [])
        returns = [r["daily_return_pct"] / 100.0 for r in returns_list]
        
        sample_size = len(returns)
        
        base_result = {
            "metric_name": "sortino_ratio",
            "value": 0.0,
            "calculation_basis": "daily_returns",
            "formula_description": "(Return - Risk-Free Rate) / Downside Deviation",
            "source_tables": ["equity_snapshots"],
            "source_module": "benchmark_service",
            "lookback_window": days,
            "annualization_factor": 365.0,
            "sample_size": sample_size,
            "data_quality_flag": "insufficient_data" if sample_size < 2 else "ok",
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "annualized_return_pct": 0.0,
                "downside_deviation_used": 0.0,
                "risk_free_rate_assumed": risk_free_rate,
                "target_return_assumed": target_return
            }
        }
        
        if sample_size < 2:
            return base_result
            
        avg_return = sum(returns) / sample_size
        annualized_return_pct = avg_return * 365 * 100
        
        daily_target = target_return / 365.0
        downside_diffs = [min(0.0, r - daily_target) ** 2 for r in returns]
        downside_variance = sum(downside_diffs) / sample_size if downside_diffs else 0.0
        downside_dev = math.sqrt(downside_variance)
        annualized_downside_deviation_pct = downside_dev * math.sqrt(365) * 100
        
        sortino = 0.0
        if downside_dev > 0:
            sortino = (avg_return - (risk_free_rate / 365.0)) / downside_dev * math.sqrt(365)
            
        base_result["value"] = sortino
        base_result["metadata"]["annualized_return_pct"] = annualized_return_pct
        base_result["metadata"]["downside_deviation_used"] = annualized_downside_deviation_pct
        
        return base_result
    
    def get_max_drawdown(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Calculate maximum drawdown directly from equity_snapshots.
        Returns the absolute deepest percentage drop from a peak to a trough.
        """
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        where_clauses = ["user_id = ?", "timestamp_utc >= ?"]
        params = [user_id, start_date.isoformat()]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
            
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT equity, timestamp_utc
                FROM equity_snapshots
                WHERE {where_sql}
                ORDER BY timestamp_utc ASC
                """,
                params
            ).fetchall()
            
        sample_size = len(rows)
        
        base_result = {
            "metric_name": "max_drawdown",
            "value": 0.0,
            "calculation_basis": "snapshot_history",
            "formula_description": "Deepest absolute peak-to-trough percentage drop",
            "source_tables": ["equity_snapshots"],
            "source_module": "benchmark_service",
            "lookback_window": days,
            "annualization_factor": None,
            "sample_size": sample_size,
            "data_quality_flag": "insufficient_data" if sample_size < 2 else "ok",
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "peak_timestamp": None,
                "trough_timestamp": None,
                "recovery_timestamp": None,
                "duration_seconds": None,
                "current_drawdown_from_peak_percent": 0.0
            }
        }
        
        if sample_size < 2:
            return base_result
            
        def _parse_iso(iso_str: str) -> datetime:
            if iso_str.endswith('Z'):
                iso_str = iso_str.replace('Z', '+00:00')
            return datetime.fromisoformat(iso_str)
            
        max_dd_pct = 0.0
        
        current_peak = float(rows[0]["equity"] or 0.0)
        current_peak_time = rows[0]["timestamp_utc"]
        
        global_peak = current_peak
        
        max_dd_peak = current_peak
        max_dd_peak_time = current_peak_time
        max_dd_trough = current_peak
        max_dd_trough_time = current_peak_time
        max_dd_recovery_time = None
        
        last_equity = current_peak
        
        for row in rows:
            equity = float(row["equity"] or 0.0)
            t_utc = row["timestamp_utc"]
            last_equity = equity
            
            if equity > current_peak:
                # If we're recovering the active max_dd right now
                if max_dd_pct > 0 and current_peak == max_dd_peak and max_dd_recovery_time is None and equity >= max_dd_peak:
                    max_dd_recovery_time = t_utc
                    
                current_peak = equity
                current_peak_time = t_utc
                
                if equity > global_peak:
                    global_peak = equity
            else:
                if current_peak > 0:
                    dd_pct = ((current_peak - equity) / current_peak) * 100
                    if dd_pct > max_dd_pct:
                        max_dd_pct = dd_pct
                        max_dd_peak = current_peak
                        max_dd_peak_time = current_peak_time
                        max_dd_trough = equity
                        max_dd_trough_time = t_utc
                        max_dd_recovery_time = None  # Reset recovery since trough deepened
                        
        current_dd_pct = 0.0
        if global_peak > 0 and last_equity < global_peak:
            current_dd_pct = ((global_peak - last_equity) / global_peak) * 100
            
        duration_sec = None
        if max_dd_peak_time and max_dd_trough_time and max_dd_peak_time != max_dd_trough_time:
            t1 = _parse_iso(max_dd_peak_time)
            t2 = _parse_iso(max_dd_trough_time)
            duration_sec = (t2 - t1).total_seconds()
            
        base_result["value"] = max_dd_pct
        base_result["metadata"]["peak_timestamp"] = max_dd_peak_time
        base_result["metadata"]["trough_timestamp"] = max_dd_trough_time
        base_result["metadata"]["recovery_timestamp"] = max_dd_recovery_time
        base_result["metadata"]["duration_seconds"] = duration_sec
        base_result["metadata"]["current_drawdown_from_peak_percent"] = current_dd_pct
        
        return base_result
        
    def get_daily_returns(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Calculate canonical daily returns using absolute start-of-day vs end-of-day equity snapshots.
        
        Specifications:
        - Source: `equity_snapshots`, strictly grouping by canonical `DATE(timestamp_utc)`.
        - Frequency: Daily.
        - Missing Points Handling: Deterministically handled by linking the nearest chronological 
          bounds within existing days. Outright gap days will connect their adjacent endpoints effectively simulating zero-change holding periods if no equity updates happen, although matching bounds filters explicitly executed bounds.
        - Realized vs MTM: Fully Mark-to-Market (MTM) based on absolute total equity summation.
        """
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        where_clauses = ["user_id = ?", "timestamp_utc >= ?"]
        params = [user_id, start_date.isoformat()]
        
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
        
        where_sql = " AND ".join(where_clauses)
        
        with self.db.get_connection() as conn:
            # Match bounds per day
            rows = conn.execute(
                f"""
                WITH daily_bounds AS (
                    SELECT 
                        DATE(timestamp_utc) as play_date,
                        MIN(timestamp_utc) as start_time,
                        MAX(timestamp_utc) as end_time
                    FROM equity_snapshots
                    WHERE {where_sql}
                    GROUP BY DATE(timestamp_utc)
                )
                SELECT 
                    db.play_date,
                    e_start.equity as start_equity,
                    e_end.equity as end_equity
                FROM daily_bounds db
                JOIN equity_snapshots e_start ON e_start.timestamp_utc = db.start_time AND e_start.user_id = ?
                JOIN equity_snapshots e_end ON e_end.timestamp_utc = db.end_time AND e_end.user_id = ?
                ORDER BY db.play_date ASC
                """,
                params + [user_id, user_id]
            ).fetchall()
            
        returns_list = []
        cumulative_pct = 0.0
        
        for row in rows:
            start_eq = float(row["start_equity"] or 0.0)
            end_eq = float(row["end_equity"] or 0.0)
            
            daily_pct = 0.0
            if start_eq > 0:
                daily_pct = ((end_eq - start_eq) / start_eq) * 100
            
            cumulative_pct += daily_pct
            
            returns_list.append({
                "date": row["play_date"],
                "start_equity": start_eq,
                "end_equity": end_eq,
                "daily_return_pct": daily_pct
            })
            
        return {
            "returns": returns_list,
            "total_return_pct": cumulative_pct,
            "period_days": days
        }

    def get_rolling_metrics(
        self,
        user_id: str,
        broker_account_id: Optional[str] = None,
        days: int = 365,
        risk_free_rate: float = 0.0,
        target_return: float = 0.0
    ) -> Dict[str, Any]:
        """
        Calculate rolling 30-day and 90-day advanced analytics (Sharpe, Sortino, Max Drawdown)
        over the specified historical boundary.
        
        Safety Constraint: Generates the rolling window derived from canonical daily
        returns sequentially in memory. For production environments, scaling past 
        multi-decade bounds may require a materialized view in the database to prevent locking.
        """
        daily_data = self.get_daily_returns(user_id, broker_account_id, days)
        returns_list = daily_data.get("returns", [])
        
        if not returns_list:
            return {"rolling_30d": [], "rolling_90d": [], "period_days": days}
            
        def calculate_window_metrics(window_data: List[Dict], safe_rfr: float, safe_tgt: float) -> Dict[str, float]:
            if not window_data:
                return {"sharpe": 0.0, "sortino": 0.0, "max_drawdown_percent": 0.0}
                
            n = len(window_data)
            rets = [r["daily_return_pct"] / 100.0 for r in window_data]
            avg_return = sum(rets) / n
            
            variance = sum((r - avg_return)**2 for r in rets) / n
            std_dev = math.sqrt(variance)
            
            daily_target = safe_tgt / 365.0
            downside_diffs = [min(0.0, r - daily_target)**2 for r in rets]
            downside_variance = sum(downside_diffs) / n if downside_diffs else 0.0
            downside_dev = math.sqrt(downside_variance)
            
            sharpe = 0.0
            if std_dev > 0:
                sharpe = (avg_return - (safe_rfr / 365.0)) / std_dev * math.sqrt(365)
                
            sortino = 0.0
            if downside_dev > 0:
                sortino = (avg_return - (safe_rfr / 365.0)) / downside_dev * math.sqrt(365)
                
            max_dd = 0.0
            peak = window_data[0]["end_equity"]
            for row in window_data:
                eq = row["end_equity"]
                if eq > peak:
                    peak = eq
                else:
                    if peak > 0:
                        dd = ((peak - eq) / peak) * 100
                        if dd > max_dd:
                            max_dd = dd
                            
            return {
                "sharpe": sharpe,
                "sortino": sortino,
                "max_drawdown_percent": max_dd
            }
            
        results_30d = []
        results_90d = []
        
        total_len = len(returns_list)
        for i in range(total_len):
            current_date = returns_list[i]["date"]
            
            if i >= 29:
                window_30 = returns_list[i-29 : i+1]
                metrics = calculate_window_metrics(window_30, risk_free_rate, target_return)
                results_30d.append({
                    "date": current_date,
                    **metrics
                })
                
            if i >= 89:
                window_90 = returns_list[i-89 : i+1]
                metrics = calculate_window_metrics(window_90, risk_free_rate, target_return)
                results_90d.append({
                    "date": current_date,
                    **metrics
                })
                
        return {
            "rolling_30d": results_30d,
            "rolling_90d": results_90d,
            "period_days": days,
            "last_updated_utc": datetime.now(timezone.utc).isoformat()
        }

    def get_correlation(
        self,
        user_id: str,
        benchmark_symbol: str = "BTCUSDT",
        broker_account_id: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Calculate Pearson correlation coefficient between bot returns and benchmark returns.
        Matches chronological daily return bounds.
        """
        bot_daily = self.get_daily_returns(user_id, broker_account_id, days)
        returns_list = bot_daily.get("returns", [])
        
        base_result = {
            "metric_name": "correlation",
            "value": 0.0,
            "calculation_basis": "daily_returns",
            "formula_description": "Covariance(Bot, Benchmark) / (StdDev(Bot) * StdDev(Benchmark))",
            "source_tables": ["equity_snapshots", "benchmark_prices"],
            "source_module": "benchmark_service",
            "lookback_window": days,
            "annualization_factor": None,
            "sample_size": 0,
            "data_quality_flag": "ok",
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "benchmark_symbol": benchmark_symbol
            }
        }
        
        if len(returns_list) < 2:
            base_result["data_quality_flag"] = "insufficient_bot_data"
            return base_result
            
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        with self.db.get_connection() as conn:
            benchmark_rows = conn.execute(
                """
                SELECT date, close_price as price
                FROM benchmark_prices
                WHERE symbol = ? AND date >= ?
                ORDER BY date ASC
                """,
                [benchmark_symbol, start_date_str]
            ).fetchall()
            
        if not benchmark_rows or len(benchmark_rows) < 2:
            base_result["data_quality_flag"] = "missing_benchmark_data"
            return base_result
            
        bench_rets = {}
        for i in range(1, len(benchmark_rows)):
            prev_price = float(benchmark_rows[i-1]["price"])
            curr_price = float(benchmark_rows[i]["price"])
            r = ((curr_price - prev_price) / prev_price) * 100 if prev_price > 0 else 0.0
            bench_rets[benchmark_rows[i]["date"]] = r
            
        aligned_bot = []
        aligned_bench = []
        
        for br in returns_list:
            d = br["date"]
            if d in bench_rets:
                aligned_bot.append(br["daily_return_pct"])
                aligned_bench.append(bench_rets[d])
                
        sample_size = len(aligned_bot)
        base_result["sample_size"] = sample_size
        
        if sample_size < 2:
            base_result["data_quality_flag"] = "insufficient_aligned_data"
            return base_result
            
        bot_mean = sum(aligned_bot) / sample_size
        bench_mean = sum(aligned_bench) / sample_size
        
        covar = sum((b - bot_mean) * (m - bench_mean) for b, m in zip(aligned_bot, aligned_bench)) / sample_size
        var_bot = sum((b - bot_mean)**2 for b in aligned_bot) / sample_size
        var_bench = sum((m - bench_mean)**2 for m in aligned_bench) / sample_size
        
        if var_bot == 0 or var_bench == 0:
            base_result["data_quality_flag"] = "zero_variance"
            base_result["metadata"]["aligned_bot"] = aligned_bot
            base_result["metadata"]["aligned_bench"] = aligned_bench
            return base_result
            
        corr = covar / math.sqrt(var_bot * var_bench)
        base_result["value"] = corr
        
        return base_result


# Singleton instance
_benchmark_service_instance = None

def get_benchmark_service() -> BenchmarkService:
    """Get singleton benchmark service instance"""
    global _benchmark_service_instance
    if _benchmark_service_instance is None:
        _benchmark_service_instance = BenchmarkService(DB())
    return _benchmark_service_instance
