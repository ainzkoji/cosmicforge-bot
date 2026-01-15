"""
Global Analytics - D+ Cross-Run Analytics

Two layers:
1. Run Layer: immutable truth (events, trades, run_summary)
2. Global Layer: derived analytics (strategy health, calibration, regime stats)

Key features:
- Idempotent rollups via analytics_runs ledger
- Environment separation (LIVE/PAPER)
- Rebuild verification support
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from datetime import datetime
import json
import sqlite3


@dataclass
class StrategyPerformance:
    """Global strategy performance metrics."""
    strategy: str
    version: str
    symbol: str
    timeframe: str
    mode: str
    environment: str
    
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    breakevens: int = 0
    
    net_pnl: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    total_fees: float = 0.0
    
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    expectancy: float = 0.0
    
    avg_duration_minutes: float = 0.0
    
    last_updated: Optional[str] = None


@dataclass
class ConfidenceBucket:
    """Confidence calibration bucket."""
    strategy: str
    symbol: str
    timeframe: str
    mode: str
    environment: str
    bucket_low: float
    bucket_high: float
    
    count: int = 0
    wins: int = 0
    losses: int = 0
    breakevens: int = 0
    
    net_pnl: float = 0.0
    avg_pnl: float = 0.0
    win_rate: float = 0.0


class GlobalAnalytics:
    """
    Cross-run analytics with idempotent rollups.
    
    Key dimensions for all global aggregates:
    - environment (LIVE/PAPER/BACKTEST)
    - exchange (BINANCE_FUTURES, etc.)
    - account_id (user-defined stable identifier)
    """
    
    def __init__(self, db_path: str = "data/bot.db"):
        self._db_path = db_path
        self._init_tables()
        self._migrate_tables()  # Add missing columns to existing tables
    
    def _migrate_tables(self):
        """Add missing columns to existing tables for backward compatibility."""
        conn = sqlite3.connect(self._db_path)
        try:
            # Check and add columns to global_strategy_performance
            cursor = conn.execute("PRAGMA table_info(global_strategy_performance)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            if "exchange" not in existing_cols:
                conn.execute("ALTER TABLE global_strategy_performance ADD COLUMN exchange TEXT DEFAULT 'BINANCE_FUTURES'")
            if "account_id" not in existing_cols:
                conn.execute("ALTER TABLE global_strategy_performance ADD COLUMN account_id TEXT DEFAULT 'default'")
            
            # Check and add columns to global_confidence_buckets
            cursor = conn.execute("PRAGMA table_info(global_confidence_buckets)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            if "exchange" not in existing_cols:
                conn.execute("ALTER TABLE global_confidence_buckets ADD COLUMN exchange TEXT DEFAULT 'BINANCE_FUTURES'")
            if "account_id" not in existing_cols:
                conn.execute("ALTER TABLE global_confidence_buckets ADD COLUMN account_id TEXT DEFAULT 'default'")
            
            # Check and add columns to global_regime_performance
            cursor = conn.execute("PRAGMA table_info(global_regime_performance)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            if "exchange" not in existing_cols:
                conn.execute("ALTER TABLE global_regime_performance ADD COLUMN exchange TEXT DEFAULT 'BINANCE_FUTURES'")
            if "account_id" not in existing_cols:
                conn.execute("ALTER TABLE global_regime_performance ADD COLUMN account_id TEXT DEFAULT 'default'")
            
            # Check and add columns to global_risk_blocks
            cursor = conn.execute("PRAGMA table_info(global_risk_blocks)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            if "exchange" not in existing_cols:
                conn.execute("ALTER TABLE global_risk_blocks ADD COLUMN exchange TEXT DEFAULT 'BINANCE_FUTURES'")
            if "account_id" not in existing_cols:
                conn.execute("ALTER TABLE global_risk_blocks ADD COLUMN account_id TEXT DEFAULT 'default'")
            
            # Check and add columns to analytics_runs
            cursor = conn.execute("PRAGMA table_info(analytics_runs)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            if "exchange" not in existing_cols:
                conn.execute("ALTER TABLE analytics_runs ADD COLUMN exchange TEXT")
            if "account_id" not in existing_cols:
                conn.execute("ALTER TABLE analytics_runs ADD COLUMN account_id TEXT")
            if "environment" not in existing_cols:
                conn.execute("ALTER TABLE analytics_runs ADD COLUMN environment TEXT")
            
            conn.commit()
        except Exception:
            pass  # Columns may already exist
        finally:
            conn.close()
    
    def _init_tables(self):
        """Create global analytics tables with full key dimensions."""
        conn = sqlite3.connect(self._db_path)
        try:
            # Analytics processing ledger (idempotency)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics_runs (
                    run_id TEXT PRIMARY KEY,
                    environment TEXT,
                    exchange TEXT,
                    account_id TEXT,
                    processed_at TEXT NOT NULL,
                    analytics_version TEXT DEFAULT '1.0',
                    status TEXT NOT NULL,
                    error_message TEXT
                )
            """)
            
            # Global strategy performance - keyed by env/exchange/account/strategy
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_strategy_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    environment TEXT NOT NULL,
                    exchange TEXT NOT NULL DEFAULT 'BINANCE_FUTURES',
                    account_id TEXT NOT NULL DEFAULT 'default',
                    strategy TEXT NOT NULL,
                    version TEXT DEFAULT '1.0',
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    
                    total_trades INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    breakevens INTEGER DEFAULT 0,
                    
                    net_pnl REAL DEFAULT 0,
                    gross_profit REAL DEFAULT 0,
                    gross_loss REAL DEFAULT 0,
                    total_fees REAL DEFAULT 0,
                    
                    avg_win REAL DEFAULT 0,
                    avg_loss REAL DEFAULT 0,
                    profit_factor REAL DEFAULT 0,
                    expectancy REAL DEFAULT 0,
                    avg_duration_minutes REAL DEFAULT 0,
                    
                    last_updated TEXT,
                    
                    UNIQUE(environment, exchange, account_id, strategy, version, symbol, timeframe, mode)
                )
            """)
            
            # Global confidence buckets - keyed by env/exchange/account
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_confidence_buckets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    environment TEXT NOT NULL,
                    exchange TEXT NOT NULL DEFAULT 'BINANCE_FUTURES',
                    account_id TEXT NOT NULL DEFAULT 'default',
                    strategy TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    bucket_low REAL NOT NULL,
                    bucket_high REAL NOT NULL,
                    
                    count INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    breakevens INTEGER DEFAULT 0,
                    
                    net_pnl REAL DEFAULT 0,
                    avg_pnl REAL DEFAULT 0,
                    win_rate REAL DEFAULT 0,
                    
                    last_updated TEXT,
                    
                    UNIQUE(environment, exchange, account_id, strategy, symbol, timeframe, mode, bucket_low, bucket_high)
                )
            """)
            
            # Global regime performance - keyed by env/exchange/account
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_regime_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    environment TEXT NOT NULL,
                    exchange TEXT NOT NULL DEFAULT 'BINANCE_FUTURES',
                    account_id TEXT NOT NULL DEFAULT 'default',
                    regime TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    
                    total_trades INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    
                    net_pnl REAL DEFAULT 0,
                    avg_pnl REAL DEFAULT 0,
                    win_rate REAL DEFAULT 0,
                    
                    last_updated TEXT,
                    
                    UNIQUE(environment, exchange, account_id, regime, symbol, timeframe, mode)
                )
            """)
            
            # Global risk block stats - keyed by env/exchange/account
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_risk_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    environment TEXT NOT NULL,
                    exchange TEXT NOT NULL DEFAULT 'BINANCE_FUTURES',
                    account_id TEXT NOT NULL DEFAULT 'default',
                    reason_code TEXT NOT NULL,
                    symbol TEXT,
                    mode TEXT,
                    
                    blocks_count INTEGER DEFAULT 0,
                    last_updated TEXT,
                    
                    UNIQUE(environment, exchange, account_id, reason_code, symbol, mode)
                )
            """)
            
            conn.commit()
        finally:
            conn.close()
    
    # =========================================================================
    # IDEMPOTENT ROLLUP
    # =========================================================================
    
    def is_run_processed(self, run_id: str) -> bool:
        """Check if a run has already been processed."""
        conn = sqlite3.connect(self._db_path)
        try:
            cursor = conn.execute(
                "SELECT status FROM analytics_runs WHERE run_id = ?",
                (run_id,)
            )
            row = cursor.fetchone()
            return row is not None and row[0] == "SUCCESS"
        finally:
            conn.close()
    
    def mark_run_processed(
        self,
        run_id: str,
        status: str = "SUCCESS",
        error: str = None,
        environment: str = "PAPER",
        exchange: str = "BINANCE_FUTURES",
        account_id: str = "default",
    ):
        """Mark a run as processed in the ledger."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO analytics_runs 
                (run_id, environment, exchange, account_id, processed_at, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (run_id, environment, exchange, account_id, datetime.utcnow().isoformat(), status, error))
            conn.commit()
        finally:
            conn.close()
    
    def process_run(
        self,
        run_id: str,
        environment: str = "PAPER",
        exchange: str = "BINANCE_FUTURES",
        account_id: str = "default",
    ) -> bool:
        """
        Process a run into global analytics (idempotent).
        Returns True if processed, False if already done.
        """
        if self.is_run_processed(run_id):
            return False
        
        try:
            # Get trades from this run
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(
                    "SELECT * FROM trades WHERE run_id = ? AND status = 'CLOSED'",
                    (run_id,)
                )
                trades = [dict(row) for row in cursor.fetchall()]
            finally:
                conn.close()
            
            if not trades:
                self.mark_run_processed(run_id, "SUCCESS", None, environment, exchange, account_id)
                return True
            
            # Aggregate into global tables
            self._rollup_strategy_performance(trades, environment, exchange, account_id)
            self._rollup_confidence_buckets(trades, environment, exchange, account_id)
            
            self.mark_run_processed(run_id, "SUCCESS", None, environment, exchange, account_id)
            return True
            
        except Exception as e:
            self.mark_run_processed(run_id, "FAILED", str(e), environment, exchange, account_id)
            return False
    
    def _rollup_strategy_performance(self, trades: List[dict], environment: str, exchange: str, account_id: str):
        """Roll up trades into global strategy performance."""
        conn = sqlite3.connect(self._db_path)
        try:
            for trade in trades:
                strategy = trade.get("strategy", "unknown")
                symbol = trade.get("symbol", "unknown")
                timeframe = trade.get("timeframe", "15m")
                mode = trade.get("mode", "PRECISION")
                pnl = trade.get("realized_pnl", 0) or 0
                fees = trade.get("fees", 0) or 0
                
                is_win = pnl > 0
                is_loss = pnl < 0
                
                # Upsert into global table (keyed by env/exchange/account)
                conn.execute("""
                    INSERT INTO global_strategy_performance 
                    (environment, exchange, account_id, strategy, version, symbol, timeframe, mode,
                     total_trades, wins, losses, net_pnl, gross_profit, gross_loss, total_fees, last_updated)
                    VALUES (?, ?, ?, ?, '1.0', ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(environment, exchange, account_id, strategy, version, symbol, timeframe, mode) 
                    DO UPDATE SET
                        total_trades = total_trades + 1,
                        wins = wins + ?,
                        losses = losses + ?,
                        net_pnl = net_pnl + ?,
                        gross_profit = gross_profit + ?,
                        gross_loss = gross_loss + ?,
                        total_fees = total_fees + ?,
                        last_updated = ?
                """, (
                    environment, exchange, account_id,
                    strategy, symbol, timeframe, mode,
                    1 if is_win else 0,
                    1 if is_loss else 0,
                    pnl,
                    pnl if is_win else 0,
                    abs(pnl) if is_loss else 0,
                    fees,
                    datetime.utcnow().isoformat(),
                    # For UPDATE
                    1 if is_win else 0,
                    1 if is_loss else 0,
                    pnl,
                    pnl if is_win else 0,
                    abs(pnl) if is_loss else 0,
                    fees,
                    datetime.utcnow().isoformat(),
                ))
            
            conn.commit()
        finally:
            conn.close()
    
    def _rollup_confidence_buckets(self, trades: List[dict], environment: str, exchange: str, account_id: str):
        """Roll up trades into confidence calibration buckets."""
        # Define buckets: 0.5-0.6, 0.6-0.7, 0.7-0.8, 0.8-0.9, 0.9-1.0
        buckets = [(0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0)]
        
        conn = sqlite3.connect(self._db_path)
        try:
            for trade in trades:
                confidence = trade.get("entry_confidence", 0) or 0
                strategy = trade.get("strategy", "unknown")
                symbol = trade.get("symbol", "unknown")
                timeframe = trade.get("timeframe", "15m")
                mode = trade.get("mode", "PRECISION")
                pnl = trade.get("realized_pnl", 0) or 0
                
                # Find bucket
                bucket = None
                for low, high in buckets:
                    if low <= confidence < high:
                        bucket = (low, high)
                        break
                
                if not bucket:
                    continue
                
                is_win = pnl > 0
                is_loss = pnl < 0
                
                conn.execute("""
                    INSERT INTO global_confidence_buckets
                    (environment, exchange, account_id, strategy, symbol, timeframe, mode, bucket_low, bucket_high,
                     count, wins, losses, net_pnl, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                    ON CONFLICT(environment, exchange, account_id, strategy, symbol, timeframe, mode, bucket_low, bucket_high)
                    DO UPDATE SET
                        count = count + 1,
                        wins = wins + ?,
                        losses = losses + ?,
                        net_pnl = net_pnl + ?,
                        last_updated = ?
                """, (
                    environment, exchange, account_id,
                    strategy, symbol, timeframe, mode, bucket[0], bucket[1],
                    1 if is_win else 0,
                    1 if is_loss else 0,
                    pnl,
                    datetime.utcnow().isoformat(),
                    # For UPDATE
                    1 if is_win else 0,
                    1 if is_loss else 0,
                    pnl,
                    datetime.utcnow().isoformat(),
                ))
            
            conn.commit()
        finally:
            conn.close()
    # =========================================================================
    # QUERIES
    # =========================================================================
    
    def get_strategy_leaderboard(
        self,
        environment: str = "PAPER",
        exchange: str = "BINANCE_FUTURES",
        account_id: str = "default",
        limit: int = 20,
    ) -> List[dict]:
        """Get strategy performance leaderboard for specific env/exchange/account."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute("""
                SELECT 
                    strategy, symbol, timeframe, mode,
                    total_trades, wins, losses,
                    net_pnl, total_fees,
                    CASE WHEN total_trades > 0 THEN ROUND(wins * 100.0 / total_trades, 1) ELSE 0 END as win_rate,
                    CASE WHEN gross_loss > 0 THEN ROUND(gross_profit / gross_loss, 2) ELSE 0 END as profit_factor
                FROM global_strategy_performance
                WHERE environment = ? AND exchange = ? AND account_id = ?
                ORDER BY net_pnl DESC
                LIMIT ?
            """, (environment, exchange, account_id, limit))
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_confidence_calibration(
        self,
        strategy: str = None,
        environment: str = "PAPER",
        exchange: str = "BINANCE_FUTURES",
        account_id: str = "default",
    ) -> List[dict]:
        """Get confidence calibration data for specific env/exchange/account."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            query = """
                SELECT 
                    strategy, bucket_low, bucket_high, count, wins, losses,
                    net_pnl,
                    CASE WHEN count > 0 THEN ROUND(wins * 100.0 / count, 1) ELSE 0 END as win_rate,
                    CASE WHEN count > 0 THEN ROUND(net_pnl / count, 2) ELSE 0 END as avg_pnl
                FROM global_confidence_buckets
                WHERE environment = ? AND exchange = ? AND account_id = ?
            """
            params = [environment, exchange, account_id]
            
            if strategy:
                query += " AND strategy = ?"
                params.append(strategy)
            
            query += " ORDER BY strategy, bucket_low"
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_unprocessed_runs(self) -> List[str]:
        """Get run IDs that haven't been processed yet."""
        conn = sqlite3.connect(self._db_path)
        try:
            cursor = conn.execute("""
                SELECT r.run_id FROM runs r
                LEFT JOIN analytics_runs ar ON r.run_id = ar.run_id
                WHERE ar.run_id IS NULL OR ar.status != 'SUCCESS'
            """)
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def rebuild_from_runs(self, environment: str = "PAPER"):
        """
        Rebuild global analytics from all runs.
        Use for verification/correction.
        """
        conn = sqlite3.connect(self._db_path)
        try:
            # Clear global tables for this environment
            conn.execute("DELETE FROM global_strategy_performance WHERE environment = ?", (environment,))
            conn.execute("DELETE FROM global_confidence_buckets WHERE environment = ?", (environment,))
            conn.execute("DELETE FROM analytics_runs")
            conn.commit()
        finally:
            conn.close()
        
        # Reprocess all runs
        unprocessed = self.get_unprocessed_runs()
        for run_id in unprocessed:
            self.process_run(run_id, environment)


# Singleton
_analytics: Optional[GlobalAnalytics] = None


def get_global_analytics(db_path: str = "data/bot.db") -> GlobalAnalytics:
    global _analytics
    if _analytics is None:
        _analytics = GlobalAnalytics(db_path)
    return _analytics
