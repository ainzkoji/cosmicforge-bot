"""
Run Manager - D Persistence System

Manages run lifecycle:
- STARTING → RUNNING → STOPPING → COMPLETED/FAILED
- Config snapshot at start
- Summary generation at end
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime
import json
import sqlite3
import threading

from shared_lib.persistence.ids import generate_run_id, set_run_id
from shared_lib.persistence.events import emit_info, emit_error, EventType


class RunStatus(Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class Environment(Enum):
    LIVE = "LIVE"
    PAPER = "PAPER"
    BACKTEST = "BACKTEST"


@dataclass
class RunSummary:
    """Summary statistics for a run."""
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    breakevens: int = 0
    win_rate: float = 0.0
    
    net_pnl: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    total_fees: float = 0.0
    
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    avg_r: float = 0.0
    
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    
    # Breakdowns
    by_strategy: Dict[str, dict] = None
    by_symbol: Dict[str, dict] = None
    by_mode: Dict[str, dict] = None
    
    # Trade stats
    avg_duration_minutes: float = 0.0
    longest_trade_minutes: float = 0.0
    
    # Risk stats
    risk_blocks_count: int = 0
    breaker_events: int = 0
    
    def to_dict(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": self.win_rate,
            "net_pnl": self.net_pnl,
            "gross_profit": self.gross_profit,
            "gross_loss": self.gross_loss,
            "total_fees": self.total_fees,
            "profit_factor": self.profit_factor,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "avg_r": self.avg_r,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_pct": self.max_drawdown_pct,
            "by_strategy": self.by_strategy or {},
            "by_symbol": self.by_symbol or {},
            "by_mode": self.by_mode or {},
            "avg_duration_minutes": self.avg_duration_minutes,
            "risk_blocks_count": self.risk_blocks_count,
            "breaker_events": self.breaker_events,
        }


class RunManager:
    """
    Manages run lifecycle and persistence.
    """
    
    _instance: Optional['RunManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls, db_path: str = "data/bot.db"):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._db_path = db_path
                    cls._instance._current_run_id: Optional[str] = None
                    cls._instance._init_table()
        return cls._instance
    
    def _init_table(self):
        """Ensure runs table exists."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    environment TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    version TEXT,
                    config_json TEXT,
                    summary_json TEXT,
                    error_summary TEXT,
                    notes TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()
    
    def start_run(
        self,
        environment: Environment = Environment.PAPER,
        config: Optional[Dict[str, Any]] = None,
        version: str = "1.0.0",
        notes: Optional[str] = None,
    ) -> str:
        """
        Start a new run. Returns run_id.
        """
        run_id = generate_run_id()
        self._current_run_id = run_id
        
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                INSERT INTO runs (
                    run_id, status, environment, started_at,
                    version, config_json, notes,
                    mode, interval_seconds, max_symbols
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                RunStatus.STARTING.value,
                environment.value,
                datetime.utcnow().isoformat(),
                version,
                json.dumps(config) if config else None,
                notes,
                "PAPER", # Legacy mode
                60,      # Legacy interval
                10       # Legacy max_symbols
            ))
            conn.commit()
        finally:
            conn.close()
        
        # Emit run start event
        emit_info(EventType.RUN_START, {
            "run_id": run_id,
            "environment": environment.value,
            "version": version,
        })
        
        if config:
            emit_info(EventType.RUN_CONFIG_SNAPSHOT, {"config": config})
        
        return run_id
    
    def set_running(self):
        """Transition to RUNNING status."""
        if not self._current_run_id:
            return
        
        self._update_status(RunStatus.RUNNING)
        emit_info(EventType.RUN_READY, {"run_id": self._current_run_id})
    
    def request_stop(self):
        """Request graceful stop."""
        if not self._current_run_id:
            return
        
        self._update_status(RunStatus.STOPPING)
        emit_info(EventType.RUN_STOP_REQUESTED, {"run_id": self._current_run_id})
    
    def end_run(self, summary: Optional[RunSummary] = None):
        """
        End run successfully with optional summary.
        """
        if not self._current_run_id:
            return
        
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                UPDATE runs SET
                    status = ?,
                    ended_at = ?,
                    summary_json = ?
                WHERE run_id = ?
            """, (
                RunStatus.COMPLETED.value,
                datetime.utcnow().isoformat(),
                json.dumps(summary.to_dict()) if summary else None,
                self._current_run_id,
            ))
            conn.commit()
        finally:
            conn.close()
        
        emit_info(EventType.RUN_END, {
            "run_id": self._current_run_id,
            "status": "COMPLETED",
            "summary": summary.to_dict() if summary else None,
        })
        
        self._current_run_id = None
    
    def fail_run(self, error: str):
        """Mark run as failed."""
        if not self._current_run_id:
            return
        
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                UPDATE runs SET
                    status = ?,
                    ended_at = ?,
                    error_summary = ?
                WHERE run_id = ?
            """, (
                RunStatus.FAILED.value,
                datetime.utcnow().isoformat(),
                error,
                self._current_run_id,
            ))
            conn.commit()
        finally:
            conn.close()
        
        emit_error(EventType.RUN_FAILED, {
            "run_id": self._current_run_id,
            "error": error,
        })
        
        self._current_run_id = None
    
    def _update_status(self, status: RunStatus):
        """Update run status."""
        if not self._current_run_id:
            return
        
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute(
                "UPDATE runs SET status = ? WHERE run_id = ?",
                (status.value, self._current_run_id)
            )
            conn.commit()
        finally:
            conn.close()
    
    def get_run(self, run_id: str) -> Optional[dict]:
        """Get run details by ID."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_current_run_id(self) -> Optional[str]:
        """Get current run ID."""
        return self._current_run_id
    
    def list_runs(self, limit: int = 20) -> list:
        """List recent runs."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            # Check which columns exist
            cursor = conn.execute("PRAGMA table_info(runs)")
            columns = {row[1] for row in cursor.fetchall()}
            
            # Build query with available columns
            select_cols = ["run_id", "status"]
            if "started_at" in columns:
                select_cols.append("started_at")
            if "ended_at" in columns:
                select_cols.append("ended_at")
            if "environment" in columns:
                select_cols.append("environment")
            
            cursor = conn.execute(f"""
                SELECT {', '.join(select_cols)}
                FROM runs ORDER BY run_id DESC LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()


# Singleton access
_manager: Optional[RunManager] = None


def get_run_manager(db_path: str = "data/bot.db") -> RunManager:
    """Get or create the run manager singleton."""
    global _manager
    if _manager is None:
        _manager = RunManager(db_path)
    return _manager


def start_run(
    environment: Environment = Environment.PAPER,
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """Convenience function to start a run."""
    return get_run_manager().start_run(environment, config)


def end_run(summary: Optional[RunSummary] = None):
    """Convenience function to end a run."""
    get_run_manager().end_run(summary)


def fail_run(error: str):
    """Convenience function to fail a run."""
    get_run_manager().fail_run(error)
