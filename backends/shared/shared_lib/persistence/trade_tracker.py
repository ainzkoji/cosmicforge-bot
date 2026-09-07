"""
Trade Tracker - D Persistence System

Tracks complete trade lifecycle with trade_id:
- Entry → TP1 → adds → final exit
- Links signals → decisions → orders → fills
- Computes realized PnL, R multiple
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime
import json
import sqlite3

from shared_lib.persistence.ids import generate_trade_id, get_current_run_id
from shared_lib.persistence.events import emit_info, EventType


class TradeStatus(Enum):
    OPEN = "OPEN"
    TP1_HIT = "TP1_HIT"
    RUNNER = "RUNNER"
    CLOSED = "CLOSED"


class ExitReason(Enum):
    TP1 = "TP1"
    TP2 = "TP2"
    STOP_LOSS = "STOP_LOSS"
    BREAK_EVEN = "BREAK_EVEN"
    TIME_STOP = "TIME_STOP"
    SIGNAL_EXIT = "SIGNAL_EXIT"
    MANUAL = "MANUAL"
    UNKNOWN = "UNKNOWN"


@dataclass
class Trade:
    """Complete trade lifecycle record."""
    trade_id: str
    run_id: str
    symbol: str
    side: str
    strategy: str
    mode: str
    timeframe: str
    
    # Entry
    entry_time: datetime
    entry_price: float
    entry_qty: float
    entry_confidence: float
    
    # Exit (populated on close)
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[ExitReason] = None
    
    # Metrics
    realized_pnl: float = 0.0
    fees: float = 0.0
    r_multiple: Optional[float] = None
    initial_stop: Optional[float] = None
    
    # Flags
    tp1_hit: bool = False
    tp1_time: Optional[datetime] = None
    add_count: int = 0
    
    # Status
    status: TradeStatus = TradeStatus.OPEN


class TradeTracker:
    """
    Tracks all trades with unique trade_id.
    Links signals → decisions → orders → fills.
    """
    
    def __init__(self, db_path: str = "data/bot.db"):
        self._db_path = db_path
        self._active_trades: Dict[str, Trade] = {}  # symbol -> Trade
        self._init_table()
    
    def _init_table(self):
        """Ensure trades table exists."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    
                    entry_time TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    entry_qty REAL NOT NULL,
                    entry_confidence REAL NOT NULL,
                    
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
                    
                    status TEXT NOT NULL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_run ON trades(run_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
            
            conn.commit()
        finally:
            conn.close()
    
    def open_trade(
        self,
        symbol: str,
        side: str,
        strategy: str,
        mode: str,
        timeframe: str,
        entry_price: float,
        entry_qty: float,
        entry_confidence: float,
        initial_stop: Optional[float] = None,
    ) -> str:
        """
        Open a new trade. Returns trade_id.
        """
        trade_id = generate_trade_id()
        run_id = get_current_run_id() or "unknown"
        
        trade = Trade(
            trade_id=trade_id,
            run_id=run_id,
            symbol=symbol,
            side=side,
            strategy=strategy,
            mode=mode,
            timeframe=timeframe,
            entry_time=datetime.utcnow(),
            entry_price=entry_price,
            entry_qty=entry_qty,
            entry_confidence=entry_confidence,
            initial_stop=initial_stop,
            status=TradeStatus.OPEN,
        )
        
        self._active_trades[symbol] = trade
        self._persist_trade(trade)
        
        emit_info(EventType.POSITION_OPENED, {
            "trade_id": trade_id,
            "symbol": symbol,
            "side": side,
            "strategy": strategy,
            "entry_price": entry_price,
            "qty": entry_qty,
        }, trade_id=trade_id, symbol=symbol, strategy=strategy, mode=mode)
        
        return trade_id
    
    def record_tp1(self, symbol: str, fill_price: float):
        """Record TP1 hit for a trade."""
        trade = self._active_trades.get(symbol)
        if not trade:
            return
        
        trade.tp1_hit = True
        trade.tp1_time = datetime.utcnow()
        trade.status = TradeStatus.TP1_HIT
        
        self._update_trade(trade)
        
        emit_info(EventType.TP1_HIT, {
            "trade_id": trade.trade_id,
            "fill_price": fill_price,
        }, trade_id=trade.trade_id, symbol=symbol)
    
    def record_add(self, symbol: str, add_price: float, add_qty: float):
        """Record an add to existing trade."""
        trade = self._active_trades.get(symbol)
        if not trade:
            return
        
        trade.add_count += 1
        self._update_trade(trade)
        
        emit_info(EventType.ADD_FILLED, {
            "trade_id": trade.trade_id,
            "add_price": add_price,
            "add_qty": add_qty,
            "add_count": trade.add_count,
        }, trade_id=trade.trade_id, symbol=symbol)
    
    def close_trade(
        self,
        symbol: str,
        exit_price: float,
        exit_reason: ExitReason,
        realized_pnl: float,
        fees: float = 0.0,
    ) -> Optional[Trade]:
        """
        Close a trade. Returns the closed trade.
        """
        trade = self._active_trades.pop(symbol, None)
        if not trade:
            return None
        
        trade.exit_time = datetime.utcnow()
        trade.exit_price = exit_price
        trade.exit_reason = exit_reason
        trade.realized_pnl = realized_pnl
        trade.fees = fees
        trade.status = TradeStatus.CLOSED
        
        # Calculate R multiple if we have initial stop
        if trade.initial_stop and trade.initial_stop != trade.entry_price:
            r_risk = abs(trade.entry_price - trade.initial_stop)
            if r_risk > 0:
                trade.r_multiple = realized_pnl / r_risk
        
        self._update_trade(trade)
        
        emit_info(EventType.POSITION_CLOSED, {
            "trade_id": trade.trade_id,
            "exit_price": exit_price,
            "exit_reason": exit_reason.value,
            "realized_pnl": realized_pnl,
            "r_multiple": trade.r_multiple,
            "duration_minutes": (trade.exit_time - trade.entry_time).total_seconds() / 60,
        }, trade_id=trade.trade_id, symbol=symbol)
        
        return trade
    
    def get_active_trade(self, symbol: str) -> Optional[Trade]:
        """Get active trade for a symbol."""
        return self._active_trades.get(symbol)
    
    def get_trade_id(self, symbol: str) -> Optional[str]:
        """Get trade_id for active trade on symbol."""
        trade = self._active_trades.get(symbol)
        return trade.trade_id if trade else None
    
    def _persist_trade(self, trade: Trade):
        """Insert trade into database."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                INSERT INTO trades (
                    trade_id, run_id, symbol, side, strategy, mode, timeframe,
                    entry_time, entry_price, entry_qty, entry_confidence,
                    initial_stop, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.trade_id, trade.run_id, trade.symbol, trade.side,
                trade.strategy, trade.mode, trade.timeframe,
                trade.entry_time.isoformat(), trade.entry_price,
                trade.entry_qty, trade.entry_confidence,
                trade.initial_stop, trade.status.value,
            ))
            conn.commit()
        finally:
            conn.close()
    
    def _update_trade(self, trade: Trade):
        """Update trade in database."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                UPDATE trades SET
                    exit_time = ?,
                    exit_price = ?,
                    exit_reason = ?,
                    realized_pnl = ?,
                    fees = ?,
                    r_multiple = ?,
                    tp1_hit = ?,
                    tp1_time = ?,
                    add_count = ?,
                    status = ?
                WHERE trade_id = ?
            """, (
                trade.exit_time.isoformat() if trade.exit_time else None,
                trade.exit_price,
                trade.exit_reason.value if trade.exit_reason else None,
                trade.realized_pnl,
                trade.fees,
                trade.r_multiple,
                1 if trade.tp1_hit else 0,
                trade.tp1_time.isoformat() if trade.tp1_time else None,
                trade.add_count,
                trade.status.value,
                trade.trade_id,
            ))
            conn.commit()
        finally:
            conn.close()
    
    def get_trades_by_run(self, run_id: str) -> List[dict]:
        """Get all trades for a run."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(
                "SELECT * FROM trades WHERE run_id = ? ORDER BY entry_time",
                (run_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_trade_summary(self, run_id: str) -> dict:
        """Get aggregate summary for a run."""
        conn = sqlite3.connect(self._db_path)
        try:
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN realized_pnl = 0 THEN 1 ELSE 0 END) as breakevens,
                    SUM(realized_pnl) as net_pnl,
                    SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) as gross_profit,
                    SUM(CASE WHEN realized_pnl < 0 THEN ABS(realized_pnl) ELSE 0 END) as gross_loss,
                    SUM(fees) as total_fees,
                    AVG(r_multiple) as avg_r
                FROM trades
                WHERE run_id = ? AND status = 'CLOSED'
            """, (run_id,))
            row = cursor.fetchone()
            
            if not row or row[0] == 0:
                return {"total_trades": 0}
            
            total, wins, losses, be, net_pnl, gross_profit, gross_loss, fees, avg_r = row
            
            return {
                "total_trades": total,
                "wins": wins or 0,
                "losses": losses or 0,
                "breakevens": be or 0,
                "win_rate": (wins / total * 100) if total > 0 else 0,
                "net_pnl": net_pnl or 0,
                "gross_profit": gross_profit or 0,
                "gross_loss": gross_loss or 0,
                "total_fees": fees or 0,
                "profit_factor": (gross_profit / gross_loss) if gross_loss and gross_loss > 0 else 0,
                "avg_r": avg_r or 0,
            }
        finally:
            conn.close()


# Global instance
_tracker: Optional[TradeTracker] = None


def get_trade_tracker(db_path: str = "data/bot.db") -> TradeTracker:
    """Get or create the trade tracker singleton."""
    global _tracker
    if _tracker is None:
        _tracker = TradeTracker(db_path)
    return _tracker
