"""
Event System - D Persistence Audit Trail

Standardized event taxonomy with append-only logging.
Every event has: run_id, timestamp, event_type, payload.
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List
from datetime import datetime
import json
import sqlite3
import threading

from app.persistence.ids import get_current_run_id, generate_event_id, generate_cycle_id


class EventLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class EventType(Enum):
    """Standardized event taxonomy."""
    
    # Run lifecycle
    RUN_START = "RUN_START"
    RUN_CONFIG_SNAPSHOT = "RUN_CONFIG_SNAPSHOT"
    RUN_READY = "RUN_READY"
    RUN_STOP_REQUESTED = "RUN_STOP_REQUESTED"
    RUN_END = "RUN_END"
    RUN_FAILED = "RUN_FAILED"
    
    # Strategy/signal
    STRATEGY_SIGNAL = "STRATEGY_SIGNAL"
    REGIME_CLASSIFIED = "REGIME_CLASSIFIED"
    
    # Policy + risk gating
    TRADE_POLICY_DECISION = "TRADE_POLICY_DECISION"
    RISK_GATE_ALLOW = "RISK_GATE_ALLOW"
    RISK_GATE_BLOCK = "RISK_GATE_BLOCK"
    BREAKER_STATE_CHANGE = "BREAKER_STATE_CHANGE"
    
    # Execution
    ORDER_SUBMIT = "ORDER_SUBMIT"
    ORDER_ACK = "ORDER_ACK"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_PARTIAL_FILL = "ORDER_PARTIAL_FILL"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_CANCELED = "ORDER_CANCELED"
    
    # Position management
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_PHASE_CHANGE = "POSITION_PHASE_CHANGE"
    TP1_HIT = "TP1_HIT"
    STOP_MOVED_TO_BE = "STOP_MOVED_TO_BE"
    TRAIL_STOP_UPDATED = "TRAIL_STOP_UPDATED"
    ADD_PLACED = "ADD_PLACED"
    ADD_FILLED = "ADD_FILLED"
    ADD_EXITED = "ADD_EXITED"
    TIME_STOP_EXIT = "TIME_STOP_EXIT"
    POSITION_CLOSED = "POSITION_CLOSED"
    RESET_PENDING_STARTED = "RESET_PENDING_STARTED"
    RESET_CONFIRMED_REENTRY = "RESET_CONFIRMED_REENTRY"
    RESET_COOLDOWN_ACTIVE = "RESET_COOLDOWN_ACTIVE"
    
    # Reconciliation
    STATE_SYNC_START = "STATE_SYNC_START"
    STATE_SYNC_DIFF_FOUND = "STATE_SYNC_DIFF_FOUND"
    STATE_SYNC_REPAIRED = "STATE_SYNC_REPAIRED"
    STATE_SYNC_OK = "STATE_SYNC_OK"
    
    # Errors
    EXCHANGE_ERROR = "EXCHANGE_ERROR"
    DATA_ERROR = "DATA_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass
class Event:
    """Structured event for audit trail."""
    event_type: EventType
    level: EventLevel
    payload: Dict[str, Any] = field(default_factory=dict)
    
    # Optional context
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    strategy: Optional[str] = None
    mode: Optional[str] = None
    trade_id: Optional[str] = None
    
    # Auto-populated
    event_id: str = field(default_factory=generate_event_id)
    run_id: Optional[str] = field(default_factory=get_current_run_id)
    cycle_id: Optional[int] = None
    ts: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "run_id": self.run_id,
            "trade_id": self.trade_id,
            "cycle_id": self.cycle_id,
            "ts": self.ts,
            "event_type": self.event_type.value,
            "level": self.level.value,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "strategy": self.strategy,
            "mode": self.mode,
            "payload": self.payload,
        }


class EventStore:
    """
    Append-only event store.
    Events are never updated (audit trail integrity).
    """
    
    _instance: Optional['EventStore'] = None
    _lock = threading.Lock()
    
    def __new__(cls, db_path: str = "data/bot.db"):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._db_path = db_path
                    cls._instance._init_table()
        return cls._instance
    
    def _init_table(self):
        """Ensure events table exists with all columns."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    trade_id TEXT,
                    cycle_id INTEGER,
                    ts TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    level TEXT NOT NULL,
                    symbol TEXT,
                    timeframe TEXT,
                    strategy TEXT,
                    mode TEXT,
                    payload_json TEXT
                )
            """)
            
            # Migration: add missing columns to existing table
            cursor = conn.execute("PRAGMA table_info(events)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            if "ts" not in existing_cols:
                # Old table might use 'timestamp' or 'created_at'
                for old_name in ["timestamp", "created_at", "timestamp_utc"]:
                    if old_name in existing_cols:
                        # Can't rename in SQLite, so create ts as alias view
                        conn.execute(f"ALTER TABLE events RENAME COLUMN {old_name} TO ts")
                        break
                else:
                    # ts column doesn't exist, add it with default
                    try:
                        conn.execute("ALTER TABLE events ADD COLUMN ts TEXT DEFAULT '1970-01-01T00:00:00'")
                    except Exception:
                        pass
            
            # Ensure other columns exist
            for col, default in [
                ("trade_id", "TEXT"),
                ("cycle_id", "INTEGER"),
                ("level", "TEXT DEFAULT 'INFO'"),
                ("timeframe", "TEXT"),
                ("strategy", "TEXT"),
                ("mode", "TEXT"),
                ("payload_json", "TEXT"),
            ]:
                col_name = col.split()[0] if " " in col else col
                if col_name not in existing_cols:
                    try:
                        conn.execute(f"ALTER TABLE events ADD COLUMN {col} {default if default != col else ''}")
                    except Exception:
                        pass
            
            # Indexes for fast queries (only if ts column exists)
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_events_run_ts ON events(run_id, ts)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_events_trade ON events(trade_id, ts)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, ts)")
            except Exception:
                pass
            
            conn.commit()
        finally:
            conn.close()
    
    def emit(self, event: Event) -> str:
        """
        Append event to store. Returns event_id.
        This is append-only - events are never updated.
        """
        # Add cycle_id if not set
        if event.cycle_id is None:
            event.cycle_id = generate_cycle_id()
        
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                INSERT INTO events (
                    event_id, run_id, trade_id, cycle_id, ts, timestamp_utc,
                    event_type, level, symbol, timeframe, strategy, mode,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.event_id,
                event.run_id,
                event.trade_id,
                event.cycle_id,
                event.ts,
                event.ts, # Write to legacy column too
                event.event_type.value,
                event.level.value,
                event.symbol,
                event.timeframe,
                event.strategy,
                event.mode,
                json.dumps(event.payload),
            ))
            conn.commit()
        finally:
            conn.close()
        
        return event.event_id
    
    def query(
        self,
        run_id: Optional[str] = None,
        trade_id: Optional[str] = None,
        event_type: Optional[EventType] = None,
        symbol: Optional[str] = None,
        level: Optional[EventLevel] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[dict]:
        """Query events with filters."""
        conditions = []
        params = []
        
        if run_id:
            conditions.append("run_id = ?")
            params.append(run_id)
        if trade_id:
            conditions.append("trade_id = ?")
            params.append(trade_id)
        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type.value)
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if level:
            conditions.append("level = ?")
            params.append(level.value)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(f"""
                SELECT * FROM events
                WHERE {where_clause}
                ORDER BY ts DESC
                LIMIT ? OFFSET ?
            """, params + [limit, offset])
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
    
    def get_by_trade(self, trade_id: str) -> List[dict]:
        """Get all events for a trade (for debugging)."""
        return self.query(trade_id=trade_id, limit=1000)
    
    def get_by_run(self, run_id: str, limit: int = 500) -> List[dict]:
        """Get events for a run."""
        return self.query(run_id=run_id, limit=limit)


# Singleton instance
_store: Optional[EventStore] = None


def get_event_store(db_path: str = "data/bot.db") -> EventStore:
    """Get or create the event store singleton."""
    global _store
    if _store is None:
        _store = EventStore(db_path)
    return _store


# Convenience functions
def emit_event(
    event_type: EventType,
    payload: Dict[str, Any],
    level: EventLevel = EventLevel.INFO,
    symbol: Optional[str] = None,
    trade_id: Optional[str] = None,
    strategy: Optional[str] = None,
    mode: Optional[str] = None,
) -> str:
    """Emit an event to the store. Returns event_id."""
    event = Event(
        event_type=event_type,
        level=level,
        payload=payload,
        symbol=symbol,
        trade_id=trade_id,
        strategy=strategy,
        mode=mode,
    )
    return get_event_store().emit(event)


def emit_info(event_type: EventType, payload: Dict[str, Any], **kwargs) -> str:
    """Emit INFO level event."""
    return emit_event(event_type, payload, EventLevel.INFO, **kwargs)


def emit_warn(event_type: EventType, payload: Dict[str, Any], **kwargs) -> str:
    """Emit WARN level event."""
    return emit_event(event_type, payload, EventLevel.WARN, **kwargs)


def emit_error(event_type: EventType, payload: Dict[str, Any], **kwargs) -> str:
    """Emit ERROR level event."""
    return emit_event(event_type, payload, EventLevel.ERROR, **kwargs)
