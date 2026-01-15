"""
TraceRecorder - Manages decision trace lifecycle.

Records complete decision traces from market snapshot through execution.
Every evaluation cycle per symbol gets a unique trace_id that threads
through all related events.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
import sqlite3
import threading

# Thread-local storage for current trace context
_trace_local = threading.local()


@dataclass
class StrategySignal:
    """One strategy's output."""
    strategy_name: str
    signal: str  # BUY/SELL/HOLD
    confidence: float
    reason: str
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GateDecision:
    """Risk gate decision."""
    allowed: bool
    reason_code: str
    reason: str
    severity: str = "INFO"
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    """Order execution result."""
    status: str  # ORDER_PLACED, REJECTED, ERROR, etc.
    order_id: Optional[str] = None
    fill_price: Optional[float] = None
    fill_qty: Optional[float] = None
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DecisionTrace:
    """Complete decision trace record."""
    trace_id: str
    run_id: str
    cycle_id: str
    symbol: str
    ts: str
    
    # Context
    account_id: str = "default"
    environment: str = "paper"
    timeframe: str = "15m"
    
    # Market snapshot
    last_price: float = 0.0
    mark_price: float = 0.0
    
    # Risk snapshot
    equity: float = 0.0
    margin_used: float = 0.0
    margin_level: float = 0.0
    drawdown_pct: float = 0.0
    open_positions_count: int = 0
    
    # Always-On Fields (New)
    regime_state: str = "UNKNOWN"
    regime_confidence: float = 0.0
    exposure_freeze: bool = False
    kill_switch_state: str = "NORMAL"
    portfolio_risk_budget: float = 0.0
    portfolio_risk_used: float = 0.0
    
    # Strategy outputs
    strategy_signals: List[StrategySignal] = field(default_factory=list)
    chosen_strategy: Optional[str] = None
    final_signal: str = "HOLD"
    final_confidence: float = 0.0
    reason_codes: str = "NONE" # Added
    
    # Gate decision
    gate_allowed: bool = True
    gate_reason: str = ""
    gate_details: Dict[str, Any] = field(default_factory=dict)
    
    # Action plan
    intended_action: str = "HOLD"
    sizing: Dict[str, Any] = field(default_factory=dict)
    sl_plan: Optional[float] = None
    tp_plan: Optional[float] = None
    
    # Execution result
    order_id: Optional[str] = None
    execution_status: str = "None"
    execution_error: Optional[str] = None
    fill_price: Optional[float] = None
    fill_qty: Optional[float] = None
    
    # Final outcome
    final_state_change: str = ""
    final_position: str = "NONE"


class TraceRecorder:
    """
    Records decision traces to database.
    """
    
    def __init__(self, db_path: str = "data/bot.db"):
        self._db_path = db_path
        self._traces: Dict[str, DecisionTrace] = {}
        self._lock = threading.Lock()
    
    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=1)
    
    def start_trace(
        self,
        run_id: str,
        cycle_id: str,
        symbol: str,
        account_id: str = "default",
        environment: str = "paper",
        timeframe: str = "15m",
    ) -> str:
        """Start a new trace. Returns trace_id."""
        trace_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc).isoformat()
        
        trace = DecisionTrace(
            trace_id=trace_id,
            run_id=run_id,
            cycle_id=cycle_id,
            symbol=symbol,
            ts=ts,
            account_id=account_id,
            environment=environment,
            timeframe=timeframe,
        )
        
        with self._lock:
            self._traces[trace_id] = trace
        
        # Set thread-local context
        _trace_local.trace_id = trace_id
        _trace_local.run_id = run_id
        _trace_local.cycle_id = cycle_id
        _trace_local.symbol = symbol
        
        return trace_id
    
    def record_market(
        self,
        trace_id: str,
        last_price: float,
        equity: float,
        margin_used: float = 0.0,
        margin_level: float = 0.0,
        drawdown_pct: float = 0.0,
        open_positions_count: int = 0,
        mark_price: Optional[float] = None,
        # Always-On Fields
        regime_state: str = "UNKNOWN",
        regime_confidence: float = 0.0,
        exposure_freeze: bool = False,
        kill_switch_state: str = "NORMAL",
        portfolio_risk_budget: float = 0.0,
        portfolio_risk_used: float = 0.0,
    ):
        """Record market and risk snapshot."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.last_price = last_price
                trace.mark_price = mark_price or last_price
                trace.equity = equity
                trace.margin_used = margin_used
                trace.margin_level = margin_level
                trace.drawdown_pct = drawdown_pct
                trace.open_positions_count = open_positions_count
                
                # New fields
                trace.regime_state = regime_state
                trace.regime_confidence = regime_confidence
                trace.exposure_freeze = exposure_freeze
                trace.kill_switch_state = kill_switch_state
                trace.portfolio_risk_budget = portfolio_risk_budget
                trace.portfolio_risk_used = portfolio_risk_used
    
    def record_strategies(
        self,
        trace_id: str,
        signals: List[StrategySignal],
        chosen_strategy: Optional[str] = None,
        final_signal: str = "HOLD",
        final_confidence: float = 0.0,
        reason_codes: str = "",
    ):
        """Record strategy outputs."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.strategy_signals = signals
                trace.chosen_strategy = chosen_strategy
                trace.final_signal = final_signal
                trace.final_confidence = final_confidence
                trace.reason_codes = reason_codes
    
    def record_gate(
        self,
        trace_id: str,
        allowed: bool,
        reason_code: str,
        reason: str = "",
        details: Optional[Dict] = None,
    ):
        """Record gate decision."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.gate_allowed = allowed
                trace.gate_reason = reason_code
                trace.gate_details = details or {}
    
    def record_intent(
        self,
        trace_id: str,
        action: str,
        sizing: Optional[Dict] = None,
        sl_plan: Optional[float] = None,
        tp_plan: Optional[float] = None,
    ):
        """Record intended action (before execution)."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.intended_action = action
                trace.sizing = sizing or {}
                trace.sl_plan = sl_plan
                trace.tp_plan = tp_plan
    
    def record_execution(
        self,
        trace_id: str,
        status: str,
        order_id: Optional[str] = None,
        fill_price: Optional[float] = None,
        fill_qty: Optional[float] = None,
        error: Optional[str] = None,
    ):
        """Record execution result."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.execution_status = status
                trace.order_id = order_id
                trace.fill_price = fill_price
                trace.fill_qty = fill_qty
                trace.execution_error = error
    
    def finalize(
        self,
        trace_id: str,
        state_change: str = "",
        final_position: str = "NONE",
    ):
        """Finalize and persist trace to database."""
        with self._lock:
            trace = self._traces.pop(trace_id, None)
        
        if not trace:
            return
        
        trace.final_state_change = state_change
        trace.final_position = final_position
        
        # Persist to DB
        try:
            conn = self._conn()
            try:
                signals_json = json.dumps([
                    {
                        "strategy": s.strategy_name,
                        "signal": s.signal,
                        "confidence": s.confidence,
                        "reason": s.reason,
                    }
                    for s in trace.strategy_signals
                ])
                
                conn.execute(
                    """
                    INSERT INTO decision_traces (
                        trace_id, run_id, cycle_id, account_id, environment,
                        symbol, timeframe, ts,
                        last_price, mark_price,
                        equity, margin_used, margin_level, drawdown_pct, open_positions_count,
                        regime_state, regime_confidence, exposure_freeze, kill_switch_state,
                        portfolio_risk_budget, portfolio_risk_used,
                        strategy_signals_json, chosen_strategy, signal, confidence, reason_codes,
                        gate_allowed, gate_reason, gate_details_json,
                        intended_action, sizing_json, sl_plan, tp_plan,
                        order_id, execution_status, execution_error, fill_price, fill_qty,
                        final_state_change, final_position
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trace.trace_id, trace.run_id, trace.cycle_id, trace.account_id, trace.environment,
                        trace.symbol, trace.timeframe, trace.ts,
                        trace.last_price, trace.mark_price,
                        trace.equity, trace.margin_used, trace.margin_level, trace.drawdown_pct, trace.open_positions_count,
                        trace.regime_state, trace.regime_confidence, 1 if trace.exposure_freeze else 0, trace.kill_switch_state,
                        trace.portfolio_risk_budget, trace.portfolio_risk_used,
                        signals_json, trace.chosen_strategy, trace.final_signal, trace.final_confidence, trace.reason_codes,
                        1 if trace.gate_allowed else 0, trace.gate_reason, json.dumps(trace.gate_details),
                        trace.intended_action, json.dumps(trace.sizing), trace.sl_plan, trace.tp_plan,
                        trace.order_id, trace.execution_status, trace.execution_error, trace.fill_price, trace.fill_qty,
                        trace.final_state_change, trace.final_position,
                    )
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            # Don't crash on trace persistence failure
            print(f"[TraceRecorder] Failed to persist trace {trace_id}: {e}")
    
    def get_trace(self, trace_id: str) -> Optional[Dict]:
        """Retrieve a trace from database."""
        try:
            conn = self._conn()
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT * FROM decision_traces WHERE trace_id = ?",
                    (trace_id,)
                ).fetchone()
                return dict(row) if row else None
            finally:
                conn.close()
        except Exception:
            return None
    
    def list_traces(
        self,
        symbol: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List recent traces."""
        try:
            conn = self._conn()
            conn.row_factory = sqlite3.Row
            try:
                query = "SELECT * FROM decision_traces WHERE 1=1"
                params = []
                
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
                if run_id:
                    query += " AND run_id = ?"
                    params.append(run_id)
                
                query += " ORDER BY ts DESC LIMIT ?"
                params.append(limit)
                
                rows = conn.execute(query, params).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()
        except Exception:
            return []


# Global instance
_recorder: Optional[TraceRecorder] = None


def get_trace_recorder(db_path: str = "data/bot.db") -> TraceRecorder:
    """Get or create global trace recorder."""
    global _recorder
    if _recorder is None:
        _recorder = TraceRecorder(db_path)
    return _recorder


def get_current_trace_id() -> Optional[str]:
    """Get current thread's trace_id."""
    return getattr(_trace_local, "trace_id", None)


def get_current_trace_context() -> Dict[str, str]:
    """Get current thread's trace context."""
    return {
        "trace_id": getattr(_trace_local, "trace_id", None),
        "run_id": getattr(_trace_local, "run_id", None),
        "cycle_id": getattr(_trace_local, "cycle_id", None),
        "symbol": getattr(_trace_local, "symbol", None),
    }
