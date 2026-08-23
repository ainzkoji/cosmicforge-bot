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
    position_id: Optional[str] = None
    
    # Execution result
    order_id: Optional[str] = None
    execution_status: str = "None"
    execution_error: Optional[str] = None
    fill_price: Optional[float] = None
    fill_qty: Optional[float] = None
    
    # Final outcome
    final_state_change: str = ""
    final_position: str = "NONE"

    # ── Frozen indicator snapshot (ML feature capture) ────────────────────────
    # Regime indicators (from RegimeClassifier via MasterEnsemble meta)
    adx: Optional[float] = None
    atr_pct: Optional[float] = None
    ma_slope: Optional[float] = None
    compression_ratio: Optional[float] = None
    breakout_pressure: Optional[float] = None
    # Ensemble scores
    buy_score: Optional[float] = None
    sell_score: Optional[float] = None
    threshold: Optional[float] = None
    active_strategy_count: Optional[int] = None
    htf_opposed: Optional[bool] = None
    # Adaptive engine state
    aggressiveness_score: Optional[float] = None
    confidence_gate_modifier: Optional[float] = None
    size_multiplier: Optional[float] = None
    rolling_win_rate: Optional[float] = None
    rolling_expectancy: Optional[float] = None
    loss_streak: Optional[int] = None

    # ── ML inference result (Step 5D-2) ──────────────────────────────────────
    # Populated by MLEntryScorer after policy evaluation, before execution.
    # NULL when ML is disabled, model unavailable, or the trace is a HOLD cycle.
    ml_score: Optional[float] = None           # [0.0, 1.0] entry quality score
    ml_action: Optional[str] = None            # "ALLOW" | "BLOCK" | "SHADOW" | "SKIP"
    ml_model_version: Optional[str] = None     # artifact filename e.g. "entry_quality_v1.0_20260322"
    ml_threshold: Optional[float] = None       # threshold used for the BLOCK decision

    # ── Multi-instance auditing (Observability Fix 5) ─────────────────────────
    bot_instance_id: Optional[str] = None      # bot instance that generated this trace

    # ── [STAGE 2 AUDIT] Detailed Entry Funnel Telemetry (Added 2026-04-04) ──
    allocation_mode: Optional[str] = None
    base_size: Optional[float] = None
    final_size: Optional[float] = None
    final_qty: Optional[float] = None
    min_qty: Optional[float] = None
    min_notional: Optional[float] = None
    submit_attempted: bool = False
    broker_response: Optional[str] = None
    fill_recorded: bool = False
    position_opened: bool = False
    rejection_reason: Optional[str] = None

    # ── Event Awareness (Phase 1) ─────────────────────────────────────────────
    # Set when the cycle was skipped due to an active event blackout window.
    event_blocked: bool = False
    event_block_reason: Optional[str] = None      # e.g. "HIGH_IMPACT_USD_CPI_BLACKOUT"
    event_block_event_id: Optional[str] = None
    event_block_type: Optional[str] = None        # GLOBAL_BLACKOUT | SYMBOL_BLACKOUT | FEED_STALE_FAILSAFE
    event_block_details: Optional[str] = None     # JSON string with window metadata


class TraceRecorder:
    """
    Records decision traces to database.
    """
    
    def __init__(self, db_path: str = "../../data/bot.db"):
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
        bot_instance_id: Optional[str] = None,
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
            bot_instance_id=bot_instance_id,
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
        # Frozen indicator snapshot — adaptive engine state
        aggressiveness_score: Optional[float] = None,
        confidence_gate_modifier: Optional[float] = None,
        size_multiplier: Optional[float] = None,
        rolling_win_rate: Optional[float] = None,
        rolling_expectancy: Optional[float] = None,
        loss_streak: Optional[int] = None,
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

                # Always-On Fields
                trace.regime_state = regime_state
                trace.regime_confidence = regime_confidence
                trace.exposure_freeze = exposure_freeze
                trace.kill_switch_state = kill_switch_state
                trace.portfolio_risk_budget = portfolio_risk_budget
                trace.portfolio_risk_used = portfolio_risk_used

                # Adaptive engine state snapshot
                trace.aggressiveness_score = aggressiveness_score
                trace.confidence_gate_modifier = confidence_gate_modifier
                trace.size_multiplier = size_multiplier
                trace.rolling_win_rate = rolling_win_rate
                trace.rolling_expectancy = rolling_expectancy
                trace.loss_streak = loss_streak
    
    def record_regime(
        self,
        trace_id: str,
        regime_state: str,
        regime_confidence: float = 0.0,
    ):
        """
        Update regime fields on an in-flight trace.

        Called from the runner after MasterEnsemble returns so that the
        decision_traces.regime_state column is populated with the real
        classified regime rather than the default "UNKNOWN".
        """
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.regime_state      = regime_state or "UNKNOWN"
                trace.regime_confidence = float(regime_confidence)

    def record_strategies(
        self,
        trace_id: str,
        signals: List[StrategySignal],
        chosen_strategy: Optional[str] = None,
        final_signal: str = "HOLD",
        final_confidence: float = 0.0,
        reason_codes: str = "",
        # Frozen indicator snapshot — strategy / regime indicators
        adx: Optional[float] = None,
        atr_pct: Optional[float] = None,
        ma_slope: Optional[float] = None,
        compression_ratio: Optional[float] = None,
        breakout_pressure: Optional[float] = None,
        buy_score: Optional[float] = None,
        sell_score: Optional[float] = None,
        threshold: Optional[float] = None,
        active_strategy_count: Optional[int] = None,
        htf_opposed: Optional[bool] = None,
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
                trace.adx = adx
                trace.atr_pct = atr_pct
                trace.ma_slope = ma_slope
                trace.compression_ratio = compression_ratio
                trace.breakout_pressure = breakout_pressure
                trace.buy_score = buy_score
                trace.sell_score = sell_score
                trace.threshold = threshold
                trace.active_strategy_count = active_strategy_count
                trace.htf_opposed = htf_opposed

    def record_sizing(
        self,
        trace_id: str,
        allocation_mode: str,
        base_size: float,
        final_size: float,
        final_qty: float,
        min_qty: float = 0.0,
        min_notional: float = 0.0,
        rejection_reason: Optional[str] = None,
    ):
        """Record detailed sizing telemetry."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.allocation_mode = allocation_mode
                trace.base_size = base_size
                trace.final_size = final_size
                trace.final_qty = final_qty
                trace.min_qty = min_qty
                trace.min_notional = min_notional
                if rejection_reason:
                    trace.rejection_reason = rejection_reason

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

    def link_position(
        self,
        trace_id: str,
        position_id: str | None,
    ) -> None:
        """Persist a durable trace->position link as soon as position_id exists."""
        if not position_id:
            return

        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.position_id = position_id
                return

        conn = self._conn()
        try:
            conn.execute(
                "UPDATE decision_traces SET position_id = ? WHERE trace_id = ?",
                (position_id, trace_id),
            )
            conn.commit()
        finally:
            conn.close()
    
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
                trace.order_id = order_id
                trace.execution_status = status
                trace.execution_error = error
                trace.fill_price = fill_price
                trace.fill_qty = fill_qty
                
                # STAGE 2 AUDIT: Link broker response and submission flag
                trace.submit_attempted = True
                trace.broker_response = status
                if error:
                    trace.rejection_reason = f"BROKER_REJECT: {error}"
                trace.execution_error = error
    
    def record_event_block(
        self,
        trace_id: str,
        reason: str,
        details: Optional[Dict] = None,
    ) -> None:
        """Mark a trace as blocked by an active event blackout window."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.event_blocked = True
                trace.event_block_reason = reason
                trace.event_block_event_id = str((details or {}).get("event_id")) if (details or {}).get("event_id") is not None else None
                trace.event_block_type = (details or {}).get("block_type")
                trace.event_block_details = json.dumps(details or {})
                # Mirror into gate fields so dashboard queries work without schema changes
                trace.gate_allowed = False
                trace.gate_reason = "EVENT_BLACKOUT"
                trace.gate_details = {"event_block_reason": reason, **(details or {})}

    def record_ml_score(
        self,
        trace_id: str,
        score: Optional[float],
        action: Optional[str],
        model_version: Optional[str],
        threshold: Optional[float],
    ):
        """Record ML entry quality scorer result (Step 5D-2)."""
        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.ml_score = score
                trace.ml_action = action
                trace.ml_model_version = model_version
                trace.ml_threshold = threshold

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
                        intended_action, sizing_json, sl_plan, tp_plan, position_id,
                        order_id, execution_status, execution_error, fill_price, fill_qty,
                        final_state_change, final_position,
                        adx, atr_pct, ma_slope, compression_ratio, breakout_pressure,
                        buy_score, sell_score, threshold, active_strategy_count, htf_opposed,
                        aggressiveness_score, confidence_gate_modifier, size_multiplier,
                        rolling_win_rate, rolling_expectancy, loss_streak,
                        ml_score, ml_action, ml_model_version, ml_threshold,
                        bot_instance_id,
                        allocation_mode, base_size, final_size, final_qty,
                        min_qty, min_notional, submit_attempted,
                        broker_response, fill_recorded, position_opened,
                        rejection_reason,
                        event_blocked, event_block_reason, event_block_event_id,
                        event_block_type, event_block_details
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?
                    )
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
                        trace.intended_action, json.dumps(trace.sizing), trace.sl_plan, trace.tp_plan, trace.position_id,
                        trace.order_id, trace.execution_status, trace.execution_error, trace.fill_price, trace.fill_qty,
                        trace.final_state_change, trace.final_position,
                        trace.adx, trace.atr_pct, trace.ma_slope, trace.compression_ratio, trace.breakout_pressure,
                        trace.buy_score, trace.sell_score, trace.threshold, trace.active_strategy_count,
                        1 if trace.htf_opposed else (0 if trace.htf_opposed is not None else None),
                        trace.aggressiveness_score, trace.confidence_gate_modifier, trace.size_multiplier,
                        trace.rolling_win_rate, trace.rolling_expectancy, trace.loss_streak,
                        float(trace.ml_score) if trace.ml_score is not None else None,
                        trace.ml_action,
                        trace.ml_model_version,
                        float(trace.ml_threshold) if trace.ml_threshold is not None else None,
                        trace.bot_instance_id,
                        trace.allocation_mode,
                        trace.base_size,
                        trace.final_size,
                        trace.final_qty,
                        trace.min_qty,
                        trace.min_notional,
                        1 if trace.submit_attempted else 0,
                        trace.broker_response,
                        1 if trace.fill_recorded else 0,
                        1 if trace.position_opened else 0,
                        trace.rejection_reason,
                        1 if trace.event_blocked else 0,
                        trace.event_block_reason,
                        trace.event_block_event_id,
                        trace.event_block_type,
                        trace.event_block_details,
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


def get_trace_recorder(db_path: str = None) -> TraceRecorder:
    """Get or create global trace recorder.

    When called with no argument, derives the DB path from DB() so that
    the TraceRecorder always writes to the same database as every other
    persistence component (resolved via DATABASE_URL if set).
    """
    global _recorder
    if _recorder is None:
        if db_path is None:
            from shared_lib.persistence.db import DB
            db_path = DB().path
        _recorder = TraceRecorder(db_path)
    return _recorder

def reset_trace_recorder():
    """Clear the singleton instance so it can be re-initialized with a new path."""
    global _recorder
    _recorder = None


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
