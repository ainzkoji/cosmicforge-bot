"""
Position Manager - A+ Execution Phase State Machine

Implements position phases:
- ENTERED → SEEKING_TP1 → TP1_TAKEN → RUNNER_TRAILING → EXITING
- RESET_PENDING (flat, waiting for confirmation)

Handles:
- Phase transitions
- Break-even moves after TP1
- Trailing stops
- Time-based exits
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from datetime import datetime, timedelta


class PositionPhase(Enum):
    """Position lifecycle phases."""
    FLAT = "FLAT"                    # No position
    ENTERED = "ENTERED"              # Just opened
    SEEKING_TP1 = "SEEKING_TP1"      # Working toward TP1
    TP1_EXECUTING = "TP1_EXECUTING"  # Partial close order sent, awaiting fill
    TP1_FILLED = "TP1_FILLED"        # Partial close confirmed; runner alive, pre-trailing
    TP1_TAKEN = "TP1_TAKEN"          # Legacy alias (kept for backward compat)
    BREAK_EVEN_PENDING = "BREAK_EVEN_PENDING"  # BE stop update sent to exchange, awaiting confirmation
    TRAILING_UPDATE_PENDING = "TRAILING_UPDATE_PENDING"  # Trailing stop cancel-replace in progress
    RUNNER_TRAILING = "RUNNER_TRAILING"  # Trailing the runner
    EXITING = "EXITING"              # Exit in progress
    RESET_PENDING = "RESET_PENDING"  # Closed due to opposite signal, waiting


class PositionSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass
class TPLevels:
    """Take profit levels for a position."""
    tp1_price: float
    tp1_r: float
    tp1_close_fraction: float
    tp2_price: float
    tp2_r: float
    tp1_hit: bool = False
    tp1_fill_price: Optional[float] = None


@dataclass
class StopLevels:
    """Stop loss tracking."""
    initial_stop: float
    current_stop: float
    break_even_price: Optional[float] = None
    is_break_even: bool = False
    trail_atr_mult: float = 1.2
    # Order IDs for cancel-replace (populated after exchange placement)
    sl_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    # Break-even exchange confirmation tracking
    be_activation_ts: Optional[str] = None    # ISO timestamp when BE update was sent to exchange
    be_exchange_confirmed: bool = False        # True only after update_protection() succeeded
    # FIX-D: break-even buffer observability (set by _move_to_break_even)
    be_buffer_amount: Optional[float] = None      # $ buffer above/below entry applied at TP1
    be_buffer_pct_of_sl: Optional[float] = None  # buffer as % of original SL distance
    # Trailing stop update tracking
    trailing_last_update_ts: Optional[str] = None    # ISO timestamp of last live trailing stop update
    trailing_last_stop_price: Optional[float] = None  # Stop price confirmed at exchange on last trailing update


@dataclass
class PositionState:
    """Complete state for a position."""
    symbol: str
    side: PositionSide
    phase: PositionPhase
    
    # Entry info
    position_id: Optional[str]
    entry_price: float
    entry_time: datetime
    entry_qty: float
    current_qty: float
    
    # R calculation
    r_value: float  # Entry - Initial Stop (absolute)
    
    # TP/SL
    tp: TPLevels
    sl: StopLevels
    
    # Trailing
    highest_since_entry: float = 0.0  # For longs
    lowest_since_entry: float = 999999.0  # For shorts
    
    # Time tracking
    max_duration_seconds: int = 10800  # 3 hours default
    
    # TP1 partial-close execution tracking
    tp1_exec_qty: float = 0.0          # Qty sent to exchange for TP1 partial close
    tp1_fill_qty: float = 0.0          # Actual qty broker confirmed as closed
    tp1_exec_ts: Optional[str] = None  # ISO timestamp of TP1 order submission
    
    # Metadata
    strategy_name: str = ""
    mode: str = "PRECISION"  # PRECISION or FLOW
    exchange_position_active: bool = True
    reconciliation_status: str = "MANAGED"
    reconciliation_reason: Optional[str] = None
    last_reconciled_at: Optional[str] = None


@dataclass 
class ResetState:
    """State for reset-pending symbols."""
    symbol: str
    closed_side: PositionSide  # Side we just closed
    target_side: PositionSide  # Side we want to enter
    reset_started_at: datetime
    confirm_count: int = 0  # Consecutive confirmations
    cooldown_until: Optional[datetime] = None
    close_reason: str = "signal"  # "signal", "stop_loss", "time_stop"


class StrategyType(Enum):
    """Strategy types with different SL/TP profiles."""
    TREND = "TREND"
    MEAN_REVERSION = "MEAN_REVERSION"
    BREAKOUT = "BREAKOUT"


@dataclass
class StrategyTypeConfig:
    """TP/SL config per strategy type (locked spec)."""
    k_sl: float
    tp1_r: float
    tp2_r: float
    tp1_close_fraction: float
    k_trail: float
    max_sl_atr: float  # Reject if SL exceeds this


# Locked strategy-type configs per A+ spec
STRATEGY_TYPE_CONFIGS = {
    StrategyType.TREND: StrategyTypeConfig(
        k_sl=1.2,       # Slightly wider, breathing room
        tp1_r=1.0,
        tp2_r=2.2,
        tp1_close_fraction=0.5,
        k_trail=1.2,
        max_sl_atr=2.0,
    ),
    StrategyType.MEAN_REVERSION: StrategyTypeConfig(
        k_sl=0.8,       # Tight - fail fast
        tp1_r=0.8,      # Win-rate biased
        tp2_r=1.6,
        tp1_close_fraction=0.6,
        k_trail=0.9,
        max_sl_atr=1.2,  # Reject wide stops
    ),
    StrategyType.BREAKOUT: StrategyTypeConfig(
        k_sl=1.0,       # Structure-first
        tp1_r=1.0,
        tp2_r=2.5,
        tp1_close_fraction=0.5,
        k_trail=1.2,
        max_sl_atr=2.0,
    ),
}


@dataclass
class PositionManagerConfig:
    """Configuration for position management - A+ Final Spec."""
    # Anti-flip cooldowns
    min_hold_seconds: int = 600        # 10 min - no signal exits before this
    reset_cooldown_seconds: int = 900  # 15 min - after opposite signal close
    sl_cooldown_seconds: int = 1800    # 30 min - after stop loss (longer!)
    confirm_ticks_required: int = 3
    reentry_min_confidence: float = 0.80
    
    # TP/SL (Precision defaults)
    precision_k_sl: float = 1.2
    precision_tp1_r: float = 1.0
    precision_tp2_r: float = 2.2
    precision_tp1_close_fraction: float = 0.5
    precision_k_trail: float = 1.2
    precision_max_duration_seconds: int = 10800  # 3h
    
    # TP/SL (Flow defaults)
    flow_k_sl: float = 0.8
    flow_tp1_r: float = 0.8
    flow_tp2_r: float = 1.6
    flow_tp1_close_fraction: float = 0.6
    flow_k_trail: float = 0.9
    flow_max_duration_seconds: int = 7200  # 2h
    
    # Break-even
    be_fee_buffer_mult: float = 1.2  # 1.2x fees as buffer
    taker_fee_rate: float = 0.0005  # 0.05%
    # FIX-D: after TP1, buffer = max(be_sl_distance_buffer_pct * original_SL_distance, fee_buffer)
    # This prevents suffocating the trade with a $80 stop on a $500+ SL-distance asset.
    be_sl_distance_buffer_pct: float = 0.20  # 20% of original SL distance

    # Fee-aware gating
    min_edge_multiplier: float = 2.0  # TP1 must be 2x fees


class PositionManager:
    """
    Manages position phases and state transitions.

    OWNERSHIP CONTRACT
    ──────────────────
    SymbolState (runner.py / StateStore  db):
        Broker-aligned facts: position, entry_price, entry_qty, pending_open.
        Persisted by runner every cycle. Broker truth wins on divergence.
        Source of truth for "are we in a trade".

    PositionManager._positions (this class):
        Lifecycle phases: SEEKING_TP1 → TP1_TAKEN → RUNNER_TRAILING → EXITING.
        Advanced exit state: is_break_even, trailing_active, highest/lowest_since_entry.
        Order IDs for cancel-replace: sl.sl_order_id, sl.tp_order_id.
        Persisted to position_lifecycle_state via StateStore on EVERY mutation.
        MUST be restored from DB on restart — never rebuilt from ATR defaults
        when a DB record exists.

    Synchronisation rule:
        runner.step_symbol() updates SymbolState first (broker-synced),
        then calls PositionManager.update_price(). Neither module independently
        mutates the other's fields.

    Core principles:
    - No flips: close → reset → confirm → re-enter
    - Break-even after TP1
    - Trail after TP1
    - Time-based exit if stagnant
    """

    def __init__(
        self,
        config: Optional[PositionManagerConfig] = None,
        store=None,           # StateStore instance (optional, enables DB persistence)
        bot_instance_id: str = "default",
    ):
        self.config = config or PositionManagerConfig()
        self._positions: Dict[str, PositionState] = {}
        self._resets: Dict[str, ResetState] = {}
        self._store = store                       # StateStore — None in tests/paper mode
        self._bot_id = bot_instance_id

    # ── Persistence ──────────────────────────────────────────────────────────

    def _persist_lifecycle(self, symbol: str) -> None:
        """Write current PositionState for symbol to DB (fire-and-forget, logs errors)."""
        if not self._store:
            return
        pos = self._positions.get(symbol)
        if not pos:
            return
        try:
            existing_lifecycle = None
            try:
                existing_lifecycle = self._store.load_lifecycle_state(symbol, self._bot_id)
            except Exception:
                existing_lifecycle = None

            if existing_lifecycle:
                if not pos.position_id and existing_lifecycle.get("position_id"):
                    pos.position_id = existing_lifecycle.get("position_id")
                if not pos.sl.sl_order_id and existing_lifecycle.get("sl_order_id"):
                    pos.sl.sl_order_id = str(existing_lifecycle.get("sl_order_id"))
                if not pos.sl.tp_order_id and existing_lifecycle.get("tp_order_id"):
                    pos.sl.tp_order_id = str(existing_lifecycle.get("tp_order_id"))

            self._store.save_lifecycle_state(
                symbol=symbol,
                lifecycle={
                    "position_id": pos.position_id,
                    "phase": pos.phase.value,
                    "original_stop": pos.sl.initial_stop,
                    "current_stop": pos.sl.current_stop,
                    "original_tp1": pos.tp.tp1_price,
                    "original_tp2": pos.tp.tp2_price,
                    "is_break_even": pos.sl.is_break_even,
                    "tp1_hit": pos.tp.tp1_hit,
                    "trailing_active": pos.phase == PositionPhase.RUNNER_TRAILING,
                    "highest_since_entry": pos.highest_since_entry,
                    "lowest_since_entry": pos.lowest_since_entry,
                    "entry_qty_remaining": pos.current_qty,
                    "sl_order_id": pos.sl.sl_order_id,
                    "tp_order_id": pos.sl.tp_order_id,
                    "exchange_position_active": 1 if pos.exchange_position_active else 0,
                    "reconciliation_status": pos.reconciliation_status,
                    "reconciliation_reason": pos.reconciliation_reason,
                    "last_reconciled_at": pos.last_reconciled_at,
                    # TP1 partial-close tracking
                    "tp1_exec_qty": pos.tp1_exec_qty,
                    "tp1_fill_qty": pos.tp1_fill_qty,
                    "tp1_exec_ts": pos.tp1_exec_ts,
                    # Break-even exchange confirmation
                    "be_activation_ts": pos.sl.be_activation_ts,
                    "be_exchange_confirmed": pos.sl.be_exchange_confirmed,
                    # FIX-D: break-even buffer observability
                    "be_buffer_amount": pos.sl.be_buffer_amount,
                    "be_buffer_pct_of_sl": pos.sl.be_buffer_pct_of_sl,
                    # Trailing stop update tracking
                    "trailing_last_update_ts": pos.sl.trailing_last_update_ts,
                    "trailing_last_stop_price": pos.sl.trailing_last_stop_price,
                },
                bot_instance_id=self._bot_id,
            )
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                f"[PM_PERSIST] {symbol}: Failed to persist lifecycle state: {e}"
            )

    def restore_from_persisted(self, symbol: str, lifecycle: dict, entry_price: float,
                                entry_qty: float, side_str: str) -> Optional[PositionState]:
        """
        Reconstruct PositionState from a persisted lifecycle dict (from DB).
        This is the canonical restart path — do NOT use ATR defaults when this exists.

        Args:
            symbol: Trading symbol
            lifecycle: dict returned from StateStore.load_lifecycle_state()
            entry_price: From SymbolState (broker truth)
            entry_qty: From SymbolState (broker truth)
            side_str: "LONG" or "SHORT" from SymbolState
        Returns:
            Restored PositionState (also stored in self._positions[symbol])
        """
        import logging
        _log = logging.getLogger(__name__)

        try:
            phase_val = lifecycle.get("phase", "SEEKING_TP1")
            phase = PositionPhase(phase_val)
        except ValueError:
            phase = PositionPhase.SEEKING_TP1
            _log.warning(f"[PM_RESTORE_FROM_DB] {symbol}: unknown phase '{phase_val}', defaulting to SEEKING_TP1")

        side = PositionSide.LONG if side_str.upper() == "LONG" else PositionSide.SHORT
        original_stop = lifecycle.get("original_stop") or 0.0
        current_stop  = lifecycle.get("current_stop")  or original_stop
        tp1_price     = lifecycle.get("original_tp1")  or 0.0
        tp2_price     = lifecycle.get("original_tp2")  or 0.0
        r_value = abs(entry_price - original_stop)
        highest = lifecycle.get("highest_since_entry") or entry_price
        lowest  = lifecycle.get("lowest_since_entry")  or entry_price
        qty_remaining = lifecycle.get("entry_qty_remaining") or entry_qty
        mode_cfg = self.get_mode_config("PRECISION")

        state = PositionState(
            symbol=symbol,
            side=side,
            phase=phase,
            position_id=lifecycle.get("position_id"),
            entry_price=entry_price,
            entry_time=datetime.utcnow(),  # Not persisted; set to now on restart
            entry_qty=entry_qty,
            current_qty=float(qty_remaining),
            r_value=r_value,
            tp=TPLevels(
                tp1_price=float(tp1_price),
                tp1_r=mode_cfg["tp1_r"],
                tp1_close_fraction=mode_cfg["tp1_close_fraction"],
                tp2_price=float(tp2_price),
                tp2_r=mode_cfg["tp2_r"],
                tp1_hit=bool(lifecycle.get("tp1_hit", False)),
            ),
            sl=StopLevels(
                initial_stop=float(original_stop),
                current_stop=float(current_stop),
                is_break_even=bool(lifecycle.get("is_break_even", False)),
                sl_order_id=lifecycle.get("sl_order_id"),
                tp_order_id=lifecycle.get("tp_order_id"),
                be_activation_ts=lifecycle.get("be_activation_ts"),
                be_exchange_confirmed=bool(lifecycle.get("be_exchange_confirmed", False)),
                # FIX-D: restore break-even buffer observability fields
                be_buffer_amount=lifecycle.get("be_buffer_amount"),
                be_buffer_pct_of_sl=lifecycle.get("be_buffer_pct_of_sl"),
                trailing_last_update_ts=lifecycle.get("trailing_last_update_ts"),
                trailing_last_stop_price=lifecycle.get("trailing_last_stop_price"),
            ),
            highest_since_entry=float(highest),
            lowest_since_entry=float(lowest),
            max_duration_seconds=mode_cfg["max_duration"],
            tp1_exec_qty=float(lifecycle.get("tp1_exec_qty") or 0.0),
            tp1_fill_qty=float(lifecycle.get("tp1_fill_qty") or 0.0),
            tp1_exec_ts=lifecycle.get("tp1_exec_ts"),
            exchange_position_active=bool(lifecycle.get("exchange_position_active", True)),
            reconciliation_status=str(lifecycle.get("reconciliation_status") or "RESTORED"),
            reconciliation_reason=lifecycle.get("reconciliation_reason"),
            last_reconciled_at=lifecycle.get("last_reconciled_at"),
        )

        self._positions[symbol] = state
        _log.info(
            f"[PM_RESTORE_FROM_DB] {symbol}: phase={phase.value} "
            f"stop={current_stop} tp1={tp1_price} tp2={tp2_price} "
            f"be={state.sl.is_break_even} tp1_hit={state.tp.tp1_hit} "
            f"trailing={phase == PositionPhase.RUNNER_TRAILING} "
            f"tp1_exec_qty={state.tp1_exec_qty} tp1_fill_qty={state.tp1_fill_qty}"
        )
        return state

    def get_position(self, symbol: str) -> Optional[PositionState]:
        """Get current position state for a symbol."""
        return self._positions.get(symbol)
    
    def get_reset(self, symbol: str) -> Optional[ResetState]:
        """Get reset state for a symbol."""
        return self._resets.get(symbol)
    
    def is_in_position(self, symbol: str) -> bool:
        """Check if we have an open position."""
        pos = self._positions.get(symbol)
        return pos is not None and pos.phase not in [PositionPhase.FLAT, PositionPhase.EXITING]
    
    def is_in_reset(self, symbol: str) -> bool:
        """Check if symbol is in reset-pending state."""
        return symbol in self._resets
    
    def get_mode_config(self, mode: str) -> dict:
        """Get TP/SL config for a trading mode."""
        if mode == "FLOW":
            return {
                "k_sl": self.config.flow_k_sl,
                "tp1_r": self.config.flow_tp1_r,
                "tp2_r": self.config.flow_tp2_r,
                "tp1_close_fraction": self.config.flow_tp1_close_fraction,
                "k_trail": self.config.flow_k_trail,
                "max_duration": self.config.flow_max_duration_seconds,
            }
        return {
            "k_sl": self.config.precision_k_sl,
            "tp1_r": self.config.precision_tp1_r,
            "tp2_r": self.config.precision_tp2_r,
            "tp1_close_fraction": self.config.precision_tp1_close_fraction,
            "k_trail": self.config.precision_k_trail,
            "max_duration": self.config.precision_max_duration_seconds,
        }
    
    def compute_mfe_mae(self, symbol: str) -> tuple[Optional[float], Optional[float]]:
        """
        Compute Max Favorable Excursion and Max Adverse Excursion as percentages.

        MFE = peak unrealized gain from entry (always >= 0)
        MAE = worst unrealized loss from entry (always <= 0)

        Returns (None, None) if position is not tracked (e.g. after restart without restore).
        """
        pos = self._positions.get(symbol)
        if pos is None or pos.entry_price <= 0:
            return None, None

        ep = pos.entry_price
        if pos.side == PositionSide.LONG:
            mfe = (pos.highest_since_entry - ep) / ep * 100.0
            mae = (pos.lowest_since_entry - ep) / ep * 100.0
        else:  # SHORT
            mfe = (ep - pos.lowest_since_entry) / ep * 100.0
            mae = (ep - pos.highest_since_entry) / ep * 100.0

        # Enforce invariants: MFE >= 0, MAE <= 0
        mfe = max(0.0, mfe)
        mae = min(0.0, mae)
        return round(mfe, 6), round(mae, 6)

    # =========================================================================
    # POSITION LIFECYCLE
    # =========================================================================
    
    def open_position(
        self,
        symbol: str,
        side: PositionSide,
        position_id: Optional[str],
        entry_price: float,
        qty: float,
        stop_price: float,
        tp1_price: float,
        tp2_price: float,
        mode: str = "PRECISION",
        strategy_name: str = "",
        sl_order_id: Optional[str] = None,
        tp_order_id: Optional[str] = None,
    ) -> PositionState:
        """
        Open a new position and initialize state.
        """
        mode_cfg = self.get_mode_config(mode)
        
        # Calculate R value
        if side == PositionSide.LONG:
            r_value = abs(entry_price - stop_price)
        else:
            r_value = abs(stop_price - entry_price)
        
        state = PositionState(
            symbol=symbol,
            side=side,
            phase=PositionPhase.SEEKING_TP1,  # Skip ENTERED, go straight to seeking
            position_id=position_id,
            entry_price=entry_price,
            entry_time=datetime.utcnow(),
            entry_qty=qty,
            current_qty=qty,
            r_value=r_value,
            tp=TPLevels(
                tp1_price=tp1_price,
                tp1_r=mode_cfg["tp1_r"],
                tp1_close_fraction=mode_cfg["tp1_close_fraction"],
                tp2_price=tp2_price,
                tp2_r=mode_cfg["tp2_r"],
            ),
            sl=StopLevels(
                initial_stop=stop_price,
                current_stop=stop_price,
                trail_atr_mult=mode_cfg["k_trail"],
                sl_order_id=sl_order_id,
                tp_order_id=tp_order_id,
            ),
            highest_since_entry=entry_price,
            lowest_since_entry=entry_price,
            max_duration_seconds=mode_cfg["max_duration"],
            strategy_name=strategy_name,
            mode=mode,
            exchange_position_active=True,
            reconciliation_status="MANAGED",
        )
        
        self._positions[symbol] = state
        
        # Clear any reset state
        if symbol in self._resets:
            del self._resets[symbol]

        self._persist_lifecycle(symbol)  # Persist immediately after open
        return state
    
    def update_price(self, symbol: str, current_price: float, current_atr: float) -> Optional[str]:
        """
        Update position with current price. Returns action if needed.
        
        Actions: "HIT_TP1", "HIT_TP2", "HIT_STOP", "TRAIL_UPDATED", "TIME_EXIT", None
        """
        pos = self._positions.get(symbol)
        if not pos or pos.phase == PositionPhase.FLAT:
            return None
        
        # Update high/low tracking — both directions required for MFE/MAE computation
        pos.highest_since_entry = max(pos.highest_since_entry, current_price)
        pos.lowest_since_entry = min(pos.lowest_since_entry, current_price)
        
        # Check time exit
        if self._check_time_exit(pos):
            pos.phase = PositionPhase.EXITING
            self._persist_lifecycle(symbol)
            return "TIME_EXIT"
        
        # Check stop hit
        if self._check_stop_hit(pos, current_price):
            pos.phase = PositionPhase.EXITING
            self._persist_lifecycle(symbol)
            return "HIT_STOP"
        
        # Phase-specific updates
        if pos.phase == PositionPhase.SEEKING_TP1:
            if self._check_tp1_hit(pos, current_price):
                pos.tp.tp1_hit = True
                pos.tp.tp1_fill_price = current_price
                pos.phase = PositionPhase.TP1_TAKEN
                self._move_to_break_even(pos)
                self._persist_lifecycle(symbol)  # Persist: TP1 hit + BE active
                return "HIT_TP1"
        
        elif pos.phase == PositionPhase.TP1_TAKEN:
            # Transition to trailing
            pos.phase = PositionPhase.RUNNER_TRAILING
            self._persist_lifecycle(symbol)  # Persist: phase transition
        
        elif pos.phase == PositionPhase.RUNNER_TRAILING:
            # Update trailing stop
            if self._update_trailing_stop(pos, current_price, current_atr):
                self._persist_lifecycle(symbol)  # Persist: trailing anchor moved
                return "TRAIL_UPDATED"
            
            # Check TP2
            if self._check_tp2_hit(pos, current_price):
                pos.phase = PositionPhase.EXITING
                self._persist_lifecycle(symbol)
                return "HIT_TP2"
        
        return None
    
    def close_position(self, symbol: str, reason: str = "manual") -> Optional[PositionState]:
        """
        Close a position and return the final state.
        Deletes persisted lifecycle state from DB.
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        pos.phase = PositionPhase.FLAT
        del self._positions[symbol]

        # Delete persisted lifecycle — symbol is now flat
        if self._store:
            try:
                self._store.delete_lifecycle_state(symbol, bot_instance_id=self._bot_id)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    f"[PM_CLOSE] {symbol}: Failed to delete lifecycle state: {e}"
                )
        return pos

    def update_protection_order_ids(
        self,
        symbol: str,
        *,
        sl_order_id: str | None = None,
        tp_order_id: str | None = None,
        status: str = "PROTECTED",
        reason: str | None = None,
    ) -> bool:
        """
        Persist exchange-visible SL/TP IDs after restore/reconciliation repair.

        Returns False when PositionManager is not currently tracking the symbol.
        The store-level fallback is handled by runner for rows not loaded in memory.
        """
        pos = self._positions.get(symbol)
        if not pos:
            return False

        if "DUPLICATE_4130" in {str(sl_order_id or ""), str(tp_order_id or "")}:
            _log.critical(
                "[LIFECYCLE_TRUTH] %s: refused placeholder DUPLICATE_4130 "
                "as protection order evidence (status=%s reason=%s)",
                symbol,
                status,
                reason,
            )
            return False

        if sl_order_id:
            pos.sl.sl_order_id = str(sl_order_id)
        if tp_order_id:
            pos.sl.tp_order_id = str(tp_order_id)
        pos.exchange_position_active = True
        pos.reconciliation_status = status
        pos.reconciliation_reason = reason
        pos.last_reconciled_at = datetime.utcnow().isoformat()
        self._persist_lifecycle(symbol)
        return True

    def mark_position_flat(self, symbol: str, reason: str = "reconciled_flat") -> Optional[PositionState]:
        """
        Mark a tracked lifecycle flat while preserving an auditable FLAT row in DB.
        """
        pos = self._positions.get(symbol)
        if not pos:
            if self._store:
                try:
                    self._store.mark_lifecycle_flat(symbol, reason, bot_instance_id=self._bot_id)
                except Exception:
                    pass
            return None

        pos.phase = PositionPhase.FLAT
        pos.exchange_position_active = False
        pos.reconciliation_status = "FLAT"
        pos.reconciliation_reason = reason
        pos.last_reconciled_at = datetime.utcnow().isoformat()
        self._persist_lifecycle(symbol)
        del self._positions[symbol]
        return pos
    
    def close_for_opposite_signal(self, symbol: str, target_side: PositionSide) -> Optional[PositionState]:
        """
        Close position due to opposite signal and enter reset state.
        This is the anti-flip mechanism.
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        closed_side = pos.side
        final_state = self.close_position(symbol, reason="opposite_signal")
        
        # Enter reset-pending state
        self._resets[symbol] = ResetState(
            symbol=symbol,
            closed_side=closed_side,
            target_side=target_side,
            reset_started_at=datetime.utcnow(),
            confirm_count=0,
            cooldown_until=datetime.utcnow() + timedelta(seconds=self.config.reset_cooldown_seconds),
            close_reason="signal",
        )
        
        return final_state
    
    def close_for_stop_hit(self, symbol: str) -> Optional[PositionState]:
        """
        Close position due to stop loss hit.
        Uses LONGER cooldown than signal-based close (prevents revenge trading).
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        closed_side = pos.side
        final_state = self.close_position(symbol, reason="stop_loss")
        
        # Enter reset with LONGER cooldown (SL cooldown > reset cooldown)
        self._resets[symbol] = ResetState(
            symbol=symbol,
            closed_side=closed_side,
            target_side=PositionSide.FLAT,  # No target yet
            reset_started_at=datetime.utcnow(),
            confirm_count=0,
            cooldown_until=datetime.utcnow() + timedelta(seconds=self.config.sl_cooldown_seconds),
            close_reason="stop_loss",
        )
        
        return final_state
    
    def close_for_time_stop(self, symbol: str) -> Optional[PositionState]:
        """
        Close position due to time stop.
        Uses standard reset cooldown.
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        closed_side = pos.side
        final_state = self.close_position(symbol, reason="time_stop")
        
        # Enter reset state
        self._resets[symbol] = ResetState(
            symbol=symbol,
            closed_side=closed_side,
            target_side=PositionSide.FLAT,
            reset_started_at=datetime.utcnow(),
            confirm_count=0,
            cooldown_until=datetime.utcnow() + timedelta(seconds=self.config.reset_cooldown_seconds),
            close_reason="time_stop",
        )
        
        return final_state
    
    # =========================================================================
    # ANTI-FLIP LOGIC
    # =========================================================================
    
    def can_open_after_reset(
        self,
        symbol: str,
        signal_side: PositionSide,
        confidence: float,
    ) -> tuple[bool, str]:
        """
        Check if we can open a new position after reset.
        
        Returns: (allowed, reason)
        """
        reset = self._resets.get(symbol)
        if not reset:
            return True, "no_reset_pending"
        
        now = datetime.utcnow()
        
        # Check cooldown
        if reset.cooldown_until and now < reset.cooldown_until:
            remaining = (reset.cooldown_until - now).seconds
            return False, f"cooldown_active:{remaining}s"
        
        # Check if signal matches expected target
        if signal_side != reset.target_side:
            # Signal changed direction - reset the confirmation counter
            reset.target_side = signal_side
            reset.confirm_count = 0
            return False, "signal_direction_changed"
        
        # Check confidence
        if confidence < self.config.reentry_min_confidence:
            return False, f"confidence_too_low:{confidence:.2f}"
        
        # Increment confirmation
        reset.confirm_count += 1
        
        # Check confirmation count
        if reset.confirm_count < self.config.confirm_ticks_required:
            return False, f"confirming:{reset.confirm_count}/{self.config.confirm_ticks_required}"
        
        # All checks passed - clear reset and allow
        del self._resets[symbol]
        return True, "confirmed"
    
    def can_exit_position(self, symbol: str, reason: str = "signal") -> tuple[bool, str]:
        """
        Check if we can exit a position (respects min hold time).
        
        TP/SL exits always allowed. Signal-based exits respect min hold.
        """
        if reason in ["stop", "tp1", "tp2", "time"]:
            return True, "always_allowed"
        
        pos = self._positions.get(symbol)
        if not pos:
            return True, "no_position"
        
        elapsed = (datetime.utcnow() - pos.entry_time).total_seconds()
        if elapsed < self.config.min_hold_seconds:
            remaining = self.config.min_hold_seconds - elapsed
            return False, f"min_hold:{int(remaining)}s"
        
        return True, "hold_time_passed"
    
    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================
    
    def _check_stop_hit(self, pos: PositionState, price: float) -> bool:
        """Check if stop loss is hit."""
        if pos.side == PositionSide.LONG:
            return price <= pos.sl.current_stop
        return price >= pos.sl.current_stop
    
    def _check_tp1_hit(self, pos: PositionState, price: float) -> bool:
        """Check if TP1 is hit."""
        if pos.side == PositionSide.LONG:
            return price >= pos.tp.tp1_price
        return price <= pos.tp.tp1_price
    
    def _check_tp2_hit(self, pos: PositionState, price: float) -> bool:
        """Check if TP2 is hit."""
        if pos.side == PositionSide.LONG:
            return price >= pos.tp.tp2_price
        return price <= pos.tp.tp2_price
    
    def _check_time_exit(self, pos: PositionState) -> bool:
        """Check if max duration exceeded (only before TP1)."""
        if pos.tp.tp1_hit:
            return False  # Allow more time for runner
        
        elapsed = (datetime.utcnow() - pos.entry_time).total_seconds()
        return elapsed > pos.max_duration_seconds
    
    def _move_to_break_even(self, pos: PositionState):
        """
        FIX-D: Move stop to a buffered break-even after TP1.

        Buffer = max(be_sl_distance_buffer_pct * original_SL_distance, fee_buffer)

        The fee-only buffer was ~$80 on BTC ($67k × 0.12%) — far too tight for
        normal post-TP1 oscillation.  The 20% of original SL distance gives the
        runner room to breathe toward TP2 while locking in a small profit.

        Guards:
        - Never moves stop BACKWARDS (never weakens protection).
        - Falls back to fee-only buffer if initial_stop is missing.
        - Logs structured events for all key decisions.
        """
        import logging as _be_m_log
        _log = _be_m_log.getLogger(__name__)

        fee_buffer = pos.entry_price * self.config.taker_fee_rate * 2 * self.config.be_fee_buffer_mult
        sl_distance = abs(pos.entry_price - pos.sl.initial_stop) if pos.sl.initial_stop > 0 else 0.0

        if sl_distance <= 0:
            _log.warning(
                "[BREAK_EVEN_BUFFER_MISSING_ORIGINAL_SL] %s: "
                "initial_stop=%.6f — falling back to fee-only buffer=%.6f",
                pos.symbol, pos.sl.initial_stop, fee_buffer,
            )
            buffer = fee_buffer
        else:
            sl_pct_buffer = sl_distance * self.config.be_sl_distance_buffer_pct
            buffer = max(sl_pct_buffer, fee_buffer)

        if pos.side == PositionSide.LONG:
            be_price = pos.entry_price + buffer
        else:
            be_price = pos.entry_price - buffer

        # Never-loosen guard — proposed stop must improve protection
        if pos.side == PositionSide.LONG and be_price <= pos.sl.current_stop:
            _log.warning(
                "[BREAK_EVEN_BUFFER_SKIPPED_WOULD_WEAKEN_STOP] %s: "
                "proposed_be=%.6f <= current_stop=%.6f — skipping",
                pos.symbol, be_price, pos.sl.current_stop,
            )
            return
        if pos.side == PositionSide.SHORT and be_price >= pos.sl.current_stop:
            _log.warning(
                "[BREAK_EVEN_BUFFER_SKIPPED_WOULD_WEAKEN_STOP] %s: "
                "proposed_be=%.6f >= current_stop=%.6f — skipping",
                pos.symbol, be_price, pos.sl.current_stop,
            )
            return

        buffer_pct = (buffer / sl_distance * 100.0) if sl_distance > 0 else 0.0

        _log.info(
            "[BREAK_EVEN_BUFFER_CALCULATED] %s: side=%s entry=%.6f "
            "initial_stop=%.6f sl_distance=%.6f buffer=%.6f (%.1f%% of SL dist) "
            "be_price=%.6f  tp2=%.6f [TP2_TARGET_RETAINED_AFTER_TP1]",
            pos.symbol, pos.side.value, pos.entry_price,
            pos.sl.initial_stop, sl_distance, buffer, buffer_pct,
            be_price, pos.tp.tp2_price,
        )

        pos.sl.break_even_price = be_price
        pos.sl.current_stop = be_price
        pos.sl.is_break_even = True
        pos.sl.be_buffer_amount = buffer
        pos.sl.be_buffer_pct_of_sl = buffer_pct

        _log.info(
            "[BREAK_EVEN_BUFFER_APPLIED] %s: new_stop=%.6f be_buffer_$=%.6f "
            "be_buffer_pct_sl=%.1f%% prior_fee_only_buffer=%.6f  "
            "tp2_price=%.6f tp2_still_active=True",
            pos.symbol, be_price, buffer, buffer_pct, fee_buffer, pos.tp.tp2_price,
        )
    
    def _update_trailing_stop(self, pos: PositionState, price: float, atr: float) -> bool:
        """Update trailing stop. Returns True if stop was moved."""
        trail_distance = atr * pos.sl.trail_atr_mult
        
        if pos.side == PositionSide.LONG:
            new_stop = pos.highest_since_entry - trail_distance
            if new_stop > pos.sl.current_stop:
                pos.sl.current_stop = new_stop
                return True
        else:
            new_stop = pos.lowest_since_entry + trail_distance
            if new_stop < pos.sl.current_stop:
                pos.sl.current_stop = new_stop
                return True
        
        return False
    
    # =========================================================================
    # STATE SERIALIZATION
    # =========================================================================
    
    def get_state_summary(self, symbol: str) -> dict:
        """Get summary of position/reset state for a symbol."""
        pos = self._positions.get(symbol)
        reset = self._resets.get(symbol)
        
        result = {
            "symbol": symbol,
            "has_position": pos is not None,
            "has_reset": reset is not None,
        }
        
        if pos:
            result["position"] = {
                "side": pos.side.value,
                "phase": pos.phase.value,
                "entry_price": pos.entry_price,
                "current_stop": pos.sl.current_stop,
                "tp1_price": pos.tp.tp1_price,
                "tp1_hit": pos.tp.tp1_hit,
                "is_break_even": pos.sl.is_break_even,
                "mode": pos.mode,
            }
        
        if reset:
            result["reset"] = {
                "closed_side": reset.closed_side.value,
                "target_side": reset.target_side.value,
                "confirm_count": reset.confirm_count,
                "cooldown_until": reset.cooldown_until.isoformat() if reset.cooldown_until else None,
            }
        
        return result


