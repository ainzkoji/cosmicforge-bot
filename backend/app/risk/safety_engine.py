"""
Unified Safety Engine - Complete 4-Layer Protection System

Prevents account liquidation through comprehensive pre-trade, sizing, protective, and monitoring controls.

Architecture:
    Layer A: Pre-Trade Gating (Hard Blocks)
    Layer B: Sizing Controls (Exposure Reduction)
    Layer C: Protective Orders (Loss Containment)
    Layer D: Post-Trade Monitoring (Circuit Breakers)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

from app.persistence.db import DB, utc_now_iso
from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig, PositionRisk
from app.risk.account_protection import AccountProtection

logger = logging.getLogger(__name__)


class BlockReason(Enum):
    """Reasons for blocking a trade."""
    # Layer A
    DAILY_LOSS_LIMIT = "daily_loss_limit_exceeded"
    MAX_POSITIONS = "max_open_positions_reached"
    MAX_LEVERAGE = "max_leverage_exceeded"
    MAX_TRADES_DAY = "max_trades_per_day_reached"
    MARKET_CONDITIONS = "unsafe_market_conditions"
    BROKER_UNHEALTHY = "broker_health_check_failed"
    LOW_CONFIDENCE = "strategy_confidence_below_threshold"
    KYC_REQUIRED = "kyc_requirements_not_met"
    
    # Layer B
    MARGIN_BUFFER = "margin_buffer_violation"
    TOTAL_EXPOSURE = "total_exposure_limit_exceeded"
    
    # Layer C
    STOP_TOO_WIDE = "stop_loss_distance_too_wide"
    LEVERAGE_RISK = "leverage_risk_too_high"
    
    # Layer D
    ORDER_FAILURES = "repeated_order_failures"
    HIGH_SLIPPAGE = "excessive_slippage_detected"
    LIQUIDATION_RISK = "liquidation_proximity_warning"
    CIRCUIT_BREAKER = "symbol_circuit_breaker_active"


@dataclass
class MarketConditions:
    """Market condition metrics."""
    symbol: str
    spread_pct: float  # Bid-ask spread as % of mid price
    volatility: float  # ATR or similar
    volume_24h: float
    is_safe: bool
    reason: Optional[str] = None


@dataclass
class BrokerHealth:
    """Broker health status."""
    broker_id: str
    is_healthy: bool
    time_sync_ok: bool
    rate_limit_ok: bool
    last_check: str
    last_error: Optional[str] = None


@dataclass
class SafetyDecision:
    """Result of safety check."""
    allowed: bool
    block_reason: Optional[BlockReason] = None
    message: str = ""
    layer: str = ""  # A, B, C, or D
    
    # Sizing adjustments
    original_size: float = 0.0
    adjusted_size: float = 0.0
    size_reduction_pct: float = 0.0
    
    # Details for logging
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


@dataclass
class SafetyConfig:
    """Configuration for SafetyEngine."""
    # Layer A: Pre-Trade Gating
    max_leverage: float = 20.0
    max_trades_per_day: int = 100
    min_strategy_confidence: float = 0.3  # 30% minimum confidence
    max_spread_pct: float = 0.005  # 0.5% max spread
    max_volatility_multiplier: float = 3.0  # Reject if ATR > 3x normal
    require_kyc_for_live: bool = True
    
    # Layer B: Sizing Controls
    min_margin_buffer_pct: float = 0.30  # Keep 30% free margin minimum
    max_total_exposure_mult: float = 2.0  # Max 2x equity in notional
    volatility_scaling_enabled: bool = True
    volatility_base_atr: Dict[str, float] = None  # Base ATR per symbol
    
    # Layer C: Protective Orders
    max_stop_distance_pct: float = 0.10  # Max 10% stop distance
    max_compound_risk_pct: float = 0.15  # Max risk from (stop * leverage)
    
    # Layer D: Circuit Breakers
    max_order_failures: int = 3  # Pause after 3 consecutive failures
    max_slippage_pct: float = 0.02  # 2% max acceptable slippage
    liquidation_margin_ratio_threshold: float = 1.2  # Warn if margin ratio < 1.2
    circuit_breaker_cooldown_minutes: int = 30
    
    def __post_init__(self):
        if self.volatility_base_atr is None:
            self.volatility_base_atr = {}


class SafetyEngine:
    """
    Unified safety engine coordinating all protection layers.
    
    Call order:
    1. check_pre_trade() - Layer A gating
    2. calculate_safe_size() - Layer B sizing
    3. validate_protective_orders() - Layer C validation
    4. record_trade_result() - Layer D monitoring
    """
    
    def __init__(
        self,
        db: DB,
        risk_budget: RiskBudgetEngine,
        protection: AccountProtection,
        config: SafetyConfig
    ):
        self.db = db
        self.risk_budget = risk_budget
        self.protection = protection
        self.config = config
        self._init_monitoring_state()
    
    def _init_monitoring_state(self):
        """Initialize monitoring tables if they don't exist."""
        with self.db.connect() as conn:
            # Trade counters
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_trade_counts (
                    config_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    trade_count INTEGER DEFAULT 0,
                    PRIMARY KEY(config_id, date)
                )
            """)
            
            # Order failure tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS order_failures (
                    config_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    consecutive_failures INTEGER DEFAULT 0,
                    last_failure_at TEXT,
                    paused_until TEXT,
                    PRIMARY KEY(config_id, symbol)
                )
            """)
            
            # Slippage tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS slippage_monitoring (
                    config_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    avg_slippage_pct REAL DEFAULT 0.0,
                    max_slippage_pct REAL DEFAULT 0.0,
                    sample_count INTEGER DEFAULT 0,
                    last_updated TEXT,
                    PRIMARY KEY(config_id, symbol)
                )
            """)
    
    # =========================================================================
    # LAYER A: PRE-TRADE GATING (Hard Blocks)
    # =========================================================================
    
    def check_pre_trade(
        self,
        config_id: str,
        symbol: str,
        confidence: float,
        leverage: float,
        current_equity: float,
        open_positions: int,
        market_conditions: Optional[MarketConditions] = None,
        broker_health: Optional[BrokerHealth] = None,
        user_kyc_approved: bool = True,
        is_live_mode: bool = False
    ) -> SafetyDecision:
        """
        Layer A: Check all pre-trade gates.
        
        Returns SafetyDecision with allowed=False if any gate blocks.
        """
        
        # Gate 1: Daily trade count
        today = datetime.now(timezone.utc).date().isoformat()
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT trade_count FROM daily_trade_counts WHERE config_id = ? AND date = ?",
                (config_id, today)
            ).fetchone()
            
            trade_count = row["trade_count"] if row else 0
            
            if trade_count >= self.config.max_trades_per_day:
                return SafetyDecision(
                    allowed=False,
                    block_reason=BlockReason.MAX_TRADES_DAY,
                    message=f"Daily trade limit reached: {trade_count}/{self.config.max_trades_per_day}",
                    layer="A"
                )
        
        # Gate 2: Leverage limit
        if leverage > self.config.max_leverage:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.MAX_LEVERAGE,
                message=f"Leverage {leverage}x exceeds maximum {self.config.max_leverage}x",
                layer="A"
            )
        
        # Gate 3: Strategy confidence
        if confidence < self.config.min_strategy_confidence:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.LOW_CONFIDENCE,
                message=f"Strategy confidence {confidence:.1%} below threshold {self.config.min_strategy_confidence:.1%}",
                layer="A"
            )
        
        # Gate 4: Market conditions
        if market_conditions and not market_conditions.is_safe:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.MARKET_CONDITIONS,
                message=f"Unsafe market conditions: {market_conditions.reason}",
                layer="A",
                details={"spread_pct": market_conditions.spread_pct, "volatility": market_conditions.volatility}
            )
        
        # Gate 5: Broker health
        if broker_health and not broker_health.is_healthy:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.BROKER_UNHEALTHY,
                message=f"Broker health check failed: {broker_health.last_error}",
                layer="A"
            )
        
        # Gate 6: KYC requirements (live mode only)
        if is_live_mode and self.config.require_kyc_for_live and not user_kyc_approved:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.KYC_REQUIRED,
                message="KYC approval required for live trading",
                layer="A"
            )
        
        # Gate 7: Check for circuit breaker on this symbol
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT paused_until FROM order_failures WHERE config_id = ? AND symbol = ?",
                (config_id, symbol)
            ).fetchone()
            
            if row and row["paused_until"]:
                paused_until_dt = datetime.fromisoformat(row["paused_until"])
                if datetime.now(timezone.utc) < paused_until_dt:
                    return SafetyDecision(
                        allowed=False,
                        block_reason=BlockReason.CIRCUIT_BREAKER,
                        message=f"Symbol {symbol} circuit breaker active until {row['paused_until']}",
                        layer="A"
                    )
        
        # All gates passed
        return SafetyDecision(allowed=True, message="All pre-trade gates passed", layer="A")
    
    # =========================================================================
    # LAYER B: SIZING CONTROLS (Exposure Reduction)
    # =========================================================================
    
    def calculate_safe_size(
        self,
        config_id: str,
        symbol: str,
        base_size: float,
        entry_price: float,
        current_equity: float,
        margin_used: float,
        margin_available: float,
        leverage: float = 1.0,
        atr: Optional[float] = None,
        total_notional_exposure: float = 0.0
    ) -> SafetyDecision:
        """
        Layer B: Calculate safe position size with all controls applied.
        Includes Compensating Controls: Higher leverage -> Lower max exposure.
        """
        adjusted_size = base_size
        reductions = []
        
        notional = base_size * entry_price
        
        # Compensating Control: Leverage Penalty
        # If leverage is high, we artificially reduce the max exposure for this trade
        # e.g., if leverage > 10x, reduce size by 20%
        # if leverage > 20x, reduce size by 40%
        if leverage > 10.0:
            penalty_factor = 0.8
            if leverage > 20.0:
                penalty_factor = 0.6
            
            penalty_size = adjusted_size * penalty_factor
            if penalty_size < adjusted_size:
                reductions.append(f"Compensating Control: High Leverage ({leverage}x) -> Size reduced by {1-penalty_factor:.0%}")
                adjusted_size = penalty_size

        # Control 1: Margin buffer
        # Ensure we maintain minimum free margin
        required_margin_buffer = current_equity * self.config.min_margin_buffer_pct
        available_after_trade = margin_available - (notional / max(1.0, leverage)) 
        
        if available_after_trade < required_margin_buffer:
            # Reduce size to maintain buffer
            # Solve for size: (avail - (size*price/lev)) = buffer
            # avail - buffer = size*price/lev
            # (avail - buffer) * lev / price = size
            max_allowed_notional = (margin_available - required_margin_buffer) * leverage
            if max_allowed_notional < available_after_trade: # check logic
                 pass

            if available_after_trade < required_margin_buffer:
                 # Recalculate precisely
                 max_size_for_buffer = max(0, (margin_available - required_margin_buffer) * leverage / entry_price)
                 if max_size_for_buffer < adjusted_size:
                     adjusted_size = max_size_for_buffer
                     reductions.append(f"Margin buffer: {base_size:.4f} → {adjusted_size:.4f}")
        
        # Control 2: Total exposure cap
        max_total_exposure = current_equity * self.config.max_total_exposure_mult
        # Compensate: Reduce total exposure limits for high leverage users
        if leverage > 15.0:
             max_total_exposure *= 0.75 # 25% reduction in global exposure cap
             
        available_exposure = max_total_exposure - total_notional_exposure
        if available_exposure < (adjusted_size * entry_price):
                adjusted_size = max(0, available_exposure / entry_price)
                reductions.append(f"Total exposure: reduced to fit {available_exposure:.2f} USDT budget")
        
        # Control 3: Volatility scaling
        if self.config.volatility_scaling_enabled and atr:
            base_atr = self.config.volatility_base_atr.get(symbol, atr)  # Use current as base if not set
            if base_atr > 0:
                volatility_ratio = atr / base_atr
                
                if volatility_ratio > 1.5:  # High volatility (>150% of normal)
                    # Reduce size by volatility ratio
                    scale_factor = 1.0 / volatility_ratio
                    scaled_size = adjusted_size * scale_factor
                    reductions.append(f"Volatility scaling: {volatility_ratio:.2f}x normal → {scale_factor:.2%} size")
                    adjusted_size = scaled_size
        
        # Calculate reduction percentage
        size_reduction_pct = 0.0
        if base_size > 0:
            size_reduction_pct = (base_size - adjusted_size) / base_size
        
        # If size reduced to zero or near-zero, block trade
        if adjusted_size < (base_size * 0.1):  # Less than 10% of original
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.MARGIN_BUFFER if "Margin" in str(reductions) else BlockReason.TOTAL_EXPOSURE,
                message=f"Position size reduced below minimum threshold. Reductions: {'; '.join(reductions)}",
                layer="B",
                original_size=base_size,
                adjusted_size=adjusted_size,
                size_reduction_pct=size_reduction_pct,
                details={"reductions": reductions}
            )
        
        return SafetyDecision(
            allowed=True,
            message=f"Size controls applied: {len(reductions)} adjustments" if reductions else "No adjustments needed",
            layer="B",
            original_size=base_size,
            adjusted_size=adjusted_size,
            size_reduction_pct=size_reduction_pct,
            details={"reductions": reductions}
        )
    
    # =========================================================================
    # LAYER C: PROTECTIVE ORDERS (Loss Containment)
    # =========================================================================
    
    def validate_protective_orders(
        self,
        symbol: str,
        entry_price: float,
        stop_loss_price: float,
        leverage: float,
        position_size: float
    ) -> SafetyDecision:
        """
        Layer C: Validate stop-loss and protective order parameters.
        
        Ensures:
        - Stop isn't too wide
        - Combined risk (stop * leverage) isn't excessive
        """
        
        # Calculate stop distance
        stop_distance_pct = abs(entry_price - stop_loss_price) / entry_price
        
        # Check 1: Max stop distance
        if stop_distance_pct > self.config.max_stop_distance_pct:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.STOP_TOO_WIDE,
                message=f"Stop-loss distance {stop_distance_pct:.2%} exceeds maximum {self.config.max_stop_distance_pct:.2%}",
                layer="C",
                details={"entry": entry_price, "stop": stop_loss_price, "distance_pct": stop_distance_pct}
            )
        
        # Check 2: Compound risk (stop * leverage)
        # This is the actual loss% if stop is hit with leverage
        compound_risk_pct = stop_distance_pct * leverage
        
        if compound_risk_pct > self.config.max_compound_risk_pct:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.LEVERAGE_RISK,
                message=f"Compound risk {compound_risk_pct:.2%} (stop {stop_distance_pct:.2%} × {leverage}x leverage) exceeds maximum {self.config.max_compound_risk_pct:.2%}",
                layer="C",
                details={"stop_distance_pct": stop_distance_pct, "leverage": leverage, "compound_risk_pct": compound_risk_pct}
            )
        
        return SafetyDecision(
            allowed=True,
            message="Protective orders validated",
            layer="C",
            details={"stop_distance_pct": stop_distance_pct, "compound_risk_pct": compound_risk_pct}
        )
    
    # =========================================================================
    # LAYER D: POST-TRADE MONITORING (Circuit Breakers)
    # =========================================================================
    
    def record_order_result(
        self,
        config_id: str,
        symbol: str,
        success: bool,
        error_message: Optional[str] = None
    ):
        """
        Record order execution result.
        
        Triggers circuit breaker after repeated failures.
        """
        now = utc_now_iso()
        
        with self.db.connect() as conn:
            if success:
                # Reset failure counter on success
                conn.execute(
                    """
                    INSERT INTO order_failures (config_id, symbol, consecutive_failures, last_failure_at, paused_until)
                    VALUES (?, ?, 0, NULL, NULL)
                    ON CONFLICT(config_id, symbol) DO UPDATE SET
                        consecutive_failures = 0,
                        paused_until = NULL
                    """,
                    (config_id, symbol)
                )
            else:
                # Increment failure counter
                row = conn.execute(
                    "SELECT consecutive_failures FROM order_failures WHERE config_id = ? AND symbol = ?",
                    (config_id, symbol)
                ).fetchone()
                
                failures = (row["consecutive_failures"] if row else 0) + 1
                
                # Trigger circuit breaker if threshold reached
                paused_until = None
                if failures >= self.config.max_order_failures:
                    cooldown_dt = datetime.now(timezone.utc) + timedelta(minutes=self.config.circuit_breaker_cooldown_minutes)
                    paused_until = cooldown_dt.isoformat()
                    logger.error(
                        f"CIRCUIT BREAKER TRIGGERED: {symbol} paused until {paused_until} "
                        f"after {failures} consecutive failures"
                    )
                
                conn.execute(
                    """
                    INSERT INTO order_failures (config_id, symbol, consecutive_failures, last_failure_at, paused_until)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(config_id, symbol) DO UPDATE SET
                        consecutive_failures = ?,
                        last_failure_at = ?,
                        paused_until = ?
                    """,
                    (config_id, symbol, failures, now, paused_until, failures, now, paused_until)
                )
    
    def record_slippage(
        self,
        config_id: str,
        symbol: str,
        expected_price: float,
        executed_price: float
    ):
        """
        Record trade slippage.
        
        Monitors excessive slippage and can trigger symbol pause.
        """
        slippage_pct = abs(executed_price - expected_price) / expected_price
        now = utc_now_iso()
        
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT avg_slippage_pct, max_slippage_pct, sample_count FROM slippage_monitoring WHERE config_id = ? AND symbol = ?",
                (config_id, symbol)
            ).fetchone()
            
            if row:
                # Update running average
                current_avg = row["avg_slippage_pct"]
                count = row["sample_count"]
                new_avg = (current_avg * count + slippage_pct) / (count + 1)
                new_max = max(row["max_slippage_pct"], slippage_pct)
                new_count = count + 1
            else:
                new_avg = slippage_pct
                new_max = slippage_pct
                new_count = 1
            
            conn.execute(
                """
                INSERT INTO slippage_monitoring (config_id, symbol, avg_slippage_pct, max_slippage_pct, sample_count, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(config_id, symbol) DO UPDATE SET
                    avg_slippage_pct = ?,
                    max_slippage_pct = ?,
                    sample_count = ?,
                    last_updated = ?
                """,
                (config_id, symbol, new_avg, new_max, new_count, now, new_avg, new_max, new_count, now)
            )
            
            # Warning for high slippage
            if slippage_pct > self.config.max_slippage_pct:
                logger.warning(
                    f"HIGH SLIPPAGE: {symbol} - {slippage_pct:.2%} "
                    f"(expected: {expected_price}, executed: {executed_price})"
                )
    
    def check_liquidation_risk(
        self,
        current_equity: float,
        margin_used: float
    ) -> SafetyDecision:
        """
        Check if account is approaching liquidation.
        
        Margin Ratio = Equity / Maintenance Margin
        Liquidation occurs when ratio approaches 1.0
        """
        if margin_used <= 0:
            return SafetyDecision(allowed=True, message="No margin used", layer="D")
        
        margin_ratio = current_equity / margin_used
        
        if margin_ratio < self.config.liquidation_margin_ratio_threshold:
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.LIQUIDATION_RISK,
                message=f"Liquidation risk: Margin ratio {margin_ratio:.2f} below threshold {self.config.liquidation_margin_ratio_threshold}",
                layer="D",
                details={"margin_ratio": margin_ratio, "equity": current_equity, "margin_used": margin_used}
            )
        
        return SafetyDecision(
            allowed=True,
            message=f"Margin ratio healthy: {margin_ratio:.2f}",
            layer="D",
            details={"margin_ratio": margin_ratio}
        )
    
    def increment_trade_count(self, config_id: str):
        """Increment daily trade counter."""
        today = datetime.now(timezone.utc).date().isoformat()
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_trade_counts (config_id, date, trade_count)
                VALUES (?, ?, 1)
                ON CONFLICT(config_id, date) DO UPDATE SET
                    trade_count = trade_count + 1
                """,
                (config_id, today)
            )
