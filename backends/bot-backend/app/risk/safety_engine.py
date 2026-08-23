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

from shared_lib.persistence.db import DB, utc_now_iso
from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig, PositionRisk
from app.risk.account_protection import AccountProtection
from app.risk.risk_policy import RiskPolicy
from app.risk.sizing_engine import SizingResult
from app.risk.dynamic_threshold import (
    DynamicThresholdCalculator,
    log_threshold_event,
    get_dynamic_threshold_calculator,
)
logger = logging.getLogger(__name__)


def normalize_threshold(value: float) -> float:
    """
    Normalize confidence threshold to 0.0-1.0 range.
    
    Handles config values that may be specified as:
    - 0.0-1.0 (already normalized)
    - 0-100 (percentage format - will be divided by 100)
    
    Examples:
        normalize_threshold(0.50) -> 0.50
        normalize_threshold(50) -> 0.50
        normalize_threshold(50.0) -> 0.50
    
    Args:
        value: Threshold value to normalize
        
    Returns:
        Normalized threshold in 0.0-1.0 range
        
    Raises:
        ValueError: If normalized value is not in valid range
    """
    # If value > 1.0, assume it's in 0-100 range
    if value > 1.0:
        normalized = value / 100.0
        logger.info(f"Normalized threshold from {value} (0-100 scale) to {normalized:.3f} (0.0-1.0 scale)")
    else:
        normalized = value
    
    # Validate final range
    if not (0.0 <= normalized <= 1.0):
        raise ValueError(f"Invalid threshold value: {value} (normalized: {normalized}). Must be in 0.0-1.0 range after normalization.")
    
    return normalized


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
    
    # Dual confidence thresholds for flexibility
    # - hard: Standard threshold for normal trading (raised from 0.10 to reduce low-quality signals)
    # - soft: Fallback threshold for daily activity targets
    min_confidence_hard: float = 0.40
    # Standard threshold
    
    # Daily Activity Fallback
    min_confidence_soft: float = 0.05  # Soft confidence threshold for fallback trades (5%)
    daily_activity_fallback_enabled: bool = False  # B-6 Fix: never soften thresholds due to inactivity — waiting is correct behavior
    daily_activity_fallback_hours: int = 24  # Hours of inactivity before fallback activates
    fallback_position_size_multiplier: float = 0.25  # Reduce size to 25% for fallback trades
    fallback_max_leverage: float = 5.0  # Max leverage for fallback trades (3x-5x recommended)
    
    max_spread_pct: float = 0.005  # 0.5% max spread
    max_volatility_multiplier: float = 3.0  # Reject if ATR > 3x normal
    require_kyc_for_live: bool = True
    
    # Layer B: Sizing Controls
    min_margin_buffer_pct: float = 0.15  # Keep 15% free margin minimum (was 30% — too restrictive, zeroed out all fallback sizes)
    max_total_exposure_mult: float = 2.0  # Max 2x equity in notional
    volatility_scaling_enabled: bool = True
    volatility_base_atr: Dict[str, float] = None  # Base ATR per symbol
    
    # Layer C: Protective Orders
    max_stop_distance_pct: float = 0.10  # Max 10% stop distance
    max_compound_risk_pct: float = 0.15  # Max risk from (stop * leverage)
    
    # Layer D: Circuit Breakers
    max_order_failures: int = 10  # Pause after 10 consecutive failures (was 5 — too sensitive for margin errors)
    max_slippage_pct: float = 0.02  # 2% max acceptable slippage
    liquidation_margin_ratio_threshold: float = 1.2  # Warn if margin ratio < 1.2
    circuit_breaker_cooldown_minutes: int = 1  # Base cooldown (exponential backoff applied at trigger time, cap=3min)
    
    def __post_init__(self):
        if self.volatility_base_atr is None:
            self.volatility_base_atr = {}
        
        # Normalize confidence thresholds (handle 0-100 scale values)
        self.min_confidence_hard = normalize_threshold(self.min_confidence_hard)
        self.min_confidence_soft = normalize_threshold(self.min_confidence_soft)
        
        # Validate that soft <= hard
        if self.min_confidence_soft > self.min_confidence_hard:
            logger.warning(
                f"min_confidence_soft ({self.min_confidence_soft:.3f}) is greater than "
                f"min_confidence_hard ({self.min_confidence_hard:.3f}). Swapping values."
            )
            self.min_confidence_soft, self.min_confidence_hard = self.min_confidence_hard, self.min_confidence_soft
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
        config: SafetyConfig,
        risk_policy: Optional[RiskPolicy] = None,
        dyn_threshold_calculator: Optional[DynamicThresholdCalculator] = None
    ):
        self.db = db
        self.risk_budget = risk_budget
        self.protection = protection
        self.config = config
        self.risk_policy = risk_policy
        # Dynamic confidence threshold: adapts per-symbol from rolling history
        self._dyn_threshold = dyn_threshold_calculator or get_dynamic_threshold_calculator()
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
            
            # Daily activity tracking for fallback mechanism
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_activity_tracking (
                    config_id TEXT PRIMARY KEY,
                    last_trade_timestamp TEXT,
                    last_fallback_trade_timestamp TEXT,
                    total_trades INTEGER DEFAULT 0,
                    total_fallback_trades INTEGER DEFAULT 0
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
        is_live_mode: bool = False,
        **kwargs
    ) -> SafetyDecision:
        """
        Layer A: Check all pre-trade gates.
        Wraps internal logic to provide comprehensive logging.
        """
        # Execute internal logic
        decision = self._check_pre_trade_internal(
            config_id, symbol, confidence, leverage, current_equity, 
            open_positions, market_conditions, broker_health, 
            user_kyc_approved, is_live_mode, **kwargs
        )

        # Mandatory Logging for Analysis
        strategy_name = kwargs.get('strategy_name', 'unknown')
        signal = kwargs.get('signal', 'unknown')
        
        # Determine threshold used for the outer eval log
        is_fallback_mode = kwargs.get('is_fallback_mode', False)
        use_soft_threshold = kwargs.get('use_soft_threshold', False) or is_fallback_mode
        if use_soft_threshold:
            threshold_used = self.config.min_confidence_soft
        else:
            dyn_result = self._dyn_threshold.get_threshold(symbol)
            threshold_used = dyn_result.threshold
        
        decision_str = "PASS" if decision.allowed else "BLOCK"
        reason_code = decision.block_reason.value if decision.block_reason else "None"
        
        # Mandatory Logging for Analysis
        logger.debug(
            f"Layer A Eval: symbol={symbol}, strategy_name={strategy_name}, signal={signal}, "
            f"confidence_raw={confidence:.4f}, confidence_normalized={confidence:.4f}, "
            f"threshold_used={threshold_used:.4f}, decision={decision_str}, reason_code={reason_code}"
        )

        return decision

    def _check_pre_trade_internal(
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
        is_live_mode: bool = False,
        **kwargs
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
        
        # Gate 3: Strategy confidence — Dynamic Adaptive Threshold
        # Check if we should apply fallback soft threshold (daily-activity mode)
        is_fallback_mode = kwargs.get('is_fallback_mode', False)
        use_soft_threshold = kwargs.get('use_soft_threshold', False) or is_fallback_mode
        strategy_name = kwargs.get('strategy_name', None)

        # --- Step 1: Logic check removed ---
        # The strategy (MasterEnsemble) already records the confidence sample.
        # Recording it again here in Layer A causes double-weighting and ceiling creep.

        # --- Step 2: Compute dynamic threshold ---
        if use_soft_threshold:
            # Daily-activity fallback mode: honour the configured soft (low-bar) threshold.
            threshold = self.config.min_confidence_soft
            threshold_type = "soft"
            dyn_result = None
        else:
            dyn_result = self._dyn_threshold.get_threshold(symbol)
            threshold = dyn_result.threshold
            threshold_type = dyn_result.threshold_type
            # Critical: during cold start the module fallback (0.5) may be HIGHER
            # than the actual configured hard threshold. Always cap the threshold at
            # config.min_confidence_hard so cold start is never MORE restrictive.
            if dyn_result.threshold_type == "fallback_static":
                threshold = min(threshold, self.config.min_confidence_hard)
                threshold_type = "fallback_hard_config"

        # --- Step 3: Log confidence check details ---
        logger.debug(
            f"Confidence Check [{symbol}] - "
            f"Raw: {confidence:.4f} ({confidence:.2%}), "
            f"Threshold: {threshold:.4f} ({threshold:.2%}) [{threshold_type}], "
            f"Strategy: {strategy_name or 'unknown'}"
        )

        # --- Step 4: Compare and decide ---
        decision_label = "PASS" if confidence >= threshold else "BLOCK"

        # Emit structured JSON debug log + human-readable INFO line
        if dyn_result is not None:
            log_threshold_event(symbol, confidence, dyn_result, decision_label)

        if confidence < threshold:
            block_msg = (
                f"Strategy confidence {confidence:.4f} ({confidence:.2%}) below "
                f"{threshold_type} threshold {threshold:.4f} ({threshold:.2%})"
            )
            signal_dir = kwargs.get('signal', '?')
            print(f"BLOCKED [{symbol} | Signal.{signal_dir}] - {block_msg} - Reason: {BlockReason.LOW_CONFIDENCE.value}")

            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.LOW_CONFIDENCE,
                message=block_msg,
                layer="A",
                details={
                    "raw_confidence": confidence,
                    "threshold": threshold,
                    "threshold_type": threshold_type,
                    "strategy_name": strategy_name,
                    "is_fallback_mode": is_fallback_mode,
                    "samples_available": dyn_result.samples_available if dyn_result else None,
                }
            )

        print(
            f"PASSED_LAYER_A [{symbol}] - Confidence {confidence:.2%} meets {threshold_type} threshold {threshold:.2%} "
            f"(Layer A passed — sizing, risk, and execution gates still apply)"
        )
        
        # Gate 4: Market conditions (Volatility Spike Entry Gate & Spread Gate)
        if market_conditions:
            # 🚀 Phase 3: Spread Gate (Execution Cost Realism)
            # If the bid/ask spread is > 10 bps, the hidden cost is too high for entry.
            if market_conditions.spread_pct > 0.001:
                 return SafetyDecision(
                    allowed=False,
                    block_reason=BlockReason.MARKET_CONDITIONS,
                    message=f"Spread Gate: Current spread {market_conditions.spread_pct*100:.3f}% is greater than max allowed 0.1%. Entry suspended to prevent excessive slippage.",
                    layer="A",
                    details={"spread_pct": market_conditions.spread_pct, "max_allowable": 0.001}
                )

            if not market_conditions.is_safe:
                return SafetyDecision(
                    allowed=False,
                    block_reason=BlockReason.MARKET_CONDITIONS,
                    message=f"Unsafe market conditions: {market_conditions.reason}",
                    layer="A",
                    details={"spread_pct": market_conditions.spread_pct, "volatility": market_conditions.volatility}
                )
                
            # Volatility Spike Entry Gate
            # Check if current volatility (e.g. 5m ATR) is > 3x normal
            base_atr = self.config.volatility_base_atr.get(symbol, 0.0)
            if base_atr > 0 and market_conditions.volatility > (base_atr * self.config.max_volatility_multiplier):
                 return SafetyDecision(
                    allowed=False,
                    block_reason=BlockReason.MARKET_CONDITIONS,
                    message=f"Volatility Spike Gate: Current volatility {market_conditions.volatility:.4f} is > {self.config.max_volatility_multiplier}x base ({base_atr:.4f}). Entries suspended.",
                    layer="A",
                    details={"volatility": market_conditions.volatility, "base_atr": base_atr, "max_multiplier": self.config.max_volatility_multiplier}
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
        return SafetyDecision(
            allowed=True,
            message="All pre-trade gates passed",
            layer="A",
            details={
                "threshold": threshold,
                "threshold_type": threshold_type,
                "confidence": confidence,
                "is_fallback_mode": is_fallback_mode,
                "samples_available": dyn_result.samples_available if dyn_result else None,
            }
        )
    
    # =========================================================================
    # LAYER B: SIZING CONTROLS (Exposure Reduction)
    # =========================================================================
    
    def calculate_safe_size(
        self,
        config_id: str,
        symbol: str,
        sizing_result: SizingResult,
        entry_price: float,
        current_equity: float,
        margin_used: float,
        margin_available: float,
        leverage: float = 1.0,
        atr: Optional[float] = None,
        total_notional_exposure: float = 0.0,
        contract_size: float = 1.0, # <-- NEW
        **kwargs
    ) -> SafetyDecision:
        """
        Layer B: Calculate safe position size with all controls applied.
        Includes Compensating Controls: Higher leverage -> Lower max exposure.
        """
        base_size = sizing_result.quantity
        adjusted_size = base_size
        reductions = []

        # ── Strict user sizing mode (Option A) ────────────────────────────────
        # All soft heuristic reductions below are disabled.
        # The user's allocation is authoritative; Layer B only validates hard
        # affordability (does the account have enough free margin?) and rejects
        # or passes through.  All commented-out blocks are preserved for future
        # re-activation if Option A is reverted.
        # ─────────────────────────────────────────────────────────────────────

        # Notional = Qty * Price * ContractSize
        c_size = contract_size if contract_size > 0 else 1.0
        # If contract_size != 1.0 (e.g., forex 100000), we must account for it
        notional = sizing_result.notional * c_size if c_size != 1.0 else sizing_result.notional

        # DISABLED (Option A) — Compensating Control: Leverage Penalty.
        # Higher leverage no longer automatically reduces the approved size.
        # if leverage > 10.0:
        #     penalty_factor = 0.8
        #     if leverage > 20.0:
        #         penalty_factor = 0.6
        #     penalty_size = adjusted_size * penalty_factor
        #     if penalty_size < adjusted_size:
        #         reductions.append(f"Compensating Control: High Leverage ({leverage}x) -> Size reduced by {1-penalty_factor:.0%}")
        #         adjusted_size = penalty_size

        # Hard affordability check: reject if the account literally cannot fund this trade.
        # This is NOT a heuristic reduction — it is a hard binary gate.
        # "Cannot fund" = margin_available < required margin for this notional.
        required_margin = notional / max(1.0, leverage)
        if margin_available > 0 and required_margin > margin_available:
            # Account cannot afford this trade at all — reject cleanly.
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.MARGIN_BUFFER,
                message=(
                    f"Insufficient margin: need {required_margin:.2f} USDT, "
                    f"available {margin_available:.2f} USDT"
                ),
                layer="B",
                original_size=base_size,
                adjusted_size=0.0,
                size_reduction_pct=1.0,
                details={"reductions": ["hard_affordability_block"], "required_margin": required_margin, "margin_available": margin_available}
            )

        # DISABLED (Option A) — Margin buffer soft reduction.
        # Previously shrunk size to keep 15% free margin; now replaced by hard reject above.
        # required_margin_buffer = current_equity * self.config.min_margin_buffer_pct
        # available_after_trade = margin_available - (notional / max(1.0, leverage))
        # if margin_available > required_margin_buffer and available_after_trade < required_margin_buffer:
        #     max_size_for_buffer = max(0, (margin_available - required_margin_buffer) * leverage / entry_price)
        #     if max_size_for_buffer < adjusted_size:
        #         adjusted_size = max_size_for_buffer
        #         reductions.append(f"Margin buffer: {base_size:.4f} -> {adjusted_size:.4f}")

        # DISABLED (Option A) — Total exposure cap.
        # Fixed allocations already skip this; non-fixed modes now also skip under Option A.
        # is_fixed_allocation = sizing_result.details.get("sizing_method") == "fixed_amount_strict"
        # if not is_fixed_allocation:
        #     max_total_exposure = current_equity * self.config.max_total_exposure_mult
        #     if leverage > 15.0:
        #         max_total_exposure *= 0.75
        #     available_exposure = max_total_exposure - total_notional_exposure
        #     if available_exposure < (adjusted_size * entry_price):
        #         adjusted_size = max(0, available_exposure / entry_price)
        #         reductions.append(f"Total exposure: reduced to fit {available_exposure:.2f} USDT budget")

        # DISABLED (Option A) — Volatility scaling.
        # ATR spike no longer reduces position size automatically.
        # if self.config.volatility_scaling_enabled and atr:
        #     base_atr = self.config.volatility_base_atr.get(symbol, atr)
        #     if base_atr > 0:
        #         volatility_ratio = atr / base_atr
        #         if volatility_ratio > 1.5:
        #             scale_factor = 1.0 / volatility_ratio
        #             scaled_size = adjusted_size * scale_factor
        #             reductions.append(f"Volatility scaling: {volatility_ratio:.2f}x normal → {scale_factor:.2%} size")
        #             adjusted_size = scaled_size

        # Calculate reduction percentage (will be 0.0 under Option A unless affordability block fired)
        size_reduction_pct = 0.0
        if base_size > 0:
            size_reduction_pct = (base_size - adjusted_size) / base_size

        return SafetyDecision(
            allowed=True,
            message="No adjustments needed" if not reductions else f"Size controls applied: {len(reductions)} adjustments",
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
        
        # Dynamic Enforcement via Policy (Priority)
        final_stop_distance_pct = stop_distance_pct
        clamped_info = None

        if self.risk_policy:
            # Enforce policy (Clamping)
            enforcement = self.risk_policy.enforce(stop_distance_pct)
            
            if enforcement.is_clamped:
                final_stop_distance_pct = enforcement.adjusted_stop_loss_pct
                clamped_info = enforcement
            
            # If policy explicitly blocks (e.g. hard cap violation), return immediately
            if not enforcement.allowed:
                 return SafetyDecision(
                    allowed=False,
                    block_reason=BlockReason.LEVERAGE_RISK,
                    message=enforcement.reason,
                    layer="C",
                    details=enforcement.details
                )
        
        # Check 1: Max stop distance (using FINAL/CLAMPED distance)
        # Note: If we clamped it, it should pass this check if the policy and config are aligned.
        if final_stop_distance_pct > self.config.max_stop_distance_pct + 0.0001: # Epsilon
            return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.STOP_TOO_WIDE,
                message=f"Stop-loss distance {final_stop_distance_pct:.2%} exceeds maximum {self.config.max_stop_distance_pct:.2%}",
                layer="C",
                details={"entry": entry_price, "stop": stop_loss_price, "distance_pct": final_stop_distance_pct}
            )
        
        # Check 2: Compound risk (stop * leverage)
        compound_risk_pct = final_stop_distance_pct * leverage
        
        if compound_risk_pct > self.config.max_compound_risk_pct + 0.0001: # Epsilon
             return SafetyDecision(
                allowed=False,
                block_reason=BlockReason.LEVERAGE_RISK,
                message=f"Compound risk {compound_risk_pct:.2%} (stop {final_stop_distance_pct:.2%} × {leverage}x leverage) exceeds maximum {self.config.max_compound_risk_pct:.2%}",
                layer="C",
                details={"stop_distance_pct": final_stop_distance_pct, "leverage": leverage, "compound_risk_pct": compound_risk_pct}
            )
            
        # Check 3: Maintenance Margin Liquidation Defense
        # Moved to sizing phase (executor.py) to allow DYNAMIC LEVERAGE REDUCTION
        # rather than just binary blocking, and to incorporate exact exchange parameters 
        # (MMR + Fees + Funding buffer).
        
        # Success (possibly clamped)
        details = {"stop_distance_pct": final_stop_distance_pct, "compound_risk_pct": compound_risk_pct}
        if clamped_info:
            details["clamped"] = True
            details["clamp_reason"] = clamped_info.reason
            message = clamped_info.reason
        else:
            message = "Protective orders validated"
            
        return SafetyDecision(
            allowed=True,
            message=message,
            layer="C",
            details=details
        )
    
    # =========================================================================
    # LAYER D: POST-TRADE MONITORING (Circuit Breakers)
    # =========================================================================
    
    def record_order_result(
        self,
        config_id: str,
        symbol: str,
        success: bool,
        error_message: Optional[str] = None,
        regime: Optional[str] = None
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
                # Security Gate: Don't count "Insufficient margin" as a system failure.
                # Account balance issues shouldn't trigger a symbol-wide circuit breaker.
                if error_message and ("Insufficient margin" in error_message or "-2019" in error_message):
                    return

                # Increment failure counter
                row = conn.execute(
                    "SELECT consecutive_failures FROM order_failures WHERE config_id = ? AND symbol = ?",
                    (config_id, symbol)
                ).fetchone()
                
                failures = (row["consecutive_failures"] if row else 0) + 1
                
                # Dynamic failure threshold based on regime
                max_failures = self.config.max_order_failures
                if regime == "HIGH_VOLATILITY":
                    max_failures = 5 # halved sensitivity in high vol
                
                # Trigger circuit breaker if threshold reached (exponential backoff, cap=3min)
                paused_until = None
                if failures >= max_failures:
                    extra = failures - max_failures  # 0 on first trigger
                    dynamic_cooldown = min(
                        3,  # hard cap: never more than 3 minutes
                        self.config.circuit_breaker_cooldown_minutes * (2 ** extra)
                    )
                    cooldown_dt = datetime.now(timezone.utc) + timedelta(minutes=dynamic_cooldown)
                    paused_until = cooldown_dt.isoformat()
                    logger.error(
                        f"CIRCUIT BREAKER TRIGGERED: {symbol} paused until {paused_until} "
                        f"after {failures} consecutive failures (cooldown={dynamic_cooldown:.1f}min)"
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
    
    # =========================================================================
    # DAILY ACTIVITY FALLBACK MECHANISM
    # =========================================================================
    
    def check_daily_activity_fallback(
        self,
        config_id: str
    ) -> Dict[str, Any]:
        """
        Check if daily activity fallback should be activated.
        
        Returns:
            Dictionary with:
            - should_activate: bool
            - hours_since_last_trade: float
            - last_trade_timestamp: Optional[str]
            - reason: str
        """
        if not self.config.daily_activity_fallback_enabled:
            return {
                "should_activate": False,
                "hours_since_last_trade": 0,
                "last_trade_timestamp": None,
                "reason": "Daily activity fallback is disabled"
            }
        
        now = datetime.now(timezone.utc)
        
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT last_trade_timestamp FROM daily_activity_tracking WHERE config_id = ?",
                (config_id,)
            ).fetchone()
            
            if not row or not row["last_trade_timestamp"]:
                # No trades recorded yet — this is a fresh bot, NOT an inactive one.
                # Do NOT activate fallback; let the strategy trade normally on first run.
                logger.debug(f"Daily activity fallback: No trades yet for config {config_id} — treating as fresh start (fallback NOT activated)")
                return {
                    "should_activate": False,
                    "hours_since_last_trade": 0,
                    "last_trade_timestamp": None,
                    "reason": "No trades recorded yet (fresh bot — fallback disabled)"
                }
            
            last_trade_dt = datetime.fromisoformat(row["last_trade_timestamp"])
            hours_since = (now - last_trade_dt).total_seconds() / 3600
            
            if hours_since >= self.config.daily_activity_fallback_hours:
                logger.info(
                    f"Daily activity fallback: {hours_since:.1f}h since last trade "
                    f"(threshold: {self.config.daily_activity_fallback_hours}h)"
                )
                return {
                    "should_activate": True,
                    "hours_since_last_trade": hours_since,
                    "last_trade_timestamp": row["last_trade_timestamp"],
                    "reason": f"No trades for {hours_since:.1f} hours (threshold: {self.config.daily_activity_fallback_hours}h)"
                }
            
            return {
                "should_activate": False,
                "hours_since_last_trade": hours_since,
                "last_trade_timestamp": row["last_trade_timestamp"],
                "reason": f"Last trade was {hours_since:.1f}h ago (threshold not reached)"
            }
    
    def update_trade_timestamp(
        self,
        config_id: str,
        is_fallback_trade: bool = False
    ):
        """
        Update the last trade timestamp for daily activity tracking.
        
        Args:
            config_id: Configuration ID
            is_fallback_trade: Whether this was a fallback trade
        """
        now = utc_now_iso()
        
        with self.db.connect() as conn:
            # Check if record exists
            row = conn.execute(
                "SELECT total_trades FROM daily_activity_tracking WHERE config_id = ?",
                (config_id,)
            ).fetchone()
            
            if row:
                # Update existing record
                if is_fallback_trade:
                    conn.execute(
                        """
                        UPDATE daily_activity_tracking 
                        SET last_trade_timestamp = ?,
                            last_fallback_trade_timestamp = ?,
                            total_trades = total_trades + 1,
                            total_fallback_trades = total_fallback_trades + 1
                        WHERE config_id = ?
                        """,
                        (now, now, config_id)
                    )
                    logger.info(f"✅ DAILY_ACTIVITY_FALLBACK_USED for config {config_id}")
                else:
                    conn.execute(
                        """
                        UPDATE daily_activity_tracking 
                        SET last_trade_timestamp = ?,
                            total_trades = total_trades + 1
                        WHERE config_id = ?
                        """,
                        (now, config_id)
                    )
            else:
                # Insert new record
                conn.execute(
                    """
                    INSERT INTO daily_activity_tracking 
                    (config_id, last_trade_timestamp, last_fallback_trade_timestamp, total_trades, total_fallback_trades)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (config_id, now, now if is_fallback_trade else None, 1 if is_fallback_trade else 0)
                )
                if is_fallback_trade:
                    logger.info(f"✅ DAILY_ACTIVITY_FALLBACK_USED for config {config_id} (first trade)")

    def _db_write_with_retry(self, operation: callable, max_retries: int = 3):
        """Helper to retry sqlite3 database operations on locked errors."""
        import sqlite3
        import time
        last_exception = None
        for i in range(max_retries):
            try:
                return operation()
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and i < max_retries - 1:
                    last_exception = e
                    time.sleep(0.1 * (2 ** i))
                    continue
                raise
        if last_exception:
            raise last_exception

    def reset_symbol_circuit_breaker(self, config_id: str, symbol: str | None = None) -> dict:
        """
        Reset consecutive failures for a specific symbol or all symbols for a config.
        Called interactively from API to restore trading after a circuit breaker pause.
        
        Args:
            config_id: Configuration ID to reset for
            symbol: Specific symbol to reset, or None for all symbols under config
            
        Returns:
            Dict of {"config_id:symbol": {"old_failures": X, "new_failures": 0, "old_paused_until": Y}}
        """
        details = {}
        
        def _execute_reset():
            with self.db.connect() as conn:
                if symbol:
                    # Get old state
                    row = conn.execute(
                        "SELECT consecutive_failures, paused_until FROM order_failures WHERE config_id = ? AND symbol = ?",
                        (config_id, symbol)
                    ).fetchone()
                    
                    if row:
                        details[f"{config_id}:{symbol}"] = {
                            "old_failures": row["consecutive_failures"],
                            "old_paused_until": row["paused_until"],
                            "new_failures": 0
                        }
                        
                    # Reset specific
                    conn.execute(
                        "UPDATE order_failures SET consecutive_failures = 0, paused_until = NULL, last_failure_reason = NULL WHERE config_id = ? AND symbol = ?",
                        (config_id, symbol)
                    )
                else:
                    # Get old state for all
                    rows = conn.execute(
                        "SELECT symbol, consecutive_failures, paused_until FROM order_failures WHERE config_id = ?",
                        (config_id,)
                    ).fetchall()
                    
                    for row in rows:
                        sym = row["symbol"]
                        details[f"{config_id}:{sym}"] = {
                            "old_failures": row["consecutive_failures"],
                            "old_paused_until": row["paused_until"],
                            "new_failures": 0
                        }
                    
                    # Reset all for config
                    conn.execute(
                        "UPDATE order_failures SET consecutive_failures = 0, paused_until = NULL, last_failure_reason = NULL WHERE config_id = ?",
                        (config_id,)
                    )
            return details
            
        return self._db_write_with_retry(_execute_reset)
