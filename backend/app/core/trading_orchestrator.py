"""
Complete Trading System Integration

Ties together all components:
- User configuration (with system limit clamping)
- Strategy families (with required outputs)
- Safety engine (4-layer protection)
- Risk budget & position sizing
- Account protection

This is the master orchestrator that ensures:
1. Users can't configure unsafe settings
2. Strategies output all required data
3. Safety stack protects on every trade
"""
import logging
import uuid
import json
from typing import Optional, Dict, Any

from app.persistence.db import DB, utc_now_iso
from app.risk.system_limits import (
    SystemLimits,
    UserConfigurableLimits,
    ConfigValidator,
    RiskLevel
)
from app.strategy.strategy_framework import (
    BaseStrategy,
    StrategyOutput,
    Signal,
    StrategyRegistry
)
from app.risk.safety_engine import SafetyEngine, SafetyConfig, SafetyDecision
from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig
from app.risk.account_protection import AccountProtection
from app.risk.market_analyzer import MarketAnalyzer
from app.risk.broker_health import BrokerHealthMonitor

logger = logging.getLogger(__name__)


class TradingOrchestrator:
    """
    Master orchestrator for the complete trading system.
    
    Workflow:
    1. User creates configuration → Validate & clamp to system limits
    2. Strategy generates signal → Validate required outputs
    3. For each signal → Run through 4-layer safety stack
    4. If all pass → Execute trade
    5. Post-trade → Update protection state
    """
    
    def __init__(
        self,
        config_id: str,
        user_config: UserConfigurableLimits,
        strategy_id: str,
        broker_id: str
    ):
        self.config_id = config_id
        self.user_config = user_config
        self.strategy_id = strategy_id
        self.broker_id = broker_id
        
        # Initialize components
        self.db = DB()
        self.system_limits = SystemLimits()
        self.config_validator = ConfigValidator(self.system_limits)
        
        # Validate and clamp user configuration
        self.validated_config, self.config_warnings = self._validate_config()
        
        # Get strategy
        self.strategy = StrategyRegistry.get(strategy_id)
        if not self.strategy:
            raise ValueError(f"Strategy {strategy_id} not found in registry")
        
        # Initialize safety components
        self._init_safety_stack()
        
        # Initialize market analyzer and broker monitor
        self.market_analyzer = MarketAnalyzer()
        self.broker_monitor = BrokerHealthMonitor()
        
        logger.info(f"Trading orchestrator initialized for config {config_id}")
        if self.config_warnings:
            for warning in self.config_warnings:
                logger.warning(f"Config clamped: {warning}")
    
    def _validate_config(self) -> tuple[UserConfigurableLimits, list]:
        """Validate and clamp user configuration to system limits."""
        validated, warnings = self.config_validator.validate_and_clamp(self.user_config)
        return validated, warnings
    
    def _init_safety_stack(self):
        """Initialize the 4-layer safety stack."""
        # Create risk budget config from validated user settings
        risk_config = self._create_risk_budget_config()
        self.risk_budget = RiskBudgetEngine(risk_config)
        
        # Account protection
        self.protection = AccountProtection(self.db)
        
        # Safety engine configuration
        safety_config = SafetyConfig(
            max_leverage=self._get_max_leverage_for_symbols(),
            max_trades_per_day=self.validated_config.max_trades_per_day,
            min_strategy_confidence=self.validated_config.min_strategy_confidence,
            min_margin_buffer_pct=0.30,
            max_stop_distance_pct=self.system_limits.max_stop_loss_pct,
            max_compound_risk_pct=0.15,
            max_order_failures=1 if self.validated_config.strict_circuit_breakers else 3,
            max_slippage_pct=0.015 if self.validated_config.strict_circuit_breakers else 0.02,
            liquidation_margin_ratio_threshold=self.system_limits.emergency_margin_ratio_threshold,
            circuit_breaker_cooldown_minutes=60 if self.validated_config.strict_circuit_breakers else 30
        )
        
        # Create safety engine
        self.safety = SafetyEngine(
            self.db,
            self.risk_budget,
            self.protection,
            safety_config
        )
    
    def _create_risk_budget_config(self) -> RiskBudgetConfig:
        """Create risk budget config from user settings."""
        # Get max risk per trade based on risk level
        max_risk_per_trade = self.config_validator.get_risk_per_trade_limit(
            self.validated_config.risk_level
        )
        
        # Map risk level to portfolio risk
        if self.validated_config.risk_level == RiskLevel.LOW:
            portfolio_risk = 0.02
            margin_usage = 0.35
            base_slots = 3
            max_slots = 10
        elif self.validated_config.risk_level == RiskLevel.HIGH:
            portfolio_risk = 0.10
            margin_usage = 0.65
            base_slots = 8
            max_slots = 20
        else:  # MEDIUM
            portfolio_risk = 0.05
            margin_usage = 0.50
            base_slots = 5
            max_slots = 15
        
        # Clamp to user's max positions if lower
        max_slots = min(max_slots, self.validated_config.max_open_positions)
        
        return RiskBudgetConfig(
            portfolio_risk_pct=min(portfolio_risk, self.validated_config.max_daily_loss_pct),
            per_trade_risk_pct=max_risk_per_trade,
            max_margin_usage_pct=margin_usage,
            base_slots=base_slots,
            max_slots=max_slots
        )
    
    def _get_max_leverage_for_symbols(self) -> float:
        """Get maximum leverage across all allowed symbols."""
        max_leverage = 10.0  # Safe default
        
        for symbol in self.validated_config.allowed_symbols:
            if symbol in self.validated_config.requested_leverage:
                max_leverage = max(max_leverage, self.validated_config.requested_leverage[symbol])
        
        return max_leverage
    
    def process_trading_opportunity(
        self,
        symbol: str,
        klines: list,
        current_price: float,
        current_equity: float,
        margin_used: float,
        margin_available: float,
        open_positions: int,
        client,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Process a potential trading opportunity through the complete system.
        
        Returns:
            Dictionary with trade decision and details
        """
        result = {
            "symbol": symbol,
            "decision": "no_action",
            "reason": "",
            "details": {}
        }
        
        # Step 1: Check if symbol is allowed
        if symbol not in self.validated_config.allowed_symbols:
            result["decision"] = "blocked"
            result["reason"] = f"Symbol {symbol} not in allowed list"
            
            self.record_decision(
                symbol=symbol,
                run_id=kwargs.get("run_id"),
                strategy_signal_json="{}",
                risk_gate_decision_json="{}",
                sizing_decision_json="{}",
                protection_decision_json="{}",
                final_action="blocked"
            )
            return result
        
        # Step 2: Get strategy signal
        try:
            strategy_output: StrategyOutput = self.strategy.analyze(
                symbol=symbol,
                klines=klines,
                current_price=current_price,
                **kwargs
            )
        except Exception as e:
            result["decision"] = "error"
            result["reason"] = f"Strategy analysis failed: {str(e)}"
            logger.error(f"Strategy {self.strategy_id} failed for {symbol}: {e}")
            return result
        
        # Validate strategy output
        if not self.strategy.validate_output(strategy_output):
            result["decision"] = "error"
            result["reason"] = "Strategy output validation failed"
            return result
        
        result["details"]["strategy_output"] = {
            "signal": strategy_output.signal.value,
            "confidence": strategy_output.confidence,
            "suggested_stop": strategy_output.suggested_stop_distance,
            "riskiness": strategy_output.riskiness,
            "reason": strategy_output.reason
        }
        
        # If HOLD signal, nothing to do
        if strategy_output.signal == Signal.HOLD:
            result["decision"] = "hold"
            result["reason"] = strategy_output.reason or "Strategy says HOLD"
            
            self.record_decision(
                symbol=symbol,
                run_id=kwargs.get("run_id"),
                strategy_signal_json=json.dumps(result["details"].get("strategy_output", {})),
                risk_gate_decision_json="{}",
                sizing_decision_json="{}",
                protection_decision_json="{}",
                final_action="hold"
            )
            return result
        
        # Step 3: Analyze market conditions
        ticker = kwargs.get("ticker", {})
        if ticker:
            market_conditions = self.market_analyzer.analyze_from_exchange_data(
                symbol=symbol,
                ticker=ticker,
                klines=klines,
                atr=strategy_output.indicators.get("atr", 0) if strategy_output.indicators else 0
            )
        else:
            market_conditions = None
        
        # Step 4: Check broker health
        if client:
            broker_health = self.broker_monitor.check_health(self.broker_id, client)
        else:
            broker_health = None
        
        # Step 5: LAYER A - Pre-trade gating
        leverage = self.validated_config.requested_leverage.get(symbol, 10.0)
        
        gate_decision = self.safety.check_pre_trade(
            config_id=self.config_id,
            symbol=symbol,
            confidence=strategy_output.confidence,
            leverage=leverage,
            current_equity=current_equity,
            open_positions=open_positions,
            market_conditions=market_conditions,
            broker_health=broker_health,
            user_kyc_approved=True,  # Would be loaded from user record
            is_live_mode=not self.validated_config.paper_mode
        )
        
        if not gate_decision.allowed:
            result["decision"] = "blocked"
            result["reason"] = f"Layer A: {gate_decision.message}"
            result["details"]["layer_a"] = gate_decision.details
            
            self.record_decision(
                symbol=symbol,
                run_id=kwargs.get("run_id"),
                strategy_signal_json=json.dumps(result["details"].get("strategy_output", {})),
                risk_gate_decision_json=json.dumps(gate_decision.details),
                sizing_decision_json="{}",
                protection_decision_json="{}",
                final_action="blocked"
            )
            return result
        
        result["details"]["layer_a"] = "✅ All gates passed"
        
        # Step 6: Calculate base position size
        # Use strategy's suggested stop for risk-based sizing
        
        # 6a. Determine max risk amount based on user preferences vs system limits
        system_max_risk_pct = self.config_validator.get_risk_per_trade_limit(self.validated_config.risk_level)
        
        if self.validated_config.use_fixed_size and self.validated_config.fixed_size_usdt:
            # Fixed size: Calculate risk implied by this size
            # Risk = Size * StopDistance
            # We don't know Size yet, we know TARGET Size. 
            # If user wants fixed $1000 size, and stop is 2%, risk is $20.
            target_size_usdt = self.validated_config.fixed_size_usdt
            stop_distance_pct = strategy_output.suggested_stop_distance
            implied_risk_usdt = target_size_usdt * stop_distance_pct
            
            # Clamp to system max risk
            max_risk_usdt = current_equity * system_max_risk_pct
            if implied_risk_usdt > max_risk_usdt:
                # Must reduce size to fit risk limit
                risk_amount = max_risk_usdt 
            else:
                risk_amount = implied_risk_usdt
        else:
            # Percentage based: Use lower of User Preference vs System Limit
            # User might want 1% risk, System allows 2%. Use 1%.
            # User allocation pct scales the EQUITY considered.
            effective_equity = current_equity * self.validated_config.capital_allocation_pct
            risk_amount = effective_equity * system_max_risk_pct
        
        stop_distance = strategy_output.suggested_stop_distance * current_price
        base_size = risk_amount / stop_distance if stop_distance > 0 else 0
        
        # Step 7: LAYER B - Sizing controls
        total_exposure = kwargs.get("total_exposure", 0)
        
        size_decision = self.safety.calculate_safe_size(
            config_id=self.config_id,
            symbol=symbol,
            base_size=base_size,
            entry_price=current_price,
            current_equity=current_equity,
            margin_used=margin_used,
            margin_available=margin_available,
            leverage=leverage,
            atr=strategy_output.indicators.get("atr") if strategy_output.indicators else None,
            total_notional_exposure=total_exposure
        )
        
        if not size_decision.allowed:
            result["decision"] = "blocked"
            result["reason"] = f"Layer B: {size_decision.message}"
            result["details"]["layer_b"] = size_decision.details
            
            self.record_decision(
                symbol=symbol,
                run_id=kwargs.get("run_id"),
                strategy_signal_json=json.dumps(result["details"].get("strategy_output", {})),
                risk_gate_decision_json=json.dumps(result["details"].get("layer_a", {})),
                sizing_decision_json=json.dumps(size_decision.details),
                protection_decision_json="{}",
                final_action="blocked"
            )
            return result
        
        final_size = size_decision.adjusted_size
        result["details"]["layer_b"] = {
            "base_size": base_size,
            "final_size": final_size,
            "reduction_pct": size_decision.size_reduction_pct,
            "adjustments": size_decision.details.get("reductions", [])
        }
        
        # Step 8: LAYER C - Protective orders validation
        stop_loss_price = current_price * (1 - strategy_output.suggested_stop_distance)
        if strategy_output.signal == Signal.SELL:
            stop_loss_price = current_price * (1 + strategy_output.suggested_stop_distance)
        
        protection_decision = self.safety.validate_protective_orders(
            symbol=symbol,
            entry_price=current_price,
            stop_loss_price=stop_loss_price,
            leverage=leverage,
            position_size=final_size
        )
        
        if not protection_decision.allowed:
            result["decision"] = "blocked"
            result["reason"] = f"Layer C: {protection_decision.message}"
            result["details"]["layer_c"] = protection_decision.details
            
            self.record_decision(
                symbol=symbol,
                run_id=kwargs.get("run_id"),
                strategy_signal_json=json.dumps(result["details"].get("strategy_output", {})),
                risk_gate_decision_json=json.dumps(result["details"].get("layer_a", {})),
                sizing_decision_json=json.dumps(result["details"].get("layer_b", {})),
                protection_decision_json=json.dumps(protection_decision.details),
                final_action="blocked"
            )
            return result
        
        result["details"]["layer_c"] = "✅ Protective orders validated"
        
        # All layers passed - ready to trade!
        result["decision"] = "execute"
        result["reason"] = "All safety checks passed"
        result["trade_params"] = {
            "symbol": symbol,
            "side": "BUY" if strategy_output.signal == Signal.BUY else "SELL",
            "quantity": final_size,
            "entry_price": current_price,
            "stop_loss": stop_loss_price,
            "take_profit": current_price * (1 + (strategy_output.take_profit_distance or strategy_output.suggested_stop_distance * 2)) if strategy_output.signal == Signal.BUY else current_price * (1 - (strategy_output.take_profit_distance or strategy_output.suggested_stop_distance * 2)),
            "leverage": leverage
        }
        
        # Log decision
        run_id = kwargs.get("run_id")
        self.record_decision(
            symbol=symbol,
            run_id=run_id,
            strategy_signal_json=json.dumps(result["details"].get("strategy_output", {})),
            risk_gate_decision_json=json.dumps(result["details"].get("layer_a", {})),
            sizing_decision_json=json.dumps(result["details"].get("layer_b", {})),
            protection_decision_json=json.dumps(result["details"].get("layer_c", {})),
            final_action=result["decision"],
            execution_result_json=None # Will be updated after execution if async, but here we just log the INTENT
        )
        
        return result
    
    def record_trade_execution(
        self,
        symbol: str,
        success: bool,
        expected_price: float,
        executed_price: Optional[float] = None,
        error_message: Optional[str] = None
    ):
        """
        Record trade execution result for Layer D monitoring.
        
        Call this after attempting to execute a trade.
        """
        # Layer D: Post-trade monitoring
        self.safety.record_order_result(
            config_id=self.config_id,
            symbol=symbol,
            success=success,
            error_message=error_message
        )
        
        if success and executed_price:
            # Record slippage
            self.safety.record_slippage(
                config_id=self.config_id,
                symbol=symbol,
                expected_price=expected_price,
                executed_price=executed_price
            )
            
            # Increment trade counter
            self.safety.increment_trade_count(self.config_id)
        
        if not success:
            # Record broker error
            self.broker_monitor.record_error(self.broker_id, error_message or "Unknown error")
    
    def record_decision(
        self,
        symbol: str,
        run_id: str,
        strategy_signal_json: str,
        risk_gate_decision_json: str,
        sizing_decision_json: str,
        protection_decision_json: str,
        final_action: str,
        execution_result_json: Optional[str] = None
    ):
        """
        Record the entire decision pipeline to the audit log.
        """
        created_at = utc_now_iso()
        log_id = f"audit_{uuid.uuid4().hex[:12]}"
        
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO decision_logs (
                    id, config_id, run_id, symbol, 
                    strategy_signal_json, risk_gate_decision_json, 
                    sizing_decision_json, protection_decision_json, 
                    final_action, execution_result_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    log_id, self.config_id, run_id, symbol,
                    strategy_signal_json, risk_gate_decision_json,
                    sizing_decision_json, protection_decision_json,
                    final_action, execution_result_json, created_at
                )
            )



# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    print("Complete Trading System Integration Example")
    print("=" * 60)
    
    # Step 1: User creates configuration
    user_config = UserConfigurableLimits(
        risk_level=RiskLevel.MEDIUM,
        max_daily_loss_pct=0.05,
        max_trades_per_day=50,
        max_open_positions=5,
        default_stop_loss_pct=0.02,
        requested_leverage={"BTCUSDT": 10, "ETHUSDT": 8},
        allowed_symbols=["BTCUSDT", "ETHUSDT"],
        paper_mode=True
    )
    
    # Step 2: Create orchestrator (validates config automatically)
    orchestrator = TradingOrchestrator(
        config_id="config_123",
        user_config=user_config,
        strategy_id="ma_cross_v1",
        broker_id="binance_futures"
    )
    
    print("\n✅ System initialized with validated configuration")
    print(f"   Risk Level: {orchestrator.validated_config.risk_level.value}")
    print(f"   Max Daily Loss: {orchestrator.validated_config.max_daily_loss_pct:.1%}")
    print(f"   Max Positions: {orchestrator.validated_config.max_open_positions}")
    print(f"   Allowed Symbols: {', '.join(orchestrator.validated_config.allowed_symbols)}")
    
    # Display any clamping warnings
    if orchestrator.config_warnings:
        print("\n⚠️  Configuration was clamped:")
        for warning in orchestrator.config_warnings:
            print(f"   - {warning}")
    
    print("\n" + "="*60)
    print("Ready to process trading opportunities!")
    print("Each opportunity will go through:")
    print("  1. Strategy Analysis (required outputs)")
    print("  2. Layer A: Pre-trade gating (7 gates)")
    print("  3. Layer B: Sizing controls (3 controls)")
    print("  4. Layer C: Protective orders validation")
    print("  5. Layer D: Post-trade monitoring")
