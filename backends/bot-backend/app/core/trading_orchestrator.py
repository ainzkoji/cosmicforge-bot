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
import time
import uuid
import json
from typing import Optional, Dict, Any

from shared_lib.persistence.db import DB, utc_now_iso
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
    StrategyRegistry,
    StrategyFamily
)
from app.risk.safety_engine import SafetyEngine, SafetyConfig, SafetyDecision
from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig
from app.risk.account_protection import AccountProtection
from app.risk.market_analyzer import MarketAnalyzer
from app.risk.broker_health import BrokerHealthMonitor
from app.risk.risk_policy import RiskPolicy
from app.policy.policy_engine import PolicyEngine, PolicyContext, Action, ReasonCode

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
        broker_id: str,
        **kwargs
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

        # ✅ RESOLVE RISK POLICY
        self.risk_policy = RiskPolicy.resolve(self.validated_config.risk_level)
        logger.info(f"Resolved Risk Policy: {self.risk_policy.config.label} (Max Risk: {self.risk_policy.config.max_compound_risk_pct:.1%})")
        
        # Get strategy (Hybrid Approach: Registry OR Injected Instance)
        self.strategy = None
        
        # 1. Try injected instance (System 1 Legacy Compatibility)
        # 1. Try injected instance (System 1 Legacy Compatibility OR System 2 Direct Injection)
        if kwargs.get("strategy_instance"):
            inst = kwargs.get("strategy_instance")
            if isinstance(inst, BaseStrategy):
                 logger.info(f"Using injected System 2 strategy: {inst.name}")
                 self.strategy = inst
            else:
                 logger.info(f"Adapting legacy strategy instance: {type(inst).__name__}")
                 self.strategy = LegacyStrategyAdapter(inst)
            
        # 2. Try System 2 Registry
        if not self.strategy:
            self.strategy = StrategyRegistry.get(strategy_id)
            
        # 3. Fail if neither found
        if not self.strategy:
            # Fallback: Check System 1 Registry lazily? 
            # If we are here, it means we didn't inject an instance AND it's not in System 2 registry.
            # We can't auto-instantiate System 1 strategies here because they need 'client'.
            raise ValueError(f"Strategy {strategy_id} not found in registry and no instance provided")
        
        # Initialize safety components
        self._init_safety_stack()
        
        # Initialize market analyzer and broker monitor
        self.market_analyzer = MarketAnalyzer()
        # Increase time drift tolerance to 30s to handle local machine clock skew
        self.broker_monitor = BrokerHealthMonitor(max_time_drift_ms=30000)
        
        # Stop-loss cooldown memory map (Symbol -> Unix Expiration Timestamp)
        self._stop_loss_cooldowns: Dict[str, float] = {}
        self._cooldown_duration_sec = 40.0 # ~2 candles on the HTF map
        self._current_regimes: Dict[str, str] = {} # Per-symbol regime tracking for dynamic safety
        
        logger.info(f"Trading orchestrator initialized for config {config_id}")
        if self.config_warnings:
            for warning in self.config_warnings:
                logger.warning(f"Config clamped: {warning}")


    def _validate_config(self) -> tuple[UserConfigurableLimits, list]:
        """Validate and clamp user configuration to system limits."""
        validated, warnings = self.config_validator.validate_and_clamp(self.user_config)
        return validated, warnings
    


    # ...

    def _init_safety_stack(self):
        """Initialize the 4-layer safety stack."""
        # Create risk budget config from validated user settings
        risk_config = self._create_risk_budget_config()
        self.risk_budget = RiskBudgetEngine(risk_config)
        
        # Account protection
        self.protection = AccountProtection(self.db)
        
        # Safety engine configuration
        # ✅ USE DYNAMIC POLICY limits where applicable (Layer C)
        safety_config = SafetyConfig(
            max_leverage=self._get_max_leverage_for_symbols(),
            max_trades_per_day=self.validated_config.max_trades_per_day,
            min_confidence_hard=self.validated_config.min_strategy_confidence,
            min_margin_buffer_pct=0.30,
            
            # ✅ DYNAMIC LIMITS from Policy
            max_stop_distance_pct=self.risk_policy.config.max_stop_loss_pct,
            max_compound_risk_pct=self.risk_policy.config.max_compound_risk_pct,
            
            # ✅ CIRCUIT BREAKER: More forgiving settings
            max_order_failures=3 if self.validated_config.strict_circuit_breakers else 5,
            max_slippage_pct=0.015 if self.validated_config.strict_circuit_breakers else 0.02,
            liquidation_margin_ratio_threshold=self.system_limits.emergency_margin_ratio_threshold,
            circuit_breaker_cooldown_minutes=10 if self.validated_config.strict_circuit_breakers else 5
        )
        
        # Create safety engine
        self.safety = SafetyEngine(
            self.db,
            self.risk_budget,
            self.protection,
            safety_config,
            risk_policy=self.risk_policy # ✅ PASS POLICY INSTANCE
        )
        
        # Create policy engine
        self.policy_engine = PolicyEngine(
            budget_engine=self.risk_budget,
            min_confidence=self.validated_config.min_strategy_confidence,
            max_stop_distance_pct=self.risk_policy.config.max_stop_loss_pct,
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
        
        # Step 0: Check Stop-Loss cooldowns
        now_ts = time.time()
        cooldown_exp = self._stop_loss_cooldowns.get(symbol, 0.0)
        if now_ts < cooldown_exp:
            result["decision"] = "blocked"
            result["reason"] = f"Symbol on post-stop-loss cooldown for {cooldown_exp - now_ts:.1f}s"
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
        # Clean up expired cooldowns silently
        elif cooldown_exp > 0:
            del self._stop_loss_cooldowns[symbol]

        
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
            # Store regime for dynamic safety (circuit breakers, etc)
            if strategy_output.meta and "regime" in strategy_output.meta:
                self._current_regimes[symbol] = strategy_output.meta["regime"]
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
            "reason": strategy_output.reason,
            "meta": strategy_output.meta
        }
        
        # If HOLD signal, nothing to do
        if strategy_output.signal == Signal.HOLD:
            result["decision"] = "hold"
            result["reason"] = strategy_output.reason or "Strategy says HOLD"
            
            # ✅ LOG HOLD REASON (Debugging "No Trades")
            logger.info(
                f"Strategy {self.strategy_id} HOLD for {symbol}. "
                f"Confidence={strategy_output.confidence:.4f}, "
                f"Reason={result['reason']}"
            )
            
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
        
        # Check daily activity fallback BEFORE Layer A
        fallback_status = self.safety.check_daily_activity_fallback(self.config_id)
        is_fallback_mode = fallback_status["should_activate"]
        is_fallback_trade = False  # Will be set to True if fallback allows trade
        
        if is_fallback_mode:
            logger.info(
                f"⚠️ DAILY ACTIVITY FALLBACK ACTIVE: {fallback_status['reason']} "
                f"(confidence threshold: {self.safety.config.min_confidence_soft:.1%})"
            )
        
        # Step 5: LAYER A - Pre-trade gating
        leverage = self.validated_config.requested_leverage.get(symbol, 10.0)
        
        # Cap leverage for fallback trades
        if is_fallback_mode:
            original_leverage = leverage
            leverage = min(leverage, self.safety.config.fallback_max_leverage)
            if leverage < original_leverage:
                logger.info(
                    f"Fallback mode: Leverage capped from {original_leverage}× to {leverage}×"
                )
        
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
            is_live_mode=not self.validated_config.paper_mode,
            is_fallback_mode=is_fallback_mode,
            strategy_name=self.strategy_id,
            signal=str(strategy_output.signal)
        )
        
        if not gate_decision.allowed:
            result["decision"] = "blocked"
            result["reason"] = f"Layer A: {gate_decision.message}"
            result["details"]["layer_a"] = gate_decision.details
            if is_fallback_mode:
                result["details"]["fallback_attempted"] = True
                result["details"]["fallback_status"] = fallback_status
            
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
        
        # If we passed Layer A in fallback mode, mark this as a fallback trade
        if is_fallback_mode:
            is_fallback_trade = True
            result["details"]["layer_a"] = "✅ All gates passed (FALLBACK MODE ACTIVE)"
            result["details"]["fallback_status"] = fallback_status
        else:
            result["details"]["layer_a"] = "✅ All gates passed"
        
        # ── Step 6: Delegate Sizing to PolicyEngine ────────────────────────
        # PolicyEngine handles ATR-based sizing, % risk, max compound risk, and notional clamping.
        effective_equity = current_equity * self.validated_config.capital_allocation_pct

        risk_limit_pct = self.config_validator.get_risk_per_trade_limit(
            self.validated_config.risk_level
        ) * 100.0

        ctx = PolicyContext(
            symbol=symbol,
            signal=strategy_output.signal.value,
            confidence=strategy_output.confidence,
            position="NONE",  # Orchestrator calculates new open
            entry_price=current_price,
            atr=strategy_output.indicators.get("atr", 0.0) if strategy_output.indicators else 0.0,
            equity=effective_equity,
            margin_available=margin_available,
            margin_used=margin_used,
            open_positions_count=open_positions,
            total_exposure=kwargs.get("total_exposure", 0.0),
            leverage=leverage,
            risk_level=self.validated_config.risk_level,
            account_risk_pct=risk_limit_pct,
            max_leverage=self._get_max_leverage_for_symbols(),
            min_notional=5.0,
            max_notional=effective_equity * self._get_max_leverage_for_symbols(),
            trade_amount_mode="fixed" if (self.validated_config.use_fixed_size and self.validated_config.fixed_size_usdt) else "atr_risk",
            trade_amount_value=self.validated_config.fixed_size_usdt if self.validated_config.use_fixed_size else 0.0,
            execution_mode="live" if not self.validated_config.paper_mode else "paper",
            now_ms=int(time.time() * 1000),
            # Set un-tracked gates to infinity (SafetyEngine Layer A handled these)
            max_daily_loss=effective_equity,
            max_daily_trades=9999, 
            max_open_positions=self.validated_config.max_open_positions,
        )

        decision = self.policy_engine.evaluate(ctx)

        if not decision.allowed:
            result["decision"] = "blocked"
            result["reason"] = f"PolicyEngine Sizing Blocked: {decision.reason}"
            self.record_decision(
                symbol=symbol, run_id=kwargs.get("run_id"),
                strategy_signal_json=json.dumps(result["details"].get("strategy_output", {})),
                risk_gate_decision_json=json.dumps(result["details"].get("layer_a", {})),
                sizing_decision_json=json.dumps({"blocked_by": "policy"}),
                protection_decision_json="{}",
                final_action="blocked"
            )
            return result

        base_size = decision.quantity
        target_notional_usdt = decision.notional

        logger.info(
            f"Policy Sizing [{symbol}]: equity={effective_equity:.2f}, "
            f"target_notional={target_notional_usdt:.2f} USDT, "
            f"price={current_price:.6f}, base_size={base_size:.4f} units "
            f"(Method: {decision.details.get('sizing_method', 'unknown')})"
        )

        
        # Step 7: LAYER B - Sizing controls
        total_exposure = kwargs.get("total_exposure", 0)
        
        from app.risk.sizing_engine import SizingResult
        
        # Reconstruct a generic SizingResult to fulfill SafetyEngine's Layer B signature
        # Crucially, pass through decision.details so Layer B can see strict fixed sizing metadata.
        sizing_result = SizingResult(
            allowed=True,
            reason="policy_approved",
            quantity=decision.quantity,
            notional=decision.notional,
            required_leverage=leverage,
            effective_risk_pct=0.0,
            margin_utilization_pct=0.0,
            details=decision.details
        )
        
        size_decision = self.safety.calculate_safe_size(
            config_id=self.config_id,
            symbol=symbol,
            sizing_result=sizing_result,
            entry_price=current_price,
            current_equity=current_equity,
            margin_used=margin_used,
            margin_available=margin_available,
            leverage=leverage,
            atr=strategy_output.indicators.get("atr") if strategy_output.indicators else None,
            total_notional_exposure=total_exposure,
            is_fallback_trade=is_fallback_trade
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
        
        # ✅ Handle Clamping from Layer C
        final_stop_distance_pct = strategy_output.suggested_stop_distance
        if protection_decision.details and protection_decision.details.get("clamped"):
            final_stop_distance_pct = protection_decision.details.get("stop_distance_pct")
            result["details"]["layer_c"] = f"⚠️ Risk Clamped: {protection_decision.message}"
        else:
            result["details"]["layer_c"] = "✅ Protective orders validated"
        
        # Recalculate prices with verified/clamped distance
        if strategy_output.signal == Signal.SELL:
             stop_loss_price = current_price * (1 + final_stop_distance_pct)
        else: # BUY
             stop_loss_price = current_price * (1 - final_stop_distance_pct)
             
        # Recalculate TP if it relies on SL distance
        tp_distance = strategy_output.take_profit_distance or (final_stop_distance_pct * 2.0)
        
        if strategy_output.signal == Signal.SELL:
            take_profit_price = current_price * (1 - tp_distance)
        else:
            take_profit_price = current_price * (1 + tp_distance)
        
        # Stage 3A: Apply adaptive_size_multiplier AFTER all safety layers pass.
        # Disabled — strict user sizing mode (Option A).
        # The user's allocation value is authoritative. Adaptive multipliers (loss streak,
        # drawdown penalties) must not reduce it. Kept for future re-activation.
        _adaptive_mult    = float(kwargs.get("adaptive_size_multiplier", 1.0))
        _policy_base_size = final_size   # quantity approved by all safety layers
        _adaptive_applied = False
        # DISABLED (Option A): if 0.0 < _adaptive_mult < 1.0:
        #     final_size        = _policy_base_size * _adaptive_mult
        #     _adaptive_applied = True
        #     logger.info(
        #         "[Stage3A] %s: adaptive_size_multiplier=%.3f qty %.6f → %.6f",
        #         symbol, _adaptive_mult, _policy_base_size, final_size,
        #     )
        if _adaptive_applied:
            pass  # unreachable; preserved for structural symmetry

        # Retrieve sizing trace fields from policy decision for the audit record
        _layer_b_details = result["details"].get("layer_b", {})
        _pe_details      = decision.details  # PolicyDecision.details (set by policy engine)
        _alloc_mode      = _pe_details.get("allocation_mode", ctx.trade_amount_mode)
        _base_margin     = _pe_details.get("base_margin")      # user's stated fixed margin (if fixed mode)
        _final_margin    = _pe_details.get("final_margin")     # final user margin (strict fixed mode)
        _cap_reason      = _pe_details.get("cap_reason")       # None = no cap fired
        _cap_applied     = _pe_details.get("cap_applied", False)
        _risk_warning    = _pe_details.get("risk_warning", False)
        _base_notional_usdt = _pe_details.get("base_notional_usdt")
        _final_notional_usdt = _pe_details.get("final_notional_usdt", target_notional_usdt)
        _base_qty_before_cap = _pe_details.get("base_qty")
        _stop_distance_pct = _pe_details.get("stop_distance_pct")
        _max_risk_capital = _pe_details.get("max_risk_capital")
        _sizing_method = _pe_details.get("sizing_method")
        _admin_sizing_message = _pe_details.get("admin_message")

        # All layers passed - but check final size before execution
        if final_size <= 0:
            result["decision"] = "blocked"
            result["reason"]   = "final_size is 0.0 (post-adaptive calculation)"
            _layer_b_details["size_zero_blocked"] = True
            _layer_b_details["final_size"] = 0.0
            from app.policy.policy_engine import ReasonCode
            result["details"]["layer_b"]["reason_code"] = ReasonCode.SIZE_ZERO
        else:
            result["decision"] = "execute"
            result["reason"]   = protection_decision.message
        # Compute sizing trace fields for audit/UI
        _base_margin_usdt  = _base_margin   # user's stated margin (fixed mode), or None for atr_risk
        _final_margin_usdt = _final_margin  # strict fixed margin, or None if not fixed
        # Under Option A the adaptive multiplier is disabled; size_changed reflects
        # any remaining change from ATR cap or Layer B affordability rejection.
        _size_changed      = bool(_cap_applied) or (round(final_size, 8) != round(_policy_base_size, 8))
        _size_change_reason: str | None = None
        if _size_changed:
            if _cap_applied:
                _size_change_reason = f"atr_safety_cap: base={_base_margin_usdt:.4f} capped={_final_margin_usdt:.4f}"
            else:
                _size_change_reason = "layer_b_adjustment"

        result["trade_params"] = {
            "symbol":           symbol,
            "side":             "BUY" if strategy_output.signal == Signal.BUY else "SELL",
            "quantity":         final_size,
            "entry_price":      current_price,
            "stop_loss":        stop_loss_price,
            "take_profit":      take_profit_price,
            "leverage":         leverage,
            "is_fallback_trade": is_fallback_trade,
            # Sizing audit trace — satisfies product contract visibility requirement
            "sizing_trace": {
                "allocation_type":        _pe_details.get("allocation_type") or _alloc_mode,
                "allocation_mode":        _alloc_mode,
                "sizing_method":          _sizing_method,
                "user_fixed_margin_usdt": _pe_details.get("user_fixed_margin_usdt"),
                "base_size":              round(_policy_base_size, 8),  # post-safety-layers, pre-adaptive
                "adaptive_multiplier":    round(_adaptive_mult, 6),     # recorded but not applied (Option A)
                "adaptive_applied":       _adaptive_applied,            # always False under Option A
                "final_size":             round(final_size, 8),
                "base_margin_usdt":       _base_margin_usdt,   # user's stated margin (fixed mode only)
                "final_margin_usdt":      _final_margin_usdt,  # strict fixed margin (fixed mode only)
                "capped_margin_usdt":     _final_margin_usdt,  # alias kept for backward compat
                "base_notional_usdt":      _base_notional_usdt,
                "final_notional_usdt":     _final_notional_usdt,
                "base_qty_before_cap":     _base_qty_before_cap,
                "calculated_qty":          _pe_details.get("calculated_qty"),
                "rounded_qty":             _pe_details.get("rounded_qty"),
                "final_qty":               round(final_size, 8),
                "entry_price":             _pe_details.get("entry_price"),
                "leverage":                leverage,
                "cap_applied":            _cap_applied,
                "cap_reason":             _cap_reason,
                "account_risk_pct":        _pe_details.get("account_risk_pct"),
                "stop_distance_pct":       _stop_distance_pct,
                "theoretical_risk_usdt":   _pe_details.get("theoretical_risk_usdt"),
                "theoretical_risk_pct":    _pe_details.get("theoretical_risk_pct"),
                "risk_warning":            _risk_warning,
                "max_risk_capital":        _max_risk_capital,
                "risk_level":              _pe_details.get("risk_level"),
                "risk_level_label":        _pe_details.get("risk_level_label"),
                "notional_cap_usdt":       _pe_details.get("notional_cap_usdt"),
                "margin_cap_usdt":         _pe_details.get("margin_cap_usdt"),
                "atr_cap_margin_usdt":     _pe_details.get("atr_cap_margin_usdt"),
                "admin_message":           _admin_sizing_message,
                "size_changed":           _size_changed,
                "size_change_reason":     _size_change_reason,
                "min_notional":           ctx.min_notional,
            },
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
        # ── [ORCH_DECISION] Single-line decision summary per symbol per cycle ─────────
        # This is the authoritative final decision log for this symbol.  After PASSED_LAYER_A
        # there are 13+ more gates; this log shows what happened to each one.
        # Grep for [ORCH_DECISION] to see why PASSED_LAYER_A symbols did or did not open.
        logger.info(
            "[ORCH_DECISION] symbol=%s decision=%s reason=%s",
            symbol,
            result["decision"].upper(),
            result.get("reason", ""),
        )

        return result
    
    def record_trade_execution(
        self,
        symbol: str,
        success: bool,
        expected_price: float,
        executed_price: Optional[float] = None,
        error_message: Optional[str] = None,
        is_fallback_trade: bool = False,
        action: str = "OPEN" # Add action parameter for stop-loss detection
    ):
        """
        Record trade execution result for Layer D monitoring.
        
        Call this after attempting to execute a trade.
        """
        # Set cooldown if this was a Stop Loss hit
        if success and action == "HIT_STOP":
            self._stop_loss_cooldowns[symbol] = time.time() + self._cooldown_duration_sec
            logger.info(f"🛑 [COOLDOWN] Symbol {symbol} hit stop-loss. Entries blocked for {self._cooldown_duration_sec}s.")

        # Layer D: Post-trade monitoring
        regime = self._current_regimes.get(symbol)
        self.safety.record_order_result(
            config_id=self.config_id,
            symbol=symbol,
            success=success,
            error_message=error_message,
            regime=regime
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
            
            # Update daily activity tracking timestamp
            self.safety.update_trade_timestamp(
                config_id=self.config_id,
                is_fallback_trade=is_fallback_trade
            )
        
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




class LegacyStrategyAdapter(BaseStrategy):
    """
    Adapts a System 1 Legacy Strategy (get_signal) to System 2 BaseStrategy (analyze).
    """
    def __init__(self, legacy_instance):
        self.legacy = legacy_instance
        super().__init__(
            strategy_id=getattr(legacy_instance, "name", "legacy"),
            family=StrategyFamily.TREND_FOLLOWING, # Default assumption
            name=getattr(legacy_instance, "name", "Legacy Strategy"),
            description=getattr(legacy_instance, "description", "Adapted legacy strategy")
        )

    def analyze(self, symbol: str, klines: list, current_price: float, **kwargs) -> StrategyOutput:
        # Call legacy get_signal
        # Note: Legacy strategies typically use their own internal client/data fetching.
        # Using passed 'klines' is not supported by legacy .get_signal() interface.
        try:
            try:
                result = self.legacy.get_signal(symbol, **kwargs)
            except TypeError:
                # Fallback if the legacy signature doesn't accept kwargs
                result = self.legacy.get_signal(symbol)
            
            # Map System 1 Signal enum/string to System 2 Signal enum
            sig_val = result.signal
            final_signal = Signal.HOLD
            
            # Handle String or Enum
            if hasattr(sig_val, "name"):
                sig_name = sig_val.name.upper()
            else:
                sig_name = str(sig_val).upper()
                
            if sig_name == "BUY":
                final_signal = Signal.BUY
            elif sig_name == "SELL":
                final_signal = Signal.SELL
                
            # Dynamic stop distance using ATR
            stop_dist = 0.0
            atr_val = 0.0
            
            if klines and len(klines) >= 14:
                from app.policy.policy_engine import calculate_atr
                atr_val = calculate_atr(klines, period=14)
                if atr_val > 0 and current_price > 0:
                    stop_dist = (atr_val * 2.0) / current_price
            
            # Legacy strategies MUST have a valid stop distance to be traded
            if final_signal != Signal.HOLD and stop_dist <= 0:
                return StrategyOutput(
                    signal=Signal.HOLD,
                    confidence=0.0,
                    suggested_stop_distance=0.01,  # Pydantic requires >0
                    riskiness=0.5,
                    reason="legacy_no_atr",
                    indicators={"atr": 0.0}
                )
            
            return StrategyOutput(
                signal=final_signal,
                confidence=float(getattr(result, "confidence", 0.0)),
                suggested_stop_distance=stop_dist,
                riskiness=0.5, # Default medium risk
                reason=getattr(result, "reason", "Legacy signal"),
                indicators={"atr": atr_val} if atr_val > 0 else {},
                meta=getattr(result, "meta", None)
            )
        except Exception as e:
            logger.error(f"Legacy strategy adaptation failed: {e}")
            return StrategyOutput(
                signal=Signal.HOLD,
                confidence=0.0,
                suggested_stop_distance=0.01,
                reason=f"Adapter Error: {e}"
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
