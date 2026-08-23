"""
Safety Engine Integration Example

Shows how to integrate SafetyEngine into the trading loop.
"""
from shared_lib.persistence.db import DB
from app.risk.safety_engine import SafetyEngine, SafetyConfig, MarketConditions, BrokerHealth
from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig
from app.risk.account_protection import AccountProtection
from app.risk.market_analyzer import MarketAnalyzer
from app.risk.broker_health import BrokerHealthMonitor


def create_safety_engine(config_id: str, risk_profile: str = "balanced") -> SafetyEngine:
    """
    Factory to create SafetyEngine with proper dependencies.
    
    Args:
        config_id: User configuration ID
        risk_profile: conservative | balanced | aggressive
        
    Returns:
        Configured SafetyEngine ready for use
    """
    db = DB()
    
    # Initialize risk budget (from user's risk profile)
    if risk_profile == "conservative":
        risk_config = RiskBudgetConfig(
            portfolio_risk_pct=0.02,
            max_margin_usage_pct=0.35,
            base_slots=3,
            max_slots=10
        )
    elif risk_profile == "aggressive":
        risk_config = RiskBudgetConfig(
            portfolio_risk_pct=0.10,
            max_margin_usage_pct=0.65,
            base_slots=8,
            max_slots=30
        )
    else:  # balanced
        risk_config = RiskBudgetConfig(
            portfolio_risk_pct=0.05,
            max_margin_usage_pct=0.50,
            base_slots=5,
            max_slots=20
        )
    
    risk_budget = RiskBudgetEngine(risk_config)
    
    # Initialize account protection
    protection = AccountProtection(db)
    
    # Initialize safety config
    safety_config = SafetyConfig(
        max_leverage=20.0,
        max_trades_per_day=100,
        min_strategy_confidence=0.3,
        min_margin_buffer_pct=0.30,
        max_stop_distance_pct=0.10,
        max_compound_risk_pct=0.15
    )
    
    # Create safety engine
    return SafetyEngine(db, risk_budget, protection, safety_config)


# ============================================================================
# INTEGRATION EXAMPLE: Trading Loop
# ============================================================================

def trading_loop_with_safety(config_id: str, broker_account_id: str, client, strategy):
    """
    Example trading loop with complete safety stack.
    
    This shows how to use SafetyEngine at each stage.
    """
    # Initialize safety engine
    safety = create_safety_engine(config_id, risk_profile="balanced")
    market_analyzer = MarketAnalyzer()
    broker_monitor = BrokerHealthMonitor()
    
    # Get current account state
    account = client.account()
    current_equity = float(account['totalWalletBalance'])
    margin_used = float(account['totalMaintMargin'])
    margin_available = float(account['availableBalance'])
    
    # Update risk budget state
    safety.risk_budget.update_account_state(
        equity=current_equity,
        margin_used=margin_used,
        margin_available=margin_available
    )
    
    # For each symbol to trade
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        print(f"\n=== Processing {symbol} ===")
        
        # Step 1: Get strategy signal
        signal = strategy.generate_signal(symbol)  # Returns signal + confidence
        
        if signal.action == "NONE":
            continue
        
        # Step 2: Get market data for analysis
        ticker = client.get_ticker(symbol)
        klines = client.klines(symbol, "1h", limit=50)
        atr = strategy.calculate_atr(klines)
        
        # Analyze market conditions (Layer A component)
        market_conditions = market_analyzer.analyze_from_exchange_data(
            symbol=symbol,
            ticker=ticker,
            klines=klines,
            atr=atr
        )
        
        # Check broker health (Layer A component)
        broker_health = broker_monitor.check_health("binance_futures", client)
        
        # Get open positions count
        positions = client.get_positions()
        open_positions = len([p for p in positions if float(p['positionAmt']) != 0])
        
        # ====================================================================
        # LAYER A: PRE-TRADE GATING
        # ====================================================================
        gate_decision = safety.check_pre_trade(
            config_id=config_id,
            symbol=symbol,
            confidence=signal.confidence,
            leverage=10.0,  # From user config
            current_equity=current_equity,
            open_positions=open_positions,
            market_conditions=market_conditions,
            broker_health=broker_health,
            user_kyc_approved=True,
            is_live_mode=True
        )
        
        if not gate_decision.allowed:
            print(f"❌ BLOCKED by Layer A: {gate_decision.message}")
            continue
        
        print(f"✅ Layer A: {gate_decision.message}")
        
        # Step 3: Calculate base position size
        entry_price = float(ticker['lastPrice'])
        stop_loss_price = entry_price * 0.98  # 2% stop
        
        # Risk-based sizing
        risk_amount = current_equity * 0.01  # 1% risk
        stop_distance = abs(entry_price - stop_loss_price)
        base_size = risk_amount / stop_distance if stop_distance > 0 else 0
        
        # ====================================================================
        # LAYER B: SIZING CONTROLS
        # ====================================================================
        total_exposure = sum([
            float(p['notional']) for p in positions
        ])
        
        size_decision = safety.calculate_safe_size(
            config_id=config_id,
            symbol=symbol,
            base_size=base_size,
            entry_price=entry_price,
            current_equity=current_equity,
            margin_used=margin_used,
            margin_available=margin_available,
            atr=atr,
            total_notional_exposure=total_exposure
        )
        
        if not size_decision.allowed:
            print(f"❌ BLOCKED by Layer B: {size_decision.message}")
            continue
        
        final_size = size_decision.adjusted_size
        print(f"✅ Layer B: Size {base_size:.4f} → {final_size:.4f} ({size_decision.message})")
        
        # ====================================================================
        # LAYER C: PROTECTIVE ORDERS
        # ====================================================================
        protection_decision = safety.validate_protective_orders(
            symbol=symbol,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            leverage=10.0,
            position_size=final_size
        )
        
        if not protection_decision.allowed:
            print(f"❌ BLOCKED by Layer C: {protection_decision.message}")
            continue
        
        print(f"✅ Layer C: {protection_decision.message}")
        
        # ====================================================================
        # EXECUTE TRADE
        # ====================================================================
        try:
            # Place order via your executor
            order = client.place_market_order(
                symbol=symbol,
                side="BUY" if signal.action == "LONG" else "SELL",
                quantity=final_size
            )
            
            executed_price = float(order['avgPrice'])
            
            print(f"✅ Order executed: {order['orderId']} @ {executed_price}")
            
            # ====================================================================
            # LAYER D: POST-TRADE MONITORING
            # ====================================================================
            
            # Record successful order
            safety.record_order_result(config_id, symbol, success=True)
            
            # Record slippage
            safety.record_slippage(config_id, symbol, entry_price, executed_price)
            
            # Increment trade counter
            safety.increment_trade_count(config_id)
            
            # Check liquidation risk
            liq_check = safety.check_liquidation_risk(current_equity, margin_used)
            if not liq_check.allowed:
                print(f"⚠️ LIQUIDATION RISK: {liq_check.message}")
            
        except Exception as e:
            print(f"❌ Order failed: {str(e)}")
            
            # Record failed order (triggers circuit breaker after N failures)
            safety.record_order_result(config_id, symbol, success=False, error_message=str(e))
            broker_monitor.record_error("binance_futures", str(e))


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    # This would be called from your main trading loop
    # with actual client and strategy instances
    
    print("Safety Engine Integration Example")
    print("==================================")
    print()
    print("The SafetyEngine provides 4-layer protection:")
    print("  Layer A: Pre-Trade Gating (hard blocks)")
    print("  Layer B: Sizing Controls (exposure limits)")
    print("  Layer C: Protective Orders (SL/TP validation)")
    print("  Layer D: Post-Trade Monitoring (circuit breakers)")
    print()
    print("See function 'trading_loop_with_safety' for integration example")
