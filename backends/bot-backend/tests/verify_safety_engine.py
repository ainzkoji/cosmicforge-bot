
import sys
import os
import sqlite3

# Add bot-backend to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.risk.safety_engine import SafetyEngine, SafetyConfig
from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig
from app.risk.account_protection import AccountProtection
from shared_lib.persistence.db import DB

def verify():
    print("Verifying SafetyEngine initialization...")
    
    # 1. Setup DB
    import tempfile
    tf = tempfile.NamedTemporaryFile(delete=False)
    tf.close()
    db_path = tf.name
    print(f"Using temp DB: {db_path}")
    db = DB(db_path)
    
    # 2. Setup Config
    try:
        config = SafetyConfig(
            min_confidence_hard=0.30,
            min_confidence_soft=0.05,
            daily_activity_fallback_enabled=True,
            daily_activity_fallback_hours=24,
            fallback_position_size_multiplier=0.25,
            fallback_max_leverage=5.0
        )
        print("SafetyConfig initialized successfully.")
    except Exception as e:
        print(f"FAILED to initialize SafetyConfig: {e}")
        return

    # 3. Setup Dependencies
    risk_config = RiskBudgetConfig(
        portfolio_risk_pct=0.05,
        per_trade_risk_pct=0.01,
        max_margin_usage_pct=0.50,
        base_slots=5,
        max_slots=15
    )
    risk_budget = RiskBudgetEngine(risk_config)
    protection = AccountProtection(db)
    
    # 4. Initialize Engine
    try:
        engine = SafetyEngine(db, risk_budget, protection, config)
        print("SafetyEngine initialized successfully.")
    except Exception as e:
        print(f"FAILED to initialize SafetyEngine: {e}")
        return

    # 5. Check Table
    try:
        with db.connect() as conn:
            # Check if table exists
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_activity_tracking'")
            row = cursor.fetchone()
            if row:
                print("Table 'daily_activity_tracking' EXISTS.")
            else:
                print("Table 'daily_activity_tracking' DOES NOT EXIST!")
                
            # Check daily_trade_counts (older table)
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_trade_counts'")
            row = cursor.fetchone()
            if row:
                print("Table 'daily_trade_counts' EXISTS.")
            else:
                print("Table 'daily_trade_counts' DOES NOT EXIST!")
                
    except Exception as e:
        print(f"FAILED to check table: {e}")

    # 6. Verify Fallback Position Sizing
    print("\nVerifying Fallback Position Sizing...")
    try:
        config_id = "verify_config"
        base_size = 0.2
        print(f"Config fallback multiplier: {engine.config.fallback_position_size_multiplier}")
        
        decision = engine.calculate_safe_size(
            config_id=config_id,
            symbol="BTCUSDT",
            base_size=base_size,
            entry_price=50000.0,
            current_equity=10000.0,
            margin_used=2000.0,
            margin_available=8000.0,
            leverage=10.0,
            is_fallback_trade=True
        )
        
        print(f"Decision Allowed: {decision.allowed}")
        print(f"Original Size: {base_size}")
        print(f"Adjusted Size: {decision.adjusted_size}")
        print(f"Reductions: {decision.details.get('reductions', [])}")
        
        expected = base_size * 0.25
        if abs(decision.adjusted_size - expected) < 0.001:
            print("✅ Position size correctly reduced to 25%")
        else:
            print(f"❌ Position size mismatch! Expected {expected}, got {decision.adjusted_size}")
            
    except Exception as e:
        print(f"FAILED to verify position sizing: {e}")

if __name__ == "__main__":
    verify()
