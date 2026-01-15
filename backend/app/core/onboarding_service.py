import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from app.persistence.db import DB
from app.schemas.onboarding import StrategyItem

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ============================================================================
# 1. Strategy Catalog (Hardcoded for now)
# ============================================================================

STRATEGIES = [
    StrategyItem(
        id="safe_trend",
        name="SafeTrend Voyager",
        description="A conservative trend-following strategy ideal for beginners. It avoids choppy markets and only trades clear trends.",
        difficulty="Beginner",
        tags=["Trend", "Conservative", "Long-Term"],
        min_capital=100.0
    ),
    StrategyItem(
        id="mean_reversion",
        name="MeanReversion Pulse",
        description="Buys when prices dip too far and sells when they rally too high. Good for sideways markets.",
        difficulty="Intermediate",
        tags=["Mean Reversion", "Swings"],
        min_capital=250.0
    ),
    StrategyItem(
        id="scalp_master",
        name="Velocity Scalper",
        description="High-frequency trading strategy for small price movements. Requires low-latency execution and higher risk tolerance.",
        difficulty="Advanced",
        tags=["Scalping", "High Frequency", "Aggressive"],
        min_capital=500.0
    )
]

def get_strategy_catalog() -> List[StrategyItem]:
    return STRATEGIES

# ============================================================================
# 2. Defaults Generator
# ============================================================================

def generate_risk_defaults(risk_tolerance: str) -> Dict[str, Any]:
    """
    Returns a RiskPolicy dict based on tolerance.
    """
    if risk_tolerance == "low":
        return {
            "max_daily_loss": 2.0, # 2%
            "max_position_size_usdt": 100.0,
            "max_leverage": 2,
            "stop_loss_pct": 0.02
        }
    elif risk_tolerance == "medium":
        return {
            "max_daily_loss": 5.0, # 5%
            "max_position_size_usdt": 500.0,
            "max_leverage": 5,
            "stop_loss_pct": 0.05
        }
    else: # high
        return {
            "max_daily_loss": 10.0, # 10%
            "max_position_size_usdt": 2000.0,
            "max_leverage": 10,
            "stop_loss_pct": 0.10
        }

def generate_bot_defaults(strategy_id: str, capital: float) -> Dict[str, Any]:
    """
    Returns a Bot Config dict.
    """
    return {
        "strategy_id": strategy_id,
        "symbol_whitelist": ["BTCUSDT", "ETHUSDT"], # Default safe symbols
        "timeframe": "1h",
        "position_sizing": {
            "type": "fixed_usdt",
            "amount": min(capital * 0.1, 50.0) # 10% of capital or 50 USDT max
        }
    }

# ============================================================================
# 3. Onboarding Service
# ============================================================================

def get_onboarding_state(user_id: str) -> Dict[str, Any]:
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM onboarding_profiles WHERE user_id = ?", (user_id,)).fetchone()
        
        if not row:
            # Initialize if not exists
            now = utc_now_iso()
            conn.execute(
                """
                INSERT INTO onboarding_profiles (user_id, status, current_step, data_json, created_at, updated_at) 
                VALUES (?, 'not_started', 'welcome', '{}', ?, ?)
                """,
                (user_id, now, now)
            )
            return {
                "status": "not_started",
                "current_step": "welcome",
                "data": {}
            }
            
        data = dict(row)
        return {
            "status": data["status"],
            "current_step": data["current_step"],
            "data": json.loads(data["data_json"]) if data["data_json"] else {},
            "recommended_defaults": json.loads(data["recommended_defaults"]) if data["recommended_defaults"] else None
        }

def update_onboarding_step(user_id: str, step: str, step_data: Dict[str, Any]) -> None:
    db = DB()
    
    # Get current data to merge
    current = get_onboarding_state(user_id)
    merged_data = current["data"]
    merged_data.update(step_data)
    
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE onboarding_profiles 
            SET current_step = ?, 
                data_json = ?, 
                status = 'in_progress', 
                updated_at = ? 
            WHERE user_id = ?
            """,
            (step, json.dumps(merged_data), utc_now_iso(), user_id)
        )

def complete_onboarding(user_id: str) -> Dict[str, Any]:
    """
    Finalizes onboarding, generates defaults, and saves them.
    """
    # 1. Get final data
    state = get_onboarding_state(user_id)
    data = state["data"]
    
    # 2. Generate Defaults
    risk = generate_risk_defaults(data.get("risk_tolerance", "low").lower())
    bot = generate_bot_defaults(data.get("strategy_preference", "safe_trend"), data.get("capital_allocation", 100.0))
    
    defaults = {"risk_policy": risk, "bot_template": bot}
    
    # 3. Save
    db = DB()
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE onboarding_profiles 
            SET status = 'completed', 
                recommended_defaults = ?, 
                completed_at = ?, 
                updated_at = ? 
            WHERE user_id = ?
            """,
            (json.dumps(defaults), now, now, user_id)
        )
        
    return defaults

# ============================================================================
# 4. Decision Engine (Gating)
# ============================================================================

def get_next_steps(user_id: str) -> Dict[str, Any]:
    blockers = []
    db = DB()
    
    # 1. Check Broker
    with db.connect() as conn:
        broker_count = conn.execute(
            "SELECT COUNT(*) FROM broker_accounts WHERE user_id = ? AND status != 'disconnected'", 
            (user_id,)
        ).fetchone()[0]
        if broker_count == 0:
            blockers.append("NO_BROKER")

    # 2. Check Subscription (Page 5)
    from app.core import billing_service
    # If live trading is the goal, check detailed entitlement
    can_live = billing_service.check_entitlement(user_id, "live_trading")
    if not can_live:
         # Check if it's because of plan or just not allowed
         sub = billing_service.get_user_subscription(user_id)
         if sub["status"] == "active" and not sub["entitlements"].get("live_trading"):
             # Plan doesn't support it
             blockers.append("PLAN_UPGRADE_REQUIRED")
         elif sub["status"] != "active":
             blockers.append("SUBSCRIPTION_INACTIVE")

    # 3. Check KYC (from policies)
    # Mock check for now or DB check
    # We call kyc_policy.check_kyc_gate
    from app.core.kyc_policy import check_kyc_gate, KYCAction
    allowed, msg = check_kyc_gate(user_id, KYCAction.START_LIVE_TRADING)
    if not allowed:
        blockers.append("KYC_REQUIRED")

    # Recommendation
    if "NO_BROKER" in blockers:
        action = "CONNECT_BROKER"
    elif "KYC_REQUIRED" in blockers:
        action = "COMPLETE_KYC"
    elif "PLAN_UPGRADE_REQUIRED" in blockers or "SUBSCRIPTION_INACTIVE" in blockers:
        action = "MANAGE_BILLING"
    else:
        action = "CREATE_BOT"

    return {
        "can_proceed_to_live": len(blockers) == 0,
        "blockers": blockers,
        "recommended_action": action
    }
