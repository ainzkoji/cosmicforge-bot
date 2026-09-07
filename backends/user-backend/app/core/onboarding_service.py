import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from fastapi import HTTPException
from shared_lib.persistence.db import DB
from app.schemas.onboarding import (
    StrategyItem, ExperienceLevel, RiskTolerance, AllocationModel,
    RiskPolicyPreset, BotSetupBlueprint, ExperienceData, RiskData,
    StrategySelectionData, AllocationData, WelcomeData, NextStepDecision
)
# Lazy imports to avoid circular deps if any
# from app.core import billing_service (imported inside functions)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ============================================================================
# 1. Strategy Catalog & Validation
# ============================================================================

STRATEGIES = [
    StrategyItem(
        id="safe_trend",
        name="SafeTrend Voyager",
        description="A conservative trend-following strategy ideal for beginners. It avoids choppy markets and only trades clear trends.",
        difficulty="Beginner",
        tags=["Trend", "Conservative", "Long-Term"],
        min_capital=100.0,
        compatible_markets=["crypto", "forex"]
    ),
    StrategyItem(
        id="mean_reversion",
        name="MeanReversion Pulse",
        description="Buys when prices dip too far and sells when they rally too high. Good for sideways markets.",
        difficulty="Intermediate",
        tags=["Mean Reversion", "Swings"],
        min_capital=250.0,
        compatible_markets=["crypto"]
    ),
    StrategyItem(
        id="scalp_master",
        name="Velocity Scalper",
        description="High-frequency trading strategy for small price movements. Requires low-latency execution and higher risk tolerance.",
        difficulty="Advanced",
        tags=["Scalping", "High Frequency", "Aggressive"],
        min_capital=500.0,
        compatible_markets=["crypto"]
    )
]

def get_strategy_catalog() -> List[StrategyItem]:
    return STRATEGIES

def validate_strategy_choice(strategy_id: str) -> StrategyItem:
    found = next((s for s in STRATEGIES if s.id == strategy_id), None)
    if not found:
        raise ValueError(f"Strategy {strategy_id} not found in catalog.")
    return found

# ============================================================================
# 2. Risk & Allocation Logic (The Brains)
# ============================================================================

def get_risk_preset(tolerance: RiskTolerance) -> RiskPolicyPreset:
    if tolerance == "low":
        return RiskPolicyPreset(
            id="low",
            max_daily_loss_pct=2.0,
            max_position_size_usdt=100.0,
            max_leverage=1,
            stop_loss_pct=0.02,
            max_open_positions=1,
            drawdown_limit_pct=5.0
        )
    elif tolerance == "medium":
        return RiskPolicyPreset(
            id="medium",
            max_daily_loss_pct=5.0,
            max_position_size_usdt=500.0,
            max_leverage=3,
            stop_loss_pct=0.05,
            max_open_positions=3,
            drawdown_limit_pct=10.0
        )
    else: # high
        return RiskPolicyPreset(
            id="high",
            max_daily_loss_pct=10.0,
            max_position_size_usdt=2000.0,
            max_leverage=10,
            stop_loss_pct=0.10,
            max_open_positions=5,
            drawdown_limit_pct=20.0
        )

def clamp_risk_policy(policy: RiskPolicyPreset, experience: ExperienceLevel) -> RiskPolicyPreset:
    """Clamps the risk policy based on user's experience level."""
    clamped = policy.copy()
    
    if experience == "beginner":
        # Beginner hard caps
        clamped.max_leverage = min(clamped.max_leverage, 1) # No leverage
        clamped.max_daily_loss_pct = min(clamped.max_daily_loss_pct, 2.0)
        clamped.max_open_positions = min(clamped.max_open_positions, 1)
        
    elif experience == "intermediate":
        # Intermediate caps
        clamped.max_leverage = min(clamped.max_leverage, 3)
        
    # Advanced gets full policy limits
    return clamped

def validate_allocation(amount: float, alloc_type: AllocationModel, risk_tolerance: RiskTolerance) -> None:
    """
    Validates capital allocation against risk profile limits.
    """
    if alloc_type == "percentage":
        # Percentage limits: Low=40%, Med=60%, High=80%
        limit_map = {"low": 40.0, "medium": 60.0, "high": 80.0}
        limit = limit_map.get(risk_tolerance, 40.0)
        
        if amount > limit:
            raise ValueError(f"Allocation {amount}% exceeds limit of {limit}% for {risk_tolerance} risk profile.")
            
    # Fixed amount limits could depend on user balance check (omitted for now as we don't know balance yet)
    pass

# ============================================================================
# 3. Onboarding Service (State Management)
# ============================================================================

def get_onboarding_state(user_id: str) -> Dict[str, Any]:
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM onboarding_profiles WHERE user_id = ?", (user_id,)).fetchone()
        
        if not row:
            # Initialize
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
                "data": {},
                "recommended_setup": None
            }
            
        data = dict(row)
        return {
            "status": data["status"],
            "current_step": data["current_step"],
            "data": json.loads(data["data_json"]) if data["data_json"] else {},
            "recommended_setup": json.loads(data["recommended_defaults"]) if data["recommended_defaults"] else None,
            "last_updated": data["updated_at"]
        }

def update_onboarding_step(user_id: str, step: str, raw_data: Dict[str, Any]) -> None:
    # 1. Validation & Schema Enforcement
    try:
        if step == "welcome":
            WelcomeData(**raw_data)
        elif step == "experience":
            ExperienceData(**raw_data)
        elif step == "risk":
            RiskData(**raw_data)
        elif step == "strategy":
            req = StrategySelectionData(**raw_data)
            validate_strategy_choice(req.strategy_id)
        elif step == "allocation":
            # Need previous answers to validate limits!
            current_state = get_onboarding_state(user_id)
            saved_data = current_state["data"]
            risk_tol = saved_data.get("risk_tolerance", "low") # Default low if missing
            
            req = AllocationData(**raw_data)
            validate_allocation(req.amount, req.type, risk_tol) # type: ignore
        elif step == "summary":
            pass
        else:
            raise ValueError(f"Unknown step {step}")
            
    except Exception as e:
        raise ValueError(f"Invalid data for step {step}: {str(e)}")

    # 2. Persistence
    db = DB()
    current = get_onboarding_state(user_id)
    merged_data = current["data"]
    merged_data.update(raw_data)
    
    # If starting, set status
    new_status = 'in_progress'
    if step == "welcome" and current["status"] == "not_started":
        new_status = 'in_progress'
    elif current["status"] == "completed":
        new_status = "completed" # Don't revert if already done? Or maybe allow re-editing?
        
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE onboarding_profiles 
            SET current_step = ?, 
                data_json = ?, 
                status = ?, 
                updated_at = ? 
            WHERE user_id = ?
            """,
            (step, json.dumps(merged_data), new_status, utc_now_iso(), user_id)
        )

def complete_onboarding(user_id: str) -> BotSetupBlueprint:
    """
    Finalizes onboarding, generates clamped setup, and saves it.
    """
    # 1. Get final collected data
    state = get_onboarding_state(user_id)
    data = state["data"]
    
    # Ensure all required fields exist
    try:
        exp_level: ExperienceLevel = data.get("experience_level", "beginner")
        risk_tol: RiskTolerance = data.get("risk_tolerance", "low")
        strat_id = data.get("strategy_id", "safe_trend")
        alloc_amt = data.get("amount", 100.0)
        alloc_type: AllocationModel = data.get("type", "fixed_amount")
    except KeyError:
        raise ValueError("Incomplete onboarding data. Cannot finalize.")

    # 2. Generate Logic
    # a. Strategy
    strategy_info = validate_strategy_choice(strat_id)
    
    # b. Risk Policy (Clamped)
    base_policy = get_risk_preset(risk_tol)
    clamped_policy = clamp_risk_policy(base_policy, exp_level)
    
    # c. Blueprint
    blueprint = BotSetupBlueprint(
        strategy_id=strat_id,
        strategy_name=strategy_info.name,
        risk_policy=clamped_policy,
        allocation_usdt=alloc_amt if alloc_type == "fixed_amount" else 0.0, # Placeholder
        allocation_type=alloc_type,
        allocation_value=alloc_amt
    )
    
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
            (blueprint.json(), now, now, user_id)
        )
        
    return blueprint

# ============================================================================
# 4. Decision Engine (Gating)
# ============================================================================

def get_next_steps(user_id: str) -> NextStepDecision:
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

    # 2. Check Subscription
    from app.core import billing_service
    # Assuming standard function signature, may need adjustment based on actual file
    try:
        sub = billing_service.get_user_subscription(user_id)
        # Mocking check for now as billing service might be minimal
        if sub and sub.get("status") != "active":
             blockers.append("SUBSCRIPTION_INACTIVE")
    except Exception:
        pass # Fail safe if billing service not fully ready
        
    # 3. Check KYC
    # Assuming kyc_policy exists based on previous file reads
    try:
        from shared_lib.core.policy.kyc_policy import check_kyc_gate, KYCAction
        allowed, msg = check_kyc_gate(user_id, KYCAction.START_LIVE_TRADING)
        if not allowed:
            blockers.append("KYC_REQUIRED")
    except ImportError:
        pass # Skip if not implemented yet

    # Recommendation Logic
    if "NO_BROKER" in blockers:
        action = "CONNECT_BROKER"
    elif "KYC_REQUIRED" in blockers:
        action = "COMPLETE_KYC"
    elif "SUBSCRIPTION_INACTIVE" in blockers:
        action = "MANAGE_BILLING"
    else:
        action = "CREATE_BOT"

    return NextStepDecision(
        ready_for_live=(len(blockers) == 0),
        blockers=blockers,
        next_action=action
    )
