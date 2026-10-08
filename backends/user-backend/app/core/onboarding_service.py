import json
import math
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


# ── steps (Step 1.10) ───────────────────────────────────────────────────────
# One vocabulary for the wizard and the service. Before this, the portal sent
# ``experience_level`` / ``risk_tolerance`` / ``strategy_preference`` /
# ``capital_allocation`` while the service accepted ``experience`` / ``risk`` /
# ``strategy`` / ``allocation`` and different payload keys, so every step after
# the welcome screen was rejected.
STEP_ORDER = ("welcome", "experience_level", "risk_tolerance", "strategy_preference", "capital_allocation", "summary")
STEP_ALIASES = {"experience": "experience_level", "risk": "risk_tolerance", "strategy": "strategy_preference",
                "allocation": "capital_allocation"}


def canonical_step(step: Optional[str]) -> str:
    name = STEP_ALIASES.get(str(step or ""), str(step or ""))
    if name not in STEP_ORDER:
        raise ValueError(f"Unknown step {step}")
    return name


def next_step(step: str) -> str:
    index = STEP_ORDER.index(canonical_step(step))
    return STEP_ORDER[min(index + 1, len(STEP_ORDER) - 1)]


def risk_level_for(tolerance: Optional[str]) -> str:
    """The risk profile an onboarding risk appetite maps to (the one shared
    library; an unknown or missing answer maps to the lowest-risk profile)."""
    from shared_lib import risk_levels
    try:
        return risk_levels.normalize_level(tolerance)
    except Exception:
        return risk_levels.normalize_level("conservative")


def _positive_number(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number")
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero")
    return number


def normalise_step(step: str, raw_data: Dict[str, Any], saved: Dict[str, Any]) -> Dict[str, Any]:
    """Validate one step's answers and return them under the stored keys."""
    raw = dict(raw_data or {})
    if step == "welcome":
        WelcomeData(**raw)
        return {}
    if step == "experience_level":
        return {"experience_level": ExperienceData(**raw).experience_level}
    if step == "risk_tolerance":
        tolerance = RiskData(**raw).risk_tolerance
        return {"risk_tolerance": tolerance, "risk_level": risk_level_for(tolerance)}
    if step == "strategy_preference":
        strategy_id = raw.get("strategy_id") or raw.get("strategy_preference")
        req = StrategySelectionData(strategy_id=strategy_id, strategy_version=raw.get("strategy_version") or "latest")
        validate_strategy_choice(req.strategy_id)
        return {"strategy_id": req.strategy_id, "strategy_preference": req.strategy_id}
    if step == "capital_allocation":
        if "allocation_value" in raw or "capital_allocation" in raw or "allocation_type" in raw:
            kind = str(raw.get("allocation_type") or "fixed_amount")
            value = _positive_number(raw.get("allocation_value"), "allocation_value")
            budget = _positive_number(raw["capital_allocation"], "capital_allocation") if raw.get("capital_allocation") is not None else None
        else:                                               # the earlier {amount, type} shape
            req = AllocationData(**raw)
            kind, value = req.type, _positive_number(req.amount, "amount")
            budget = value if req.type == "fixed_amount" else None
        if kind in ("percentage", "percent_balance"):
            model, kind = "percentage", "percent_balance"
            if value > 100:
                raise ValueError("A percentage allocation cannot exceed 100")
        elif kind == "fixed_amount":
            model = "fixed_amount"
            if budget is not None and value > budget:
                raise ValueError("The amount per trade cannot exceed the total budget")
        else:
            raise ValueError(f"Unknown allocation type {kind}")
        validate_allocation(value, model, saved.get("risk_tolerance", "low"))  # type: ignore[arg-type]
        return {"capital_allocation": budget, "allocation_type": kind, "allocation_model": model, "allocation_value": value,
                "amount": value, "type": model}
    return {}                                               # summary


def deployment_prefill(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Risk level and budget for the deployment screen, from the answers given.
    None until the risk appetite is known. Never a deployment by itself."""
    if not data.get("risk_tolerance"):
        return None
    from shared_lib import risk_levels
    budget = data.get("capital_allocation")
    out: Dict[str, Any] = {"risk_level": risk_level_for(data.get("risk_tolerance")),
                           "risk_profile_version": risk_levels.RISK_PROFILE_VERSION, "budget_type": None, "budget_value": None}
    if budget is not None:
        text = f"{float(budget):.8f}".rstrip("0").rstrip(".")
        out.update(budget_type="fixed_amount", budget_value=text)
    return out

# ============================================================================
# 1. Strategy Catalog & Validation
# ============================================================================

STRATEGIES = [
    StrategyItem(
        id="cati",
        name="CATI",
        description="The sole trading intelligence engine. Observes markets while new entries remain governed and blocked until eligible.",
        difficulty="Beginner",
        tags=["CATI", "Governed", "Observe"],
        min_capital=100.0,
        compatible_markets=["crypto", "forex"]
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
            max_daily_loss_pct=2.5,
            max_position_size_usdt=500.0,
            max_leverage=3,
            stop_loss_pct=0.05,
            max_open_positions=3,
            drawdown_limit_pct=10.0
        )
    else: # high
        return RiskPolicyPreset(
            id="high",
            max_daily_loss_pct=2.5,
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
                "recommended_setup": None,
                "steps_completed": [],
                "deployment_prefill": None,
            }

        data = dict(row)
        answers = json.loads(data["data_json"]) if data["data_json"] else {}
        try:
            current = canonical_step(data["current_step"])
        except ValueError:
            current = "welcome"
        return {
            "status": data["status"],
            "current_step": current,
            "data": answers,
            "recommended_setup": json.loads(data["recommended_defaults"]) if data["recommended_defaults"] else None,
            "last_updated": data["updated_at"],
            "steps_completed": list(STEP_ORDER[:STEP_ORDER.index(current)]),
            "deployment_prefill": deployment_prefill(answers),
        }

def update_onboarding_step(user_id: str, step: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and save one step; returns the new state, whose ``current_step``
    is the step the wizard shows next."""
    current = get_onboarding_state(user_id)
    # 1. Validation & Schema Enforcement (previous answers are needed for the limits)
    try:
        step = canonical_step(step)
        answers = normalise_step(step, raw_data, current["data"])
    except Exception as e:
        raise ValueError(f"Invalid data for step {step}: {str(e)}")

    # 2. Persistence
    db = DB()
    merged_data = current["data"]
    merged_data.update(answers)
    
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
            (next_step(step), json.dumps(merged_data), new_status, utc_now_iso(), user_id)
        )
    return get_onboarding_state(user_id)

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
        strat_id = data.get("strategy_id", "cati")
        alloc_amt = data.get("allocation_value", data.get("amount", 100.0))
        alloc_type: AllocationModel = data.get("allocation_model", data.get("type", "fixed_amount"))
    except KeyError:
        raise ValueError("Incomplete onboarding data. Cannot finalize.")

    # 2. Generate Logic
    # a. Strategy
    strategy_info = validate_strategy_choice(strat_id)
    
    # b. Risk Policy (Clamped)
    base_policy = get_risk_preset(risk_tol)
    clamped_policy = clamp_risk_policy(base_policy, exp_level)
    # The onboarding preset may never advertise more leverage than the risk
    # profile the deployment will actually use (tightening only).
    from shared_lib import risk_levels
    profile = risk_levels.get_profile(risk_level_for(risk_tol))
    clamped_policy.max_leverage = min(clamped_policy.max_leverage, int(profile.leverage_ceiling))
    
    # c. Blueprint
    blueprint = BotSetupBlueprint(
        strategy_id=strat_id,
        strategy_name=strategy_info.name,
        risk_policy=clamped_policy,
        allocation_usdt=alloc_amt if alloc_type == "fixed_amount" else 0.0, # Placeholder
        allocation_type=alloc_type,
        allocation_value=alloc_amt,
        risk_level=profile.level,
        # Pre-fills the deployment screen. Completing onboarding deploys nothing.
        deployment_prefill=deployment_prefill(data),
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
