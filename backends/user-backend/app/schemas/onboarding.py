from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field, root_validator

# --- Enums & Consts ---
ExperienceLevel = Literal["beginner", "intermediate", "advanced"]
RiskTolerance = Literal["low", "medium", "high"]
AllocationModel = Literal["fixed_amount", "percentage"]

# --- Step Specific Data Models ---

class ExperienceData(BaseModel):
    experience_level: ExperienceLevel

class RiskData(BaseModel):
    risk_tolerance: RiskTolerance

class StrategySelectionData(BaseModel):
    strategy_id: str
    strategy_version: Optional[str] = "latest"

class AllocationData(BaseModel):
    amount: float = Field(..., gt=0, description="Amount in USDT or Percentage (0-100)")
    type: AllocationModel = "fixed_amount"

class WelcomeData(BaseModel):
    # Just a marker for the welcome step, maybe user accepted terms
    accepted_terms: bool = True

# --- Policies & Defaults Objects ---

class RiskPolicyPreset(BaseModel):
    id: str  # low, medium, high
    max_daily_loss_pct: float
    max_position_size_usdt: float
    max_leverage: int
    stop_loss_pct: float
    max_open_positions: int
    drawdown_limit_pct: float
    
class BotSetupBlueprint(BaseModel):
    strategy_id: str
    strategy_name: str
    risk_policy: RiskPolicyPreset
    allocation_usdt: float  # Estimated if percentage
    allocation_type: AllocationModel
    allocation_value: float
    
# --- API Requests/Responses ---

class OnboardingStepData(BaseModel):
    # This remains as a flexible container for the "state" response
    # But ideally constructed from the smaller models
    experience_level: Optional[ExperienceLevel] = None
    risk_tolerance: Optional[RiskTolerance] = None
    strategy_preference: Optional[str] = None
    capital_allocation: Optional[float] = None
    capital_currency: Optional[str] = "USDT"
    allocation_model: Optional[AllocationModel] = None

class OnboardingStateResponse(BaseModel):
    status: str
    current_step: Optional[str]
    data: Optional[OnboardingStepData]
    recommended_setup: Optional[BotSetupBlueprint] = None
    last_updated: Optional[str] = None

class SaveStepRequest(BaseModel):
    # Step name determines validation in service layer
    step: Literal["welcome", "experience", "risk", "strategy", "allocation", "summary"]
    data: Dict[str, Any]

class StrategyItem(BaseModel):
    id: str
    name: str
    description: str
    difficulty: str
    tags: List[str]
    min_capital: float
    compatible_markets: List[str] = ["crypto"]

class StrategyCatalogResponse(BaseModel):
    strategies: List[StrategyItem]

class NextStepDecision(BaseModel):
    ready_for_live: bool
    blockers: List[str] # NO_BROKER, KYC_REQUIRED, PLAN_LIMIT, etc.
    next_action: str # CONNECT_BROKER, UPGRADE_PLAN, CREATE_BOT
