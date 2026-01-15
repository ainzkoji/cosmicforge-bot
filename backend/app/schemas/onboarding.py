from typing import Optional, List, Dict, Any
from pydantic import BaseModel

class OnboardingStepData(BaseModel):
    experience_level: Optional[str] = None # beginner, intermediate, advanced
    risk_tolerance: Optional[str] = None # low, medium, high
    strategy_preference: Optional[str] = None
    capital_allocation: Optional[float] = None
    capital_currency: Optional[str] = "USDT"
    allocation_model: Optional[str] = None # fixed_amount, percentage (future)

class OnboardingStateResponse(BaseModel):
    status: str
    current_step: Optional[str]
    data: Optional[OnboardingStepData]
    recommended_defaults: Optional[Dict[str, Any]] = None

class SaveStepRequest(BaseModel):
    step: str # welcome, experience, risk, strategy, capital, summary
    data: Dict[str, Any]

class StrategyItem(BaseModel):
    id: str
    name: str
    description: str
    difficulty: str
    tags: List[str]
    min_capital: float

class StrategyCatalogResponse(BaseModel):
    strategies: List[StrategyItem]

class NextStepDecision(BaseModel):
    can_proceed_to_live: bool
    blockers: List[str] # NO_BROKER, KYC_REQUIRED, etc.
    recommended_action: str
