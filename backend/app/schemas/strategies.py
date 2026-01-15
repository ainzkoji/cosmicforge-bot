from __future__ import annotations
from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field
from datetime import datetime

class StrategyDSLInput(BaseModel):
    id: str
    type: Literal["int", "float", "bool", "string", "select"]
    default: Any
    min: Optional[float] = None
    max: Optional[float] = None
    options: Optional[List[str]] = None

class StrategyDSLIndicator(BaseModel):
    id: str
    type: str  # e.g. "EMA", "RSI"
    period: Any  # Can be int or reference string "$input_id"
    source: str = "close"
    params: Optional[Dict[str, Any]] = None

class StrategyDSLCondition(BaseModel):
    left: str
    operator: Literal["crosses_above", "crosses_below", "greater_than", "less_than", "equals"]
    right: Any  # Can be value or reference

class StrategyDSL(BaseModel):
    mode: Literal["standard_v1"] = "standard_v1"
    inputs: List[StrategyDSLInput] = []
    indicators: List[StrategyDSLIndicator] = []
    entry_conditions: List[StrategyDSLCondition] = []
    exit_conditions: List[StrategyDSLCondition] = []
    
    # Optional advanced settings for future
    risk_management: Optional[Dict[str, Any]] = None

class StrategyVersionBase(BaseModel):
    version_number: int
    changelog: Optional[str] = None

class StrategyVersionCreate(StrategyVersionBase):
    spec_json: StrategyDSL
    param_schema_json: Optional[Dict[str, Any]] = None

class StrategyVersion(StrategyVersionBase):
    id: str
    strategy_id: str
    created_at: datetime
    
    # We return the full spec only on detail interactions usually
    spec_json: Optional[StrategyDSL] = None 
    param_schema_json: Optional[Dict[str, Any]] = None

class StrategyBase(BaseModel):
    name: str
    description: Optional[str] = None
    visibility: Literal["official", "community", "private", "premium"] = "private"
    market_types: List[str] = ["crypto"] # crypto, forex, stocks
    timeframes: List[str] = ["1m", "5m", "15m", "1h", "4h", "1d"]
    tags: List[str] = []
    recommended_risk_style: Optional[Literal["conservative", "aggressive", "moderate"]] = None
    entitlement_tier: str = "free"
    
class StrategyCreate(StrategyBase):
    constraints_json: Optional[Dict[str, Any]] = None
    # Initial version spec can be passed here
    initial_version: Optional[StrategyDSL] = None

class StrategyUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    status: Optional[str] = None # active, deprecated etc

class Strategy(StrategyBase):
    id: str
    owner_id: Optional[str] = None
    status: str # draft, active, deprecated, removed, under_review, rejected
    is_public: bool = False
    is_premium: bool = False
    
    # Cached metrics
    metrics_json: Optional[Dict[str, Any]] = None
    constraints_json: Optional[Dict[str, Any]] = None
    
    created_at: datetime
    updated_at: datetime
    
    # Relations
    versions: Optional[List[StrategyVersion]] = []
    latest_version: Optional[StrategyVersion] = None

class StrategyListResponse(BaseModel):
    items: List[Strategy]
    count: int
