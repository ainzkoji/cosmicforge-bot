from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field, validator

class BotConfigForex(BaseModel):
    base_currency: Literal["USD"] = Field("USD", description="Fixed to USD for now")
    forex_allowlist: List[str] = Field(..., min_items=1, description="List of allowed pairs e.g. ['EUR_USD']")
    leverage_cap: float = Field(30.0, le=50.0, description="Max leverage cap (default 30x, max 50x)")
    session_policy: Literal[True] = Field(True, description="Enforce market hours")

    @validator("forex_allowlist")
    def validate_pairs(cls, v):
        # Basic validation for pair format
        for pair in v:
            if "_" not in pair and "/" not in pair:
                 raise ValueError(f"Invalid pair format: {pair}. Expected 'EUR_USD' or similar.")
        return v

class BotInstanceCreate(BaseModel):
    name: str
    strategy_id: str
    broker_connection_id: str
    market_type: Literal["crypto", "forex", "stocks"] = "crypto"
    
    # Generic config bucket, validated based on market_type
    config: Dict[str, Any] = {}
    
    # Forex specific (optional in payload, but enforced if market_type=forex)
    forex_config: Optional[BotConfigForex] = None

    @validator("config")
    def validate_config(cls, v, values):
        return v

    @validator("forex_config", always=True)
    def validate_forex_config(cls, v, values):
        market = values.get("market_type")
        if market == "forex":
            if not v:
                # Apply safe defaults if missing: limit to EUR_USD, 30x leverage
                return BotConfigForex(forex_allowlist=["EUR_USD"], leverage_cap=30.0, session_policy=True)
            return v
        return v
