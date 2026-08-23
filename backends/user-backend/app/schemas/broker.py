from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, validator
from datetime import datetime

class BrokerExchange(str, Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    BINGX = "bingx"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    ALPACA = "alpaca"
    OANDA = "oanda"

class BrokerMarket(str, Enum):
    CRYPTO = "crypto"
    STOCKS = "stocks"
    FOREX = "forex"

class BrokerEnvironment(str, Enum):
    LIVE = "live"
    PRACTICE = "practice"
    DEMO = "demo"
    TESTNET = "testnet"

class BrokerCatalogItem(BaseModel):
    broker_id: str
    display_name: str
    market_types: List[str] # crypto, forex, stocks
    auth_types: List[str]   # api_key, oauth, mt5
    features: List[str]     # spot, futures, leverage
    required_permissions: List[str]
    is_available: bool = True
    unavailable_reason: Optional[str] = None
    affiliate_info: Optional[Dict[str, str]] = None

class BrokerCatalogResponse(BaseModel):
    brokers: List[BrokerCatalogItem]

class CreateBrokerAccountRequest(BaseModel):
    broker_id: str
    market_type: str
    label: Optional[str] = None
    environment: str = "live" # or demo

class BrokerAccountResponse(BaseModel):
    id: str
    broker_id: str
    market_type: str
    label: Optional[str]
    status: str
    environment: str
    account_type: Optional[str]
    capabilities: Optional[Dict[str, Any]]
    masked_key: Optional[str]
    last_validated_at: Optional[str]
    last_error: Optional[str] = None
    created_at: str

class BrokerCredentialsSubmit(BaseModel):
    # Dynamic fields depending on broker, but standardizing on a dict or specific common fields
    # For safety, we might want specific fields for validation
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    passphrase: Optional[str] = None
    
    # OANDA specific
    api_token: Optional[str] = None
    account_id: Optional[str] = None
    
    # MT5
    login: Optional[str] = None
    password: Optional[str] = None
    server: Optional[str] = None
    
    # Generic bucket for flexibility
    extras: Optional[Dict[str, Any]] = None

    # Environment overrides
    environment: Optional[str] = None

class OandaBrokerConnection(BaseModel):
    """Specific schema for validating OANDA connection payloads"""
    broker: str = Field(..., regex="^oanda$")
    market: str = Field(..., regex="^forex$")
    environment: BrokerEnvironment
    access_token: str = Field(..., min_length=10, description="OANDA v20 Personal Access Token")
    account_id: str = Field(..., description="OANDA Account ID (e.g. 101-001-1234567-001)")
    label: Optional[str] = None
    is_default: bool = False
    metadata: Optional[Dict[str, Any]] = None

    @validator("account_id")
    def validate_account_id(cls, v):
        # Basic OANDA v20 account ID format check: 3-3-7-3 digits often, but length check is safer minimum
        # Regex for standard format: ^\d{3}-\d{3}-\d{7}-\d{3}$
        # But we'll be lenient to start, mostly ensuring it's not empty and looks structurally okay
        if not v or len(v) < 5:
            raise ValueError("Invalid OANDA Account ID format")
        return v

class ValidationResponse(BaseModel):
    success: bool
    status: str
    capabilities: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None
    message: Optional[str] = None
