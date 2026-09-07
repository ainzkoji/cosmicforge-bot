from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from datetime import datetime

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
    
    # MT5
    login: Optional[str] = None
    password: Optional[str] = None
    server: Optional[str] = None
    
    # Generic bucket for flexibility
    extras: Optional[Dict[str, Any]] = None

class ValidationResponse(BaseModel):
    success: bool
    status: str
    capabilities: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None
    message: Optional[str] = None
