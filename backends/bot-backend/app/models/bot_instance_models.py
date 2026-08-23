"""
BotInstance Models

Database models for multi-user bot instance execution contexts.
Each BotInstance binds a user, broker account, strategy, and execution parameters.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import json


class BotHealthStatus:
    TRADING = "TRADING"
    PAUSED_RISK_LIMIT = "PAUSED_RISK_LIMIT"
    PAUSED_CIRCUIT_BREAKER = "PAUSED_CIRCUIT_BREAKER"
    PAUSED_KILL_SWITCH = "PAUSED_KILL_SWITCH"
    PAUSED_EVENT_BLACKOUT = "PAUSED_EVENT_BLACKOUT"
    PAUSED_CONSECUTIVE_LOSS_COOLDOWN = "PAUSED_CONSECUTIVE_LOSS_COOLDOWN"
    PAUSED_MAX_DAILY_TRADES = "PAUSED_MAX_DAILY_TRADES"
    PAUSED_MAX_OPEN_POSITIONS = "PAUSED_MAX_OPEN_POSITIONS"
    ERROR_SIZING_FAILURE = "ERROR_SIZING_FAILURE"
    ERROR_EXCHANGE_DISCONNECTED = "ERROR_EXCHANGE_DISCONNECTED"
    ERROR_STRATEGY_UNAVAILABLE = "ERROR_STRATEGY_UNAVAILABLE"
    ERROR_EXECUTION_FAILURE = "ERROR_EXECUTION_FAILURE"
    WAITING_FOR_SETUP = "WAITING_FOR_SETUP"
    UNKNOWN = "UNKNOWN"


@dataclass
class BotInstance:
    """
    BotInstance represents a user's active strategy execution context.
    
    Each instance ties together:
    - User identity
    - Broker account
    - Strategy configuration
    - Risk profile
    - Execution symbols and timeframes
    - Runtime state
    """
    id: str  # UUID
    user_id: str
    broker_account_id: str
    market_type: str  # CRYPTO, FOREX
    strategy_id: str
    strategy_version: str
    
    # Auto Pilot Configuration
    risk_level: str  # conservative, balanced, aggressive
    
    # Legacy / Optional
    config_id: Optional[str] = None  # Deprecated
    risk_profile_id: Optional[str] = None  # Deprecated
    
    # Execution context (stored as JSON in DB)
    symbols: List[str] = field(default_factory=list)
    timeframes: List[str] = field(default_factory=list)
    allocation_type: str = "fixed_amount"  # percent_balance, fixed_amount
    allocation_value: float = 0.0
    
    # Lifecycle
    mode: str = "paper" # paper, live
    status: str = "active" # active, paused, stopped, error
    created_at: str = ""
    updated_at: str = ""
    
    capital_allocation: Optional[float] = None # Global budget cap
    capital_allocation_type: str = "fixed_amount" # percent_balance or fixed_amount (default)
    started_at: Optional[str] = None
    stopped_at: Optional[str] = None
    
    # Runtime state
    last_run_at: Optional[str] = None
    last_error: Optional[str] = None
    total_trades: int = 0
    active_positions: int = 0
    broker_id: Optional[str] = None  # Joined from broker_accounts
    
    # Broker health and validation state
    broker_health_status: str = "ok"
    broker_error_code: Optional[str] = None
    broker_blocked_at: Optional[str] = None
    
    # Explicit block state observability fields
    block_category: Optional[str] = None
    block_reason_code: Optional[str] = None
    block_reason_detail: Optional[str] = None
    blocked_since: Optional[str] = None
    last_validated_at: Optional[str] = None
    last_validation_error: Optional[str] = None

    # Section F — user-facing bot health (product safety)
    bot_health_status: str = BotHealthStatus.UNKNOWN
    bot_health_message: Optional[str] = None
    bot_health_reason_code: Optional[str] = None
    bot_health_recommended_action: Optional[str] = None
    bot_health_updated_at: Optional[str] = None
    last_warning: Optional[str] = None
    
    @classmethod
    def from_db_row(cls, row: Any) -> BotInstance:
        """Create BotInstance from database row."""
        # Convert sqlite3.Row to dict to allow .get() and debugging
        d = dict(row)
        
        return cls(
            id=d["id"],
            user_id=d["user_id"],
            broker_account_id=d["broker_account_id"],
            market_type=d["market_type"],
            strategy_id=d["strategy_id"],
            strategy_version=d["strategy_version"],
            risk_level=d.get("risk_level", "balanced"), # Default for migration
            config_id=d.get("config_id"),
            risk_profile_id=d.get("risk_profile_id"),
            symbols=json.loads(d.get("symbols_json") or "[]"),
            timeframes=json.loads(d.get("timeframes_json") or "[]"),
            allocation_type=d["allocation_type"],
            allocation_value=d["allocation_value"],
            capital_allocation=d.get("capital_allocation"), 
            capital_allocation_type=d.get("capital_allocation_type", "fixed_amount"),
            mode=d["mode"],
            status=d["status"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            started_at=d.get("started_at"),
            stopped_at=d.get("stopped_at"),
            last_run_at=d.get("last_run_at"),
            last_error=d.get("last_error"),
            total_trades=d.get("total_trades", 0),
            active_positions=d.get("active_positions", 0),
            broker_id=d.get("broker_id"),
            broker_health_status=d.get("broker_health_status", "ok"),
            broker_error_code=d.get("broker_error_code"),
            broker_blocked_at=d.get("broker_blocked_at"),
            block_category=d.get("block_category"),
            block_reason_code=d.get("block_reason_code"),
            block_reason_detail=d.get("block_reason_detail"),
            blocked_since=d.get("blocked_since"),
            last_validated_at=d.get("last_validated_at"),
            last_validation_error=d.get("last_validation_error"),
            bot_health_status=d.get("bot_health_status", BotHealthStatus.UNKNOWN),
            bot_health_message=d.get("bot_health_message"),
            bot_health_reason_code=d.get("bot_health_reason_code"),
            bot_health_recommended_action=d.get("bot_health_recommended_action"),
            bot_health_updated_at=d.get("bot_health_updated_at"),
            last_warning=d.get("last_warning"),
        )
    
    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "broker_account_id": self.broker_account_id,
            "market_type": self.market_type,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "risk_level": self.risk_level,
            "config_id": self.config_id,
            "risk_profile_id": self.risk_profile_id,
            "symbols_json": json.dumps(self.symbols),
            "timeframes_json": json.dumps(self.timeframes),
            "allocation_type": self.allocation_type,
            "allocation_value": self.allocation_value,
            "capital_allocation": self.capital_allocation,
            "capital_allocation_type": self.capital_allocation_type,
            "mode": self.mode,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "last_run_at": self.last_run_at,
            "last_error": self.last_error,
            "total_trades": self.total_trades,
            "active_positions": self.active_positions,
            "broker_health_status": self.broker_health_status,
            "broker_error_code": self.broker_error_code,
            "broker_blocked_at": self.broker_blocked_at,
            "block_category": self.block_category,
            "block_reason_code": self.block_reason_code,
            "block_reason_detail": self.block_reason_detail,
            "blocked_since": self.blocked_since,
            "last_validated_at": self.last_validated_at,
            "last_validation_error": self.last_validation_error
            ,
            "bot_health_status": self.bot_health_status,
            "bot_health_message": self.bot_health_message,
            "bot_health_reason_code": self.bot_health_reason_code,
            "bot_health_recommended_action": self.bot_health_recommended_action,
            "bot_health_updated_at": self.bot_health_updated_at,
            "last_warning": self.last_warning,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        # Compute lifecycle category for UI
        lifecycle_cat = "inactive"
        if self.status in ("active", "error") and self.broker_health_status == "broker_blocked":
            lifecycle_cat = "blocked"
        elif self.status == "active":
            lifecycle_cat = "tradable"
        elif self.status == "archived":
            lifecycle_cat = "archived"
        elif self.status == "deleted":
            lifecycle_cat = "deleted"

        return {
            "id": self.id,
            "lifecycle_category": lifecycle_cat,
            "user_id": self.user_id,
            "broker_account_id": self.broker_account_id,
            "market_type": self.market_type,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "risk_level": self.risk_level,
            "config_id": self.config_id,
            "symbols": self.symbols,
            "timeframes": self.timeframes,
            "allocation_type": self.allocation_type,
            "allocation_value": self.allocation_value,
            "capital_allocation": self.capital_allocation,
            "capital_allocation_type": self.capital_allocation_type,
            "mode": self.mode,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "last_run_at": self.last_run_at,
            "last_error": self.last_error,
            "total_trades": self.total_trades,
            "active_positions": self.active_positions,
            "broker_id": self.broker_id,
            "broker_health_status": self.broker_health_status,
            "block_category": self.block_category,
            "block_reason_code": self.block_reason_code,
            "block_reason_detail": self.block_reason_detail,
            "blocked_since": self.blocked_since,
            "last_validated_at": self.last_validated_at,
            "last_validation_error": self.last_validation_error
            ,
            "bot_health_status": self.bot_health_status,
            "bot_health_message": self.bot_health_message,
            "bot_health_reason_code": self.bot_health_reason_code,
            "bot_health_recommended_action": self.bot_health_recommended_action,
            "bot_health_updated_at": self.bot_health_updated_at,
            "last_warning": self.last_warning,
        }


@dataclass
class CreateBotInstanceRequest:
    """Request data for creating a new bot instance."""
    user_id: str
    broker_account_id: str
    market_type: str
    strategy_id: str
    strategy_version: str
    risk_level: str
    symbols: List[str]
    timeframes: List[str]
    allocation_type: str
    allocation_value: float
    mode: str  # paper or live
    config_id: Optional[str] = None
    risk_profile_id: Optional[str] = None
    capital_allocation: Optional[float] = None
    capital_allocation_type: str = "fixed_amount"
    
    def validate(self) -> List[str]:
        """Validate the request data."""
        errors = []
        
        if not self.user_id:
            errors.append("user_id is required")
        
        if not self.broker_account_id:
            errors.append("broker_account_id is required")
        
        if self.market_type not in ["CRYPTO", "FOREX"]:
            errors.append("market_type must be CRYPTO or FOREX")
        
        if not self.strategy_id:
            errors.append("strategy_id is required")
        
        if not self.symbols or len(self.symbols) == 0:
            errors.append("At least one symbol is required")
        
        if not self.timeframes or len(self.timeframes) == 0:
            errors.append("At least one timeframe is required")
        
        if self.allocation_type not in ["percent_balance", "fixed_amount"]:
            errors.append("allocation_type must be percent_balance or fixed_amount")
        
        if self.allocation_type == "percent_balance":
            if self.allocation_value <= 0 or self.allocation_value > 100:
                errors.append("allocation_value must be between 0 and 100 for percent_balance")
        else:
            if self.allocation_value <= 0:
                errors.append("allocation_value must be positive for fixed_amount")
        
        if self.mode not in ["paper", "live"]:
            errors.append("mode must be paper or live")
            
        if self.capital_allocation_type not in ["percent_balance", "fixed_amount"]:
            errors.append("capital_allocation_type must be percent_balance or fixed_amount")
            
        if self.capital_allocation_type == "percent_balance":
            if self.capital_allocation and (self.capital_allocation <= 0 or self.capital_allocation > 100):
                errors.append("capital_allocation must be between 0 and 100 for percent_balance")
        else:
            if self.capital_allocation and self.capital_allocation <= 0:
                errors.append("capital_allocation must be positive for fixed_amount")
        
        return errors
