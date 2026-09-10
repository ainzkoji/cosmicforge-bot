"""
Bot Run Context - Per-Bot Configuration Model

Encapsulates all configuration needed to run a single bot instance.
Replaces global settings when running in multi-user mode.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class BotRunContext:
    """
    Per-bot runtime configuration.
    
    Used by PaperRunner to execute trades for a specific bot instance
    instead of using global settings.
    """
    # Identity
    user_id: str
    bot_instance_id: str
    broker_account_id: str
        
    # Trading Configuration
    symbols: List[str]  # List of symbols this bot trades
    strategy_id: str    # Strategy identifier (e.g., "sma_cross", "supertrend")
    
    # Run Identity (Has default, must be after non-defaults)
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    
    # Execution Settings
    execution_mode: str = "paper"  # "paper" or "broker"
    interval: str = "15m"          # Kline interval
    market_type: str = "CRYPTO"
    
    # Risk Limits (per-bot)
    risk_level: str = "balanced"
    max_leverage: float = 10.0
    daily_max_loss_usdt: float = 100.0
    max_open_positions: int = 3
    max_trades_daily: int = 6
    min_risk_reward: float = 1.8
    
    # Position Sizing
    trade_usdt_per_order: float = 10.0
    min_notional_usdt: float = 5.0
    
    # Stop Loss / Take Profit
    stop_loss_pct: float = 0.02    # 2%
    take_profit_pct: float = 0.04  # 4%
    
    # Broker Credentials (encrypted)
    broker_api_key: Optional[str] = None
    broker_api_secret: Optional[str] = None
    broker_base_url: Optional[str] = None
    broker_type: str = "binance"
    broker_tls_mode: str = "strict"  # "strict" or "insecure"
    broker_environment: Optional[str] = None  # "demo" | "live" — always from broker_accounts.environment (NEVER from credential blob)
    
    # Strategy-specific parameters
    strategy_params: Dict[str, Any] = field(default_factory=dict)
    
    # Allocation Config
    allocation_type: str = "fixed_usdt"  # "fixed_usdt", "percent_equity", "risk_based"
    allocation_value: float = 10.0
    capital_budget: float = 0.0
    risk_per_trade: float = 0.0
    max_weekly_drawdown_pct: float = 0.0
    max_monthly_drawdown_pct: float = 0.0
    higher_timeframe: str = "4h"
    effective_policy_hash: str = ""
    effective_policy: Any = None
    
    def __post_init__(self):
        """Validate context on creation."""
        if not self.user_id:
            raise ValueError("user_id is required")
        if not self.bot_instance_id:
            raise ValueError("bot_instance_id is required")
        if not self.symbols:
            raise ValueError("symbols list cannot be empty")
        if not self.strategy_id:
            raise ValueError("strategy_id is required")
        
        # Ensure symbols are uppercase
        self.symbols = [s.upper() for s in self.symbols]
        self.market_type = str(self.market_type or "UNKNOWN").upper()
    
    @classmethod
    def from_bot_instance(
        cls,
        instance: Any,
        broker_credentials: Dict[str, str],
        risk_params: Any = None,
        account_environment: Optional[str] = None,
    ) -> BotRunContext:
        """
        Create BotRunContext from a BotInstance model + Risk Params.

        Args:
            instance:            BotInstance ORM object
            broker_credentials:  Decrypted credential dict (from resolver or legacy shim)
            risk_params:         Optional risk parameter dict
            account_environment: Authoritative environment from broker_accounts.environment.
                                 MUST be passed from the account row — never from the
                                 credential blob.  Defaults to "live" if omitted.
        """
        """
        Create BotRunContext from a BotInstance model + Risk Params.
        """
        # Parse symbols from instance config
        # Parse symbols from instance config — handle list, JSON array string, or CSV string
        import json as _json
        raw_syms = instance.symbols
        if isinstance(raw_syms, list):
            symbols = raw_syms
        else:
            raw_syms = str(raw_syms or "").strip()
            if raw_syms.startswith("["):
                # JSON array: '["BTCUSDT","ETHUSDT"]'
                try:
                    symbols = _json.loads(raw_syms)
                except Exception:
                    symbols = [s.strip().strip('"') for s in raw_syms.strip("[]").split(",") if s.strip()]
            else:
                # Plain CSV: 'BTCUSDT,ETHUSDT'
                symbols = [s.strip() for s in raw_syms.split(",") if s.strip()]
        
        # Parse strategy params
        strategy_params = {}
        
        # Extract risk values
        daily_loss_pct = 0.05
        max_trades = 20
        max_pos = 5
        lev = 20.0
        stop_pct = 0.02
        tp_pct = 0.04
        risk_level = instance.risk_level if hasattr(instance, "risk_level") else "medium"
        
        if risk_params:
            # risk_params should be a dict (from preset)
            def get_val(obj, key, default):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            daily_loss_pct = float(get_val(risk_params, "daily_loss_limit_pct", 0.05))
            max_pos = int(get_val(risk_params, "max_position_slots", 5))
            stop_pct = float(get_val(risk_params, "stop_loss_multiplier", 2.0)) * 0.01 # Approximate conversion? No, preset is multiplier. 
            # Actually Orchestrator handles SL/TP. Context is for PaperRunner limits.
            # Using defaults for now, Orchestrator enforces dynamic SL.
            
        # Calc Daily Loss USDT
        # If capital_allocation is provided, use it. Else default base.
        if instance.capital_allocation is None or float(instance.capital_allocation or 0.0) <= 0:
            raise ValueError("CAPITAL_BUDGET_REQUIRED: explicit capital_allocation is required")
        capital = float(instance.capital_allocation)
        daily_max_loss_usdt = capital * daily_loss_pct
            
        # Helper for interval
        interval = "1m"
        if instance.timeframes and len(instance.timeframes) > 0:
            interval = instance.timeframes[0]
            
        # Trade Config
        trade_usdt = 10.0
        if instance.allocation_type == "fixed_amount":
            trade_usdt = float(instance.allocation_value)
        elif instance.allocation_type == "percent_balance":
            trade_usdt = 100.0 

        # Check if Auto Pilot
        if instance.strategy_id == "master_ensemble":
            # Auto Pilot Specific Constraints
            lev = 10.0
            # Normalize risk level (UI presets -> Internal Enums)
            MAPPING = {
                "aggressive": "high",
                "balanced": "medium",
                "conservative": "low"
            }
            risk_level = MAPPING.get(risk_level.lower(), risk_level)
            
            # Ensure risk level is valid, fallback to medium
            if risk_level not in ("low", "medium", "high"):
                risk_level = "medium"
        
        return cls(
            user_id=instance.user_id,
            bot_instance_id=instance.id,
            broker_account_id=instance.broker_account_id,
            symbols=symbols,
            strategy_id=instance.strategy_id,
            execution_mode=instance.mode or "paper",
            interval=interval,
            market_type=getattr(instance, "market_type", "UNKNOWN"),
            risk_level=risk_level,
            max_leverage=lev,
            daily_max_loss_usdt=daily_max_loss_usdt,
            max_open_positions=max_pos,
            max_trades_daily=max_trades,
            min_risk_reward=float(get_val(risk_params, "min_risk_reward", 0.0)) if risk_params else 0.0,
            trade_usdt_per_order=trade_usdt,
            min_notional_usdt=5.0,
            stop_loss_pct=stop_pct,
            take_profit_pct=tp_pct,
            broker_api_key=broker_credentials.get("api_key") or broker_credentials.get("bridge_token"),
            broker_api_secret=broker_credentials.get("api_secret"),
            broker_base_url=broker_credentials.get("base_url") or broker_credentials.get("bridge_url"),
            broker_type=broker_credentials.get("broker_type", "binance"),
            broker_tls_mode=broker_credentials.get("tls_mode", "strict") if broker_credentials.get("tls_mode") == "insecure" else "strict",
            # CRITICAL: use account_environment (from broker_accounts.environment — authoritative)
            # Never use broker_credentials["environment"] — that is the blob value and may be stale.
            broker_environment=account_environment or "live",
            strategy_params=strategy_params,
            allocation_type=instance.allocation_type or "fixed_usdt",
            allocation_value=float(instance.allocation_value or 10.0),
            capital_budget=capital,
        )

    @classmethod
    def from_effective_policy(
        cls,
        policy: Any,
        broker_credentials: Dict[str, str],
    ) -> "BotRunContext":
        """Build the legacy runner context without re-resolving policy values."""
        risk_map = {"conservative": "low", "balanced": "medium", "medium": "medium", "aggressive": "high"}
        if policy.position_allocation_type == "fixed_amount":
            trade_usdt = policy.position_allocation_value
        else:
            trade_usdt = policy.capital_budget * policy.position_allocation_value / 100.0
        return cls(
            user_id=policy.user_id,
            bot_instance_id=policy.bot_instance_id,
            broker_account_id=policy.broker_account_id,
            symbols=list(policy.symbols),
            strategy_id=policy.strategy_id,
            execution_mode=policy.execution_mode,
            interval=policy.timeframe,
            market_type=policy.market_type,
            risk_level=risk_map.get(policy.risk_level, policy.risk_level),
            max_leverage=policy.max_leverage,
            daily_max_loss_usdt=policy.max_daily_loss,
            max_open_positions=policy.max_open_positions,
            max_trades_daily=policy.max_daily_trades,
            min_risk_reward=policy.min_risk_reward,
            trade_usdt_per_order=trade_usdt,
            min_notional_usdt=policy.minimum_notional,
            stop_loss_pct=policy.stop_loss_fraction,
            take_profit_pct=max(policy.stop_loss_fraction * policy.min_risk_reward, policy.stop_loss_fraction),
            broker_api_key=broker_credentials.get("api_key") or broker_credentials.get("bridge_token"),
            broker_api_secret=broker_credentials.get("api_secret"),
            broker_base_url=broker_credentials.get("base_url") or broker_credentials.get("bridge_url"),
            broker_type=broker_credentials.get("broker_type", "binance"),
            broker_tls_mode=broker_credentials.get("tls_mode", "strict") if broker_credentials.get("tls_mode") == "insecure" else "strict",
            broker_environment=policy.broker_environment,
            allocation_type=policy.position_allocation_type,
            allocation_value=policy.position_allocation_value,
            capital_budget=policy.capital_budget,
            risk_per_trade=policy.risk_per_trade,
            max_weekly_drawdown_pct=policy.max_weekly_drawdown,
            max_monthly_drawdown_pct=policy.max_monthly_drawdown,
            higher_timeframe=policy.higher_timeframe,
            effective_policy_hash=policy.policy_hash,
            effective_policy=policy,
        )
    
    def get_trade_amount_settings(self) -> tuple:
        """
        Map BotRunContext allocation fields to PolicyContext trade amount mode/value.
        
        Returns:
            (trade_amount_mode, trade_amount_value)
            
        Mapping:
        - "fixed_usdt" or "fixed_amount" -> ("fixed", value in USDT)
        - "percent_balance" or "percent_equity" or "percent" -> ("percent", value as %)
        - "risk_based" or anything else -> ("atr_risk", 0.0)
        """
        allocation = self.allocation_type.lower() if self.allocation_type else ""
        
        if allocation in ("fixed_usdt", "fixed_amount", "fixed"):
            return "fixed", self.allocation_value
        elif allocation in ("percent_balance", "percent_equity", "percent"):
            return "percent", self.allocation_value
        else:
            # Default: use ATR-based risk sizing
            return "atr_risk", 0.0

