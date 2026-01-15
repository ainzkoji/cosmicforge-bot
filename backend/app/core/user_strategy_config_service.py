"""
User Strategy Configuration Service

Manages user-specific strategy configurations that link strategies to broker accounts
with custom risk parameters and protection settings.
"""
from __future__ import annotations

import uuid
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from app.persistence.db import DB, utc_now_iso
from app.risk.risk_budget import RiskBudgetConfig, RiskProfile

logger = logging.getLogger(__name__)


# =========================
# Domain Models
# =========================
class UserStrategyConfig:
    """Represents a user's strategy configuration."""
    def __init__(self, data: Dict[str, Any]):
        self.id = data["id"]
        self.user_id = data["user_id"]
        self.broker_account_id = data["broker_account_id"]
        self.strategy_id = data["strategy_id"]
        self.name = data["name"]
        self.status = data["status"]
        self.created_at = data["created_at"]
        self.updated_at = data["updated_at"]
        self.activated_at = data.get("activated_at")


class RiskParameters:
    """Risk management parameters for a configuration."""
    def __init__(self, data: Dict[str, Any]):
        self.config_id = data["config_id"]
        self.risk_profile = data["risk_profile"]
        self.portfolio_risk_pct = float(data["portfolio_risk_pct"])
        self.per_trade_risk_pct = float(data["per_trade_risk_pct"])
        self.max_margin_usage_pct = float(data["max_margin_usage_pct"])
        self.max_drawdown_pct = float(data["max_drawdown_pct"])
        self.daily_loss_limit_pct = float(data["daily_loss_limit_pct"])
        self.position_sizing_method = data["position_sizing_method"]
        self.base_position_slots = int(data["base_position_slots"])
        self.max_position_slots = int(data["max_position_slots"])
        self.stop_loss_multiplier = float(data.get("stop_loss_multiplier", 2.0))
        self.take_profit_multiplier = float(data.get("take_profit_multiplier", 4.0))
        self.parameters_json = data.get("parameters_json", "{}")
        
    def to_risk_budget_config(self) -> RiskBudgetConfig:
        """Convert to RiskBudgetConfig for the trading engine."""
        return RiskBudgetConfig(
            portfolio_risk_pct=self.portfolio_risk_pct,
            per_trade_risk_pct=self.per_trade_risk_pct,
            max_margin_usage_pct=self.max_margin_usage_pct,
            base_slots=self.base_position_slots,
            max_slots=self.max_position_slots,
            drawdown_trigger_pct=self.max_drawdown_pct,
        )


# =========================
# Service
# =========================
class UserStrategyConfigService:
    """Service for managing user strategy configurations."""
    
    def __init__(self, db: DB):
        self.db = db
    
    def create_config(
        self, 
        user_id: str, 
        broker_account_id: str,
        strategy_id: str,
        name: str,
        risk_params: Dict[str, Any],
        strategy_params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new strategy configuration for a user.
        
        Args:
            user_id: User ID
            broker_account_id: Broker account ID to link to
            strategy_id: Strategy ID to use
            name: User-friendly name for this config
            risk_params: Risk parameters (risk_profile, portfolio_risk_pct, etc.)
            strategy_params: Optional strategy-specific parameter overrides
            
        Returns:
            config_id: The created configuration ID
        """
        config_id = f"config_{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()
        
        with self.db.connect() as conn:
            # 1. Create main config record
            conn.execute(
                """
                INSERT INTO user_strategy_configs (
                    id, user_id, broker_account_id, strategy_id, name, 
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (config_id, user_id, broker_account_id, strategy_id, name, 
                 "draft", now, now)
            )
            
            # 2. Create risk parameters
            conn.execute(
                """
                INSERT INTO risk_parameters (
                    config_id, risk_profile, portfolio_risk_pct, per_trade_risk_pct,
                    max_margin_usage_pct, max_drawdown_pct, daily_loss_limit_pct,
                    position_sizing_method, base_position_slots, max_position_slots,
                    stop_loss_multiplier, take_profit_multiplier, parameters_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    config_id,
                    risk_params.get("risk_profile", "balanced"),
                    risk_params.get("portfolio_risk_pct", 0.05),
                    risk_params.get("per_trade_risk_pct", 0.01),
                    risk_params.get("max_margin_usage_pct", 0.50),
                    risk_params.get("max_drawdown_pct", 0.15),
                    risk_params.get("daily_loss_limit_pct", 0.05),
                    risk_params.get("position_sizing_method", "risk_based"),
                    risk_params.get("base_position_slots", 5),
                    risk_params.get("max_position_slots", 20),
                    risk_params.get("stop_loss_multiplier", 2.0),
                    risk_params.get("take_profit_multiplier", 4.0),
                    json.dumps(risk_params.get("additional_params", {}))
                )
            )
            
            # 3. Create strategy parameters if provided
            if strategy_params:
                conn.execute(
                    "INSERT INTO strategy_parameters (config_id, overrides_json) VALUES (?, ?)",
                    (config_id, json.dumps(strategy_params))
                )
            else:
                conn.execute(
                    "INSERT INTO strategy_parameters (config_id, overrides_json) VALUES (?, ?)",
                    (config_id, "{}")
                )
            
            # 4. Create protection state record
            conn.execute(
                """
                INSERT INTO protection_state (
                    config_id, is_protected, daily_loss_today, consecutive_losses, updated_at
                ) VALUES (?, 0, 0.0, 0, ?)
                """,
                (config_id, now)
            )
        
        logger.info(f"Created strategy config {config_id} for user {user_id}")
        return config_id
    
    def get_config(self, config_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a configuration with all its parameters."""
        with self.db.connect() as conn:
            # Get main config
            row = conn.execute(
                "SELECT * FROM user_strategy_configs WHERE id = ?",
                (config_id,)
            ).fetchone()
            
            if not row:
                return None
            
            # Check ownership
            if user_id and row["user_id"] != user_id:
                return None
            
            config = dict(row)
            
            # Get risk parameters
            risk_row = conn.execute(
                "SELECT * FROM risk_parameters WHERE config_id = ?",
                (config_id,)
            ).fetchone()
            
            if risk_row:
                config["risk_parameters"] = dict(risk_row)
            
            # Get strategy parameters
            params_row = conn.execute(
                "SELECT * FROM strategy_parameters WHERE config_id = ?",
                (config_id,)
            ).fetchone()
            
            if params_row:
                config["strategy_parameters"] = json.loads(params_row["overrides_json"])
            
            # Get protection state
            protection_row = conn.execute(
                "SELECT * FROM protection_state WHERE config_id = ?",
                (config_id,)
            ).fetchone()
            
            if protection_row:
                config["protection_state"] = dict(protection_row)
            
            return config
    
    def list_configs(self, user_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all configurations for a user."""
        with self.db.connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM user_strategy_configs WHERE user_id = ? AND status = ? ORDER BY updated_at DESC",
                    (user_id, status)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM user_strategy_configs WHERE user_id = ? ORDER BY updated_at DESC",
                    (user_id,)
                ).fetchall()
            
            configs = []
            for row in rows:
                config = dict(row)
                
                # Get risk parameters summary
                risk_row = conn.execute(
                    "SELECT risk_profile, portfolio_risk_pct, daily_loss_limit_pct FROM risk_parameters WHERE config_id = ?",
                    (config["id"],)
                ).fetchone()
                
                if risk_row:
                    config["risk_summary"] = dict(risk_row)
                
                configs.append(config)
            
            return configs
    
    def update_config(
        self, 
        config_id: str, 
        user_id: str,
        updates: Dict[str, Any]
    ) -> bool:
        """Update a configuration."""
        with self.db.connect() as conn:
            # Verify ownership
            row = conn.execute(
                "SELECT user_id FROM user_strategy_configs WHERE id = ?",
                (config_id,)
            ).fetchone()
            
            if not row or row["user_id"] != user_id:
                return False
            
            now = utc_now_iso()
            
            # Update main config if name provided
            if "name" in updates:
                conn.execute(
                    "UPDATE user_strategy_configs SET name = ?, updated_at = ? WHERE id = ?",
                    (updates["name"], now, config_id)
                )
            
            # Update risk parameters if provided
            if "risk_parameters" in updates:
                risk_updates = updates["risk_parameters"]
                set_clauses = []
                params = []
                
                for key, value in risk_updates.items():
                    set_clauses.append(f"{key} = ?")
                    params.append(value)
                
                params.append(config_id)
                
                conn.execute(
                    f"UPDATE risk_parameters SET {', '.join(set_clauses)} WHERE config_id = ?",
                    params
                )
            
            # Update strategy parameters if provided
            if "strategy_parameters" in updates:
                conn.execute(
                    "UPDATE strategy_parameters SET overrides_json = ? WHERE config_id = ?",
                    (json.dumps(updates["strategy_parameters"]), config_id)
                )
            
            conn.execute(
                "UPDATE user_strategy_configs SET updated_at = ? WHERE id = ?",
                (now, config_id)
            )
        
        return True
    
    def activate_config(self, config_id: str, user_id: str) -> bool:
        """
        Activate a configuration for trading.
        Only one config can be active per broker account.
        """
        with self.db.connect() as conn:
            # Get config
            row = conn.execute(
                "SELECT user_id, broker_account_id FROM user_strategy_configs WHERE id = ?",
                (config_id,)
            ).fetchone()
            
            if not row or row["user_id"] != user_id:
                return False
            
            broker_account_id = row["broker_account_id"]
            now = utc_now_iso()
            
            # Deactivate any other active configs for this broker account
            conn.execute(
                """
                UPDATE user_strategy_configs 
                SET status = 'paused', updated_at = ?
                WHERE broker_account_id = ? AND status = 'active' AND id != ?
                """,
                (now, broker_account_id, config_id)
            )
            
            # Activate this config
            conn.execute(
                """
                UPDATE user_strategy_configs 
                SET status = 'active', activated_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, now, config_id)
            )
        
        logger.info(f"Activated config {config_id} for user {user_id}")
        return True
    
    def deactivate_config(self, config_id: str, user_id: str) -> bool:
        """Deactivate a configuration (pause trading)."""
        with self.db.connect() as conn:
            # Verify ownership
            row = conn.execute(
                "SELECT user_id FROM user_strategy_configs WHERE id = ?",
                (config_id,)
            ).fetchone()
            
            if not row or row["user_id"] != user_id:
                return False
            
            now = utc_now_iso()
            conn.execute(
                "UPDATE user_strategy_configs SET status = 'paused', updated_at = ? WHERE id = ?",
                (now, config_id)
            )
        
        logger.info(f"Deactivated config {config_id} for user {user_id}")
        return True
    
    def get_active_config_for_account(self, broker_account_id: str) -> Optional[Dict[str, Any]]:
        """Get the active configuration for a broker account."""
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT id FROM user_strategy_configs WHERE broker_account_id = ? AND status = 'active'",
                (broker_account_id,)
            ).fetchone()
            
            if not row:
                return None
            
            return self.get_config(row["id"])
    
    def get_risk_budget_config(self, config_id: str) -> Optional[RiskBudgetConfig]:
        """Get RiskBudgetConfig for a configuration."""
        config = self.get_config(config_id)
        if not config or "risk_parameters" not in config:
            return None
        
        risk_params = RiskParameters(config["risk_parameters"])
        return risk_params.to_risk_budget_config()
    
    @staticmethod
    def get_risk_profile_preset(profile: str) -> Dict[str, Any]:
        """Get preset risk parameters for a profile."""
        presets = {
            "conservative": {
                "risk_profile": "conservative",
                "portfolio_risk_pct": 0.02,
                "per_trade_risk_pct": 0.005,
                "max_margin_usage_pct": 0.35,
                "max_drawdown_pct": 0.10,
                "daily_loss_limit_pct": 0.03,
                "position_sizing_method": "risk_based",
                "base_position_slots": 3,
                "max_position_slots": 10,
                "stop_loss_multiplier": 2.0,
                "take_profit_multiplier": 6.0,
                "additional_params": {
                    "min_confidence_score": 0.8,
                    "volatility_filter_enabled": True
                }
            },
            "balanced": {
                "risk_profile": "balanced",
                "portfolio_risk_pct": 0.05,
                "per_trade_risk_pct": 0.01,
                "max_margin_usage_pct": 0.50,
                "max_drawdown_pct": 0.15,
                "daily_loss_limit_pct": 0.05,
                "position_sizing_method": "risk_based",
                "base_position_slots": 5,
                "max_position_slots": 20,
                "stop_loss_multiplier": 2.0,
                "take_profit_multiplier": 4.0,
                "additional_params": {
                    "min_confidence_score": 0.5,
                    "volatility_filter_enabled": True
                }
            },
            "aggressive": {
                "risk_profile": "aggressive",
                "portfolio_risk_pct": 0.10,
                "per_trade_risk_pct": 0.02,
                "max_margin_usage_pct": 0.65,
                "max_drawdown_pct": 0.25,
                "daily_loss_limit_pct": 0.10,
                "position_sizing_method": "risk_based",
                "base_position_slots": 8,
                "max_position_slots": 30,
                "stop_loss_multiplier": 1.5,
                "take_profit_multiplier": 3.0,
                "additional_params": {
                    "min_confidence_score": 0.3,
                    "volatility_filter_enabled": False,
                    "strict_circuit_breakers": True
                }
            }
        }
        
        return presets.get(profile, presets["balanced"])
