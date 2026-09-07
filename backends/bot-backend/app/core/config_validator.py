"""
Custom Strategy Configuration Validator

Validates user-created custom strategies against system limits and logical constraints.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of strategy configuration validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    clamped_config: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "clamped_config": self.clamped_config,
        }


class CustomStrategyValidator:
    """
    Validator for custom strategy specifications.
    
    Ensures:
    - Required fields are present
    - Indicator parameters are within allowed ranges
    - Entry/exit conditions are logically consistent
    - System limits are respected
    """
    
    # Supported indicators and their parameter constraints
    SUPPORTED_INDICATORS = {
        "ema": {
            "required_params": ["period"],
            "param_constraints": {
                "period": {"type": int, "min": 5, "max": 200}
            }
        },
        "sma": {
            "required_params": ["period"],
            "param_constraints": {
                "period": {"type": int, "min": 5, "max": 200}
            }
        },
        "rsi": {
            "required_params": ["period"],
            "param_constraints": {
                "period": {"type": int, "min": 2, "max": 50},
                "overbought": {"type": int, "min": 60, "max": 90, "default": 70},
                "oversold": {"type": int, "min": 10, "max": 40, "default": 30},
            }
        },
        "bollinger_bands": {
            "required_params": ["period", "std_dev"],
            "param_constraints": {
                "period": {"type": int, "min": 5, "max": 50},
                "std_dev": {"type": float, "min": 1.0, "max": 4.0}
            }
        },
        "macd": {
            "required_params": ["fast_period", "slow_period", "signal_period"],
            "param_constraints": {
                "fast_period": {"type": int, "min": 5, "max": 50},
                "slow_period": {"type": int, "min": 10, "max": 100},
                "signal_period": {"type": int, "min": 5, "max": 30}
            }
        },
        "atr": {
            "required_params": ["period"],
            "param_constraints": {
                "period": {"type": int, "min": 5, "max": 50}
            }
        },
        "supertrend": {
            "required_params": ["period", "multiplier"],
            "param_constraints": {
                "period": {"type": int, "min": 5, "max": 50},
                "multiplier": {"type": float, "min": 1.0, "max": 5.0}
            }
        },
        "vwap": {
            "required_params": [],
            "param_constraints": {}
        },
        "donchian_channels": {
            "required_params": ["period"],
            "param_constraints": {
                "period": {"type": int, "min": 10, "max": 100}
            }
        },
    }
    
    # Supported condition operators
    SUPPORTED_OPERATORS = [
        "greater_than", "less_than", "equal_to",
        "crosses_above", "crosses_below",
        "between", "outside_range"
    ]
    
    # Logical operators
    LOGICAL_OPERATORS = ["and", "or"]
    
    def validate(self, strategy_spec: Dict[str, Any]) -> ValidationResult:
        """
        Validate a custom strategy specification.
        
        Args:
            strategy_spec: User-provided strategy configuration
        
        Returns:
            ValidationResult with errors, warnings, and clamped config
        """
        errors = []
        warnings = []
        clamped_config = {}
        
        # 1. Validate required top-level fields
        required_fields = ["name", "description", "indicators", "entry_conditions", "exit_conditions"]
        for field in required_fields:
            if field not in strategy_spec:
                errors.append(f"Missing required field: {field}")
        
        if errors:
            return ValidationResult(is_valid=False, errors=errors)
        
        # 2. Validate indicators
        indicators_result = self._validate_indicators(strategy_spec.get("indicators", []))
        errors.extend(indicators_result["errors"])
        warnings.extend(indicators_result["warnings"])
        clamped_config["indicators"] = indicators_result["clamped"]
        
        # 3. Validate entry conditions
        entry_result = self._validate_conditions(
            strategy_spec.get("entry_conditions", []),
            clamped_config["indicators"]
        )
        errors.extend(entry_result["errors"])
        warnings.extend(entry_result["warnings"])
        clamped_config["entry_conditions"] = entry_result["clamped"]
        
        # 4. Validate exit conditions
        exit_result = self._validate_conditions(
            strategy_spec.get("exit_conditions", []),
            clamped_config["indicators"]
        )
        errors.extend(exit_result["errors"])
        warnings.extend(exit_result["warnings"])
        clamped_config["exit_conditions"] = exit_result["clamped"]
        
        # 5. Copy other fields
        clamped_config["name"] = strategy_spec["name"]
        clamped_config["description"] = strategy_spec["description"]
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            clamped_config=clamped_config if is_valid else None
        )
    
    def _validate_indicators(self, indicators: List[Dict]) -> Dict:
        """Validate indicator configurations."""
        errors = []
        warnings = []
        clamped_indicators = []
        
        if not indicators or len(indicators) == 0:
            errors.append("At least one indicator is required")
            return {"errors": errors, "warnings": warnings, "clamped": []}
        
        if len(indicators) > 10:
            warnings.append(f"Using {len(indicators)} indicators may slow execution. Recommended max: 10")
        
        for i, indicator in enumerate(indicators):
            if "type" not in indicator:
                errors.append(f"Indicator {i+1} missing 'type' field")
                continue
            
            ind_type = indicator["type"].lower()
            
            if ind_type not in self.SUPPORTED_INDICATORS:
                errors.append(f"Unsupported indicator type: {ind_type}. Supported: {', '.join(self.SUPPORTED_INDICATORS.keys())}")
                continue
            
            ind_spec = self.SUPPORTED_INDICATORS[ind_type]
            ind_params = indicator.get("parameters", {})
            clamped_params = {}
            
            # Check required parameters
            for req_param in ind_spec["required_params"]:
                if req_param not in ind_params:
                    errors.append(f"Indicator '{ind_type}' missing required parameter: {req_param}")
            
            # Validate and clamp parameters
            for param_name, constraints in ind_spec["param_constraints"].items():
                if param_name not in ind_params:
                    if "default" in constraints:
                        clamped_params[param_name] = constraints["default"]
                    continue
                
                value = ind_params[param_name]
                expected_type = constraints["type"]
                
                # Type check
                if not isinstance(value, expected_type):
                    try:
                        value = expected_type(value)
                    except:
                        errors.append(f"Indicator '{ind_type}' parameter '{param_name}' must be {expected_type.__name__}")
                        continue
                
                # Range check
                min_val = constraints.get("min")
                max_val = constraints.get("max")
                
                if min_val is not None and value < min_val:
                    warnings.append(f"Indicator '{ind_type}' parameter '{param_name}' clamped from {value} to minimum {min_val}")
                    value = min_val
                
                if max_val is not None and value > max_val:
                    warnings.append(f"Indicator '{ind_type}' parameter '{param_name}' clamped from {value} to maximum {max_val}")
                    value = max_val
                
                clamped_params[param_name] = value
            
            clamped_indicators.append({
                "type": ind_type,
                "parameters": clamped_params
            })
        
        return {"errors": errors, "warnings": warnings, "clamped": clamped_indicators}
    
    def _validate_conditions(self, conditions: List[Dict], indicators: List[Dict]) -> Dict:
        """Validate entry/exit conditions."""
        errors = []
        warnings = []
        clamped_conditions = []
        
        if not conditions or len(conditions) == 0:
            errors.append("At least one condition is required")
            return {"errors": errors, "warnings": warnings, "clamped": []}
        
        indicator_names = [ind["type"] for ind in indicators]
        
        for i, condition in enumerate(conditions):
            cond_type = condition.get("type")
            
            if cond_type not in ["condition", "logical_operator"]:
                errors.append(f"Condition {i+1} has invalid type: {cond_type}. Must be 'condition' or 'logical_operator'")
                continue
            
            if cond_type == "condition":
                # Validate condition structure
                if "left" not in condition:
                    errors.append(f"Condition {i+1} missing 'left' field")
                    continue
                
                if "operator" not in condition:
                    errors.append(f"Condition {i+1} missing 'operator' field")
                    continue
                
                if "right" not in condition:
                    errors.append(f"Condition {i+1} missing 'right' field")
                    continue
                
                # Validate operator
                if condition["operator"] not in self.SUPPORTED_OPERATORS:
                    errors.append(f"Condition {i+1} has unsupported operator: {condition['operator']}")
                    continue
                
                # Validate indicator references
                left_ref = condition["left"]
                if not self._is_valid_reference(left_ref, indicator_names):
                    errors.append(f"Condition {i+1} references unknown indicator: {left_ref}")
                
                clamped_conditions.append(condition)
            
            else:  # logical_operator
                if "logical" not in condition:
                    errors.append(f"Logical operator condition {i+1} missing 'logical' field")
                    continue
                
                if condition["logical"].lower() not in self.LOGICAL_OPERATORS:
                    errors.append(f"Condition {i+1} has invalid logical operator: {condition['logical']}")
                    continue
                
                clamped_conditions.append(condition)
        
        return {"errors": errors, "warnings": warnings, "clamped": clamped_conditions}
    
    def _is_valid_reference(self, reference: str, indicator_names: List[str]) -> bool:
        """Check if a reference is valid (indicator name or literal value)."""
        # Check if it's a number
        try:
            float(reference)
            return True
        except ValueError:
            pass
        
        # Check if it's an indicator reference
        return reference in indicator_names or reference in ["price", "volume", "close", "high", "low", "open"]


# Global singleton
_validator = CustomStrategyValidator()


def get_validator() -> CustomStrategyValidator:
    """Get the global validator instance."""
    return _validator
