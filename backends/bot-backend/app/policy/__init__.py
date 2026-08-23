# app/policy/__init__.py
"""
Policy Engine Module - Single Source of Truth for Trade Decisions

Usage:
    from app.policy.policy_engine import (
        PolicyEngine,
        PolicyContext,
        PolicyDecision,
        ReasonCode,
        Action,
        RiskLevel,
        TradeAmountMode,
        get_policy_engine,
        calculate_atr,
        compute_budget_usdt,
    )
"""
from app.policy.policy_engine import (
    PolicyEngine,
    PolicyContext,
    PolicyDecision,
    ReasonCode,
    Action,
    RiskLevel,
    RiskProfile,
    TradeAmountMode,
    RISK_PROFILES,
    get_policy_engine,
    reset_policy_engine,
    calculate_atr,
)



__all__ = [
    # New unified API
    "PolicyEngine",
    "PolicyContext", 
    "PolicyDecision",
    "ReasonCode",
    "Action",
    "RiskLevel",
    "RiskProfile",
    "TradeAmountMode",
    "RISK_PROFILES",
    "get_policy_engine",
    "reset_policy_engine",
    "calculate_atr",

]
