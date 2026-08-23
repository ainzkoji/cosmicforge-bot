"""
Shadow Trading System — Research Layer

Observes rejected/non-executed trade opportunities from the live pipeline,
records hypothetical trade definitions, evaluates outcomes asynchronously,
and provides analytics.

This module NEVER places orders, NEVER writes to trade_fills, and NEVER
alters any live execution behavior. It is a passive observer only.

Modules:
  config    — env-driven feature flags and tuning parameters
  store     — all DB persistence for shadow data
  capture   — classifies and records shadow candidates at decision time
  evaluator — asynchronous outcome resolution using future market data
  analytics — research queries and reporting
"""
from app.shadow.config import get_shadow_config, ShadowConfig
from app.shadow.capture import get_shadow_capture, ShadowCapture
from app.shadow.store import get_shadow_store, ShadowStore
from app.shadow.evaluator import get_shadow_evaluator, ShadowEvaluator
from app.shadow.analytics import get_shadow_analytics, ShadowAnalytics

__all__ = [
    "get_shadow_config", "ShadowConfig",
    "get_shadow_capture", "ShadowCapture",
    "get_shadow_store", "ShadowStore",
    "get_shadow_evaluator", "ShadowEvaluator",
    "get_shadow_analytics", "ShadowAnalytics",
]
