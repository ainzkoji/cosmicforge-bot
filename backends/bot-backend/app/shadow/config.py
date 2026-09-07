"""
Shadow Trading System — Configuration

All shadow system parameters are driven by environment variables so the system
can be toggled without code changes. Defaults are conservative:
  SHADOW_ENABLED=False  →  zero overhead on existing deployments
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ShadowConfig:
    """Runtime configuration for the shadow trading system."""

    # ── Master switch ─────────────────────────────────────────────────────────
    # Set SHADOW_ENABLED=true in .env to activate the system.
    enabled: bool = False

    # ── Category capture flags ────────────────────────────────────────────────
    # Each flag controls whether that rejection category produces shadow records.
    capture_ml_blocked: bool = True
    capture_threshold_blocked: bool = True
    capture_correlation_blocked: bool = True
    capture_already_open: bool = True
    capture_executor_rejected: bool = True
    capture_safety_blocked: bool = False    # noisier; off by default
    capture_near_miss: bool = False          # HOLDs within N% of threshold

    # "Near miss" threshold: capture HOLDs where confidence >= threshold * factor
    # Example: threshold=0.50, factor=0.90 → capture HOLDs with confidence>=0.45
    near_miss_factor: float = 0.90

    # ── Expiry window ─────────────────────────────────────────────────────────
    # A shadow trade expires (outcome=EXPIRED) if neither TP nor SL hits
    # within this many bars of the entry timeframe.
    expiry_bars: int = 48          # 48 × 15m = 12 hours

    # ── Outcome evaluation tuning ─────────────────────────────────────────────
    # Max shadow trades evaluated per pass (rate-limit friendly)
    max_eval_per_pass: int = 20

    # Assumed round-trip fee rate for net PnL calculation (taker + taker)
    assumed_fee_rate: float = 0.001   # 0.10%

    # ── Observability ─────────────────────────────────────────────────────────
    # Print per-cycle summary line  (not per-symbol)
    cycle_summary: bool = True

    @classmethod
    def from_env(cls) -> "ShadowConfig":
        """Load config from environment variables."""

        def _bool(key: str, default: bool) -> bool:
            val = os.environ.get(key, "").strip().lower()
            if val in ("1", "true", "yes", "on"):
                return True
            if val in ("0", "false", "no", "off"):
                return False
            return default

        def _int(key: str, default: int) -> int:
            try:
                return int(os.environ.get(key, str(default)))
            except (ValueError, TypeError):
                return default

        def _float(key: str, default: float) -> float:
            try:
                return float(os.environ.get(key, str(default)))
            except (ValueError, TypeError):
                return default

        return cls(
            enabled=_bool("SHADOW_ENABLED", False),
            capture_ml_blocked=_bool("SHADOW_CAPTURE_ML_BLOCKED", True),
            capture_threshold_blocked=_bool("SHADOW_CAPTURE_THRESHOLD_BLOCKED", True),
            capture_correlation_blocked=_bool("SHADOW_CAPTURE_CORRELATION_BLOCKED", True),
            capture_already_open=_bool("SHADOW_CAPTURE_ALREADY_OPEN", True),
            capture_executor_rejected=_bool("SHADOW_CAPTURE_EXECUTOR_REJECTED", True),
            capture_safety_blocked=_bool("SHADOW_CAPTURE_SAFETY_BLOCKED", False),
            capture_near_miss=_bool("SHADOW_CAPTURE_NEAR_MISS", False),
            near_miss_factor=_float("SHADOW_NEAR_MISS_FACTOR", 0.90),
            expiry_bars=_int("SHADOW_EXPIRY_BARS", 48),
            max_eval_per_pass=_int("SHADOW_MAX_EVAL_PER_PASS", 20),
            assumed_fee_rate=_float("SHADOW_ASSUMED_FEE_RATE", 0.001),
            cycle_summary=_bool("SHADOW_CYCLE_SUMMARY", True),
        )


# ── Singleton ─────────────────────────────────────────────────────────────────
_config: Optional[ShadowConfig] = None


def get_shadow_config() -> ShadowConfig:
    """Return the singleton ShadowConfig (loaded once from env)."""
    global _config
    if _config is None:
        _config = ShadowConfig.from_env()
    return _config


def reset_shadow_config() -> None:
    """Force re-read from env (useful for tests)."""
    global _config
    _config = None
# Activated: 2026-04-04 — SHADOW_ENABLED=True set in .env (Phase 5F-2 v3)
