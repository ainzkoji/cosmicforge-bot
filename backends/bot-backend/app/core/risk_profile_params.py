"""Risk parameters of a risk-based deployment, from the versioned profile library.

``BotInstanceService.get_risk_profile_preset`` keeps serving the legacy presets
for legacy bots. A bot deployed through the Step 1 contract
(``allocation_type == "risk_based"``) is governed by ``shared_lib.risk_levels``
at the version persisted on the row, translated here into the ``risk_params``
mapping ``resolve_effective_bot_policy`` already understands. The engine's 0.4 %
system ceiling is applied by that resolver, never relaxed here.
"""
from __future__ import annotations

from typing import Any, Dict

from shared_lib import risk_levels


def risk_based_params(instance: Any) -> Dict[str, Any]:
    profile = risk_levels.get_profile(getattr(instance, "risk_level", None),
                                      getattr(instance, "risk_profile_version", None) or None)
    return {
        "risk_profile": profile.level,
        "risk_profile_version": profile.version,
        # fractions, the units resolve_effective_bot_policy expects
        "per_trade_risk_pct": float(profile.fraction("per_trade_risk_pct")),
        "max_open_risk_fraction": float(profile.fraction("max_open_risk_pct")),
        "daily_loss_limit_pct": float(profile.fraction("daily_loss_pause_pct")),
        "max_drawdown_pct": float(profile.fraction("drawdown_stop_pct")),
        "drawdown_reduce_pct": float(profile.fraction("drawdown_reduce_pct")),
        "max_leverage": float(profile.leverage_ceiling),
        "leverage_ceiling": float(profile.leverage_ceiling),
        "max_position_slots": int(profile.max_positions),
        # the frozen CATI geometry supplies its own stop; these keep the legacy shape valid
        "stop_loss_multiplier": 2.0,
        "take_profit_multiplier": 4.0,
        "additional_params": {"volatility_filter_enabled": True},
    }


__all__ = ["risk_based_params"]
