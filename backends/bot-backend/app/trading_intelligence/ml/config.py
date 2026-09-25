"""CATI ML switches -- AUTO_ACTIVE_IF_ELIGIBLE, never an authority by themselves.

``CATI_ML_SHADOW_ENABLED`` / ``CATI_ML_ENABLED`` are OPERATOR OVERRIDES that
can only switch ML OFF (``0``/``false``/``off``). Unset = AUTO: a promoted
estimator is used only when the model is PROMOTED in the registry AND the
Section 25 phase authorizes CATI; otherwise the deterministic CATI estimator
is used (never V2). Legacy V2 ``ML_ENABLED`` / ``ML_SHADOW_MODE`` /
``ML_HARD_BLOCK_FLOOR`` are never read. A directly constructed
``CATIMLConfig()`` is explicit OFF (tests / tooling).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

ENV_CATI_ML = "CATI_ML_ENABLED"
ENV_CATI_ML_SHADOW = "CATI_ML_SHADOW_ENABLED"
_TRUE = ("1", "true", "yes", "on")


def _flag(name: str) -> bool:
    """AUTO unless explicitly switched off (operator override)."""
    from app.activation.model import OperatorOverride, operator_override

    return operator_override(name) == OperatorOverride.AUTO


@dataclass(frozen=True)
class CATIMLConfig:
    ml_enabled: bool = False
    shadow_enabled: bool = False

    @classmethod
    def from_env(cls) -> "CATIMLConfig":
        return cls(ml_enabled=_flag(ENV_CATI_ML), shadow_enabled=_flag(ENV_CATI_ML_SHADOW))


__all__ = ["CATIMLConfig", "ENV_CATI_ML", "ENV_CATI_ML_SHADOW"]
