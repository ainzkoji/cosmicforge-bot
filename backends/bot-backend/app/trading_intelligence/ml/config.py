"""CATI ML flags -- OFF by default, and never an authority by themselves.

``CATI_ML_SHADOW_ENABLED`` lets shadow estimators record evidence.
``CATI_ML_ENABLED`` is necessary but NOT sufficient for a promoted estimator
to be used: the model must be PROMOTED in the registry AND the Section 25
promotion phase must authorize CATI authority for the scope. Legacy V2
``ML_ENABLED`` / ``ML_SHADOW_MODE`` / ``ML_HARD_BLOCK_FLOOR`` are never read.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

ENV_CATI_ML = "CATI_ML_ENABLED"
ENV_CATI_ML_SHADOW = "CATI_ML_SHADOW_ENABLED"
_TRUE = ("1", "true", "yes", "on")


def _flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUE


@dataclass(frozen=True)
class CATIMLConfig:
    ml_enabled: bool = False
    shadow_enabled: bool = False

    @classmethod
    def from_env(cls) -> "CATIMLConfig":
        return cls(ml_enabled=_flag(ENV_CATI_ML), shadow_enabled=_flag(ENV_CATI_ML_SHADOW))


__all__ = ["CATIMLConfig", "ENV_CATI_ML", "ENV_CATI_ML_SHADOW"]
