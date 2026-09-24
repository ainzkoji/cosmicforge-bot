"""CATI active-execution configuration (Section 20.18) -- OFF by default.

``CATI_ACTIVE_EXECUTION_ENABLED`` is a SEPARATE, explicit flag. It is not
``CATI_CYCLE_SHADOW_ENABLED`` (shadow evidence) and is never implied by it.
Unset / anything but an explicit truthy value = disabled. Section 25
promotion policy is what may enable it later; nothing in this codebase sets
it, and the runner never calls the CATI execution boundary.

``CATI_EXIT_INTENT_ROUTING_ENABLED`` separately gates routing Section 19
ExitDecisions to PositionManager/executor (``process_exit_decision``);
also OFF by default, and it additionally requires active execution.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

ENV_ACTIVE_EXECUTION = "CATI_ACTIVE_EXECUTION_ENABLED"
ENV_EXIT_INTENT_ROUTING = "CATI_EXIT_INTENT_ROUTING_ENABLED"
_TRUE = ("1", "true", "yes", "on")


def _flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUE


@dataclass(frozen=True)
class CATIExecutionConfig:
    active_execution_enabled: bool = False
    exit_intent_routing_enabled: bool = False
    #: environments the boundary may ever submit to while not promoted
    allowed_environments: tuple = ("DEMO", "TESTNET", "PAPER")

    @classmethod
    def from_env(cls) -> "CATIExecutionConfig":
        return cls(active_execution_enabled=_flag(ENV_ACTIVE_EXECUTION),
                   exit_intent_routing_enabled=_flag(ENV_EXIT_INTENT_ROUTING))


def is_active_execution_enabled() -> bool:
    return CATIExecutionConfig.from_env().active_execution_enabled


__all__ = ["ENV_ACTIVE_EXECUTION", "ENV_EXIT_INTENT_ROUTING", "CATIExecutionConfig", "is_active_execution_enabled"]
