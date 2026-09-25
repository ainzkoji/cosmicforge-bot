"""CATI active-execution configuration (Section 20.18).

AUTO_ACTIVE_IF_ELIGIBLE: the Section 25 governance phase is the authority,
not a flag an operator must remember. ``CATI_ACTIVE_EXECUTION_ENABLED`` is an
OPERATOR OVERRIDE that can only switch execution OFF (``0``/``false``/``off``);
unset (AUTO) lets the boundary ask ``GovernanceAuthority`` -- which refuses
every entry below M6, live below M7, unpromoted M7 scopes and anything while
the kill switch is on. A flag can never grant what governance has not.

``CATI_EXIT_INTENT_ROUTING_ENABLED`` is the same kind of override for routing
Section 19 ExitDecisions (``process_exit_decision``), which additionally
requires active execution.

A directly constructed ``CATIExecutionConfig()`` is explicit OFF (tests,
tooling); ``from_env()`` is the runtime reading. Runtime CATI entries also
need the runner's authority switch, which does not exist yet
(``app.activation.cati.RUNTIME_AUTHORITY_SWITCH_IMPLEMENTED``).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

ENV_ACTIVE_EXECUTION = "CATI_ACTIVE_EXECUTION_ENABLED"
ENV_EXIT_INTENT_ROUTING = "CATI_EXIT_INTENT_ROUTING_ENABLED"
_TRUE = ("1", "true", "yes", "on")


def _flag(name: str) -> bool:
    """AUTO unless explicitly switched off (operator override)."""
    from app.activation.model import OperatorOverride, operator_override

    return operator_override(name) == OperatorOverride.AUTO


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
