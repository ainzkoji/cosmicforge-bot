"""AUTO_ACTIVE_IF_ELIGIBLE: capability state derived from prerequisites.

A finished capability never waits on an operator remembering a flag. Its
state is COMPUTED from facts every time it is asked:

* every prerequisite satisfied               -> ACTIVE
* a prerequisite unmet / unknown             -> BLOCKED  (first unmet reason)
* the platform or venue cannot provide it    -> UNSUPPORTED

Unknown is never "satisfied": a prerequisite whose fact is ``None`` blocks
with ``<NAME>_UNKNOWN``. Nothing here can grant authority that governance has
not granted: CATI execution / ML authority prerequisites ARE the Section 25
migration phase, the kill switch and the certification evidence.

Legacy enable flags survive only as an OPERATOR OVERRIDE, and only in the
safe direction:

* unset, or any truthy value  -> AUTO (the prerequisites decide)
* ``0`` / ``false`` / ``off``  -> FORCE_OFF (BLOCKED: OPERATOR_DISABLED)

A flag can therefore switch a capability OFF, never ON past its
prerequisites.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

ACTIVATION_MODEL_VERSION = "auto-activation-v1"


class ActivationState(str, Enum):
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    UNSUPPORTED = "UNSUPPORTED"


class OperatorOverride(str, Enum):
    AUTO = "AUTO"
    FORCE_OFF = "FORCE_OFF"


OPERATOR_DISABLED = "OPERATOR_DISABLED"
_OFF = ("0", "false", "no", "off", "disabled")


def operator_override(env_name: Optional[str], environ: Optional[Mapping[str, str]] = None) -> OperatorOverride:
    """Tri-state reading of a legacy flag: only an explicit OFF is honoured."""
    if not env_name:
        return OperatorOverride.AUTO
    raw = (environ if environ is not None else os.environ).get(env_name)
    if raw is not None and raw.strip().lower() in _OFF:
        return OperatorOverride.FORCE_OFF
    return OperatorOverride.AUTO


@dataclass(frozen=True)
class Prerequisite:
    name: str
    satisfied: Optional[bool]          # None = unknown (blocks)
    reason: str                        # the blocked reason when not satisfied
    detail: str = ""
    #: True when an unmet prerequisite means the capability can never exist here (not merely "not yet")
    structural: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "satisfied": self.satisfied, "reason": self.reason, "detail": self.detail,
                "structural": self.structural}


@dataclass(frozen=True)
class ActivationDecision:
    capability: str
    scope: str
    state: ActivationState
    reasons: Tuple[str, ...]
    prerequisites: Tuple[Prerequisite, ...] = ()
    override: str = OperatorOverride.AUTO.value
    authority: str = "NONE"               # EVIDENCE_ONLY | DEMO | SCOPED_LIVE | LIVE | NONE
    version: str = ACTIVATION_MODEL_VERSION
    detail: Mapping[str, Any] = field(default_factory=dict)

    @property
    def active(self) -> bool:
        return self.state == ActivationState.ACTIVE

    @property
    def reason(self) -> Optional[str]:
        return self.reasons[0] if self.reasons else None

    def to_dict(self) -> Dict[str, Any]:
        return {"capability": self.capability, "scope": self.scope, "state": self.state.value,
                "reason": self.reason, "reasons": list(self.reasons), "override": self.override,
                "authority": self.authority if self.active else "NONE",
                "prerequisites": [p.to_dict() for p in self.prerequisites], "version": self.version,
                "detail": dict(self.detail)}


def decide(capability: str, prerequisites: Sequence[Prerequisite], *, scope: str = "GLOBAL",
           override: OperatorOverride = OperatorOverride.AUTO, authority: str = "EVIDENCE_ONLY",
           detail: Optional[Mapping[str, Any]] = None) -> ActivationDecision:
    """Pure: the same facts always produce the same decision."""
    prereqs = tuple(prerequisites)
    unmet = [p for p in prereqs if p.satisfied is not True]
    reasons = tuple((p.reason if p.satisfied is False else f"{p.name.upper()}_UNKNOWN") for p in unmet)
    if any(p.structural and p.satisfied is False for p in unmet):
        state = ActivationState.UNSUPPORTED
    elif override == OperatorOverride.FORCE_OFF:
        state, reasons = ActivationState.BLOCKED, (OPERATOR_DISABLED,) + reasons
    elif unmet:
        state = ActivationState.BLOCKED
    else:
        state = ActivationState.ACTIVE
    return ActivationDecision(capability=capability, scope=scope, state=state, reasons=reasons,
                              prerequisites=prereqs, override=override.value, authority=authority,
                              detail=dict(detail or {}))


__all__ = ["ACTIVATION_MODEL_VERSION", "ActivationDecision", "ActivationState", "OPERATOR_DISABLED",
           "OperatorOverride", "Prerequisite", "decide", "operator_override"]
