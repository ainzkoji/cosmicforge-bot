"""Section 22 certification contracts.

Every analytical record is immutable and hash-identified by its analytical
content. Operational timestamps (``started_at`` / ``completed_at``) are
metadata: they are carried on the record but never enter an id or hash, so
the same inputs always produce the same artifact identity.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import CERTIFICATION_SCHEMA_VERSION


class CertificationStage(str, Enum):
    FAST = "FAST"
    MEDIUM = "MEDIUM"
    STRESS = "STRESS"
    FULL = "FULL"
    HOLDOUT = "HOLDOUT"
    FORWARD_DEMO = "FORWARD_DEMO"


STAGE_ORDER = tuple(s.value for s in CertificationStage)


class CertificationStatus(str, Enum):
    NOT_RUN = "NOT_RUN"
    RUNNING = "RUNNING"
    PASS = "PASS"
    FAIL = "FAIL"
    BLOCKED_DATA = "BLOCKED_DATA"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class OverallCertificationStatus(str, Enum):
    CERTIFIED = "CERTIFIED"
    NOT_CERTIFIED = "NOT_CERTIFIED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    BLOCKED_BY_DATA = "BLOCKED_BY_DATA"
    FORWARD_DEMO_REQUIRED = "FORWARD_DEMO_REQUIRED"


class GateStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    BLOCKED = "BLOCKED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class Gate(str, Enum):
    A_INTEGRITY = "A_INTEGRITY"
    B_NET_EXPECTANCY = "B_NET_EXPECTANCY"
    C_COST_STRESS = "C_COST_STRESS"
    D_HOLDOUT = "D_HOLDOUT"
    E_CONCENTRATION = "E_CONCENTRATION"
    F_CALIBRATION = "F_CALIBRATION"
    G_EVIDENCE_COUNT = "G_EVIDENCE_COUNT"
    H_FORWARD_DEMO = "H_FORWARD_DEMO"
    I_OPERATIONAL = "I_OPERATIONAL"


#: Every gate is mandatory for CERTIFIED; none may be averaged away.
MANDATORY_GATES = tuple(g.value for g in Gate)


class CertReason(str, Enum):
    POLICY_INCOMPLETE = "CERTIFICATION_POLICY_INCOMPLETE"
    NO_REAL_DATA = "NO_REAL_MARKET_DATA"
    SYNTHETIC_SOURCE_REFUSED = "SYNTHETIC_SOURCE_CANNOT_CERTIFY"
    INSUFFICIENT_COVERAGE = "INSUFFICIENT_DATA_COVERAGE"
    INSUFFICIENT_SAMPLES = "INSUFFICIENT_SAMPLES"
    NO_ACCEPTED_EVIDENCE = "NO_ACCEPTED_EVIDENCE"
    LOOKAHEAD_VIOLATION = "LOOKAHEAD_VIOLATION"
    REPLAY_HASH_UNSTABLE = "REPLAY_HASH_UNSTABLE"
    MANIFEST_INVALID = "DATASET_MANIFEST_INVALID"
    NEGATIVE_EXPECTANCY = "NET_EXPECTANCY_NOT_POSITIVE"
    LOW_PROBABILITY_POSITIVE = "P_EXPECTANCY_POSITIVE_BELOW_REQUIREMENT"
    COST_STRESS_NOT_CREDIBLE = "COST_STRESS_1_5X_NOT_CREDIBLE"
    COST_STRESS_CATASTROPHIC = "COST_STRESS_2X_CATASTROPHIC"
    HOLDOUT_NOT_RUN = "HOLDOUT_NOT_RUN"
    HOLDOUT_NOT_FROZEN = "HOLDOUT_REQUIRES_POLICY_FREEZE"
    HOLDOUT_BURNED = "HOLDOUT_ALREADY_BURNED"
    HOLDOUT_POLICY_CHANGED = "HOLDOUT_POLICY_HASH_CHANGED"
    HOLDOUT_DRAWDOWN = "HOLDOUT_DRAWDOWN_EXCEEDED"
    CONCENTRATION_EXCEEDED = "CONCENTRATION_EXCEEDED"
    CALIBRATION_FAILED = "CALIBRATION_NOT_RELIABLE"
    FORWARD_DEMO_DAYS = "FORWARD_DEMO_DAYS_INSUFFICIENT"
    FORWARD_DEMO_EVIDENCE = "FORWARD_DEMO_EVIDENCE_INSUFFICIENT"
    OPERATIONAL_DEFECT = "UNRESOLVED_OPERATIONAL_DEFECT"
    PRECEDING_STAGES = "PRECEDING_STAGES_INSUFFICIENT"
    OUT_OF_SCOPE = "OUT_OF_CERTIFICATION_SCOPE"


@dataclass(frozen=True)
class CertificationScope:
    """What a result is ABOUT. A Binance crypto result says nothing about
    Forex, IBKR, futures, other brokers or other timeframes."""

    asset_class: str
    venue: str
    environment: str
    symbol_universe: Tuple[str, ...]
    timeframe: str
    setup_families: Tuple[str, ...]
    side_scope: Tuple[str, ...]
    data_start: int
    data_end: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset_class", str(self.asset_class).upper())
        object.__setattr__(self, "venue", str(self.venue).upper())
        object.__setattr__(self, "environment", str(self.environment).upper())
        object.__setattr__(self, "symbol_universe", tuple(sorted({s.upper() for s in self.symbol_universe})))
        object.__setattr__(self, "setup_families", tuple(sorted(set(self.setup_families))))
        object.__setattr__(self, "side_scope", tuple(sorted({s.upper() for s in self.side_scope})))
        if self.data_end < self.data_start:
            raise ValueError("certification scope ends before it starts")

    def to_dict(self) -> dict:
        return {"asset_class": self.asset_class, "venue": self.venue, "environment": self.environment,
                "symbol_universe": list(self.symbol_universe), "timeframe": self.timeframe,
                "setup_families": list(self.setup_families), "side_scope": list(self.side_scope),
                "data_start": self.data_start, "data_end": self.data_end}

    @property
    def scope_hash(self) -> str:
        return stable_hash(self.to_dict())

    def covers(self, *, asset_class: str, venue: str, timeframe: str, symbol: Optional[str] = None,
               environment: Optional[str] = None, setup_family: Optional[str] = None,
               side: Optional[str] = None) -> bool:
        """True only when the queried deployment lies INSIDE this scope."""
        if str(asset_class).upper() != self.asset_class or str(venue).upper() != self.venue:
            return False
        if timeframe != self.timeframe:
            return False
        if environment is not None and str(environment).upper() != self.environment:
            return False
        if symbol is not None and str(symbol).upper() not in self.symbol_universe:
            return False
        if setup_family is not None and setup_family not in self.setup_families:
            return False
        return side is None or str(side).upper() in self.side_scope


class _FrozenDict(dict):
    """A dict that refuses mutation (JSON-serializable, hashable content)."""

    def _ro(self, *_a, **_k):
        raise TypeError("certification results are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _ro  # type: ignore[assignment]


def freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _FrozenDict({str(k): freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(v) for v in value)
    if isinstance(value, Enum):
        return value.value
    return value


@dataclass(frozen=True)
class GateResult:
    gate: str
    status: str
    reason_codes: Tuple[str, ...] = ()
    observed: Mapping[str, Any] = field(default_factory=dict)
    required: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))
        object.__setattr__(self, "observed", freeze(self.observed))
        object.__setattr__(self, "required", freeze(self.required))

    def to_dict(self) -> dict:
        return {"gate": self.gate, "status": self.status, "reason_codes": list(self.reason_codes),
                "observed": self.observed, "required": self.required}


@dataclass(frozen=True)
class CertificationStageResult:
    stage: str
    status: str
    metrics: Mapping[str, Any] = field(default_factory=dict)
    gate_results: Tuple[GateResult, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: Tuple[str, ...] = ()
    artifact_refs: Mapping[str, str] = field(default_factory=dict)
    schema_version: str = CERTIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", freeze(self.metrics))
        object.__setattr__(self, "diagnostics", freeze(self.diagnostics))
        object.__setattr__(self, "artifact_refs", freeze(self.artifact_refs))
        object.__setattr__(self, "gate_results", tuple(self.gate_results))
        object.__setattr__(self, "reason_codes", tuple(dict.fromkeys(self.reason_codes)))

    def to_dict(self) -> dict:
        return {"stage": self.stage, "status": self.status, "metrics": self.metrics,
                "gate_results": [g.to_dict() for g in self.gate_results], "diagnostics": self.diagnostics,
                "reason_codes": list(self.reason_codes), "artifact_refs": self.artifact_refs,
                "schema_version": self.schema_version}

    @property
    def result_hash(self) -> str:
        return stable_hash(self.to_dict())


@dataclass(frozen=True)
class CertificationRun:
    stage: str
    scope: CertificationScope
    dataset_manifest_id: str
    dataset_hash: str
    policy_freeze_hash: str
    certification_policy_hash: str
    version_hashes: Mapping[str, str]
    result: CertificationStageResult
    reason_codes: Tuple[str, ...] = ()
    #: operational metadata -- NEVER part of the analytical identity
    started_at: Optional[int] = None
    completed_at: Optional[int] = None
    schema_version: str = CERTIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "version_hashes", freeze(self.version_hashes))
        object.__setattr__(self, "reason_codes", tuple(dict.fromkeys(self.reason_codes)))

    def _identity(self) -> dict:
        return {"stage": self.stage, "scope": self.scope.to_dict(), "dataset_hash": self.dataset_hash,
                "dataset_manifest_id": self.dataset_manifest_id, "policy_freeze_hash": self.policy_freeze_hash,
                "certification_policy_hash": self.certification_policy_hash,
                "version_hashes": self.version_hashes, "schema_version": self.schema_version}

    @property
    def certification_run_id(self) -> str:
        return short_id("crun", self._identity())

    @property
    def artifact_hash(self) -> str:
        return stable_hash({**self._identity(), "result": self.result.to_dict(),
                            "reason_codes": list(self.reason_codes)})

    @property
    def status(self) -> str:
        return self.result.status

    def to_dict(self) -> dict:
        return {"certification_run_id": self.certification_run_id, **self._identity(),
                "result": self.result.to_dict(), "reason_codes": list(self.reason_codes),
                "artifact_hash": self.artifact_hash,
                "metadata": {"started_at": self.started_at, "completed_at": self.completed_at}}


__all__ = [
    "CertificationStage", "CertificationStatus", "OverallCertificationStatus", "GateStatus", "Gate",
    "MANDATORY_GATES", "STAGE_ORDER", "CertReason", "CertificationScope", "GateResult",
    "CertificationStageResult", "CertificationRun", "freeze",
]
