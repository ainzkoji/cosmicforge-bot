"""BrokerHealthContext + SystemHealthContext (pre-Section-17 closure, item 2).

Broker health is scoped to ONE broker account on one venue/environment. It
is produced from the runtime's EXISTING broker-health state (the per-bot,
per-account circuit breaker and the persisted broker quarantine flag -- see
``integration/context_adapters.broker_health_from_runner``); CATI never runs
a second, independent health system.

``UNKNOWN`` means the canonical source genuinely could not determine health
(e.g. it could not be read). A CATI caller that simply did not pass a
context is a different, explicitly reported condition
(``BROKER_HEALTH_NOT_PROVIDED``), never silently UNKNOWN.

None of this affects existing position management: the veto only gates NEW
CATI opportunities; reconciliation/protection keep running under runtime
safety exactly as before.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from app.trading_intelligence.versions import BROKER_HEALTH_SCHEMA_VERSION


class BrokerHealthStatus(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class BrokerHealthContext:
    broker_account_id: Optional[str]
    venue: Optional[str]
    environment: Optional[str]
    status: str  # BrokerHealthStatus
    observed_at: Optional[int]
    source: str
    #: ms between the source's observation and ``observed_at``; None = unknown.
    freshness_ms: Optional[int] = None
    reason_codes: Tuple[str, ...] = ()
    version: str = BROKER_HEALTH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.status not in {s.value for s in BrokerHealthStatus}:
            raise ValueError(f"unknown broker health status {self.status!r}")


@dataclass(frozen=True)
class SystemHealthContext:
    """CATI abstention signals. Does not replace broker hard-risk checks --
    hard risk (circuit breaker in PolicyEngine) remains superior."""

    broker_health: Optional[BrokerHealthContext] = None
    component_errors: Tuple[str, ...] = ()
    data_quality_fault: bool = False


__all__ = ["BrokerHealthStatus", "BrokerHealthContext", "SystemHealthContext"]
