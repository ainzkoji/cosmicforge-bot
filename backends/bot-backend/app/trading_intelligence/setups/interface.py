"""Common SetupSpecialist interface (Section 11.1).

Deliberately NOT the old ``app.strategy.base.Strategy`` /
``app.strategy.strategy_framework.BaseStrategy`` interface: CATI is not
another legacy strategy plugin. A specialist's ``discover`` never returns a
BUY/SELL/HOLD signal or a confidence score -- it returns a tuple of
independent, self-describing SetupCandidate objects (zero is normal).
"""
from __future__ import annotations

from typing import Any, Protocol, Tuple

from app.trading_intelligence.contracts.data_quality import Capability
from app.trading_intelligence.contracts.setup import SetupCandidate


class SetupSpecialist(Protocol):
    """Every implementation must be deterministic, causal, and free of
    network calls, account/user state, execution, and final trade authority."""

    #: Stable family name, e.g. "TREND_PULLBACK_V2".
    setup_family: str
    #: This specialist's own discovery-logic version (distinct from
    #: SETUP_CANDIDATE_SCHEMA_VERSION, which versions the *contract shape*).
    setup_version: str
    #: Capabilities this specialist's geometry/evidence would ideally read.
    #: Missing ones must degrade to availability metadata, not fabricated
    #: values (Section 11.3.4 / 11.4.3 / 11.6).
    required_capabilities: Tuple[Capability, ...]

    def discover(
        self,
        *,
        snapshot: Any,
        market_state: Any,
        regime_distribution: Any,
        policy: Any,
    ) -> Tuple[SetupCandidate, ...]:
        ...


__all__ = ["SetupSpecialist"]
