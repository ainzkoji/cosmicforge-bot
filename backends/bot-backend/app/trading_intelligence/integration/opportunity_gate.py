"""TradePlan admission gate (Phase 5H).

An opportunity may reach TradePlan only when EVERY input below is positively
established. Each requirement is checked in a fixed order and every failure
is reported with its own reason code; an unknown (``None``) input is a
failure, never a pass.

This gate is additive: it sits in front of the existing Section 18 builder
and never relaxes the Section 20 execution boundary (which stays disabled
unless CATI_ACTIVE_EXECUTION_ENABLED is set by governance).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Tuple

REQUIREMENTS: Tuple[Tuple[str, str], ...] = (
    ("market_data_ready", "MARKET_DATA_NOT_READY"),
    ("asset_class_intelligence_ready", "ASSET_CLASS_INTELLIGENCE_NOT_READY"),
    ("venue_instrument_active", "VENUE_INSTRUMENT_INACTIVE"),
    ("account_eligible", "ACCOUNT_NOT_ELIGIBLE"),
    ("api_execution_supported", "API_EXECUTION_UNSUPPORTED"),
    ("economics_available", "ECONOMICS_UNAVAILABLE"),
    ("portfolio_accepts", "PORTFOLIO_REJECTED"),
    ("risk_accepts", "RISK_REJECTED"),
    ("capital_available", "CAPITAL_UNAVAILABLE"),
    ("internal_transfer_confirmed", "INTERNAL_TRANSFER_NOT_CONFIRMED"),
    ("credential_valid", "CREDENTIAL_INVALID"),
    ("permissions_valid", "PERMISSIONS_INVALID"),
)
GATE_VERSION = "opportunity-gate-v1"


@dataclass(frozen=True)
class GateResult:
    admitted: bool
    reason_codes: Tuple[str, ...]
    version: str = GATE_VERSION


def admit_to_trade_plan(inputs: Mapping[str, Optional[bool]]) -> GateResult:
    """``internal_transfer_confirmed`` may be True when no transfer is needed
    (the capital planner says NO_ACTION / LOGICAL); the caller must pass
    ``capital.planner.capital_readiness(plan, transfer_status=..., reservation_status=...).ready`` for it
    (the execution boundary re-checks the same readiness before hard risk)."""
    reasons = tuple(code for key, code in REQUIREMENTS if inputs.get(key) is not True)
    return GateResult(admitted=not reasons, reason_codes=reasons)


__all__ = ["GATE_VERSION", "GateResult", "REQUIREMENTS", "admit_to_trade_plan"]
