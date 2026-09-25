"""Separate multi-asset certification scopes (Phase 6A/6B).

Certification is NOT one opaque certificate over everything. Each scope is
(asset class, venue/source, product) with its OWN dataset manifest, universe
hash, config/policy hashes, time range, holdout and economics assumptions.
A result certifies only its scope (``CertificationScope.covers``).

The frozen Section 22 policy values are not touched here: every scope is
evaluated by the existing ``canonical_certification_policy()`` gates. This
module only declares which scopes exist, what each needs, and refuses to
treat a scope as certifiable while any input is missing.

Holdouts stay separate because ``HoldoutRegistry`` keys a holdout by the
scope's dataset hash; ``holdout_namespace`` makes the scope explicit in the
reservation window's identity too. Nothing here opens a holdout.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

SCOPE_REGISTRY_VERSION = "multi-asset-scopes-v1"

READY, BLOCKED_DATA, BLOCKED_CAPABILITY, BLOCKED_FREEZE = "READY_FOR_REPLAY", "BLOCKED_DATA", "BLOCKED_CAPABILITY", "BLOCKED_FREEZE"

#: every artifact a scope must pin before its holdout may even be reserved
REQUIRED_FREEZE_INPUTS = ("dataset_manifest_hash", "universe_hash", "config_hash", "policy_hash",
                          "feature_contract_hash", "economics_hash", "cost_assumptions_hash", "strategy_logic_hash",
                          "certification_config_hash", "source_versions", "time_range")


@dataclass(frozen=True)
class ScopeSpec:
    scope_id: str
    asset_class: str
    venue: str
    product: str
    kind: str                  # EXECUTION_VENUE | REFERENCE_DATA
    requires_api_execution: bool
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


SCOPES: Mapping[str, ScopeSpec] = {s.scope_id: s for s in (
    ScopeSpec("CRYPTO/BINANCE/PERPETUAL", "CRYPTO", "BINANCE_USDM", "PERPETUAL", "EXECUTION_VENUE", True,
              "the existing Section 22 scope"),
    ScopeSpec("CRYPTO/BYBIT/PERPETUAL", "CRYPTO", "BYBIT_LINEAR", "PERPETUAL", "EXECUTION_VENUE", True),
    ScopeSpec("CRYPTO/BINGX/PERPETUAL", "CRYPTO", "BINGX_SWAP", "PERPETUAL", "EXECUTION_VENUE", True),
    ScopeSpec("FX/REFERENCE_DATA/CATI_INTELLIGENCE", "FX", "FX_REFERENCE", "REFERENCE", "REFERENCE_DATA", False,
              "intelligence quality on provider reference data; certifies no execution venue"),
    ScopeSpec("FX/BYBIT/FX_PERPETUAL", "FX", "BYBIT_LINEAR", "FX_PERPETUAL", "EXECUTION_VENUE", True,
              "V5 instruments-info lists symbolType=forex perpetuals (EURUSDUSDT, GBPUSDUSDT, USDJPYUSDT; "
              "launched 2026-09-08) -- venue history is short, so the reference scope carries the learning"),
    ScopeSpec("FX/BINGX/TRADFI", "FX", "BINGX_TRADFI", "TRADFI", "EXECUTION_VENUE", True,
              "NCFX* contracts in the official swap contract API; API execution resolved per instrument "
              "(apiStateOpen/status); adapter not demo-validated"),
)}


@dataclass(frozen=True)
class ScopeFreeze:
    scope_id: str
    inputs: Mapping[str, Any]

    @property
    def missing(self) -> Tuple[str, ...]:
        return tuple(k for k in REQUIRED_FREEZE_INPUTS if self.inputs.get(k) in (None, "", [], {}))

    @property
    def freeze_hash(self) -> str:
        return hashlib.sha256(json.dumps({"scope_id": self.scope_id, **dict(self.inputs)}, sort_keys=True,
                                         default=str).encode()).hexdigest()


def holdout_namespace(scope_id: str, dataset_hash: str) -> str:
    """Scope-qualified dataset identity passed to HoldoutRegistry.reserve."""
    if scope_id not in SCOPES:
        raise KeyError(scope_id)
    return hashlib.sha256(f"{scope_id}|{dataset_hash}".encode()).hexdigest()


def scope_readiness(scope_id: str, *, freeze: Optional[ScopeFreeze], data_days: Optional[float],
                    min_days: float, api_execution_supported: Optional[bool]) -> Tuple[str, Tuple[str, ...]]:
    """Whether a scope may be REPLAYED. Never implies certification."""
    spec = SCOPES[scope_id]
    reasons = []
    if spec.requires_api_execution and api_execution_supported is not True:
        reasons.append("API_EXECUTION_NOT_SUPPORTED" if api_execution_supported is False else "API_EXECUTION_UNVERIFIED")
    if data_days is None or data_days < min_days:
        reasons.append(f"DATA_DAYS_{0 if data_days is None else int(data_days)}_LT_{int(min_days)}")
    if freeze is None or freeze.missing:
        reasons.append("FREEZE_INCOMPLETE:" + ",".join(freeze.missing if freeze else REQUIRED_FREEZE_INPUTS))
    if not reasons:
        return READY, ()
    status = (BLOCKED_CAPABILITY if reasons[0].startswith("API_") else
              BLOCKED_DATA if reasons[0].startswith("DATA_") else BLOCKED_FREEZE)
    return status, tuple(reasons)


__all__ = ["BLOCKED_CAPABILITY", "BLOCKED_DATA", "BLOCKED_FREEZE", "READY", "REQUIRED_FREEZE_INPUTS",
           "SCOPES", "SCOPE_REGISTRY_VERSION", "ScopeFreeze", "ScopeSpec", "holdout_namespace", "scope_readiness"]
