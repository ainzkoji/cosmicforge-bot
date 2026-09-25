"""The GlobalMarketState contract (Section 24 ``contracts.global_market_state``).

One immutable, versioned, hashed, point-in-time view of the WHOLE market a
CATI decision epoch saw -- derived from the per-instrument ``MarketState``s
CATI already computed for that epoch (nothing is fetched or recomputed).

* causal: only MarketStates whose latest closed candle is at or before
  ``decision_time`` contribute;
* broker- and tenant-neutral: the inputs carry no account data (MarketState
  P3) and the contract has no user / account / bot field; the same canonical
  instrument seen on several venues counts ONCE;
* asset-class aware: crypto context, USD / currency-factor context from FX
  legs, cross-asset stress across every class present;
* missing is never zero: every component is ``AVAILABLE`` with a value or
  ``UNAVAILABLE`` with a reason (e.g. ``INSUFFICIENT_INPUTS_2_LT_3``,
  ``NO_FX_INSTRUMENTS``, ``NOT_COMPUTED_IN_CYCLE``).

It is CONTEXT and EVIDENCE: it does not admit, veto, rank or size anything.
Consuming it as a decision input is a governed policy-version change (the
frozen Section 22 policy hash would change), never a silent one.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash

GLOBAL_MARKET_STATE_SCHEMA_VERSION = "global-market-state-v1"
GLOBAL_MARKET_STATE_ENGINE_VERSION = "global-market-state-engine-v1"

AVAILABLE, UNAVAILABLE = "AVAILABLE", "UNAVAILABLE"


@dataclass(frozen=True)
class GlobalComponent:
    name: str
    status: str                                 # AVAILABLE | UNAVAILABLE
    label: Optional[str] = None                 # categorical state, e.g. RISK_ON / USD_STRONG
    value: Optional[float] = None               # numeric summary (never a substitute 0)
    inputs: int = 0                             # distinct canonical instruments used
    reason: Optional[str] = None                # why UNAVAILABLE
    detail: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in (AVAILABLE, UNAVAILABLE):
            raise ValueError(f"component status {self.status!r}")
        if self.status == UNAVAILABLE and not self.reason:
            raise ValueError(f"{self.name}: UNAVAILABLE needs a reason")
        if self.status == UNAVAILABLE and (self.value is not None or self.label is not None):
            raise ValueError(f"{self.name}: an UNAVAILABLE component carries no value")

    @classmethod
    def unavailable(cls, name: str, reason: str, *, inputs: int = 0) -> "GlobalComponent":
        return cls(name, UNAVAILABLE, None, None, inputs, reason)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["detail"] = dict(self.detail)
        return d


COMPONENTS = ("risk_regime", "crypto_context", "usd_factor", "currency_factors", "volatility", "liquidity",
              "breadth", "correlation", "funding_basis", "cross_asset_stress", "event_risk", "data_quality")


@dataclass(frozen=True)
class GlobalMarketState:
    decision_time: int
    timeframe: str
    asset_classes: Tuple[str, ...]
    components: Mapping[str, GlobalComponent]
    input_market_state_ids: Tuple[str, ...]      # sorted: lineage
    input_hash: str                              # hash of the (canonical id, market_state_id, data_hash) inputs
    schema_version: str = GLOBAL_MARKET_STATE_SCHEMA_VERSION
    engine_version: str = GLOBAL_MARKET_STATE_ENGINE_VERSION

    def __post_init__(self) -> None:
        missing = [c for c in COMPONENTS if c not in self.components]
        if missing:
            raise ValueError(f"GlobalMarketState missing components {missing}")

    def _identity(self) -> Dict[str, Any]:
        return {"decision_time": self.decision_time, "timeframe": self.timeframe,
                "asset_classes": list(self.asset_classes),
                "components": {k: self.components[k].to_dict() for k in sorted(self.components)},
                "input_hash": self.input_hash, "schema_version": self.schema_version,
                "engine_version": self.engine_version}

    @property
    def state_hash(self) -> str:
        return stable_hash(self._identity())

    @property
    def global_state_id(self) -> str:
        return short_id("gms", self._identity())

    def component(self, name: str) -> GlobalComponent:
        return self.components[name]

    def to_dict(self) -> Dict[str, Any]:
        return {**self._identity(), "global_state_id": self.global_state_id, "state_hash": self.state_hash,
                "input_market_state_ids": list(self.input_market_state_ids)}


__all__ = ["AVAILABLE", "COMPONENTS", "GLOBAL_MARKET_STATE_ENGINE_VERSION", "GLOBAL_MARKET_STATE_SCHEMA_VERSION",
           "GlobalComponent", "GlobalMarketState", "UNAVAILABLE"]
