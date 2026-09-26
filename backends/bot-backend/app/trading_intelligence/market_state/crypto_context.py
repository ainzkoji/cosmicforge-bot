"""Canonical informational crypto view of already computed causal MarketState."""
from dataclasses import dataclass, asdict
from typing import Mapping
from app.trading_intelligence.contracts.global_market_state import GlobalComponent, AVAILABLE
from app.trading_intelligence.contracts.immutable import freeze
from app.trading_intelligence.hashing import stable_hash

@dataclass(frozen=True)
class CryptoMarketContext:
    canonical_symbol: str
    as_of_ms: int
    components: Mapping[str, GlobalComponent]
    version: str = "crypto-market-context-v1"

    def __post_init__(self):
        object.__setattr__(self, "components", freeze(self.components))

    def to_dict(self):
        body = {"canonical_symbol": self.canonical_symbol, "as_of_ms": self.as_of_ms,
                "version": self.version, "components": {k: v.to_dict() for k, v in self.components.items()}}
        return {**body, "context_hash": stable_hash(body)}

    @property
    def context_hash(self):
        return self.to_dict()["context_hash"]


def build_crypto_context(state, *, as_of_ms):
    causal = (state.latest_closed_candle_time <= as_of_ms
              and getattr(state, "decision_time", as_of_ms) <= as_of_ms)
    fields = {
        "market_structure": (state.structure_state, "structure_integrity"),
        "funding": (state.derivatives_state, "funding_current"),
        "basis": (state.derivatives_state, "basis"),
        "open_interest": (state.derivatives_state, "open_interest"),
        "spread": (state.liquidity_state, "spread_bps"),
        "order_book_depth": (state.liquidity_state, "top_book_depth"),
    }
    components = {}
    for name, (family, field) in fields.items():
        value = getattr(family, field, None) if causal and family.available else None
        reason = "NON_CAUSAL_MARKET_STATE" if not causal else (":".join(family.reason_codes) or "FEATURE_UNAVAILABLE")
        components[name] = (GlobalComponent.unavailable(name, reason) if value is None else
                            GlobalComponent(name, AVAILABLE, value=value, inputs=1))
    components["liquidations"] = GlobalComponent.unavailable("liquidations", "NOT_AVAILABLE_IN_MARKET_STATE")
    return CryptoMarketContext(state.instrument_key.canonical_symbol, as_of_ms, components)
