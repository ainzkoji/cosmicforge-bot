"""Declared CATI economic adapter statuses and broker -> adapter resolution
(Section 17.17).

A venue is not "supported" because an exchange directory exists. Status is
a declared, versioned fact about what an adapter has PROVEN:

* binance_usdm / DEMO -> DEMO_VALIDATED: contract suite passes on recorded
  demo-fapi.binance.com payloads AND the live demo market-data endpoints were
  verified (2026-09-23). Not PRODUCTION_VALIDATED: no planned-vs-realized
  execution-cost calibration exists yet.
* binance_usdm / TESTNET, REAL -> SHADOW_VALIDATED: identical API and code
  path, not exercised against those environments here.
* forex_reference, dated_futures_reference -> UNVALIDATED (default): contract-
  ready, but no real broker integration feeds them yet.
* every other broker (bybit, bingx, oanda, ibkr, mt4/5, ...) -> no adapter:
  ``UnsupportedVenueAdapter`` (fails closed).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.venue_economics import AdapterValidationStatus as S
from app.trading_intelligence.venue.adapter import BaseVenueEconomicAdapter, UnsupportedVenueAdapter
from app.trading_intelligence.venue.policy import VenueCostPolicy
from app.trading_intelligence.versions import ADAPTER_STATUS_REGISTRY_VERSION

ADAPTER_STATUS_REGISTRY: Mapping[Tuple[str, str], str] = {
    ("binance_usdm", "DEMO"): S.DEMO_VALIDATED.value,
    ("binance_usdm", "TESTNET"): S.SHADOW_VALIDATED.value,
    ("binance_usdm", "REAL"): S.SHADOW_VALIDATED.value,
}
REGISTRY_VERSION = ADAPTER_STATUS_REGISTRY_VERSION


def _binance():
    from app.trading_intelligence.venue.binance import BinanceUsdmEconomicAdapter, collect_binance_raw

    return BinanceUsdmEconomicAdapter, collect_binance_raw


#: broker_type (BotRunContext / broker_accounts) -> (adapter class, raw collector) factory
_BROKER_ADAPTERS: Dict[str, Callable[[], Tuple[type, Callable[..., Any]]]] = {"binance": _binance}


def resolve_adapter(broker_type: Optional[str], policy: Optional[VenueCostPolicy] = None
                    ) -> Tuple[BaseVenueEconomicAdapter, Optional[Callable[..., Any]]]:
    """(adapter, collector). An unknown broker gets the fail-closed
    ``UnsupportedVenueAdapter`` and no collector."""
    factory = _BROKER_ADAPTERS.get(str(broker_type or "").strip().lower())
    if factory is None:
        return UnsupportedVenueAdapter(broker_type or "UNKNOWN", policy), None
    cls, collector = factory()
    return cls(policy), collector


__all__ = ["ADAPTER_STATUS_REGISTRY", "REGISTRY_VERSION", "resolve_adapter"]
