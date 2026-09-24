"""Modeled historical venue economics for certification replay (Section 22.12).

History has no order book, depth or funding snapshots, and no broker
execution can exist for it. The replay therefore feeds the CANONICAL
Section 17 Binance adapter + venue cost model (the same code the shadow
runtime uses) a raw snapshot DERIVED from the historical candle and the
canonical research cost model (``app.replay.cost_model``):

* top of book  = candle close -/+ the model's half-spread (quantity unknown)
* depth        = ABSENT (no depth history) -> the venue model's own
                 conservative slippage fallback applies
* funding      = the model's funding rate on the standard 8h schedule
* metadata     = a versioned static USD-M instrument spec (precision only)
* fees         = the venue policy's conservative fallback tier

Every observation built this way carries ``REPLAY_MODELED_VENUE_ECONOMICS``
in its reason codes: the costs are MODELED assumptions, never proof of live
execution quality (forward demo validates that). Cost stress multiplies the
modeled spread and funding here and every fee/spread/slippage/funding term
in the venue policy (``stress_venue_policy``) -- the market path is untouched.

The environment is the MARKET-DATA environment of the history (Binance
production candles => ``REAL``); no broker account or credential is involved.
"""
from __future__ import annotations

import math
from dataclasses import replace
from typing import Any, Mapping, Optional

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.trading_intelligence.venue.adapter import VenueRawSnapshot
from app.trading_intelligence.venue.context import VenueEconomicContext
from app.trading_intelligence.venue.policy import VenueCostPolicy, default_venue_cost_policy
from app.trading_intelligence.versions import REPLAY_VENUE_MODEL_VERSION

REPLAY_REASON = "REPLAY_MODELED_VENUE_ECONOMICS"
COST_PROVENANCE = "REPLAY_MODELED_RESEARCH_COST_MODEL"
REPLAY_ENVIRONMENT = "REAL"
_FUNDING_MS = 8 * 3_600_000

#: public USD-M precision (tick, step, min notional); affects rounding only
_INSTRUMENT_SPEC: Mapping[str, tuple] = {
    "BTCUSDT": ("0.10", "0.001", "100"), "ETHUSDT": ("0.01", "0.001", "20"),
    "BNBUSDT": ("0.010", "0.01", "5"), "SOLUSDT": ("0.0100", "1", "5"), "XRPUSDT": ("0.0001", "0.1", "5"),
}


def _derived_spec(price: float) -> tuple:
    """Deterministic precision for symbols without a recorded spec."""
    mag = math.floor(math.log10(max(price, 1e-12)))
    return (f"{10.0 ** (mag - 4):.10f}".rstrip("0"), "1" if price < 10 else "0.001", "5")


def _exchange_info(symbol: str, price: float) -> dict:
    tick, step, notional = _INSTRUMENT_SPEC.get(symbol, _derived_spec(price))
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    return {"symbol": symbol, "pair": symbol, "contractType": "PERPETUAL", "status": "TRADING", "baseAsset": base,
            "quoteAsset": "USDT", "marginAsset": "USDT",
            "filters": [{"filterType": "PRICE_FILTER", "tickSize": tick},
                        {"filterType": "LOT_SIZE", "stepSize": step, "minQty": step},
                        {"filterType": "MIN_NOTIONAL", "notional": notional}]}


def replay_raw_snapshot(symbol: str, close: float, decision_time: int, *, cost_model: CostModel,
                        cost_multiplier: float = 1.0) -> VenueRawSnapshot:
    sym = str(symbol).upper()
    half = cost_model.spread * float(cost_multiplier)
    bid, ask = close * (1.0 - half), close * (1.0 + half)
    next_funding = (int(decision_time) // _FUNDING_MS + 1) * _FUNDING_MS
    payloads = {
        "exchange_info_symbol": _exchange_info(sym, close), "exchange_info_as_of": int(decision_time),
        "funding_info": [],
        "book_ticker": {"symbol": sym, "bidPrice": repr(bid), "askPrice": repr(ask), "time": int(decision_time)},
        "premium_index": {"symbol": sym, "markPrice": repr(close), "indexPrice": repr(close),
                          "lastFundingRate": repr(cost_model.funding_rate * float(cost_multiplier)),
                          "nextFundingTime": next_funding, "time": int(decision_time)},
    }
    return VenueRawSnapshot(venue_symbol=sym, payloads=payloads, captured_at=int(decision_time),
                            reason_codes=(REPLAY_REASON,))


def stress_venue_policy(policy: VenueCostPolicy, multiplier: float) -> VenueCostPolicy:
    """Every cost term x ``multiplier`` (fees, spread, slippage, funding).
    Uncertainty fractions scale with the term they multiply, so they are
    left as-is -- scaling them too would double count the stress."""
    m = float(multiplier)
    if m == 1.0:
        return policy
    scale = lambda mapping: {k: (tuple(x * m for x in v) if isinstance(v, tuple) else v * m)  # noqa: E731
                             for k, v in dict(mapping).items()}
    return replace(
        policy, fallback_fee_rates=scale(policy.fallback_fee_rates),
        fallback_commission_per_contract=scale(policy.fallback_commission_per_contract),
        liquidity_bucket_spread_bps=scale(policy.liquidity_bucket_spread_bps),
        fallback_spread_bps=scale(policy.fallback_spread_bps),
        liquidity_bucket_slippage_bps=scale(policy.liquidity_bucket_slippage_bps),
        venue_class_slippage_bps=scale(policy.venue_class_slippage_bps),
        conservative_slippage_bps=policy.conservative_slippage_bps * m,
        min_slippage_bps_floor=policy.min_slippage_bps_floor * m,
        beyond_depth_penalty_bps=policy.beyond_depth_penalty_bps * m,
        fallback_funding_rate_per_interval=policy.fallback_funding_rate_per_interval * m,
    )


def replay_venue_context(symbol: str, close: float, decision_time: int, *,
                         cost_model: Optional[CostModel] = None, policy: Optional[VenueCostPolicy] = None,
                         cost_multiplier: float = 1.0, run_id: Optional[str] = None) -> VenueEconomicContext:
    from app.trading_intelligence.venue.registry import resolve_adapter

    base_policy = policy or default_venue_cost_policy()
    stressed = stress_venue_policy(base_policy, cost_multiplier)
    adapter, _collector = resolve_adapter("binance", stressed)
    raw = replay_raw_snapshot(symbol, close, decision_time, cost_model=cost_model or BINANCE_FUTURES_STANDARD,
                              cost_multiplier=cost_multiplier)
    return VenueEconomicContext(adapter=adapter, raw=raw, environment=REPLAY_ENVIRONMENT,
                                decision_time=int(decision_time), run_id=run_id, policy=stressed)


def replay_venue_model_identity(cost_model: Optional[CostModel] = None,
                                policy: Optional[VenueCostPolicy] = None) -> Mapping[str, Any]:
    cm = cost_model or BINANCE_FUTURES_STANDARD
    p = policy or default_venue_cost_policy()
    return {"version": REPLAY_VENUE_MODEL_VERSION, "cost_provenance": COST_PROVENANCE,
            "research_cost_model_hash": cm.model_hash, "venue_cost_policy_hash": p.policy_hash,
            "environment": REPLAY_ENVIRONMENT, "depth": "ABSENT_NO_HISTORY", "book_quantity": "UNKNOWN"}


__all__ = ["replay_venue_context", "replay_raw_snapshot", "stress_venue_policy", "replay_venue_model_identity",
           "REPLAY_REASON", "COST_PROVENANCE", "REPLAY_ENVIRONMENT"]
