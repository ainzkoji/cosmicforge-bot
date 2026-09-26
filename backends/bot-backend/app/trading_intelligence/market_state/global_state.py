"""GlobalMarketState engine (Section 24): the epoch's per-instrument MarketStates -> one global view.

Pure and deterministic: the same MarketStates in any order produce the same
state and hash. Nothing is fetched and no feature is recomputed; every number
is an aggregate of a value the MarketState already carries. A component with
too few usable inputs is UNAVAILABLE with a reason -- never a neutral 0.

Components
----------
* ``crypto_context``     BTC trend direction/strength (the crypto reference), else UNAVAILABLE.
* ``breadth``            share of usable instruments trending UP minus DOWN, in [-1, 1].
* ``volatility``         median volatility percentile; label CALM / NORMAL / ELEVATED.
* ``liquidity``          median spread percentile and stale-book share.
* ``funding_basis``      median funding percentile + crowding share (derivatives only).
* ``usd_factor``         mean USD-leg trend sign across FX pairs (USD base +, USD quote -),
                         stablecoin-quoted crypto excluded (a USDT quote is not a USD view).
* ``currency_factors``   the same per currency present in >= MIN_INPUTS FX pairs.
* ``cross_asset_stress`` share of instruments in a volatility shock across ALL classes.
* ``risk_regime``        RISK_ON / RISK_OFF / MIXED / STRESS from breadth + crypto context + stress.
* ``correlation``        NOT computed in the cycle (the portfolio layer owns correlations):
                         UNAVAILABLE ``NOT_COMPUTED_IN_CYCLE``.
* ``event_risk``         the calendar source state if supplied, else UNAVAILABLE.
* ``data_quality``       counts VALID / DEGRADED / excluded inputs.
"""
from __future__ import annotations

from statistics import median
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.global_market_state import (AVAILABLE, UNAVAILABLE, GlobalComponent,
                                                                    GlobalMarketState)
from app.trading_intelligence.hashing import stable_hash

MIN_INPUTS = 3
_STABLE_QUOTES = frozenset({"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "USD1"})
_CRYPTO_REFERENCES = ("BTC", "XBT")


def _dedupe(states: Iterable[Any], decision_time: int) -> Tuple[List[Any], Dict[str, int]]:
    """One MarketState per canonical instrument (best data quality, then venue name): broker-neutral."""
    excluded = {"future_candle": 0, "invalid": 0, "duplicate_venue": 0}
    best: Dict[str, Any] = {}
    rank = {"VALID": 0, "DEGRADED": 1}
    for ms in states:
        if ms is None:
            continue
        if int(ms.latest_closed_candle_time) > int(decision_time) or int(ms.decision_time) > int(decision_time):
            excluded["future_candle"] += 1          # causal: nothing after the decision boundary
            continue
        if not ms.is_usable:
            excluded["invalid"] += 1
            continue
        key = f"{ms.instrument_key.asset_class}:{ms.instrument_key.canonical_symbol}"
        cur = best.get(key)
        cand = (rank.get(ms.data_quality.level.value, 2), ms.instrument_key.venue, ms.market_state_id)
        if cur is None:
            best[key] = ms
            continue
        excluded["duplicate_venue"] += 1
        if cand < (rank.get(cur.data_quality.level.value, 2), cur.instrument_key.venue, cur.market_state_id):
            best[key] = ms
    return [best[k] for k in sorted(best)], excluded


def _trend_sign(ms: Any) -> Optional[int]:
    t = ms.trend_state
    if not t.available:
        return None
    return {"UP": 1, "DOWN": -1, "FLAT": 0}.get(t.direction)


def _crypto(states: Sequence[Any]) -> GlobalComponent:
    refs = [s for s in states if s.instrument_key.asset_class == "CRYPTO"
            and s.instrument_key.base_asset.upper() in _CRYPTO_REFERENCES]
    if not refs:
        return GlobalComponent.unavailable("crypto_context", "NO_CRYPTO_REFERENCE_INSTRUMENT")
    ref = refs[0]
    t, v = ref.trend_state, ref.volatility_state
    if not t.available:
        return GlobalComponent.unavailable("crypto_context", "REFERENCE_TREND_UNAVAILABLE", inputs=1)
    return GlobalComponent("crypto_context", AVAILABLE, label=f"BTC_{t.direction}", value=float(t.strength),
                           inputs=1, detail={"reference": ref.instrument_key.canonical_symbol,
                                             "volatility_percentile": v.percentile if v.available else None,
                                             "shock": bool(v.shock_state) if v.available else None})


def _breadth(states: Sequence[Any]) -> GlobalComponent:
    signs = [x for x in (_trend_sign(s) for s in states) if x is not None]
    if len(signs) < MIN_INPUTS:
        return GlobalComponent.unavailable("breadth", f"INSUFFICIENT_INPUTS_{len(signs)}_LT_{MIN_INPUTS}",
                                           inputs=len(signs))
    up, down = signs.count(1), signs.count(-1)
    b = (up - down) / len(signs)
    label = "BROAD_UP" if b >= 0.3 else ("BROAD_DOWN" if b <= -0.3 else "MIXED")
    return GlobalComponent("breadth", AVAILABLE, label=label, value=round(b, 6), inputs=len(signs),
                           detail={"up": up, "down": down, "flat": signs.count(0)})


def _median_component(name: str, values: List[float], labeler, detail: Optional[Mapping[str, Any]] = None
                      ) -> GlobalComponent:
    if len(values) < MIN_INPUTS:
        return GlobalComponent.unavailable(name, f"INSUFFICIENT_INPUTS_{len(values)}_LT_{MIN_INPUTS}",
                                           inputs=len(values))
    m = float(median(values))
    return GlobalComponent(name, AVAILABLE, label=labeler(m), value=round(m, 6), inputs=len(values),
                           detail=dict(detail or {}))


def _volatility(states: Sequence[Any]) -> GlobalComponent:
    vals = [s.volatility_state.percentile for s in states
            if s.volatility_state.available and s.volatility_state.percentile is not None]
    return _median_component("volatility", vals,
                             lambda m: "ELEVATED" if m >= 0.8 else ("CALM" if m <= 0.2 else "NORMAL"))


def _liquidity(states: Sequence[Any]) -> GlobalComponent:
    usable = [s for s in states if s.liquidity_state.available and s.liquidity_state.spread_percentile is not None]
    stale = sum(1 for s in states if s.liquidity_state.available and s.liquidity_state.stale_book)
    return _median_component("liquidity", [s.liquidity_state.spread_percentile for s in usable],
                             lambda m: "THIN" if m >= 0.8 else "NORMAL",
                             {"stale_book_share": round(stale / len(states), 6) if states else None})


def _funding(states: Sequence[Any]) -> GlobalComponent:
    d = [s.derivatives_state for s in states if s.derivatives_state.available]
    vals = [x.funding_percentile for x in d if x.funding_percentile is not None]
    crowded = sum(1 for x in d if (x.crowding_state or "").upper().startswith("CROWDED"))
    return _median_component("funding_basis", vals,
                             lambda m: "LONG_CROWDED" if m >= 0.85 else ("SHORT_CROWDED" if m <= 0.15 else "BALANCED"),
                             {"crowded_share": round(crowded / len(d), 6) if d else None})


def _currency(states: Sequence[Any]) -> Tuple[GlobalComponent, GlobalComponent]:
    """Structural currency legs of FX pairs only (a stablecoin quote is not a currency view)."""
    legs: Dict[str, List[int]] = {}
    for s in states:
        k = s.instrument_key
        if k.asset_class != "FX":
            continue
        sign = _trend_sign(s)
        if sign is None:
            continue
        base, quote = k.base_asset.upper(), k.quote_asset.upper()
        if quote in _STABLE_QUOTES:
            quote = "USD"
        legs.setdefault(base, []).append(sign)
        legs.setdefault(quote, []).append(-sign)
    if not legs:
        none = GlobalComponent.unavailable("usd_factor", "NO_FX_INSTRUMENTS")
        return none, GlobalComponent.unavailable("currency_factors", "NO_FX_INSTRUMENTS")
    usd = legs.get("USD", [])
    usd_c = (GlobalComponent.unavailable("usd_factor", f"INSUFFICIENT_INPUTS_{len(usd)}_LT_{MIN_INPUTS}",
                                         inputs=len(usd)) if len(usd) < MIN_INPUTS else
             GlobalComponent("usd_factor", AVAILABLE,
                             label=("USD_STRONG" if sum(usd) / len(usd) >= 0.3 else
                                    "USD_WEAK" if sum(usd) / len(usd) <= -0.3 else "USD_NEUTRAL"),
                             value=round(sum(usd) / len(usd), 6), inputs=len(usd)))
    per = {c: round(sum(v) / len(v), 6) for c, v in sorted(legs.items()) if len(v) >= MIN_INPUTS}
    cur_c = (GlobalComponent("currency_factors", AVAILABLE, label="COMPUTED", inputs=sum(len(v) for v in legs.values()),
                             detail={"factor_scores": per,
                                     "insufficient": sorted(c for c, v in legs.items() if len(v) < MIN_INPUTS)})
             if per else GlobalComponent.unavailable("currency_factors", "NO_CURRENCY_WITH_MIN_INPUTS",
                                                     inputs=sum(len(v) for v in legs.values())))
    return usd_c, cur_c


def _stress(states: Sequence[Any]) -> GlobalComponent:
    vs = [s for s in states if s.volatility_state.available]
    if len(vs) < MIN_INPUTS:
        return GlobalComponent.unavailable("cross_asset_stress", f"INSUFFICIENT_INPUTS_{len(vs)}_LT_{MIN_INPUTS}",
                                           inputs=len(vs))
    by_class: Dict[str, List[bool]] = {}
    for s in vs:
        by_class.setdefault(s.instrument_key.asset_class, []).append(bool(s.volatility_state.shock_state))
    share = sum(sum(v) for v in by_class.values()) / len(vs)
    return GlobalComponent("cross_asset_stress", AVAILABLE,
                           label="STRESS" if share >= 0.4 else ("ELEVATED" if share >= 0.2 else "NORMAL"),
                           value=round(share, 6), inputs=len(vs),
                           detail={"shock_share_by_class": {c: round(sum(v) / len(v), 6)
                                                            for c, v in sorted(by_class.items())}})


def _risk_regime(crypto: GlobalComponent, breadth: GlobalComponent, stress: GlobalComponent) -> GlobalComponent:
    if stress.status == AVAILABLE and stress.label == "STRESS":
        return GlobalComponent("risk_regime", AVAILABLE, label="STRESS", inputs=stress.inputs,
                               detail={"driver": "cross_asset_stress"})
    if breadth.status != AVAILABLE:
        return GlobalComponent.unavailable("risk_regime", f"BREADTH_{breadth.reason}", inputs=breadth.inputs)
    crypto_dir = crypto.label if crypto.status == AVAILABLE else None
    if breadth.label == "BROAD_UP" and crypto_dir != "BTC_DOWN":
        label = "RISK_ON"
    elif breadth.label == "BROAD_DOWN" and crypto_dir != "BTC_UP":
        label = "RISK_OFF"
    else:
        label = "MIXED"
    return GlobalComponent("risk_regime", AVAILABLE, label=label, inputs=breadth.inputs,
                           detail={"breadth": breadth.label, "crypto": crypto_dir or "UNAVAILABLE",
                                   "stress": stress.label if stress.status == AVAILABLE else "UNAVAILABLE"})


def _event(event_source_state: Optional[str], observed_at: Optional[int], decision_time: int, max_age_ms: int) -> GlobalComponent:
    if not event_source_state:
        return GlobalComponent.unavailable("event_risk", "EVENT_CONTEXT_NOT_SUPPLIED")
    s = str(event_source_state).upper()
    if s != "AVAILABLE":
        return GlobalComponent.unavailable("event_risk", f"EVENT_CALENDAR_{s}")
    if observed_at is None or observed_at > decision_time:
        return GlobalComponent.unavailable("event_risk", "CALENDAR_TIMESTAMP_UNAVAILABLE_OR_NON_CAUSAL")
    if decision_time - observed_at > max_age_ms:
        return GlobalComponent.unavailable("event_risk", "EVENT_CALENDAR_STALE")
    return GlobalComponent("event_risk", AVAILABLE, label="CALENDAR_AVAILABLE", inputs=1)


def build_global_market_state(market_states: Iterable[Any], *, decision_time: int, timeframe: str,
                              event_source_state: Optional[str] = None, event_observed_at: Optional[int] = None,
                              event_max_age_ms: int = 3_600_000) -> GlobalMarketState:
    raw = [m for m in market_states if m is not None]
    states, excluded = _dedupe(raw, decision_time)
    crypto = _crypto(states)
    breadth = _breadth(states)
    stress = _stress(states)
    usd, cur = _currency(states)
    levels = [s.data_quality.level.value for s in states]
    dq = GlobalComponent("data_quality", AVAILABLE if states else UNAVAILABLE,
                         label=None if not states else ("DEGRADED" if "DEGRADED" in levels else "VALID"),
                         inputs=len(states), reason=None if states else "NO_USABLE_MARKET_STATES",
                         detail={"valid": levels.count("VALID"), "degraded": levels.count("DEGRADED"),
                                 "received": len(raw), **{f"excluded_{k}": v for k, v in excluded.items()}})
    components = {
        "crypto_context": crypto, "breadth": breadth, "volatility": _volatility(states),
        "liquidity": _liquidity(states), "funding_basis": _funding(states), "usd_factor": usd,
        "currency_factors": cur, "cross_asset_stress": stress,
        "risk_regime": _risk_regime(crypto, breadth, stress),
        "correlation": GlobalComponent.unavailable("correlation", "NOT_COMPUTED_IN_CYCLE"),
        "event_risk": _event(event_source_state, event_observed_at, decision_time, event_max_age_ms), "data_quality": dq,
    }
    inputs = sorted((f"{s.instrument_key.asset_class}:{s.instrument_key.canonical_symbol}", s.market_state_id,
                     s.data_hash) for s in states)
    return GlobalMarketState(decision_time=int(decision_time), timeframe=str(timeframe),
                             asset_classes=tuple(sorted({s.instrument_key.asset_class for s in states})),
                             components=components, input_market_state_ids=tuple(i[1] for i in inputs),
                             input_hash=stable_hash(inputs))


__all__ = ["MIN_INPUTS", "build_global_market_state"]
