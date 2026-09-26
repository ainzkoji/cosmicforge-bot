"""FXMarketContext (Phase 5B): the canonical FX view CATI reasons over.

Deterministic, causal (only data at or before ``as_of_ms``), versioned,
tenant-neutral (no account data) and broker-neutral (the reference feed is a
provider, the venue quote is whatever venue the instrument key names).

Every input that is missing is ``UNAVAILABLE`` with a reason in
``unavailable`` -- never a 0 that downstream code could read as "no spread",
"no funding" or "no divergence".
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

from app.market_data.divergence import compute_divergence
from app.market_data.fx_reference import fx_session

FX_CONTEXT_VERSION = "fx-market-context-v2"
USD_CODES = frozenset({"USD", "USDT", "USDC"})


@dataclass(frozen=True)
class FXMarketContext:
    canonical_symbol: str
    venue: str
    venue_symbol: str
    as_of_ms: int
    base_currency: str
    quote_currency: str
    currency_exposure_long: Mapping[str, int]     # unit legs for one LONG unit
    usd_exposure_long: int                        # +1 USD long, -1 USD short, 0 = no USD leg
    session: str
    session_overlap: bool
    market_open: bool
    reference_price: Optional[float]
    reference_spread_bps: Optional[float]
    venue_price: Optional[float]
    venue_spread_bps: Optional[float]
    divergence_bps: Optional[float]
    spread_state: str                             # NORMAL | WIDE | UNAVAILABLE
    funding_rate: Optional[float]
    rate_differential: Optional[float]
    calendar_risk: str                            # CLEAR | ELEVATED | BLACKOUT | UNAVAILABLE
    unavailable: Mapping[str, str] = field(default_factory=dict)
    reference_provider: Optional[str] = None
    calendar_observed_at: Optional[int] = None
    calendar_availability: str = "UNAVAILABLE_WITH_REASON"
    version: str = FX_CONTEXT_VERSION

    def __post_init__(self):
        from app.trading_intelligence.contracts.immutable import freeze
        object.__setattr__(self, "unavailable", freeze(self.unavailable))
        object.__setattr__(self, "currency_exposure_long", freeze(self.currency_exposure_long))

    @property
    def context_hash(self) -> str:
        d = asdict(self)
        return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()

    @property
    def tradable(self) -> bool:
        """Minimum inputs for an FX entry: market open + a reference price."""
        return self.market_open and self.reference_price is not None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["context_hash"] = self.context_hash
        d["tradable"] = self.tradable
        return d


def build_fx_context(*, instrument_key: Any, as_of_ms: int, reference: Optional[Mapping[str, Any]],
                     venue_quote: Optional[Mapping[str, Any]] = None, funding_rate: Optional[float] = None,
                     rate_differential: Optional[float] = None, calendar_state: Optional[str] = None,
                     calendar_observed_at: Optional[int] = None, calendar_max_age_ms: int = 3_600_000,
                     wide_spread_bps: float = 8.0) -> FXMarketContext:
    """``reference``: fx_reference_quotes row closed at/before as_of_ms;
    ``venue_quote``: {"mark", "last", "bid", "ask", "time"} from the execution
    venue (may be None when the pair is only researched, not traded)."""
    base, quote = instrument_key.base_asset.upper(), instrument_key.quote_asset.upper()
    unavailable: Dict[str, str] = {}
    session = fx_session(as_of_ms)
    market_open = session != "CLOSED"
    if venue_quote is not None and venue_quote.get("time") is not None and int(venue_quote["time"]) > as_of_ms:
        venue_quote = None  # a quote from the future is not causal
        unavailable["venue_quote"] = "VENUE_QUOTE_AFTER_DECISION_TIME"
    if reference is not None and int(reference.get("open_time", 0)) > as_of_ms:
        reference = None
        unavailable["reference"] = "REFERENCE_AFTER_DECISION_TIME"
    div = compute_divergence(as_of_ms=as_of_ms, reference=dict(reference) if reference else None,
                             venue_mark=_f(venue_quote, "mark"), venue_last=_f(venue_quote, "last"),
                             venue_bid=_f(venue_quote, "bid"), venue_ask=_f(venue_quote, "ask"),
                             settlement_asset=getattr(instrument_key, "settlement_asset", None) or quote)
    ref_price = div.reference_mid if div.status == "AVAILABLE" or div.reason == "VENUE_PRICE_UNAVAILABLE" else None
    if ref_price is None:
        unavailable.setdefault("reference", div.reason or "REFERENCE_UNAVAILABLE")
    venue_price = div.venue_mid or div.venue_mark or div.venue_last
    if venue_price is None:
        unavailable.setdefault("venue_price", "VENUE_QUOTE_UNAVAILABLE")
    divergence = div.mid_vs_reference_bps if div.mid_vs_reference_bps is not None else div.mark_vs_reference_bps
    if divergence is None:
        unavailable.setdefault("divergence", div.reason or "DIVERGENCE_UNAVAILABLE")
    spread = div.venue_spread_bps
    spread_state = "UNAVAILABLE" if spread is None else ("WIDE" if spread > wide_spread_bps else "NORMAL")
    if spread is None:
        unavailable["spread"] = "NO_BID_ASK"
    if funding_rate is None:
        unavailable["funding"] = "FUNDING_UNAVAILABLE"
    if rate_differential is None:
        unavailable["rate_differential"] = "NO_RATE_SOURCE"
    cal = calendar_state or "UNAVAILABLE"
    calendar_availability = "AVAILABLE"
    if cal in ("UNAVAILABLE", "STALE"):
        calendar_availability = "STALE" if cal == "STALE" else "UNAVAILABLE_WITH_REASON"
        unavailable["calendar"] = "CALENDAR_STALE" if cal == "STALE" else "NO_VALIDATED_CALENDAR_SOURCE"
    elif calendar_observed_at is None or calendar_observed_at > as_of_ms:
        cal, calendar_availability = "UNAVAILABLE", "UNAVAILABLE_WITH_REASON"
        unavailable["calendar"] = "CALENDAR_TIMESTAMP_UNAVAILABLE_OR_NON_CAUSAL"
    elif as_of_ms - calendar_observed_at > calendar_max_age_ms:
        cal, calendar_availability = "STALE", "STALE"
        unavailable["calendar"] = "CALENDAR_STALE"
    legs = {base: +1, quote: -1}
    usd = (1 if base in USD_CODES else 0) - (1 if quote in USD_CODES else 0)
    return FXMarketContext(
        canonical_symbol=instrument_key.canonical_symbol, venue=instrument_key.venue,
        venue_symbol=instrument_key.venue_symbol, as_of_ms=int(as_of_ms), base_currency=base, quote_currency=quote,
        currency_exposure_long=legs, usd_exposure_long=usd, session=session,
        session_overlap=session == "LONDON_NY_OVERLAP", market_open=market_open, reference_price=ref_price,
        reference_spread_bps=div.reference_spread_bps, venue_price=venue_price, venue_spread_bps=div.venue_spread_bps,
        divergence_bps=divergence, spread_state=spread_state, funding_rate=funding_rate,
        rate_differential=rate_differential, calendar_risk=cal, unavailable=dict(sorted(unavailable.items())),
        reference_provider=reference.get("provider") if reference else None,
        calendar_observed_at=calendar_observed_at, calendar_availability=calendar_availability,
    )


def _f(d: Optional[Mapping[str, Any]], k: str) -> Optional[float]:
    if not d or d.get(k) in (None, ""):
        return None
    try:
        return float(d[k])
    except (TypeError, ValueError):
        return None


__all__ = ["FXMarketContext", "FX_CONTEXT_VERSION", "build_fx_context"]
