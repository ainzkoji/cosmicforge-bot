"""What the portal is told about each market (Section H, Step 7.3).

``market_availability(db, user_id)`` answers, per market of the registry, ``{available, reasons}`` from four
independent sources, read at the moment of asking:

* **adapter support** -- an execution adapter exists for the market's venue
  (``platform_adapter.factory.ADAPTER_VERSIONS``: Binance, Bybit, BingX only);
* **research certification** -- some family declares the market (``pipeline.family_markets``) and the research
  register says that family is CERTIFIED (for traditional markets no family declares one at all);
* **activation stage** -- traditional markets also need a DEMO (or later) stage in the activation ledger
  (``markets.activation``); crypto markets are governed by the family governance log instead and are not
  re-gated here;
* **the user's connected accounts** -- a connected broker account on the market's venue.

Crypto markets keep their existing behaviour: this service does not add a gate to the crypto deployment path
(the deployment preview remains the authority there); it reports the facts. A traditional market is available
only when every source agrees, which no market does in this build, so ``CRYPTO_AND_TRADFI`` is never offered as
available and a deploy or preview asking for it is refused with ``TRADFI_NOT_AVAILABLE``.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Mapping, Optional

from app.trading_intelligence.pipeline import family_markets

from . import activation as A
from .calendar import VERIFIED
from .guard import MarketAvailabilityGuard
from .registry import MarketRegistry, default_registry

logger = logging.getLogger(__name__)

CRYPTO_ONLY, CRYPTO_AND_TRADFI = "CRYPTO_ONLY", "CRYPTO_AND_TRADFI"
MARKET_SCOPES = (CRYPTO_ONLY, CRYPTO_AND_TRADFI)
AVAILABILITY_VERSION = "1.0.0"

NO_EXECUTION_ADAPTER = "NO_EXECUTION_ADAPTER"
NO_CERTIFIED_FAMILY = "NO_CERTIFIED_FAMILY"
NO_FAMILY_DECLARES_MARKET = "NO_FAMILY_DECLARES_MARKET"
CALENDAR_UNVERIFIED = "CALENDAR_UNVERIFIED"
NO_CONNECTED_ACCOUNT = "NO_CONNECTED_ACCOUNT"
NO_TRADFI_MARKET_AVAILABLE = "NO_TRADFI_MARKET_AVAILABLE"

REASON_TEXT: Mapping[str, str] = {
    NO_EXECUTION_ADAPTER: "The platform has no execution adapter for this venue.",
    NO_CERTIFIED_FAMILY: "No strategy family has a research certification for this market.",
    NO_FAMILY_DECLARES_MARKET: "No strategy family was researched for this market.",
    CALENDAR_UNVERIFIED: "The market calendar has not been verified against an authoritative source.",
    NO_CONNECTED_ACCOUNT: "You have no connected account on this venue.",
    NO_TRADFI_MARKET_AVAILABLE: "Traditional markets (futures, FX, equities) are not available yet.",
}


def _reason(code: str) -> Dict[str, str]:
    base = code.split(":", 1)[0]
    return {"code": code, "message": REASON_TEXT.get(base, code.replace("_", " ").capitalize() + ".")}


def connected_venues(db: Any, user_id: Optional[str]) -> Dict[str, int]:
    """Connected broker accounts of the user per venue (broker id)."""
    if not user_id:
        return {}
    out: Dict[str, int] = {}
    try:
        with db.connect() as c:
            rows = c.execute("SELECT broker_id, status FROM broker_accounts WHERE user_id=?", (user_id,)).fetchall()
    except Exception as exc:  # noqa: BLE001 -- unreadable accounts are "none", never "some"
        logger.warning("market availability: broker accounts unreadable: %s", type(exc).__name__)
        return {}
    for r in rows:
        if str(r[1] or "").lower() in {"connected", "active"}:
            venue = str(r[0] or "").lower()
            out[venue] = out.get(venue, 0) + 1
    return out


def market_availability(db: Any, user_id: Optional[str], *, registry: Optional[MarketRegistry] = None,
                        adapter_available: Optional[Callable[[str], bool]] = None,
                        certification_lookup: Optional[Callable[[str, str], Mapping[str, Any]]] = None,
                        ledger: Optional[A.MarketActivationLedger] = None) -> Dict[str, Any]:
    registry = registry or default_registry()
    adapter_available = adapter_available or A.default_adapter_available
    certification_lookup = certification_lookup or A.default_certification_lookup(db)
    ledger = ledger or A.MarketActivationLedger(db, registry=registry)
    guard = MarketAvailabilityGuard(registry)
    venues = connected_venues(db, user_id)
    markets: List[Dict[str, Any]] = []
    for market in registry.markets.values():
        calendar = registry.calendar(market.calendar_id)
        reasons: List[str] = []
        if not adapter_available(market.venue):
            reasons.append(NO_EXECUTION_ADAPTER)
        families = family_markets.families_supporting(market.market_id)
        certified: List[str] = []
        stage = None
        if market.is_tradfi:
            if not families:
                reasons.append(NO_FAMILY_DECLARES_MARKET)
            for fid in families:
                try:
                    if (certification_lookup(fid, market.market_id) or {}).get("status") == "CERTIFIED":
                        certified.append(fid)
                except Exception:  # noqa: BLE001 -- unreadable evidence certifies nothing
                    pass
            if not certified:
                reasons.append(NO_CERTIFIED_FAMILY)
            if not guard.calendar_trusted(market.market_id):
                reasons.append(CALENDAR_UNVERIFIED)
            try:
                stage = ledger.highest_stage(market.market_id)
            except Exception:  # noqa: BLE001 -- an unreadable ledger is no stage
                stage = A.NONE
            if stage not in (A.DEMO, A.LIVE):
                reasons.append(f"ACTIVATION_STAGE_{stage}")
        if not venues.get(market.venue):
            reasons.append(NO_CONNECTED_ACCOUNT)
        # crypto: an account is the user's choice, not a platform refusal -- reported, not blocking
        blocking = [r for r in reasons if market.is_tradfi or r != NO_CONNECTED_ACCOUNT]
        markets.append({
            "market_id": market.market_id, "asset_class": market.asset_class, "market_type": market.market_type,
            "venue": market.venue, "product": market.product, "is_tradfi": market.is_tradfi,
            "calendar_id": calendar.calendar_id, "calendar_verification_status": calendar.verification_status,
            "calendar_verified": calendar.verification_status == VERIFIED, "families": list(families),
            "certified_families": certified, "activation_stage": stage,
            "connected_accounts": venues.get(market.venue, 0),
            "available": not blocking, "reasons": [_reason(r) for r in reasons]})
    crypto_available = any(m["available"] for m in markets if not m["is_tradfi"])
    tradfi = [m for m in markets if m["is_tradfi"]]
    tradfi_available = bool(tradfi) and any(m["available"] for m in tradfi)
    tradfi_reasons = sorted({r["code"] for m in tradfi for r in m["reasons"]}) if tradfi else [NO_TRADFI_MARKET_AVAILABLE]
    return {
        "version": AVAILABILITY_VERSION, "data_versions": dict(registry.data_versions), "markets": markets,
        "scopes": {
            CRYPTO_ONLY: {"available": crypto_available,
                          "reasons": [] if crypto_available else [_reason(NO_EXECUTION_ADAPTER)]},
            CRYPTO_AND_TRADFI: {"available": crypto_available and tradfi_available,
                                "reasons": [] if tradfi_available else
                                [_reason(NO_TRADFI_MARKET_AVAILABLE)] + [_reason(c) for c in tradfi_reasons]},
        },
        "default_scope": CRYPTO_ONLY,
        "live_trading": "DISABLED",
    }


def tradfi_blocker_detail(db: Any, user_id: Optional[str], **kw: Any) -> Dict[str, Any]:
    """The reasons a ``CRYPTO_AND_TRADFI`` request is refused, for the deployment blocker."""
    try:
        view = market_availability(db, user_id, **kw)
        scope = view["scopes"][CRYPTO_AND_TRADFI]
        return {"available": scope["available"], "reasons": scope["reasons"]}
    except Exception as exc:  # noqa: BLE001 -- the answer is still "not available"
        logger.warning("market availability unavailable: %s", type(exc).__name__)
        return {"available": False, "reasons": [_reason("MARKET_AVAILABILITY_UNREADABLE")]}


__all__ = ["CRYPTO_ONLY", "CRYPTO_AND_TRADFI", "MARKET_SCOPES", "AVAILABILITY_VERSION", "market_availability",
           "tradfi_blocker_detail", "connected_venues"]
