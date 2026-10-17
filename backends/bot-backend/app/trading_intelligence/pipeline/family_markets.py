"""Which markets and calendars each registry family supports (Section H, Step 7.2).

Kept apart from ``family_registry.FamilyDefinition`` on purpose: a family's ``definition_hash`` is the hash of
its plain definition and is recorded in the governance log at registration, so adding a field there would
change every hash and invalidate every registration. The declaration here is keyed by ``family_id`` and does
not enter any hash.

A declaration only states what a family's rules were written and researched for. It authorizes nothing: a
family still needs its certification and governance stage, and a market its calendar, adapter and activation.
Every family the registry defines must be declared (tested), and the declared markets must agree with the
definition's ``supported_markets`` (venue, product).

No family declares a traditional market: Mandate 004 (hypothesis 18) is a crypto perpetual hypothesis and its
outcome does not transfer to any other market (``research/trend_v2_tradfi/SPEC.md``).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Tuple

CRYPTO_USDT_PERPETUAL = "CRYPTO_USDT_PERPETUAL"
CRYPTO_24_7 = "CRYPTO_24_7"
BINANCE_USDT_PERPETUAL = "CRYPTO_BINANCE_USDT_PERPETUAL"


@dataclass(frozen=True)
class FamilyMarketDeclaration:
    family_id: str
    market_types: Tuple[str, ...]
    markets: Tuple[str, ...]          # market ids of app/markets/data/markets.json
    calendars: Tuple[str, ...]        # calendar ids of app/markets/data/calendars.json
    note: str = ""


def _crypto_binance(family_id: str, note: str) -> FamilyMarketDeclaration:
    return FamilyMarketDeclaration(family_id=family_id, market_types=(CRYPTO_USDT_PERPETUAL,),
                                   markets=(BINANCE_USDT_PERPETUAL,), calendars=(CRYPTO_24_7,), note=note)


FAMILY_MARKETS: Mapping[str, FamilyMarketDeclaration] = {
    d.family_id: d for d in (
        _crypto_binance("DAILY_TREND", "Mandate 004 / hypothesis 18: Binance USD-M USDT perpetuals, daily UTC bars."),
        _crypto_binance("RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", "Mandate 003 / hypothesis 15 (failed): Binance USDT perpetuals."),
        _crypto_binance("TREND_PULLBACK_V2", "Forecast-line specialist on Binance USDT perpetuals."),
        _crypto_binance("BREAKOUT_VOL_EXPANSION_V2", "Forecast-line specialist on Binance USDT perpetuals."),
        _crypto_binance("RANGE_MEAN_REVERSION_V2", "Forecast-line specialist on Binance USDT perpetuals."),
        _crypto_binance("MOMENTUM_CONTINUATION_V1", "Forecast-line specialist on Binance USDT perpetuals."),
    )
}


def declaration(family_id: str) -> Optional[FamilyMarketDeclaration]:
    return FAMILY_MARKETS.get(family_id)


def families_supporting(market_id: str) -> Tuple[str, ...]:
    return tuple(sorted(fid for fid, d in FAMILY_MARKETS.items() if market_id in d.markets))


def supports(family_id: str, market_id: str) -> bool:
    d = FAMILY_MARKETS.get(family_id)
    return d is not None and market_id in d.markets


def as_plain() -> Dict[str, Dict[str, object]]:
    return {fid: {"market_types": list(d.market_types), "markets": list(d.markets), "calendars": list(d.calendars),
                  "note": d.note} for fid, d in sorted(FAMILY_MARKETS.items())}


__all__ = ["CRYPTO_USDT_PERPETUAL", "CRYPTO_24_7", "BINANCE_USDT_PERPETUAL", "FamilyMarketDeclaration",
           "FAMILY_MARKETS", "declaration", "families_supporting", "supports", "as_plain"]
