"""Market, calendar and contract definitions loaded from versioned data files (Step 7.2).

``app/markets/data/`` holds three JSON files (``calendars.json``, ``markets.json``, ``contracts.json``), each
with a ``schema_version`` and a ``data_version``. The registry validates them together and fails loudly on any
inconsistency: an unknown calendar, a duplicate id, an entry without a ``source`` or a
``verification_status``, a VERIFIED session calendar without a stated coverage period (holidays outside the
data are unknown), or a PLATFORM_CONVENTION status anywhere except a continuous crypto calendar.

Production callers use ``default_registry()``; tests build a registry from fixture data with
``MarketRegistry.from_documents``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from app.trading_intelligence.contracts.instrument import CRYPTO

from .calendar import CONTINUOUS, PLATFORM_CONVENTION, SESSIONS, VERIFICATION_STATUSES, VERIFIED, MarketCalendar
from .contracts import ContractSpec
from .roll import ContractSeries, RollRule

SCHEMA_VERSION = "1"
DATA_DIR = Path(__file__).resolve().parent / "data"
CONTRACT_SOURCES = ("VENUE_RULES_CACHE", "DATA_FILE")


class MarketRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class Freshness:
    timeframe: str
    max_missing_bars: int
    source: str


@dataclass(frozen=True)
class MarketDefinition:
    market_id: str
    asset_class: str
    market_type: str
    venue: str
    product: str
    calendar_id: str
    settlement_currency: str
    contract_source: str
    freshness: Freshness
    source: str
    verification_status: str
    description: str = ""

    @property
    def is_tradfi(self) -> bool:
        return self.asset_class != CRYPTO

    def to_plain(self) -> Dict[str, Any]:
        return {"market_id": self.market_id, "asset_class": self.asset_class, "market_type": self.market_type,
                "venue": self.venue, "product": self.product, "calendar_id": self.calendar_id,
                "settlement_currency": self.settlement_currency, "contract_source": self.contract_source,
                "freshness": {"timeframe": self.freshness.timeframe, "max_missing_bars": self.freshness.max_missing_bars,
                              "source": self.freshness.source},
                "source": self.source, "verification_status": self.verification_status, "is_tradfi": self.is_tradfi}


def _need(d: Mapping[str, Any], key: str, where: str) -> Any:
    value = d.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise MarketRegistryError(f"{where}: {key} is required")
    return value


def _status(d: Mapping[str, Any], where: str) -> str:
    status = _need(d, "verification_status", where)
    if status not in VERIFICATION_STATUSES:
        raise MarketRegistryError(f"{where}: verification_status must be one of {VERIFICATION_STATUSES}")
    _need(d, "source", where)
    return status


@dataclass
class MarketRegistry:
    calendars: Dict[str, MarketCalendar] = field(default_factory=dict)
    markets: Dict[str, MarketDefinition] = field(default_factory=dict)
    contracts: Dict[str, ContractSpec] = field(default_factory=dict)
    series: Dict[str, ContractSeries] = field(default_factory=dict)
    data_versions: Dict[str, str] = field(default_factory=dict)

    # ---------------------------------------------------------------------------------------- loading
    @classmethod
    def from_documents(cls, *, calendars: Mapping[str, Any], markets: Mapping[str, Any],
                       contracts: Optional[Mapping[str, Any]] = None) -> "MarketRegistry":
        contracts = contracts or {"schema_version": SCHEMA_VERSION, "data_version": "empty", "contracts": [], "series": []}
        reg = cls()
        for name, doc in (("calendars", calendars), ("markets", markets), ("contracts", contracts)):
            if str(doc.get("schema_version")) != SCHEMA_VERSION:
                raise MarketRegistryError(f"{name}: schema_version {doc.get('schema_version')!r} is not {SCHEMA_VERSION}")
            reg.data_versions[name] = str(_need(doc, "data_version", name))

        for raw in calendars.get("calendars") or ():
            where = f"calendar {raw.get('calendar_id')}"
            _status(raw, where)
            cal = MarketCalendar.from_plain(raw)
            if cal.calendar_id in reg.calendars:
                raise MarketRegistryError(f"{where}: defined twice")
            if cal.verification_status == PLATFORM_CONVENTION and cal.kind != CONTINUOUS:
                raise MarketRegistryError(f"{where}: PLATFORM_CONVENTION is only accepted for a continuous calendar")
            if cal.verification_status == VERIFIED and cal.kind == SESSIONS and cal.coverage is None:
                raise MarketRegistryError(f"{where}: a VERIFIED session calendar states the period its holiday data covers")
            reg.calendars[cal.calendar_id] = cal

        for raw in markets.get("markets") or ():
            where = f"market {raw.get('market_id')}"
            status = _status(raw, where)
            fresh = raw.get("freshness") or {}
            market = MarketDefinition(
                market_id=_need(raw, "market_id", where), asset_class=_need(raw, "asset_class", where),
                market_type=_need(raw, "market_type", where), venue=str(_need(raw, "venue", where)).lower(),
                product=_need(raw, "product", where), calendar_id=_need(raw, "calendar_id", where),
                settlement_currency=_need(raw, "settlement_currency", where),
                contract_source=_need(raw, "contract_source", where),
                freshness=Freshness(str(_need(fresh, "timeframe", where + ".freshness")),
                                    int(_need(fresh, "max_missing_bars", where + ".freshness")),
                                    str(_need(fresh, "source", where + ".freshness"))),
                source=raw["source"], verification_status=status, description=str(raw.get("description") or ""))
            if market.market_id in reg.markets:
                raise MarketRegistryError(f"{where}: defined twice")
            if market.calendar_id not in reg.calendars:
                raise MarketRegistryError(f"{where}: unknown calendar {market.calendar_id}")
            if market.contract_source not in CONTRACT_SOURCES:
                raise MarketRegistryError(f"{where}: contract_source must be one of {CONTRACT_SOURCES}")
            if status == PLATFORM_CONVENTION and (market.asset_class != CRYPTO
                                                  or not reg.calendars[market.calendar_id].is_continuous):
                raise MarketRegistryError(f"{where}: PLATFORM_CONVENTION is only accepted for a continuous crypto market")
            reg.markets[market.market_id] = market

        for raw in contracts.get("contracts") or ():
            where = f"contract {raw.get('symbol')}"
            _status(raw, where)
            spec = ContractSpec.from_plain(raw)
            key = f"{spec.venue}:{spec.symbol}"
            if key in reg.contracts:
                raise MarketRegistryError(f"{where}: defined twice")
            if spec.calendar_id not in reg.calendars:
                raise MarketRegistryError(f"{where}: unknown calendar {spec.calendar_id}")
            reg.contracts[key] = spec

        for raw in contracts.get("series") or ():
            where = f"series {raw.get('root')}"
            _status(raw, where)
            venue = str(_need(raw, "venue", where)).lower()
            members = []
            for symbol in _need(raw, "contracts", where):
                key = f"{venue}:{symbol}"
                if key not in reg.contracts:
                    raise MarketRegistryError(f"{where}: unknown contract {symbol}")
                members.append(reg.contracts[key])
            rule = raw.get("roll_rule") or {}
            series = ContractSeries(root=_need(raw, "root", where), venue=venue,
                                    rule=RollRule(str(rule.get("kind") or ""), int(rule.get("sessions") or 0)),
                                    contracts=tuple(members))
            reg.series[f"{venue}:{series.root}"] = series
        return reg

    @classmethod
    def load(cls, data_dir: Optional[Path] = None) -> "MarketRegistry":
        base = Path(data_dir) if data_dir else DATA_DIR

        def read(name: str) -> Mapping[str, Any]:
            return json.loads((base / name).read_text(encoding="utf-8"))

        return cls.from_documents(calendars=read("calendars.json"), markets=read("markets.json"),
                                  contracts=read("contracts.json"))

    # ---------------------------------------------------------------------------------------- lookups
    def market(self, market_id: str) -> MarketDefinition:
        try:
            return self.markets[market_id]
        except KeyError:
            raise MarketRegistryError(f"unknown market {market_id!r}") from None

    def calendar(self, calendar_id: str) -> MarketCalendar:
        try:
            return self.calendars[calendar_id]
        except KeyError:
            raise MarketRegistryError(f"unknown calendar {calendar_id!r}") from None

    def calendar_for(self, market_id: str) -> MarketCalendar:
        return self.calendar(self.market(market_id).calendar_id)

    def contract(self, venue: str, symbol: str) -> Optional[ContractSpec]:
        return self.contracts.get(f"{str(venue).lower()}:{symbol}")

    def markets_for_venue(self, venue: str) -> List[MarketDefinition]:
        return [m for m in self.markets.values() if m.venue == str(venue).lower()]

    def crypto_markets(self) -> List[MarketDefinition]:
        return [m for m in self.markets.values() if not m.is_tradfi]

    def tradfi_markets(self) -> List[MarketDefinition]:
        return [m for m in self.markets.values() if m.is_tradfi]

    def summary(self) -> Dict[str, Any]:
        return {"data_versions": dict(self.data_versions),
                "calendars": [c.to_summary() for c in self.calendars.values()],
                "markets": [m.to_plain() for m in self.markets.values()]}


@lru_cache(maxsize=1)
def default_registry() -> MarketRegistry:
    """The definitions shipped in ``app/markets/data`` (read once per process)."""
    return MarketRegistry.load()


__all__ = ["SCHEMA_VERSION", "DATA_DIR", "MarketRegistryError", "Freshness", "MarketDefinition", "MarketRegistry",
           "default_registry"]
