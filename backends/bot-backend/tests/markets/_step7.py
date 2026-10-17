"""Explicit fixture calendars and contracts of the Step 7 tests.

Every value here is synthetic and exists only to exercise the engines; none describes a real venue. The
production data files in ``app/markets/data`` are never edited by a test.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from app.markets.calendar import MarketCalendar
from app.markets.registry import MarketRegistry

COVERAGE = {"from": "2026-01-01", "to": "2027-12-31"}

#: overnight futures-shaped week in Chicago time, with a holiday, an early close and a maintenance break
FUTURES_FIXTURE: Dict[str, Any] = {
    "calendar_id": "FIXTURE_OVERNIGHT", "version": "t1", "kind": "SESSIONS", "timezone": "America/Chicago",
    "verification_status": "VERIFIED", "source": "test fixture",
    "weekly_sessions": [
        {"open_weekday": 6, "open": "17:00", "close_weekday": 0, "close": "16:00"},
        {"open_weekday": 0, "open": "17:00", "close_weekday": 1, "close": "16:00"},
        {"open_weekday": 1, "open": "17:00", "close_weekday": 2, "close": "16:00"},
        {"open_weekday": 2, "open": "17:00", "close_weekday": 3, "close": "16:00"},
        {"open_weekday": 3, "open": "17:00", "close_weekday": 4, "close": "16:00"},
    ],
    "holidays": ["2026-12-25"],
    "early_closes": {"2026-11-27": "12:15"},
    "maintenance_windows": [{"weekdays": [1], "start": "03:00", "end": "03:15"}],
    "coverage": COVERAGE,
}

CASH_FIXTURE: Dict[str, Any] = {
    "calendar_id": "FIXTURE_CASH", "version": "t1", "kind": "SESSIONS", "timezone": "America/New_York",
    "verification_status": "VERIFIED", "source": "test fixture",
    "weekly_sessions": [{"open_weekday": d, "open": "09:30", "close_weekday": d, "close": "16:00"} for d in range(5)],
    "holidays": ["2026-07-03"], "early_closes": {}, "maintenance_windows": [], "coverage": COVERAGE,
}

CRYPTO_FIXTURE: Dict[str, Any] = {
    "calendar_id": "CRYPTO_24_7", "version": "t1", "kind": "CONTINUOUS", "timezone": "UTC",
    "verification_status": "PLATFORM_CONVENTION", "source": "test fixture",
}

UNVERIFIED_FIXTURE: Dict[str, Any] = {**FUTURES_FIXTURE, "calendar_id": "FIXTURE_UNVERIFIED",
                                      "verification_status": "UNVERIFIED"}


def calendar(doc: Dict[str, Any], **over: Any) -> MarketCalendar:
    return MarketCalendar.from_plain({**doc, **over})


def _market(market_id: str, asset_class: str, venue: str, calendar_id: str, status: str = "VERIFIED",
            **over: Any) -> Dict[str, Any]:
    base = {"market_id": market_id, "asset_class": asset_class, "market_type": f"{asset_class}_FIXTURE",
            "venue": venue, "product": "FIXTURE", "calendar_id": calendar_id, "settlement_currency": "USD",
            "contract_source": "DATA_FILE",
            "freshness": {"timeframe": "1d", "max_missing_bars": 1, "source": "test fixture"},
            "verification_status": status, "source": "test fixture"}
    base.update(over)
    return base


def contract_doc(symbol: str, last_trade_ms: Optional[int], *, listing_ms: Optional[int] = None,
                 venue: str = "fixture_futures", **over: Any) -> Dict[str, Any]:
    base = {"symbol": symbol, "series": "FX1", "venue": venue, "asset_class": "FUTURES", "contract_type": "FUTURE",
            "settlement_currency": "USD", "multiplier": "50", "tick_size": "0.25", "quantity_step": "1",
            "min_quantity": "1", "margin": {"kind": "FIXED_PER_CONTRACT", "initial": "12000", "maintenance": "11000"},
            "calendar_id": "FIXTURE_OVERNIGHT", "listing_ms": listing_ms, "last_trade_ms": last_trade_ms,
            "carry": {"kind": "FUTURES_BASIS"}, "verification_status": "VERIFIED", "source": "test fixture"}
    base.update(over)
    return base


def registry(*, contracts: Optional[List[Dict[str, Any]]] = None, series: Optional[List[Dict[str, Any]]] = None,
             extra_markets: Optional[List[Dict[str, Any]]] = None) -> MarketRegistry:
    calendars = {"schema_version": "1", "data_version": "fixture",
                 "calendars": [CRYPTO_FIXTURE, FUTURES_FIXTURE, CASH_FIXTURE, UNVERIFIED_FIXTURE]}
    markets = {"schema_version": "1", "data_version": "fixture", "markets": [
        _market("CRYPTO_BINANCE_USDT_PERPETUAL", "CRYPTO", "binance", "CRYPTO_24_7", "PLATFORM_CONVENTION",
                contract_source="VENUE_RULES_CACHE"),
        _market("FIX_FUTURES", "FUTURES", "fixture_futures", "FIXTURE_OVERNIGHT"),
        _market("FIX_CASH", "EQUITY", "fixture_equity", "FIXTURE_CASH",
                freshness={"timeframe": "1h", "max_missing_bars": 2, "source": "test fixture"}),
        _market("FIX_UNVERIFIED", "FUTURES", "fixture_futures", "FIXTURE_UNVERIFIED", "UNVERIFIED"),
        *(extra_markets or [])]}
    docs = {"schema_version": "1", "data_version": "fixture", "contracts": contracts or [], "series": series or []}
    return MarketRegistry.from_documents(calendars=calendars, markets=markets, contracts=docs)


def fresh_db(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(str(tmp_path / f"step7_{uuid.uuid4().hex[:8]}.db"))
    migrate(db)
    return db
