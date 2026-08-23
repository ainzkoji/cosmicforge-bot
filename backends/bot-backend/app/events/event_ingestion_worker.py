"""
EventIngestionWorker — background task that fetches economic and crypto events
from external sources and stores them in economic_events.

Sources supported:
  1. Macro JSON feed   — Any URL returning a JSON array of event objects.
                         Supports ForexFactory-compatible format and generic mappings.
                         Configured via EVENT_MACRO_JSON_URL.
  2. CoinMarketCal API — Crypto-specific events (token unlocks, upgrades, listings).
                         Requires COINMARKETCAL_API_KEY + COINMARKETCAL_ENABLED=True.

Both sources are DISABLED by default. To enable:
  EVENT_INGESTION_ENABLED=True
  EVENT_MACRO_JSON_URL=https://your-calendar-feed.com/events.json
  COINMARKETCAL_API_KEY=<your-key>
  COINMARKETCAL_ENABLED=True

After each successful fetch the CalendarSyncWorker will pick up the new events
on its next tick and generate the corresponding event_blackout_windows rows.

Design invariants:
  - Never modifies positions or trading state.
  - All DB writes go through insert_event() which uses INSERT OR REPLACE.
  - Errors are logged and retried on next interval — never crash the worker.
  - Timestamps are always normalised to UTC ISO-8601 before storage.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import insert_event

logger = logging.getLogger(__name__)

# How many days ahead to fetch events (keeps the DB lean)
_LOOKAHEAD_DAYS = 14
# Max items to ingest per fetch call per source
_MAX_ITEMS = 200


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_forexfactory_time(date_str: str, time_str: str) -> Optional[str]:
    """
    Convert ForexFactory date/time strings to UTC ISO-8601.

    Inputs:  date_str = "Apr 10, 2026"  time_str = "8:30am"
    Output:  "2026-04-10T08:30:00+00:00"

    Times are assumed EST (UTC-5) which is the typical FX calendar convention.
    We convert to UTC by adding 5 hours (no DST adjustment for simplicity).
    """
    try:
        date_str = date_str.strip()
        time_str = time_str.strip().lower().replace(" ", "")

        # Parse date
        parts = re.split(r"[\s,]+", date_str)
        if len(parts) < 3:
            return None
        month_name = parts[0].lower()[:3]
        month = _MONTH_MAP.get(month_name)
        if not month:
            return None
        day = int(parts[1])
        year = int(parts[2])

        # Parse time — accept "8:30am", "2:00pm", "Tentative", "All Day"
        if time_str in ("tentative", "allday", "all day", ""):
            hour, minute = 12, 0  # noon UTC placeholder
        else:
            match = re.match(r"(\d{1,2}):(\d{2})(am|pm)", time_str)
            if not match:
                return None
            hour = int(match.group(1))
            minute = int(match.group(2))
            meridiem = match.group(3)
            if meridiem == "pm" and hour != 12:
                hour += 12
            elif meridiem == "am" and hour == 12:
                hour = 0
            # Treat as EST (UTC-5) → add 5h for UTC
            hour = (hour + 5) % 24

        dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def _parse_iso_utc(raw: str) -> Optional[str]:
    """Parse ISO-8601 or ISO-like strings to UTC ISO."""
    if not raw:
        return None
    try:
        raw = raw.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def _normalize_impact(raw: str) -> Optional[str]:
    """Map impact strings to HIGH / MEDIUM / LOW."""
    mapping = {
        "high": "HIGH", "red": "HIGH", "3": "HIGH",
        "medium": "MEDIUM", "med": "MEDIUM", "orange": "MEDIUM", "2": "MEDIUM",
        "low": "LOW", "yellow": "LOW", "gray": "LOW", "grey": "LOW", "1": "LOW",
        "4": "HIGH", "5": "HIGH",
    }
    return mapping.get(raw.strip().lower())


# ---------------------------------------------------------------------------
# Macro JSON feed parser
# ---------------------------------------------------------------------------

_MACRO_TITLE_KEYS  = ("title", "name", "event", "description")
_MACRO_CURRENCY_KEYS = ("country", "currency", "country_currency", "nation")
_MACRO_DATE_KEYS   = ("date", "datetime", "scheduled_utc", "event_date", "time")
_MACRO_TIME_KEYS   = ("time",)
_MACRO_IMPACT_KEYS = ("impact", "importance", "priority", "risk_level")


def _first(d: dict, keys) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None:
            return str(v).strip()
    return ""


def _parse_macro_item(item: dict) -> Optional[Dict]:
    """
    Parse a single macro event item from a flexible JSON structure.

    Returns a dict ready for insert_event(), or None if essential fields missing.
    """
    title = _first(item, _MACRO_TITLE_KEYS)
    if not title:
        return None

    currency = _first(item, _MACRO_CURRENCY_KEYS).upper() or "USD"
    impact_raw = _first(item, _MACRO_IMPACT_KEYS)
    impact = _normalize_impact(impact_raw) if impact_raw else "MEDIUM"
    if not impact:
        impact = "LOW"

    # Determine event type from title keywords
    title_lower = title.lower()
    if "fomc" in title_lower or "fed rate" in title_lower or "federal open" in title_lower:
        event_type = "FOMC"
    elif "cpi" in title_lower:
        event_type = "CPI"
    elif "nfp" in title_lower or "non-farm" in title_lower or "payroll" in title_lower:
        event_type = "NFP"
    elif "gdp" in title_lower:
        event_type = "GDP"
    elif "pmi" in title_lower:
        event_type = "PMI"
    elif "rate decision" in title_lower or "interest rate" in title_lower:
        event_type = "RATE_DECISION"
    elif "pce" in title_lower:
        event_type = "PCE"
    elif "retail sales" in title_lower:
        event_type = "RETAIL_SALES"
    elif "unlock" in title_lower:
        event_type = "TOKEN_UNLOCK"
    elif "upgrade" in title_lower or "hardfork" in title_lower or "hard fork" in title_lower:
        event_type = "UPGRADE"
    elif "listing" in title_lower:
        event_type = "LISTING"
    else:
        event_type = "ECONOMIC"

    # Try to parse scheduled_utc
    date_val = _first(item, _MACRO_DATE_KEYS)
    time_val = item.get("time", "")

    scheduled_utc = _parse_iso_utc(date_val)
    if scheduled_utc is None and date_val and time_val:
        scheduled_utc = _parse_forexfactory_time(date_val, str(time_val))
    if scheduled_utc is None and date_val:
        # Try treating date_val alone as ISO
        scheduled_utc = _parse_iso_utc(date_val + "T12:00:00+00:00")

    if not scheduled_utc:
        return None

    # Numeric forecast/previous fields
    def _safe_float(k: str) -> Optional[float]:
        v = item.get(k)
        if v is None:
            return None
        try:
            cleaned = re.sub(r"[^0-9.\-]", "", str(v))
            return float(cleaned) if cleaned else None
        except Exception:
            return None

    return {
        "title": title[:200],
        "event_type": event_type,
        "country_currency": currency[:10],
        "impact_level": impact,
        "scheduled_utc": scheduled_utc,
        "forecast_val": _safe_float("forecast"),
        "previous_val": _safe_float("previous"),
    }


# ---------------------------------------------------------------------------
# CoinMarketCal fetcher
# ---------------------------------------------------------------------------

_CMC_API_URL = "https://developers.coinmarketcal.com/v1/events"
_CMC_COIN_SYMBOLS_TO_WATCH = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "AVAX", "MATIC",
    "DOT", "LINK", "UNI", "DOGE", "LTC", "ATOM", "TRX",
}


def _importance_to_impact(importance: float) -> str:
    if importance >= 4:
        return "HIGH"
    if importance >= 2.5:
        return "MEDIUM"
    return "LOW"


def _fetch_coinmarketcal(
    api_key: str,
    min_importance: int,
    timeout: int = 10,
) -> List[Dict]:
    """
    Fetch upcoming crypto events from CoinMarketCal API.

    API docs: https://developers.coinmarketcal.com
    Free tier: 100 requests/day, up to 150 events per request.
    """
    now = datetime.now(timezone.utc)
    date_from = now.strftime("%Y-%m-%d")
    date_to = (now + timedelta(days=_LOOKAHEAD_DAYS)).strftime("%Y-%m-%d")

    url = (
        f"{_CMC_API_URL}"
        f"?dateRangeStart={date_from}"
        f"&dateRangeEnd={date_to}"
        f"&page=1&max=150"
    )

    req = Request(
        url,
        headers={
            "x-api-key": api_key,
            "Accept": "application/json",
            "User-Agent": "CosmicForge-EventBot/1.0",
        },
    )

    with urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode())

    results = []
    body = data.get("body", data) if isinstance(data, dict) else data
    if not isinstance(body, list):
        return results

    for ev in body[:_MAX_ITEMS]:
        # Filter by importance
        importance = float(ev.get("importance", 0) or 0)
        if importance < min_importance:
            continue

        # Filter by coin relevance
        coins = ev.get("coins", [])
        symbols = {c.get("symbol", "").upper() for c in coins if isinstance(c, dict)}
        if symbols and not symbols.intersection(_CMC_COIN_SYMBOLS_TO_WATCH):
            continue

        # Parse title
        title_obj = ev.get("title", {})
        title = (
            title_obj.get("en") if isinstance(title_obj, dict) else str(title_obj)
        ) or ev.get("description", {}).get("en") or ""
        title = title.strip()[:200]
        if not title:
            continue

        # Parse date
        scheduled_raw = ev.get("date_event") or ev.get("created_date", "")
        scheduled_utc = _parse_iso_utc(scheduled_raw)
        if not scheduled_utc:
            continue

        # Currency: use first matched coin symbol or "CRYPTO"
        currency = next(iter(symbols), "CRYPTO") if symbols else "CRYPTO"

        # Event type
        title_lower = title.lower()
        if "unlock" in title_lower:
            event_type = "TOKEN_UNLOCK"
        elif "upgrade" in title_lower or "hardfork" in title_lower:
            event_type = "UPGRADE"
        elif "listing" in title_lower:
            event_type = "LISTING"
        elif "burn" in title_lower:
            event_type = "TOKEN_BURN"
        elif "launch" in title_lower or "mainnet" in title_lower:
            event_type = "LAUNCH"
        elif "airdrop" in title_lower:
            event_type = "AIRDROP"
        else:
            event_type = "CRYPTO_EVENT"

        results.append({
            "title": title,
            "event_type": event_type,
            "country_currency": currency,
            "impact_level": _importance_to_impact(importance),
            "scheduled_utc": scheduled_utc,
            "forecast_val": None,
            "previous_val": None,
        })

    return results


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class EventIngestionWorker:
    """
    Background async worker that fetches economic + crypto events from
    external sources and stores them in economic_events.

    The CalendarSyncWorker generates blackout windows from these events
    on its next hourly tick automatically.
    """

    def __init__(
        self,
        db: DB,
        *,
        enabled: bool = False,
        interval_hours: int = 6,
        macro_json_url: str = "",
        coinmarketcal_api_key: str = "",
        coinmarketcal_enabled: bool = False,
        coinmarketcal_min_importance: int = 3,
        http_timeout: int = 15,
        on_after_ingest: Optional[Callable[[], None]] = None,
    ) -> None:
        self._db = db
        self._enabled = enabled
        self._interval = interval_hours * 3600
        self._macro_url = macro_json_url.strip()
        self._cmc_key = coinmarketcal_api_key.strip()
        self._cmc_enabled = coinmarketcal_enabled and bool(self._cmc_key)
        self._cmc_min_importance = coinmarketcal_min_importance
        self._timeout = http_timeout
        self._after_ingest = on_after_ingest
        self._running = False
        self._backoff_until: float = 0.0

    async def start(self) -> None:
        if not self._enabled:
            logger.info("[EventIngestion] disabled — not starting")
            return
        if not self._macro_url and not self._cmc_enabled:
            logger.info("[EventIngestion] no sources configured — not starting")
            return
        self._running = True
        logger.info(
            "[EventIngestion] starting — macro_url=%s cmc=%s interval=%dh",
            bool(self._macro_url), self._cmc_enabled, self._interval // 3600,
        )
        while self._running:
            try:
                await asyncio.to_thread(self._fetch_once)
            except Exception as exc:
                logger.exception("[EventIngestion] unhandled error: %s", exc)
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        self._running = False

    def _fetch_once(self) -> None:
        now = time.monotonic()
        if now < self._backoff_until:
            return

        total = 0

        if self._macro_url:
            try:
                count = self._fetch_macro_json()
                total += count
                logger.info("[EventIngestion] macro feed: %d events ingested", count)
            except Exception as exc:
                logger.warning("[EventIngestion] macro feed error: %s", exc)
                self._backoff_until = now + 1800  # 30-min backoff on error

        if self._cmc_enabled:
            try:
                count = self._fetch_cmc()
                total += count
                logger.info("[EventIngestion] CoinMarketCal: %d events ingested", count)
            except HTTPError as exc:
                logger.warning("[EventIngestion] CoinMarketCal HTTP %d: %s", exc.code, exc.reason)
                self._backoff_until = now + (3600 if exc.code == 429 else 1800)
            except Exception as exc:
                logger.warning("[EventIngestion] CoinMarketCal error: %s", exc)
                self._backoff_until = now + 1800

        if total:
            logger.info("[EventIngestion] total %d events upserted this cycle", total)
            if self._after_ingest is not None:
                try:
                    self._after_ingest()
                except Exception as exc:
                    logger.warning("[EventIngestion] post-ingest hook failed: %s", exc)

    def _fetch_macro_json(self) -> int:
        """Fetch and ingest macro events from the configured JSON URL."""
        url = self._build_macro_url()
        req = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "CosmicForge-EventBot/1.0",
            },
        )
        with urlopen(req, timeout=self._timeout) as resp:
            raw = resp.read().decode()
        data = json.loads(raw)

        # Accept array or {"events": [...], "data": [...], etc.}
        items: list = []
        if isinstance(data, list):
            items = data
        else:
            for key in ("events", "data", "results", "items", "calendar"):
                if isinstance(data.get(key), list):
                    items = data[key]
                    break

        if not items:
            logger.debug("[EventIngestion] macro feed: no items found in response")
            return 0

        count = 0
        for raw_item in items[:_MAX_ITEMS]:
            if not isinstance(raw_item, dict):
                continue
            parsed = _parse_macro_item(raw_item)
            if parsed is None:
                continue
            # Only ingest events within the lookahead window
            try:
                scheduled_dt = datetime.fromisoformat(parsed["scheduled_utc"])
                now_dt = datetime.now(timezone.utc)
                if scheduled_dt < now_dt - timedelta(hours=1):
                    continue
                if scheduled_dt > now_dt + timedelta(days=_LOOKAHEAD_DAYS):
                    continue
            except Exception:
                continue

            try:
                insert_event(
                    self._db,
                    title=parsed["title"],
                    event_type=parsed["event_type"],
                    country_currency=parsed["country_currency"],
                    impact_level=parsed["impact_level"],
                    scheduled_utc=parsed["scheduled_utc"],
                    forecast_val=parsed.get("forecast_val"),
                    previous_val=parsed.get("previous_val"),
                    source="macro_json",
                )
                count += 1
            except Exception as exc:
                logger.debug("[EventIngestion] insert error for %s: %s", parsed["title"], exc)

        return count

    def _build_macro_url(self) -> str:
        """Return the macro JSON URL (may add date params if the URL supports it)."""
        url = self._macro_url
        now = datetime.now(timezone.utc)
        future = now + timedelta(days=_LOOKAHEAD_DAYS)
        # Inject date params only if URL supports query params (no existing params)
        if "?" not in url:
            url += (
                f"?from={now.strftime('%Y-%m-%d')}"
                f"&to={future.strftime('%Y-%m-%d')}"
            )
        return url

    def _fetch_cmc(self) -> int:
        """Fetch and ingest crypto events from CoinMarketCal API."""
        items = _fetch_coinmarketcal(
            api_key=self._cmc_key,
            min_importance=self._cmc_min_importance,
            timeout=self._timeout,
        )
        count = 0
        for item in items:
            try:
                insert_event(
                    self._db,
                    title=item["title"],
                    event_type=item["event_type"],
                    country_currency=item["country_currency"],
                    impact_level=item["impact_level"],
                    scheduled_utc=item["scheduled_utc"],
                    source="coinmarketcal",
                )
                count += 1
            except Exception as exc:
                logger.debug("[EventIngestion] CMC insert error for %s: %s", item["title"], exc)
        return count


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_event_ingestion_worker(
    db: DB,
    *,
    on_after_ingest: Optional[Callable[[], None]] = None,
) -> EventIngestionWorker:
    """Factory that reads config and constructs the worker."""
    from app.core.config import settings

    return EventIngestionWorker(
        db=db,
        enabled=getattr(settings, "EVENT_INGESTION_ENABLED", False),
        interval_hours=getattr(settings, "EVENT_INGESTION_INTERVAL_HOURS", 6),
        macro_json_url=getattr(settings, "EVENT_MACRO_JSON_URL", ""),
        coinmarketcal_api_key=getattr(settings, "COINMARKETCAL_API_KEY", ""),
        coinmarketcal_enabled=getattr(settings, "COINMARKETCAL_ENABLED", False),
        coinmarketcal_min_importance=getattr(settings, "COINMARKETCAL_MIN_IMPORTANCE", 3),
        on_after_ingest=on_after_ingest,
    )
