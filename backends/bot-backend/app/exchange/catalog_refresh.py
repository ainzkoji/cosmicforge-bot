"""Event-driven venue catalog refresh requests (Section 7.11).

Besides age (``activation.account_status.DISCOVERY_MAX_AGE_MS``) two events
say a venue/environment catalog may no longer describe what the venue accepts:

* an INSTRUMENT-RELATED venue API error on an order (unknown / delisted
  symbol, symbol not trading, price/quantity/notional filter violation) --
  classified conservatively from the venue's own error code or message;
  transport, auth, balance and rate-limit errors are NOT instrument errors;
* a CHANGE of the account mode the broker reports for a connected account
  (e.g. Bybit classic <-> UTA), first observations are not changes.

Either records ONE pending request per (venue, environment) -- repeats only
refresh its timestamp, so an error burst is a single request. Requests are
consumed by the existing throttled ``account_status.refresh_if_stale`` (on
market-status reads and in the background ``discovery_refresh_loop``), which
still makes at most one venue call per ``REFRESH_MIN_INTERVAL_MS``. A request
never marks anything tradable and never changes a capability decision: it
only makes a refresh happen sooner. It is cleared by a successful sync.

State is in-process (the bot-backend is the single writer of the catalog);
after a restart the age rule still applies. No error text is stored or
logged -- only a bounded reason class (error strings can embed signed URLs).
"""
from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

TRIGGER_INSTRUMENT_ERROR = "INSTRUMENT_API_ERROR"
TRIGGER_ACCOUNT_MODE_CHANGED = "ACCOUNT_MODE_CHANGED"

#: Binance USD-M error codes that are facts about the instrument / its filters, never about the account
_BINANCE_INSTRUMENT_CODES = {
    "-1121": "SYMBOL_UNKNOWN",            # Invalid symbol
    "-4140": "SYMBOL_NOT_TRADING",        # Invalid symbol status for opening position
    "-1111": "PRECISION_FILTER",          # Precision is over the maximum defined for this asset
    "-4014": "PRICE_FILTER",              # Price not increased by tick size
    "-4023": "QUANTITY_FILTER",           # Quantity not increased by step size
    "-4164": "MIN_NOTIONAL_FILTER",       # Order's notional must be no smaller than ...
    "-4003": "QUANTITY_FILTER",           # Quantity less than or equal to zero / below min
    "-4005": "QUANTITY_FILTER",           # Quantity greater than max quantity
}
#: message patterns (Bybit retMsg / BingX msg), lower-case
_MESSAGE_CLASSES = (
    (re.compile(r"(invalid|unknown|illegal) symbol|symbol (is )?(invalid|not exist|does not exist|not found|"
                r"not supported)|contract (does )?not exist"), "SYMBOL_UNKNOWN"),
    (re.compile(r"symbol (is )?(not (open|trading|online)|offline|suspended|delisted|closed)|"
                r"not open for trading|trading (is )?(suspended|halted|closed)|delist"), "SYMBOL_NOT_TRADING"),
    (re.compile(r"tick ?size|price precision|price (is )?(not )?(a )?multiple"), "PRICE_FILTER"),
    (re.compile(r"qty ?step|lot ?size|quantity precision|qty precision|order quantity (is )?(invalid|too)"),
     "QUANTITY_FILTER"),
    (re.compile(r"min(imum)? (order )?(notional|order value|value)"), "MIN_NOTIONAL_FILTER"),
)
_BINANCE_CODE = re.compile(r'"code"\s*:\s*(-\d+)|code[=: ]+(-\d+)')

_lock = threading.Lock()
_pending: Dict[Tuple[str, str], Dict[str, Any]] = {}
_modes: Dict[str, str] = {}

VENUE_KEY = {"binance": "binance_usdm", "bybit": "bybit_linear", "bingx": "bingx_swap"}
_CLIENT_BROKER = {"BinanceFuturesClient": "binance", "BybitClient": "bybit", "BingXClient": "bingx"}


def _now() -> int:
    return int(time.time() * 1000)


def classify_instrument_error(broker: str, error: Any) -> Optional[str]:
    """A bounded reason class when ``error`` is an instrument-related venue rejection, else None."""
    text = str(error or "")
    if not text:
        return None
    if str(broker or "").lower() == "binance":
        for m in _BINANCE_CODE.finditer(text):
            cls = _BINANCE_INSTRUMENT_CODES.get(m.group(1) or m.group(2))
            if cls:
                return cls
    low = text.lower()
    for pattern, cls in _MESSAGE_CLASSES:
        if pattern.search(low):
            return cls
    return None


def _catalog_environment(environment: Any) -> Optional[str]:
    from shared_lib.broker.environment import normalize_environment

    try:
        return normalize_environment(getattr(environment, "value", environment)).value.upper()
    except Exception:
        return None


def request_refresh(broker: str, environment: Any, trigger: str, *, detail: str = "",
                    now_ms: Optional[int] = None) -> bool:
    """Record a refresh request; True when it is new for this venue/environment (repeats coalesce)."""
    b = str(broker or "").lower()
    venue, env = VENUE_KEY.get(b), _catalog_environment(environment)
    if venue is None or env is None:
        return False
    now = int(now_ms if now_ms is not None else _now())
    with _lock:
        new = (venue, env) not in _pending
        _pending[(venue, env)] = {"trigger": trigger, "detail": detail, "requested_ms": now,
                                  "first_requested_ms": _pending.get((venue, env), {}).get("first_requested_ms", now)}
    if new:
        logger.info("[CATALOG_REFRESH] requested venue=%s env=%s trigger=%s detail=%s", venue, env, trigger, detail)
        from app.ops import multi_asset_metrics as mm

        mm.catalog_refresh_requested(venue, trigger)
    return new


def pending(venue: str, environment: str) -> Optional[Dict[str, Any]]:
    with _lock:
        p = _pending.get((venue, str(environment).upper()))
        return dict(p) if p else None


def clear(venue: str, environment: str, *, synced_ms: int) -> None:
    """A successful sync satisfies every request made up to ``synced_ms``."""
    with _lock:
        p = _pending.get((venue, str(environment).upper()))
        if p is not None and p["requested_ms"] <= synced_ms:
            _pending.pop((venue, str(environment).upper()), None)


def pending_all() -> Dict[Tuple[str, str], Dict[str, Any]]:
    with _lock:
        return {k: dict(v) for k, v in _pending.items()}


def observe_instrument_error(broker: str, environment: Any, error: Any, *, now_ms: Optional[int] = None
                             ) -> Optional[str]:
    """Called on a venue order rejection. Returns the reason class when a refresh was requested."""
    try:
        cls = classify_instrument_error(broker, error)
        if cls is None:
            return None
        request_refresh(broker, environment, TRIGGER_INSTRUMENT_ERROR, detail=cls, now_ms=now_ms)
        return cls
    except Exception:  # observation must never affect the order path
        return None


def client_venue_environment(client: Any) -> Tuple[Optional[str], Optional[str]]:
    """(broker, environment) of an exchange client from its class + base URL; (None, None) if not certain."""
    from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url

    broker = _CLIENT_BROKER.get(type(client).__name__)
    base = str(getattr(client, "base_url", "") or "").rstrip("/")
    if broker is None or not base:
        return None, None
    for env in BrokerEnvironment:
        try:
            if resolve_base_url(broker, env).rstrip("/") == base:
                return broker, env.value.upper()
        except Exception:
            continue
    return broker, None  # an unregistered host: never guess the environment


def observe_client_error(client: Any, error: Any) -> Optional[str]:
    broker, env = client_venue_environment(client)
    if broker is None or env is None:
        return None
    return observe_instrument_error(broker, env, error)


def observe_account_mode(broker_account_id: str, broker: str, environment: Any, mode: Optional[str], *,
                         now_ms: Optional[int] = None) -> bool:
    """Record the account mode the broker reported; True when it CHANGED (then a refresh is requested).
    An unreadable mode (None) is not a change and does not overwrite the last known one."""
    try:
        if not broker_account_id or not mode:
            return False
        m = str(mode).upper()
        with _lock:
            prev = _modes.get(broker_account_id)
            _modes[broker_account_id] = m
        if prev is None or prev == m:
            return False
        logger.info("[CATALOG_REFRESH] account_mode_changed account=%s broker=%s %s -> %s", broker_account_id,
                    broker, prev, m)
        request_refresh(broker, environment, TRIGGER_ACCOUNT_MODE_CHANGED, detail=f"{prev}->{m}", now_ms=now_ms)
        return True
    except Exception:
        return False


def _reset_for_tests() -> None:
    with _lock:
        _pending.clear()
        _modes.clear()


__all__ = ["TRIGGER_ACCOUNT_MODE_CHANGED", "TRIGGER_INSTRUMENT_ERROR", "classify_instrument_error", "clear",
           "client_venue_environment", "observe_account_mode", "observe_client_error", "observe_instrument_error",
           "pending", "pending_all", "request_refresh"]
