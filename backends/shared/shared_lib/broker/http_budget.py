"""Persistent admission at the physical HTTP boundary, including retries.

No credentials, signed query strings, or response bodies enter budget evidence.
Binance requests are charged from a versioned endpoint-weight catalogue
(``BINANCE_ENDPOINT_WEIGHTS``): the WORST-CASE published IP weight of each path
over every parameter variant and HTTP method. Unknown Binance endpoints fail
closed. This replaced a flat 100 units per request, which let one account's
~8-request cycle plus a client rebuild exhaust the reconciliation share and
starved the market-data collector (2026-11-01, account read READ_FAILED).
"""
import json
import os
import time
from contextvars import ContextVar
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qsl, urlsplit

import requests

from shared_lib.broker import venue_contract as C
from .rate_budget import ENTRY, PROTECTION, RECONCILIATION, REASON_CLOSED_BANNED, SCOPE_UID, RateBudget

priority_context = ContextVar("venue_http_priority", default=RECONCILIATION)
#: Version of the catalogue below; recorded with the policy, bump on any change.
BINANCE_WEIGHT_CATALOGUE_VERSION = "binance-usdm-ip-weights-2026-11-01"
#: Worst-case REQUEST_WEIGHT (per IP and minute) of each supported USD-M path, the
#: maximum over its parameter variants and methods, from Binance's documentation
#: (developers.binance.com/docs/derivatives/usds-margined-futures, read 2026-11-01).
#: Conservative choices, never under the documented figure:
#: * /fapi/v2/account, /fapi/v2/balance, /fapi/v2/positionRisk: 10 (endpoint pages
#:   still say 5; the change log raised them to 10 from 2024-09-03);
#: * order placement (POST /fapi/v1/order, /fapi/v1/algoOrder) costs 0 IP weight
#:   (orders count on the separate per-account ORDERS limit) and is charged 1, the
#:   path's cancel / query cost;
#: * /fapi/v1/fundingRate states no weight (it shares a separate 500 / 5 min pool
#:   with fundingInfo): 10 is assumed, not documented.
#: The demo host publishes 6000 / min; the budget keeps the 2400 production limit.
BINANCE_ENDPOINT_WEIGHTS = {
    "/fapi/v1/algoOrder": 1, "/fapi/v1/allOpenOrders": 1, "/fapi/v1/depth": 20,
    "/fapi/v1/exchangeInfo": 1, "/fapi/v1/fundingInfo": 1, "/fapi/v1/income": 30,
    "/fapi/v1/klines": 10, "/fapi/v1/leverage": 1, "/fapi/v1/openAlgoOrders": 40,
    "/fapi/v1/openOrders": 40, "/fapi/v1/order": 1, "/fapi/v1/premiumIndex": 10,
    "/fapi/v1/ticker/bookTicker": 5, "/fapi/v1/ticker/price": 2, "/fapi/v1/time": 1,
    "/fapi/v1/userTrades": 5, "/fapi/v2/account": 10, "/fapi/v2/balance": 10,
    "/fapi/v2/positionRisk": 10, "/fapi/v1/listenKey": 1,
    "/fapi/v1/commissionRate": 20, "/fapi/v1/positionSide/dual": 30,
    "/fapi/v1/aggTrades": 20, "/fapi/v1/openInterest": 1, "/fapi/v2/ticker/price": 2,
    "/fapi/v1/ping": 1, "/fapi/v1/assetIndex": 10, "/fapi/v1/fundingRate": 10,
}
BINANCE_PATHS = frozenset(BINANCE_ENDPOINT_WEIGHTS)


def request_weight(venue, path):
    """The weight one physical request to ``path`` reserves (Binance: the catalogue; others: 1)."""
    return BINANCE_ENDPOINT_WEIGHTS[path] if venue == C.BINANCE else 1


def _fields(url, kwargs):
    fields = dict(parse_qsl(urlsplit(url).query))
    for key in ("params", "data", "json"):
        value = kwargs.get(key)
        if isinstance(value, (str, bytes)):
            try:
                value = json.loads(value)
            except (ValueError, UnicodeError):
                value = dict(parse_qsl(value.decode() if isinstance(value, bytes) else value))
        if isinstance(value, dict):
            fields.update(value)
    return fields


def _priority(method, path, fields):
    read_priority = priority_context.get()
    if method in ("GET", "HEAD") or path.endswith(("listenKey", "userDataStream")):
        return read_priority
    reducing = method == "DELETE" or "cancel" in path.lower() or any(
        str(fields.get(key, "")).lower() == "true" for key in ("reduceOnly", "closePosition"))
    if reducing:
        return "EMERGENCY" if read_priority == "EMERGENCY" else PROTECTION
    return ENTRY


class BudgetedHTTP:
    def __init__(self, budget, venue, *, account_ref, server_address, transport=None, clock=None):
        self.budget, self.venue = budget, venue
        self.account_ref, self.server_address = account_ref, server_address
        self.transport = transport if transport is not None else requests.Session()
        self.clock = clock or (lambda: int(time.time() * 1000))

    def request(self, method, url, **kwargs):
        method, path, now = method.upper(), urlsplit(url).path, self.clock()
        if self.venue == C.BINANCE and path not in BINANCE_PATHS:
            raise C.AdapterError(C.UNSUPPORTED, "HTTP_ENDPOINT_WEIGHT_NOT_REGISTERED", venue=self.venue)
        priority = _priority(method, path, _fields(url, kwargs))
        grant = self.budget.reserve(self.venue, self.server_address, now, priority=priority,
                                    weight=request_weight(self.venue, path),
                                    category="HTTP:" + method + ":" + path, account_ref=self.account_ref)
        if not grant.granted:
            banned = grant.reason == REASON_CLOSED_BANNED
            raise C.AdapterError(C.BANNED if banned else C.RATE_LIMITED, "LOCAL_HTTP_BUDGET_REFUSED", venue=self.venue,
                                 retry_after_s=grant.retry_after_s(now),
                                 banned_until_ms=grant.retry_at_ms if banned else None)
        # Redirects would send uncharged requests and possibly signed credentials.
        kwargs["allow_redirects"] = False
        response = self.transport.request(method, url, **kwargs)
        now = self.clock()
        if self.venue == C.BYBIT and response.status_code == 403:
            # Bybit requires at least ten minutes without HTTP sessions after an
            # IP-frequency ban. Conservatively apply that wait to any HTTP 403.
            error = C.AdapterError(C.BANNED, "HTTP_BUDGET_BYBIT_IP_REFUSAL", venue=self.venue,
                                   http_status=403, retry_after_s=600, banned_until_ms=now + 600_000)
            self.budget.note_error(error, self.server_address, now, account_ref=self.account_ref)
            raise error
        if response.status_code in (418, 429):
            retry = response.headers.get("Retry-After")
            try:
                retry = max(0.0, float(retry))
            except (TypeError, ValueError):
                try:
                    retry = max(0.0, parsedate_to_datetime(retry).timestamp() - now / 1000)
                except (TypeError, ValueError, OverflowError):
                    retry = None
            error = C.AdapterError(C.BANNED if response.status_code == 418 else C.RATE_LIMITED,
                                   "HTTP_BUDGET_VENUE_REFUSAL", venue=self.venue,
                                   http_status=response.status_code, retry_after_s=retry)
            self.budget.note_error(error, self.server_address, now, account_ref=self.account_ref)
            raise error
        if self.venue == C.BYBIT and response.status_code == 200:
            try:
                body = response.json()
            except (ValueError, AttributeError):
                body = None
            if isinstance(body, dict) and body.get("retCode") in (10006, "10006"):
                try:
                    wait = max(1.0, (float(response.headers["X-Bapi-Limit-Reset-Timestamp"]) - now) / 1000)
                except (KeyError, TypeError, ValueError):
                    wait = 1.0
                error = C.AdapterError(C.RATE_LIMITED, "HTTP_BUDGET_BYBIT_UID_REFUSAL", venue=self.venue, retry_after_s=wait)
                self.budget.note_error(error, self.server_address, now, scope=SCOPE_UID, account_ref=self.account_ref)
                raise error
        if self.venue == C.BINANCE:
            try:
                used = float(response.headers["X-MBX-USED-WEIGHT-1M"])
            except (KeyError, TypeError, ValueError):
                used = None
            if used is not None:
                self.budget.observe(C.RateLimitObservation(self.venue, used, None, 60, None, None, now),
                                    self.server_address)
        return response

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("DELETE", url, **kwargs)

    def put(self, url, **kwargs):
        return self.request("PUT", url, **kwargs)

    def close(self):
        self.transport.close()


def build_budgeted_client(db, auth):
    from shared_lib.broker.client_factory import build_client_from_auth
    http = BudgetedHTTP(RateBudget(db), auth.broker_type.lower(), account_ref=auth.account_id,
                        server_address=os.environ.get("CATI_EGRESS_SCOPE", "shared-runtime-egress"))
    try:
        return build_client_from_auth(auth, http=http)
    except BaseException:
        http.close()
        raise


def client_http(venue, provided, default_factory):
    """Cover direct read-only validation clients as well as resolved factories.

    Unresolved clients share an egress allowance, never an account allowance.
    They still have no raw CREATE authority without a scoped boundary permit.
    """
    if provided is not None:
        return provided
    from shared_lib.core.production import production_enabled
    if not production_enabled():
        return default_factory()
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    db = DB()
    ensure_cati_platform_schema(db)
    return BudgetedHTTP(RateBudget(db), venue, account_ref="UNRESOLVED_VALIDATION_CLIENT",
                        server_address=os.environ.get("CATI_EGRESS_SCOPE", "shared-runtime-egress"))


def public_get(db, url, **kwargs):
    """Unauthenticated runtime market reads share the same egress allowance."""
    from .rate_budget import MARKET_DATA
    http = BudgetedHTTP(RateBudget(db), C.BINANCE, account_ref="PUBLIC_MARKET_DATA",
                        server_address=os.environ.get("CATI_EGRESS_SCOPE", "shared-runtime-egress"))
    token = priority_context.set(MARKET_DATA)
    try:
        return http.get(url, **kwargs)
    finally:
        priority_context.reset(token)
        http.close()
