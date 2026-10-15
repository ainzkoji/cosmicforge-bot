"""Private-stream setup from an already resolved account client.

Only listen-key lifecycle endpoints are sent here; no order endpoint is reachable.
Sources and environment limitations: docs/exchanges/EXCHANGE_OPERATIONS.md.
"""
import hashlib
import hmac
from urllib.parse import urlencode

import requests

from . import contract as C


def supported(venue, environment):
    return venue in (C.BINANCE, C.BYBIT) or (venue == C.BINGX and environment == C.LIVE)


def specification(adapter):
    venue, environment, client = adapter.venue, adapter.environment, adapter._client
    if not supported(venue, environment):
        raise adapter._unsupported("BingX VST private-stream endpoint has not been verified")
    if venue == C.BYBIT:
        expires = adapter._now() + 10_000
        signature = hmac.new(client.api_secret.encode(), f"GET/realtime{expires}".encode(), hashlib.sha256).hexdigest()
        host = "stream-demo.bybit.com" if environment == C.DEMO else "stream.bybit.com"
        return C.UserStreamSpec(venue, f"wss://{host}/v5/private", "AUTH_FRAME", 20, None,
                                ({"op": "auth", "args": [client.api_key, expires, signature]},
                                 {"op": "subscribe", "args": ["order.linear", "execution.linear", "position.linear", "wallet"]}))
    old_key = getattr(adapter, "_listen_key", None)
    if venue == C.BINANCE:
        path, method, header = "/fapi/v1/listenKey", "POST", "X-MBX-APIKEY"
        host = "demo-fstream.binance.com" if environment == C.DEMO else "fstream.binance.com"
    else:
        path, method, header = "/openApi/user/auth/userDataStream", "PUT" if old_key else "POST", "X-BX-APIKEY"
        host = "open-api-swap.bingx.com"
    try:
        request = client.session.request if venue == C.BINANCE else requests.request
        response = request(method, client.base_url + path, headers={header: client.api_key},
                                    params={"listenKey": old_key} if old_key and venue == C.BINGX else {}, timeout=10)
        if response.status_code in (418, 429):
            raise C.AdapterError(C.BANNED if response.status_code == 418 else C.RATE_LIMITED,
                                 "STREAM_KEY_RATE_LIMIT", venue=venue)
        if response.status_code >= 400:
            raise C.AdapterError(C.VENUE_UNAVAILABLE, "STREAM_KEY_HTTP_REFUSED", venue=venue)
        body = response.json()
        if not isinstance(body, dict) or body.get("code", 0) not in (0, "0"):
            raise C.AdapterError(C.PERMISSION_DENIED, "STREAM_KEY_REFUSED", venue=venue)
        data = body.get("data") if isinstance(body.get("data"), dict) else body
        key = data.get("listenKey") or (old_key if method == "PUT" else None)
        if not isinstance(key, str) or not key or len(key) > 4096:
            raise C.AdapterError(C.MALFORMED_RESPONSE, "STREAM_KEY_MISSING", venue=venue)
    except (requests.RequestException, ValueError):
        raise C.AdapterError(C.VENUE_UNAVAILABLE, "STREAM_KEY_TRANSPORT_FAILED", venue=venue) from None
    adapter._listen_key = key
    query = {"listenKey": key}
    if venue == C.BINANCE:
        query["events"] = "ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,listenKeyExpired,ALGO_UPDATE"
    path = "/private/ws" if venue == C.BINANCE else "/swap-market"
    return C.UserStreamSpec(venue, f"wss://{host}{path}?{urlencode(query)}", "LISTEN_KEY_IN_URL", 1800, 3600)
