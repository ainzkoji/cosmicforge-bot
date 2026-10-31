"""Private-stream setup from an already resolved account client.

Only listen-key lifecycle endpoints are sent here; no order endpoint is reachable.
Sources and environment limitations: docs/exchanges/EXCHANGE_OPERATIONS.md.
"""
import hashlib
import hmac
from urllib.parse import quote, urlencode

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
        request = client.session.request if venue == C.BINANCE else getattr(client, "http", requests).request
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
    if venue == C.BINANCE:
        # doc (user-data-streams, read 2026-10-31): base wss://fstream.binance.com/private, user data at
        # /ws/<listenKey>, every event type. The previous URL added an ``events`` filter joined with commas; the
        # documentation joins events with "/", so the filter was not one it describes. The demo host's /private
        # route is not documented: USER_STREAM stays a certification item there.
        return C.UserStreamSpec(venue, f"wss://{host}/private/ws/{quote(key, safe='')}", "LISTEN_KEY_IN_URL", 1800, 3600)
    return C.UserStreamSpec(venue, f"wss://{host}/swap-market?{urlencode({'listenKey': key})}", "LISTEN_KEY_IN_URL",
                            1800, 3600)
