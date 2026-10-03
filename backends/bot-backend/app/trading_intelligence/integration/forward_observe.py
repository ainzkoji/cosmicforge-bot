"""Public reference-venue observations; no credentials, orders or historical backfill."""
import logging
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from app.market_data.store import MarketDataStore, SeriesId

logger = logging.getLogger(__name__)
VERSION = "CATI_FORWARD_PUBLIC_MICROSTRUCTURE_V1"
SYMBOLS = ("BTCUSDT", "ETHUSDT")
_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cati-public-observe")
_lock = threading.Lock()
_future = None
_last = 0.


def book_walk(book, notional=1000.):
    def price(levels):
        quote, units = 0., 0.
        for price, size in levels:
            price, size = float(price), float(size)
            if not math.isfinite(price) or not math.isfinite(size) or price <= 0 or size < 0:
                raise ValueError("invalid book level")
            used = min(notional-quote, price*size)
            quote += used
            units += used/price
            if quote >= notional:
                return quote/units
        return None
    buy, sell = price(book["asks"]), price(book["bids"])
    mid = (float(book["asks"][0][0])+float(book["bids"][0][0]))/2
    return None if buy is None or sell is None else 10000*(buy-sell)/mid


def collect(db, *, fetch=None, symbols=SYMBOLS):
    store = MarketDataStore(db)
    if fetch is None:
        def fetch(path, params):
            response = requests.get("https://fapi.binance.com"+path, params=params, timeout=(3, 6))
            response.raise_for_status()
            return response.json()
    written = 0
    for symbol in symbols:
        sid = SeriesId(venue="BINANCE_USDM", venue_symbol=symbol, canonical_symbol=symbol[:-4]+"/USDT:PERP",
            asset_class="CRYPTO", product_type="PERPETUAL", source="binance_public_forward_reference",
            source_version=VERSION, environment="REAL")
        endpoints = [
            ("/fapi/v1/depth", {"symbol": symbol, "limit": 20}, ("bid", "ask", "spread_bps", "book_bid_depth", "book_ask_depth", "book_imbalance", "execution_cost_proxy_bps")),
            ("/fapi/v1/premiumIndex", {"symbol": symbol}, ("mark_price", "index_price", "basis_bps", "funding_rate", "next_funding_time")),
            ("/fapi/v1/openInterest", {"symbol": symbol}, ("open_interest",)),
            ("/fapi/v1/aggTrades", {"symbol": symbol, "limit": 1000}, ("aggressor_buy_quote_fraction", "aggregate_trade_quote_volume")),
        ]
        for path, params, features in endpoints:
            try:
                payload = fetch(path, params)
                received_at = int(time.time()*1000)
                provenance = {"availability": "FORWARD_OBSERVE_ONLY", "reference_market_only": True,
                              "endpoint": path, "received_at": received_at, "environment": "REAL",
                              "not_executed": True}
                values = {}
                if "depth" in path:
                    bids, asks = payload["bids"], payload["asks"]
                    if any(not math.isfinite(float(p)) or not math.isfinite(float(q)) or float(p)<=0 or float(q)<0 for p,q in bids+asks):
                        raise ValueError("invalid book level")
                    bid, ask = float(bids[0][0]), float(asks[0][0])
                    if not 0 < bid <= ask:
                        raise ValueError("invalid/crossed book")
                    bd = sum(float(p)*float(q) for p, q in bids)
                    ad = sum(float(p)*float(q) for p, q in asks)
                    values = dict(bid=bid, ask=ask, spread_bps=10000*(ask-bid)/((ask+bid)/2),
                                  book_bid_depth=bd, book_ask_depth=ad, book_imbalance=(bd-ad)/(bd+ad),
                                  execution_cost_proxy_bps=book_walk(payload))
                    provenance.update(book=payload, depth_units="quote USDT, top20 levels",
                                      proxy_notional_USDT=1000, proxy_excludes_fees_funding_and_latency=True)
                elif "premiumIndex" in path:
                    mark, index = float(payload["markPrice"]), float(payload["indexPrice"])
                    if mark <= 0 or index <= 0:
                        raise ValueError("invalid mark/index")
                    values = dict(mark_price=mark, index_price=index, basis_bps=10000*(mark/index-1),
                                  funding_rate=float(payload["lastFundingRate"]), next_funding_time=float(payload["nextFundingTime"]))
                    provenance["source_payload"] = payload
                elif "openInterest" in path:
                    values = dict(open_interest=float(payload["openInterest"]))
                    provenance["source_payload"] = payload
                else:
                    if not payload:
                        raise ValueError("no recent trades returned")
                    if any(int(t["T"]) > received_at for t in payload):
                        raise ValueError("future trade timestamp")
                    if any(not isinstance(t["m"], bool) or not math.isfinite(float(t["p"])) or not math.isfinite(float(t["q"])) or float(t["p"])<=0 or float(t["q"])<0 for t in payload):
                        raise ValueError("invalid aggregate trade")
                    quote = sum(float(t["p"])*float(t["q"]) for t in payload)
                    buys = sum(float(t["p"])*float(t["q"]) for t in payload if not t["m"])
                    if quote <= 0:
                        raise ValueError("zero observed quote volume")
                    values = dict(aggressor_buy_quote_fraction=buys/quote, aggregate_trade_quote_volume=quote)
                    provenance.update(trades=payload, first_trade_at=min(t["T"] for t in payload),
                                      last_trade_at=max(t["T"] for t in payload), bounded_sample=True,
                                      complete_calendar_window_claimed=False)
                if any(not math.isfinite(v) for v in values.values() if v is not None):
                    raise ValueError("nonfinite observation")
                if values.get("open_interest", 0) < 0:
                    raise ValueError("negative open interest")
                for feature in features:
                    value = values.get(feature)
                    if value is None:
                        store.record_feature(sid, feature, received_at, unavailable_reason="INSUFFICIENT_BOOK_DEPTH")
                    else:
                        store.record_feature(sid, feature, received_at, value=value, value_json=provenance)
                    written += 1
            except Exception as exc:
                reason = "PUBLIC_OBSERVATION_FAILED:"+type(exc).__name__
                for feature in features:
                    store.record_feature(sid, feature, int(time.time()*1000), unavailable_reason=reason)
                    written += 1
        for feature in ("liquidations_long", "liquidations_short"):
            store.record_feature(sid, feature, int(time.time()*1000), unavailable_reason="NO_CAUSAL_LIQUIDATION_STREAM_CONFIGURED")
            written += 1
    logger.info("[CATI_FORWARD_OBSERVE] symbols=%s records=%s source=%s no_orders=True", ",".join(symbols), written, VERSION)
    return written


def schedule(runner):
    """One bounded asynchronous collector per canonical runtime, every five minutes."""
    global _last, _future
    if os.environ.get("COSMICFORGE_TEST_MODE") == "1":
        return
    with _lock:
        if time.monotonic()-_last < 300 or (_future is not None and not _future.done()):
            return
        from app.ops.runtime_ownership import current_owner, lease_is_stale
        owner = current_owner(runner.db, runner.db.path)
        if not owner or owner["pid"] != os.getpid() or lease_is_stale(owner["heartbeat_at"]):
            return
        _last = time.monotonic()
        _future = _pool.submit(collect, runner.db)
        _future.add_done_callback(_completed)


def _completed(future):
    try:
        future.result()
    except Exception:
        logger.exception("[CATI_FORWARD_OBSERVE] persistence failed; runtime analysis continues")
