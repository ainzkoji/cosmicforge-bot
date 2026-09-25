"""Public historical market data per venue (Phase 4B/4C/4D).

Every fetcher takes an injectable ``get(url, params) -> json`` so tests run on
recorded payload shapes and production uses ``requests``. Output rows use the
repo's canonical kline layout ``[open_time, o, h, l, c, v, close_time,
quote_volume, trades]`` (ascending, no gap filling) and feature rows
``(feature, observed_at, value)``.

Coverage honestly reported: an endpoint a venue does not offer (or offers
only for a short look-back, e.g. Binance open-interest history ~30 days)
returns ``FeatureUnavailable`` with the reason instead of fabricated zeros.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

Getter = Callable[[str, Dict[str, Any]], Any]
MINUTE_MS = 60_000
_TF_MS = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
_BYBIT_TF = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "1h": "60", "4h": "240", "1d": "D"}

BINANCE_FAPI = "https://fapi.binance.com"
BYBIT_API = "https://api.bybit.com"
BINGX_API = "https://open-api.bingx.com"


@dataclass(frozen=True)
class FeatureUnavailable:
    feature: str
    reason: str


def requests_getter(timeout: float = 30.0) -> Getter:
    import requests

    session = requests.Session()

    def get(url: str, params: Dict[str, Any]) -> Any:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    return get


def _tf_ms(tf: str) -> int:
    if tf not in _TF_MS:
        raise ValueError(f"unsupported timeframe {tf}")
    return _TF_MS[tf] * MINUTE_MS


# ── Binance USD-M ────────────────────────────────────────────────────────────

def binance_klines(get: Getter, symbol: str, tf: str, start_ms: int, end_ms: int, *, base: str = BINANCE_FAPI,
                   limit: int = 1500) -> List[list]:
    step, out, cursor = _tf_ms(tf), [], start_ms
    while cursor <= end_ms:
        batch = get(f"{base}/fapi/v1/klines", {"symbol": symbol, "interval": tf, "startTime": cursor,
                                               "endTime": end_ms, "limit": limit}) or []
        if not batch:
            break
        for k in batch:
            out.append([int(k[0]), k[1], k[2], k[3], k[4], k[5], int(k[6]), k[7], int(k[8])])
        nxt = int(batch[-1][0]) + step
        if nxt <= cursor:
            break
        cursor = nxt
    return _dedupe(out)


def binance_funding(get: Getter, symbol: str, start_ms: int, end_ms: int, *, base: str = BINANCE_FAPI) -> List[Tuple[str, int, float]]:
    out, cursor = [], start_ms
    while cursor <= end_ms:
        batch = get(f"{base}/fapi/v1/fundingRate", {"symbol": symbol, "startTime": cursor, "endTime": end_ms,
                                                    "limit": 1000}) or []
        if not batch:
            break
        out.extend(("funding_rate", int(r["fundingTime"]), float(r["fundingRate"])) for r in batch)
        nxt = int(batch[-1]["fundingTime"]) + 1
        if nxt <= cursor:
            break
        cursor = nxt
    return out


def binance_open_interest(get: Getter, symbol: str, start_ms: int, end_ms: int, *, period: str = "1h",
                          base: str = BINANCE_FAPI, now_ms: Optional[int] = None):
    """Binance serves OI history only for the most recent ~30 days."""
    horizon = (now_ms or end_ms) - 30 * 86_400_000
    if end_ms < horizon:
        return FeatureUnavailable("open_interest", "VENUE_HISTORY_LIMIT_30D")
    rows = get(f"{base}/futures/data/openInterestHist", {"symbol": symbol, "period": period,
                                                        "startTime": max(start_ms, horizon), "endTime": end_ms,
                                                        "limit": 500}) or []
    return [("open_interest", int(r["timestamp"]), float(r["sumOpenInterest"])) for r in rows]


# ── Bybit V5 ─────────────────────────────────────────────────────────────────

def bybit_klines(get: Getter, symbol: str, tf: str, start_ms: int, end_ms: int, *, category: str = "linear",
                 base: str = BYBIT_API) -> List[list]:
    step, out, cursor = _tf_ms(tf), [], start_ms
    while cursor <= end_ms:
        data = get(f"{base}/v5/market/kline", {"category": category, "symbol": symbol, "interval": _BYBIT_TF[tf],
                                               "start": cursor, "end": min(end_ms, cursor + step * 1000 - 1),
                                               "limit": 1000}) or {}
        rows = sorted(((data.get("result") or {}).get("list") or []), key=lambda r: int(r[0]))
        for r in rows:  # [start, open, high, low, close, volume, turnover]
            t = int(r[0])
            out.append([t, r[1], r[2], r[3], r[4], r[5], t + step - 1, r[6], None])
        cursor = cursor + step * 1000
    return _dedupe([r for r in out if start_ms <= r[0] <= end_ms])


def bybit_funding(get: Getter, symbol: str, start_ms: int, end_ms: int, *, base: str = BYBIT_API) -> List[Tuple[str, int, float]]:
    out, end = [], end_ms
    while end >= start_ms:
        data = get(f"{base}/v5/market/funding/history", {"category": "linear", "symbol": symbol,
                                                          "startTime": start_ms, "endTime": end, "limit": 200}) or {}
        rows = (data.get("result") or {}).get("list") or []
        if not rows:
            break
        out.extend(("funding_rate", int(r["fundingRateTimestamp"]), float(r["fundingRate"])) for r in rows)
        oldest = min(int(r["fundingRateTimestamp"]) for r in rows)
        if oldest - 1 >= end:
            break
        end = oldest - 1
    return sorted(set(out), key=lambda x: x[1])


def bybit_open_interest(get: Getter, symbol: str, start_ms: int, end_ms: int, *, interval: str = "1h",
                        base: str = BYBIT_API) -> List[Tuple[str, int, float]]:
    out, cursor = [], None
    for _ in range(200):
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "startTime": start_ms,
                  "endTime": end_ms, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = get(f"{base}/v5/market/open-interest", params) or {}
        res = data.get("result") or {}
        out.extend(("open_interest", int(r["timestamp"]), float(r["openInterest"])) for r in res.get("list") or [])
        cursor = res.get("nextPageCursor")
        if not cursor:
            break
    return sorted(set(out), key=lambda x: x[1])


# ── BingX perpetual swap ─────────────────────────────────────────────────────

def bingx_klines(get: Getter, symbol: str, tf: str, start_ms: int, end_ms: int, *, base: str = BINGX_API) -> List[list]:
    step, out, cursor = _tf_ms(tf), [], start_ms
    sym = symbol if "-" in symbol else _bingx_symbol(symbol)
    while cursor <= end_ms:
        data = get(f"{base}/openApi/swap/v3/quote/klines", {"symbol": sym, "interval": tf, "startTime": cursor,
                                                            "endTime": end_ms, "limit": 1000}) or {}
        rows = sorted(data.get("data") or [], key=lambda k: int(k["time"]))
        if not rows:
            break
        for k in rows:
            t = int(k["time"])
            out.append([t, k["open"], k["high"], k["low"], k["close"], k["volume"], t + step - 1, None, None])
        nxt = int(rows[-1]["time"]) + step
        if nxt <= cursor:
            break
        cursor = nxt
    return _dedupe([r for r in out if start_ms <= r[0] <= end_ms])


def bingx_open_interest(*_a, **_k) -> FeatureUnavailable:
    return FeatureUnavailable("open_interest", "VENUE_HAS_NO_OI_HISTORY_ENDPOINT")


def liquidations(venue: str) -> FeatureUnavailable:
    # None of the three venues publishes a complete historical liquidation
    # feed through the REST API used here; the live streams are partial
    # (Binance forceOrder is throttled to one event per symbol per second).
    return FeatureUnavailable("liquidations_long", f"{venue.upper()}_NO_COMPLETE_HISTORICAL_LIQUIDATION_FEED")


def _bingx_symbol(sym: str) -> str:
    for q in ("USDT", "USDC"):
        if sym.upper().endswith(q):
            return f"{sym[:-len(q)]}-{q}"
    return sym


def _dedupe(rows: List[list]) -> List[list]:
    seen, out = set(), []
    for r in sorted(rows, key=lambda r: r[0]):
        if r[0] in seen:
            continue
        seen.add(r[0])
        out.append(r)
    return out


FETCHERS = {
    "binance_usdm": {"klines": binance_klines, "funding": binance_funding, "open_interest": binance_open_interest},
    "bybit_linear": {"klines": bybit_klines, "funding": bybit_funding, "open_interest": bybit_open_interest},
    "bingx_swap": {"klines": bingx_klines, "open_interest": bingx_open_interest},
}

__all__ = ["BINANCE_FAPI", "BINGX_API", "BYBIT_API", "FETCHERS", "FeatureUnavailable", "binance_funding",
           "binance_klines", "binance_open_interest", "bingx_klines", "bingx_open_interest", "bybit_funding",
           "bybit_klines", "bybit_open_interest", "liquidations", "requests_getter"]
