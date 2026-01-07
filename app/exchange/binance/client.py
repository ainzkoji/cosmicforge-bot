from __future__ import annotations

import random
import time
import requests

from app.exchange.binance.signing import build_query, sign
from decimal import Decimal
from app.exchange.binance.filters import (
    extract_filters,
    round_qty,
)
from app.exchange.binance.filters import set_exchange_info


def kline_closes(klines: list) -> list[float]:
    """
    Binance kline format:
    [openTime, open, high, low, close, volume, closeTime, ...]
    """
    return [float(k[4]) for k in klines]


class BinanceFuturesClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        recv_window: int = 5000,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.recv_window = recv_window

        self._exchange_info_cache: dict | None = None
        self._exchange_info_cache_ts: float = 0.0
        # ✅ ADD: server time offset (ms)
        self._time_offset_ms: int = 0

        # load exchange info (used by filters/rounding)
        try:
            set_exchange_info(self.exchange_info())
        except Exception:
            # don't crash app at import/startup; runners can refresh later
            pass

        # sync time once (timestamp safety)
        try:
            self.sync_time()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # ✅ ADD: robust request helper (PASTE EXACTLY, UNCHANGED)
    # ------------------------------------------------------------------
    def _request(
        self, method: str, path: str, params=None, headers=None, max_retries: int = 6
    ):
        url = f"{self.base_url}{path}"
        params = dict(params or {})
        headers = dict(headers or {})

        last_err = None
        for attempt in range(max_retries + 1):
            try:
                r = requests.request(
                    method, url, params=params, headers=headers, timeout=15
                )

                # Rate limit / temp ban
                if r.status_code in (418, 429):
                    ra = r.headers.get("Retry-After")
                    sleep_s = float(ra) if ra else (0.4 * (2**attempt))
                    sleep_s += random.uniform(0, 0.2)
                    time.sleep(min(sleep_s, 10.0))
                    continue

                # Timestamp drift
                if r.status_code == 400 and "timestamp" in r.text.lower():
                    try:
                        self.sync_time()
                    except Exception:
                        pass
                    continue

                # Server errors
                if r.status_code >= 500:
                    time.sleep(min(0.4 * (2**attempt), 8.0))
                    continue

                r.raise_for_status()
                return r.json() if r.content else None

            except (requests.Timeout, requests.ConnectionError) as e:
                last_err = e
                time.sleep(min(0.4 * (2**attempt), 8.0))
                continue
            except Exception as e:
                last_err = e
                break

        raise RuntimeError(
            f"Binance request failed after retries: {method} {path} ({last_err})"
        )

    # ---------------- TIME SYNC (PUBLIC) ----------------

    def _public_get(self, path: str, params: dict | None = None) -> dict:
        r = requests.get(f"{self.base_url}{path}", params=params or {}, timeout=20)
        if r.status_code >= 400:
            raise RuntimeError(f"Binance HTTP {r.status_code}: {r.text}")
        return r.json()

    def _server_time_ms(self) -> int:
        data = self._public_get("/fapi/v1/time")
        return int(data["serverTime"])

    def sync_time(self) -> int:
        """
        Computes and stores local->server time offset.
        Positive offset means local clock is behind server.
        """
        local_ms = int(time.time() * 1000)
        server_ms = self._server_time_ms()
        self._time_offset_ms = server_ms - local_ms
        return self._time_offset_ms

    # ---------------- SIGNED REQUESTS ----------------

    def _signed_get(self, path: str, params: dict | None = None) -> dict:
        return self._signed_request("GET", path, params)

    def _signed_post(self, path: str, params: dict | None = None) -> dict:
        return self._signed_request("POST", path, params)

    def _signed_delete(self, path: str, params: dict | None = None) -> dict:
        return self._signed_request("DELETE", path, params)

    def _signed_request(
        self, method: str, path: str, params: dict | None = None
    ) -> dict:
        if not self.api_key or not self.api_secret:
            raise ValueError("Missing BINANCE_API_KEY or BINANCE_API_SECRET in .env")

        params = params or {}

        # ✅ use offset timestamp
        params["timestamp"] = int(time.time() * 1000) + int(self._time_offset_ms)
        params["recvWindow"] = self.recv_window

        query = build_query(params)
        signature = sign(self.api_secret, query)

        url = f"{self.base_url}{path}?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}

        # First attempt
        if method == "GET":
            r = requests.get(url, headers=headers, timeout=20)
        elif method == "POST":
            r = requests.post(url, headers=headers, timeout=20)
        elif method == "DELETE":
            r = requests.delete(url, headers=headers, timeout=20)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # ✅ If timestamp error, sync + retry once
        if r.status_code == 400:
            try:
                data = r.json()
            except Exception:
                data = None

            if isinstance(data, dict) and data.get("code") == -1021:
                # resync and retry once
                self.sync_time()
                params["timestamp"] = int(time.time() * 1000) + int(
                    self._time_offset_ms
                )
                query = build_query(params)
                signature = sign(self.api_secret, query)
                url = f"{self.base_url}{path}?{query}&signature={signature}"

                if method == "GET":
                    r = requests.get(url, headers=headers, timeout=20)
                elif method == "POST":
                    r = requests.post(url, headers=headers, timeout=20)
                elif method == "DELETE":
                    r = requests.delete(url, headers=headers, timeout=20)

        if r.status_code >= 400:
            raise RuntimeError(f"Binance HTTP {r.status_code}: {r.text}")
        return r.json()

    # ---------------- PUBLIC ----------------

    def ping(self) -> dict:
        # (left untouched)
        r = requests.get(f"{self.base_url}/fapi/v1/ping", timeout=20)
        return {"status_code": r.status_code}

    def exchange_info(self) -> dict:
        # ✅ UPDATED to use _request helper
        return self._request("GET", "/fapi/v1/exchangeInfo")

    def exchange_info_cached(self, ttl_seconds: int = 60) -> dict:
        now = time.time()
        if (
            self._exchange_info_cache
            and (now - self._exchange_info_cache_ts) < ttl_seconds
        ):
            return self._exchange_info_cache

        data = self.exchange_info()
        self._exchange_info_cache = data
        self._exchange_info_cache_ts = now
        return data

    def klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> list:
        # ✅ UPDATED to use _request helper
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        return self._request("GET", "/fapi/v1/klines", params=params)

    def mark_price(self, symbol: str) -> dict:
        # ✅ UPDATED to use _request helper
        return self._request(
            "GET",
            "/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )

    def all_prices(self) -> list:
        # ✅ UPDATED to use _request helper
        return self._request("GET", "/fapi/v1/ticker/price")

    def last_price(self, symbol: str) -> float:
        # ✅ UPDATED to use _request helper
        data = self._request(
            "GET",
            "/fapi/v1/ticker/price",
            params={"symbol": symbol},
        )
        return float(data["price"])

    # ---------------- ACCOUNT / TRADING ----------------

    def account_balance(self) -> dict:
        return self._signed_get("/fapi/v2/balance", {})

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return self._signed_post(
            "/fapi/v1/leverage",
            {"symbol": symbol.upper(), "leverage": leverage},
        )

    def place_market_order(self, symbol: str, side: str, quantity: float) -> dict:
        return self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": symbol.upper(),
                "side": side,
                "type": "MARKET",
                "quantity": quantity,
            },
        )

    def open_orders(self, symbol: str | None = None) -> dict:
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return self._signed_get("/fapi/v1/openOrders", params)

    def position_risk(self, symbol: str | None = None) -> dict:
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return self._signed_get("/fapi/v2/positionRisk", params)

    def position_risk_all(self) -> list:
        """
        Fetch ALL futures positions risk info (no symbol filter).
        Binance returns a list of positionRisk entries.
        """
        data = self._signed_get("/fapi/v2/positionRisk", {})
        return data if isinstance(data, list) else []

    def get_position_amt(self, symbol: str) -> float:
        data = self.position_risk(symbol)
        if not (isinstance(data, list) and data):
            return 0.0

        # pick the entry with the largest absolute positionAmt
        best = 0.0
        for p in data:
            try:
                amt = float(p.get("positionAmt", "0") or "0")
            except Exception:
                amt = 0.0
            if abs(amt) > abs(best):
                best = amt
        return best

    def close_position_market(self, symbol: str) -> dict:
        amt = self.get_position_amt(symbol)

        # float tolerance
        if abs(amt) < 1e-12:
            return {"status": "no_position", "symbol": symbol}

        side = "SELL" if amt > 0 else "BUY"
        qty = abs(amt)

        return self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": symbol.upper(),
                "side": side,
                "type": "MARKET",
                "quantity": qty,
                "reduceOnly": "true",
            },
        )

    def user_trades(
        self,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list:
        params: dict = {"symbol": symbol.upper(), "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._signed_get("/fapi/v1/userTrades", params)

    # ---------------- PROTECTION ORDERS ----------------

    def place_stop_market(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        reduce_only: bool = True,
    ) -> dict:
        return self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": symbol.upper(),
                "side": side,
                "type": "STOP_MARKET",
                "stopPrice": stop_price,
                "closePosition": "true",
                "workingType": "CONTRACT_PRICE",
            },
        )

    def place_take_profit_market(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        reduce_only: bool = True,
    ) -> dict:
        return self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": symbol.upper(),
                "side": side,
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": stop_price,
                "closePosition": "true",
                "workingType": "CONTRACT_PRICE",
            },
        )

    def cancel_all_orders(self, symbol: str) -> dict:
        return self._signed_delete(
            "/fapi/v1/allOpenOrders",
            {"symbol": symbol.upper()},
        )

    def get_position_info(self, symbol: str) -> dict | None:
        data = self.position_risk(symbol)
        if isinstance(data, list) and data:
            return data[0]
        return None

    def get_order(self, symbol: str, order_id: int) -> dict:
        return self._signed_get(
            "/fapi/v1/order",
            {"symbol": symbol.upper(), "orderId": int(order_id)},
        )
