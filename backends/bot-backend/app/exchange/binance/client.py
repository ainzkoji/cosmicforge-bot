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

        # Use session for connection pooling
        self.session = requests.Session()

        self._exchange_info_cache: dict | None = None
        self._exchange_info_cache_ts: float = 0.0
        self._time_offset_ms: int = 0

        try:
            set_exchange_info(self.exchange_info())
        except Exception:
            pass

        try:
            self.sync_time()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # robust request helper
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
                # ✅ DEBUG: Lower timeout to 5s to detection stalls faster
                # Use session for pooling
                r = self.session.request(
                    method, url, params=params, headers=headers, timeout=5
                )

                if r.status_code in (418, 429):
                    ra = r.headers.get("Retry-After")
                    sleep_s = float(ra) if ra else (0.4 * (2**attempt))
                    sleep_s += random.uniform(0, 0.2)
                    time.sleep(min(sleep_s, 10.0))
                    continue

                if r.status_code == 400 and "timestamp" in r.text.lower():
                    try:
                        self.sync_time()
                    except Exception:
                        pass
                    continue

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

    # ---------------- TIME SYNC ----------------

    def _public_get(self, path: str, params: dict | None = None) -> dict:
        r = self.session.get(f"{self.base_url}{path}", params=params or {}, timeout=20)
        if r.status_code >= 400:
            raise RuntimeError(f"Binance HTTP {r.status_code}: {r.text}")
        return r.json()

    def server_time(self) -> int:
        return self._server_time_ms()

    def _server_time_ms(self) -> int:
        data = self._public_get("/fapi/v1/time")
        return int(data["serverTime"])

    def sync_time(self) -> int:
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
        params["timestamp"] = int(time.time() * 1000) + int(self._time_offset_ms)
        params["recvWindow"] = self.recv_window

        query = build_query(params)
        signature = sign(self.api_secret, query)

        url = f"{self.base_url}{path}?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}

        if method == "GET":
            r = self.session.get(url, headers=headers, timeout=20)
        elif method == "POST":
            r = self.session.post(url, headers=headers, timeout=20)
        elif method == "DELETE":
            r = self.session.delete(url, headers=headers, timeout=20)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        if r.status_code == 400:
            try:
                data = r.json()
            except Exception:
                data = None

            if isinstance(data, dict) and data.get("code") == -1021:
                self.sync_time()
                params["timestamp"] = int(time.time() * 1000) + int(
                    self._time_offset_ms
                )
                query = build_query(params)
                signature = sign(self.api_secret, query)
                url = f"{self.base_url}{path}?{query}&signature={signature}"

                if method == "GET":
                    r = self.session.get(url, headers=headers, timeout=20)
                elif method == "POST":
                    r = self.session.post(url, headers=headers, timeout=20)
                elif method == "DELETE":
                    r = self.session.delete(url, headers=headers, timeout=20)

        if r.status_code >= 400:
            try:
                err_data = r.json()
                if isinstance(err_data, dict) and err_data.get("code") == -4130:
                    # An open stop or take profit order with GTE and closePosition in the direction is existing.
                    return {"orderId": "DUPLICATE_4130", "status": "NEW", "msg": "duplicate_ignored"}
            except Exception:
                pass
            raise RuntimeError(f"Binance HTTP {r.status_code}: {r.text}")
        return r.json()

    # ---------------- PUBLIC ----------------

    def ping(self) -> dict:
        r = self.session.get(f"{self.base_url}/fapi/v1/ping", timeout=20)
        return {"status_code": r.status_code}

    def exchange_info(self) -> dict:
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

    def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        """
        Get standardized sizing filters for a symbol.
        """
        try:
            info = self.exchange_info_cached()
            return extract_filters(info, symbol)
        except Exception:
            # Fallback if symbol not found or error
            return SymbolFilters()

    def list_instruments(self):
        """
        Get list of all instrument specs from exchange.
        Required by InstrumentRegistry for populating symbol specs.
        """
        from app.models.unified_trading import InstrumentSpec
        import sys
        
        ei = self.exchange_info_cached()
        specs = []
        errors = []
        
        for s in ei.get("symbols", []):
            symbol = s.get("symbol", "")
            if not symbol:
                continue
                
            try:
                filters = extract_filters(ei, symbol)
                spec = InstrumentSpec(
                    broker_id="binance",
                    symbol_canonical=symbol,
                    symbol_exchange=symbol,  # Same as canonical for Binance
                    asset_class="crypto_perp",  # LOWERCASE - Binance Futures is crypto perp
                    base_currency=s.get("baseAsset", ""),
                    quote_currency=s.get("quoteAsset", "USDT"),
                    margin_currency=s.get("quoteAsset", "USDT"),
                    settlement_currency=s.get("quoteAsset", "USDT"),
                    contract_size=filters.contract_size,
                    tick_size=filters.tick_size,
                    step_size=filters.step_size,
                    min_qty=filters.min_qty,
                    max_qty=filters.max_qty,
                    min_notional=filters.min_notional,
                    price_precision=s.get("pricePrecision", 2),
                    qty_precision=s.get("quantityPrecision", 3),
                    max_leverage=125,  # Binance default max
                    supports_per_order_leverage=True,
                )
                specs.append(spec)
            except Exception as e:
                # Log first few errors for debugging
                if len(errors) < 3:
                    errors.append(f"{symbol}: {type(e).__name__}: {e}")
        
        if errors:
            print(f"[CLIENT DEBUG] list_instruments errors (first 3): {errors}", file=sys.stderr)
        print(f"[CLIENT DEBUG] Successfully created {len(specs)} instrument specs", file=sys.stderr)
        
        return specs

    def get_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        Get current market prices for specified symbols.
        Returns dict mapping symbol -> price.
        """
        from typing import List, Dict
        
        # Fetch all ticker prices
        response = self._request("GET", "/fapi/v1/ticker/price")
        
        # Build lookup
        price_map = {}
        symbol_set = set(symbols) if symbols else None
        
        for item in response:
            symbol = item.get("symbol", "")
            if symbol_set and symbol not in symbol_set:
                continue
            price_map[symbol] = float(item.get("price", 0.0))
        
        return price_map

    def place_order(self, req):
        """
        Unified interface for placing orders.
        Maps OrderRequest to Binance API.
        """
        from app.models.unified_trading import UnifiedOrder, OrderStatus
        
        # Set leverage if specified
        if req.leverage:
            self.set_leverage(req.symbol, int(req.leverage))
        
        # Build params
        params = {
            "symbol": req.symbol,
            "side": req.side.value.upper(),
            "type": req.type.upper(),
            "quantity": float(req.qty),
        }
        if getattr(req, "client_order_id", None):
            params["newClientOrderId"] = str(req.client_order_id)
        
        if req.reduce_only:
            params["reduceOnly"] = "true"
        
        # Execute
        response = self._signed_post("/fapi/v1/order", params=params)
        
        # Map to UnifiedOrder
        return UnifiedOrder(
            client_order_id=str(response.get("clientOrderId", "")),
            broker_order_id=str(response.get("orderId", "")),
            symbol=req.symbol,
            side=req.side,
            type=req.type,
            qty_ordered=req.qty,
            qty_filled=Decimal(response.get("executedQty", "0")),
            avg_fill_price=Decimal(response.get("avgPrice") or response.get("price", "0")),
            status=OrderStatus.FILLED if response.get("status") == "FILLED" else OrderStatus.NEW,
            timestamp=int(response.get("updateTime", response.get("transactTime", 0))),
            reduce_only=req.reduce_only
        )

    def place_protection(self, req):
        """
        Place stop-loss and take-profit orders via /fapi/v1/algoOrder.
        Returns ProtectionResult with status='success' only when all requested
        orders are confirmed placed. Any exception surfaces as status='failed'.

        NOTE: Binance /fapi/v1/algoOrder responses use 'algoId' (NOT 'orderId').
        Prices are rounded to the symbol's tick size to avoid HTTP -1111 errors.
        """
        from app.models.unified_trading import ProtectionResult, Side
        from app.exchange.binance.filters import round_price_down, round_price_up, _tick
        import logging
        _log = logging.getLogger(__name__)

        result = ProtectionResult(status="initiated")
        exit_side = "SELL" if req.position_side == Side.BUY else "BUY"

        try:
            # ── Resolve tick size once and snap prices to valid precision ──────────────
            # Binance rejects prices that don't match the symbol's PRICE_FILTER tickSize
            # (HTTP 400 code -1111: "Precision is over the maximum defined for this asset.")
            try:
                tick = _tick(req.symbol)
            except Exception:
                tick = 0.0001  # safe fallback for unknown symbols

            from app.exchange.binance.filters import normalize_protection_price
            
            pos_side_str = "LONG" if req.position_side == Side.BUY else "SHORT"

            if req.sl_price:
                sl_str = normalize_protection_price(req.sl_price, tick, pos_side_str, "SL")
                _log.info(
                    f"[FORENSIC] {req.symbol} side={pos_side_str} tick={tick} "
                    f"raw={req.sl_price} norm_str={sl_str} endpoint=/fapi/v1/algoOrder leg=SL"
                )
                sl_params = {
                    "algoType": "CONDITIONAL",
                    "symbol": req.symbol,
                    "side": exit_side,
                    "type": "STOP_MARKET",
                    "stopPrice": sl_str,       # exact string
                    "triggerPrice": sl_str,    # exact string
                    "closePosition": "true",
                    "workingType": "CONTRACT_PRICE"
                }
                sl_response = self._signed_post("/fapi/v1/algoOrder", params=sl_params)
                # Binance algo orders return 'algoId', not 'orderId'
                _sl_id = (
                    sl_response.get("algoId")
                    or sl_response.get("orderId")
                    or sl_response.get("clientAlgoId")
                )
                if _sl_id is None:
                    _log.error(f"[PLACE_PROTECTION] {req.symbol}: SL response missing algoId/orderId. Raw: {sl_response}")
                    raise ValueError(f"SL algoOrder response has no id field. Keys: {list(sl_response.keys())}")
                result.sl_order_id = str(_sl_id)

            if req.tp_price:
                tp_str = normalize_protection_price(req.tp_price, tick, pos_side_str, "TP")
                _log.info(
                    f"[FORENSIC] {req.symbol} side={pos_side_str} tick={tick} "
                    f"raw={req.tp_price} norm_str={tp_str} endpoint=/fapi/v1/algoOrder leg=TP"
                )
                tp_params = {
                    "algoType": "CONDITIONAL",
                    "symbol": req.symbol,
                    "side": exit_side,
                    "type": "TAKE_PROFIT_MARKET",
                    "stopPrice": tp_str,       # exact string
                    "triggerPrice": tp_str,    # exact string
                    "closePosition": "true",
                    "workingType": "CONTRACT_PRICE"
                }
                tp_response = self._signed_post("/fapi/v1/algoOrder", params=tp_params)
                # Binance algo orders return 'algoId', not 'orderId'
                _tp_id = (
                    tp_response.get("algoId")
                    or tp_response.get("orderId")
                    or tp_response.get("clientAlgoId")
                )
                if _tp_id is None:
                    _log.error(f"[PLACE_PROTECTION] {req.symbol}: TP response missing algoId/orderId. Raw: {tp_response}")
                    raise ValueError(f"TP algoOrder response has no id field. Keys: {list(tp_response.keys())}")
                result.tp_order_id = str(_tp_id)

            result.status = "success"
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            _log.error(
                f"[PLACE_PROTECTION] {req.symbol}: protection placement failed: {e}. "
                f"sl_order_id={result.sl_order_id} tp_order_id={result.tp_order_id}"
            )

        return result



    def get_algo_orders(self, symbol: str, raise_on_error: bool = False) -> list:
        """
        S3: Return open algo orders for protection verification.
        Conditional orders placed via /fapi/v1/algoOrder are listed at
        GET /fapi/v1/openAlgoOrders (NOT /fapi/v1/algoOrders which does not exist).
        """
        import logging
        _log = logging.getLogger(__name__)
        try:
            # The correct Binance Futures endpoint for listing open algo/conditional orders
            result = self._signed_get("/fapi/v1/openAlgoOrders", {"symbol": symbol.upper()})
            # Response is {"algoOrders": [...]} or a list depending on API version
            if isinstance(result, dict):
                result = result.get("algoOrders", result.get("orders", []))
            return result if isinstance(result, list) else []
        except Exception as e:
            _log.warning(f"[ALGO_ORDERS] {symbol}: get_algo_orders failed: {e}")
            if raise_on_error:
                raise
            return []

    def get_positions(self):
        """
        Get all open positions.
        """
        from app.models.unified_trading import UnifiedPosition, Side, PositionMode
        
        raw = self.position_risk()
        positions = []
        
        for p in raw:
            amt = Decimal(p["positionAmt"])
            if amt == 0:
                continue
            
            side = Side.BUY if amt > 0 else Side.SELL
            qty = abs(amt)
            
            positions.append(UnifiedPosition(
                symbol=p["symbol"],
                broker_id="binance",
                side=side,
                quantity=qty,
                entry_price=Decimal(p["entryPrice"]),
                current_price=Decimal(p.get("markPrice", p["entryPrice"])),
                unrealized_pnl=Decimal(p["unRealizedProfit"]),
                realized_pnl=Decimal("0"),
                margin_used=Decimal(p.get("initialMargin", "0")),
                leverage=Decimal(p["leverage"]),
                mode=PositionMode.ONE_WAY,
                timestamp=int(time.time() * 1000)
            ))
        
        return positions

    def klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> list:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        return self._request("GET", "/fapi/v1/klines", params=params)

    def historical_klines(
        self,
        symbol: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: int
    ) -> list:
        """
        Fetch historical klines with automatic pagination for backtesting.
        
        Args:
            symbol: Trading pair (e.g. 'BTCUSDT')
            interval: Candlestick interval ('1m', '5m', '1h', etc.)
            start_time_ms: Start time in milliseconds (Unix timestamp)
            end_time_ms: End time in milliseconds (Unix timestamp)
        
        Returns:
            List of klines in Binance format:
            [[open_time, open, high, low, close, volume, close_time, ...], ...]
        """
        all_klines = []
        current_start = start_time_ms
        limit = 1500  # Binance API max per request
        
        while current_start < end_time_ms:
            params = {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": current_start,
                "endTime": end_time_ms,
                "limit": limit
            }
            
            batch = self._request("GET", "/fapi/v1/klines", params=params)
            
            if not batch:
                break
            
            all_klines.extend(batch)
            
            # Move to next batch (use close time of last candle + 1ms)
            last_close_time = int(batch[-1][6])
            current_start = last_close_time + 1
            
            # Rate limiting (100ms delay between requests)
            if current_start < end_time_ms:
                time.sleep(0.1)
        
        return all_klines

    def mark_price(self, symbol: str) -> dict:
        return self._request(
            "GET",
            "/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )

    def book_ticker(self, symbol: str) -> dict:
        """
        Best bid/ask snapshot. Useful fallback when last price endpoint misbehaves.
        """
        return self._request(
            "GET",
            "/fapi/v1/ticker/bookTicker",
            params={"symbol": symbol.upper()},
        )

    # ---------------- SUBSTITUTED METHOD (ONLY CHANGE) ----------------

    def last_price(self, symbol: str) -> float:
        """
        Robust price getter.

        Primary: /fapi/v1/ticker/price (last traded)
        Fallback 1: /fapi/v1/premiumIndex (markPrice)
        Fallback 2: /fapi/v1/ticker/bookTicker (mid of bid/ask)

        Handles rare cases where Binance returns {} or missing fields.
        """
        symbol = symbol.upper().strip()

        def _to_float(x):
            try:
                v = float(x)
                if v > 0:
                    return v
            except Exception:
                return None
            return None

        last_err = None
        data = None  # ✅ ensures error messages can reference data safely

        # small retry loop for transient {} payloads
        for _ in range(3):
            try:
                data = self._request(
                    "GET",
                    "/fapi/v1/ticker/price",
                    params={"symbol": symbol},
                )

                # Handle both raw float and dict payloads
                if isinstance(data, (int, float, str)):
                    return float(data)

                if isinstance(data, dict):
                    # ✅ FIX: Binance sometimes returns {} for ticker/price
                    px = data.get("price") if data else None
                    if px is None or px == "":
                        # ✅ fallback to mark price (premiumIndex)
                        mp = self.mark_price(symbol)
                        if isinstance(mp, dict):
                            mp_px = mp.get("markPrice")  # markPrice is the one we want
                            if mp_px is not None and mp_px != "":
                                return float(mp_px)
                        raise RuntimeError(
                            f"last_price unavailable for {symbol} "
                            f"(ticker returned {data}, premiumIndex returned {mp})"
                        )
                    return float(px)

                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("symbol") == symbol:
                            p = _to_float(item.get("price"))
                            if p is not None:
                                return p
                    last_err = f"ticker/price list payload missing symbol {symbol}"
                    time.sleep(0.15)
                    continue

                # ✅ do NOT raise here; preserve retry + fallbacks
                last_err = f"Unexpected last_price payload ({type(data).__name__}) for {symbol}: {data}"
                time.sleep(0.15)
                continue

            except Exception as e:
                last_err = str(e)
                time.sleep(0.15)
                continue

        # Fallback 1: mark price
        try:
            mp = self.mark_price(symbol)
            p = _to_float((mp or {}).get("markPrice"))
            if p is not None:
                return p
            last_err = f"premiumIndex missing markPrice for {symbol}: {mp}"
        except Exception as e:
            last_err = f"premiumIndex failed: {e}"

        # Fallback 2: bid/ask mid
        try:
            bt = self.book_ticker(symbol)
            bid = _to_float((bt or {}).get("bidPrice"))
            ask = _to_float((bt or {}).get("askPrice"))
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
                if mid > 0:
                    return mid
            last_err = f"bookTicker missing bid/ask for {symbol}: {bt}"
        except Exception as e:
            last_err = f"bookTicker failed: {e}"

        raise RuntimeError(f"Price fetch failed for {symbol}. Last error: {last_err}")

    # ---------------- ACCOUNT / TRADING ----------------

    def account(self) -> dict:
        return self._signed_get("/fapi/v2/account", {})

    def account_balance(self) -> dict:
        return self._signed_get("/fapi/v2/balance", {})

    def get_account_snapshot(self) -> dict:
        """
        Get normalized account snapshot for equity tracking.
        Returns standardized dict across all brokers.
        """
        acc = self.account()
        return {
            "wallet_balance": float(acc.get("totalWalletBalance", 0.0)),
            "equity": float(acc.get("totalMarginBalance", 0.0)),
            "available_balance": float(acc.get("availableBalance", 0.0)),
            "unrealized_pnl": float(acc.get("totalUnrealizedProfit", 0.0)),
            "margin_used": float(acc.get("totalInitialMargin", 0.0)),
            "currency": "USDT",
            "raw": {k: v for k, v in acc.items() if k not in ["assets", "positions"]}
        }

    def get_transfers_history(self, start_time=None, end_time=None, limit=100, cursor=None) -> dict:
        """
        Get deposit/withdraw history from Binance.
        Normalized response for transfer tracking.
        """
        # Binance Futures doesn't have a unified transfer history endpoint easily accessible
        # This would require calling multiple endpoints (deposit history, withdraw history)
        # For now, return empty - can implement if needed
        return {"items": [], "next_cursor": None}

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return self._signed_post(
            "/fapi/v1/leverage",
            {"symbol": symbol.upper(), "leverage": leverage},
        )

    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> dict:
        params = {
            "symbol": symbol.upper(),
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        if client_order_id:
            params["newClientOrderId"] = str(client_order_id)
        return self._signed_post("/fapi/v1/order", params)

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
        data = self._signed_get("/fapi/v2/positionRisk", {})
        return data if isinstance(data, list) else []

    def get_position_amt(self, symbol: str) -> float:
        data = self.position_risk(symbol)
        if not (isinstance(data, list) and data):
            return 0.0

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
        if abs(amt) < 1e-12:
            return {"status": "no_position", "symbol": symbol}

        side = "SELL" if amt > 0 else "BUY"
        qty = abs(amt)
        # Round close quantity to step size to avoid LOT_SIZE errors.
        # We round DOWN to avoid exceeding position size; executor confirms flat.
        try:
            exch = self.exchange_info_cached()
            flt = extract_filters(exch, symbol)
            qty = float(round_qty(qty, flt.step_size))
        except Exception:
            pass

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

    def income_history(
        self,
        symbol: str | None = None,
        income_type: str | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list:
        params: dict = {"limit": limit}
        if symbol:
            params["symbol"] = symbol.upper()
        if income_type:
            params["incomeType"] = income_type
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._signed_get("/fapi/v1/income", params)

    # ---------------- PROTECTION ORDERS ----------------

    def place_stop_market(
        self,
        symbol: str,
        side: str,
        stop_price: float | str,
        reduce_only: bool = True,
    ) -> dict:
        stop_price_str = str(stop_price)
        import logging
        logging.getLogger(__name__).info(f"[FORENSIC] {symbol} side={side} leg=SL endpoint=/fapi/v1/algoOrder type=STOP_MARKET stop_price={stop_price_str}")
        return self._signed_post(
            "/fapi/v1/algoOrder",
            {
                "algoType": "CONDITIONAL",
                "symbol": symbol.upper(),
                "side": side,
                "type": "STOP_MARKET",
                "stopPrice": stop_price_str,
                "triggerPrice": stop_price_str,
                "closePosition": "true",
                "workingType": "CONTRACT_PRICE",
            },
        )

    def place_take_profit_market(
        self,
        symbol: str,
        side: str,
        stop_price: float | str,
        reduce_only: bool = True,
    ) -> dict:
        stop_price_str = str(stop_price)
        import logging
        logging.getLogger(__name__).info(f"[FORENSIC] {symbol} side={side} leg=TP endpoint=/fapi/v1/algoOrder type=TAKE_PROFIT_MARKET stop_price={stop_price_str}")
        return self._signed_post(
            "/fapi/v1/algoOrder",
            {
                "algoType": "CONDITIONAL",
                "symbol": symbol.upper(),
                "side": side,
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": stop_price_str,
                "triggerPrice": stop_price_str,
                "closePosition": "true",
                "workingType": "CONTRACT_PRICE",
            },
        )

    def cancel_all_orders(self, symbol: str) -> dict:
        return self._signed_delete(
            "/fapi/v1/allOpenOrders",
            {"symbol": symbol.upper()},
        )

    def update_protection(self, req) -> dict:
        """
        Cancel-replace SL/TP orders via Binance Futures.
        req: ProtectionUpdateRequest instance.

        Steps:
        1. Cancel old SL/TP by orderId if provided (or cancel-all for symbol).
        2. Place new STOP_MARKET with closePosition=true (reduce-only equivalent).
        3. Place new TAKE_PROFIT_MARKET with closePosition=true.
        Returns ProtectionResult-compatible dict.
        """
        import logging
        _log = logging.getLogger(__name__)
        symbol = req.symbol.upper()
        new_sl_id = None
        new_tp_id = None
        cancel_error = None
        replace_error = None

        is_long = str(getattr(req, "position_side", "LONG")).upper() == "LONG"
        exit_side = "SELL" if is_long else "BUY"

        # ---- Cancel old orders ----
        try:
            sl_oid = getattr(req, "old_sl_order_id", None)
            tp_oid = getattr(req, "old_tp_order_id", None)
            if sl_oid or tp_oid:
                for oid in [sl_oid, tp_oid]:
                    if oid:
                        try:
                            self._signed_delete("/fapi/v1/algoOrder", {
                                "symbol": symbol,
                                "algoId": oid,
                            })
                        except Exception as ce:
                            _log.warning(
                                f"[UPDATE_PROTECTION] {symbol}: cancel algoId={oid} "
                                f"failed (may already be filled/gone): {ce}"
                            )
            else:
                self.cancel_all_orders(symbol)
        except Exception as e:
            cancel_error = str(e)
            _log.error(f"[UPDATE_PROTECTION] {symbol}: cancel phase failed: {e}")

        # ---- Place new SL ----
        if getattr(req, "new_sl_price", None) is not None:
            try:
                from app.exchange.binance.filters import normalize_protection_price, _tick
                try:
                    tick = _tick(req.symbol)
                except Exception:
                    tick = 0.0001
                sl_str = normalize_protection_price(
                    req.new_sl_price, tick, "LONG" if is_long else "SHORT", "SL"
                )
                _log.info(
                    f"[FORENSIC] {req.symbol} side={'LONG' if is_long else 'SHORT'} tick={tick} "
                    f"raw={req.new_sl_price} norm_str={sl_str} endpoint=/fapi/v1/algoOrder leg=SL (mutation:{req.reason})"
                )
                sl_res = self._signed_post(
                    "/fapi/v1/algoOrder",
                    {
                        "algoType": "CONDITIONAL",
                        "symbol": symbol,
                        "side": exit_side,
                        "type": "STOP_MARKET",
                        "stopPrice": sl_str,
                        "triggerPrice": sl_str,
                        "closePosition": "true",
                        "workingType": "CONTRACT_PRICE",
                    },
                )
                new_sl_id = str(sl_res.get("algoId") or sl_res.get("orderId", ""))
                _log.info(f"[UPDATE_PROTECTION] {symbol}: new SL={req.new_sl_price} id={new_sl_id} reason={req.reason}")
            except Exception as e:
                replace_error = f"SL_PLACE_FAILED: {e}"
                _log.error(
                    f"[UPDATE_PROTECTION] {symbol}: CRITICAL — SL replace FAILED after cancel. "
                    f"Position now unprotected. Trigger ensure_protection IMMEDIATELY. Error: {e}"
                )

        # ---- Place new TP ----
        if getattr(req, "new_tp_price", None) is not None:
            try:
                from app.exchange.binance.filters import normalize_protection_price, _tick
                try:
                    tick = _tick(req.symbol)
                except Exception:
                    tick = 0.0001
                tp_str = normalize_protection_price(
                    req.new_tp_price, tick, "LONG" if is_long else "SHORT", "TP"
                )
                _log.info(
                    f"[FORENSIC] {req.symbol} side={'LONG' if is_long else 'SHORT'} tick={tick} "
                    f"raw={req.new_tp_price} norm_str={tp_str} endpoint=/fapi/v1/algoOrder leg=TP (mutation:{req.reason})"
                )
                tp_res = self._signed_post(
                    "/fapi/v1/algoOrder",
                    {
                        "algoType": "CONDITIONAL",
                        "symbol": symbol,
                        "side": exit_side,
                        "type": "TAKE_PROFIT_MARKET",
                        "stopPrice": tp_str,
                        "triggerPrice": tp_str,
                        "closePosition": "true",
                        "workingType": "CONTRACT_PRICE",
                    },
                )
                new_tp_id = str(tp_res.get("algoId") or tp_res.get("orderId", ""))
                _log.info(f"[UPDATE_PROTECTION] {symbol}: new TP={req.new_tp_price} id={new_tp_id} reason={req.reason}")
            except Exception as e:
                replace_error = (replace_error or "") + f" TP_PLACE_FAILED: {e}"
                _log.error(f"[UPDATE_PROTECTION] {symbol}: TP replace failed: {e}")

        # ---- S5: Raise on any replace error instead of returning REPLACE_PARTIAL_FAILURE ----
        # Callers in executor.py check status and raise on non-OK, but raising here is
        # defense-in-depth — if old callers bypass the check, they still get an exception.
        if replace_error:
            _log.critical(
                f"[UPDATE_PROTECTION] {symbol}: PARTIAL FAILURE — cancel succeeded but "
                f"replace failed. Position is now NAKED (no SL). "
                f"new_sl_id={new_sl_id} new_tp_id={new_tp_id} error={replace_error}"
            )
            raise RuntimeError(
                f"[SEV1-S5] update_protection replace failed for {symbol}: {replace_error}"
            )

        return {
            "sl_order_id": new_sl_id,
            "tp_order_id": new_tp_id,
            "status": "OK",
            "error": None,
        }

    def get_position_info(self, symbol: str) -> dict | None:
        data = self.position_risk(symbol)
        if isinstance(data, list) and data:
            # ✅ FIX: Scan all entries for the active position (Hedge Mode returns multiple)
            # Default to the first one if all are flat
            selected = data[0]
            for p in data:
                try:
                    amt = float(p.get("positionAmt", "0") or "0")
                    if abs(amt) > 0:
                        selected = p
                        break
                except Exception:
                    continue
            return selected
        return None

    def get_order(self, symbol: str, order_id: int) -> dict:
        return self._signed_get(
            "/fapi/v1/order",
            {"symbol": symbol.upper(), "orderId": int(order_id)},
        )

    def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> dict:
        return self._signed_get(
            "/fapi/v1/order",
            {"symbol": symbol.upper(), "origClientOrderId": str(client_order_id)},
        )

    # ---------------- CONNECTIVITY TEST (Consolidated from wrapper) ----------------

    def test_connection(self) -> dict:
        """Test API connection by fetching account info"""
        try:
            # Try to fetch account data
            account_data = self.account()
            
            if account_data and isinstance(account_data, dict):
                # Check what capabilities the API key has
                capabilities = ["read"]
                
                # If we can get account data, we have read access
                # Check for additional permissions
                try:
                    balance = self.account_balance()
                    if balance:
                        capabilities.append("balance")
                except:
                    pass
                
                # Assume trading capabilities if account fetch succeeded
                # (a more robust check would attempt a test order in test mode)
                capabilities.extend(["trade", "futures"])
                
                return {
                    "success": True,
                    "message": "Connection successful",
                    "account_type": "Binance Futures",
                    "capabilities": capabilities
                }
            else:
                return {
                    "success": False,
                    "error": "Invalid response from Binance API"
                }
                
        except Exception as e:
            error_msg = str(e)
            
            # Parse common errors
            if "Invalid API-key" in error_msg or "API-key format invalid" in error_msg:
                return {"success": False, "error": "Invalid API key or secret"}
            elif "Signature for this request is not valid" in error_msg:
                return {"success": False, "error": "Invalid API secret (signature mismatch)"}
            elif "IP address not authorized" in error_msg:
                return {"success": False, "error": "IP address not whitelisted"}
            elif "timestamp" in error_msg.lower():
                return {"success": False, "error": "Server time synchronization issue"}
            else:
                return {"success": False, "error": f"Connection failed: {error_msg}"}
