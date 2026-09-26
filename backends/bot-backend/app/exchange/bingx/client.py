from __future__ import annotations
import time
import requests
import json
from typing import Dict, Any, Optional, List
from app.exchange.bingx.signing import sign_bingx, get_timestamp
from shared_lib.core.security.redaction import redact_text as _redact
from app.models.unified_trading import SymbolFilters
from app.exchange.binance.filters import extract_filters

class BingXClient:
    """
    BingX V2 API Client (USDT-M Futures/Swap).
    """
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, base_url: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        
        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            # Canonical URL table: DEMO -> VST host, LIVE -> mainnet. `testnet`
            # used to be ignored, silently validating demo accounts on mainnet.
            from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
            self.base_url = resolve_base_url("bingx", BrokerEnvironment.DEMO if testnet else BrokerEnvironment.LIVE)

    def _normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol to BingX format: AVAXUSDT -> AVAX-USDT
        BingX requires symbols to end with -USDT or -USDC.
        """
        symbol = symbol.upper().strip()
        
        # If already has hyphen, return as-is
        if '-' in symbol:
            return symbol
        
        # Common quote currencies
        for quote in ['USDT', 'USDC', 'USD']:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                return f"{base}-{quote}"
        
        # If no recognized quote currency, assume USDT
        return f"{symbol}-USDT"

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        payload = payload or {}
        
        # 1. Add Timestamp
        payload["timestamp"] = str(get_timestamp())
        
        # 2. Sort and Stringify for Signature
        # Filter None/Empty
        filtered = {k: v for k, v in payload.items() if v is not None}
        sorted_items = sorted(filtered.items())
        query_string = "&".join([f"{k}={v}" for k, v in sorted_items])
        
        # 3. Sign
        signature = sign_bingx(self.api_secret, query_string)
        final_query = f"{query_string}&signature={signature}"
        
        headers = {
            "X-BX-APIKEY": self.api_key,
        }
        
        # 4. Execute
        try:
            if method == "GET":
                full_url = f"{url}?{final_query}"
                r = requests.get(full_url, headers=headers, timeout=10)
            elif method == "POST":
                # Using form-urlencoded for compatibility
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                r = requests.post(url, data=final_query, headers=headers, timeout=10)
            elif method == "DELETE":
                full_url = f"{url}?{final_query}"
                r = requests.delete(full_url, headers=headers, timeout=10)
            else:
                raise ValueError(f"Method {method} not supported")
                
            if r.status_code >= 400:
                raise RuntimeError(f"BingX HTTP {r.status_code}: {r.text}")
                
            data = r.json()
            
            # Check BingX Business Code
            # code=0 means success
            if data.get("code") != 0:
                msg = data.get("msg", "Unknown error")
                raise RuntimeError(f"BingX API Error: {msg} (Code {data.get('code')})")
                
            return data
            
        except requests.RequestException as e:
            raise RuntimeError(_redact(f"BingX Network Error: {e}"))

    # ------------------ LEGACY / FACTORY INTERFACE ------------------

    def test_connection(self) -> Dict[str, Any]:
        """Test API connection using balance endpoint."""
        try:
            self.account()
            return {
                "success": True,
                "message": "Connection successful",
                "account_type": "BingX Futures",
                "capabilities": ["read", "trade", "futures"]
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def ping(self) -> bool:
        try:
            self.server_time()
            return True
        except:
            return False

    def server_time(self) -> int:
        """Get server time in milliseconds."""
        data = self._request("GET", "/openApi/swap/v2/server/time")
        # BingX returns {"data": {"serverTime": 123...}, "code": 0}
        return int(data["data"]["serverTime"])

    # ------------------ MARKET DATA ------------------

    def last_price(self, symbol: str) -> float:
        """Get last traded price."""
        # /openApi/swap/v2/quote/ticker?symbol=BTC-USDT
        data = self._request("GET", "/openApi/swap/v2/quote/ticker", {"symbol": self._normalize_symbol(symbol)})
        # Response: {"data": {"symbol": "...", "lastPrice": "...", ...}}
        return float(data["data"]["lastPrice"])

    def exchange_info_cached(self) -> dict:
        """Binance-shaped filters keyed by the RUNTIME symbol (BTCUSDT), cached
        5 minutes. (Previously keyed by BTC-USDT, so the runtime's filter
        lookups silently fell back to empty filters.)"""
        cache = getattr(self, "_ei_cache", None)
        if cache is not None and time.time() - cache[0] < self._CACHE_TTL_S:
            return cache[1]
        symbols = []
        for ins in self.discover_instruments():
            if not ins.api_tradable:
                continue
            filters = [{"filterType": "PRICE_FILTER", "tickSize": format(ins.tick_size or 0, "f")},
                       {"filterType": "LOT_SIZE", "stepSize": format(ins.qty_step or 0, "f"),
                        "minQty": format(ins.min_qty or ins.qty_step or 0, "f"), "maxQty": "0"}]
            if ins.min_notional:
                filters.append({"filterType": "MIN_NOTIONAL", "notional": str(ins.min_notional)})
            symbols.append({"symbol": self._runtime_symbol(ins.venue_symbol), "filters": filters, "status": "TRADING",
                            "baseAsset": ins.base_currency, "quoteAsset": ins.settlement_asset})
        info = {"symbols": symbols}
        self._ei_cache = (time.time(), info)
        return info

    def klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> list:
        """
        Get klines. 
        Returns: [open_time, open, high, low, close, volume] (Ascending)
        """
        # Map intervals. BingX: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M
        # Matches standardized strings mostly.
        
        # Uses V3 for klines usually recommmended/more robust? Or V2.
        # Let's try V3 if accessible, else V2. 
        # API Notes said V3 for klines. /openApi/swap/v3/quote/klines
        
        data = self._request("GET", "/openApi/swap/v3/quote/klines", {
            "symbol": self._normalize_symbol(symbol),
            "interval": interval,
            "limit": limit
        })
        # Response: {"data": [{"open":.., "close":.., "high":.., "low":.., "volume":.., "time":..}, ...]}
        # List is usually descending in BingX? Need to check.
        # "The returned data is sorted in descending order of time" (Common)
        # Let's verify sort.
        
        raw_list = data.get("data", [])
        result = []
        for k in raw_list:
            result.append([
                k["time"],
                float(k["open"]),
                float(k["high"]),
                float(k["low"]),
                float(k["close"]),
                float(k["volume"])
            ])
            
        # Ensure Ascending
        if len(result) > 1 and result[0][0] > result[-1][0]:
            result.reverse()
            
        return result

    # ------------------ ACCOUNT & POSITIONS ------------------

    def account(self) -> dict:
        """
        Get account balance formatted like Binance keys.
        """
        data = self._request("GET", "/openApi/swap/v2/user/balance")
        # Response: {"data": {"balance": {"balance": 100, "equity": 100, "unrealisedPL": 0, "availableMargin": 90...}}}
        # Check structure carefully. Returns "data": { "balance": ..., "asset": "USDT" }?
        # Actually usually returns { "balance": { "totalWalletBalance": ... } } no..
        # BingX structure: {"code":0, "data": { "balance": { "userId":..., "balance":..., "equity":..., "unrealisedPNL":..., "availableMargin":... } } }
        
        bal_data = data.get("data", {}).get("balance", {})
        
        return {
            "totalWalletBalance": float(bal_data.get("balance", 0.0)),
            "totalMarginBalance": float(bal_data.get("equity", 0.0)),
            "availableBalance": float(bal_data.get("availableMargin", 0.0)),
            "totalUnrealizedProfit": float(bal_data.get("unrealisedPNL", 0.0))
        }

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
            "margin_used": 0.0,  # BingX doesn't expose margin directly
            "currency": "USDT",
            "raw": acc
        }

    # ------------------ WALLET / INTERNAL TRANSFER ------------------
    # Moves between THIS account's own wallets only (FUND <-> PFUTURES).
    # There is deliberately NO withdrawal method on this client.

    def asset_transfer(self, transfer_type: str, asset: str, amount: str) -> dict:
        """POST /openApi/api/v3/post/asset/transfer (type e.g. FUND_PFUTURES)."""
        return self._request("POST", "/openApi/api/v3/post/asset/transfer",
                             {"type": transfer_type, "asset": asset.upper(), "amount": str(amount)})

    def asset_transfer_history(self, transfer_type: str, start_time_ms: int | None = None,
                               end_time_ms: int | None = None, size: int = 100, current: int = 1) -> dict:
        return self._request("GET", "/openApi/api/v3/asset/transfer", {
            "type": transfer_type, "startTime": start_time_ms, "endTime": end_time_ms,
            "size": size, "current": current,
        })

    def get_transfers_history(self, start_time=None, end_time=None, limit=100, cursor=None) -> dict:
        """
        Get deposit/withdraw history from BingX.
        Normalized response for transfer tracking.
        """
        # BingX transfer endpoints not commonly exposed for swap accounts
        return {"items": [], "next_cursor": None}

    def position_risk(self, symbol: str | None = None) -> list:
        """
        Get position risk.
        BingX returns separate Long/Short positions. 
        We map them to signed amounts for 'One-Way' compatibility if possible, 
        or just list them as is (Hedge Mode).
        Bot standard: Signed amount. Long = +, Short = -.
        """
        payload = {}
        if symbol:
            payload["symbol"] = self._normalize_symbol(symbol)
            
        data = self._request("GET", "/openApi/swap/v2/user/positions", payload)
        # Response: {"data": [ { "symbol": "BTC-USDT", "positionAmt": "0.1", "positionSide": "LONG", ... } ]}
        
        raw_list = data.get("data", [])
        if not raw_list:
             return []
             
        remapped = []
        for p in raw_list:
            raw_amt = float(p.get("positionAmt", 0.0))
            side_str = p.get("positionSide", "LONG") # LONG or SHORT
            
            # Sign the amount
            final_amt = raw_amt if side_str == "LONG" else -raw_amt
            
            remapped.append({
                "symbol": self._runtime_symbol(p["symbol"]),
                "positionAmt": final_amt,
                "entryPrice": float(p.get("avgPrice", 0.0)),
                "unRealizedProfit": float(p.get("unrealisedPNL", 0.0)),
                "leverage": p.get("leverage", "1"),
                "liquidationPrice": float(p.get("liquidationPrice", 0.0)),
                "marginType": "isolated" if p.get("marginMode") == "ISOLATED" else "cross",
                "bingx_side": side_str
            })
            
        return remapped

    def get_position_amt(self, symbol: str) -> float:
        """Get net position amount."""
        risks = self.position_risk(symbol)
        total = 0.0
        for r in risks:
            total += r["positionAmt"]
        return total

    def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        """
        Get standardized sizing filters.
        """
        try:
            info = self.exchange_info_cached()
            return extract_filters(info, symbol)
        except Exception:
            return SymbolFilters()

    # ------------------ TRADING ------------------

    def place_market_order(self, symbol: str, side: str, quantity: float) -> dict:
        """
        Place Market Order.
        """
        side = side.upper() # BUY/SELL
        symbol_normalized = self._normalize_symbol(symbol)
        
        payload = {
            "symbol": symbol_normalized,
            "side": side,
            "type": "MARKET",
            "quantity": str(quantity),
            "reduceOnly": "false"
        }
        
        res = self._request("POST", "/openApi/swap/v2/trade/order", payload)
        
        # Response: {"data": {"orderId": 123, ...}}
        order_data = res.get("data", {})
        
        return {
            "orderId": str(order_data.get("orderId", "")),
            "avgPrice": 0.0, # Async
            "status": "NEW",
            "symbol": symbol,
            "executedQty": "0.0",
            "origQty": str(quantity),
            "side": side,
            "type": "MARKET"
        }

    def close_position_market(self, symbol: str) -> dict:
        """
        Close entire position keying off current size.
        """
        # 1. Get current size
        net_amt = self.get_position_amt(symbol)
        if net_amt == 0:
            return {"status": "FLAT", "orderId": "0", "avgPrice": 0.0}
            
        # 2. Determine Close Side
        side = "SELL" if net_amt > 0 else "BUY"
        qty_str = str(abs(net_amt))
        
        # 3. Reduce Only Order
        payload = {
            "symbol": self._normalize_symbol(symbol),
            "side": side,
            "type": "MARKET",
            "quantity": qty_str,
            "reduceOnly": "true"
        }
        
        res = self._request("POST", "/openApi/swap/v2/trade/order", payload)
        
        order_data = res.get("data", {})
        return {
            "orderId": str(order_data.get("orderId", "")),
            "avgPrice": 0.0,
            "status": "NEW",
            "symbol": symbol,
            "side": side,
            "type": "MARKET_CLOSE",
            "executedQty": "0.0"
        }

    def cancel_all_orders(self, symbol: str) -> dict:
        """Cancel all open orders for symbol."""
        # /openApi/swap/v2/trade/allOpenOrders (DELETE)
        return self._request("DELETE", "/openApi/swap/v2/trade/allOpenOrders", {
            "symbol": self._normalize_symbol(symbol)
        })

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        """Set leverage."""
        # /openApi/swap/v2/trade/leverage
        # Params: symbol, leverage, side="LONG" or "SHORT"
        # Since we use One-Way usually, we might need to set for BOTH or just one?
        # BingX requires setting for Long and Short separately in Hedge mode.
        # Safest is to set for Both.
        
        symbol_normalized = self._normalize_symbol(symbol)
        try:
            # Set LONG
            self._request("POST", "/openApi/swap/v2/trade/leverage", {
                "symbol": symbol_normalized,
                "leverage": str(leverage),
                "side": "LONG"
            })
            # Set SHORT
            self._request("POST", "/openApi/swap/v2/trade/leverage", {
                "symbol": symbol_normalized,
                "leverage": str(leverage),
                "side": "SHORT"
            })
            return {"status": "ok"}
        except Exception as e:
            # If "already set", ignore.
            if "already" in str(e).lower():
                return {"status": "ok_ignored"}
            raise e

    def place_stop_market(self, symbol: str, side: str, stop_price: float, reduce_only: bool = True) -> dict:
        """
        Place Stop Loss Market Order.
        In BingX, we use TRIGGER orders.
        endpoint: /openApi/swap/v2/trade/order
        type: STOP_MARKET or TAKE_PROFIT_MARKET?
        Actually standard 'type'="STOP_MARKET" exists.
        Requires 'stopPrice'.
        """
        side = side.upper()
        
        # Note: In most bots, "side" passed here is the ORDER side (Exit side).
        # e.g. If long, we call place_stop_market(side="SELL").
        
        symbol_normalized = self._normalize_symbol(symbol)
        payload = {
            "symbol": symbol_normalized,
            "side": side,
            "type": "STOP_MARKET",
            "stopPrice": str(stop_price),
            "quantity": "0", # Close position? 
            # BingX requires quantity AND reduceOnly=true typically?
            # Or allows "close all"? 
            # If we don't know the exact qty, we might have issues.
            # But the caller usually doesn't pass qty for full protection?
            # Wait, contract says `place_stop_market(symbol, side, stop_price, qty)` in some versions.
            # My contract in Phase 1 analysis had `place_stop_market(..., reduce_only)`.
            # If no qty is passed, how do we close?
            # Existing Bybit implementation used `closeOnTrigger=True` to close all.
            # BingX might not have `closeOnTrigger`.
            # We might need to fetch position size.
        }
        
        # Fetch position size to be safe
        # (This adds latency, but ensures correctness)
        # Optimization: Pass Quantity if possible, but signature doesn't require it? 
        # The executor usually handles "full size".
        # Let's check if BingX supports "close position" trigger.
        # Some docs mention `workingType` etc.
        # I'll implement "fetch size" strategy for robustness.
        
        net_amt = self.get_position_amt(symbol)
        qty = abs(net_amt)
        if qty == 0:
             # Just place dummy or return?
             # Raising error might be better to signal "Nothing to protect".
             return {"status": "SKIPPED_NO_POS"}

        payload["quantity"] = str(qty)
        payload["reduceOnly"] = "true"
        
        res = self._request("POST", "/openApi/swap/v2/trade/order", payload)
        order_data = res.get("data", {})
        
        return {
            "orderId": str(order_data.get("orderId", "")),
            "status": "NEW",
            "type": "STOP_MARKET",
            "stopPrice": float(stop_price)
        }

    def place_take_profit_market(self, symbol: str, side: str, stop_price: float) -> dict:
        """Place Take Profit Market Order."""
        side = side.upper()
        
        # Similar to Stop Market, fetch size.
        net_amt = self.get_position_amt(symbol)
        qty = abs(net_amt)
        if qty == 0:
             return {"status": "SKIPPED_NO_POS"}
             
        payload = {
            "symbol": self._normalize_symbol(symbol),
            "side": side,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": str(stop_price),
            "quantity": str(qty),
            "reduceOnly": "true"
        }
        
        res = self._request("POST", "/openApi/swap/v2/trade/order", payload)
        order_data = res.get("data", {})
        
        return {
            "orderId": str(order_data.get("orderId", "")),
            "status": "NEW",
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": float(stop_price)
        }

    # ================== EXECUTION PARITY (canonical contract) ==================
    # Outward symbols use the runtime's concatenated form (BTCUSDT); the
    # venue's hyphenated form (BTC-USDT) stays inside this client. Our
    # client_order_id is sent as ``clientOrderID`` (lookup key when the
    # submission outcome is unknown).

    _STATUS_MAP = {"NEW": "NEW", "PENDING": "NEW", "PARTIALLY_FILLED": "PARTIALLY_FILLED", "FILLED": "FILLED",
                   "CANCELED": "CANCELED", "CANCELLED": "CANCELED", "FAILED": "REJECTED", "EXPIRED": "EXPIRED"}
    _CACHE_TTL_S = 300

    @staticmethod
    def _runtime_symbol(symbol: str) -> str:
        return str(symbol or "").upper().replace("-", "")

    def _fmt_qty(self, symbol: str, qty) -> str:
        from decimal import Decimal, ROUND_DOWN
        q = Decimal(str(qty))
        try:
            step = Decimal(str(self.get_symbol_filters(symbol).step_size or 0))
        except Exception:
            step = Decimal("0")
        if step > 0:
            q = (q / step).to_integral_value(rounding=ROUND_DOWN) * step
        return format(q.normalize(), "f")

    def _order_view(self, o: dict) -> dict:
        return {
            "symbol": self._runtime_symbol(o.get("symbol")), "orderId": str(o.get("orderId", "")) or None,
            "clientOrderId": o.get("clientOrderId") or o.get("clientOrderID") or None,
            "status": self._STATUS_MAP.get(str(o.get("status", "")).upper()),
            "executedQty": o.get("executedQty") or "0", "origQty": o.get("origQty"),
            "avgPrice": o.get("avgPrice") or "0", "side": str(o.get("side", "")).upper(),
            "type": str(o.get("type", "")).upper(), "stopPrice": o.get("stopPrice") or None,
            "reduceOnly": str(o.get("reduceOnly", "")).lower() == "true",
            "updateTime": int(o.get("updateTime") or o.get("time") or 0),
        }

    def place_order(self, req):
        from decimal import Decimal
        from app.models.unified_trading import OrderStatus, UnifiedOrder
        if req.leverage:
            self.set_leverage(req.symbol, int(req.leverage))
        market = str(req.type.value if hasattr(req.type, "value") else req.type).upper() == "MARKET"
        payload = {"symbol": self._normalize_symbol(req.symbol), "side": req.side.value.upper(),
                   "positionSide": "BOTH", "type": "MARKET" if market else "LIMIT",
                   "quantity": self._fmt_qty(req.symbol, req.qty)}
        if not market:
            payload["price"] = str(req.price)
        if getattr(req, "client_order_id", None):
            payload["clientOrderID"] = str(req.client_order_id)[:40]
        if req.reduce_only:
            payload["reduceOnly"] = "true"
        res = self._request("POST", "/openApi/swap/v2/trade/order", payload)
        order = (res.get("data") or {}).get("order") or res.get("data") or {}
        view = self._order_view(order)
        status = {"FILLED": OrderStatus.FILLED, "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
                  "CANCELED": OrderStatus.CANCELED, "REJECTED": OrderStatus.REJECTED}.get(view["status"], OrderStatus.NEW)
        return UnifiedOrder(
            client_order_id=str(view["clientOrderId"] or payload.get("clientOrderID") or ""),
            broker_order_id=str(view["orderId"] or ""), symbol=req.symbol, side=req.side, type=req.type,
            qty_ordered=req.qty, qty_filled=Decimal(str(view["executedQty"] or 0)),
            avg_fill_price=Decimal(str(view["avgPrice"] or 0)), status=status, timestamp=int(time.time() * 1000),
            reduce_only=req.reduce_only)

    def _query_order(self, symbol: str, **ident) -> dict:
        res = self._request("GET", "/openApi/swap/v2/trade/order", {"symbol": self._normalize_symbol(symbol), **ident})
        order = (res.get("data") or {}).get("order") or {}
        return self._order_view(order) if order else {}

    def get_order(self, symbol: str, order_id) -> dict:
        return self._query_order(symbol, orderId=str(order_id))

    def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> dict:
        return self._query_order(symbol, clientOrderId=str(client_order_id))

    def open_orders(self, symbol: str | None = None) -> list:
        params = {"symbol": self._normalize_symbol(symbol)} if symbol else {}
        res = self._request("GET", "/openApi/swap/v2/trade/openOrders", params)
        return [self._order_view(o) for o in (res.get("data") or {}).get("orders") or []]

    def get_open_orders(self, symbol: str | None = None) -> list:
        return self.open_orders(symbol)

    def get_algo_orders(self, symbol: str, raise_on_error: bool = False) -> list:
        """BingX lists STOP_MARKET / TAKE_PROFIT_MARKET in openOrders already."""
        return []

    def cancel_order(self, symbol: str, order_id) -> bool:
        try:
            self._request("DELETE", "/openApi/swap/v2/trade/order", {"symbol": self._normalize_symbol(symbol),
                                                                       "orderId": str(order_id)})
            return True
        except RuntimeError:
            return False

    def cancel_all(self, symbol: str) -> dict:
        return self.cancel_all_orders(symbol)

    def user_trades(self, symbol: str, start_time_ms: int | None = None, end_time_ms: int | None = None,
                    limit: int = 100) -> list:
        now = int(time.time() * 1000)
        res = self._request("GET", "/openApi/swap/v2/trade/allFillOrders", {
            "symbol": self._normalize_symbol(symbol), "tradingUnit": "COIN",
            "startTs": start_time_ms or now - 7 * 86_400_000, "endTs": end_time_ms or now})
        rows = (res.get("data") or {}).get("fill_orders") or (res.get("data") or {}).get("fillOrders") or []
        return [{"symbol": self._runtime_symbol(f.get("symbol")), "orderId": str(f.get("orderId", "")),
                 "id": f.get("tradeId") or f.get("filledTm"), "qty": f.get("volume") or f.get("qty"),
                 "price": f.get("price"), "commission": abs(float(f.get("commission") or 0)),
                 "commissionAsset": f.get("currency") or "USDT", "side": str(f.get("side", "")).upper()}
                for f in rows]

    def position_risk_all(self) -> list:
        return self.position_risk()

    def get_position_info(self, symbol: str) -> dict | None:
        rows = self.position_risk(symbol)
        for r in rows:
            if abs(float(r.get("positionAmt") or 0)) > 0:
                return r
        return rows[0] if rows else None

    def get_positions(self):
        from decimal import Decimal
        from app.models.unified_trading import PositionMode, Side, UnifiedPosition
        out = []
        for p in self.position_risk():
            amt = Decimal(str(p.get("positionAmt") or 0))
            if amt == 0:
                continue
            out.append(UnifiedPosition(
                symbol=p["symbol"], broker_id="bingx", side=Side.BUY if amt > 0 else Side.SELL, quantity=abs(amt),
                entry_price=Decimal(str(p.get("entryPrice") or 0)), current_price=Decimal(str(p.get("entryPrice") or 0)),
                unrealized_pnl=Decimal(str(p.get("unRealizedProfit") or 0)), realized_pnl=Decimal("0"),
                margin_used=Decimal("0"), leverage=Decimal(str(p.get("leverage") or 1)), mode=PositionMode.ONE_WAY,
                timestamp=int(time.time() * 1000)))
        return out

    def place_protection(self, req):
        from app.models.unified_trading import ProtectionResult, Side
        result = ProtectionResult(status="initiated")
        exit_side = "SELL" if req.position_side == Side.BUY else "BUY"
        try:
            if req.sl_price:
                r = self.place_stop_market(req.symbol, exit_side, float(req.sl_price), reduce_only=True)
                if not r.get("orderId"):
                    raise RuntimeError(f"SL not placed: {r.get('status')}")
                result.sl_order_id = r["orderId"]
            if req.tp_price:
                r = self.place_take_profit_market(req.symbol, exit_side, float(req.tp_price))
                if not r.get("orderId"):
                    raise RuntimeError(f"TP not placed: {r.get('status')}")
                result.tp_order_id = r["orderId"]
            result.status = "success"
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
        return result

    def update_protection(self, req) -> dict:
        """Place-new-then-cancel-old: the new stop exists BEFORE the old one
        is removed, so the position is never without a stop. Raises if the
        new stop cannot be placed (old one is kept)."""
        side = "SELL" if str(getattr(req, "position_side", "LONG")).upper() == "LONG" else "BUY"
        new_sl = new_tp = None
        if getattr(req, "new_sl_price", None) is not None:
            r = self.place_stop_market(req.symbol, side, float(req.new_sl_price), reduce_only=True)
            new_sl = r.get("orderId")
            if not new_sl:
                raise RuntimeError(f"[SEV1-S5] update_protection: new SL not placed for {req.symbol}")
            if getattr(req, "old_sl_order_id", None):
                self.cancel_order(req.symbol, req.old_sl_order_id)
        if getattr(req, "new_tp_price", None) is not None:
            r = self.place_take_profit_market(req.symbol, side, float(req.new_tp_price))
            new_tp = r.get("orderId")
            if new_tp and getattr(req, "old_tp_order_id", None):
                self.cancel_order(req.symbol, req.old_tp_order_id)
        return {"sl_order_id": new_sl or getattr(req, "old_sl_order_id", None),
                "tp_order_id": new_tp or getattr(req, "old_tp_order_id", None), "status": "OK", "error": None}

    def get_prices(self, symbols: List[str]) -> Dict[str, float]:
        res = self._request("GET", "/openApi/swap/v2/quote/price", {})
        wanted = {s.upper() for s in symbols} if symbols else None
        out = {}
        for t in res.get("data") or []:
            sym = self._runtime_symbol(t.get("symbol"))
            if t.get("price") and (wanted is None or sym in wanted):
                out[sym] = float(t["price"])
        return out

    def get_ticker(self, symbol: str) -> dict:
        d = self._request("GET", "/openApi/swap/v2/quote/ticker", {"symbol": self._normalize_symbol(symbol)}).get("data") or {}
        return {"symbol": self._runtime_symbol(symbol), "lastPrice": d.get("lastPrice"), "bidPrice": d.get("bidPrice"),
                "askPrice": d.get("askPrice"), "volume24h": d.get("volume"), "turnover24h": d.get("quoteVolume")}

    def get_orderbook(self, symbol: str, limit: int = 50) -> dict:
        d = self._request("GET", "/openApi/swap/v2/quote/depth", {"symbol": self._normalize_symbol(symbol),
                                                                   "limit": limit}).get("data") or {}
        return {"bids": [[float(p), float(q)] for p, q in d.get("bids") or []],
                "asks": [[float(p), float(q)] for p, q in d.get("asks") or []], "time": int(d.get("T") or 0)}

    def get_funding(self, symbol: str) -> dict:
        d = self._request("GET", "/openApi/swap/v2/quote/premiumIndex", {"symbol": self._normalize_symbol(symbol)}).get("data") or {}
        if isinstance(d, list):
            d = d[0] if d else {}
        return {"symbol": self._runtime_symbol(symbol), "fundingRate": d.get("lastFundingRate"),
                "nextFundingTime": d.get("nextFundingTime"), "markPrice": d.get("markPrice"),
                "indexPrice": d.get("indexPrice"), "fundingIntervalHours": d.get("fundingIntervalHours")}

    def exchange_info(self) -> dict:
        return self.exchange_info_cached()

    def discover_instruments(self) -> list:
        from app.exchange.instruments import parse_bingx_contract
        data = self._request("GET", "/openApi/swap/v2/quote/contracts")
        return [i for i in (parse_bingx_contract(c) for c in data.get("data") or []) if i is not None]

    def list_instruments(self):
        out = []
        for i in self.discover_instruments():
            spec = i.to_instrument_spec("bingx")
            out.append(spec.model_copy(update={"symbol_canonical": self._runtime_symbol(i.venue_symbol)}))
        return out

    def get_instrument(self, symbol: str):
        want = self._runtime_symbol(symbol)
        return next((i for i in self.discover_instruments() if self._runtime_symbol(i.venue_symbol) == want), None)

    def get_balance(self) -> dict:
        from decimal import Decimal
        a = self.account()
        return {"wallet": Decimal(str(a["totalWalletBalance"])), "equity": Decimal(str(a["totalMarginBalance"])),
                "available": Decimal(str(a["availableBalance"]))}

    def get_account_permissions(self) -> dict:
        from shared_lib.broker.permissions import unverified
        return unverified("bingx", "bingx:not-inspectable",
                          note="BingX key permissions are not API-inspectable here").to_dict()

    def get_account_capabilities(self, environment: str = "live") -> dict:
        from shared_lib.broker.capabilities import declared_profile
        return declared_profile("bingx").for_account(self.get_account_permissions().get("permissions")).to_dict()

    def get_trading_fee_rates(self, symbol: str) -> dict:
        """BingX swap account commission; unavailable fields stay None."""
        data = self._request("GET", "/openApi/swap/v2/user/commissionRate").get("data") or {}
        rates = data.get("commission") or {}
        return {"maker": rates.get("makerCommissionRate"), "taker": rates.get("takerCommissionRate")}
