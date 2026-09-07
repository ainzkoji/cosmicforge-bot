from __future__ import annotations
import time
import requests
import json
from typing import Dict, Any, Optional, List
from app.exchange.bingx.signing import sign_bingx, get_timestamp
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
            # Default to Mainnet. VST (Demo) requires specific VST credentials and URL.
            self.base_url = "https://open-api.bingx.com"

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
            raise RuntimeError(f"BingX Network Error: {str(e)}")

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
        """
        Get exchange info (contracts).
        Normalized to Binance format: { "symbols": [ { "symbol": "...", "filters": [...] } ] }
        """
        data = self._request("GET", "/openApi/swap/v2/quote/contracts")
        # Response: {"data": [{"symbol": "BTC-USDT", "pricePrecision": 2, ...}]}
        
        symbols = []
        for item in data.get("data", []):
            # Only enabled contracts? status: 1=Online
            if item.get("status") != 1:
                continue
                
            # Filter Construction
            price_prec = item.get("pricePrecision", 2)
            qty_step = item.get("stepSize", "0.0001") # stepSize or minQty?
            
            # Tick size estimation: 1 / 10^price_prec
            tick_size = f"{10**-price_prec:.{price_prec}f}"
            
            bin_filters = [
                {
                    "filterType": "PRICE_FILTER",
                    "tickSize": tick_size
                },
                {
                    "filterType": "LOT_SIZE",
                    "stepSize": str(qty_step),
                    "minQty": str(item.get("minQty", qty_step)), # Fallback
                    "maxQty": str(item.get("maxQty", "1000000"))
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "notional": item.get("minNotional", "5.0")
                }
            ]
            
            symbols.append({
                "symbol": item["symbol"],
                "filters": bin_filters,
                "status": "TRADING",
                "baseAsset": item.get("asset", ""),  # e.g. BTC
                "quoteAsset": item.get("currency", "USDT") # e.g. USDT
            })
            
        return {"symbols": symbols}

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
                "symbol": p["symbol"],
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
