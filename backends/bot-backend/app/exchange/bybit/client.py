from __future__ import annotations
import time
import requests
import json
from typing import Dict, Any, Optional, List
from app.exchange.bybit.signing import sign_v5, sign_legacy_v2
from app.models.unified_trading import SymbolFilters
from app.exchange.binance.filters import extract_filters

class BybitClient:
    """
    Bybit V5 API Client
    Backward compatible with legacy 'test_connection' used by broker_service.
    Duck-typed to match BinanceFuturesClient surface area.
    """
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, base_url: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Base URL logic matching config logic
        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
            
        self.recv_window = 5000
    
    def _request_v5(self, method: str, path: str, payload: dict | None = None) -> dict:
        """
        Execute V5 Signed Request
        """
        url = f"{self.base_url}{path}"
        payload = payload or {}
        timestamp = str(int(time.time() * 1000))
        
        # Prepare payload string for signing
        if method == "GET":
            # For GET, payload is query params string
            # Sort explicitly to match Bybit signing requirement
            # Filter out None values
            filtered_payload = {k: v for k, v in payload.items() if v is not None}
            payload_str = '&'.join([f"{k}={v}" for k, v in sorted(filtered_payload.items())])
            full_url = f"{url}?{payload_str}" if payload_str else url
        else:
            # For POST, payload is JSON string
            payload_str = json.dumps(payload)
            full_url = url
            
        signature = sign_v5(self.api_secret, payload_str, timestamp, self.api_key, self.recv_window)
        
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": str(self.recv_window),
            "Content-Type": "application/json"
        }
        
        if method == "GET":
            r = requests.get(full_url, headers=headers, timeout=10)
        elif method == "POST":
            r = requests.post(full_url, data=payload_str, headers=headers, timeout=10)
        else:
            raise ValueError(f"Method {method} not supported")
            
        if r.status_code >= 400:
            # Try to parse error from body
            try:
                err_body = r.json()
                msg = err_body.get("retMsg", r.text)
            except:
                msg = r.text
            raise RuntimeError(f"Bybit HTTP {r.status_code}: {msg}")
            
        return r.json()

    # ------------------ LEGACY INTERFACE (Required by broker_service.py) ------------------

    def test_connection(self) -> Dict[str, Any]:
        """
        Test API connection.
        """
        try:
            # Try V5 first
            data = self._request_v5("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"})
            
            # V5 success check
            if data.get("retCode") == 0:
                 return {
                    "success": True,
                    "message": "Connection successful",
                    "account_type": "ByBit V5",
                    "capabilities": ["read", "trade", "futures", "spot", "unified"]
                }
            else:
                 return {
                    "success": False,
                    "error": data.get("retMsg", "Unknown error")
                }

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ------------------ DUCK-TYPED INTERFACE (Binance Compatibility) ------------------

    def last_price(self, symbol: str) -> float:
        """Get last traded price."""
        data = self._request_v5("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol.upper()})
        if data["retCode"] != 0 or not data["result"]["list"]:
             # Fallback or error?
             # If symbol is invalid, Bybit returns empty list or error.
             # Duck typing expects float or error.
            raise RuntimeError(f"Bybit price error: {data.get('retMsg')}")
        return float(data["result"]["list"][0]["lastPrice"])

    def account(self) -> dict:
        """
        Get account balance formatted like Binance keys.
        """
        data = self._request_v5("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        # Fallback to CONTRACT if UNIFIED returns nothing (older accounts)
        if not data.get("result", {}).get("list"):
            data = self._request_v5("GET", "/v5/account/wallet-balance", {"accountType": "CONTRACT"})
            
        if not data.get("result", {}).get("list"):
            return {"totalWalletBalance": 0.0, "totalMarginBalance": 0.0, "availableBalance": 0.0, "totalUnrealizedProfit": 0.0}

        # Bybit unified returns list of wallets (one per coin, or one consolidated)
        # For linear USDT trading, we look for USDT coin or the Unified Equity.
        # Unified account: totalEquity, totalWalletBalance, totalAvailableBalance are top-level or per coin?
        # V5 Unified: 'list' has 1 item.
        wallet = data["result"]["list"][0]
        
        # totalEquity is the most accurate "Net Asset Value"
        total_equity = float(wallet.get("totalEquity", 0.0)) 
        
        # totalWalletBalance (excluding UPnl)
        total_wallet = float(wallet.get("totalWalletBalance", 0.0))
        
        # totalAvailableBalance (Margin Balance - Maint Margin) 
        # Note: Bybit 'totalAvailableBalance' might be what we want.
        total_avail = float(wallet.get("totalAvailableBalance", 0.0))
        
        return {
            "totalWalletBalance": total_wallet,
            "totalMarginBalance": total_equity,
            "availableBalance": total_avail, 
            "totalUnrealizedProfit": float(wallet.get("totalPerpUPL", 0.0)) 
        }

    def get_account_snapshot(self) -> dict:
        """
        Get normalized account snapshot for equity tracking.
        """
        acc = self.account()
        return {
            "wallet_balance": float(acc.get("totalWalletBalance", 0.0)),
            "equity": float(acc.get("totalMarginBalance", 0.0)),
            "available_balance": float(acc.get("availableBalance", 0.0)),
            "unrealized_pnl": float(acc.get("totalUnrealizedProfit", 0.0)),
            "margin_used": 0.0,  # Bybit doesn't expose this directly in simple format
            "currency": "USDT",
            "raw": acc
        }

    def get_transfers_history(self, start_time=None, end_time=None, limit=100, cursor=None) -> dict:
        """
        Get deposit/withdraw history from Bybit V5.
        """
        # Bybit V5: /v5/asset/deposit/query-record and /v5/asset/withdraw/query-record
        # For now return empty, can implement if API access is available
        return {"items": [], "next_cursor": None}

    def position_risk(self, symbol: str | None = None) -> list:
        """
        Get position risk. If symbol provided, returns list with one item (Binance style).
        """
        params = {"category": "linear", "settleCoin": "USDT"}
        if symbol:
            params["symbol"] = symbol.upper()
            
        data = self._request_v5("GET", "/v5/position/list", params)
        if data["retCode"] != 0:
            return []
            
        remapped = []
        for p in data["result"]["list"]:
             # Calculate signed size for Binance compatibility
            size = float(p.get("size", 0.0))
            side = p.get("side", "")
            amt = size if side == "Buy" else -size
            
            remapped.append({
                "symbol": p["symbol"],
                "positionAmt": amt,
                "entryPrice": float(p.get("avgPrice", 0.0)),
                "unRealizedProfit": float(p.get("unrealisedPnl", 0.0)),
                "leverage": p.get("leverage", "1"),
                "liquidationPrice": p.get("liqPrice", 0.0),
                "marginType": "cross" if p.get("tradeMode", 0) == 0 else "isolated", # 0=Cross, 1=Isolated
                # Provide raw fields incase of debugging needs
                "bybit_side": side
            })
        return remapped

    def get_position_info(self, symbol: str) -> dict | None:
        """Get single position info dict."""
        risks = self.position_risk(symbol)
        return risks[0] if risks else None

    def get_position_amt(self, symbol: str) -> float:
        info = self.get_position_info(symbol)
        return float(info.get("positionAmt", 0.0)) if info else 0.0

    def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        """
        Get standardized sizing filters for a symbol.
        Reuses Binance filter extraction logic since we mimic the structure.
        """
        try:
            info = self.exchange_info_cached()
            return extract_filters(info, symbol)
        except Exception:
             # Fallback
            return SymbolFilters()

    def exchange_info_cached(self) -> dict:
        """
        Get exchange info (instruments).
        Returns Binance-compatible structure: { "symbols": [ { "symbol": "...", "filters": [...] } ] }
        """
        data = self._request_v5("GET", "/v5/market/instruments-info", {"category": "linear", "status": "Trading"})
        if data["retCode"] != 0:
            return {"symbols": []}
            
        symbols = []
        for inst in data["result"]["list"]:
            if inst["quoteCoin"] != "USDT": continue
            
            # Map Bybit filters to Binance filters
            price_filter = inst.get("priceFilter", {})
            lot_filter = inst.get("lotSizeFilter", {})
            
            # Extract real minNotional (default to 5.0 if missing)
            min_notional = lot_filter.get("minNotionalValue", "5.0")
            
            bin_filters = [
                {
                    "filterType": "PRICE_FILTER",
                    "tickSize": price_filter.get("tickSize", "0.01")
                },
                {
                    "filterType": "LOT_SIZE",
                    "stepSize": lot_filter.get("qtyStep", "0.001"),
                    "minQty": lot_filter.get("minOrderQty", "0.001"),
                    "maxQty": lot_filter.get("maxOrderQty", "1000000")
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "notional": str(min_notional)
                }
            ]
            
            symbols.append({
                "symbol": inst["symbol"],
                "filters": bin_filters,
                "status": "TRADING",
                "baseAsset": inst.get("baseCoin", ""),
                "quoteAsset": inst.get("quoteCoin", "")
            })
            
        return {"symbols": symbols}

    def klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> list:
        """
        Get klines. 
        Binance Format: [open_time, open, high, low, close, volume, ...]
        """
        # Map generic interval strings to Bybit enum
        i = "1"
        if interval == "1m": i = "1"
        elif interval == "3m": i = "3"
        elif interval == "5m": i = "5"
        elif interval == "15m": i = "15"
        elif interval == "30m": i = "30"
        elif interval == "1h": i = "60"
        elif interval == "2h": i = "120"
        elif interval == "4h": i = "240"
        elif interval == "6h": i = "360"
        elif interval == "12h": i = "720"
        elif interval == "1d": i = "D"
        elif interval == "1w": i = "W"
        elif interval == "1M": i = "M"
            
        data = self._request_v5("GET", "/v5/market/kline", {
            "category": "linear",
            "symbol": symbol.upper(),
            "interval": i,
            "limit": limit
        })
        
        if data["retCode"] != 0:
            return []
            
        # Reverse order to be Ascending (Time) like Binance
        raw = data["result"]["list"]
        raw.reverse()
        return raw

    def place_market_order(self, symbol: str, side: str, quantity: float) -> dict:
        """
        Place Market Order.
        Returns normalized dict including 'orderId', 'avgPrice' (if available).
        """
        side = side.capitalize() # "BUY" -> "Buy"
        qty_str = str(quantity)
        
        payload = {
            "category": "linear",
            "symbol": symbol.upper(),
            "side": side,
            "orderType": "Market",
            "qty": qty_str,
            "timeInForce": "GTC"
        }
        
        res = self._request_v5("POST", "/v5/order/create", payload)
        
        if res["retCode"] != 0:
             raise RuntimeError(f"Bybit Order Failed: {res.get('retMsg')}")
        
        result = res.get("result", {})
        return {
            "orderId": result.get("orderId", ""),
            "avgPrice": 0.0, # Async, usually 0.0 immediately
            "status": "NEW",
            "symbol": symbol,
            "executedQty": "0.0",
            "origQty": qty_str,
            "side": side.upper(),
            "type": "MARKET",
            "bybit_ret_code": res["retCode"]
        }

    def cancel_all_orders(self, symbol: str) -> dict:
        """Cancel all orders."""
        return self._request_v5("POST", "/v5/order/cancel-all", {
            "category": "linear",
            "symbol": symbol.upper()
        })
        
    def close_position_market(self, symbol: str) -> dict:
        """
        Close position by placing market order of full size.
        """
        # Fetch current size first (imperative for Bybit)
        amt = self.get_position_amt(symbol)
        if amt == 0:
            return {"status": "FLAT", "orderId": "0", "avgPrice": 0.0}
            
        side = "Sell" if amt > 0 else "Buy"
        qty_str = str(abs(amt))
        
        payload = {
            "category": "linear",
            "symbol": symbol.upper(),
            "side": side,
            "orderType": "Market",
            "qty": qty_str,
            "reduceOnly": True,
            "timeInForce": "IOC" # Immediate close
        }
        
        res = self._request_v5("POST", "/v5/order/create", payload)
        
        if res["retCode"] != 0:
             raise RuntimeError(f"Bybit Close Failed: {res.get('retMsg')}")
             
        result = res.get("result", {})
        return {
            "orderId": result.get("orderId", ""),
            "avgPrice": 0.0, 
            "status": "NEW",
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET_CLOSE",
            "executedQty": "0.0"
        }

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        """Set leverage."""
        try:
            return self._request_v5("POST", "/v5/position/set-leverage", {
                "category": "linear",
                "symbol": symbol.upper(),
                "buyLeverage": str(leverage),
                "sellLeverage": str(leverage)
            })
        except RuntimeError as e:
            # Ignore "leverage not modified" error
            if "not modified" in str(e):
                return {"retCode": 0, "msg": "already_set"}
            raise

    def place_stop_market(self, symbol: str, side: str, stop_price: float, reduce_only: bool = True) -> dict:
        """
        Place Stop Loss via Trading Stop (Position-attached).
        Returns normalized order-like dict.
        """
        # Note: 'side' passed here is usually the EXIT side (e.g. SELL if Long).
        # Bybit set-trading-stop applies to the position.
        # If we are Long, we set stopLoss.
        
        # We need to infer if we are setting for Buy or Sell position?
        # NOT RELIABLE only from 'side'.
        # However, Binance 'place_stop_market' is an ORDER.
        # If we use trading-stop, it persists on position.
        
        # Strategy: Use Conditional Order to act like a Stop-Market Order.
        # This matches Binance semantics better than set-trading-stop for one-off/scale-out.
        
        bybit_side = side.capitalize()
        
        payload = {
            "category": "linear",
            "symbol": symbol.upper(),
            "side": bybit_side,
            "orderType": "Market",
            "qty": "0", # To be filled? NO, Stop Order needs qty if it's an order.
            # But the 'executor' usually places SL for the FULL size?
            # Executor.py calls place_protection_orders -> _place_sl_with_retry...
            # The qty passed to place_protection_orders is the entry qty.
            # BUT _place_sl_with_retry uses closePosition=True (implies full close)?
            
            # If we want to support "Entire Position" stop:
            "triggerPrice": str(stop_price),
            "triggerDirection": 2 if bybit_side == "Sell" else 1, # Sell=Fall(2), Buy=Rise(1)
            "reduceOnly": True,
            "closeOnTrigger": True # IMPORTANT: closes entire position
        }
        
        # Note: closeOnTrigger=True ignores qty
        
        res = self._request_v5("POST", "/v5/order/create", payload)
        if res["retCode"] != 0:
            raise RuntimeError(f"Bybit SL Failed: {res.get('retMsg')}")
            
        result = res.get("result", {})
        return {
            "orderId": result.get("orderId", ""),
            "status": "NEW",
            "type": "STOP_MARKET",
            "stopPrice": float(stop_price),
            "bybit_link_id": result.get("orderLinkId", "")
        }

    def place_take_profit_market(self, symbol: str, side: str, stop_price: float) -> dict:
        """
        Place Take Profit via Conditional Order (Close on Trigger).
        """
        bybit_side = side.capitalize()
        
        payload = {
            "category": "linear",
            "symbol": symbol.upper(),
            "side": bybit_side,
            "orderType": "Market",
            "triggerPrice": str(stop_price),
            "triggerDirection": 1 if bybit_side == "Buy" else 2, # Buy=Rise(1), Sell=Fall(2) 
            "reduceOnly": True,
            "closeOnTrigger": True # Closes entire position
        }
        
        res = self._request_v5("POST", "/v5/order/create", payload)
        if res["retCode"] != 0:
            raise RuntimeError(f"Bybit TP Failed: {res.get('retMsg')}")
            
        result = res.get("result", {})
        return {
            "orderId": result.get("orderId", ""),
            "status": "NEW",
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": float(stop_price)
        }

    def update_protection(self, req) -> dict:
        """
        Cancel-replace SL/TP orders via Bybit V5.
        req: ProtectionUpdateRequest instance.

        Steps:
        1. Cancel old SL/TP orders (by orderId if provided, else cancel-all for symbol).
        2. Place new STOP_MARKET with reduce_only=True.
        3. Place new TAKE_PROFIT_MARKET with reduce_only=True.
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
        sl_side = "Sell" if is_long else "Buy"
        tp_side = "Sell" if is_long else "Buy"

        # ---- Cancel old orders ----
        try:
            sl_oid = getattr(req, "old_sl_order_id", None)
            tp_oid = getattr(req, "old_tp_order_id", None)
            if sl_oid or tp_oid:
                for oid in [sl_oid, tp_oid]:
                    if oid:
                        try:
                            self._request_v5("POST", "/v5/order/cancel", {
                                "category": "linear",
                                "symbol": symbol,
                                "orderId": oid,
                            })
                        except Exception as ce:
                            _log.warning(
                                f"[UPDATE_PROTECTION] {symbol}: cancel orderId={oid} "
                                f"failed (may already be gone): {ce}"
                            )
            else:
                self.cancel_all_orders(symbol)
        except Exception as e:
            cancel_error = str(e)
            _log.error(f"[UPDATE_PROTECTION] {symbol}: cancel phase failed: {e}")

        # ---- Place new SL ----
        if getattr(req, "new_sl_price", None) is not None:
            try:
                sl_res = self.place_stop_market(symbol, sl_side, float(req.new_sl_price), reduce_only=True)
                new_sl_id = sl_res.get("orderId") or sl_res.get("bybit_link_id")
                _log.info(f"[UPDATE_PROTECTION] {symbol}: new SL={req.new_sl_price} reason={req.reason}")
            except Exception as e:
                replace_error = f"SL_PLACE_FAILED: {e}"
                _log.error(
                    f"[UPDATE_PROTECTION] {symbol}: CRITICAL — SL replace FAILED after cancel. "
                    f"Position now has no stop-loss. Trigger ensure_protection. Error: {e}"
                )

        # ---- Place new TP ----
        if getattr(req, "new_tp_price", None) is not None:
            try:
                tp_res = self.place_take_profit_market(symbol, tp_side, float(req.new_tp_price))
                new_tp_id = tp_res.get("orderId")
                _log.info(f"[UPDATE_PROTECTION] {symbol}: new TP={req.new_tp_price} reason={req.reason}")
            except Exception as e:
                replace_error = (replace_error or "") + f" TP_PLACE_FAILED: {e}"
                _log.error(f"[UPDATE_PROTECTION] {symbol}: TP replace failed: {e}")

        status = "REPLACE_PARTIAL_FAILURE" if replace_error else "OK"
        return {
            "sl_order_id": new_sl_id,
            "tp_order_id": new_tp_id,
            "status": status,
            "error": replace_error or cancel_error,
        }

    def ping(self) -> bool:
        try:
             self._request_v5("GET", "/v5/market/time")
             return True
        except:
            return False

    def server_time(self) -> int:
        """Get server time in milliseconds."""
        data = self._request_v5("GET", "/v5/market/time")
        if data["retCode"] != 0:
            raise RuntimeError(f"Bybit Time Failed: {data.get('retMsg')}")
        
        # Result: {"timeSecond": "...", "timeNano": "..."}
        # Use nano for precision, convert to ms
        nano = int(data["result"]["timeNano"])
        return nano // 1_000_000

    def sync_time(self):
        # Bybit auto-handles time offset mostly, but we can implement if needed.
        pass
