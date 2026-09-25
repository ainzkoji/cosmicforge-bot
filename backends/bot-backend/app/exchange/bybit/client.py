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

    # ------------------ WALLET / INTERNAL TRANSFER (V5 asset) ------------------
    # Moves between THIS account's own wallets only (FUND <-> UNIFIED etc.).
    # There is deliberately NO withdrawal method on this client.

    def query_api_key(self) -> dict:
        return self._request_v5("GET", "/v5/user/query-api")

    def account_info(self) -> dict:
        """/v5/account/info: unifiedMarginStatus (1 = classic, >=3 = UTA)."""
        return self._request_v5("GET", "/v5/account/info")

    def inter_transfer(self, transfer_id: str, coin: str, amount: str, from_account_type: str,
                       to_account_type: str) -> dict:
        """POST /v5/asset/transfer/inter-transfer. ``transfer_id`` is a
        caller-supplied UUID: the broker de-duplicates on it, and it is the
        lookup key when the submission outcome is unknown."""
        return self._request_v5("POST", "/v5/asset/transfer/inter-transfer", {
            "transferId": str(transfer_id), "coin": coin.upper(), "amount": str(amount),
            "fromAccountType": from_account_type, "toAccountType": to_account_type,
        })

    def query_inter_transfers(self, transfer_id: str | None = None, coin: str | None = None,
                              start_time_ms: int | None = None, end_time_ms: int | None = None,
                              limit: int = 50, cursor: str | None = None) -> dict:
        return self._request_v5("GET", "/v5/asset/transfer/query-inter-transfer-list", {
            "transferId": transfer_id, "coin": coin.upper() if coin else None,
            "startTime": start_time_ms, "endTime": end_time_ms, "limit": limit, "cursor": cursor,
        })

    def account_coin_balance(self, account_type: str, coin: str) -> dict:
        """GET /v5/asset/transfer/query-account-coin-balance -> transferBalance."""
        return self._request_v5("GET", "/v5/asset/transfer/query-account-coin-balance",
                                {"accountType": account_type, "coin": coin.upper()})

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
        """Binance-shaped instrument filters for EVERY linear instrument
        (cursor-paginated via discover_instruments), cached for 5 minutes.
        (The previous version read one unpaginated page and USDT only.)"""
        cache = getattr(self, "_ei_cache", None)
        if cache is not None and time.time() - cache[0] < 300:
            return cache[1]
        symbols = []
        for ins in self.discover_instruments("linear"):
            if not ins.api_tradable:
                continue
            raw = dict(ins.venue_metadata.get("raw_filters") or {})
            filters = [{"filterType": "PRICE_FILTER", "tickSize": raw.get("tickSize") or "0"},
                       {"filterType": "LOT_SIZE", "stepSize": raw.get("qtyStep") or "0",
                        "minQty": raw.get("minOrderQty") or "0", "maxQty": raw.get("maxOrderQty") or "0"}]
            if raw.get("minNotionalValue"):
                filters.append({"filterType": "MIN_NOTIONAL", "notional": raw["minNotionalValue"]})
            symbols.append({"symbol": ins.venue_symbol, "filters": filters, "status": "TRADING",
                            "baseAsset": ins.base_currency, "quoteAsset": ins.settlement_asset})
        info = {"symbols": symbols}
        self._ei_cache = (time.time(), info)
        return info

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

    # ================== EXECUTION PARITY (canonical contract) ==================
    # Binance-shaped responses so the existing executor / fill resolution /
    # protection checks work unchanged. Order identity: our client_order_id
    # is sent as ``orderLinkId`` (broker-side idempotency + lookup key).

    _STATUS_MAP = {
        "New": "NEW", "PartiallyFilled": "PARTIALLY_FILLED", "Filled": "FILLED", "Cancelled": "CANCELED",
        "PartiallyFilledCanceled": "CANCELED", "Rejected": "REJECTED", "Deactivated": "CANCELED",
        "Untriggered": "NEW", "Triggered": "NEW", "Active": "NEW",
    }

    def _ok(self, res: dict, what: str) -> dict:
        if (res or {}).get("retCode") != 0:
            raise RuntimeError(f"Bybit {what} failed: retCode={res.get('retCode')} {res.get('retMsg')}")
        return res.get("result") or {}

    def _category_for(self, symbol: str) -> str:
        return "linear"

    def _fmt_qty(self, symbol: str, qty) -> str:
        """Round DOWN to the instrument qty step (never str(float) -> '1e-05')."""
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
        status = self._STATUS_MAP.get(str(o.get("orderStatus", "")), None)
        otype = str(o.get("orderType", "")).upper()
        stop_type = str(o.get("stopOrderType", ""))
        if stop_type in ("StopLoss", "Stop", "PartialStopLoss", "TrailingStop"):
            otype = "STOP_MARKET"
        elif stop_type in ("TakeProfit", "PartialTakeProfit"):
            otype = "TAKE_PROFIT_MARKET"
        return {
            "symbol": o.get("symbol"), "orderId": o.get("orderId"), "clientOrderId": o.get("orderLinkId") or None,
            "status": status, "executedQty": o.get("cumExecQty") or "0", "origQty": o.get("qty"),
            "avgPrice": o.get("avgPrice") or "0", "side": str(o.get("side", "")).upper(), "type": otype,
            "stopPrice": o.get("triggerPrice") or o.get("stopLoss") or o.get("takeProfit") or None,
            "reduceOnly": bool(o.get("reduceOnly")), "updateTime": int(o.get("updatedTime") or 0),
            "bybit_stop_order_type": stop_type or None,
        }

    def place_order(self, req):
        """Canonical OrderRequest -> UnifiedOrder (MARKET / LIMIT, reduce-only aware)."""
        from decimal import Decimal
        from app.models.unified_trading import OrderStatus, UnifiedOrder
        if req.leverage:
            self.set_leverage(req.symbol, int(req.leverage))
        payload = {
            "category": self._category_for(req.symbol), "symbol": req.symbol.upper(),
            "side": "Buy" if req.side.value.lower() == "buy" else "Sell",
            "orderType": "Market" if str(req.type.value if hasattr(req.type, "value") else req.type).upper() == "MARKET" else "Limit",
            "qty": self._fmt_qty(req.symbol, req.qty), "positionIdx": 0,
        }
        if payload["orderType"] == "Limit":
            payload["price"] = str(req.price)
            payload["timeInForce"] = "GTC"
        if getattr(req, "client_order_id", None):
            payload["orderLinkId"] = str(req.client_order_id)[:36]
        if req.reduce_only:
            payload["reduceOnly"] = True
        res = self._ok(self._request_v5("POST", "/v5/order/create", payload), "order create")
        # Bybit acknowledges asynchronously: the fill is resolved by
        # fill_resolution through get_order / user_trades (never assumed).
        return UnifiedOrder(
            client_order_id=str(res.get("orderLinkId") or payload.get("orderLinkId") or ""),
            broker_order_id=str(res.get("orderId", "")), symbol=req.symbol, side=req.side, type=req.type,
            qty_ordered=req.qty, qty_filled=Decimal("0"), avg_fill_price=Decimal("0"), status=OrderStatus.NEW,
            timestamp=int(time.time() * 1000), reduce_only=req.reduce_only,
        )

    def _find_order(self, symbol: str, **ident) -> dict:
        params = {"category": self._category_for(symbol), "symbol": symbol.upper(), **ident}
        for path in ("/v5/order/realtime", "/v5/order/history"):
            res = self._ok(self._request_v5("GET", path, params), "order query")
            rows = res.get("list") or []
            if rows:
                return self._order_view(rows[0])
        return {}

    def get_order(self, symbol: str, order_id) -> dict:
        return self._find_order(symbol, orderId=str(order_id))

    def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> dict:
        return self._find_order(symbol, orderLinkId=str(client_order_id))

    def open_orders(self, symbol: str | None = None) -> list:
        """Active orders incl. conditional and position TP/SL, Binance-typed."""
        out = []
        for flt in ("Order", "StopOrder", "tpslOrder"):
            params = {"category": "linear", "orderFilter": flt}
            if symbol:
                params["symbol"] = symbol.upper()
            else:
                params["settleCoin"] = "USDT"
            res = self._ok(self._request_v5("GET", "/v5/order/realtime", params), "open orders")
            out.extend(self._order_view(o) for o in res.get("list") or [])
        return out

    def get_open_orders(self, symbol: str | None = None) -> list:
        return self.open_orders(symbol)

    def get_algo_orders(self, symbol: str, raise_on_error: bool = False) -> list:
        """Bybit has no separate algo book: conditional + TP/SL orders are
        already returned by open_orders(); nothing extra to report."""
        return []

    def cancel_order(self, symbol: str, order_id) -> bool:
        res = self._request_v5("POST", "/v5/order/cancel", {"category": "linear", "symbol": symbol.upper(),
                                                              "orderId": str(order_id)})
        return (res or {}).get("retCode") == 0

    def cancel_all(self, symbol: str) -> dict:
        return self.cancel_all_orders(symbol)

    def user_trades(self, symbol: str, start_time_ms: int | None = None, end_time_ms: int | None = None,
                    limit: int = 100) -> list:
        """Executions in Binance userTrades shape (orderId, qty, price, commission)."""
        params = {"category": "linear", "symbol": symbol.upper(), "limit": min(int(limit), 100),
                  "startTime": start_time_ms, "endTime": end_time_ms}
        res = self._ok(self._request_v5("GET", "/v5/execution/list", params), "executions")
        return [{"symbol": e.get("symbol"), "orderId": e.get("orderId"), "id": e.get("execId"),
                 "qty": e.get("execQty"), "price": e.get("execPrice"), "commission": e.get("execFee"),
                 "commissionAsset": e.get("feeCurrency") or "USDT", "time": int(e.get("execTime") or 0),
                 "side": str(e.get("side", "")).upper()} for e in res.get("list") or []]

    def position_risk_all(self) -> list:
        return self.position_risk()

    def get_positions(self):
        from decimal import Decimal
        from app.models.unified_trading import PositionMode, Side, UnifiedPosition
        out = []
        for p in self.position_risk():
            amt = Decimal(str(p.get("positionAmt") or 0))
            if amt == 0:
                continue
            out.append(UnifiedPosition(
                symbol=p["symbol"], broker_id="bybit", side=Side.BUY if amt > 0 else Side.SELL, quantity=abs(amt),
                entry_price=Decimal(str(p.get("entryPrice") or 0)), current_price=Decimal(str(p.get("entryPrice") or 0)),
                unrealized_pnl=Decimal(str(p.get("unRealizedProfit") or 0)), realized_pnl=Decimal("0"),
                margin_used=Decimal("0"), leverage=Decimal(str(p.get("leverage") or 1)), mode=PositionMode.ONE_WAY,
                timestamp=int(time.time() * 1000)))
        return out

    def get_position_stop(self, symbol: str) -> float:
        res = self._ok(self._request_v5("GET", "/v5/position/list", {"category": "linear", "symbol": symbol.upper()}),
                       "position")
        for p in res.get("list") or []:
            if float(p.get("size") or 0) > 0:
                return float(p.get("stopLoss") or 0)
        return 0.0

    def place_protection(self, req):
        """Position-attached SL/TP via /v5/position/trading-stop (tpslMode=Full):
        closes the whole position, is atomic per call, and is amended in place
        by update_protection (no cancel-then-replace window without a stop)."""
        from app.models.unified_trading import ProtectionResult
        result = ProtectionResult(status="initiated")
        payload = {"category": "linear", "symbol": req.symbol.upper(), "tpslMode": "Full", "positionIdx": 0}
        if req.sl_price:
            payload["stopLoss"] = self._px(req.sl_price)
            payload["slTriggerBy"] = "LastPrice"
        if req.tp_price:
            payload["takeProfit"] = self._px(req.tp_price)
            payload["tpTriggerBy"] = "LastPrice"
        try:
            self._ok(self._request_v5("POST", "/v5/position/trading-stop", payload), "trading-stop")
            if req.sl_price:
                result.sl_order_id = f"POSITION_SL:{req.symbol.upper()}"
            if req.tp_price:
                result.tp_order_id = f"POSITION_TP:{req.symbol.upper()}"
            result.status = "success"
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
        return result

    def get_prices(self, symbols: List[str]) -> Dict[str, float]:
        res = self._ok(self._request_v5("GET", "/v5/market/tickers", {"category": "linear"}), "tickers")
        wanted = {s.upper() for s in symbols} if symbols else None
        return {t["symbol"]: float(t["lastPrice"]) for t in res.get("list") or []
                if t.get("lastPrice") and (wanted is None or t["symbol"] in wanted)}

    def get_ticker(self, symbol: str) -> dict:
        res = self._ok(self._request_v5("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol.upper()}),
                       "ticker")
        t = (res.get("list") or [{}])[0]
        return {"symbol": symbol.upper(), "lastPrice": t.get("lastPrice"), "bidPrice": t.get("bid1Price"),
                "askPrice": t.get("ask1Price"), "markPrice": t.get("markPrice"), "indexPrice": t.get("indexPrice"),
                "volume24h": t.get("volume24h"), "turnover24h": t.get("turnover24h"),
                "openInterest": t.get("openInterest")}

    def get_orderbook(self, symbol: str, limit: int = 50) -> dict:
        res = self._ok(self._request_v5("GET", "/v5/market/orderbook", {"category": "linear", "symbol": symbol.upper(),
                                                                          "limit": limit}), "orderbook")
        return {"bids": [[float(p), float(q)] for p, q in res.get("b") or []],
                "asks": [[float(p), float(q)] for p, q in res.get("a") or []], "time": int(res.get("ts") or 0)}

    def get_funding(self, symbol: str) -> dict:
        t = self.get_ticker(symbol)
        res = self._ok(self._request_v5("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol.upper()}),
                       "funding")
        row = (res.get("list") or [{}])[0]
        return {"symbol": symbol.upper(), "fundingRate": row.get("fundingRate"),
                "nextFundingTime": row.get("nextFundingTime"), "markPrice": t.get("markPrice")}

    def exchange_info(self) -> dict:
        return self.exchange_info_cached()

    def _instruments_page(self, category: str, cursor: str | None) -> dict:
        return self._ok(self._request_v5("GET", "/v5/market/instruments-info",
                                         {"category": category, "limit": 1000, "cursor": cursor}), "instruments")

    def discover_instruments(self, category: str = "linear") -> list:
        """Every instrument V5 lists for ``category`` (cursor-paginated)."""
        from app.exchange.instruments import parse_bybit_instrument
        out, cursor = [], None
        for _ in range(50):
            res = self._instruments_page(category, cursor)
            for r in res.get("list") or []:
                ins = parse_bybit_instrument(r, category=category)
                if ins is not None:
                    out.append(ins)
            cursor = res.get("nextPageCursor") or None
            if not cursor:
                break
        return out

    def list_instruments(self):
        return [i.to_instrument_spec("bybit") for i in self.discover_instruments()]

    def get_instrument(self, symbol: str):
        for i in self.discover_instruments():
            if i.venue_symbol == symbol.upper():
                return i
        return None

    def get_balance(self) -> dict:
        from decimal import Decimal
        a = self.account()
        return {"wallet": Decimal(str(a["totalWalletBalance"])), "equity": Decimal(str(a["totalMarginBalance"])),
                "available": Decimal(str(a["availableBalance"]))}

    def get_account_permissions(self) -> dict:
        from shared_lib.broker.permissions import normalize_bybit_query_api, unverified
        data = self.query_api_key()
        if (data or {}).get("retCode") != 0:
            return unverified("bybit", "bybit:/v5/user/query-api").to_dict()
        return normalize_bybit_query_api(data.get("result") or {}).to_dict()

    def get_account_capabilities(self, environment: str = "live") -> dict:
        from shared_lib.broker.capabilities import declared_profile
        perms = self.get_account_permissions().get("permissions")
        return declared_profile("bybit").for_account(perms).to_dict()

    @staticmethod
    def _px(v) -> str:
        from decimal import Decimal
        return format(Decimal(str(v)).normalize(), "f")

    def update_protection(self, req) -> dict:
        """Amend the position-attached SL/TP IN PLACE (trading-stop). There is
        no cancel step, so no window in which the position has no stop. A TP
        of None leaves the existing TP untouched. Raises on failure (same
        fail-closed contract as the Binance client)."""
        payload = {"category": "linear", "symbol": req.symbol.upper(), "tpslMode": "Full", "positionIdx": 0}
        if getattr(req, "new_sl_price", None) is not None:
            payload["stopLoss"] = self._px(req.new_sl_price)
            payload["slTriggerBy"] = "LastPrice"
        if getattr(req, "new_tp_price", None) is not None:
            payload["takeProfit"] = self._px(req.new_tp_price)
            payload["tpTriggerBy"] = "LastPrice"
        res = self._request_v5("POST", "/v5/position/trading-stop", payload)
        if (res or {}).get("retCode") not in (0, 34040):  # 34040 = not modified (already at that level)
            raise RuntimeError(f"[SEV1-S5] update_protection failed for {req.symbol}: {res.get('retMsg')}")
        sym = req.symbol.upper()
        return {"sl_order_id": f"POSITION_SL:{sym}" if "stopLoss" in payload else getattr(req, "old_sl_order_id", None),
                "tp_order_id": f"POSITION_TP:{sym}" if "takeProfit" in payload else getattr(req, "old_tp_order_id", None),
                "status": "OK", "error": None}
