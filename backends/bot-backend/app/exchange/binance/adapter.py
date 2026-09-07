from typing import List, Dict, Optional, Any
from decimal import Decimal
import time

from app.models.unified_trading import (
    InstrumentSpec, 
    UnifiedOrder, 
    UnifiedPosition, 
    UnifiedFill, 
    OrderRequest,
    ProtectionRequest, 
    ProtectionResult,
    AssetClass,
    Side,
    OrderStatus,
    PositionMode,
    IdempotencyMode
)
from app.exchange.interface import ExchangeClient, BrokerCapabilities
from app.exchange.binance.client import BinanceFuturesClient

# Import legacy filter extraction just for the Shim
from app.exchange.binance.filters import extract_filters

class BinanceAdapter(ExchangeClient):
    """
    SHIM ADAPTER throughout Phase 1 & 2.
    Wraps existing BinanceFuturesClient to provide Generic Interface.
    """
    def __init__(self, client: BinanceFuturesClient):
        self._client = client
        self._capabilities = BrokerCapabilities(
            position_mode=PositionMode.ONE_WAY, # Default assumption for Shim
            supports_hedging=True,       
            supports_reduce_only=True,
            supports_market_orders=True,
            supports_attached_sl_tp=False, 
            supports_separate_protection=True,
            supports_oco=False, 
            idempotency_mode=IdempotencyMode.CLIENT_ORDER_ID,
            supports_fills_endpoint=True
        )

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._capabilities

    def get_server_time(self) -> int:
        res = self._client.time()
        return int(res.get("serverTime", time.time()*1000))

    def list_instruments(self) -> List[InstrumentSpec]:
        """
        Maps Binance exchangeInfo -> InstrumentSpecs.
        CRITICAL: Hardcodes contract_size=1.0 for Crypto Perps.
        """
        info = self._client.exchange_info_cached() # Assuming this is hydrated
        if not info:
            # Fallback or force fetch
            info = self._client.exchange_info()
            
        specs = []
        for s in info.get("symbols", []):
            if s.get("contractType") != "PERPETUAL":
                continue 
            
            # Filter extraction (reusing legacy logic to be safe)
            # We could do raw parsing, but let's trust existing filters helper for now
            # Actually, generic parsing is safer for the Shim
            sym = s["symbol"]
            
            price_prec = int(s.get("pricePrecision", 2))
            qty_prec = int(s.get("quantityPrecision", 2))
            
            # Filters
            step_size = Decimal("0")
            tick_size = Decimal("0")
            min_qty = Decimal("0")
            min_notional = Decimal("0")
            
            for f in s.get("filters", []):
                ft = f.get("filterType")
                if ft == "LOT_SIZE":
                    step_size = Decimal(f["stepSize"])
                    min_qty = Decimal(f["minQty"])
                elif ft == "PRICE_FILTER":
                    tick_size = Decimal(f["tickSize"])
                elif ft == "MIN_NOTIONAL":
                    min_notional = Decimal(f.get("notional", "0"))
                    
            specs.append(InstrumentSpec(
                symbol_canonical=sym,
                symbol_exchange=sym,
                asset_class=AssetClass.CRYPTO_PERP,
                base_currency=s.get("baseAsset", ""),
                quote_currency=s.get("quoteAsset", ""),
                margin_currency=s.get("quoteAsset", "USDT"), # Usually USDT
                settlement_currency=s.get("quoteAsset", "USDT"),
                
                contract_size=Decimal("1.0"), # <--- KEY ASSUMPTION PRESERVED
                tick_size=tick_size,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
                
                price_precision=price_prec,
                qty_precision=qty_prec,
                
                max_leverage=Decimal("125"), # Discovery needed for real max, defaulting high
                supports_per_order_leverage=True
            ))
        return specs

    def get_prices(self, symbols: List[str]) -> Dict[str, Decimal]:
        # Binance ticker logic
        # Optimize: get all tickers if symbols list is large/None
        raw = self._client.ticker_price()
        out = {}
        target_set = set(symbols) if symbols else None
        
        for item in raw:
            s = item["symbol"]
            if target_set and s not in target_set:
                continue
            out[s] = Decimal(item.get("price", "0"))
        return out

    def get_klines(self, symbol: str, interval: str, limit: int) -> List[Any]:
        return self._client.klines(symbol=symbol, interval=interval, limit=limit)

    def place_order(self, req: OrderRequest) -> UnifiedOrder:
        
        # 1. Leverage (Shim side-effect)
        if req.leverage is not None:
            self._client.set_leverage(req.symbol, int(req.leverage))

        # 2. Place Order
        s_side = req.side.value.upper() # BUY/SELL
        
        # Market vs Limit
        if req.type != "market": # OrderType.MARKET string value check or enum
           # Simulating support for now to stick to shim scope
           # If strictly enforced, only market orders passed by Executor for now
           pass
           
        resp = self._client.place_market_order(
            symbol=req.symbol,
            side=s_side,
            quantity=float(req.qty), # Legacy client expects float
            reduce_only=req.reduce_only,
            client_order_id=req.client_order_id,
        )
        
        # 3. Map Response to UnifiedOrder
        return self._map_order(resp, req.symbol)

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
            self._client.cancel_order(symbol, order_id=order_id)
            return True
        except:
            return False

    def get_order(self, symbol: str, order_id: str) -> UnifiedOrder:
        resp = self._client.get_order(symbol, order_id=order_id)
        return self._map_order(resp, symbol)

    def list_open_orders(self, symbol: Optional[str] = None) -> List[UnifiedOrder]:
        if symbol:
            resp = self._client.open_orders(symbol)
        else:
            # Careful, this might be heavy on Binance
            resp = self._client.open_orders() # empty implies all?
            
        return [self._map_order(o, o.get("symbol", symbol)) for o in resp]

    def get_fills(self, symbol: str, start_time: int, limit: int = 100) -> List[UnifiedFill]:
        # Binance "userTrades"
        trades = self._client.account_trades(symbol=symbol, startTime=start_time, limit=limit)
        fills = []
        for t in trades:
            fills.append(UnifiedFill(
                fill_id=str(t["id"]),
                order_id=str(t["orderId"]),
                client_order_id=None, # User trades endpoint often misses clientID
                symbol=symbol,
                side=Side.BUY if t["side"]=="BUY" else Side.SELL,
                qty=Decimal(t["qty"]),
                price=Decimal(t["price"]),
                commission=Decimal(t["commission"]),
                commission_asset=t["commissionAsset"],
                timestamp=int(t["time"]),
                is_maker=t["isMaker"]
            ))
        return fills

    def place_protection(self, req: ProtectionRequest) -> ProtectionResult:
        # Shim implementation: just call legacy stop methods
        # This wrapper replicates executor's protection calls
        
        exit_side = "SELL" if req.position_side == Side.BUY else "BUY"
        res = ProtectionResult(status="initiated")
        
        try:
            if req.sl_price:
                # Retry Logic for Binance -2021 (Order triggering immediately)
                # If it fails, push it slightly further and retry once
                try:
                    sl_resp = self._client.place_stop_market(
                        req.symbol, 
                        exit_side, 
                        float(req.sl_price)
                    )
                except Exception as e:
                    s_err = str(e)
                    if '"code":-2021' in s_err or "would immediately trigger" in s_err:
                        # Logic: Push 2 ticks further
                        # Need tick size to do this properly. Protocol doesn't expose quick tick lookup on self.
                        # But for shim, we can assume or fetch.
                        # Easier: Just fail for now, OR fetch info.
                        # Let's try to fetch info cached.
                        info = self._client.exchange_info_cached()
                        ticks = Decimal("0")
                        for s in info.get("symbols", []):
                            if s["symbol"] == req.symbol:
                                for f in s.get("filters", []):
                                    if f["filterType"] == "PRICE_FILTER":
                                        ticks = Decimal(f["tickSize"])
                                break
                        
                        if ticks > 0:
                            # Push 20 ticks (safe buffer) or just 0.1%?
                            # Executor used 2 ticks buffer.
                            # We'll adapt: if SELL (Long Close), lower price. If BUY (Short Close), raise price.
                            # buffer = ticks * 2
                            buf = float(ticks * 5) # safer
                            old_sl = float(req.sl_price)
                            new_sl = old_sl - buf if exit_side == "SELL" else old_sl + buf
                            
                            sl_resp = self._client.place_stop_market(
                                req.symbol,
                                exit_side,
                                new_sl
                            )
                        else:
                            raise e 
                    else:
                        raise e

                res.sl_order_id = str(sl_resp["orderId"])
                if res.sl_order_id == "DUPLICATE_4130":
                    try:
                        _existing = self._client.open_orders(req.symbol)
                        _sl_target = float(req.sl_price)
                        _best_sl: dict | None = None
                        _best_diff = float("inf")
                        for _o in (_existing if isinstance(_existing, list) else []):
                            if str(_o.get("type", "")).upper() != "STOP_MARKET":
                                continue
                            _sp = float(_o.get("stopPrice", 0) or 0)
                            if _sp <= 0:
                                continue
                            _diff = abs(_sp - _sl_target) / max(_sl_target, 1e-9)
                            if _diff < _best_diff:
                                _best_diff = _diff
                                _best_sl = _o
                        if _best_sl and _best_diff < 0.005:
                            res.sl_order_id = str(_best_sl["orderId"])
                    except Exception:
                        pass

            if req.tp_price:
                tp_resp = self._client.place_take_profit_market(
                    req.symbol,
                    exit_side,
                    float(req.tp_price)
                )
                res.tp_order_id = str(tp_resp["orderId"])
                if res.tp_order_id == "DUPLICATE_4130":
                    try:
                        _existing = self._client.open_orders(req.symbol)
                        _tp_target = float(req.tp_price)
                        _best_tp: dict | None = None
                        _best_diff = float("inf")
                        for _o in (_existing if isinstance(_existing, list) else []):
                            if str(_o.get("type", "")).upper() != "TAKE_PROFIT_MARKET":
                                continue
                            _sp = float(_o.get("stopPrice", 0) or 0)
                            if _sp <= 0:
                                continue
                            _diff = abs(_sp - _tp_target) / max(_tp_target, 1e-9)
                            if _diff < _best_diff:
                                _best_diff = _diff
                                _best_tp = _o
                        if _best_tp and _best_diff < 0.005:
                            res.tp_order_id = str(_best_tp["orderId"])
                    except Exception:
                        pass

            unresolved_ids = {
                oid
                for oid in (res.sl_order_id, res.tp_order_id)
                if str(oid or "") == "DUPLICATE_4130"
            }
            if unresolved_ids:
                raise RuntimeError(
                    "Binance returned duplicate protection sentinel -4130, "
                    "but no matching live broker protection order ID could be resolved"
                )
                
            res.status = "success"
        except Exception as e:
            res.status = "failed"
            res.error = str(e)
            
        return res

    def get_positions(self) -> List[UnifiedPosition]:
        # Binance positionRisk
        # Shim NOTE: We should assume ONE_WAY mostly, but let's parse raw
        raw = self._client.position_risk() # list of dicts
        out = []
        
        for p in raw:
            amt = Decimal(p["positionAmt"])
            if amt == 0:
                continue
                
            entry = Decimal(p["entryPrice"])
            
            # Infer side
            side = Side.BUY if amt > 0 else Side.SELL
            qty = abs(amt)
            
            # Pnl
            upnl = Decimal(p["unRealizedProfit"])
            lev = Decimal(p["leverage"])
            
            # Mark price not always in positionRisk, but 'markPrice' often is
            mark = Decimal(p.get("markPrice", p.get("entryPrice"))) 
            
            # Margin Type check for Mode
            # Binance: "marginType": "isolated" or "cross". Not hedged vs one-way.
            # "positionSide": "BOTH" (OneWay) or "LONG"/"SHORT" (Hedge)
            pos_side = p.get("positionSide", "BOTH")
            mode = PositionMode.HEDGE if pos_side in ("LONG", "SHORT") else PositionMode.ONE_WAY
            
            out.append(UnifiedPosition(
                symbol=p["symbol"],
                broker_id="binance",
                side=side,
                quantity=qty,
                entry_price=entry,
                current_price=mark,
                unrealized_pnl=upnl,
                realized_pnl=Decimal("0"), # Not in snapshot
                margin_used=Decimal(p.get("initialMargin", "0")), # or isolatedWallet
                leverage=lev,
                mode=mode,
                timestamp=int(time.time()*1000)
            ))
        return out

    def get_balance(self) -> Dict[str, Decimal]:
        acc = self._client.account()
        wallet = Decimal(acc.get("totalWalletBalance", "0"))
        avail = Decimal(acc.get("availableBalance", "0"))
        # Unclear what "Equity" map is in generic, usually Wallet + UPnL
        upnl = Decimal(acc.get("totalUnrealizedProfit", "0"))
        
        return {
            "wallet": wallet,
            "equity": wallet + upnl,
            "available": avail
        }
        
    def close_position(self, symbol: str, position_id: Optional[str] = None) -> UnifiedOrder:
        # Shim: Close All (Market)
        # Ignores position_id for Binance OneWay
        resp = self._client.close_position_market(symbol)
        return self._map_order(resp, symbol)

    def _map_order(self, r: dict, symbol: str) -> UnifiedOrder:
        """Internal helper to convert Binance Order JSON to UnifiedModel"""
        # Status map
        s = r.get("status", "NEW")
        st = OrderStatus.NEW
        if s == "FILLED": st = OrderStatus.FILLED
        elif s == "PARTIALLY_FILLED": st = OrderStatus.PARTIALLY_FILLED
        elif s == "CANCELED": st = OrderStatus.CANCELED
        elif s == "REJECTED": st = OrderStatus.REJECTED
        elif s == "EXPIRED": st = OrderStatus.EXPIRED
        
        return UnifiedOrder(
            client_order_id=str(r.get("clientOrderId", "")),
            broker_order_id=str(r.get("orderId", "")),
            symbol=symbol,
            side=Side.BUY if r.get("side") == "BUY" else Side.SELL,
            type=r.get("type", "MARKET"),
            qty_ordered=Decimal(r.get("origQty", "0")),
            qty_filled=Decimal(r.get("executedQty", "0")),
            avg_fill_price=Decimal(r.get("avgPrice", "0")),
            status=st,
            timestamp=int(r.get("updateTime", r.get("transactTime", 0))),
            reduce_only=bool(r.get("reduceOnly", False))
        )
