"""
OANDA Adapter implementing ExchangeClient Protocol.

Wraps OandaClient to provide the generic ExchangeClient interface
for Forex trading via OANDA v20 API.
"""
import time
from typing import List, Dict, Optional, Any
from decimal import Decimal

from app.models.unified_trading import (
    InstrumentSpec,
    UnifiedOrder,
    UnifiedPosition,
    UnifiedFill,
    OrderRequest,
    ProtectionRequest,
    ProtectionResult,
    Side,
    OrderType,
    PositionMode,
    IdempotencyMode
)
from app.exchange.interface import ExchangeClient, BrokerCapabilities
from app.exchange.oanda.client import OandaClient
from app.exchange.oanda.mapping import (
    canonical_to_oanda,
    oanda_to_canonical,
    map_oanda_instrument_to_spec,
    map_oanda_order_to_unified,
    map_oanda_trade_to_position,
    map_oanda_transaction_to_fill
)


class OandaAdapter(ExchangeClient):
    """
    OANDA v20 Adapter implementing ExchangeClient protocol.
    
    Key Features:
    - Ticket-based position mode (PositionMode.TICKET)
    - Attached SL/TP support via orderCreate
    - Units-based trading (contract_size=1 in InstrumentSpec)
    - Market hours enforcement via session guard
    """
    
    def __init__(self, client: OandaClient):
        self._client = client
        self._capabilities = BrokerCapabilities(
            position_mode=PositionMode.TICKET,
            supports_hedging=True,  # OANDA allows multiple trades per instrument
            supports_ticket_mode=True,
            supports_reduce_only=True,
            supports_market_orders=True,
            supports_per_symbol_leverage=False,  # Account-level margin
            supports_attached_sl_tp=True,  # Can attach SL/TP to order
            supports_separate_protection=True,  # Can also modify trade protection
            supports_oco=False,
            supports_trailing_stop=True,
            idempotency_mode=IdempotencyMode.CLIENT_ORDER_ID,  # clientExtensions
            idempotency_key_header=None,
            supports_fills_endpoint=True
        )
    
    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._capabilities
    
    # ==========================================
    # DISCOVERY
    # ==========================================
    
    def get_server_time(self) -> int:
        """Returns server time in milliseconds."""
        # OANDA doesn't have dedicated time endpoint
        # Use account summary timestamp
        try:
            summary = self._client.get_account_summary()
            time_str = summary.get("lastTransactionID", "")
            # Fallback to local time
            return int(time.time() * 1000)
        except:
            return int(time.time() * 1000)
    
    def list_instruments(self) -> List[InstrumentSpec]:
        """
        Fetches and maps OANDA instruments to InstrumentSpecs.
        """
        instruments = self._client.get_instruments()
        specs = []
        
        for inst in instruments:
            try:
                spec = map_oanda_instrument_to_spec(inst)
                specs.append(spec)
            except Exception as e:
                # Log and skip malformed instruments
                print(f"[OANDA] Failed to map instrument {inst.get('name')}: {e}")
                continue
        
        return specs
    
    # ==========================================
    # MARKET DATA
    # ==========================================
    
    def get_prices(self, symbols: List[str]) -> Dict[str, Decimal]:
        """
        Fetches current mid prices for symbols.
        """
        # Convert canonical to OANDA format
        oanda_symbols = [canonical_to_oanda(s) for s in symbols]
        
        pricing = self._client.get_pricing(oanda_symbols)
        prices = {}
        
        for price_data in pricing.get("prices", []):
            instrument = price_data["instrument"]
            canonical = oanda_to_canonical(instrument)
            
            # Use mid price (average of bid/ask)
            bids = price_data.get("bids", [])
            asks = price_data.get("asks", [])
            
            if bids and asks:
                bid = Decimal(bids[0]["price"])
                ask = Decimal(asks[0]["price"])
                mid = (bid + ask) / Decimal("2")
                prices[canonical] = mid
            elif bids:
                prices[canonical] = Decimal(bids[0]["price"])
            elif asks:
                prices[canonical] = Decimal(asks[0]["price"])
        
        return prices
    
    def get_klines(self, symbol: str, interval: str, limit: int) -> List[Any]:
        """
        Fetches historical candles.
        
        Interval mapping:
        - "1m" -> "M1"
        - "5m" -> "M5"
        - "1h" -> "H1"
        - "1d" -> "D"
        """
        oanda_symbol = canonical_to_oanda(symbol)
        
        # Map interval to OANDA granularity
        granularity_map = {
            "1m": "M1",
            "5m": "M5",
            "15m": "M15",
            "1h": "H1",
            "4h": "H4",
            "1d": "D",
        }
        granularity = granularity_map.get(interval, "M5")
        
        candles = self._client.get_candles(
            instrument=oanda_symbol,
            granularity=granularity,
            count=limit
        )
        
        # Convert to standard OHLCV format
        result = []
        for candle in candles:
            if not candle.get("complete"):
                continue
            
            mid = candle.get("mid", {})
            result.append({
                "timestamp": candle.get("time"),
                "open": float(mid.get("o", 0)),
                "high": float(mid.get("h", 0)),
                "low": float(mid.get("l", 0)),
                "close": float(mid.get("c", 0)),
                "volume": int(candle.get("volume", 0))
            })
        
        return result
    
    # ==========================================
    # TRADING
    # ==========================================
    
    def place_order(self, req: OrderRequest) -> UnifiedOrder:
        """
        Places an order via OANDA.
        
        Supports attached SL/TP via stopLossOnFill/takeProfitOnFill.
        """
        oanda_symbol = canonical_to_oanda(req.symbol)
        
        # Units (positive for BUY, negative for SELL)
        units_str = str(int(req.qty)) if req.side == Side.BUY else str(-int(req.qty))
        
        # Build order spec
        order_spec: Dict[str, Any] = {
            "type": "MARKET" if req.type == OrderType.MARKET else "LIMIT",
            "instrument": oanda_symbol,
            "units": units_str,
            "timeInForce": req.time_in_force
        }
        
        # Client Order ID (idempotency)
        if req.client_order_id:
            order_spec["clientExtensions"] = {
                "id": req.client_order_id,
                "tag": "cosmicforge"
            }
        
        # Limit price
        if req.price and req.type != OrderType.MARKET:
            order_spec["price"] = str(float(req.price))
        
        # Attached Protection
        if req.sl_price:
            order_spec["stopLossOnFill"] = {
                "price": str(float(req.sl_price)),
                "timeInForce": "GTC"
            }
        
        if req.tp_price:
            order_spec["takeProfitOnFill"] = {
                "price": str(float(req.tp_price)),
                "timeInForce": "GTC"
            }
        
        # Execute
        response = self._client.create_order({"order": order_spec})
        
        # Parse response
        order_created = response.get("orderFillTransaction") or response.get("orderCreateTransaction")
        if not order_created:
            raise RuntimeError(f"OANDA order creation failed: {response}")
        
        return map_oanda_order_to_unified(order_created, req.symbol)
    
    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancels an order."""
        try:
            self._client.cancel_order(order_id)
            return True
        except:
            return False
    
    def get_order(self, symbol: str, order_id: str) -> UnifiedOrder:
        """Fetches order details."""
        order = self._client.get_order(order_id)
        return map_oanda_order_to_unified(order, symbol)
    
    def list_open_orders(self, symbol: Optional[str] = None) -> List[UnifiedOrder]:
        """Lists open orders."""
        oanda_symbol = canonical_to_oanda(symbol) if symbol else None
        orders = self._client.get_orders(instrument=oanda_symbol, state="PENDING")
        
        result = []
        for order in orders:
            inst = order.get("instrument", "")
            canonical = oanda_to_canonical(inst)
            result.append(map_oanda_order_to_unified(order, canonical))
        
        return result
    
    # ==========================================
    # FILLS & PROTECTION
    # ==========================================
    
    def get_fills(self, symbol: str, start_time: int, limit: int = 100) -> List[UnifiedFill]:
        """
        Fetches fills (transactions) for symbol.
        """
        # Convert timestamp to ISO8601
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
        from_time = dt.isoformat().replace("+00:00", "Z")
        
        # Fetch transactions
        txns = self._client.get_transactions(from_time=from_time)
        
        # Filter ORDER_FILL transactions for symbol
        oanda_symbol = canonical_to_oanda(symbol)
        fills = []
        
        for txn in txns:
            if txn.get("type") != "ORDER_FILL":
                continue
            if txn.get("instrument") != oanda_symbol:
                continue
            
            fills.append(map_oanda_transaction_to_fill(txn))
            
            if len(fills) >= limit:
                break
        
        return fills
    
    def place_protection(self, req: ProtectionRequest) -> ProtectionResult:
        """
        Places or modifies protection on existing position.
        
        OANDA strategy: Modify trade's SL/TP using modify_trade endpoint.
        """
        oanda_symbol = canonical_to_oanda(req.symbol)
        
        # Find open trade(s) for this symbol
        trades = self._client.get_open_trades(instrument=oanda_symbol)
        
        if not trades:
            return ProtectionResult(
                status="failed",
                error="No open trades found for symbol"
            )
        
        # Apply protection to first matching trade (or all if needed)
        # For simplicity, we'll modify the first trade
        trade = trades[0]
        trade_id = str(trade["id"])
        
        modifications = {}
        
        if req.sl_price:
            modifications["stopLoss"] = {
                "price": str(float(req.sl_price)),
                "timeInForce": "GTC"
            }
        
        if req.tp_price:
            modifications["takeProfit"] = {
                "price": str(float(req.tp_price)),
                "timeInForce": "GTC"
            }
        
        try:
            response = self._client.modify_trade(trade_id, modifications)
            
            sl_order_id = None
            tp_order_id = None
            
            # Extract order IDs from response
            if "stopLossOrderTransaction" in response:
                sl_order_id = str(response["stopLossOrderTransaction"]["id"])
            if "takeProfitOrderTransaction" in response:
                tp_order_id = str(response["takeProfitOrderTransaction"]["id"])
            
            return ProtectionResult(
                sl_order_id=sl_order_id,
                tp_order_id=tp_order_id,
                status="success"
            )
        except Exception as e:
            return ProtectionResult(
                status="failed",
                error=str(e)
            )
    
    # ==========================================
    # POSITION & ACCOUNT
    # ==========================================
    
    def get_positions(self) -> List[UnifiedPosition]:
        """
        Fetches open positions (trades).
        
        OANDA uses ticket-based trades, not net positions.
        """
        trades = self._client.get_open_trades()
        positions = []
        
        for trade in trades:
            try:
                positions.append(map_oanda_trade_to_position(trade))
            except Exception as e:
                print(f"[OANDA] Failed to map trade {trade.get('id')}: {e}")
                continue
        
        return positions
    
    def get_balance(self) -> Dict[str, Decimal]:
        """
        Fetches account balance.
        
        Returns wallet, equity, and available balance in USD.
        """
        summary = self._client.get_account_summary()
        
        balance = Decimal(summary.get("balance", "0"))
        nav = Decimal(summary.get("NAV", balance))  # Net Asset Value
        margin_available = Decimal(summary.get("marginAvailable", "0"))
        
        return {
            "wallet": balance,
            "equity": nav,
            "available": margin_available
        }
    
    def close_position(self, symbol: str, position_id: Optional[str] = None) -> UnifiedOrder:
        """
        Closes position.
        
        If position_id provided: close specific trade.
        Else: close all trades for symbol.
        """
        if position_id:
            # Close specific trade
            response = self._client.close_trade(position_id)
            txn = response.get("orderFillTransaction", {})
            return map_oanda_order_to_unified(txn, symbol)
        else:
            # Close all positions for symbol
            oanda_symbol = canonical_to_oanda(symbol)
            response = self._client.close_position(
                instrument=oanda_symbol,
                long_units="ALL",
                short_units="ALL"
            )
            
            # Extract fill transaction
            long_txn = response.get("longOrderFillTransaction")
            short_txn = response.get("shortOrderFillTransaction")
            
            txn = long_txn or short_txn or {}
            return map_oanda_order_to_unified(txn, symbol)
