"""
MetaTrader Adapter
Implements ExchangeClient protocol for MT4/MT5 bridge integration.
"""
from typing import List, Dict, Optional, Any
from decimal import Decimal
import time
import uuid
import os
from datetime import datetime, timezone

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
    IdempotencyMode,
    OrderType
)
from app.exchange.interface import ExchangeClient, BrokerCapabilities
from app.exchange.mt_bridge.client import MTBridgeClient
from app.exchange.mt_bridge.errors import MTBridgeError
from app.exchange.mt_bridge.capabilities import MT_CAPABILITIES

import logging
logger = logging.getLogger(__name__)


class MetaTraderBridgeAdapter(ExchangeClient):
    """
    MetaTrader Adapter implementing unified ExchangeClient interface.
    Works for both MT4 and MT5 through common bridge API.
    """
    
    def __init__(self, client: MTBridgeClient, platform: str = "mt5"):
        """
        Args:
            client: MTBridgeClient instance
            platform: "mt4" or "mt5" (for metadata/logging)
        """
        self._client = client
        self._platform = platform
        self._capabilities = MT_CAPABILITIES
        # Cache for instrument validation
        self._instruments_cache: Dict[str, Any] = {}
        self._instruments_last_update: float = 0.0
    
    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._capabilities
    
    def get_server_time(self) -> int:
        """Get server time in milliseconds"""
        health = self._client.get_health()
        time_str = health.get('time')
        
        if time_str:
            # Parse ISO8601 to timestamp
            try:
                dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            except:
                pass
        
        # Fallback to local time
        return int(time.time() * 1000)
    
    def list_instruments(self) -> List[InstrumentSpec]:
        """
        Fetch available instruments from bridge.
        Maps MT instrument specs to unified InstrumentSpec.
        """
        instruments = self._client.get_instruments()
        specs = []
        
        for inst in instruments:
            # Extract fields from bridge response
            symbol = inst.get('symbol', '')
            base = inst.get('base', '')
            quote = inst.get('quote', '')
            digits = inst.get('digits', 5)
            tick_size = Decimal(str(inst.get('tick_size', 0.00001)))
            contract_size = Decimal(str(inst.get('contract_size', 100000)))
            min_lot = Decimal(str(inst.get('min_lot', 0.01)))
            lot_step = Decimal(str(inst.get('lot_step', 0.01)))
            
            # Determine asset class (assume Forex for MT)
            asset_class = AssetClass.FOREX_SPOT
            
            specs.append(InstrumentSpec(
                symbol_canonical=symbol,
                symbol_exchange=symbol,
                asset_class=asset_class,
                base_currency=base,
                quote_currency=quote,
                margin_currency=quote,  # Usually quote for Forex
                settlement_currency=quote,
                
                contract_size=contract_size,
                tick_size=tick_size,
                step_size=lot_step,
                min_qty=min_lot,
                min_notional=None,
                
                price_precision=digits,
                qty_precision=2,  # Lots usually 2 decimals
                
                max_leverage=Decimal("500"),  # MT typically allows high leverage
                supports_per_order_leverage=False
            ))
        
        return specs
    
    def get_prices(self, symbols: List[str]) -> Dict[str, Decimal]:
        """
        Get current bid/ask prices.
        Returns mid price (bid+ask)/2 for simplicity.
        """
        prices_data = self._client.get_prices(symbols)
        result = {}
        
        for symbol, price_info in prices_data.items():
            bid = Decimal(str(price_info.get('bid', 0)))
            ask = Decimal(str(price_info.get('ask', 0)))
            # Return mid price
            result[symbol] = (bid + ask) / Decimal("2")
        
        return result
    
    def get_klines(self, symbol: str, interval: str, limit: int) -> List[Any]:
        """
        Fetch candle data from bridge.
        Maps interval from standard format to MT timeframe.
        """
        # Map standard intervals to MT timeframes
        interval_map = {
            '1m': 'M1',
            '5m': 'M5',
            '15m': 'M15',
            '30m': 'M30',
            '1h': 'H1',
            '4h': 'H4',
            '1d': 'D1'
        }
        
        mt_timeframe = interval_map.get(interval, 'H1')
        candles = self._client.get_klines(symbol, mt_timeframe, limit)
        
        # Return raw candles for now (format compatible with existing code)
        return candles
    
    
    def _ensure_instruments_cache(self):
        """Lazy load instruments for validation"""
        now = time.time()
        if now - self._instruments_last_update > 3600 or not self._instruments_cache:  # 1 hour cache
            try:
                instruments = self._client.get_instruments()
                # Map by symbol
                self._instruments_cache = {i['symbol']: i for i in instruments}
                self._instruments_last_update = now
            except Exception as e:
                logger.warning(f"Failed to refresh instruments cache: {e}")

    def place_order(self, req: OrderRequest) -> UnifiedOrder:
        """
        Place order through MT bridge.
        Note: MT uses LOTS for quantity, not units.
        """
        # Map side
        side_str = "buy" if req.side == Side.BUY else "sell"
        
        # Map order type
        order_type_map = {
            OrderType.MARKET: "market",
            OrderType.LIMIT: "limit",
            OrderType.STOP: "stop"
        }
        order_type_str = order_type_map.get(req.type, "market")
        
        # Generate client order ID if not provided
        client_order_id = req.client_order_id or f"mt_{uuid.uuid4().hex[:12]}"

        # --- QUANTITY GUARDRAILS ---
        try:
            qty_val = float(req.qty)
            max_lots = float(os.environ.get("MT_MAX_LOTS_DEFAULT", "10"))
            
            if qty_val <= 0:
                raise ValueError(f"Quantity must be positive (got {qty_val})")
            
            if qty_val > max_lots:
                raise ValueError(f"Quantity {qty_val} exceeds safety limit of {max_lots} LOTS. (Did you send units?)")
                
            # Validate against instrument spec
            self._ensure_instruments_cache()
            spec = self._instruments_cache.get(req.symbol)
            if spec:
                min_lot = float(spec.get('min_lot', 0))
                max_lot_inst = float(spec.get('max_lot', 0))
                step_lot = float(spec.get('lot_step', 0))
                
                if min_lot > 0 and qty_val < min_lot:
                     raise ValueError(f"Quantity {qty_val} below min_lot {min_lot}")
                
                if max_lot_inst > 0 and qty_val > max_lot_inst:
                     raise ValueError(f"Quantity {qty_val} exceeds instrument max_lot {max_lot_inst}")
                
        except ValueError as e:
            return UnifiedOrder(
                client_order_id=client_order_id,
                broker_order_id="",
                symbol=req.symbol,
                side=req.side,
                type=order_type_str,
                qty_ordered=req.qty,
                qty_filled=Decimal("0"),
                avg_fill_price=None,
                status=OrderStatus.REJECTED,
                timestamp=int(time.time() * 1000),
                reduce_only=False,
                error_message=str(e)
            )

        # Build order payload
        order_data = {
            'client_order_id': client_order_id,
            'symbol': req.symbol,
            'side': side_str,
            'order_type': order_type_str,
            'qty': qty_val,  # LOTS (Already float)
        }
        
        # Add price for limit/stop
        if req.price is not None:
            order_data['price'] = float(req.price)
        
        # Add SL/TP if provided
        if req.sl_price is not None:
            order_data['sl'] = float(req.sl_price)
        if req.tp_price is not None:
            order_data['tp'] = float(req.tp_price)
        
        # Place order
        try:
            resp = self._client.place_order(order_data)
            return self._map_order(resp, req.symbol, client_order_id)
        except MTBridgeError as e:
            logger.error(f"MT order placement failed: {e}")
            # Return rejected order
            return UnifiedOrder(
                client_order_id=client_order_id,
                broker_order_id="",
                symbol=req.symbol,
                side=req.side,
                type=order_type_str,
                qty_ordered=req.qty,
                qty_filled=Decimal("0"),
                avg_fill_price=None,
                status=OrderStatus.REJECTED,
                timestamp=int(time.time() * 1000),
                reduce_only=False,
                error_message=str(e)
            )
    
    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an order by ID"""
        try:
            resp = self._client.cancel_order(order_id)
            return resp.get('ok', False)
        except MTBridgeError:
            return False
    
    def get_order(self, symbol: str, order_id: str) -> UnifiedOrder:
        """Get order status by ID"""
        order_data = self._client.get_order(order_id)
        return self._map_order(order_data, symbol, order_id)
    
    def list_open_orders(self, symbol: Optional[str] = None) -> List[UnifiedOrder]:
        """
        List open orders.
        Note: MT bridge doesn't support filtering by symbol in v1.
        """
        # MT bridge doesn't have list_open_orders endpoint in contract
        # Return empty for now (positions are more relevant for MT)
        logger.warning("list_open_orders not fully supported by MT bridge v1")
        return []
    
    def get_fills(self, symbol: str, start_time: int, limit: int = 100) -> List[UnifiedFill]:
        """
        Get trade fills.
        Not supported in bridge v1 contract.
        """
        logger.warning("get_fills not supported by MT bridge v1")
        return []
    
    def place_protection(self, req: ProtectionRequest) -> ProtectionResult:
        """
        Place SL/TP on existing position.
        MT allows modifying position SL/TP but requires ticket ID.
        For v1, return unsupported (SL/TP should be attached to entry order).
        """
        logger.warning("place_protection not fully supported in MT bridge v1 (attach SL/TP to order instead)")
        return ProtectionResult(
            status="unsupported",
            error="Use attached SL/TP when placing order"
        )
    
    def get_positions(self) -> List[UnifiedPosition]:
        """
        Fetch all open positions from bridge.
        """
        positions_data = self._client.get_positions()
        positions = []
        
        for pos in positions_data:
            symbol = pos.get('symbol', '')
            ticket = pos.get('ticket', '')
            side_str = pos.get('side', 'buy')
            lots = Decimal(str(pos.get('lots', 0)))
            open_price = Decimal(str(pos.get('open_price', 0)))
            profit = Decimal(str(pos.get('profit', 0)))
            
            # Parse side
            side = Side.BUY if side_str == 'buy' else Side.SELL
            
            # Get current price (would need to fetch, use open_price as fallback)
            current_price = open_price  # Simplified
            
            # SL/TP
            sl = pos.get('sl')
            tp = pos.get('tp')
            
            positions.append(UnifiedPosition(
                symbol=symbol,
                broker_id=self._platform,
                position_id=ticket,  # MT ticket ID
                side=side,
                quantity=lots,
                entry_price=open_price,
                current_price=current_price,
                unrealized_pnl=profit,
                realized_pnl=Decimal("0"),  # Not in snapshot
                margin_used=Decimal("0"),  # Not provided by bridge
                leverage=Decimal("1"),  # Not provided
                mode=PositionMode.TICKET,
                timestamp=int(time.time() * 1000)
            ))
        
        return positions
    
    def get_balance(self) -> Dict[str, Decimal]:
        """
        Fetch account balance from bridge.
        """
        balance_data = self._client.get_balance()
        
        balance = Decimal(str(balance_data.get('balance', 0)))
        equity = Decimal(str(balance_data.get('equity', 0)))
        free_margin = Decimal(str(balance_data.get('free_margin', 0)))
        
        return {
            "wallet": balance,
            "equity": equity,
            "available": free_margin
        }
    
    def close_position(self, symbol: str, position_id: Optional[str] = None) -> UnifiedOrder:
        """
        Close a specific position by ticket ID.
        MT requires ticket ID to close specific position.
        """
        if not position_id:
            raise ValueError("position_id (ticket) required for MT close_position")
        
        # For MT, closing is done by placing opposite order
        # Bridge should handle this with a dedicated close endpoint (not in v1 contract)
        # For now, raise not implemented
        raise NotImplementedError("close_position requires bridge enhancement for ticket-based close")
    
    # ==========================================
    # INTERNAL HELPERS
    # ==========================================
    
    def _map_order(self, order_data: Dict[str, Any], symbol: str, client_order_id: str) -> UnifiedOrder:
        """
        Map bridge order response to UnifiedOrder.
        """
        # Parse status
        status_str = order_data.get('status', 'unknown')
        status_map = {
            'filled': OrderStatus.FILLED,
            'accepted': OrderStatus.NEW,
            'rejected': OrderStatus.REJECTED,
            'pending': OrderStatus.NEW
        }
        status = status_map.get(status_str, OrderStatus.NEW)
        
        # Extract quantities
        filled_qty = Decimal(str(order_data.get('filled_qty', 0)))
        avg_price = order_data.get('avg_price')
        
        return UnifiedOrder(
            client_order_id=client_order_id,
            broker_order_id=str(order_data.get('order_id', '')),
            symbol=symbol,
            side=Side.BUY,  # Would need to track from request
            type="market",  # Would need to track from request
            qty_ordered=filled_qty,  # Simplified
            qty_filled=filled_qty,
            avg_fill_price=Decimal(str(avg_price)) if avg_price else None,
            status=status,
            timestamp=int(time.time() * 1000),
            reduce_only=False
        )
