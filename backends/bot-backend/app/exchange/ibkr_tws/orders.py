"""
IBKR Order Management Module.

Handles order placement, cancellation, and status tracking.
"""

import asyncio
import logging
import time
from typing import Optional, List
from decimal import Decimal
from ib_insync import MarketOrder, LimitOrder, StopOrder, Order as IBOrder, Trade

from app.models.unified_trading import UnifiedOrder, OrderStatus, Side
from .errors import IBKROrderError

logger = logging.getLogger(__name__)


class IBKROrderManager:
    """
    Order management for IBKR TWS.
    
    Responsibilities:
    - Place market/limit/stop orders
    - Place bracket orders (entry + SL + TP)
    - Cancel orders
    - Track order status
    - Convert between lots and IB quantity units
    """
    
    def __init__(self, client, contract_provider):
        """
        Initialize order manager.
        
        Args:
            client: IBKRTwsClient instance
            contract_provider: IBKRContractProvider instance
        """
        self.client = client
        self.contracts = contract_provider
    
    async def place_order(
        self,
        symbol: str,
        side: Side,
        qty: Decimal,  # In LOTS (1.0 = 100,000 base currency units)
        order_type: str = "market",
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        stop_loss: Optional[Decimal] = None,
        take_profit: Optional[Decimal] = None,
        reduce_only: bool = False
    ) -> UnifiedOrder:
        """
        Place an order.
        
        Args:
            symbol: Canonical symbol (e.g., "EUR_USD")
            side: Side.BUY or Side.SELL
            qty: Quantity in LOTS (1.0 = 100,000 base units)
            order_type: "market", "limit", "stop", "stop_limit"
            price: Limit price (for limit orders)
            stop_price: Stop price (for stop orders)
            stop_loss: Stop loss price (for bracket orders)
            take_profit: Take profit price (for bracket orders)
            reduce_only: Ignored for IBKR (positions are net)
            
        Returns:
            UnifiedOrder with order details
        """
        self.client.check_pacing("order")
        
        # Get contract and spec
        contract = self.contracts.get_forex_contract(symbol)
        spec = await self.contracts.get_instrument_spec(symbol)
        
        # Convert LOTS to IB quantity (base currency units)
        # qty=1.0 lots = 100,000 base currency units
        ib_quantity = float(qty * spec.contract_size)
        
        # Determine action
        action = "BUY" if side == Side.BUY else "SELL"
        
        # Create IB order based on type
        order_type_lower = order_type.lower()
        
        if order_type_lower == "market":
            ib_order = MarketOrder(action, ib_quantity)
        elif order_type_lower == "limit":
            if not price:
                raise IBKROrderError("Limit order requires price")
            ib_order = LimitOrder(action, ib_quantity, float(price))
        elif order_type_lower == "stop":
            if not stop_price:
                raise IBKROrderError("Stop order requires stop_price")
            ib_order = StopOrder(action, ib_quantity, float(stop_price))
        else:
            raise IBKROrderError(f"Unsupported order type: {order_type}")
        
        # TODO: Implement bracket orders if stop_loss or take_profit specified
        # For now, log a warning
        if stop_loss or take_profit:
            logger.warning(
                "Bracket orders (SL/TP) not yet implemented. "
                "Place separate orders for stop loss and take profit."
            )
        
        try:
            # Place order
            trade: Trade = self.client.ib.placeOrder(contract, ib_order)
            
            # Wait briefly for order acknowledgment
            await asyncio.sleep(0.5)
            
            # Convert to UnifiedOrder
            unified_order = self._trade_to_unified_order(trade, symbol, side, qty, order_type_lower)
            
            logger.info(
                f"Order placed: {symbol} {side.value} {qty} lots "
                f"({ib_quantity} units) - Order ID: {trade.order.orderId}"
            )
            
            return unified_order
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            raise IBKROrderError(f"Order placement failed: {e}") from e
    
    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Broker order ID
            
        Returns:
            True if cancellation successful
        """
        try:
            # Find trade by order ID
            trades = [
                t for t in self.client.ib.trades()
                if str(t.order.orderId) == order_id
            ]
            
            if not trades:
                logger.warning(f"Order {order_id} not found in active trades")
                return False
            
            trade = trades[0]
            self.client.ib.cancelOrder(trade.order)
            
            logger.info(f"Cancelled order: {order_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    async def get_order(self, order_id: str) -> Optional[UnifiedOrder]:
        """
        Get order status.
        
        Args:
            order_id: Broker order ID
            
        Returns:
            UnifiedOrder if found, None otherwise
        """
        trades = [
            t for t in self.client.ib.trades()
            if str(t.order.orderId) == order_id
        ]
        
        if not trades:
            return None
        
        trade = trades[0]
        
        # Extract symbol from contract (best effort)
        symbol = "UNKNOWN"
        if hasattr(trade.contract, 'symbol'):
            base = trade.contract.symbol
            quote = trade.contract.currency
            symbol = f"{base}_{quote}"
        
        # Determine side
        side = Side.BUY if trade.order.action == "BUY" else Side.SELL
        
        # Convert quantity back to lots
        ib_qty = trade.order.totalQuantity
        qty_lots = Decimal(str(ib_qty / 100000))
        
        return self._trade_to_unified_order(
            trade, symbol, side, qty_lots, trade.order.orderType.lower()
        )
    
    def _trade_to_unified_order(
        self,
        trade: Trade,
        symbol: str,
        side: Side,
        qty: Decimal,
        order_type: str
    ) -> UnifiedOrder:
        """
        Convert IB Trade to UnifiedOrder.
        
        Args:
            trade: ib_insync Trade object
            symbol: Canonical symbol
            side: Order side
            qty: Quantity in lots
            order_type: Order type string
            
        Returns:
            UnifiedOrder instance
        """
        # Map IB order status to our OrderStatus
        status_map = {
            "PendingSubmit": OrderStatus.NEW,
            "PendingCancel": OrderStatus.PENDING_CANCEL,
            "PreSubmitted": OrderStatus.NEW,
            "Submitted": OrderStatus.NEW,
            "ApiPending": OrderStatus.NEW,
            "Filled": OrderStatus.FILLED,
            "Cancelled": OrderStatus.CANCELED,
            "Inactive": OrderStatus.REJECTED,
            "ApiCancelled": OrderStatus.CANCELED,
        }
        
        ib_status = trade.orderStatus.status if trade.orderStatus else "Unknown"
        status = status_map.get(ib_status, OrderStatus.NEW)
        
        # Get filled quantity (convert back to lots)
        filled_units = trade.orderStatus.filled if trade.orderStatus else 0
        qty_filled = Decimal(str(filled_units / 100000)) if filled_units > 0 else Decimal("0")
        
        # Get average fill price
        avg_fill_price = None
        if trade.orderStatus and trade.orderStatus.avgFillPrice > 0:
            avg_fill_price = Decimal(str(trade.orderStatus.avgFillPrice))
        
        return UnifiedOrder(
            client_order_id="",  # IB doesn't use client order IDs in same way
            broker_order_id=str(trade.order.orderId),
            symbol=symbol,
            side=side,
            type=order_type,
            qty_ordered=qty,
            qty_filled=qty_filled,
            avg_fill_price=avg_fill_price,
            status=status,
            timestamp=int(time.time() * 1000),
            reduce_only=False
        )
