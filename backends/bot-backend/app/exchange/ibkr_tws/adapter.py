"""
IBKR TWS Adapter - ExchangeClient Implementation.

This is the main interface the executor sees.
Wraps all IBKR TWS functionality to provide the standard ExchangeClient interface.
"""

import asyncio
import logging
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
)

from .client import IBKRTwsClient
from .contracts import IBKRContractProvider
from .market_data import IBKRMarketData
from .orders import IBKROrderManager
from .positions import IBKRPositionManager
from .capabilities import IBKR_TWS_CAPABILITIES
from .errors import IBKRError, IBKRConnectionError

logger = logging.getLogger(__name__)


class IBKRTwsAdapter:
    """
    IBKR TWS ExchangeClient Adapter.
    
    Implements the ExchangeClient protocol that the executor expects.
    No executor code changes needed - this adapter translates all calls.
    
    Responsibilities:
    - Initialize and manage TWS connection
    - Provide all ExchangeClient interface methods
    - Ensure executor remains IBKR-agnostic
    """
    
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        account_id: Optional[str] = None,
        readonly: bool = False
    ):
        """
        Initialize IBKR TWS Adapter.
        
        Args:
            host: TWS/Gateway host
            port: TWS port (7496=live, 7497=paper)
            client_id: Unique client ID
            account_id: Specific account ID (if multiple accounts)
            readonly: If True, trading disabled
        """
        self.host = host
        self.port = port
        self.client_id = client_id
        self.readonly = readonly
        
        # Core client
        self.client = IBKRTwsClient(host, port, client_id, readonly)
        
        # Sub-modules
        self.contracts = IBKRContractProvider(self.client)
        self.market_data = IBKRMarketData(self.client, self.contracts)
        self.orders = IBKROrderManager(self.client, self.contracts)
        self.positions = IBKRPositionManager(self.client, self.contracts)
        
        # State
        self._connected = False
        self._instruments_cache: Optional[List[InstrumentSpec]] = None
        
        # Set account if provided
        if account_id:
            self.client.set_account_id(account_id)
    
    async def connect(self) -> bool:
        """Connect to TWS/Gateway."""
        try:
            await self.client.connect()
            self._connected = True
            logger.info("IBKR TWS Adapter connected successfully")
            return True
        except IBKRConnectionError as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from TWS/Gateway."""
        await self.client.disconnect()
        self._connected = False
    
    def is_connected(self) -> bool:
        """Check connection status."""
        return self.client.is_connected()
    
    # =========================================================================
    # ExchangeClient Interface Implementation
    # =========================================================================
    
    async def list_instruments(self, use_cache: bool = True) -> List[InstrumentSpec]:
        """
        List all available instruments.
        
        Args:
            use_cache: If True, use cached list
            
        Returns:
            List of InstrumentSpec
        """
        if use_cache and self._instruments_cache:
            return self._instruments_cache
        
        instruments = await self.contracts.list_instruments()
        self._instruments_cache = instruments
        
        return instruments
    
    async def get_prices(self, symbols: List[str]) -> Dict[str, Decimal]:
        """
        Get current prices for symbols.
        
        Args:
            symbols: List of canonical symbols
            
        Returns:
            Dict mapping symbol -> price
        """
        return await self.market_data.get_prices(symbols)
    
    async def get_klines(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100
    ) -> List[Dict]:
        """
        Get historical klines/candles.
        
        Args:
            symbol: Canonical symbol
            timeframe: Timeframe string
            limit: Number of candles
            
        Returns:
            List of OHLCV dicts
        """
        return await self.market_data.get_klines(symbol, timeframe, limit)
    
    async def place_order(self, request: OrderRequest) -> UnifiedOrder:
        """
        Place an order.
        
        Args:
            request: OrderRequest with all order details
            
        Returns:
            UnifiedOrder with order status
        """
        return await self.orders.place_order(
            symbol=request.symbol,
            side=request.side,
            qty=request.qty,
            order_type=request.type.value if isinstance(request.type, OrderType) else request.type,
            price=request.price,
            stop_price=getattr(request, 'stop_price', None),
            reduce_only=request.reduce_only
        )
    
    async def place_protection(
        self,
        request: ProtectionRequest
    ) -> ProtectionResult:
        """
        Place stop loss and/or take profit orders.
        
        Args:
            request: ProtectionRequest with SL/TP details
            
        Returns:
            ProtectionResult with order IDs
        """
        # For IBKR, we place separate stop/limit orders
        sl_order = None
        tp_order = None
        
        if request.stop_loss_price:
            # Place stop loss as opposite side stop order
            opposite_side = Side.SELL if request.side == Side.BUY else Side.BUY
            sl_order = await self.orders.place_order(
                symbol=request.symbol,
                side=opposite_side,
                qty=request.qty,
                order_type="stop",
                stop_price=request.stop_loss_price
            )
        
        if request.take_profit_price:
            # Place take profit as opposite side limit order
            opposite_side = Side.SELL if request.side == Side.BUY else Side.BUY
            tp_order = await self.orders.place_order(
                symbol=request.symbol,
                side=opposite_side,
                qty=request.qty,
                order_type="limit",
                price=request.take_profit_price
            )
        
        return ProtectionResult(
            stop_loss_order_id=sl_order.broker_order_id if sl_order else None,
            take_profit_order_id=tp_order.broker_order_id if tp_order else None
        )
    
    async def get_positions(self) -> List[UnifiedPosition]:
        """
        Get all open positions.
        
        Returns:
            List of UnifiedPosition
        """
        return await self.positions.get_positions()
    
    async def get_balance(self) -> Dict[str, Decimal]:
        """
        Get account balance.
        
        Returns:
            Dict with wallet, equity, available, margin_used
        """
        return await self.positions.get_balance()
    
    async def close_position(
        self,
        symbol: str,
        qty: Optional[Decimal] = None
    ) -> UnifiedOrder:
        """
        Close a position by placing opposite order.
        
        Args:
            symbol: Symbol to close
            qty: Quantity to close (None = close entire position)
            
        Returns:
            UnifiedOrder for the closing order
        """
        # Get current position
        positions = await self.get_positions()
        target_position = None
        
        for pos in positions:
            if pos.symbol == symbol:
                target_position = pos
                break
        
        if not target_position:
            raise IBKRError(f"No open position found for {symbol}")
        
        # Determine close qty and side
        close_qty = qty if qty else target_position.quantity
        close_side = Side.SELL if target_position.side == Side.BUY else Side.BUY
        
        # Place market order to close
        return await self.orders.place_order(
            symbol=symbol,
            side=close_side,
            qty=close_qty,
            order_type="market"
        )
    
    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Broker order ID
            
        Returns:
            True if cancelled successfully
        """
        return await self.orders.cancel_order(order_id)
    
    async def get_order(self, order_id: str) -> Optional[UnifiedOrder]:
        """
        Get order status.
        
        Args:
            order_id: Broker order ID
            
        Returns:
            UnifiedOrder if found
        """
        return await self.orders.get_order(order_id)
    
    def get_capabilities(self):
        """Get broker capabilities."""
        return IBKR_TWS_CAPABILITIES
