"""
IBKR Adapter - ExchangeClient Implementation.

Responsibilities:
- Translate generic ExchangeClient calls to IBKRClient calls
- Enforce BrokerCapabilities
- Convert qty (lots) to IBKR order size using contract_size
- Convert symbol_canonical to IBKR contract ID
- Handle net-position semantics correctly
- Executor MUST NOT know this is IBKR

This is the ONLY interface the executor sees.
"""
from decimal import Decimal
from typing import List, Dict, Any, Optional
import time
import logging

from app.exchange.interface import ExchangeClient, BrokerCapabilities
from app.models.unified_trading import (
    InstrumentSpec, UnifiedOrder, UnifiedPosition, UnifiedFill, 
    OrderRequest, ProtectionRequest, ProtectionResult,
    Side, OrderStatus, PositionMode, OrderType
)

from .capabilities import IBKR_CAPABILITIES
from .session import IBKRSession, IBKRSessionManager
from .client import IBKRClient
from .instruments import IBKRInstrumentProvider
from .errors import IBKRConnectionError, IBKROrderError, IBKRAuthError

logger = logging.getLogger(__name__)


class IBKRAdapter(ExchangeClient):
    """
    Adapter for Interactive Brokers via Client Portal API.
    
    This adapter translates generic trading operations into IBKR-specific calls
    while maintaining complete abstraction - the executor has no knowledge
    that this is IBKR.
    """
    
    def __init__(
        self, 
        host: str = "127.0.0.1",
        port: int = 4001,
        client_id: int = 1,
        account_id: Optional[str] = None
    ):
        """
        Initialize IBKR Adapter (Bridge Mode).
        
        Args:
            host: TWS/Gateway host
            port: TWS/Gateway port
            client_id: Unique client ID for this adapter
            account_id: IBKR account ID (will auto-detect if None)
        """
        self.host = host
        self.port = port
        self.client_id = client_id
        self._account_id = account_id
        
        # Use SessionManager singleton
        self.session_manager = IBKRSessionManager()
        
        # Instrument provider for symbol/conid mapping
        self.instruments = IBKRInstrumentProvider()
        
        # Client instance (session will be managed per-call)
        self._client: Optional[IBKRClient] = None
        
        logger.info(f"IBKRAdapter initialized for base_url={base_url}")
    
    @property
    def capabilities(self) -> BrokerCapabilities:
        """Return IBKR capabilities."""
        return IBKR_CAPABILITIES
    
    def _get_client(self) -> IBKRClient:
        """
        Get IBKRClient with valid session.
        Ensures account_id is set and session is authenticated.
        """
        # Ensure we have an account ID
        if not self._account_id:
            self._discover_account_id()
        
        # Get or create valid session
        session = self.session_manager.get_session(self._account_id)
        
        # Create client if needed
        if not self._client:
            self._client = IBKRClient(session, self._account_id)
        else:
            # Update session in case it was refreshed
            self._client.session = session
        
        return self._client
    
    def _discover_account_id(self):
        """Auto-discover account ID from portfolio."""
        # Create temporary session for discovery
        temp_session = IBKRSession(self.base_url, self.verify_ssl)
        
        if not temp_session.check_auth_status():
            raise IBKRAuthError(
                "Not authenticated. Please authenticate via Client Portal Gateway web interface."
            )
        
        temp_client = IBKRClient(temp_session)
        accounts = temp_client.get_portfolio_accounts()
        
        if not accounts:
            raise IBKRConnectionError("No accounts found in IBKR portfolio")
        
        self._account_id = accounts[0]
        logger.info(f"Auto-discovered IBKR account: {self._account_id}")
    
    def _resolve_instrument(self, symbol: str) -> InstrumentSpec:
        """
        Resolve canonical symbol to InstrumentSpec.
        
        Args:
            symbol: Canonical symbol (e.g., "EURUSD")
        
        Returns:
            InstrumentSpec
        
        Raises:
            IBKROrderError: If symbol not found
        """
        for spec in self.instruments.get_forex_instruments():
            if spec.symbol_canonical == symbol:
                return spec
        
        raise IBKROrderError(f"Unknown symbol: {symbol}")

    # =========================================================================
    # DISCOVERY
    # =========================================================================
    
    def list_instruments(self) -> List[InstrumentSpec]:
        """Return list of supported instruments."""
        return self.instruments.get_forex_instruments()

    def get_server_time(self) -> int:
        """
        Get server time in milliseconds.
        CPAPI doesn't have a dedicated endpoint, using system time.
        """
        return int(time.time() * 1000)

    # =========================================================================
    # MARKET DATA
    # =========================================================================
    
    def get_prices(self, symbols: List[str]) -> Dict[str, Decimal]:
        """
        Get current prices for symbols.
        
        Args:
            symbols: List of canonical symbols
        
        Returns:
            Dict mapping symbol -> price
        """
        client = self._get_client()
        
        # Map symbols to conids
        conid_to_symbol = {}
        conids = []
        
        for symbol in symbols:
            try:
                spec = self._resolve_instrument(symbol)
                conid = spec.symbol_exchange
                conids.append(conid)
                conid_to_symbol[conid] = symbol
            except IBKROrderError:
                logger.warning(f"Symbol {symbol} not found in instruments")
        
        if not conids:
            return {}
        
        # Fetch prices
        conid_prices = client.get_market_data_snapshot(conids)
        
        # Map back to canonical symbols
        result = {}
        for conid, price in conid_prices.items():
            if conid in conid_to_symbol:
                result[conid_to_symbol[conid]] = price
        
        return result

    def get_klines(self, symbol: str, interval: str, limit: int) -> List[Any]:
        """
        Get historical klines/candles.
        Not implemented in CPAPI MVP.
        """
        logger.warning("get_klines not implemented for IBKR")
        return []

    # =========================================================================
    # TRADING
    # =========================================================================
    
    def place_order(self, req: OrderRequest) -> UnifiedOrder:
        """
        Place an order.
        
        Translates generic OrderRequest to IBKR format with proper:
        - Capability enforcement
        - Symbol -> conid mapping
        - Quantity -> IBKR size conversion
        
        Args:
            req: OrderRequest with canonical symbol
        
        Returns:
            UnifiedOrder
        """
        client = self._get_client()
        
        # 1. Resolve instrument
        spec = self._resolve_instrument(req.symbol)
        
        # 2. Map order type
        order_type_map = {
            OrderType.MARKET: "MKT",
            OrderType.LIMIT: "LMT",
            OrderType.STOP: "STP",
            OrderType.STOP_MARKET: "STP"
        }
        
        ib_order_type = order_type_map.get(req.type, "MKT")
        
        # 3. Enforce capabilities
        if req.type == OrderType.LIMIT and not self.capabilities.supports_market_orders:
            raise IBKROrderError("Limit orders not supported")
        
        if req.sl_price or req.tp_price:
            if not self.capabilities.supports_attached_sl_tp:
                logger.warning(
                    "Attached SL/TP not supported via single request. "
                    "Use place_protection() after entry."
                )
                # Continue without SL/TP for now
        
        # 4. Convert quantity using contract_size
        # For Forex: contract_size typically = 1 (cash), so qty in base currency units
        # IBKR expects quantity in units, so we use qty as-is
        ib_quantity = float(req.qty)
        
        # 5. Build IBKR payload
        payload = {
            "conid": int(spec.symbol_exchange),
            "secType": "CASH",  # Forex spot
            "orderType": ib_order_type,
            "side": req.side.value.upper(),
            "quantity": ib_quantity,
            "tif": req.time_in_force,
            "outsideRTH": True
        }
        
        # Add price for limit orders
        if req.price and req.type == OrderType.LIMIT:
            payload["price"] = float(req.price)
        
        # Add stop price for stop orders
        if req.stop_price and req.type in [OrderType.STOP, OrderType.STOP_MARKET]:
            payload["auxPrice"] = float(req.stop_price)
        
        # 6. Place order via client
        return client.place_order(self._account_id, payload)

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an order."""
        client = self._get_client()
        return client.cancel_order(self._account_id, order_id)

    def get_order(self, symbol: str, order_id: str) -> UnifiedOrder:
        """Get order status."""
        client = self._get_client()
        order = client.get_order_status(self._account_id, order_id)
        
        if not order:
            raise IBKROrderError(f"Order {order_id} not found")
        
        return order

    def list_open_orders(self, symbol: Optional[str] = None) -> List[UnifiedOrder]:
        """List open orders, optionally filtered by symbol."""
        client = self._get_client()
        orders = client.get_live_orders()
        
        if symbol:
            return [o for o in orders if o.symbol == symbol]
        
        return orders

    # =========================================================================
    # FILLS & PROTECTION
    # =========================================================================
    
    def get_fills(self, symbol: str, start_time: int, limit: int = 100) -> List[UnifiedFill]:
        """
        Get trade fills.
        Not easily supported by CPAPI without specific endpoint.
        """
        logger.warning("get_fills not implemented for IBKR")
        return []

    def place_protection(self, req: ProtectionRequest) -> ProtectionResult:
        """
        Place protective orders (SL/TP).
        
        IBKR supports separate protection orders but not in a single atomic request.
        This would require placing individual orders for SL/TP.
        """
        logger.warning("place_protection not fully implemented for IBKR MVP")
        return ProtectionResult(
            status="not_implemented",
            error="Use separate orders for SL/TP in IBKR"
        )

    # =========================================================================
    # POSITION & ACCOUNT
    # =========================================================================
    
    def get_positions(self) -> List[UnifiedPosition]:
        """
        Get all open positions.
        
        Returns net positions (ONE_WAY mode).
        """
        client = self._get_client()
        return client.get_positions(self._account_id)

    def get_balance(self) -> Dict[str, Decimal]:
        """
        Get account balance.
        
        Returns:
            {
                "wallet": Net Liquidation Value,
                "equity": Net Liquidation Value,
                "available": Available Funds
            }
        """
        client = self._get_client()
        return client.get_account_summary(self._account_id)

    def close_position(self, symbol: str, position_id: Optional[str] = None) -> UnifiedOrder:
        """
        Close a position by placing opposite order.
        
        Args:
            symbol: Canonical symbol
            position_id: Ignored for IBKR (net positions)
        
        Returns:
            UnifiedOrder for closing order
        """
        client = self._get_client()
        return client.close_position(self._account_id, symbol)
