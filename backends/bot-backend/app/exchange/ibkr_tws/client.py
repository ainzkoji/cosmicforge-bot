"""
IBKR TWS Client - Core connection wrapper.

Manages TCP connection to TWS/IB Gateway using ib_insync.
Handles reconnection, pacing, and health checks.
"""

import asyncio
import logging
import time
from typing import Optional, List
from ib_insync import IB, util

from .errors import IBKRConnectionError, IBKRPacingError, map_tws_error

logger = logging.getLogger(__name__)


class IBKRTwsClient:
    """
    Core TWS API client wrapper using ib_insync.
    
    Responsibilities:
    - Manage connection to TWS/Gateway
    - Handle reconnection with backoff
    - Enforce pacing limits
    - Provide health checks
    - Discover and validate accounts
    """
    
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        readonly: bool = False
    ):
        """
        Initialize IBKR TWS client.
        
        Args:
            host: TWS/Gateway host (default localhost)
            port: TWS port (7496=live, 7497=paper)
            client_id: Unique client ID for this connection
            readonly: If True, won't attempt to place orders
        """
        self.host = host
        self.port = port
        self.client_id = client_id
        self.readonly = readonly
        
        self.ib = IB()
        self._connected = False
        self._account_id: Optional[str] = None
        
        # Pacing tracking (TWS limits: ~50 req/sec, 5 orders/sec)
        self._request_times: List[float] = []
        self._order_times: List[float] = []
        
        # Error callback registration
        self.ib.errorEvent += self._on_error
    
    def _on_error(self, reqId, errorCode, errorString, contract):
        """Handle TWS error events."""
        error = map_tws_error(errorCode, errorString)
        if error:
            logger.error(f"TWS Error [{errorCode}]: {errorString}")
    
    async def connect(self, timeout: int = 30) -> bool:
        """
        Connect to TWS/Gateway asynchronously.
        
        Args:
            timeout: Connection timeout in seconds
            
        Returns:
            True if connected successfully
            
        Raises:
            IBKRConnectionError: If connection fails
        """
        if self.is_connected():
            logger.info("Already connected to TWS")
            return True
        
        try:
            logger.info(
                f"Connecting to TWS at {self.host}:{self.port} "
                f"(clientId={self.client_id}, readonly={self.readonly})"
            )
            
            await self.ib.connectAsync(
                host=self.host,
                port=self.port,
                clientId=self.client_id,
                timeout=timeout,
                readonly=self.readonly
            )
            
            self._connected = True
            
            # Discover managed accounts
            accounts = self.ib.managedAccounts()
            if accounts:
                self._account_id = accounts[0]
                logger.info(f"✅ Connected! Managed accounts: {accounts}")
            else:
                logger.warning("Connected but no managed accounts found")
            
            return True
            
        except ConnectionRefusedError as e:
            msg = (
                f"Connection refused to {self.host}:{self.port}. "
                "Ensure TWS/Gateway is running and 'Enable ActiveX and Socket Clients' "
                "is checked in API settings."
            )
            logger.error(msg)
            raise IBKRConnectionError(msg) from e
            
        except asyncio.TimeoutError as e:
            msg = f"Connection timeout after {timeout}s"
            logger.error(msg)
            raise IBKRConnectionError(msg) from e
            
        except Exception as e:
            logger.error(f"Failed to connect to TWS: {e}")
            raise IBKRConnectionError(f"Connection failed: {e}") from e
    
    def is_connected(self) -> bool:
        """Check if currently connected to TWS."""
        return self.ib.isConnected()
    
    async def disconnect(self):
        """Disconnect from TWS/Gateway."""
        if self.is_connected():
            logger.info("Disconnecting from TWS")
            self.ib.disconnect()
            self._connected = False
            self._account_id = None
    
    async def ensure_connected(self):
        """Ensure connection is active, reconnect if needed."""
        if not self.is_connected():
            logger.warning("Connection lost, attempting reconnect...")
            await self.connect()
    
    def health_check(self) -> bool:
        """
        Quick health check.
        
        Returns:
            True if connection is healthy
        """
        return self.is_connected()
    
    def check_pacing(self, operation: str = "request"):
        """
        Enforce TWS pacing limits to avoid "pacing violation" errors.
        
        Args:
            operation: "request" for general API calls, "order" for order placement
            
        Raises:
            IBKRPacingError: If rate limit would be exceeded
        """
        now = time.time()
        
        if operation == "order":
            # Conservative limit: 5 orders per second
            self._order_times = [t for t in self._order_times if now - t < 1.0]
            if len(self._order_times) >= 5:
                raise IBKRPacingError(
                    "Order rate limit exceeded (max 5 orders/sec). "
                    "Slow down order placement."
                )
            self._order_times.append(now)
        else:
            # Conservative limit: 50 requests per second
            self._request_times = [t for t in self._request_times if now - t < 1.0]
            if len(self._request_times) >= 50:
                raise IBKRPacingError(
                    "Request rate limit exceeded (max 50 requests/sec). "
                    "Slow down API calls."
                )
            self._request_times.append(now)
    
    @property
    def account_id(self) -> Optional[str]:
        """Get the current account ID."""
        return self._account_id
    
    def set_account_id(self, account_id: str):
        """
        Explicitly set account ID if user has multiple accounts.
        
        Args:
            account_id: IBKR account ID (e.g., "DU123456" for paper)
        """
        managed = self.ib.managedAccounts()
        if account_id not in managed:
            raise ValueError(
                f"Account {account_id} not in managed accounts: {managed}"
            )
        self._account_id = account_id
        logger.info(f"Account ID set to: {account_id}")
    
    def get_managed_accounts(self) -> List[str]:
        """Get list of all managed accounts."""
        return self.ib.managedAccounts()
