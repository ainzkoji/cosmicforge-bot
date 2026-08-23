import logging
import asyncio
from typing import Optional, Dict, Any, List
# catch import error if user hasn't installed it yet
try:
    from ib_insync import IB, util
except ImportError:
    IB = None
    util = None

# Set loop for ib_insync if needed, but in FastAPI we rely on the main loop
# util.patchAsyncio() 

logger = logging.getLogger(__name__)

class IBKRSession:
    """
    Manages the TWS/Gateway TCP session using ib_insync.
    Maintains a persistent connection.
    """
    def __init__(self, host: str, port: int, client_id: int):
        if IB is None:
            raise ImportError("ib_insync not installed. Please run `pip install ib_insync`")
            
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()
        
        # Prevent ib_insync form taking over logging completely
        # logging.getLogger("ib_insync").setLevel(logging.WARNING)

    async def connect(self) -> bool:
        """Connect to TWS/Gateway asynchronously."""
        if self.ib.isConnected():
            return True
            
        try:
            logger.info(f"Connecting to IBKR TWS at {self.host}:{self.port} clientId={self.client_id}")
            await self.ib.connectAsync(self.host, self.port, self.client_id)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to TWS: {e}")
            return False

    def is_connected(self) -> bool:
        return self.ib.isConnected()
        
    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    async def ensure_connected(self):
        """Ensure connection is active, reconnect if needed."""
        if not self.ib.isConnected():
            await self.connect()

class IBKRSessionManager:
    """
    Manages IBKR sessions per account/broker connection.
    Since TWS usually allows only one client ID per connection (or multiple with different IDs),
    we typically map one 'account' in our system to one TWS connection.
    
    For now, we assume a single TWS instance for simplicity, but design allows extension.
    """
    
    _instance: Optional['IBKRSessionManager'] = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
            
        self._sessions: Dict[str, IBKRSession] = {}
        self._initialized = True
        logger.info("IBKRSessionManager (TWS) initialized")
    
    async def get_session(self, connection_id: str, host: str = "127.0.0.1", port: int = 7496, client_id: int = 1) -> IBKRSession:
        """
        Get or create a session.
        connection_id: Unique identifier for this connection request (UUID)
        """
        # For simplicity in this refactor, we usually have ONE TWS.
        # But we'll key by connection_id to support multiple if needed later.
        
        if connection_id in self._sessions:
            session = self._sessions[connection_id]
            if session.is_connected():
                return session
            # Try reconnect?
            await session.connect()
            return session
            
        # Create new
        session = IBKRSession(host, port, client_id=client_id) 
        connected = await session.connect()
        
        if connected:
            self._sessions[connection_id] = session
            return session
        else:
            raise ConnectionError(f"Could not connect to TWS at {host}:{port}")

    def get_existing_session(self, connection_id: str) -> Optional[IBKRSession]:
        return self._sessions.get(connection_id)
