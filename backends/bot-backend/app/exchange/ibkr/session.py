import logging
import asyncio
import time
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
        # connection_id -> time.monotonic() of the last get_session() for it.
        self._last_used: Dict[str, float] = {}
        self._initialized = True
        logger.info("IBKRSessionManager (TWS) initialized")

    # Sessions are keyed by a per-request connection id, so without a bound an
    # API caller could open one per call and they would all stay connected.
    MAX_SESSIONS = 32
    SESSION_IDLE_TTL_SECONDS = 15 * 60

    def close_session(self, connection_id: str) -> bool:
        """Disconnect and forget one session. True if it existed."""
        self._last_used.pop(connection_id, None)
        session = self._sessions.pop(connection_id, None)
        if session is None:
            return False
        try:
            session.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting IBKR session {connection_id}: {e}")
        return True

    def _evict_sessions(self, now: Optional[float] = None) -> None:
        """Drop idle sessions, then the least recently used ones beyond MAX_SESSIONS - 1."""
        now = time.monotonic() if now is None else now
        for connection_id in list(self._sessions):
            if now - self._last_used.get(connection_id, now) > self.SESSION_IDLE_TTL_SECONDS:
                self.close_session(connection_id)
        while len(self._sessions) >= self.MAX_SESSIONS:
            oldest = min(self._sessions, key=lambda cid: self._last_used.get(cid, 0.0))
            self.close_session(oldest)

    async def get_session(self, connection_id: str, host: str = "127.0.0.1", port: int = 7496, client_id: int = 1) -> IBKRSession:
        """
        Get or create a session.
        connection_id: Unique identifier for this connection request (UUID)
        """
        # For simplicity in this refactor, we usually have ONE TWS.
        # But we'll key by connection_id to support multiple if needed later.

        if connection_id in self._sessions:
            session = self._sessions[connection_id]
            self._last_used[connection_id] = time.monotonic()
            if session.is_connected():
                return session
            # Try reconnect?
            await session.connect()
            return session

        # Create new
        session = IBKRSession(host, port, client_id=client_id)
        connected = await session.connect()

        if connected:
            self._evict_sessions()
            self._sessions[connection_id] = session
            self._last_used[connection_id] = time.monotonic()
            return session
        else:
            raise ConnectionError(f"Could not connect to TWS at {host}:{port}")

    def get_existing_session(self, connection_id: str) -> Optional[IBKRSession]:
        return self._sessions.get(connection_id)
