"""
Engine Client - HTTP client for calling bot-backend engine service
"""
import httpx
from typing import Dict, Any, Optional, List
from app.core.config import settings


class EngineClient:
    """HTTP client for communicating with bot-backend engine service"""
    
    def __init__(self):
        self.base_url = getattr(settings, 'ENGINE_URL', 'http://localhost:9000')
        self.api_key = getattr(settings, 'ENGINE_API_KEY', 'default-engine-key')
        self.headers = {"X-ENGINE-KEY": self.api_key}
        self.timeout = 30.0
    
    async def get_templates(self) -> List[Dict[str, Any]]:
        """Get available strategy templates from engine"""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/engine/templates",
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return data.get("strategies", [])
    
    async def start_run(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Start runner loop in engine"""
        params = payload or {}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/engine/run",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            return response.json()
    
    async def stop_run(self) -> Dict[str, Any]:
        """Stop runner loop in engine"""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/engine/stop",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
    
    async def get_status(self) -> Dict[str, Any]:
        """Get current runner status from engine"""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/engine/status",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
    
    async def get_events_tail(self, limit: int = 50) -> Dict[str, Any]:
        """Get recent events from engine"""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/engine/events/tail",
                headers=self.headers,
                params={"limit": limit}
            )
            response.raise_for_status()
            return response.json()
    
    async def get_health(self) -> Dict[str, Any]:
        """Get engine health status"""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/engine/health",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
    
    async def get_binance_balance(self) -> Dict[str, Any]:
        """Get Binance account balance from engine"""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/engine/binance/balance",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
    
    async def get_binance_price(self, symbol: str) -> Dict[str, Any]:
        """Get current price for a symbol from engine"""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/engine/binance/price",
                headers=self.headers,
                params={"symbol": symbol}
            )
            response.raise_for_status()
            return response.json()


# Singleton instance
_engine_client: Optional[EngineClient] = None


def get_engine_client() -> EngineClient:
    """Get singleton engine client instance"""
    global _engine_client
    if _engine_client is None:
        _engine_client = EngineClient()
    return _engine_client
