"""
Broker Health Monitor

Monitors broker/exchange health and connectivity:
- Time synchronization
- Rate limit status
- API responsiveness
- Recent error tracking
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from app.exchange.binance.client import BinanceFuturesClient

logger = logging.getLogger(__name__)


@dataclass
class BrokerHealth:
    """Broker health status."""
    broker_id: str
    is_healthy: bool
    time_sync_ok: bool
    rate_limit_ok: bool
    api_responsive: bool
    last_error: Optional[str] = None
    last_check: str = ""
    
    def __post_init__(self):
        if not self.last_check:
            self.last_check = datetime.now(timezone.utc).isoformat()


class BrokerHealthMonitor:
    """
    Monitors broker/exchange health.
    
    Checks:
    - Time synchronization (prevents -1021 errors)
    - Rate limit headroom
    - API ping/response time
    - Recent error tracking
    """
    
    def __init__(
        self,
        max_time_drift_ms: int = 5000,  # 5 seconds
        min_rate_limit_weight: int = 100,  # Minimum headroom
        max_ping_ms: int = 1000,  # Max acceptable ping
        error_window_minutes: int = 5  # Track errors in last N minutes
    ):
        self.max_time_drift_ms = max_time_drift_ms
        self.min_rate_limit_weight = min_rate_limit_weight
        self.max_ping_ms = max_ping_ms
        self.error_window_minutes = error_window_minutes
        
        self._recent_errors: Dict[str, list] = {}  # broker_id -> [(timestamp, error), ...]
    
    def check_health(
        self,
        broker_id: str,
        client: 'BinanceFuturesClient'
    ) -> BrokerHealth:
        """
        Check broker health.
        
        Args:
            broker_id: Identifier (e.g., "binance_futures")
            client: Exchange client
            
        Returns:
            BrokerHealth status
        """
        time_sync_ok = True
        rate_limit_ok = True
        api_responsive = True
        last_error = None
        
        try:
            # Check 1: Time synchronization
            try:
                server_time = client.server_time()
                local_time = int(time.time() * 1000)
                time_drift = abs(server_time - local_time)
                
                if time_drift > self.max_time_drift_ms:
                    time_sync_ok = False
                    last_error = f"Time drift {time_drift}ms exceeds {self.max_time_drift_ms}ms"
                    logger.warning(f"[{broker_id}] {last_error}")
            except Exception as e:
                time_sync_ok = False
                last_error = f"Time sync failed: {str(e)}"
                logger.error(f"[{broker_id}] {last_error}")
            
            # Check 2: API ping
            try:
                start = time.time()
                client.ping()
                ping_ms = (time.time() - start) * 1000
                
                if ping_ms > self.max_ping_ms:
                    api_responsive = False
                    last_error = f"High ping {ping_ms:.0f}ms"
                    logger.warning(f"[{broker_id}] {last_error}")
            except Exception as e:
                api_responsive = False
                last_error = f"API ping failed: {str(e)}"
                logger.error(f"[{broker_id}] {last_error}")
            
            # Check 3: Rate limit (if available)
            # Note: Binance returns rate limit info in headers
            # This is a simplified check - actual implementation would need response headers
            try:
                # Get exchange info which has low weight
                info = client.exchange_info_cached()
                # If we can get it, rate limit is probably OK
                # More sophisticated: track X-MBX-USED-WEIGHT header
                rate_limit_ok = True
            except Exception as e:
                rate_limit_ok = False
                last_error = f"Rate limit check failed: {str(e)}"
                logger.error(f"[{broker_id}] {last_error}")
            
            # Check 4: Recent error rate
            if broker_id in self._recent_errors:
                cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.error_window_minutes)
                recent = [
                    (ts, err) for ts, err in self._recent_errors[broker_id]
                    if datetime.fromisoformat(ts) > cutoff
                ]
                self._recent_errors[broker_id] = recent
                
                if len(recent) >= 10:  # Too many errors in window
                    last_error = f"{len(recent)} errors in last {self.error_window_minutes} minutes"
                    logger.error(f"[{broker_id}] High error rate: {last_error}")
        
        except Exception as e:
            # Catch-all for unexpected errors
            last_error = f"Health check failed: {str(e)}"
            logger.error(f"[{broker_id}] {last_error}")
            time_sync_ok = False
            api_responsive = False
        
        is_healthy = time_sync_ok and rate_limit_ok and api_responsive
        
        return BrokerHealth(
            broker_id=broker_id,
            is_healthy=is_healthy,
            time_sync_ok=time_sync_ok,
            rate_limit_ok=rate_limit_ok,
            api_responsive=api_responsive,
            last_error=last_error
        )
    
    def record_error(self, broker_id: str, error: str):
        """Record an error for tracking."""
        if broker_id not in self._recent_errors:
            self._recent_errors[broker_id] = []
        
        timestamp = datetime.now(timezone.utc).isoformat()
        self._recent_errors[broker_id].append((timestamp, error))
        
        # Limit list size
        if len(self._recent_errors[broker_id]) > 100:
            self._recent_errors[broker_id] = self._recent_errors[broker_id][-100:]
    
    def clear_errors(self, broker_id: str):
        """Clear error history for a broker."""
        if broker_id in self._recent_errors:
            self._recent_errors[broker_id] = []
