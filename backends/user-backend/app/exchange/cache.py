"""
Exchange Data Cache - Fast access to account/position data

Eliminates redundant Binance API calls by caching data with TTL.
Runner updates cache on each cycle, endpoints read from cache.
"""
from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class CachedPosition:
    """Cached position data."""
    symbol: str
    side: str  # "LONG" or "SHORT"
    qty: float
    entry_price: float
    notional: float
    unrealized_pnl: float
    leverage: int
    margin_type: str
    updated_at: float = field(default_factory=time.time)


@dataclass
class CachedAccount:
    """Cached account data."""
    equity: float  # Total equity/balance
    available_balance: float
    margin_used: float
    unrealized_pnl: float
    updated_at: float = field(default_factory=time.time)


class ExchangeDataCache:
    """
    Thread-safe cache for exchange data.
    
    Updated by runner on each cycle.
    Read by API endpoints instantly (no network calls).
    """
    
    _instance: Optional['ExchangeDataCache'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_cache()
        return cls._instance
    
    def _init_cache(self):
        self._account: Optional[CachedAccount] = None
        self._positions: Dict[str, CachedPosition] = {}
        self._raw_account: Dict[str, Any] = {}
        self._raw_positions: List[Dict[str, Any]] = []
        self._last_update: float = 0.0
        self._cache_lock = threading.RLock()
    
    def update_from_exchange(
        self,
        account_data: Dict[str, Any],
        position_data: List[Dict[str, Any]],
    ):
        """
        Update cache with fresh exchange data.
        Called by runner on each cycle.
        """
        with self._cache_lock:
            # Cache raw data
            self._raw_account = account_data
            self._raw_positions = position_data
            
            # Parse account
            self._account = CachedAccount(
                equity=float(account_data.get("totalWalletBalance", 0)),
                available_balance=float(account_data.get("availableBalance", 0)),
                margin_used=float(account_data.get("totalInitialMargin", 0)),
                unrealized_pnl=float(account_data.get("totalUnrealizedProfit", 0)),
            )
            
            # Parse positions
            self._positions.clear()
            for p in position_data:
                qty = float(p.get("positionAmt", 0))
                if abs(qty) > 0:
                    symbol = p.get("symbol", "")
                    self._positions[symbol] = CachedPosition(
                        symbol=symbol,
                        side="LONG" if qty > 0 else "SHORT",
                        qty=abs(qty),
                        entry_price=float(p.get("entryPrice", 0)),
                        notional=abs(float(p.get("notional", 0))),
                        unrealized_pnl=float(p.get("unRealizedProfit", 0)),
                        leverage=int(p.get("leverage", 1)),
                        margin_type=p.get("marginType", "cross"),
                    )
            
            self._last_update = time.time()
    
    def get_account(self) -> Optional[CachedAccount]:
        """Get cached account data (instant)."""
        with self._cache_lock:
            return self._account
    
    def get_positions(self) -> Dict[str, CachedPosition]:
        """Get cached positions (instant)."""
        with self._cache_lock:
            return dict(self._positions)
    
    def get_position(self, symbol: str) -> Optional[CachedPosition]:
        """Get cached position for symbol (instant)."""
        with self._cache_lock:
            return self._positions.get(symbol)
    
    def get_raw_account(self) -> Dict[str, Any]:
        """Get raw account data for compatibility."""
        with self._cache_lock:
            return dict(self._raw_account)
    
    def get_raw_positions(self) -> List[Dict[str, Any]]:
        """Get raw position data for compatibility."""
        with self._cache_lock:
            return list(self._raw_positions)
    
    def get_equity(self) -> float:
        """Get equity (instant)."""
        with self._cache_lock:
            return self._account.equity if self._account else 0.0
    
    def get_margin_used(self) -> float:
        """Get margin used (instant)."""
        with self._cache_lock:
            return self._account.margin_used if self._account else 0.0
    
    def get_position_count(self) -> int:
        """Get number of open positions (instant)."""
        with self._cache_lock:
            return len(self._positions)
    
    def get_gross_exposure(self) -> float:
        """Get total notional exposure (instant)."""
        with self._cache_lock:
            return sum(p.notional for p in self._positions.values())
    
    def get_cache_age_seconds(self) -> float:
        """How old is the cache?"""
        return time.time() - self._last_update if self._last_update > 0 else float('inf')
    
    def is_stale(self, max_age_seconds: float = 120.0) -> bool:
        """Check if cache is too old."""
        return self.get_cache_age_seconds() > max_age_seconds


# Singleton accessor
def get_exchange_cache() -> ExchangeDataCache:
    """Get the exchange data cache singleton."""
    return ExchangeDataCache()
