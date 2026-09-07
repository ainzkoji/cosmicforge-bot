from __future__ import annotations
import time
import logging
from typing import Dict, Optional, List
from threading import Lock

from app.models.unified_trading import InstrumentSpec
from app.exchange.interface import ExchangeClient

logger = logging.getLogger(__name__)

class InstrumentRegistry:
    """
    Central Registry for Instrument Specifications.
    Caches specs to avoid spamming 'exchangeInfo'.
    """
    def __init__(self):
        self._cache: Dict[str, Dict[str, InstrumentSpec]] = {} # broker_id -> symbol -> Spec
        self._last_refresh: Dict[str, float] = {}
        self._lock = Lock()
        
    def get_spec(self, broker_id: str, symbol: str) -> Optional[InstrumentSpec]:
        """
        Fast lookup of spec by symbol logic.
        """
        with self._lock:
            broker_cache = self._cache.get(broker_id, {})
            return broker_cache.get(symbol) or broker_cache.get(symbol.upper())

    def refresh(self, broker_id: str, client: ExchangeClient, force: bool = False):
        """
        Fetch instruments from exchange and update cache.
        Throttle unless forced.
        """
        
        now = time.time()
        with self._lock:
            last = self._last_refresh.get(broker_id, 0)
            if not force and (now - last < 3600): # 1h default throttle
                 return

        logger.info(f"Refreshing instrument registry for {broker_id}...")
        try:
            import sys
            print(f"[REGISTRY DEBUG] Calling client.list_instruments()...", file=sys.stderr)
            specs = client.list_instruments()
            print(f"[REGISTRY DEBUG] Received {len(specs)} specs from client", file=sys.stderr)
            
            new_cache = {}
            for s in specs:
                # Cache by both formats if possible, but canonical is key
                new_cache[s.symbol_canonical] = s
                new_cache[s.symbol_canonical.upper()] = s
                
            print(f"[REGISTRY DEBUG] Built cache with {len(new_cache)} entries", file=sys.stderr)
            print(f"[REGISTRY DEBUG] First 5 cache keys: {list(new_cache.keys())[:5]}", file=sys.stderr)
                
            with self._lock:
                self._cache[broker_id] = new_cache
                self._last_refresh[broker_id] = now
                
            logger.info(f"Loaded {len(specs)} instruments for {broker_id}")
            print(f"[REGISTRY DEBUG] Cache updated successfully for {broker_id}", file=sys.stderr)
            
        except Exception as e:
            import sys
            import traceback
            print(f"[REGISTRY DEBUG] ERROR in refresh: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            logger.error(f"Failed to refresh instruments for {broker_id}: {e}")
            raise


# ========== SINGLETON INSTANCE ==========
_REGISTRY_INSTANCE: InstrumentRegistry | None = None

def get_instrument_registry() -> InstrumentRegistry:
    """
    Get the GLOBAL singleton instance of InstrumentRegistry.
    CRITICAL: This must return the SAME instance across all imports.
    """
    global _REGISTRY_INSTANCE
    if _REGISTRY_INSTANCE is None:
        import sys
        print("[REGISTRY] Creating new InstrumentRegistry singleton instance", file=sys.stderr)
        _REGISTRY_INSTANCE = InstrumentRegistry()
    return _REGISTRY_INSTANCE
