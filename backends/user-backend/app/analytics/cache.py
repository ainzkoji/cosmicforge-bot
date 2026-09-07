from typing import Any, Optional
import json
from datetime import timedelta

# Simple in-memory cache for Phase 2
# Designed to be replaced by Redis later

class AnalyticsCache:
    def __init__(self):
        self._store = {}
        
    def get(self, key: str) -> Optional[Any]:
        return self._store.get(key)
        
    def set(self, key: str, value: Any, ttl: int = 300):
        self._store[key] = value

    def invalidate(self, key_pattern: str):
        # Naive implementation
        keys_to_remove = [k for k in self._store if key_pattern in k]
        for k in keys_to_remove:
            del self._store[k]

analytics_cache = AnalyticsCache()
