"""
Analytics Cache Service

Provides caching mechanism for heavy analytics queries using SQLite.
Includes decorator for easy function caching.
"""
import functools
import hashlib
import json
import logging
import inspect
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Callable, Dict, List
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

class AnalyticsCacheService:
    """Service to handle analytics result caching"""
    
    def __init__(self, db: Optional[DB] = None):
        self.db = db or DB()
        
    def get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached result if valid"""
        now = datetime.now(timezone.utc).isoformat()
        
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT metrics_json 
                FROM analytics_cache 
                WHERE cache_key = ? AND expires_at > ?
                """,
                [cache_key, now]
            ).fetchone()
            
        if row and row[0]:
            try:
                return json.loads(row[0])
            except json.JSONDecodeError:
                return None
        return None

    def set(
        self, 
        cache_key: str, 
        data: Any, 
        ttl_seconds: int,
        user_id: str,
        bot_instance_id: Optional[str] = None,
        broker_account_id: Optional[str] = None,
        timeframe: Optional[str] = None
    ):
        """Store result in cache"""
        now_dt = datetime.now(timezone.utc)
        expires_dt = now_dt + timedelta(seconds=ttl_seconds)
        
        try:
            json_data = json.dumps(data)
            
            with self.db.connect() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO analytics_cache (
                        cache_key, user_id, bot_instance_id, broker_account_id, 
                        timeframe, computed_at, expires_at, metrics_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        cache_key, 
                        user_id, 
                        bot_instance_id, 
                        broker_account_id,
                        timeframe,
                        now_dt.isoformat(),
                        expires_dt.isoformat(),
                        json_data
                    ]
                )
        except Exception as e:
            logger.error(f"Failed to cache analytics data: {e}")

    def invalidate(self, user_id: str, bot_instance_id: Optional[str] = None, broker_account_id: Optional[str] = None):
        """Invalidate cache entries based on scope"""
        where_clauses = ["user_id = ?"]
        params = [user_id]
        
        if bot_instance_id:
            where_clauses.append("bot_instance_id = ?")
            params.append(bot_instance_id)
            
        if broker_account_id:
            where_clauses.append("broker_account_id = ?")
            params.append(broker_account_id)
            
        where_sql = " AND ".join(where_clauses)
        
        try:
            with self.db.connect() as conn:
                conn.execute(
                    f"DELETE FROM analytics_cache WHERE {where_sql}",
                    params
                )
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")


def cache_analytics(ttl_seconds: int = 300):
    """
    Decorator to cache analytics function results.
    
    Arguments MUST include 'user_id' for invalidation/key generation.
    Optional args used for key: 'bot_instance_id', 'broker_account_id', 'timeframe', 'strategy', 'environment'.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Parse args to find user_id and others
            # Combine args and kwargs into a single dict for inspection
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            all_args = bound_args.arguments
            
            user_id = all_args.get("user_id")
            if not user_id:
                # Can't cache effectively without user context
                return func(*args, **kwargs)
                
            # Generate Cache Key
            # Use function name + sorted relevant args
            # We exclude 'self' if it's a method
            relevant_args = {k: v for k, v in all_args.items() if k != 'self'}
            
            # Simple consistent serialization for key
            key_str = f"{func.__name__}:{json.dumps(relevant_args, sort_keys=True, default=str)}"
            cache_key = hashlib.sha256(key_str.encode('utf-8')).hexdigest()
            
            # Helpers for storing metadata
            bot_id = all_args.get("bot_instance_id")
            broker_id = all_args.get("broker_account_id")
            timeframe = all_args.get("timeframe")
            
            # 1. Try Cache
            service = AnalyticsCacheService()
            cached = service.get(cache_key)
            if cached:
                return cached
            
            # 2. Compute
            result = func(*args, **kwargs)
            
            # 3. Store Cache
            if result:
                service.set(
                    cache_key=cache_key,
                    data=result,
                    ttl_seconds=ttl_seconds,
                    user_id=user_id,
                    bot_instance_id=bot_id,
                    broker_account_id=broker_id,
                    timeframe=timeframe
                )
                
            return result
        return wrapper
    return decorator
