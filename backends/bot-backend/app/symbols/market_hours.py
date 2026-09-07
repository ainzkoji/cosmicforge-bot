from __future__ import annotations
import logging
from datetime import datetime, time as dtime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from app.models.unified_trading import AssetClass

logger = logging.getLogger(__name__)

# Standard Forex Hours (New York Time)
# Opens: Sunday 5:00 PM ET
# Closes: Friday 5:00 PM ET
# Daily Break: 5:00 PM - 5:05 PM ET (Common rollout)

class ForexSessionGuard:
    """
    Guard validation for Forex Market Hours.
    Uses 'America/New_York' as the standard reference time.
    """
    
    TZ_NY = ZoneInfo("America/New_York")
    
    # Weekday constants (Monday=0, Sunday=6)
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6
    
    MARKET_CLOSE_HOUR = 17 # 5 PM
    MARKET_CLOSE_MINUTE = 0
    
    # Optional Daily Break (Rollover protection)
    # Block trades slightly around rollover to avoid spread spikes
    ROLLOVER_START = dtime(16, 59)
    ROLLOVER_END = dtime(17, 15) # 15 min buffer safety
    
    @classmethod
    def is_market_open(cls, asset_class: AssetClass, timestamp_ms: int) -> bool:
        """
        Check if market is open for trading.
        """
        # 1. Crypto is always open (mostly)
        if asset_class in (AssetClass.CRYPTO_PERP, AssetClass.CRYPTO_SPOT):
            return True
            
        # 2. Forex / CFDs Logic
        # Convert ms timestamp to NY Time
        dt_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=ZoneInfo("UTC"))
        dt_ny = dt_utc.astimezone(cls.TZ_NY)
        
        weekday = dt_ny.weekday()
        t = dt_ny.time()
        
        # --- WEEKEND RULE ---
        # Closed from Friday 5PM to Sunday 5PM
        
        if weekday == cls.FRIDAY:
            # Close after 5:00 PM
            if t.hour >= cls.MARKET_CLOSE_HOUR:
                return False
                
        elif weekday == cls.SATURDAY:
            # Closed all day
            return False
            
        elif weekday == cls.SUNDAY:
            # Open after 5:00 PM
            if t.hour < cls.MARKET_CLOSE_HOUR:
                return False
                
        # --- DAILY ROLLOVER RULE (Mon-Thu) ---
        # Spreads widen massively at 5PM ET. Safer to block.
        if weekday in (0, 1, 2, 3): # Mon-Thu
            if cls.ROLLOVER_START <= t <= cls.ROLLOVER_END:
                return False
                
        return True

    @classmethod
    def get_status_reason(cls, asset_class: AssetClass, timestamp_ms: int) -> str:
        """Helper to return human readable reason if closed."""
        if cls.is_market_open(asset_class, timestamp_ms):
            return "OPEN"
            
        # If we are here, it's closed, let's find why
        # (Re-running logic simplified for messaging)
        dt_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=ZoneInfo("UTC"))
        dt_ny = dt_utc.astimezone(cls.TZ_NY)
        weekday = dt_ny.weekday()
        
        if weekday == cls.SATURDAY:
            return "Weekend Closed (Saturday)"
        if weekday == cls.FRIDAY and dt_ny.hour >= 17:
            return "Weekend Closed (Friday > 5PM ET)"
        if weekday == cls.SUNDAY and dt_ny.hour < 17:
             return "Weekend Closed (Sunday < 5PM ET)"
             
        # Must be rollover
        return "Daily Rollover Break (Spread Protection)"
