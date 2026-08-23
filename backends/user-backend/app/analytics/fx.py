from decimal import Decimal
from datetime import datetime
from typing import Optional, Protocol, Dict
import logging

from app.analytics.models import MonetaryValue

logger = logging.getLogger(__name__)

# Fallback static rates for Phase 2 (Stablecoins)
STATIC_RATES = {
    "USDT": 1.0,
    "USDC": 1.0, 
    "USD": 1.0,
    "DAI": 1.0
}

class FXProvider(Protocol):
    async def get_rate(self, from_ccy: str, to_ccy: str, ts_utc: Optional[datetime] = None) -> Optional[Decimal]:
        ...

class SimpleFXService:
    """
    A lightweight FX service for converting monetary values.
    
    Phase 2 Strategy:
    1. Identity: If currencies match, return 1.0
    2. Stablecoins: Treat USDT/USDC/USD as 1:1
    3. Cross-rates: Not fully implemented (returns None if no path found)
    """
    
    def __init__(self):
        self._cache: Dict[str, Decimal] = {}

    def get_rate_sync(self, from_ccy: str, to_ccy: str) -> Optional[Decimal]:
        """
        Synchronous rate retrieval (for simple stablecoin logic).
        """
        f = from_ccy.upper()
        t = to_ccy.upper()
        
        # 1. Identity
        if f == t:
            return Decimal("1.0")
            
        # 2. Stablecoins (Approximate 1:1 for reporting simplicity in Phase 2)
        if f in STATIC_RATES and t in STATIC_RATES:
            return Decimal("1.0")
        
        # 3. Known hardcoded pairs (Placeholder for future API integration)
        # e.g. BTC -> USDT could be fetched from a recent price cache
        
        return None

    async def convert(
        self, 
        amount: Decimal, 
        from_ccy: str, 
        to_ccy: str, 
        ts_utc: Optional[datetime] = None
    ) -> Optional[Decimal]:
        """
        Converts an amount from one currency to another.
        """
        rate = self.get_rate_sync(from_ccy, to_ccy)
        
        if rate is not None:
            return amount * rate
            
        # TODO: Implement Async fetch from external source or DB for historical rates
        # rate = await self.fetch_historical_rate(...)
        
        logger.warning(f"FX Conversion failed: {from_ccy} -> {to_ccy} (Rate not found)")
        return None

    def normalize_to_reporting_currency(
        self, 
        value: MonetaryValue, 
        reporting_currency: str
    ) -> MonetaryValue:
        """
        Helper to take a MonetaryValue and force convert it.
        If conversion fails, it retains original currency but logs a warning
        (or could raise error based on strictness).
        """
        converted_amount = self.get_rate_sync(value.currency, reporting_currency)
        
        if converted_amount is not None:
             return MonetaryValue(
                 amount=value.amount * converted_amount,
                 currency=reporting_currency
             )
        
        return value

# Singleton instance
fx_service = SimpleFXService()
