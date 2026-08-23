"""
IBKR Market Data Module.

Handles price fetching and historical data retrieval.
"""

import asyncio
import logging
from typing import Dict, List
from decimal import Decimal
from ib_insync import Ticker

from .errors import IBKRDataError

logger = logging.getLogger(__name__)


class IBKRMarketData:
    """
    Market data operations for IBKR TWS.
    
    Responsibilities:
    - Get current prices (bid/ask/last)
    - Get historical OHLCV data (klines)
    - Handle market data subscriptions
    """
    
    def __init__(self, client, contract_provider):
        """
        Initialize market data handler.
        
        Args:
            client: IBKRTwsClient instance
            contract_provider: IBKRContractProvider instance
        """
        self.client = client
        self.contracts = contract_provider
    
    async def get_prices(self, symbols: List[str]) -> Dict[str, Decimal]:
        """
        Get current prices for symbols.
        
        Args:
            symbols: List of canonical symbols (e.g., ["EUR_USD", "GBP_USD"])
            
        Returns:
            Dict mapping symbol -> price (uses last price or bid/ask midpoint)
        """
        self.client.check_pacing("request")
        
        prices = {}
        
        for symbol in symbols:
            try:
                contract = self.contracts.get_forex_contract(symbol)
                
                # Request snapshot market data
                ticker: Ticker = self.client.ib.reqMktData(contract, snapshot=True)
                
                # Wait for data to populate
                await asyncio.sleep(0.5)
                
                # Prefer last price, fallback to bid/ask midpoint
                if ticker.last and ticker.last > 0:
                    prices[symbol] = Decimal(str(ticker.last))
                elif ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
                    mid = (ticker.bid + ticker.ask) / 2
                    prices[symbol] = Decimal(str(mid))
                elif ticker.close and ticker.close > 0:
                    prices[symbol] = Decimal(str(ticker.close))
                else:
                    logger.warning(f"No valid price data for {symbol}")
                
                # Cancel market data to avoid subscription accumulation
                self.client.ib.cancelMktData(contract)
                
            except Exception as e:
                logger.error(f"Failed to get price for {symbol}: {e}")
        
        return prices
    
    async def get_klines(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100
    ) -> List[Dict]:
        """
        Get historical OHLCV bars (klines).
        
        Args:
            symbol: Canonical symbol (e.g., "EUR_USD")
            timeframe: Timeframe ("1m", "5m", "15m", "1h", "4h", "1d")
            limit: Number of bars to fetch
            
        Returns:
            List of kline dicts with timestamp, open, high, low, close, volume
        """
        self.client.check_pacing("request")
        
        # Map our timeframe format to IB bar size
        bar_size_map = {
            "1m": "1 min",
            "5m": "5 mins",
            "15m": "15 mins",
            "30m": "30 mins",
            "1h": "1 hour",
            "2h": "2 hours",
            "4h": "4 hours",
            "1d": "1 day",
        }
        
        bar_size = bar_size_map.get(timeframe, "1 hour")
        
        # Calculate duration string
        # IB duration format: "X S/D/W/M/Y" (seconds/days/weeks/months/years)
        if timeframe in ["1m", "5m"]:
            duration = f"{limit * 5} D"  # Approximate
        elif timeframe in ["15m", "30m", "1h"]:
            duration = f"{limit} D"
        elif timeframe in ["2h", "4h"]:
            duration = f"{limit * 2} D"
        else:  # 1d
            duration = f"{limit} D"
        
        try:
            contract = self.contracts.get_forex_contract(symbol)
            
            bars = await self.client.ib.reqHistoricalDataAsync(
                contract=contract,
                endDateTime='',  # Current time
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='MIDPOINT',  # For Forex
                useRTH=False,  # Include outside regular trading hours
                formatDate=1  # Unix timestamp
            )
            
            # Convert IB bars to our format
            klines = []
            for bar in bars:
                klines.append({
                    "timestamp": int(bar.date.timestamp() * 1000),
                    "open": float(bar.open),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "close": float(bar.close),
                    "volume": float(bar.volume) if bar.volume else 0.0
                })
            
            logger.debug(f"Fetched {len(klines)} klines for {symbol} ({timeframe})")
            return klines
            
        except Exception as e:
            logger.error(f"Failed to get klines for {symbol}: {e}")
            raise IBKRDataError(f"Historical data request failed: {e}") from e
