"""
Historical Data Provider - Fetch and cache OHLCV data for backtesting

CRYPTO NOW, FOREX-READY design:
- Normalizes all data into Binance kline list format for strategy compatibility
- Supports provider selection via data_source ('binance', 'bybit', 'bingx', future: 'oanda', 'mt5')
- Does NOT assume 24/7 trading (forex-ready)
- Optional caching via historical_candles table + in-memory cache
"""
import logging
import time
from typing import List, Iterator, Optional, Dict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    """Return current UTC time as ISO string"""
    return datetime.now(timezone.utc).isoformat()


class HistoricalDataProvider:
    """
    Provides historical candle data for backtesting.
    
    Key methods:
    - get_klines_window(): Return N candles ending at specific time (for strategy lookback)
    - iter_candles(): Sequential iterator for backtest engine
    - get_total_candles(): Count candles in date range
    
    Returns data in BINANCE kline format for compatibility with existing strategies.
    """
    
    def __init__(
        self,
        data_source: str,
        market_type: str = "crypto",
        use_db_cache: bool = True,
        use_memory_cache: bool = True,
        db = None
    ):
        """
        Args:
            data_source: 'binance', 'bybit', 'bingx' (crypto), future: 'oanda', 'mt5' (forex)
            market_type: 'crypto' or 'forex'
            use_db_cache: If True, use historical_candles table as cache
            use_memory_cache: If True, cache klines in memory during run
            db: Optional DB instance (creates new if not provided)
        """
        self.data_source = data_source
        self.market_type = market_type
        self.use_db_cache = use_db_cache
        self.use_memory_cache = use_memory_cache
        
        # DB connection for caching
        self.db = db
        if self.use_db_cache and not self.db:
            from shared_lib.persistence.db import DB
            self.db = DB()
        
        # In-memory cache: {(symbol, interval): [klines]}
        self._memory_cache: Dict[tuple, List[List]] = {}
        
        # Exchange client for fetching data
        self.client = self._build_client()
    
    def _build_client(self):
        """Build exchange client based on data_source"""
        if self.data_source == "binance" and self.market_type == "crypto":
            from app.exchange.binance.client import BinanceFuturesClient
            # Create client without credentials (public API only for historical data)
            return BinanceFuturesClient(
                api_key="",
                api_secret="",
                base_url="https://fapi.binance.com",
                recv_window=5000
            )
        elif self.data_source == "bybit" and self.market_type == "crypto":
            # Future: Bybit client
            raise NotImplementedError(f"Bybit historical data provider not yet implemented")
        elif self.data_source == "bingx" and self.market_type == "crypto":
            # Future: BingX client
            raise NotImplementedError(f"BingX historical data provider not yet implemented")
        elif self.market_type == "forex":
            # Future: Forex data provider (Oanda, MT5, etc.)
            raise NotImplementedError(f"Forex data provider not yet implemented for {self.data_source}")
        else:
            raise ValueError(f"Unknown data source: {self.data_source} for market type: {self.market_type}")
    
    def get_klines_window(
        self,
        symbol: str,
        interval: str,
        end_open_time_ms: int,
        lookback: int
    ) -> List[List]:
        """
        Return N klines ending at (or before) a specific open time.
        
        This is the primary method used by strategy analyze() functions
        to get historical context.
        
        Args:
            symbol: e.g. 'BTCUSDT'
            interval: '1m', '5m', '1h', '1d', etc.
            end_open_time_ms: Unix timestamp ms (the "current" time in backtest)
            lookback: Number of candles to return
        
        Returns:
            List of klines in BINANCE format (most recent N candles):
            [[open_time, open, high, low, close, volume, close_time, ...], ...]
        """
        # Calculate approximate start time
        interval_ms = self._get_interval_ms(interval)
        start_time_ms = end_open_time_ms - (lookback * interval_ms)
        
        # Fetch klines
        klines = self._get_klines(symbol, interval, start_time_ms, end_open_time_ms)
        
        # Filter to only include candles at or before end_open_time_ms
        klines = [k for k in klines if int(k[0]) <= end_open_time_ms]
        
        # Return last N candles
        return klines[-lookback:] if len(klines) > lookback else klines
    
    def iter_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ) -> Iterator[List]:
        """
        Yield candles sequentially for the backtest engine.
        
        This is used by BacktestRunner to iterate through time.
        
        Args:
            symbol: e.g. 'BTCUSDT'
            interval: '1m', '5m', etc.
            start_ms: Start time (Unix timestamp ms)
            end_ms: End time (Unix timestamp ms)
        
        Yields:
            Individual klines in BINANCE format:
            [open_time, open, high, low, close, volume, close_time, ...]
        """
        klines = self._get_klines(symbol, interval, start_ms, end_ms)
        
        for kline in klines:
            open_time = int(kline[0])
            if start_ms <= open_time < end_ms:
                yield kline
    
    def get_total_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ) -> int:
        """
        Count total candles in date range.
        
        Used by backtest engine to track progress.
        
        Args:
            symbol: e.g. 'BTCUSDT'
            interval: '1m', '5m', etc.
            start_ms: Start time
            end_ms: End time
        
        Returns:
            Total number of candles
        """
        klines = self._get_klines(symbol, interval, start_ms, end_ms)
        return len([k for k in klines if start_ms <= int(k[0]) < end_ms])
    
    def _get_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ) -> List[List]:
        """
        Internal method to fetch klines with caching.
        
        Priority:
        1. Memory cache (if enabled)
        2. DB cache (if enabled)
        3. Exchange API
        """
        cache_key = (symbol, interval)
        
        # 1. Check memory cache
        if self.use_memory_cache and cache_key in self._memory_cache:
            cached = self._memory_cache[cache_key]
            # Filter to requested range
            return [k for k in cached if start_ms <= int(k[0]) < end_ms]
        
        # 2. Check DB cache
        if self.use_db_cache:
            db_klines = self._get_from_db_cache(symbol, interval, start_ms, end_ms)
            if db_klines:
                logger.debug(
                    f"DB cache hit: {symbol} {interval} {start_ms}-{end_ms} "
                    f"({len(db_klines)} candles)"
                )
                # Store in memory cache
                if self.use_memory_cache:
                    self._memory_cache[cache_key] = db_klines
                return db_klines
        
        # 3. Fetch from exchange API
        logger.info(
            f"Fetching from {self.data_source}: {symbol} {interval} "
            f"{start_ms}-{end_ms}"
        )
        klines = self._fetch_from_exchange(symbol, interval, start_ms, end_ms)
        
        # Save to DB cache
        if klines and self.use_db_cache:
            self._save_to_db_cache(symbol, interval, klines)
        
        # Save to memory cache
        if klines and self.use_memory_cache:
            self._memory_cache[cache_key] = klines
        
        return klines
    
    def _fetch_from_exchange(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ) -> List[List]:
        """
        Fetch klines from exchange API with pagination.
        
        Returns data in BINANCE format (normalized from all sources).
        """
        if self.data_source == "binance":
            # Use the new historical_klines method we added
            if hasattr(self.client, 'historical_klines'):
                return self.client.historical_klines(symbol, interval, start_ms, end_ms)
            else:
                # Fallback to manual pagination
                return self._fetch_binance_manual(symbol, interval, start_ms, end_ms)
        
        elif self.data_source == "bybit":
            # Future: Bybit historical fetch + normalize to Binance format
            raise NotImplementedError("Bybit historical fetch not implemented")
        
        elif self.data_source == "bingx":
            # Future: BingX historical fetch + normalize to Binance format
            raise NotImplementedError("BingX historical fetch not implemented")
        
        else:
            raise ValueError(f"Unknown data source: {self.data_source}")
    
    def _fetch_binance_manual(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ) -> List[List]:
        """Manual pagination fallback for Binance (if historical_klines not available)"""
        all_klines = []
        current_start = start_ms
        limit = 1500
        
        while current_start < end_ms:
            batch = self.client.klines(symbol, interval, limit)
            
            if not batch:
                break
            
            # Filter batch to requested range
            batch = [k for k in batch if start_ms <= int(k[0]) < end_ms]
            all_klines.extend(batch)
            
            # Move to next batch
            if batch:
                last_close_time = int(batch[-1][6])
                current_start = last_close_time + 1
            else:
                break
            
            # Rate limiting
            if current_start < end_ms:
                time.sleep(0.1)
        
        return all_klines
    
    def _get_from_db_cache(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ) -> Optional[List[List]]:
        """Retrieve klines from historical_candles table"""
        if not self.db:
            return None
        
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT open_time, open, high, low, close, volume,
                           quote_volume, trades
                    FROM historical_candles
                    WHERE symbol = ?
                      AND interval = ?
                      AND open_time >= ?
                      AND open_time < ?
                      AND data_source = ?
                      AND market_type = ?
                    ORDER BY open_time ASC
                    """,
                    (symbol, interval, start_ms, end_ms, 
                     self.data_source, self.market_type)
                ).fetchall()
                
                if not rows:
                    return None
                
                # Convert to Binance klines format
                klines = []
                for row in rows:
                    open_time = row['open_time']
                    close_time = self._calculate_close_time(open_time, interval)
                    
                    # Binance format: [open_time, open, high, low, close, volume, 
                    #                  close_time, quote_volume, trades, taker_buy_base, taker_buy_quote, ignore]
                    klines.append([
                        open_time,
                        str(row['open']),
                        str(row['high']),
                        str(row['low']),
                        str(row['close']),
                        str(row['volume']),
                        close_time,
                        str(row['quote_volume'] or 0),
                        row['trades'] or 0,
                        "0",  # Taker buy base asset volume
                        "0",  # Taker buy quote asset volume
                        "0"   # Ignore
                    ])
                
                return klines
                
        except Exception as e:
            logger.warning(f"DB cache retrieval failed: {e}")
            return None
    
    def _save_to_db_cache(
        self,
        symbol: str,
        interval: str,
        klines: List[List]
    ):
        """Save klines to historical_candles table"""
        if not self.db:
            return
        
        try:
            now = utc_now_iso()
            
            with self.db.connect() as conn:
                for kline in klines:
                    # Parse kline (Binance format)
                    open_time = int(kline[0])
                    open_price = float(kline[1])
                    high = float(kline[2])
                    low = float(kline[3])
                    close = float(kline[4])
                    volume = float(kline[5])
                    quote_volume = float(kline[7]) if len(kline) > 7 else 0
                    trades = int(kline[8]) if len(kline) > 8 else 0
                    
                    # Insert or ignore (UNIQUE constraint handles duplicates)
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO historical_candles (
                            symbol, interval, open_time,
                            open, high, low, close, volume,
                            quote_volume, trades,
                            market_type, quote_currency,
                            data_source, fetched_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            symbol, interval, open_time,
                            open_price, high, low, close, volume,
                            quote_volume, trades,
                            self.market_type, "USDT",  # TODO: Parse from symbol
                            self.data_source, now
                        )
                    )
            
            logger.debug(f"Cached {len(klines)} candles to DB for {symbol} {interval}")
            
        except Exception as e:
            logger.warning(f"Failed to cache klines to DB: {e}")
    
    def _get_interval_ms(self, interval: str) -> int:
        """Get interval duration in milliseconds"""
        mapping = {
            "1m": 60 * 1000,
            "3m": 3 * 60 * 1000,
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "30m": 30 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "2h": 2 * 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
            "6h": 6 * 60 * 60 * 1000,
            "12h": 12 * 60 * 60 * 1000,
            "1d": 24 * 60 * 60 * 1000,
            "3d": 3 * 24 * 60 * 60 * 1000,
            "1w": 7 * 24 * 60 * 60 * 1000,
        }
        return mapping.get(interval, 60 * 1000)
    
    def _calculate_close_time(self, open_time: int, interval: str) -> int:
        """Calculate candle close time from open time and interval"""
        interval_ms = self._get_interval_ms(interval)
        return open_time + interval_ms - 1
    
    def clear_memory_cache(self):
        """Clear in-memory cache (useful between backtest runs)"""
        self._memory_cache.clear()
    
    def preload_data(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int
    ):
        """
        Preload data into memory cache before running backtest.
        
        This can improve performance by fetching all data upfront.
        """
        logger.info(f"Preloading data: {symbol} {interval} {start_ms}-{end_ms}")
        klines = self._get_klines(symbol, interval, start_ms, end_ms)
        logger.info(f"Preloaded {len(klines)} candles into cache")


def prefetch_historical_data(
    symbol: str,
    interval: str,
    start_date: str,
    end_date: str,
    data_source: str = "binance",
    market_type: str = "crypto"
) -> int:
    """
    Utility function to prefetch and cache historical data.
    
    Useful for warming the cache before running backtests.
    
    Args:
        symbol: e.g. 'BTCUSDT'
        interval: '1m', '5m', etc.
        start_date: ISO date string 'YYYY-MM-DD' or ISO datetime
        end_date: ISO date string
        data_source: 'binance', 'bybit', etc.
        market_type: 'crypto' or 'forex'
    
    Returns:
        Number of candles fetched and cached
    """
    # Parse dates
    if 'T' not in start_date:
        start_date += 'T00:00:00Z'
    if 'T' not in end_date:
        end_date += 'T23:59:59Z'
    
    start_ts = int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp() * 1000)
    end_ts = int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp() * 1000)
    
    # Create provider and fetch data
    provider = HistoricalDataProvider(
        data_source=data_source,
        market_type=market_type,
        use_db_cache=True,
        use_memory_cache=False  # Don't store in memory for prefetch
    )
    
    klines = provider._get_klines(symbol, interval, start_ts, end_ts)
    
    logger.info(
        f"Prefetched {len(klines)} candles for {symbol} {interval} "
        f"({start_date} to {end_date})"
    )
    
    return len(klines)
