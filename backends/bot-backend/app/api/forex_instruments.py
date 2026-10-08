"""
Forex Instruments API

Provides dynamic forex instrument listings from broker APIs or fallback configuration.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Literal
import logging
from datetime import datetime, timedelta

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.symbols.universe import parse_symbols

# Authentication is declared on the router so a route added later cannot be
# public by omission.
router = APIRouter(dependencies=[Depends(get_current_user_id)])
logger = logging.getLogger(__name__)

# Simple in-memory cache with TTL
_instruments_cache: Dict[str, tuple[datetime, Any]] = {}
CACHE_TTL_SECONDS = 3600  # 1 hour
# The cache key contains caller-chosen values (broker_id, environment,
# broker_account_id), so the cache is bounded: expired entries are swept on
# every write, the oldest entries are evicted beyond CACHE_MAX_ENTRIES, and an
# oversized key is never stored.
CACHE_MAX_ENTRIES = 512
CACHE_MAX_KEY_LENGTH = 256


class ForexInstrument(BaseModel):
    """Forex instrument detail."""
    symbol: str
    base: str
    quote: str
    pip_location: Optional[int] = None
    margin_rate: Optional[float] = None


class ForexInstrumentsResponse(BaseModel):
    """Response for forex instruments listing."""
    broker_id: str
    source: Literal["broker", "fallback"]
    instruments: List[ForexInstrument]
    cached: bool = False


def parse_forex_symbol(symbol: str) -> tuple[str, str]:
    """
    Parse forex symbol into base and quote currencies.
    
    Examples:
        EUR_USD -> (EUR, USD)
        EURUSD -> (EUR, USD)
    """
    # Remove underscore if present
    clean_symbol = symbol.replace("_", "")
    
    # Most forex pairs are 6 characters (3+3)
    if len(clean_symbol) == 6:
        return clean_symbol[:3], clean_symbol[3:]
    
    # Handle edge cases (e.g., XAUUSD = gold)
    # For simplicity, split in half
    mid = len(clean_symbol) // 2
    return clean_symbol[:mid], clean_symbol[mid:]


def get_fallback_instruments() -> List[ForexInstrument]:
    """Get fallback forex instruments from settings.FOREX_SYMBOLS."""
    fallback_symbols = parse_symbols(settings.FOREX_SYMBOLS)
    
    if not fallback_symbols:
        # Strict Validation: Do not provide hardcoded defaults
        return []
    
    instruments = []
    for symbol in fallback_symbols:
        base, quote = parse_forex_symbol(symbol)
        instruments.append(ForexInstrument(
            symbol=symbol,
            base=base,
            quote=quote
        ))
    
    return instruments


def get_cached_instruments(cache_key: str) -> Optional[ForexInstrumentsResponse]:
    """Get instruments from cache if not expired."""
    entry = _instruments_cache.get(cache_key)
    if entry is not None:
        cached_time, cached_data = entry
        age = (datetime.utcnow() - cached_time).total_seconds()

        if age < CACHE_TTL_SECONDS:
            logger.info(f"Cache HIT for {cache_key} (age: {int(age)}s)")
            cached_data.cached = True
            return cached_data
        else:
            logger.info(f"Cache EXPIRED for {cache_key} (age: {int(age)}s)")
            _instruments_cache.pop(cache_key, None)

    return None


def _prune_instruments_cache(now: Optional[datetime] = None) -> None:
    """Sweep expired entries, then evict the oldest until there is room for one more."""
    now = now or datetime.utcnow()
    for key, (cached_time, _data) in list(_instruments_cache.items()):
        if (now - cached_time).total_seconds() >= CACHE_TTL_SECONDS:
            _instruments_cache.pop(key, None)
    overflow = len(_instruments_cache) - (CACHE_MAX_ENTRIES - 1)
    if overflow > 0:
        oldest_first = sorted(list(_instruments_cache.items()), key=lambda item: item[1][0])
        for key, _entry in oldest_first[:overflow]:
            _instruments_cache.pop(key, None)


def set_cached_instruments(cache_key: str, response: ForexInstrumentsResponse):
    """Store instruments in cache."""
    if len(cache_key) > CACHE_MAX_KEY_LENGTH:
        return  # caller-chosen and absurdly long: serve it, do not remember it
    if cache_key not in _instruments_cache:
        _prune_instruments_cache()
    _instruments_cache[cache_key] = (datetime.utcnow(), response)
    logger.info(f"Cache SET for {cache_key}")


def fetch_oanda_instruments(
    broker_account_id: str,
    credentials: dict,
    environment: str = "practice"
) -> List[ForexInstrument]:
    """
    Fetch instruments from OANDA API.
    
    Args:
        broker_account_id: OANDA account ID
        credentials: Decrypted credentials with api_key, account_id, environment
        environment: practice or live
    
    Returns:
        List of ForexInstrument objects
    """
    try:
        from app.exchange.oanda.client import OandaClient
        
        # Create client with credentials
        client = OandaClient(
            api_key=credentials.get("api_key"),
            account_id=credentials.get("account_id"),
            environment=credentials.get("environment", environment)
        )
        
        # Fetch instruments
        raw_instruments = client.get_instruments()
        
        # Parse and normalize
        instruments = []
        for raw in raw_instruments:
            # OANDA format: {"name": "EUR_USD", "type": "CURRENCY", ...}
            symbol = raw.get("name", "")
            if not symbol or raw.get("type") != "CURRENCY":
                continue
            
            base, quote = parse_forex_symbol(symbol)
            instruments.append(ForexInstrument(
                symbol=symbol,
                base=base,
                quote=quote,
                pip_location=raw.get("pipLocation"),
                margin_rate=raw.get("marginRate")
            ))
        
        logger.info(f"Fetched {len(instruments)} instruments from OANDA for account {broker_account_id}")
        return instruments
        
    except Exception as e:
        logger.error(f"Failed to fetch OANDA instruments: {e}")
        raise


def fetch_ibkr_instruments(
    broker_account_id: str,
    credentials: dict,
    environment: str = "paper"
) -> List[ForexInstrument]:
    """
    Fetch instruments from IBKR Gateway via Adapter.
    
    Args:
        broker_account_id: IBKR account ID (used for logging/validation)
        credentials: Decrypted credentials with account_id, gateway_url
        environment: paper or live
    
    Returns:
        List of ForexInstrument objects
    """
    try:
        from urllib.parse import urlparse
        from app.exchange.ibkr.adapter import IBKRAdapter

        # The adapter speaks to TWS / Gateway by host and port; a stored
        # gateway URL is translated instead of being passed as a keyword the
        # adapter never accepted (TypeError before Step 1.7).
        gateway_url = credentials.get("gateway_url") or "http://127.0.0.1:4001"
        parsed = urlparse(gateway_url if "://" in gateway_url else f"http://{gateway_url}")
        account_id = credentials.get("account_id")
        adapter = IBKRAdapter(
            host=parsed.hostname or "127.0.0.1",
            port=int(parsed.port or credentials.get("port") or 4001),
            client_id=int(credentials.get("client_id") or 1),
            account_id=account_id,
        )
        
        # Fetch instruments using standardized adapter interface
        specs = adapter.list_instruments()
        
        instruments = []
        for spec in specs:
            # Map InstrumentSpec to ForexInstrument
            instruments.append(ForexInstrument(
                symbol=spec.symbol_canonical, # e.g. EUR.USD
                base=spec.base_currency,
                quote=spec.quote_currency,
                # Heuristic for pip location based on tick size if available
                pip_location=-4 if "JPY" not in spec.quote_currency else -2, 
                margin_rate=float(1 / spec.max_leverage) if spec.max_leverage else 0.02
            ))
            
        logger.info(f"Fetched {len(instruments)} instruments from IBKR for account {account_id}")
        return instruments
        
    except Exception as e:
        logger.error(f"Failed to fetch IBKR instruments: {e}")
        raise




# A plain ``def`` on purpose: the broker fetches below are synchronous HTTP, so
# FastAPI runs this handler in its threadpool instead of on the event loop the
# trading runtime shares.
@router.get("/instruments", response_model=ForexInstrumentsResponse)
def get_forex_instruments(
    broker_id: str = Query(default="oanda", description="Broker ID"),
    broker_account_id: Optional[str] = Query(default=None, description="Broker account ID for live fetch"),
    environment: str = Query(default="practice", description="practice or live"),
    user_id: str = Depends(get_current_user_id),
):
    """
    Get forex instruments list.
    
    Priority:
    1. If broker_account_id + credentials provided: fetch from broker API (cached 1h)
    2. Else: return fallback from settings.FOREX_SYMBOLS
    
    Requires an authenticated user. Credentials are never accepted from the
    request: when a broker account is named, they are resolved here through the
    canonical resolver, which also enforces that the account belongs to the
    caller (Step 1.7).

    Returns:
        ForexInstrumentsResponse with instruments list and source indicator
    """
    # Build cache key. A listing fetched for a broker account is cached for the
    # caller only, so naming someone else's account id never returns their entry.
    account_scope = f"{user_id}:{broker_account_id}" if broker_account_id else "default"
    cache_key = f"{broker_id}:{account_scope}:{environment}"
    
    # Check cache first
    cached = get_cached_instruments(cache_key)
    if cached:
        return cached
    
    # A named broker account: resolve the caller's own credentials server-side.
    credentials = None
    if broker_account_id:
        try:
            from shared_lib.broker import resolve_broker_auth
            from shared_lib.persistence.db import DB
            auth = resolve_broker_auth(broker_account_id, user_id, DB())
            credentials = {"api_key": auth.api_key, "api_secret": auth.api_secret, "account_id": auth.account_id,
                           "environment": auth.environment.value, **dict(auth.extra or {})}
        except Exception as exc:
            logger.warning("forex instruments: credentials for account %s not resolved for user %s: %s",
                           broker_account_id, user_id, type(exc).__name__)
            credentials = None
    if broker_account_id and credentials:
        if credentials:
            try:
                instruments = []
                if broker_id == "oanda":
                    instruments = fetch_oanda_instruments(
                        broker_account_id=broker_account_id,
                        credentials=credentials,
                        environment=environment
                    )
                elif broker_id == "ibkr":
                    instruments = fetch_ibkr_instruments(
                        broker_account_id=broker_account_id,
                        credentials=credentials,
                        environment=environment
                    )
                
                if instruments:
                    response = ForexInstrumentsResponse(
                        broker_id=broker_id,
                        source="broker",
                        instruments=instruments,
                        cached=False
                    )
                    
                    set_cached_instruments(cache_key, response)
                    return response
            
            except Exception as e:
                logger.warning(f"Broker fetch failed for {broker_id}, falling back to env: {e}")
                # Fall through to fallback
    
    # Fallback: use settings.FOREX_SYMBOLS
    fallback_instruments = get_fallback_instruments()
    
    response = ForexInstrumentsResponse(
        broker_id=broker_id,
        source="fallback",
        instruments=fallback_instruments,
        cached=False
    )
    
    # Cache fallback too (for consistency)
    set_cached_instruments(cache_key, response)
    
    return response
