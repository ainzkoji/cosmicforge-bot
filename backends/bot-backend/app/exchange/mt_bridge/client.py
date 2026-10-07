"""
MetaTrader Bridge Client
Handles HTTP communication with user-hosted MT4/MT5 bridge.
"""
from typing import List, Dict, Optional, Any
import time
from decimal import Decimal
import requests
import logging
from datetime import datetime
from app.models.unified_trading import SymbolFilters

from app.exchange.mt_bridge.errors import (
    MTBridgeError,
    MTBridgeConnectionError,
    MTBridgeAuthError,
    MTBridgeTimeoutError
)

logger = logging.getLogger(__name__)


def _redact_for_log(text: Any, *secrets: Any, limit: int = 200) -> str:
    """A remote response body made safe for the server log: truncated, known secrets removed."""
    out = str(text or "")[:limit]
    for secret in secrets:
        if secret:
            out = out.replace(str(secret), "[REDACTED]")
    return out


def harden_bridge_client(client: Any, destination_guard: Any = None) -> None:
    """Harden a bridge client whose URL came from a user (request or stored account).

    * Redirects are never followed: only the bridge URL itself was validated,
      and a redirect could point at an address the outbound guard never saw
      (requests raises TooManyRedirects instead).
    * ``destination_guard`` -- a callable that raises ``UnsafeDestinationError``
      -- is run by ``MTBridgeClient._request`` immediately before every request.
    """
    http_session = getattr(client, "_session", None)
    if http_session is not None:
        http_session.max_redirects = 0
    if destination_guard is not None:
        client.destination_guard = destination_guard


class MTBridgeClient:
    """
    HTTP client for MetaTrader Bridge API (v1).
    Supports both MT4 and MT5 through unified contract.
    """
    
    def __init__(
        self, 
        base_url: str, 
        api_token: str,
        timeout: int = 10,
        verify_ssl: bool = True
    ):
        """
        Args:
            base_url: Bridge URL (e.g., "https://vps.example.com:8443")
            api_token: Bearer token for authentication
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        
        self._session = requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        })
        
        self._instruments_cache = {}
        self._instruments_cache_ts = 0.0
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """
        Internal HTTP request wrapper with error handling.
        """
        from shared_lib.core.production import require_broker_mutation_permission
        require_broker_mutation_permission(method, endpoint)
        url = f"{self.base_url}{endpoint}"

        # Optional hook installed by harden_bridge_client(): re-validate the
        # destination (fresh DNS lookup + outbound policy) immediately before
        # every request, so a name that has been re-pointed at an internal
        # address since the URL was validated is refused here.
        guard = getattr(self, "destination_guard", None)
        if guard is not None:
            try:
                guard()
            except ValueError as exc:  # UnsafeDestinationError
                raise MTBridgeConnectionError(
                    f"Bridge destination refused by the outbound policy ({getattr(exc, 'reason', 'UNSAFE_DESTINATION')})",
                    failure_kind="DESTINATION_NOT_ALLOWED",
                ) from None

        try:
            resp = self._session.request(
                method=method,
                url=url,
                timeout=self.timeout,
                verify=self.verify_ssl,
                **kwargs
            )

            # Parse JSON
            try:
                data = resp.json()
            except ValueError:
                # The body is whatever the remote host chose to send: it goes
                # to the server log (truncated, token removed), never into the
                # exception message, which API routes may show to a caller.
                logger.warning(
                    "MT bridge answered HTTP %s with a non-JSON body: %r",
                    resp.status_code, _redact_for_log(resp.text, self.api_token),
                )
                raise MTBridgeError(
                    f"Invalid JSON response from bridge (HTTP {resp.status_code})",
                    status_code=resp.status_code,
                    failure_kind="INVALID_RESPONSE",
                )

            # Check for errors
            if resp.status_code != 200:
                error_msg = data.get('error', 'Unknown error')
                error_code = data.get('error_code', 'UNKNOWN')
                details = data.get('details', {})

                raise MTBridgeError(
                    f"Bridge API error: {error_msg}",
                    error_code=error_code,
                    details=details,
                    status_code=resp.status_code,
                    failure_kind="HTTP_ERROR",
                )

            return data

        except requests.exceptions.Timeout:
            raise MTBridgeError(f"Bridge request timeout after {self.timeout}s", failure_kind="TIMEOUT")
        except requests.exceptions.ConnectionError as e:
            raise MTBridgeError(f"Cannot connect to bridge at {self.base_url}: {e}", failure_kind="UNREACHABLE")
        except requests.exceptions.RequestException as e:
            raise MTBridgeError(f"Bridge request failed: {e}", failure_kind="REQUEST_FAILED")
    
    # ==========================================
    # BRIDGE API ENDPOINTS (v1)
    # ==========================================
    
    def get_health(self) -> Dict[str, Any]:
        """
        GET /v1/health
        Returns: {
            ok: true,
            platform: "mt4"|"mt5",
            account: { login: "...", server: "...", currency: "..." },
            time: "ISO8601"
        }
        """
        return self._request('GET', '/v1/health')
    
    def get_instruments(self) -> List[Dict[str, Any]]:
        """
        GET /v1/instruments
        Returns: {
            instruments: [
                {
                    symbol: "EURUSD",
                    base: "EUR",
                    quote: "USD",
                    digits: 5,
                    tick_size: 0.00001,
                    contract_size: 100000,
                    min_lot: 0.01,
                    lot_step: 0.01
                },
                ...
            ]
        }
        """
        resp = self._request('GET', '/v1/instruments')
        return resp.get('instruments', [])

    def get_instruments_cached(self, ttl: int = 60) -> Dict[str, Any]:
        """Cached version of get_instruments returning dict by symbol."""
        now = time.time()
        if self._instruments_cache and (now - self._instruments_cache_ts) < ttl:
            return self._instruments_cache
            
        instruments_list = self.get_instruments()
        # Index by symbol
        self._instruments_cache = {i['symbol']: i for i in instruments_list}
        self._instruments_cache_ts = now
        return self._instruments_cache

    def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        """
        Get standardized sizing filters for MT4/MT5.
        """
        try:
            instruments = self.get_instruments_cached()
            inst = instruments.get(symbol)
            if not inst:
                return SymbolFilters()
                
            return SymbolFilters(
                symbol=symbol,
                min_qty=Decimal(str(inst.get('min_lot', 0.01))),
                max_qty=Decimal(str(inst.get('max_lot', 100000.0))),
                step_size=Decimal(str(inst.get('lot_step', 0.01))),
                tick_size=Decimal(str(inst.get('tick_size', 0.00001))),
                min_notional=Decimal("0"),
                contract_size=Decimal(str(inst.get('contract_size', 1.0)))
            )
        except Exception as e:
            logger.error(f"Error getting symbol filters for {symbol}: {e}")
            return SymbolFilters()
    
    def get_prices(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        POST /v1/prices
        Body: { symbols: ["EURUSD", "GBPUSD"] }
        Returns: {
            prices: {
                "EURUSD": { bid: 1.0, ask: 1.1, time: "ISO8601" },
                ...
            }
        }
        """
        resp = self._request('POST', '/v1/prices', json={'symbols': symbols})
        return resp.get('prices', {})
    
    def get_klines(
        self, 
        symbol: str, 
        timeframe: str, 
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """
        POST /v1/klines
        Body: {
            symbol: "EURUSD",
            timeframe: "M1"|"M5"|"M15"|"M30"|"H1"|"H4"|"D1",
            limit: 500
        }
        Returns: {
            candles: [
                {
                    time: "ISO8601",
                    open: ...,
                    high: ...,
                    low: ...,
                    close: ...,
                    volume: ...
                },
                ...
            ]
        }
        """
        resp = self._request('POST', '/v1/klines', json={
            'symbol': symbol,
            'timeframe': timeframe,
            'limit': limit
        })
        return resp.get('candles', [])
    
    def place_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v1/order
        Body: {
            client_order_id: "string",
            symbol: "EURUSD",
            side: "buy"|"sell",
            order_type: "market"|"limit"|"stop",
            qty: 0.10,  # LOTS
            price: 1.23456,  # required for limit/stop
            sl: 1.23000,  # optional
            tp: 1.24000,  # optional
            comment: "string optional"
        }
        Returns: {
            order_id: "mt_ticket_or_id",
            status: "filled"|"accepted"|"rejected",
            filled_qty: ...,
            avg_price: ...,
            raw: {...}
        }
        """
        return self._request('POST', '/v1/order', json=order_data)
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        POST /v1/order/cancel
        Body: { order_id: "..." }
        Returns: { ok: true }
        """
        return self._request('POST', '/v1/order/cancel', json={'order_id': order_id})
    
    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        GET /v1/order?order_id=...
        Returns: { order: {...} }
        """
        resp = self._request('GET', f'/v1/order?order_id={order_id}')
        return resp.get('order', {})
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """
        GET /v1/positions
        Returns: {
            positions: [
                {
                    symbol: "EURUSD",
                    ticket: "...",
                    side: "buy"|"sell",
                    lots: 0.10,
                    open_price: ...,
                    sl: ...,
                    tp: ...,
                    profit: ...,
                    open_time: "ISO8601"
                },
                ...
            ]
        }
        """
        resp = self._request('GET', '/v1/positions')
        return resp.get('positions', [])
    
    def get_balance(self) -> Dict[str, Any]:
        """
        GET /v1/balance
        Returns: {
            balance: ...,
            equity: ...,
            margin: ...,
            free_margin: ...,
            currency: "USD"
        }
        """
        return self._request('GET', '/v1/balance')
