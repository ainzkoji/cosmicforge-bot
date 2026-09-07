"""
OANDA v20 REST API Client.

Implements direct REST calls to OANDA broker using their v20 API.
Reference: https://developer.oanda.com/rest-live-v20/introduction/
"""
import requests
import time
from typing import Dict, List, Any, Optional
from decimal import Decimal


class OandaClient:
    """
    OANDA v20 REST API Client.
    
    Authentication: Bearer token in Authorization header.
    Base URLs:
    - Practice: https://api-fxpractice.oanda.com
    - Live: https://api-fxtrade.oanda.com
    """
    
    def __init__(
        self,
        api_token: str,
        account_id: str,
        practice: bool = True,
        timeout: int = 30
    ):
        self.api_token = api_token
        self.account_id = account_id
        self.practice = practice
        self.timeout = timeout
        
        # Base URL
        if practice:
            self.base_url = "https://api-fxpractice.oanda.com"
        else:
            self.base_url = "https://api-fxtrade.oanda.com"
        
        # Headers
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        
        # Cache
        self._instruments_cache: Optional[List[Dict]] = None
        self._instruments_cache_time: float = 0
        self._cache_ttl: int = 3600  # 1 hour
    
    # ==========================================
    # ACCOUNTS
    # ==========================================
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        """
        GET /v3/accounts
        
        Returns list of accounts owned by the user.
        """
        url = f"{self.base_url}/v3/accounts"
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("accounts", [])
    
    def get_account_summary(self, account_id: Optional[str] = None) -> Dict[str, Any]:
        """
        GET /v3/accounts/{accountID}/summary
        
        Returns summary of account status.
        """
        acc_id = account_id or self.account_id
        url = f"{self.base_url}/v3/accounts/{acc_id}/summary"
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("account", {})
    
    def get_account_details(self, account_id: Optional[str] = None) -> Dict[str, Any]:
        """
        GET /v3/accounts/{accountID}
        
        Returns full account details including positions and orders.
        """
        acc_id = account_id or self.account_id
        url = f"{self.base_url}/v3/accounts/{acc_id}"
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("account", {})
    
    # ==========================================
    # INSTRUMENTS
    # ==========================================
    
    def get_instruments(self, instruments: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        GET /v3/accounts/{accountID}/instruments
        
        Returns metadata for tradable instruments.
        Uses cache (1 hour TTL).
        """
        now = time.time()
        if self._instruments_cache and (now - self._instruments_cache_time) < self._cache_ttl:
            return self._instruments_cache
        
        url = f"{self.base_url}/v3/accounts/{self.account_id}/instruments"
        params = {}
        if instruments:
            params["instruments"] = ",".join(instruments)
        
        resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        resp.raise_for_status()
        
        result = resp.json().get("instruments", [])
        self._instruments_cache = result
        self._instruments_cache_time = now
        return result
    
    # ==========================================
    # PRICING
    # ==========================================
    
    def get_pricing(self, instruments: List[str]) -> Dict[str, Any]:
        """
        GET /v3/accounts/{accountID}/pricing
        
        Returns current pricing for instruments.
        
        Response Example:
        {
            "prices": [
                {
                    "instrument": "EUR_USD",
                    "time": "2024-01-01T00:00:00.000000000Z",
                    "bids": [{"price": "1.10000", "liquidity": 10000000}],
                    "asks": [{"price": "1.10001", "liquidity": 10000000}],
                    "closeoutBid": "1.09999",
                    "closeoutAsk": "1.10002"
                }
            ]
        }
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/pricing"
        params = {"instruments": ",".join(instruments)}
        
        resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
    
    def get_candles(
        self,
        instrument: str,
        granularity: str = "M5",
        count: int = 500,
        price: str = "M"  # M=mid, B=bid, A=ask
    ) -> List[Dict[str, Any]]:
        """
        GET /v3/instruments/{instrument}/candles
        
        Returns historical candle data.
        
        Granularity: S5, M1, M5, H1, D, etc.
        """
        url = f"{self.base_url}/v3/instruments/{instrument}/candles"
        params = {
            "granularity": granularity,
            "count": count,
            "price": price
        }
        
        resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("candles", [])
    
    # ==========================================
    # ORDERS
    # ==========================================
    
    def create_order(self, order_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /v3/accounts/{accountID}/orders
        
        Creates a new order.
        
        Order Spec Example (Market Order):
        {
            "order": {
                "type": "MARKET",
                "instrument": "EUR_USD",
                "units": "100000",
                "timeInForce": "FOK",
                "stopLossOnFill": {"price": "1.09000"},
                "takeProfitOnFill": {"price": "1.11000"}
            }
        }
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/orders"
        
        resp = requests.post(url, headers=self.headers, json=order_spec, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
    
    def get_orders(self, instrument: Optional[str] = None, state: str = "PENDING") -> List[Dict[str, Any]]:
        """
        GET /v3/accounts/{accountID}/orders
        
        Returns list of orders.
        State: PENDING, FILLED, CANCELLED, ALL
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/orders"
        params = {"state": state}
        if instrument:
            params["instrument"] = instrument
        
        resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("orders", [])
    
    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        GET /v3/accounts/{accountID}/orders/{orderSpecifier}
        
        Returns specific order details.
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/orders/{order_id}"
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("order", {})
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        PUT /v3/accounts/{accountID}/orders/{orderSpecifier}/cancel
        
        Cancels an order.
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/orders/{order_id}/cancel"
        resp = requests.put(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
    
    # ==========================================
    # TRADES / POSITIONS
    # ==========================================
    
    def get_open_trades(self, instrument: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        GET /v3/accounts/{accountID}/openTrades
        
        Returns all open trades (positions).
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/openTrades"
        params = {}
        if instrument:
            params["instrument"] = instrument
        
        resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("trades", [])
    
    def get_trade(self, trade_id: str) -> Dict[str, Any]:
        """
        GET /v3/accounts/{accountID}/trades/{tradeSpecifier}
        
        Returns specific trade details.
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/trades/{trade_id}"
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("trade", {})
    
    def close_trade(self, trade_id: str, units: Optional[str] = "ALL") -> Dict[str, Any]:
        """
        PUT /v3/accounts/{accountID}/trades/{tradeSpecifier}/close
        
        Closes a trade (full or partial).
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/trades/{trade_id}/close"
        body = {"units": units}
        
        resp = requests.put(url, headers=self.headers, json=body, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
    
    def modify_trade(self, trade_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        """
        PUT /v3/accounts/{accountID}/trades/{tradeSpecifier}/orders
        
        Modifies stop loss / take profit on existing trade.
        
        Example:
        {
            "stopLoss": {"price": "1.09000"},
            "takeProfit": {"price": "1.11000"}
        }
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/trades/{trade_id}/orders"
        
        resp = requests.put(url, headers=self.headers, json=modifications, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """
        GET /v3/accounts/{accountID}/positions
        
        Returns all positions (aggregated by instrument).
        Note: OANDA uses both "positions" (net) and "trades" (tickets).
        We primarily use trades for ticket-based position tracking.
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/positions"
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("positions", [])
    
    def close_position(self, instrument: str, long_units: str = "ALL", short_units: str = "ALL") -> Dict[str, Any]:
        """
        PUT /v3/accounts/{accountID}/positions/{instrument}/close
        
        Closes position for instrument (can specify long/short separately).
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/positions/{instrument}/close"
        body = {
            "longUnits": long_units,
            "shortUnits": short_units
        }
        
        resp = requests.put(url, headers=self.headers, json=body, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
    
    # ==========================================
    # TRANSACTIONS
    # ==========================================
    
    def get_transactions(
        self,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        transaction_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        GET /v3/accounts/{accountID}/transactions
        
        Returns transaction history.
        Types: ORDER_FILL, STOP_LOSS_FILLED, TAKE_PROFIT_FILLED, etc.
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/transactions"
        params = {}
        if from_time:
            params["from"] = from_time
        if to_time:
            params["to"] = to_time
        if transaction_type:
            params["type"] = transaction_type
        
        resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("transactions", [])
    
    def get_transaction(self, transaction_id: str) -> Dict[str, Any]:
        """
        GET /v3/accounts/{accountID}/transactions/{transactionID}
        
        Returns specific transaction.
        """
        url = f"{self.base_url}/v3/accounts/{self.account_id}/transactions/{transaction_id}"
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("transaction", {})
