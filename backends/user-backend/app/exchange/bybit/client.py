from __future__ import annotations
import time
import requests
import json
from typing import Dict, Any, Optional
from app.exchange.bybit.signing import sign_v5, sign_legacy_v2

class BybitClient:
    """
    Bybit V5 API Client
    Backward compatible with legacy 'test_connection' used by broker_service.
    """
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, base_url: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Base URL logic matching config logic
        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
            
        self.recv_window = 5000
    
    def _request_v5(self, method: str, path: str, payload: dict | None = None) -> dict:
        """
        Execute V5 Signed Request
        """
        url = f"{self.base_url}{path}"
        payload = payload or {}
        timestamp = str(int(time.time() * 1000))
        
        # Prepare payload string for signing
        if method == "GET":
            # For GET, payload is query params string
            payload_str = '&'.join([f"{k}={v}" for k, v in sorted(payload.items())])
            full_url = f"{url}?{payload_str}" if payload_str else url
        else:
            # For POST, payload is JSON string
            payload_str = json.dumps(payload)
            full_url = url
            
        signature = sign_v5(self.api_secret, payload_str, timestamp, self.api_key, self.recv_window)
        
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": str(self.recv_window),
            "Content-Type": "application/json"
        }
        
        if method == "GET":
            r = requests.get(full_url, headers=headers, timeout=10)
        elif method == "POST":
            r = requests.post(full_url, data=payload_str, headers=headers, timeout=10)
        else:
            raise ValueError(f"Method {method} not supported")
            
        if r.status_code >= 400:
            raise RuntimeError(f"Bybit HTTP {r.status_code}: {r.text}")
            
        return r.json()

    # ------------------ LEGACY INTERFACE (Required by broker_service.py) ------------------

    def test_connection(self) -> Dict[str, Any]:
        """
        Test API connection.
        Legacy behavior: Used V2 /private/wallet/balance. 
        New behavior: Use V5 /v5/account/wallet-balance but format output to match legacy expectation.
        """
        try:
            # Use /v5/user/query-api to check key validity and permissions
            # This works for all account types
            data = self._request_v5("GET", "/v5/user/query-api")
            
            # V5 success check
            if data.get("retCode") == 0:
                 result = data.get("result", {})
                 permissions = result.get("permissions", {})
                 
                 # Flatten permissions for logging/capabilities
                 caps = []
                 for k, v in permissions.items():
                     if isinstance(v, list):
                         caps.extend([f"{k}:{p}" for p in v])
                 
                 return {
                    "success": True,
                    "message": "Connection successful",
                    "account_type": "ByBit V5",
                    "capabilities": caps or ["read", "trade"]
                }
            else:
                 return {
                    "success": False,
                    "error": f"Bybit Error: {data.get('retMsg')} (Code {data.get('retCode')})"
                }

        except Exception as e:
            # Fallback for non-unified accounts? 
            # Actually, standard account works with V5 too usually, but "accountType" might need to be CONTRACT.
            # Let's try CONTRACT if UNIFIED fails? 
            # Or just return error.
            return {"success": False, "error": str(e)}

    # ------------------ NEW INTERFACE ------------------

    def get_account_info(self) -> Optional[Dict[str, Any]]:
        """Get account balance (Unified/Contract)"""
        try:
           return self._request_v5("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        except Exception:
            return None

    def transaction_log(self, limit: int = 50) -> list:
        """
        Fetch transaction history (Unified/Contract)
        """
        try:
            # Type TRANSFER covers in/out flows
            # Also could check for other types like DEPOSIT/WITHDRAW depending on what we want
            # For now, let's just get the log and filtered in the service if needed, or query recent.
            # Bybit V5 param: type
            # We'll default to TRANSFER for now matching Binance logic
            params = {
                "accountType": "UNIFIED",
                "category": "linear",
                "limit": limit,
                #"type": "TRANSFER" 
            }
            data = self._request_v5("GET", "/v5/account/transaction-log", params)
            if data.get("retCode") == 0:
                return data.get("result", {}).get("list", [])
            return []
        except Exception:
            return []

    def positions(self) -> list:
        """
        Fetch positions from Bybit.
        """
        try:
             # Fetch linear (USDT perp) positions
             data = self._request_v5("GET", "/v5/position/list", {"category": "linear", "settleCoin": "USDT"})
             if data.get("retCode") == 0:
                 return data.get("result", {}).get("list", [])
             return []
        except Exception:
             return []

    def account(self) -> Dict[str, Any]:
        """
        Get account information formatted for broker_service compatibility.
        Returns balance data in a format matching Binance's account() response.
        """
        def safe_float(value, default=0.0):
            """Safely convert a value to float, handling empty strings and None"""
            if value is None or value == "":
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default
        
        try:
            # First try UNIFIED account
            data = self._request_v5("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"})
            
            if data.get("retCode") == 0:
                result = data.get("result", {})
                # The list of account balances (usually one item for unified)
                coin_list = result.get("list", [])
                
                if coin_list:
                    account_info = coin_list[0]  # Usually first item for unified
                    # Extract USDT coin data
                    coins = account_info.get("coin", [])
                    usdt_coin = next((c for c in coins if c.get("coin") == "USDT"), {})
                    
                    # Format response to match Binance structure
                    return {
                        "totalWalletBalance": safe_float(usdt_coin.get("walletBalance")),
                        "totalMarginBalance": safe_float(account_info.get("totalEquity")),
                        "availableBalance": safe_float(usdt_coin.get("availableToWithdraw")),
                        "totalUnrealizedProfit": safe_float(usdt_coin.get("unrealisedPnl")),
                        "totalInitialMargin": safe_float(account_info.get("totalInitialMargin"))
                    }
            
            # If UNIFIED fails, try CONTRACT account
            data = self._request_v5("GET", "/v5/account/wallet-balance", {"accountType": "CONTRACT"})
            if data.get("retCode") == 0:
                result = data.get("result", {})
                coin_list = result.get("list", [])
                
                if coin_list:
                    account_info = coin_list[0]
                    coins = account_info.get("coin", [])
                    usdt_coin = next((c for c in coins if c.get("coin") == "USDT"), {})
                    
                    return {
                        "totalWalletBalance": safe_float(usdt_coin.get("walletBalance")),
                        "totalMarginBalance": safe_float(account_info.get("totalEquity")),
                        "availableBalance": safe_float(usdt_coin.get("availableToWithdraw")),
                        "totalUnrealizedProfit": safe_float(usdt_coin.get("unrealisedPnl")),
                        "totalInitialMargin": safe_float(account_info.get("totalInitialMargin"))
                    }
            
            # If both fail, return zeros
            return {
                "totalWalletBalance": 0.0,
                "totalMarginBalance": 0.0,
                "availableBalance": 0.0,
                "totalUnrealizedProfit": 0.0
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to fetch Bybit account data: {str(e)}")

