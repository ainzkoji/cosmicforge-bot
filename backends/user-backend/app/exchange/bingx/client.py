from __future__ import annotations
import requests
import time
from typing import Dict, Any, Optional
from app.exchange.bingx.signing import sign_bingx, get_timestamp

class BingXClient:
    """
    BingX Client for User Backend (Verification & Balance only).
    """
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, base_url: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        
        if base_url:
             self.base_url = base_url.rstrip("/")
        else:
             self.base_url = "https://open-api.bingx.com"

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        payload = payload or {}
        
        # 1. Timestamp
        payload["timestamp"] = str(get_timestamp())
        
        # 2. Sort & Sign
        filtered = {k: v for k, v in payload.items() if v is not None}
        sorted_items = sorted(filtered.items())
        query_string = "&".join([f"{k}={v}" for k, v in sorted_items])
        signature = sign_bingx(self.api_secret, query_string)
        final_query = f"{query_string}&signature={signature}"
        
        headers = {"X-BX-APIKEY": self.api_key}
        
        try:
            if method == "GET":
                r = requests.get(f"{url}?{final_query}", headers=headers, timeout=10)
            elif method == "POST":
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                r = requests.post(url, data=final_query, headers=headers, timeout=10)
            else:
                 raise ValueError(f"Method {method} not supported")
                 
            if r.status_code >= 400:
                 raise RuntimeError(f"BingX HTTP {r.status_code}: {r.text}")
            
            return r.json()
        except Exception as e:
            raise RuntimeError(f"BingX Network Error: {str(e)}")

    def test_connection(self) -> Dict[str, Any]:
        """Test API connection."""
        try:
            res = self._request("GET", "/openApi/swap/v2/user/balance")
            
            if res.get("code") == 0:
                return {
                    "success": True,
                    "message": "Connection successful",
                    "account_type": "BingX Futures",
                    "capabilities": ["read", "trade", "futures"]
                }
            
            msg = res.get("msg", "Unknown error")
            
            # Sanitize error messages
            error_lower = msg.lower()
            if "null apikey" in error_lower or "api key" in error_lower:
                friendly_error = "Authentication failed: Invalid API Key or signature. Please check your credentials."
            elif "ip" in error_lower and "whitelist" in error_lower:
                 friendly_error = "IP Address not authorized. Please check your API Key IP Whitelist settings."
            else:
                 friendly_error = f"BingX Error: {msg}"
                 
            return {"success": False, "error": friendly_error}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def account(self) -> Dict[str, Any]:
        """Get account balance normalized."""
        try:
            data = self._request("GET", "/openApi/swap/v2/user/balance")
            
            if data.get("code") != 0:
                 msg = data.get("msg", "Unknown error")
                 if "null apikey" in msg.lower() or "api key" in msg.lower():
                     raise RuntimeError("Authentication failed: Invalid API Key or signature.")
                 raise RuntimeError(f"BingX: {msg}")
            
            bal = data.get("data", {}).get("balance", {})
            return {
                "totalWalletBalance": float(bal.get("balance", 0.0)),
                "totalMarginBalance": float(bal.get("equity", 0.0)),
                "availableBalance": float(bal.get("availableMargin", 0.0)),
                "totalUnrealizedProfit": float(bal.get("unrealisedPNL", 0.0))
            }
        except Exception:
            # Return zeros on error to prevent crash in summary
            return {
                "totalWalletBalance": 0.0,
                "totalMarginBalance": 0.0,
                "availableBalance": 0.0,
                "totalUnrealizedProfit": 0.0
            }
