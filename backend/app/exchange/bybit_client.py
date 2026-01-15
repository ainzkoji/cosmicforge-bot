"""
ByBit API Integration
Supports both futures and spot trading
"""
import hashlib
import hmac
import time
from typing import Dict, Any, Optional
import requests


class ByBitClient:
    """Simple ByBit API client for testing connections"""
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        
    def _generate_signature(self, params: Dict[str, Any]) -> str:
        """Generate HMAC SHA256 signature"""
        param_str = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        return hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def test_connection(self) -> Dict[str, Any]:
        """Test API connection by fetching account info"""
        try:
            timestamp = int(time.time() * 1000)
            params = {
                "api_key": self.api_key,
                "timestamp": timestamp
            }
            params["sign"] = self._generate_signature(params)
            
            response = requests.get(
                f"{self.base_url}/v2/private/wallet/balance",
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ret_code") == 0:
                    return {
                        "success": True,
                        "message": "Connection successful",
                        "account_type": "ByBit",
                        "capabilities": ["read", "trade", "futures", "spot"]
                    }
                else:
                    return {
                        "success": False,
                        "error": data.get("ret_msg", "Unknown error")
                    }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text[:100]}"
                }
                
        except requests.exceptions.Timeout:
            return {"success": False, "error": "Connection timeout"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_account_info(self) -> Optional[Dict[str, Any]]:
        """Get account balance and info"""
        try:
            timestamp = int(time.time() * 1000)
            params = {
                "api_key": self.api_key,
                "timestamp": timestamp
            }
            params["sign"] = self._generate_signature(params)
            
            response = requests.get(
                f"{self.base_url}/v2/private/wallet/balance",
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None
