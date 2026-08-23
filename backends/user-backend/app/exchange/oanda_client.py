import requests
from typing import Dict, Any, Optional

class OandaClient:
    """
    Minimal OANDA API Client for validation and basic account info.
    Full trading logic is in bot-backend, this is just for connection storage verification.
    """
    
    def __init__(self, api_token: str, account_id: str, practice: bool = True):
        self.api_token = api_token
        self.account_id = account_id
        self.environment = "practice" if practice else "live"
        
        if self.environment == "practice":
            self.base_url = "https://api-fxpractice.oanda.com/v3"
        else:
            self.base_url = "https://api-fxtrade.oanda.com/v3"
            
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "Accept-Datetime-Format": "RFC3339"
        }

    def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.request(method, url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            # Capture response content if available for better error messages
            error_msg = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    err_json = e.response.json()
                    if 'errorMessage' in err_json:
                        error_msg = err_json['errorMessage']
                except:
                    pass
            raise Exception(f"OANDA API Error: {error_msg}")

    def get_account_summary(self) -> Dict[str, Any]:
        """
        Fetches account summary to validate credentials and get balance.
        """
        endpoint = f"/accounts/{self.account_id}/summary"
        data = self._request("GET", endpoint)
        return data.get("account", {})

    def test_connection(self) -> Dict[str, Any]:
        """
        Validates that credentials work.
        """
        try:
            account = self.get_account_summary()
            return {
                "success": True,
                "message": f"Connected to OANDA account {account.get('id')}",
                "capabilities": ["read", "trade", "forex"],
                "capital": {
                    "balance": float(account.get("balance", 0.0)),
                    "currency": account.get("currency", "USD")
                }
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
