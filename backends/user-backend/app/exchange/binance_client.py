"""
Convenience wrapper for Binance client to support broker connection testing
"""
from app.exchange.binance.client import BinanceFuturesClient
from typing import Dict, Any


class BinanceClient:
    """Wrapper around BinanceFuturesClient for broker validation"""
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        self.client = BinanceFuturesClient(
            api_key=api_key,
            api_secret=api_secret,
            base_url=base_url
        )
    
    
    def test_connection(self) -> Dict[str, Any]:
        """Test API connection by fetching account info"""
        try:
            # Try to fetch account data
            account_data = self.client.account()
            
            if account_data and isinstance(account_data, dict):
                # Check what capabilities the API key has
                capabilities = ["read"]
                
                # If we can get account data, we have read access
                # Check for additional permissions
                if account_data.get("canTrade"):
                    capabilities.extend(["trade", "futures"])
                
                if account_data.get("canDeposit"):
                    capabilities.append("deposit")
                    
                if account_data.get("canWithdraw"):
                    capabilities.append("withdraw")

                try:
                    balance = self.client.account_balance()
                    if balance:
                        capabilities.append("balance")
                except:
                    pass
                
                return {
                    "success": True,
                    "message": "Connection successful",
                    "account_type": "Binance Futures",
                    "capabilities": capabilities
                }
            else:
                return {
                    "success": False,
                    "error": "Invalid response from Binance API"
                }
                
        except Exception as e:
            error_msg = str(e)
            
            # Parse common errors
            if "Invalid API-key" in error_msg or "API-key format invalid" in error_msg:
                return {"success": False, "error": "Invalid API key or secret"}
            elif "Signature for this request is not valid" in error_msg:
                return {"success": False, "error": "Invalid API secret (signature mismatch)"}
            elif "IP address not authorized" in error_msg:
                return {"success": False, "error": "IP address not whitelisted"}
            elif "timestamp" in error_msg.lower():
                return {"success": False, "error": "Server time synchronization issue"}
            else:
                return {"success": False, "error": f"Connection failed: {error_msg}"}

    def account(self) -> Dict[str, Any]:
        """Get account information including balances and positions"""
        return self.client.account()

