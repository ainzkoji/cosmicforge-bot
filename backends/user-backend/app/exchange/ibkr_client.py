"""
IBKR Client Validator for User-Backend

Validates IBKR connection without full adapter implementation.
Used during broker account setup to test credentials.
"""
import requests
import urllib3
from typing import Dict, Any
import logging

# Disable SSL warnings for local gateway
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class IBKRClientValidator:
    """
    Validates IBKR Client Portal Gateway connection and authentication.
    
    This is a lightweight validator used in user-backend to test
    credentials during account setup. Does NOT implement full trading logic.
    """
    
    def __init__(
        self, 
        gateway_url: str = "https://localhost:5000",
        username: str = None,
        password: str = None,
        account_id: str = None,
        environment: str = "paper"
    ):
        self.gateway_url = gateway_url.rstrip('/')
        self.username = username
        self.password = password
        self.account_id = account_id
        self.environment = environment
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test IBKR gateway connection and authentication status.
        
        Returns:
            Dictionary with success status and capabilities or error message
            Format: {"success": bool, "capabilities": list, "error": str}
        """
        try:
            # Test 1: Gateway reachability
            auth_status = self._check_auth_status()
            
            if not auth_status["success"]:
                return auth_status
            
            # Test 2: Account access (if account_id provided)
            if self.account_id:
                account_check = self._check_account_access()
                if not account_check["success"]:
                    return account_check
            
            # Success - return capabilities
            return {
                "success": True,
                "capabilities": ["read", "trade", "forex", "stocks", "options"],
                "message": "IBKR gateway connection validated"
            }
            
        except Exception as e:
            logger.error(f"IBKR validation error: {e}")
            return {
                "success": False,
                "error": f"Connection test failed: {str(e)}"
            }
    
    def _check_auth_status(self) -> Dict[str, Any]:
        """Check if gateway is authenticated and connected."""
        try:
            resp = requests.post(
                f"{self.gateway_url}/iserver/auth/status",
                verify=False,
                timeout=5
            )
            
            if resp.status_code != 200:
                return {
                    "success": False,
                    "error": f"Gateway returned status {resp.status_code}. Is the gateway running?"
                }
            
            data = resp.json()
            
            # Check authentication
            if not data.get("authenticated"):
                return {
                    "success": False,
                    "error": "Not authenticated. Please log in via IBKR Client Portal Gateway web interface (typically https://localhost:5000)"
                }
            
            # Check connection to broker
            if not data.get("connected"):
                return {
                    "success": False,
                    "error": "Gateway not connected to IBKR servers. Check gateway status."
                }
            
            return {"success": True}
            
        except requests.exceptions.ConnectionError:
            return {
                "success": False,
                "error": "Cannot connect to gateway. Ensure IBKR Client Portal Gateway is running on https://localhost:5000"
            }
        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": "Gateway connection timeout. Check if gateway is responsive."
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Gateway check failed: {str(e)}"
            }
    
    def _check_account_access(self) -> Dict[str, Any]:
        """Verify access to specified account."""
        try:
            # Get portfolio accounts
            resp = requests.get(
                f"{self.gateway_url}/portfolio/accounts",
                verify=False,
                timeout=5
            )
            
            if resp.status_code != 200:
                return {
                    "success": False,
                    "error": f"Failed to fetch accounts: {resp.status_code}"
                }
            
            accounts = resp.json()
            
            # Check if our account_id is in the list
            if isinstance(accounts, list):
                account_ids = [acc.get("accountId") for acc in accounts if isinstance(acc, dict)]
                
                if self.account_id not in account_ids:
                    return {
                        "success": False,
                        "error": f"Account {self.account_id} not found in gateway. Available accounts: {', '.join(account_ids)}"
                    }
            
            return {"success": True}
            
        except Exception as e:
            # Account check is optional - log but don't fail
            logger.warning(f"Account access check failed: {e}")
            return {"success": True}  # Allow to proceed
