"""
MetaTrader Bridge Error Classes
"""

from typing import Optional, Dict


class MTBridgeError(Exception):
    """Base error for MT Bridge communication issues"""
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict] = None):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}


class MTBridgeConnectionError(MTBridgeError):
    """Raised when connection to bridge fails"""
    pass


class MTBridgeAuthError(MTBridgeError):
    """Raised when authentication fails"""
    pass


class MTBridgeTimeoutError(MTBridgeError):
    """Raised when request times out"""
    pass


class MTBridgeOrderError(MTBridgeError):
    """Raised when order placement/management fails"""
    pass
