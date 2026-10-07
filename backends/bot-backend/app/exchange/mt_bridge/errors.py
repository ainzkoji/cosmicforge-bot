"""
MetaTrader Bridge Error Classes
"""

from typing import Optional, Dict


class MTBridgeError(Exception):
    """Base error for MT Bridge communication issues

    ``status_code`` is the bridge's HTTP status when it answered at all.
    ``failure_kind`` is a fixed, bridge-independent category (``TIMEOUT``,
    ``UNREACHABLE``, ``REQUEST_FAILED``, ``INVALID_RESPONSE``, ``HTTP_ERROR``,
    ``DESTINATION_NOT_ALLOWED``). API routes report those two to a caller
    instead of the message, which can carry text the bridge chose.
    """
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict] = None,
        *,
        status_code: Optional[int] = None,
        failure_kind: Optional[str] = None,
    ):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}
        self.status_code = status_code
        self.failure_kind = failure_kind


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
