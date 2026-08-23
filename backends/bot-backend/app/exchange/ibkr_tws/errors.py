"""
IBKR TWS specific error classes.
"""


class IBKRError(Exception):
    """Base exception for IBKR TWS errors."""
    pass


class IBKRConnectionError(IBKRError):
    """Connection to TWS/Gateway failed or lost."""
    pass


class IBKRAuthError(IBKRError):
    """Authentication issue - TWS not logged in."""
    pass


class IBKROrderError(IBKRError):
    """Order placement or management failed."""
    pass


class IBKRContractError(IBKRError):
    """Contract lookup or qualification failed."""
    pass


class IBKRDataError(IBKRError):
    """Market data request failed."""
    pass


class IBKRPacingError(IBKRError):
    """TWS pacing violation - rate limit exceeded."""
    pass


class IBKRAccountError(IBKRError):
    """Account-related error."""
    pass


# TWS Error Code Mapping
# Based on: https://interactivebrokers.github.io/tws-api/message_codes.html
TWS_ERROR_MAP = {
    # Connection errors
    502: ("Not connected to TWS", IBKRConnectionError),
    504: ("Not connected to IB server", IBKRConnectionError),
    1100: ("Connectivity lost", IBKRConnectionError),
    2103: ("Market data farm connection broken", IBKRConnectionError),
    2110: ("Connectivity restored - data maintained", None),  # Info, not error
    
    # Order errors
    201: ("Order rejected - reason follows", IBKROrderError),
    399: ("Order message - warning", IBKROrderError),
    434: ("Order size does not conform to market rule", IBKROrderError),
    
    # Pacing errors
    100: ("Max rate of messages per second has been exceeded", IBKRPacingError),
    420: ("Error validating request - pacing violation", IBKRPacingError),
    
    # Data errors
    162: ("Historical data query error", IBKRDataError),
    200: ("No security definition found", IBKRContractError),
    321: ("Error validating request", IBKRDataError),
    
    # Account errors
    1102: ("Connectivity restored - data lost", IBKRConnectionError),
    2104: ("Market data farm connection OK", None),  # Info
    2106: ("Historical data farm connection OK", None),  # Info
}


def map_tws_error(error_code: int, error_msg: str) -> IBKRError:
    """
    Map TWS error code to appropriate exception.
    
    Args:
        error_code: TWS error code
        error_msg: Error message from TWS
        
    Returns:
        Appropriate IBKRError subclass instance
    """
    if error_code in TWS_ERROR_MAP:
        mapped_msg, exception_class = TWS_ERROR_MAP[error_code]
        if exception_class is None:
            # Informational message, not an error
            return None
        full_msg = f"[{error_code}] {mapped_msg}: {error_msg}"
        return exception_class(full_msg)
    
    # Unknown error code
    return IBKRError(f"[{error_code}] {error_msg}")
