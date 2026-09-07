from app.models.unified_trading import OrderStatus

class IBKRError(Exception):
    """Base class for all IBKR errors."""
    pass

class IBKRConnectionError(IBKRError):
    """Failed to connect to Client Portal Gateway."""
    pass

class IBKRAuthError(IBKRError):
    """Authentication or Session invalid."""
    pass

class IBKROrderError(IBKRError):
    """Order placement failed."""
    pass
