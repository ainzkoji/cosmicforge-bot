from __future__ import annotations
from typing import Protocol, List, Dict, Any, Optional
from decimal import Decimal
from pydantic import BaseModel

from app.models.unified_trading import (
    InstrumentSpec, 
    UnifiedOrder, 
    UnifiedPosition, 
    UnifiedFill, 
    OrderRequest,
    ProtectionRequest, 
    ProtectionResult,
    ProtectionUpdateRequest,  # cancel-replace mutation
    Side,
    PositionMode,
    IdempotencyMode,
    SymbolFilters  # <-- Added
)

class BrokerCapabilities(BaseModel):
    position_mode: PositionMode = PositionMode.ONE_WAY
    
    supports_hedging: bool = False
    supports_ticket_mode: bool = False
    supports_reduce_only: bool = True
    supports_market_orders: bool = True
    supports_per_symbol_leverage: bool = False
    
    # Protection Caps
    supports_attached_sl_tp: bool = False       # Can send with Entry?
    supports_separate_protection: bool = True   # Can send after Entry?
    supports_oco: bool = False
    supports_trailing_stop: bool = False
    supports_order_amend: bool = False          # True = native amend; False = cancel-replace
    
    # Idempotency
    idempotency_mode: IdempotencyMode = IdempotencyMode.NONE
    idempotency_key_header: Optional[str] = None 
    
    supports_fills_endpoint: bool = True

class ExchangeClient(Protocol):
    """
    Standard interface for ALL adapters (Binance, BingX, OANDA).
    Strict usage of Decimal for all monetary values.
    """
    
    @property
    def capabilities(self) -> BrokerCapabilities: ...

    # --- Discovery ---
    def list_instruments(self) -> List[InstrumentSpec]: ...
    def get_server_time(self) -> int: ...

    # --- Market Data ---
    def get_prices(self, symbols: List[str]) -> Dict[str, Decimal]: ...
    # get_klines might preserve existing generic return type for now
    def get_klines(self, symbol: str, interval: str, limit: int) -> List[Any]: ...
    
    def get_symbol_filters(self, symbol: str) -> SymbolFilters: ...

    # --- Trading ---
    def place_order(self, req: OrderRequest) -> UnifiedOrder: ...
    
    def cancel_order(self, symbol: str, order_id: str) -> bool: ...
    def get_order(self, symbol: str, order_id: str) -> UnifiedOrder: ...
    def list_open_orders(self, symbol: Optional[str] = None) -> List[UnifiedOrder]: ...

    # --- Fills & Protection ---
    def get_fills(self, symbol: str, start_time: int, limit: int = 100) -> List[UnifiedFill]: ...
    
    def place_protection(self, req: ProtectionRequest) -> ProtectionResult: ...

    def update_protection(self, req: ProtectionUpdateRequest) -> ProtectionResult:
        """
        Cancel-replace existing SL/TP orders with new prices.
        Used for trailing stop updates, break-even moves, and TP1 adjustments.
        If old_sl_order_id / old_tp_order_id are provided, those specific orders
        are cancelled first before placing new ones.
        Must set reduce_only=True on all replacement orders.
        """
        ...

    # --- Position & Account ---
    def get_positions(self) -> List[UnifiedPosition]: ...
    
    def get_balance(self) -> Dict[str, Decimal]: 
        """
        Returns { "wallet": Decimal(...), "equity": Decimal(...), "available": Decimal(...) }
        in Margin Currency.
        """
        ...
    
    def close_position(self, symbol: str, position_id: Optional[str] = None) -> UnifiedOrder: ...
