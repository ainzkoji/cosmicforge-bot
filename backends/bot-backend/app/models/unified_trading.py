from __future__ import annotations
from enum import Enum
from decimal import Decimal
from typing import Optional, List, Dict
from pydantic import BaseModel, Field, field_validator

# ==========================================
# 1. CORE ENUMS
# ==========================================

class AssetClass(str, Enum):
    CRYPTO_PERP = "crypto_perp"   # Binance/Bybit Futures
    CRYPTO_SPOT = "crypto_spot"
    FOREX_SPOT = "forex_spot"
    FOREX_CFD = "forex_cfd"       # OANDA V20
    EQUITY_CFD = "equity_cfd"
    COMMODITY_CFD = "commodity_cfd"

class PositionMode(str, Enum):
    ONE_WAY = "one_way"   # Net Position (Standard Crypto)
    HEDGE = "hedge"       # Hedge Mode (Binance Futures)
    TICKET = "ticket"     # Discrete Tickets (Forex/Metatrader)

class OrderStatus(str, Enum):
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"

class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"

class ProtectionType(str, Enum):
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    TRAILING_STOP = "trailing_stop"

# ==========================================
# 2. INSTRUMENT SPEC (Ref: foundation_design_v3.md)
# ==========================================

class InstrumentSpec(BaseModel):
    """
    Broker-Agnostic Instrument Specification.
    Strict Decimal enforcement for all sizing/price attributes.
    """
    # Identity
    symbol_canonical: str         # "EURUSD", "BTCUSDT" (System ID)
    symbol_exchange: str          # "EUR_USD", "BTC-USDT" (Broker ID)
    asset_class: AssetClass
    
    # Currency
    base_currency: str            # "EUR" (in EUR/USD)
    quote_currency: str           # "USD" (in EUR/USD)
    margin_currency: str          # "USD" (Balance currency)
    settlement_currency: str      # usually same as margin

    # Sizing (STRICT DECIMAL)
    contract_size: Decimal        # Multiplier. Crypto=1, Forex=100000
    tick_size: Decimal            # Price step (e.g. 0.00001)
    step_size: Decimal            # Qty step (e.g. 0.01 Lots)
    min_qty: Decimal              # Min order size
    min_notional: Optional[Decimal] = None
    
    # Precision (Optional helpers for formatting)
    price_precision: int = 2
    qty_precision: int = 2

    # Leverage Configuration
    max_leverage: Decimal
    supports_per_order_leverage: bool = False

    class Config:
        json_encoders = {Decimal: str}

    # Helper to validate incoming floats converted to Decimal aren't accidental
    @field_validator("contract_size", "tick_size", "step_size", "min_qty", "min_notional", mode="before")
    def force_decimal(cls, v):
        if v is None:
            return None
        return Decimal(str(v))

# ==========================================
# 3. UNIFIED POSITION & ORDER MODELS (Strict Decimal)
# ==========================================

class UnifiedPosition(BaseModel):
    # Identity
    symbol: str
    broker_id: str
    position_id: Optional[str] = None  # Ticket ID for specific positions (VS net)
    
    # State
    side: Side               # BUY (Long) / SELL (Short)
    quantity: Decimal        # Always positive
    entry_price: Decimal
    current_price: Decimal
    
    # PnL & Risk
    unrealized_pnl: Decimal
    realized_pnl: Decimal    # For this session/ticket
    margin_used: Decimal
    leverage: Decimal
    
    # Metadata
    mode: PositionMode       # Net vs Ticket
    timestamp: int

    @field_validator("quantity", "entry_price", "unrealized_pnl", "margin_used", mode="before")
    def to_decimal(cls, v):
        return Decimal(str(v))


class UnifiedOrder(BaseModel):
    client_order_id: str
    broker_order_id: str
    symbol: str
    side: Side
    type: str                # MARKET, LIMIT, STOP
    
    # Quantities
    qty_ordered: Decimal
    qty_filled: Decimal
    avg_fill_price: Optional[Decimal] = None
    
    status: OrderStatus
    timestamp: int
    reduce_only: bool = False
    
    fees: Decimal = Decimal("0")
    fee_currency: str = ""
    error_message: Optional[str] = None

    @field_validator("qty_ordered", "qty_filled", "avg_fill_price", "fees", mode="before")
    def to_decimal(cls, v):
        if v is None:
            return None
        return Decimal(str(v))


class UnifiedFill(BaseModel):
    fill_id: str
    order_id: str
    client_order_id: Optional[str]
    symbol: str
    side: Side
    
    qty: Decimal
    price: Decimal
    commission: Decimal
    commission_asset: str
    
    timestamp: int
    is_maker: bool = False

    @field_validator("qty", "price", "commission", mode="before")
    def to_decimal(cls, v):
        return Decimal(str(v))

class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"
    BOTH = "both"

class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_MARKET = "stop_market"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_MARKET = "take_profit_market"
    TRAILING_STOP_MARKET = "trailing_stop_market"

class IdempotencyMode(str, Enum):
    NONE = "none"
    CLIENT_ORDER_ID = "client_order_id"
    HEADER = "header"
    BOTH = "both"

# ... (Existing Enums: AssetClass, PositionMode, OrderStatus, Side, ProtectionType) ...

# ==========================================
# 4. ORDER REQUEST MODEL
# ==========================================

class OrderRequest(BaseModel):
    """
    Standardized Order Placement Request.
    """
    symbol: str  # Exchange Symbol (e.g. "EUR_USD")
    side: Side
    type: OrderType
    qty: Decimal # Logic Units (Contracts/Lots)
    
    # Price Fields
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    
    # Modifiers
    reduce_only: bool = False
    time_in_force: str = "GTC"
    
    # Unified Idempotency
    client_order_id: Optional[str] = None
    
    # Attached Protection (if supported)
    sl_price: Optional[Decimal] = None
    tp_price: Optional[Decimal] = None
    
    # Metadata
    leverage: Optional[Decimal] = None # For per-order leverage setting

    @field_validator("qty", "price", "stop_price", "sl_price", "tp_price", "leverage", mode="before")
    def to_decimal(cls, v):
        if v is None: return None
        return Decimal(str(v))


# ==========================================
# 5. PROTECTION MODELS
# ==========================================

class ProtectionRequest(BaseModel):
    symbol: str
    position_side: Side    # BUY (Long) or SELL (Short)
    qty: Decimal           # Usually entire position
    
    # One or more can be set
    sl_price: Optional[Decimal] = None
    tp_price: Optional[Decimal] = None
    trailing_delta: Optional[Decimal] = None
    
    is_oco: bool = False   # Broker supports OCO?
    linked_order_id: Optional[str] = None # If attaching to pending order
    reduce_only: bool = True  # Always true for protection / exit orders

    @field_validator("qty", "sl_price", "tp_price", "trailing_delta", mode="before")
    def to_decimal(cls, v):
        if v is None: return None
        return Decimal(str(v))


class ProtectionResult(BaseModel):
    sl_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    status: str
    error: Optional[str] = None


class ProtectionUpdateRequest(BaseModel):
    """
    Request to mutate existing SL/TP orders via cancel-replace.
    Used for trailing stop updates, break-even moves, and partial TP adjustments.
    """
    symbol: str
    position_side: str          # "LONG" or "SHORT"
    new_sl_price: Optional[float] = None
    new_tp_price: Optional[float] = None
    qty: float                  # Current open quantity
    old_sl_order_id: Optional[str] = None  # cancel-replace: cancel this first
    old_tp_order_id: Optional[str] = None
    reduce_only: bool = True    # Always True for exit orders
    reason: str = ""            # e.g. "TRAILING_UPDATE", "BREAK_EVEN", "TP1_PARTIAL"


# ==========================================
# 6. SYMBOL FILTERS (Sizing Limits)
# ==========================================

class SymbolFilters(BaseModel):
    """
    Standardized trading limits for a symbol.
    """
    min_qty: Decimal = Decimal("0")
    max_qty: Optional[Decimal] = None
    step_size: Decimal = Decimal("0")
    
    min_notional: Decimal = Decimal("0") # Min order value (qty * price)
    min_order_value: Optional[Decimal] = None # Alternative to min_notional
    
    tick_size: Decimal = Decimal("0") # Price tick
    
    contract_size: Decimal = Decimal("1")
    
    @field_validator("min_qty", "max_qty", "step_size", "min_notional", "min_order_value", "tick_size", "contract_size", mode="before")
    def force_decimal(cls, v):
        if v is None: return None
        return Decimal(str(v))

