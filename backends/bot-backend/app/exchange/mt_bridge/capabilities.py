"""
MetaTrader Bridge Capabilities
Defines broker capabilities for MT4/MT5 integration.
"""

from app.exchange.interface import BrokerCapabilities
from app.models.unified_trading import PositionMode, IdempotencyMode


# MT4/MT5 Bridge Capabilities
MT_CAPABILITIES = BrokerCapabilities(
    # Position Mode
    position_mode=PositionMode.TICKET,  # MT uses ticket-based positions
    
    # Hedging & Position Modes
    supports_hedging=True,  # MT allows multiple positions per symbol
    supports_ticket_mode=True,
    supports_reduce_only=False,  # MT doesn't have reduce_only concept
    
    # Order Types
    supports_market_orders=True,
    supports_per_symbol_leverage=False,
    
    # Protection Capabilities
    supports_attached_sl_tp=True,  # Can send SL/TP with entry order
    supports_separate_protection=True,  # Can modify SL/TP after
    supports_oco=False,
    supports_trailing_stop=False,  # Not in v1 bridge contract
    
    # Idempotency
    idempotency_mode=IdempotencyMode.CLIENT_ORDER_ID,
    idempotency_key_header=None,
    
    # Fills Endpoint
    supports_fills_endpoint=False  # No fills endpoint in bridge v1
)
