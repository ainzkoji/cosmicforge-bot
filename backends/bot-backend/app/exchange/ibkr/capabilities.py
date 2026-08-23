from app.exchange.interface import BrokerCapabilities
from app.models.unified_trading import PositionMode, IdempotencyMode

IBKR_CAPABILITIES = BrokerCapabilities(
    position_mode=PositionMode.ONE_WAY,
    supports_hedging=False, # IBKR US is net positions (FIFO)
    supports_ticket_mode=True, # Each order has its own trade context
    supports_reduce_only=False, # IBKR closes positions by opposite orders, not reduce-only flag
    supports_market_orders=True,
    supports_limit_orders=True, # IBKR supports limit orders
    supports_per_symbol_leverage=False, # Leverage is account-based
    
    supports_attached_sl_tp=False, # Cannot attach bracket orders in single request via CPAPI easily
    supports_separate_protection=True, # Can add/modify protection after entry
    supports_oco=True, # One-Cancels-Other supported
    supports_trailing_stop=True,
    
    idempotency_mode=IdempotencyMode.NONE, # CPAPI doesn't have standard idempotency key header
    supports_fills_endpoint=True
)

