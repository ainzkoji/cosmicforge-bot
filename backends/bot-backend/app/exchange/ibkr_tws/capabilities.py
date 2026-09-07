"""
IBKR TWS Broker Capabilities.

Defines what IBKR supports vs crypto exchanges.
"""

from dataclasses import dataclass
from typing import List


@dataclass
class BrokerCapabilities:
    """Broker capability flags."""
    
    # Order types
    supports_market_orders: bool = True
    supports_limit_orders: bool = True
    supports_stop_orders: bool = True
    supports_stop_limit_orders: bool = True
    supports_trailing_stop: bool = True
    
    # Position features
    supports_reduce_only: bool = False
    supports_hedging: bool = False
    supports_per_symbol_leverage: bool = False
    
    # Advanced order features
    supports_attached_sl_tp: bool = True  # Via bracket orders
    supports_modify_open_orders: bool = True
    supports_oco_orders: bool = True
    
    # Position mode
    position_mode: str = "net"  # IB uses net positions per contract
    
    # Bridge requirement
    requires_bridge_running: bool = True
    bridge_types_supported: List[str] = None
    
    # Rate limits (conservative, per IBKR API guidelines)
    max_orders_per_second: int = 5
    max_requests_per_second: int = 50
    
    # Market data
    supports_realtime_data: bool = True
    supports_historical_data: bool = True
    historical_data_limit_days: int = 365
    
    # Margin
    margin_enforced_by_broker: bool = True  # IBKR controls margin, not the bot


IBKR_TWS_CAPABILITIES = BrokerCapabilities(
    supports_market_orders=True,
    supports_limit_orders=True,
    supports_stop_orders=True,
    supports_stop_limit_orders=True,
    supports_trailing_stop=True,
    
    supports_reduce_only=False,  # Close position via opposite order
    supports_hedging=False,  # Net positions only
    supports_per_symbol_leverage=False,
    
    supports_attached_sl_tp=True,  # Bracket orders
    supports_modify_open_orders=True,
    supports_oco_orders=True,
    
    position_mode="net",
    
    requires_bridge_running=True,
    bridge_types_supported=["tws", "ib_gateway"],
    
    max_orders_per_second=5,
    max_requests_per_second=50,
    
    supports_realtime_data=True,
    supports_historical_data=True,
    historical_data_limit_days=365,
    
    margin_enforced_by_broker=True
)
