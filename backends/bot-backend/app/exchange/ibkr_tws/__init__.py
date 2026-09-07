"""
IBKR TWS API Integration Module.

Connects to Interactive Brokers via TWS or IB Gateway using ib_insync.
Implements ExchangeClient interface for seamless integration with the bot.
"""

from .adapter import IBKRTwsAdapter
from .capabilities import IBKR_TWS_CAPABILITIES

__all__ = ["IBKRTwsAdapter", "IBKR_TWS_CAPABILITIES"]
