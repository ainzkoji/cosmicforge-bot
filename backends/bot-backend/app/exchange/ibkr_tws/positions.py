"""
IBKR Position and Balance Management Module.

Handles position queries and account balance retrieval.
"""

import logging
import time
from typing import List, Dict, Optional
from decimal import Decimal
from ib_insync import PortfolioItem

from app.models.unified_trading import UnifiedPosition, PositionMode, Side
from .errors import IBKRAccountError

logger = logging.getLogger(__name__)


class IBKRPositionManager:
    """
    Position and balance management for IBKR TWS.
    
    Responsibilities:
    - Get open positions → UnifiedPosition
    - Get account balance and margin info
    - Convert IB position data to our format
    """
    
    def __init__(self, client, contract_provider):
        """
        Initialize position manager.
        
        Args:
            client: IBKRTwsClient instance
            contract_provider: IBKRContractProvider instance
        """
        self.client = client
        self.contracts = contract_provider
    
    async def get_positions(self) -> List[UnifiedPosition]:
        """
        Get all open positions.
        
        Returns:
            List of UnifiedPosition objects
        """
        if not self.client.account_id:
            raise IBKRAccountError("No account ID set")
        
        try:
            # Request portfolio items
            portfolio: List[PortfolioItem] = self.client.ib.portfolio(self.client.account_id)
            
            positions = []
            for item in portfolio:
                # Only include items with non-zero position
                if item.position == 0:
                    continue
                
                unified_pos = self._portfolio_item_to_position(item)
                if unified_pos:
                    positions.append(unified_pos)
            
            logger.debug(f"Retrieved {len(positions)} open positions")
            return positions
            
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            raise IBKRAccountError(f"Position retrieval failed: {e}") from e
    
    async def get_balance(self) -> Dict[str, Decimal]:
        """
        Get account balance information.
        
        Returns:
            Dict with keys: wallet, equity, available, margin_used
        """
        if not self.client.account_id:
            raise IBKRAccountError("No account ID set")
        
        try:
            # Request account summary
            summary_items = self.client.ib.accountSummary(self.client.account_id)
            
            # Build summary dict
            summary = {}
            for item in summary_items:
                summary[item.tag] = item.value
            
            # Extract key values
            # NetLiquidation = total account value
            # AvailableFunds = available for trading
            # GrossPositionValue = total position value
            
            net_liquidation = Decimal(summary.get("NetLiquidation", "0"))
            available_funds = Decimal(summary.get("AvailableFunds", "0"))
            gross_position = Decimal(summary.get("GrossPositionValue", "0"))
            
            # Calculate margin used (approximate)
            margin_used = net_liquidation - available_funds
            
            balance = {
                "wallet": net_liquidation,
                "equity": net_liquidation,
                "available": available_funds,
                "margin_used": margin_used if margin_used > 0 else Decimal("0")
            }
            
            logger.debug(f"Account balance: equity={net_liquidation}, available={available_funds}")
            return balance
            
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            raise IBKRAccountError(f"Balance retrieval failed: {e}") from e
    
    def _portfolio_item_to_position(self, item: PortfolioItem) -> Optional[UnifiedPosition]:
        """
        Convert IB PortfolioItem to UnifiedPosition.
        
        Args:
            item: ib_insync PortfolioItem
            
        Returns:
            UnifiedPosition or None if conversion fails
        """
        try:
            # Extract symbol (best effort)
            if hasattr(item.contract, 'symbol') and hasattr(item.contract, 'currency'):
                base = item.contract.symbol
                quote = item.contract.currency
                symbol = f"{base}_{quote}"
            else:
                logger.warning(f"Cannot determine symbol for portfolio item: {item}")
                return None
            
            # Position size (signed: positive=long, negative=short)
            position_size = item.position
            if position_size == 0:
                return None
            
            # Determine side
            side = Side.BUY if position_size > 0 else Side.SELL
            qty_abs = abs(position_size)
            
            # Convert from base currency units to LOTS
            # position_size is in base currency units (e.g., 100,000 for 1 lot)
            qty_lots = Decimal(str(qty_abs / 100000))
            
            # Prices
            avg_cost = Decimal(str(item.averageCost)) if item.averageCost else Decimal("0")
            market_price = Decimal(str(item.marketPrice)) if item.marketPrice else avg_cost
            
            # PnL
            unrealized_pnl = Decimal(str(item.unrealizedPNL)) if item.unrealizedPNL else Decimal("0")
            realized_pnl = Decimal(str(item.realizedPNL)) if item.realizedPNL else Decimal("0")
            
            # Position ID (use contract conId if available)
            position_id = str(item.contract.conId) if item.contract.conId else symbol
            
            return UnifiedPosition(
                symbol=symbol,
                broker_id="ibkr",
                position_id=position_id,
                side=side,
                quantity=qty_lots,
                entry_price=avg_cost,
                current_price=market_price,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=realized_pnl,
                margin_used=Decimal("0"),  # Would need separate calculation
                leverage=Decimal("1"),  # IB controls leverage per account
                mode=PositionMode.ONE_WAY,  # IB uses net positions
                timestamp=int(time.time() * 1000)
            )
            
        except Exception as e:
            logger.error(f"Failed to convert portfolio item to position: {e}")
            return None
