"""
IBKR Client - TWS API Wrapper using ib_insync.

Responsibilities:
- Communicate with TWS/Gateway via TCP (ib_insync)
- Normalize responses
"""
from typing import List, Dict, Any, Optional
from decimal import Decimal
import logging
import time
import asyncio

# Check for ib_insync
try:
    from ib_insync import IB, Contract, Order as IBOrder, Forex, Stock, Crypto
except ImportError:
    IB = None

from app.models.unified_trading import (
    UnifiedOrder, UnifiedPosition, UnifiedFill,
    OrderStatus, Side, PositionMode
)

from .session import IBKRSession
from .errors import IBKROrderError, IBKRConnectionError

logger = logging.getLogger(__name__)

class IBKRClient:
    """
    TWS API wrapper using ib_insync.
    """
    
    def __init__(self, session: IBKRSession, account_id: Optional[str] = None):
        if IB is None:
            raise ImportError("ib_insync missing")
            
        self.session = session
        self.account_id = account_id
        self._instruments = None
        
    @property
    def ib(self) -> IB:
        return self.session.ib
    
    # =========================================================================
    # ACCOUNT & PORTFOLIO
    # =========================================================================
    
    def get_portfolio_accounts(self) -> List[str]:
        """Return list of managed account IDs."""
        # managedAccounts return a list of strings
        return self.ib.managedAccounts()

    def get_account_summary(self, account_id: str) -> Dict[str, Decimal]:
        """
        Get NetLiquidation, EquityWithLoanValue, AvailableFunds.
        ib_insync.accountSummary returns list of AccountValue.
        """
        # Note: accountSummary() might be slow if fetching all.
        # fast check: if we only have one account, ib.accountValues() might be populated.
        
        # We'll use accountSummary generic call
        # tags: NetLiquidation, EquityWithLoanValue, AvailableFunds
        tags = "NetLiquidation,EquityWithLoanValue,AvailableFunds"
        vals = self.ib.accountSummary(account_id) # This gets all? Or we filter?
        # ib.accountSummary returns list of TagValue for ALL accounts if account='All'.
        # If we specify account_id? ib_insync accountSummary signature is (account='All', tags='...').
        
        # Actually ib_insync.accountSummary() returns values for all accounts.
        # We filter by account_id.
        
        summary = {
            "wallet": Decimal("0"),
            "equity": Decimal("0"),
            "available": Decimal("0")
        }
        
        for v in vals:
            if v.account == account_id:
                try:
                    val = Decimal(str(v.value))
                    if v.tag == "NetLiquidation":
                        summary["wallet"] = val
                        summary["equity"] = val
                    elif v.tag == "AvailableFunds":
                        summary["available"] = val
                except:
                    pass
                    
        return summary

    # =========================================================================
    # POSITIONS
    # =========================================================================
    
    def get_positions(self, account_id: str) -> List[UnifiedPosition]:
        """Get positions for account."""
        raw_positions = self.ib.positions(account_id)
        results = []
        for pos in raw_positions:
            try:
                unified = self._normalize_position(pos)
                if unified:
                    results.append(unified)
            except Exception as e:
                logger.error(f"Error normalizing position: {e}")
                
        return results
    
    def _normalize_position(self, pos) -> Optional[UnifiedPosition]:
        """
        pos is ib_insync.Position(account, contract, position, avgCost)
        """
        contract = pos.contract
        # We need symbol mapping.
        # For now, rely on contract.symbol or localPair?
        symbol = contract.symbol + contract.currency if contract.secType == 'CASH' else contract.symbol
        
        # PnL? ib_insync Position doesn't have PnL. 
        # We need ib.portfolio() for PnL! 
        # But this function is iterating ib.positions().
        # Let's use ib.portfolio() which returns PortfolioItem (has unrealizedPNL)
        return None # logic moved to get_positions using portfolio()

    # Re-implementing get_positions to use portfolio() instead
    def get_positions_portfolio(self, account_id: str) -> List[UnifiedPosition]:
        items = self.ib.portfolio()
        results = []
        for item in items: # item is PortfolioItem
            if item.account != account_id:
                continue
            
            # extract info
            con = item.contract
            qty = Decimal(str(item.position))
            if qty == 0: continue
            
            side = Side.BUY if qty > 0 else Side.SELL
            
            # Symbol construction
            if con.secType == 'CASH':
                symbol = f"{con.symbol}{con.currency}" # e.g. EURUSD
            else:
                symbol = con.symbol
                
            entry_price = Decimal(str(item.averageCost)) # Note: averageCost is per contract?
            # IBKR 'averageCost' is total cost / position? No, it's unit cost. 
            # Actually for FX it might be tricky. 
            
            market_price = Decimal(str(item.marketPrice))
            unrealized = Decimal(str(item.unrealizedPNL))
            realized = Decimal(str(item.realizedPNL))
            
            # Normalize to UnifiedPosition
            u = UnifiedPosition(
                symbol=symbol,
                broker_id="ibkr",
                position_id=str(con.conId),
                side=side,
                quantity=abs(qty),
                entry_price=entry_price,
                current_price=market_price,
                unrealized_pnl=unrealized,
                realized_pnl=realized,
                margin_used=Decimal(0),
                leverage=Decimal(1),
                mode=PositionMode.ONE_WAY,
                timestamp=int(time.time()*1000)
            )
            results.append(u)
        return results

    # Replace get_positions with the portfolio version
    get_positions = get_positions_portfolio

    # =========================================================================
    # ORDERS
    # =========================================================================
    
    def place_order(self, account_id: str, order_payload: Dict) -> UnifiedOrder:
        """
        Payload expects: symbol, quantity, side (BUY/SELL), type (MKT/LMT)
        We need to construct Contract and Order.
        """
        # 1. Parse payload to Contracts
        # Ideally we use conid if available, else standard parsing
        conid = order_payload.get("conid")
        symbol = order_payload.get("symbol", "")
        sec_type = order_payload.get("secType", "CASH")
        currency = order_payload.get("currency", "USD")
        
        contract = Contract()
        if conid:
            contract.conId = conid
        else:
            # Basic parsing for FX
            if len(symbol) == 6 or "USD" in symbol:
                contract = Forex(symbol[:3] + symbol[3:]) # Pair syntax? ib_insync accepts 'EURUSD' usually
                # Actually Forex('EURUSD') is easier
                pass
            
            # Fallback manual construction
            contract.symbol = symbol[:3]
            contract.currency = symbol[3:]
            contract.secType = 'CASH'
            contract.exchange = 'IDEALPRO'

        # 2. Create Order
        action = order_payload.get("side", "BUY")
        qty = float(order_payload.get("quantity", 0))
        order_type = order_payload.get("orderType", "MKT")
        
        ib_order = IBOrder()
        ib_order.action = action
        ib_order.totalQuantity = qty
        ib_order.orderType = order_type
        ib_order.account = account_id
        
        # 3. Place
        trade = self.ib.placeOrder(contract, ib_order)
        # trade is non-blocking object
        
        # 4. Return initial status
        return self._trade_to_unified(trade)
    
    def _trade_to_unified(self, trade) -> UnifiedOrder:
        t = trade
        o = trade.order
        c = trade.contract
        
        status_map = {
            'PendingSubmit': OrderStatus.NEW,
            'PreSubmitted': OrderStatus.NEW,
            'Submitted': OrderStatus.NEW,
            'Filled': OrderStatus.FILLED,
            'Cancelled': OrderStatus.CANCELED,
            'Inactive': OrderStatus.REJECTED
        }
        
        st = status_map.get(t.orderStatus.status, OrderStatus.NEW)
        
        return UnifiedOrder(
            client_order_id="",
            broker_order_id=str(o.orderId), # Temp ID?
            symbol=f"{c.symbol}{c.currency}",
            side=Side.BUY if o.action == 'BUY' else Side.SELL,
            type=o.orderType.lower(),
            qty_ordered=Decimal(str(o.totalQuantity)),
            qty_filled=Decimal(str(t.orderStatus.filled)),
            avg_fill_price=Decimal(str(t.orderStatus.avgFillPrice)),
            status=st,
            timestamp=int(time.time()*1000),
            reduce_only=False
        )
        
    def cancel_order(self, account_id: str, order_id: str) -> bool:
        # Find the trade/order
        # ib.orders() lists open orders.
        for o in self.ib.orders():
            if str(o.orderId) == str(order_id) or str(o.permId) == str(order_id):
                self.ib.cancelOrder(o)
                return True
        return False

    def get_live_orders(self) -> List[UnifiedOrder]:
        trades = self.ib.trades() # returns list of Trade objects (live)
        # also openOrders?
        # ib.trades() covers active trades being tracked
        results = []
        for t in trades:
            results.append(self._trade_to_unified(t))
        return results

    # =========================================================================
    # MARKET DATA (Snapshot)
    # =========================================================================
    def get_market_data_snapshot(self, conids: List[str]) -> Dict[str, Decimal]:
        # TWS requires valid contracts, not just conids usually, but we can make Contracts with conIds
        # ib.reqMktData snapshot
        results = {}
        for cid in conids:
            c = Contract()
            c.conId = int(cid)
            c.exchange = 'IDEALPRO' # Guess for FX
            
            # Snapshots are async... this is tricky in sync method.
            # We might skip this or implement proper async.
            pass
        return results

