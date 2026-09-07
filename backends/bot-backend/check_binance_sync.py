"""
Query actual Binance positions via API and compare with database
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient
from shared_lib.persistence.db import DB

def check_binance_positions():
    print("=" * 80)
    print("CHECKING ACTUAL BINANCE POSITIONS VS DATABASE")
    print("=" * 80)
    
    # 1. Query actual Binance API
    print("\n📡 QUERYING BINANCE API FOR POSITIONS...\n")
    
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY or "",
        api_secret=settings.BINANCE_API_SECRET or "",
        base_url=settings.BINANCE_FAPI_BASE_URL
    )
    
    try:
        positions = client.position_risk()
        
        open_positions = [p for p in positions if float(p.get('positionAmt', 0)) != 0]
        
        if open_positions:
            print(f"✅ FOUND {len(open_positions)} OPEN POSITIONS ON BINANCE:\n")
            for pos in open_positions:
                symbol = pos['symbol']
                amt = float(pos['positionAmt'])
                entry = pos['entryPrice']
                upnl = pos['unRealizedProfit']
                leverage = pos['leverage']
                
                print(f"  {symbol}:")
                print(f"    Quantity:     {amt}")
                print(f"    Entry Price:  ${entry}")
                print(f"    Leverage:     {leverage}x")
                print(f"    Unrealized PNL: ${upnl}")
                print()
        else:
            print("❌ NO OPEN POSITIONS FOUND ON BINANCE\n")
            
    except Exception as e:
        print(f"❌ ERROR querying Binance: {e}\n")
        import traceback
        traceback.print_exc()
    
    # 2. Check bot database
    print("=" * 80)
    print("CHECKING BOT DATABASE...")
    print("=" * 80 + "\n")
    
    db = DB()
    with db.connect() as conn:
        cursor = conn.execute("""
            SELECT symbol, position, entry_price, entry_qty, last_action
            FROM bot_symbol_state
            WHERE position IS NOT NULL AND position != 'flat' AND position != 'NONE'
        """)
        db_positions = cursor.fetchall()
    
    if db_positions:
        print(f"Database shows {len(db_positions)} positions:\n")
        for pos in db_positions:
            print(f"  {pos[0]}: {pos[1]} - Qty: {pos[3]}, Entry: {pos[2]}")
    else:
        print("❌ Database shows NO positions\n")
    
    print("=" * 80)
    print("🔍 ANALYSIS:")
    print("=" * 80)
    print("\nIf Binance shows positions but database doesn't:")
    print("  → Positions may have been opened manually on Binance")
    print("  → OR bot lost sync with exchange")
    print("  → Bot needs to reconcile state with exchange\n")

if __name__ == "__main__":
    check_binance_positions()
