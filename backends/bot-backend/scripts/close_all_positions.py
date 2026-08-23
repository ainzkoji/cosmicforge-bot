import sys
import os
import asyncio
import logging
from pathlib import Path

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root.parent / "shared"))

from shared_lib.persistence.db import DB
from app.core.bot_instance_service import get_bot_instance_service
from app.core.broker_service import get_decrypted_credentials
from app.runner.bot_context import BotRunContext
from app.exchange.factory import build_generic_exchange_client
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def close_all_positions():
    """
    Iterates through all bot instances and closes any open positions.
    """
    print("\n" + "="*60)
    print("🚀 STARTING: CLOSE ALL POSITIONS SCRIPT")
    print("="*60 + "\n")

    db = DB()
    service = get_bot_instance_service()
    
    # ONE-TIME INIT: Configure settings to ensure we can load them
    # (Settings are already loaded by import, but good to be sure)
    
    # 1. Fetch ALL instances (Active, Paused, Stopped)
    # We want to clean up everything.
    with db.connect() as conn:
        rows = conn.execute("SELECT id FROM bot_instances").fetchall()
        instance_ids = [r["id"] for r in rows]

    print(f"📋 Found {len(instance_ids)} bot instances in database.")
    
    for i, instance_id in enumerate(instance_ids, 1):
        try:
            instance = service.get_bot_instance(instance_id)
            if not instance:
                logger.warning(f"Instance {instance_id} not found (skipping)")
                continue

            print(f"\n[{i}/{len(instance_ids)}] Processing Instance: {instance.id}")
            print(f"   User: {instance.user_id}")
            print(f"   Account: {instance.broker_account_id}")
            print(f"   Market: {instance.market_type}")
            
            # 🛑 STOP ACTIVE BOT FIRST
            if instance.status == "active":
                try:
                    print(f"   🛑 Creating safe environment: Stopping active bot instance...")
                    service.stop_bot_instance(instance.id)
                    # Refresh instance state
                    instance = service.get_bot_instance(instance.id)
                    print(f"      ✅ Bot stopped.")
                except Exception as e:
                    logger.error(f"      ❌ Failed to stop bot: {e}")
                    # Continue anyway to try closing positions
            
            # A. Decrypt Credentials
            creds = get_decrypted_credentials(instance.broker_account_id)
            if not creds:
                logger.error(f"   ❌ Failed to decrypt credentials for {instance.broker_account_id}")
                continue

            # B. Build Context & Client
            # Use safe defaults for risk params as we just want to close
            risk_params = {"risk_profile": "safety_override"}
            
            context = BotRunContext.from_bot_instance(
                instance=instance,
                broker_credentials=creds,
                risk_params=risk_params
            )
            
            try:
                # Use generic client (Adapter) which supports close_position
                client = build_generic_exchange_client(context)
            except Exception as e:
                logger.error(f"   ❌ Failed to build client: {e}")
                continue

            # C. Fetch Positions
            try:
                # Use standard interface if available, otherwise try adapter wrapping
                # Most clients from factory are raw specific clients (BinanceFuturesClient etc.)
                # We need to handle them appropriately.
                
                # Check if it has a unified get_positions, or specific one
                if hasattr(client, 'get_positions'):
                    positions = client.get_positions()
                elif hasattr(client, 'position_risk'):
                     # Binance legacy raw client
                     # We might need to wrap it ourselves if it's not wrapped
                     # Or just use the raw method if we know it
                     raw_positions = client.position_risk()
                     # Filter for non-zero
                     positions = [p for p in raw_positions if float(p.get("positionAmt", 0)) != 0]
                else:
                    logger.warning(f"   ⚠️ Client {type(client).__name__} does not have known position method")
                    continue
                    
                if not positions:
                    print(f"   ✅ No open positions found.")
                    continue
                
                print(f"   ⚠️ FOUND {len(positions)} OPEN POSITIONS!")
                
                # D. Close Positions
                for pos in positions:
                    # Handle different position objects (dict vs Model)
                    if isinstance(pos, dict):
                        symbol = pos.get("symbol")
                        amt = float(pos.get("positionAmt", 0))
                    else:
                        symbol = pos.symbol
                        # UnifiedPosition has 'quantity' (always positive) and 'side'
                        raw_qty = float(pos.quantity)
                        # Determine sign based on side
                        # Side is an Enum, handle string or Enum comparison
                        is_short = str(pos.side).lower() == "sell"
                        amt = -raw_qty if is_short else raw_qty
                        
                    if amt == 0:
                        continue
                        
                    print(f"      - Closing {symbol} (Size: {amt})...")
                    
                    try:
                        # 1. CANCEL OPEN ORDERS FIRST to free up margin/position
                        try:
                            open_orders = client.list_open_orders(symbol)
                            if open_orders:
                                print(f"        ⚠️ found {len(open_orders)} open orders. Cancelling...")
                                for o in open_orders:
                                    client.cancel_order(symbol, o.broker_order_id)
                                print(f"        ✅ Cancelled open orders.")
                        except Exception as e:
                            print(f"        ⚠️ Failed to cancel orders (proceeding anyway): {e}")

                        # 2. Attempt to use close_position if available (Adapter)
                        if hasattr(client, 'close_position'):
                            # Adapter interface close_position usually takes just symbol
                            client.close_position(symbol)
                            print(f"        ✅ Sent close command.")
                        # Binance Raw fallback
                        elif hasattr(client, 'new_order'): 
                            side = "SELL" if amt > 0 else "BUY"
                            # MARKET CLOSE
                            client.new_order(
                                symbol=symbol,
                                side=side,
                                type="MARKET",
                                quantity=abs(amt),
                                reduceOnly=True
                            )
                            print(f"        ✅ Sent MARKET {side} to close.")
                        else:
                            print(f"        ❌ Unable to close: Client has no close method.")
                            
                    except Exception as e:
                        print(f"        ❌ FAILED to close {symbol}: {e}")

            except Exception as e:
                logger.error(f"   ❌ Error fetching/closing positions: {e}")

        except Exception as e:
            logger.error(f"   ❌ Critical error processing instance {instance_id}: {e}")

    print("\n" + "="*60)
    print("🏁 FINISHED: CLOSE ALL POSITIONS SCRIPT")
    print("="*60 + "\n")

if __name__ == "__main__":
    asyncio.run(close_all_positions())
