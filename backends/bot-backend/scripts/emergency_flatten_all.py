import asyncio
import logging
import sys
import os
import traceback
import json
from pathlib import Path

# Add backends to path
sys.path.append(str(Path(__file__).parent.parent))

from app.core.bot_instance_service import get_bot_instance_service
from app.core.broker_service import resolve_broker_auth_for_bot
from app.exchange.factory import build_exchange_client_from_auth
from app.execution.executor import BinanceExecutor
from app.runner.models import SymbolState
from app.core.config import settings
from shared_lib.persistence.db import DB
from shared_lib.persistence.state_store import StateStore
from shared_lib.persistence.audit import Audit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("emergency_flatten")

async def emergency_flatten_all():
    service = get_bot_instance_service()
    db_path = settings.DATABASE_URL.replace("sqlite:///", "")
    db = DB(path=db_path)
    audit = Audit(db)
    
    print(f"=== EMERGENCY FLATTEN STARTING (DB: {db_path}) ===")
    
    # 1. Fetch all active bot instances
    try:
        active_bots = service.get_all_bot_instances()
        active_bots = [b for b in active_bots if b.status == 'active']
        print(f"Found {len(active_bots)} active bot instances.")
    except Exception as e:
        print(f"Error fetching bots: {e}")
        return

    for bot in active_bots:
        print(f"\nProcessing Bot: {bot.id} (User: {bot.user_id}, Account: {bot.broker_account_id})")
        
        try:
            # 2. Resolve Auth
            auth = resolve_broker_auth_for_bot(bot.broker_account_id, bot.user_id)
            print(f"  Auth resolved for {auth.broker_type} ({auth.environment.value})")
            
            # 3. Build Client & Executor
            client = build_exchange_client_from_auth(auth)
            # BinanceExecutor parameters: client, risk_gate, audit, execution_mode, live_symbols
            executor = BinanceExecutor(
                client=client, 
                risk_gate=None, 
                audit=audit, 
                execution_mode=bot.mode,
                live_symbols=bot.symbols
            )
            
            # 4. Initialize StateStore for this bot
            store = StateStore(db, bot_instance_id=bot.id)
            
            # 5. Symbols are already in the bot object
            symbols = bot.symbols
            print(f"  Bot tracks {len(symbols)} symbols.")
            
            # 6. Flatten Each Symbol
            for sym in symbols:
                sym = sym.upper()
                try:
                    # Cancel all open orders
                    client.cancel_all_orders(sym)
                    print(f"    {sym}: Cancelled all orders.")
                    
                    # Close position
                    res = executor.execute_signal(sym, "CLOSE", 0.0)
                    print(f"    {sym}: Market close triggered. Details: {getattr(res, 'details', str(res))}")
                    
                    # Clean up local SymbolState in DB to avoid desync
                    st = SymbolState()
                    st.position = "NONE"
                    st.entry_qty = 0.0
                    st.entry_price = None
                    store.save_symbol(sym, st)
                    
                except Exception as e:
                    print(f"    {sym}: Error during flatten: {e}")
            
            print(f"  Bot {bot.id} successfully flatted.")
            
        except Exception as e:
            print(f"  Error processing bot {bot.id}: {e}")
            traceback.print_exc()

    print("\n=== EMERGENCY FLATTEN COMPLETE ===")

if __name__ == "__main__":
    asyncio.run(emergency_flatten_all())
