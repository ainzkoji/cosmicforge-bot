import asyncio
from app.core.bot_instance_service import get_bot_instance_service
from app.core.broker_service import get_decrypted_credentials
from app.runner.bot_context import BotRunContext
from app.exchange.factory import build_exchange_client
from app.runner.runner import PaperRunner

async def main():
    svc = get_bot_instance_service()
    bots = svc.get_active_bot_instances()
    print(f"Total active bots: {len(bots)}")

    for target in bots:
        if target.id != 'bot_54b6ea63f7ce':
            continue
        print(f"\n--- Bot: {target.id} ---")
        creds = get_decrypted_credentials(target.broker_account_id)
        if not creds:
            print("Failed to decrypt credentials!")
            continue
        
        try:
            risk_params = svc.get_risk_profile_preset(target.risk_level)
            context = BotRunContext.from_bot_instance(target, creds, risk_params)
            print(f"Mode: {context.execution_mode}, Symbols: {len(context.symbols)}")
            
            client = build_exchange_client(context)
            runner = PaperRunner(client, context=context)
            
            print(f"Runner instantiated. Trade symbols: {runner.trade_symbols}")
            
            print("Running cycle...")
            res = runner.run_cycle()
            print(f"Cycle result: {res}")
            
        except Exception as e:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
