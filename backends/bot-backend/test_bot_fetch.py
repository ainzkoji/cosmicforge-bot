import asyncio
from app.core.bot_instance_service import get_bot_instance_service
from app.core.broker_service import get_decrypted_credentials
from app.runner.bot_context import BotRunContext

svc = get_bot_instance_service()
bots = svc.get_active_bot_instances()
print(f"Total active bots: {len(bots)}")

target = next((b for b in bots if b.id == "bot_0064a4b6dd86"), None)
if not target:
    print("Bot bot_0064a4b6dd86 NOT FOUND in get_active_bot_instances() output!")
else:
    print("Bot found in active instances!")
    print(f"Decrypting creds for {target.broker_account_id}...")
    creds = get_decrypted_credentials(target.broker_account_id)
    if not creds:
        print("Failed to decrypt credentials!")
    else:
        print("Decrypted creds successfully!")
        print("Building Run Context...")
        try:
            risk_params = svc.get_risk_profile_preset(target.risk_level)
            context = BotRunContext.from_bot_instance(target, creds, risk_params)
            print("Context built successfully!")
            print(f"Mode: {context.execution_mode}, Symbols: {len(context.symbols)}")
        except Exception as e:
            print(f"Failed to build context: {e}")
