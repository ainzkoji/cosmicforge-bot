import asyncio
from app.core.bot_instance_service import get_bot_instance_service
from app.core.broker_service import get_decrypted_credentials
from app.runner.bot_context import BotRunContext

svc = get_bot_instance_service()
bots = svc.get_active_bot_instances()
print(f"Total active bots: {len(bots)}")

for target in bots:
    print(f"\n--- Bot: {target.id} ---")
    creds = get_decrypted_credentials(target.broker_account_id)
    if not creds:
        print("Failed to decrypt credentials!")
        continue
    print("Decrypted creds successfully!")
    try:
        risk_params = svc.get_risk_profile_preset(target.risk_level)
        context = BotRunContext.from_bot_instance(target, creds, risk_params)
        print("Context built successfully!")
        print(f"Mode: {context.execution_mode}, Symbols: {len(context.symbols)}")
    except Exception as e:
        print(f"Failed to build context: {e}")
