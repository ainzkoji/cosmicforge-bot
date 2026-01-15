"""Check which Binance environment the bot is actually using."""
from app.core.config import settings

print("=== BOT CONFIGURATION ===")
print(f"BINANCE_ENV: {getattr(settings, 'BINANCE_ENV', 'NOT SET')}")
print(f"BINANCE_FAPI_BASE_URL: {settings.BINANCE_FAPI_BASE_URL}")
print(f"EXECUTION_MODE: {settings.EXECUTION_MODE}")
print(f"TRADE_USDT_PER_ORDER: {settings.TRADE_USDT_PER_ORDER}")
print(f"DEFAULT_LEVERAGE: {settings.DEFAULT_LEVERAGE}")

# Check if testnet or mainnet based on URL
url = settings.BINANCE_FAPI_BASE_URL
if "testnet" in url.lower():
    print("\n✅ Bot is connected to TESTNET")
else:
    print("\n⚠️  Bot is connected to MAINNET (real money!)")
