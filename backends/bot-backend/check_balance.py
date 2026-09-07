import asyncio
import sys
import os

sys.path.append(os.path.abspath("."))
os.environ.setdefault("TZ", "UTC") 

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient

async def main():
    try:
        client = BinanceFuturesClient(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            base_url="https://testnet.binancefuture.com" if "testnet" in str(settings.BINANCE_API_KEY).lower() else "https://fapi.binance.com"
        )
        acc = client.account()
        wallet_balance = acc.get("totalWalletBalance", "0")
        available_balance = acc.get("availableBalance", "0")
        print(f"WALLET BALANCE: {wallet_balance} USDT")
        print(f"AVAILABLE BALANCE: {available_balance} USDT")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())
