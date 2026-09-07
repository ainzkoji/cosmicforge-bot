import os
import sys
from dotenv import load_dotenv

load_dotenv(".env")
sys.path.insert(0, os.path.abspath("."))

from app.core.database import SessionLocal
from app.models.unified_trading import BrokerCredential
from app.exchange.binance.client import BinanceFuturesClient

def close_btc():
    db = SessionLocal()
    cred = db.query(BrokerCredential).filter(BrokerCredential.broker_id == "binance").first()
    if not cred:
        print("No binance credentials in DB!")
        return

    client = BinanceFuturesClient(
        api_key=cred.api_key,
        api_secret=cred.api_secret,
        base_url=os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com")
    )
    
    print(f"Closing BTC with API key from DB: {cred.api_key[:5]}...")
    res = client.close_position_market("BTCUSDT")
    print("Close response:", res)

if __name__ == "__main__":
    close_btc()
