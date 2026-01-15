"""Compare bot's internal state vs Binance actual positions."""
from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient

# Setup client
client = BinanceFuturesClient(
    api_key=settings.BINANCE_API_KEY,
    api_secret=settings.BINANCE_API_SECRET,
    base_url=settings.BINANCE_FAPI_BASE_URL,
    recv_window=settings.BINANCE_RECV_WINDOW,
)

print("=== BINANCE ACTUAL POSITIONS ===")
positions = client.position_risk()
open_positions = [p for p in positions if abs(float(p.get("positionAmt", 0))) > 0.00001]

if not open_positions:
    print("No open positions on Binance! ✅")
else:
    print(f"Found {len(open_positions)} open positions:")
    for p in open_positions:
        symbol = p.get("symbol")
        amt = p.get("positionAmt")
        entry = p.get("entryPrice")
        print(f"  {symbol}: {amt} @ {entry}")

print("\n=== BOT INTERNAL STATE (from DB) ===")
import sqlite3
import json

conn = sqlite3.connect("data/bot.db")
conn.row_factory = sqlite3.Row

# Check if there's a state table
try:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='symbol_state'").fetchall()
    if rows:
        state_rows = conn.execute("SELECT * FROM symbol_state LIMIT 10").fetchall()
        for r in state_rows:
            print(dict(r))
    else:
        print("No symbol_state table found (state is in-memory only)")
except Exception as e:
    print(f"Error: {e}")

conn.close()

print("\n=== CHECKING RUNNER STATE TRACKING ===")
print("The 'Decision: ADD' suggests the runner thinks it already has a position.")
print("This could be because:")
print("1. Local state (self.state[symbol].position) is stale after restart")
print("2. State reconciliation from exchange isn't happening correctly")
