"""Force close BTC position to reset account state"""
import time
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from shared_lib.persistence.db import DB
from app.core.broker_service import get_decrypted_credentials
from app.exchange.binance.client import BinanceFuturesClient

db = DB()

print("=" * 60)
print("FORCE CLOSE BTC POSITION")
print("=" * 60)

# Get bot credentials
with db.connect() as conn:
    row = conn.execute('''
        SELECT broker_account_id, mode 
        FROM bot_instances 
        WHERE status IN ('active', 'running') 
        LIMIT 1
    ''').fetchone()

if not row:
    print("❌ No active bot found.")
    exit(1)

creds = get_decrypted_credentials(row['broker_account_id'])
base_url = "https://testnet.binancefuture.com" if row['mode'] == "paper" else "https://fapi.binance.com"
client = BinanceFuturesClient(creds['api_key'], creds['api_secret'], base_url)

# 1. Get Positions
try:
    positions = client.position_risk()
    open_pos = [p for p in positions if float(p['positionAmt']) != 0 and p['symbol'].upper() == 'BTCUSDT']
except Exception as e:
    print(f"❌ Error getting positions: {e}")
    exit(1)

if not open_pos:
    print("✅ No open BTC position found. Account is clean.")
    exit(0)

print(f"⚠️ FOUND {len(open_pos)} OPEN BTC POSITIONS. CLOSING NOW...")

# 2. Close Each
for p in open_pos:
    symbol = p['symbol']
    amt = float(p['positionAmt'])
    print(f"   Closing {symbol} (Size: {amt})...")
    
    try:
        # Use helper method if available, else manual market order
        res = client.close_position_market(symbol)
        print(f"   ✅ Closed {symbol}: {res}")
    except Exception as e:
        print(f"   ❌ Failed to close {symbol}: {e}")

print("\nAll close commands sent. Verifying...")
time.sleep(2)

# 3. Verify
positions = client.position_risk()
remaining = [p for p in positions if float(p['positionAmt']) != 0 and p['symbol'].upper() == 'BTCUSDT']

if remaining:
    print(f"❌ WARNING: BTC positions still open:")
    for p in remaining:
        print(f"   {p['symbol']}: {p['positionAmt']}")
else:
    print("✅ SUCCESS: BTC position closed.")
