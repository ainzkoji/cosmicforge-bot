"""
Check which of the bot's configured symbols exist on demo-fapi.binance.com
Run from: cosmicforge-bot/backends/bot-backend/
"""
import sys, os, requests

# Target: demo endpoint
DEMO_BASE = "https://demo-fapi.binance.com"

CONFIGURED_SYMBOLS = [
    "BTCUSDT","ETHUSDT","XRPUSDT","ADAUSDT","SOLUSDT","BNBUSDT","DOGEUSDT",
    "AVAXUSDT","MATICUSDT","LINKUSDT","ATOMUSDT","OPUSDT","ARBUSDT","LTCUSDT",
    "TRXUSDT","NEARUSDT","FILUSDT","APEUSDT","UNIUSDT","DOTUSDT","ETCUSDT"
]

print(f"Fetching exchange info from {DEMO_BASE}/fapi/v1/exchangeInfo ...\n")
try:
    r = requests.get(f"{DEMO_BASE}/fapi/v1/exchangeInfo", timeout=15)
    r.raise_for_status()
    data = r.json()
    available = {s["symbol"] for s in data.get("symbols", []) if s.get("status") == "TRADING"}
    print(f"Total symbols available on demo-fapi: {len(available)}\n")
except Exception as e:
    print(f"❌ Failed to reach demo endpoint: {e}")
    sys.exit(1)

print("Symbol availability check:")
missing = []
for sym in CONFIGURED_SYMBOLS:
    ok = sym in available
    status = "✅" if ok else "❌ MISSING"
    print(f"  {status}  {sym}")
    if not ok:
        missing.append(sym)

print(f"\n{'='*40}")
if missing:
    print(f"❌ {len(missing)} symbols NOT available on demo-fapi:")
    for s in missing:
        print(f"   - {s}")
    print("\nThese symbols will cause -1121 errors on the demo endpoint.")
    print("Fix options:")
    print("  1. Remove missing symbols from the bot's symbol list for this demo user")
    print("  2. Filter them out at runtime before placing orders on demo accounts")
else:
    print("✅ All configured symbols are available on demo-fapi!")
