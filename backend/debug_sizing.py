"""Diagnostic: Check what trade_usdt value is actually being calculated."""
from app.core.config import settings
from app.symbols.sizing import usdt_for, parse_usdt_map
from app.risk.sizing import PositionSizer, calculate_atr
from app.exchange.binance.client import BinanceFuturesClient

# Setup client
client = BinanceFuturesClient(
    api_key=settings.BINANCE_API_KEY,
    api_secret=settings.BINANCE_API_SECRET,
    base_url=settings.BINANCE_FAPI_BASE_URL,
    recv_window=settings.BINANCE_RECV_WINDOW,
)

# Get balance
balance = client.account_balance()
usdt_balance = 0.0
for b in balance:
    if b.get("asset") == "USDT":
        usdt_balance = float(b.get("availableBalance", 0))
        break

print(f"=== ACCOUNT BALANCE ===")
print(f"Available USDT: {usdt_balance}")

# Setup sizer (matching runner.py fix)
sizer = PositionSizer(
    account_risk_pct=getattr(settings, "ACCOUNT_RISK_PCT", 1.0),
    default_usdt=settings.TRADE_USDT_PER_ORDER,
    max_notional=settings.TRADE_USDT_PER_ORDER,  # ✅ FIX: Cap trades at configured size
)

# Check a sample symbol
symbol = "ETHUSDT"
klines = client.klines(symbol, "15m", 50)
price = client.last_price(symbol)
atr = calculate_atr(klines, period=14)

print(f"\n=== SIZING FOR {symbol} ===")
print(f"Current Price: {price}")
print(f"ATR (14): {atr}")
print(f"settings.TRADE_USDT_PER_ORDER: {settings.TRADE_USDT_PER_ORDER}")
print(f"settings.ACCOUNT_RISK_PCT: {getattr(settings, 'ACCOUNT_RISK_PCT', 'NOT SET')}")

# Calculate dynamic size
result = sizer.calculate_atr_size(
    account_balance=usdt_balance,
    entry_price=price,
    atr=atr,
    confidence=1.0,
)

print(f"\n=== SIZER RESULT ===")
print(f"qty: {result.qty}")
print(f"size_usdt: {result.size_usdt}")
print(f"risk_usdt: {result.risk_usdt}")
print(f"stop_distance: {result.stop_distance}")
print(f"reason: {result.reason}")

# What would be the final trade_usdt?
usdt_map = parse_usdt_map(getattr(settings, "SYMBOL_USDT_MAP", ""))
base_usdt = usdt_for(symbol, usdt_map, settings.TRADE_USDT_PER_ORDER)

print(f"\n=== FINAL TRADE USDT ===")
print(f"Base (from usdt_for): {base_usdt}")
if result.qty > 0:
    print(f"Sizer override: {result.size_usdt}")
    print(f"FINAL VALUE: {result.size_usdt}")
else:
    print(f"Sizer returned 0 -> trade_usdt = 0.0")
    print(f"FINAL VALUE: 0.0 (TRADE BLOCKED!)")

# Check notional requirement
print(f"\n=== MARGIN CHECK ===")
leverage = settings.DEFAULT_LEVERAGE
required_margin = result.size_usdt / leverage if result.size_usdt > 0 else base_usdt / leverage
print(f"Leverage: {leverage}x")
print(f"Trade Size: {result.size_usdt if result.qty > 0 else base_usdt} USDT")
print(f"Required Margin: {required_margin} USDT")
print(f"Available Balance: {usdt_balance} USDT")
if required_margin > usdt_balance:
    print("❌ MARGIN INSUFFICIENT!")
else:
    print("✅ Margin should be sufficient")
