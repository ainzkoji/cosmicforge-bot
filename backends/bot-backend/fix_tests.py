"""
Final comprehensive replacement: replace ALL inline trade_fills and equity_snapshots 
INSERT calls in test_adaptive_policies.py with the helper functions.
"""
import re

path = "tests/test_adaptive_policies.py"
with open(path, encoding="utf-8") as f:
    content = f.read()

# --- Step 1: Replace all variant INSERT INTO trade_fills ... inline ---
# Pattern: any INSERT + execute call for trade_fills
# Replace with _insert_fill(conn, "BTCUSDT", <pnl>)

# The most common pattern after all previous fixes still has mismatched argument counts
# Just do a targeted block replacement for each case

# 6-arg version (already partially fixed): ((symbol, side, action, 1.0, 1.0, pnl))
def replace_inline_fill(m):
    pnl_str = m.group(1)
    return f'_insert_fill(conn, "BTCUSDT", {pnl_str})'

content = re.sub(
    r'conn\.execute\(\s*"INSERT INTO trade_fills[^"]+",\s*\(\(?"BTCUSDT",\s*"LONG",\s*"CLOSE",\s*(?:1\.0,\s*1\.0,\s*)?(-?\d+\.0)\)\)?\s*,?\s*\)',
    replace_inline_fill,
    content,
    flags=re.DOTALL,
)

# --- Step 2: Replace inline equity_snapshots INSERT ---
def replace_inline_equity_insert(m):
    equity = m.group(1)
    return f'_insert_equity(conn, {equity})'

content = re.sub(
    r'conn\.execute\("INSERT INTO equity_snapshots[^"]+",\s*\((\d+\.0)(?:,\s*\'test\')?\)\)',
    replace_inline_equity_insert,
    content,
)

# Also replace simple value-only equity inserts still in old format
content = re.sub(
    r'conn\.execute\("INSERT INTO equity_snapshots \(equity\) VALUES \((\d+\.0)\)"\)',
    lambda m: f'_insert_equity(conn, {m.group(1)})',
    content,
)
content = re.sub(
    r'conn\.execute\("INSERT INTO equity_snapshots \(equity, user_id\) VALUES \((\d+\.0), \'test\'\)"\)',
    lambda m: f'_insert_equity(conn, {m.group(1)})',
    content,
)

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

print("Done — all inline inserts replaced with helpers.")
