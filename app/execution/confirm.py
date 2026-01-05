from __future__ import annotations

import time


def wait_until_flat(client, symbol: str, timeout_s: int = 12, poll_s: float = 0.5) -> bool:
    """
    Confirm the position is fully closed: positionAmt == 0.
    Robust against Binance returning NEW immediately for MARKET orders.
    """
    symbol = symbol.upper()
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        positions = client.position_risk(symbol=symbol)
        # your client returns list of dicts
        for p in positions:
            if p.get("symbol") == symbol:
                amt = float(p.get("positionAmt", "0") or "0")
                if abs(amt) < 1e-12:
                    return True
        time.sleep(poll_s)

    return False
