"""Deterministic market data and a broker client that cannot reach a broker.

The candle series is generated from a fixed seed, so every stage of the
lifecycle — and every re-run — sees exactly the same market. Price is driven
explicitly by the harness (``client.set_price``) so TP1, break-even, trailing
and the final close can be triggered on demand rather than waited for.
"""
from __future__ import annotations

import math
from typing import Any

#: One minute. The lifecycle proof needs several closed candles in sequence;
#: at 15m that is hours of waiting, and inventing candles instead does not
#: work -- a candle whose close time has not passed is correctly ignored by
#: MarketSnapshot, and stale data is correctly refused by the entry path.
TF_MS = 60 * 1000


def latest_closed_boundary(now_ms: int | None = None, tf_ms: int = TF_MS) -> int:
    """Close time of the most recently completed candle."""
    import time

    now_ms = now_ms or int(time.time() * 1000)
    return (now_ms // tf_ms) * tf_ms - 1


def anchor_start_ms(count: int = 260, *, now_ms: int | None = None,
                    tf_ms: int = TF_MS) -> int:
    """Open time of the first candle so the last one closes at the latest boundary.

    The runtime has real freshness guards -- the broker health monitor rejects
    a >30s clock drift, and the entry path refuses stale market data. A series
    anchored in the past, or one that runs into the future, fails those guards,
    correctly. The market *shape* stays deterministic; only where it sits on
    the clock moves, and it is always pinned to the newest closed candle.
    """
    last_open = latest_closed_boundary(now_ms, tf_ms) + 1 - tf_ms
    return last_open - (count - 1) * tf_ms


def wait_for_next_boundary(tf_ms: int = TF_MS, *, poll: float = 0.5) -> int:
    """Block until a new candle has closed. Returns its close time."""
    import time

    start = latest_closed_boundary(tf_ms=tf_ms)
    while True:
        current = latest_closed_boundary(tf_ms=tf_ms)
        if current > start:
            return current
        time.sleep(poll)


class BrokerReached(AssertionError):
    """Raised the moment anything tries to submit a real order."""


def build_candles(
    *,
    count: int = 260,
    start_ms: int,
    base_price: float = 100.0,
    drift: float = 0.0001,
    wave: float = 0.004,
    wave_period: float = 7.0,
    ripple: float = 0.0,
    ripple_period: float = 3.0,
) -> list[list[Any]]:
    """A deterministic series the real regime gate is willing to trade.

    No RNG: a small drift plus a sine. The parameters are not arbitrary. The
    first attempt used a steep monotone drift, which the real RegimeClassifier
    scored ADX 100 / STRONG_TREND -- a blocked regime, and blocking it was
    correct. scripts/phase12/calibrate.py sweeps the real classifier over this
    generator; these defaults land on WEAK_TREND at ADX ~34, the least
    trend-extreme tradeable point in that sweep, with a mild upward drift so a
    BUY is aligned with the 4h bias rather than vetoed by it.

    The ensemble needs ~200 bars before its longest moving average is defined,
    so the default count is comfortably above that.
    """
    rows: list[list[Any]] = []
    price = base_price
    for i in range(count):
        open_time = start_ms + i * TF_MS
        close_time = open_time + TF_MS - 1
        trend = base_price * drift * i
        cycle = base_price * wave * math.sin(i / wave_period)
        if ripple:
            cycle += base_price * ripple * math.sin(i / ripple_period)
        o = price
        c = base_price + trend + cycle
        h = max(o, c) * 1.0018
        low = min(o, c) * 0.9982
        volume = 1000.0 + 25.0 * math.sin(i / 7.0)
        rows.append([
            open_time, f"{o:.4f}", f"{h:.4f}", f"{low:.4f}", f"{c:.4f}",
            f"{volume:.4f}", close_time, "0", 0, "0", "0", "0",
        ])
        price = c
    return rows


class ControlledClient:
    """Read-only exchange surface with deterministic data.

    Every method that would create, amend or cancel an order raises. That is
    the mainnet prohibition made mechanical: the harness cannot reach a broker
    even if a code path tries.
    """

    def __init__(
        self,
        *,
        symbols: tuple[str, ...] = ("BTCUSDT",),
        equity: float = 10_000.0,
        start_ms: int | None = None,
        base_price: float = 100.0,
    ) -> None:
        start_ms = anchor_start_ms() if start_ms is None else int(start_ms)
        self.start_ms = start_ms
        self.symbols = tuple(symbols)
        self.equity = float(equity)
        self._candles = {s: build_candles(start_ms=start_ms, base_price=base_price) for s in self.symbols}
        self._price = {s: float(self._candles[s][-1][4]) for s in self.symbols}
        self.blocked_calls: list[str] = []

    # ── Harness controls ────────────────────────────────────────────────────

    def set_price(self, symbol: str, price: float) -> None:
        """Move the mark price without changing the closed-candle history.

        Lifecycle management (TP1, break-even, trailing, stops) reads the live
        price, not the candle series, so this is the lever the harness uses.
        """
        self._price[symbol.upper()] = float(price)

    def set_last_close(self, symbol: str, close: float) -> None:
        """Rewrite the newest closed candle's close, and the mark price.

        This is how the harness drives TP1, break-even, trailing and the final
        exit: the candle is real (its close time has passed), only its level is
        chosen.
        """
        rows = self._candles[symbol.upper()]
        last = rows[-1]
        o = float(last[1])
        c = float(close)
        rows[-1] = [
            last[0], f"{o:.4f}", f"{max(o, c) * 1.0018:.4f}",
            f"{min(o, c) * 0.9982:.4f}", f"{c:.4f}", last[5],
            last[6], "0", 0, "0", "0", "0",
        ]
        self._price[symbol.upper()] = c

    def advance_candle(self, symbol: str, close: float | None = None) -> int:
        """Append one candle. Only valid once its close time has passed."""
        rows = self._candles[symbol.upper()]
        last = rows[-1]
        open_time = int(last[0]) + TF_MS
        close_time = open_time + TF_MS - 1
        o = float(last[4])
        c = float(close if close is not None else o)
        rows.append([
            open_time, f"{o:.4f}", f"{max(o, c) * 1.0018:.4f}",
            f"{min(o, c) * 0.9982:.4f}", f"{c:.4f}", "1000.0",
            close_time, "0", 0, "0", "0", "0",
        ])
        self._price[symbol.upper()] = c
        return close_time

    def latest_close_time(self, symbol: str) -> int:
        return int(self._candles[symbol.upper()][-1][6])

    # ── Market data ─────────────────────────────────────────────────────────

    def klines(self, symbol: str, interval: str = "15m", limit: int = 250) -> list:
        rows = self._candles.get(symbol.upper())
        if rows is None:
            return []
        return [list(r) for r in rows[-int(limit):]]

    def historical_klines(self, symbol: str, interval: str = "15m", **kw) -> list:
        return self.klines(symbol, interval, limit=kw.get("limit", 250))

    def last_price(self, symbol: str) -> float:
        return float(self._price.get(symbol.upper(), 0.0))

    def get_prices(self, symbols) -> dict:
        return {s.upper(): self.last_price(s) for s in symbols}

    def mark_price(self, symbol: str) -> dict:
        return {"symbol": symbol.upper(), "markPrice": str(self.last_price(symbol))}

    def book_ticker(self, symbol: str) -> dict:
        price = self.last_price(symbol)
        return {
            "symbol": symbol.upper(),
            "bidPrice": str(price * 0.9999), "bidQty": "100",
            "askPrice": str(price * 1.0001), "askQty": "100",
        }

    def get_ticker(self, symbol: str) -> dict:
        return {"symbol": symbol.upper(), "lastPrice": str(self.last_price(symbol))}

    def server_time(self) -> int:
        # Wall clock, like a real exchange. The broker health monitor compares
        # this against local time and refuses to trade on a drift over 30s, so
        # returning a candle timestamp would fail a real production guard.
        import time

        return int(time.time() * 1000)

    def sync_time(self) -> int:
        return 0

    def ping(self) -> dict:
        return {}

    def test_connection(self) -> dict:
        return {"ok": True, "environment": "phase12-controlled"}

    # ── Instruments ─────────────────────────────────────────────────────────

    def exchange_info(self) -> dict:
        return {"symbols": [self._symbol_info(s) for s in self.symbols]}

    def exchange_info_cached(self, ttl_seconds: int = 60) -> dict:
        return self.exchange_info()

    def _symbol_info(self, symbol: str) -> dict:
        return {
            "symbol": symbol,
            "status": "TRADING",
            "baseAsset": symbol.replace("USDT", ""),
            "quoteAsset": "USDT",
            "pricePrecision": 2,
            "quantityPrecision": 3,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ],
        }

    def list_instruments(self):
        from app.models.unified_trading import AssetClass, InstrumentSpec

        return [
            InstrumentSpec(
                symbol_canonical=symbol,
                symbol_exchange=symbol,
                asset_class=AssetClass.CRYPTO_PERP,
                base_currency=symbol.replace("USDT", ""),
                quote_currency="USDT",
                margin_currency="USDT",
                settlement_currency="USDT",
                contract_size=1,
                tick_size="0.01",
                step_size="0.001",
                min_qty="0.001",
                min_notional="5",
                price_precision=2,
                qty_precision=3,
                max_leverage=20,
            )
            for symbol in self.symbols
        ]

    def get_symbol_filters(self, symbol: str):
        from app.models.unified_trading import SymbolFilters

        return SymbolFilters(
            min_qty="0.001", step_size="0.001", min_notional="5",
            tick_size="0.01", contract_size="1",
        )

    # ── Account / positions (flat: the harness owns position state) ──────────

    def account(self) -> dict:
        return {
            "totalWalletBalance": str(self.equity),
            "availableBalance": str(self.equity),
            "totalMarginBalance": str(self.equity),
            "totalUnrealizedProfit": "0",
            "positions": [],
            "assets": [{"asset": "USDT", "walletBalance": str(self.equity),
                        "availableBalance": str(self.equity)}],
        }

    def account_balance(self) -> dict:
        return self.account()

    def get_account_snapshot(self) -> dict:
        return self.account()

    def position_risk(self, symbol: str | None = None):
        return []

    def position_risk_all(self) -> list:
        return []

    def get_positions(self):
        return []

    def get_position_amt(self, symbol: str) -> float:
        return 0.0

    def get_position_info(self, symbol: str):
        return None

    def open_orders(self, symbol: str | None = None):
        return []

    def get_algo_orders(self, symbol: str, raise_on_error: bool = False) -> list:
        return []

    def user_trades(self, *a, **k) -> list:
        return []

    def income_history(self, *a, **k) -> list:
        return []

    def get_transfers_history(self, *a, **k) -> dict:
        return {"rows": [], "total": 0}

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return {"symbol": symbol, "leverage": leverage}

    # ── Everything that would reach a broker ────────────────────────────────

    def _blocked(self, name: str):
        self.blocked_calls.append(name)
        raise BrokerReached(
            f"Phase 12 validation must never reach a broker: {name}() was called"
        )

    def place_order(self, *a, **k): self._blocked("place_order")
    def place_market_order(self, *a, **k): self._blocked("place_market_order")
    def place_protection(self, *a, **k): self._blocked("place_protection")
    def place_stop_market(self, *a, **k): self._blocked("place_stop_market")
    def place_take_profit_market(self, *a, **k): self._blocked("place_take_profit_market")
    def update_protection(self, *a, **k): self._blocked("update_protection")
    def cancel_all_orders(self, *a, **k): self._blocked("cancel_all_orders")
    def close_position_market(self, *a, **k): self._blocked("close_position_market")
