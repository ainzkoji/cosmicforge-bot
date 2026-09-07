from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy


def signal_from_closes(closes, fast: int = 10, slow: int = 30):
    """
    Stateless SMA crossover helper.
    Returns: "BUY", "SELL", or "HOLD"
    """
    if len(closes) < max(fast, slow) + 1:
        return "HOLD"

    fast_prev = sum(closes[-fast - 1 : -1]) / fast
    fast_now = sum(closes[-fast:]) / fast

    slow_prev = sum(closes[-slow - 1 : -1]) / slow
    slow_now = sum(closes[-slow:]) / slow

    if fast_prev <= slow_prev and fast_now > slow_now:
        return "BUY"

    if fast_prev >= slow_prev and fast_now < slow_now:
        return "SELL"

    return "HOLD"


def sma_cross_snapshot(closes, fast: int = 10, slow: int = 30):
    if len(closes) < max(fast, slow) + 1:
        return {
            "candle_count": len(closes),
            "required_candles": max(fast, slow) + 1,
        }
    return {
        "fast_sma_previous": sum(closes[-fast - 1 : -1]) / fast,
        "fast_sma_current": sum(closes[-fast:]) / fast,
        "slow_sma_previous": sum(closes[-slow - 1 : -1]) / slow,
        "slow_sma_current": sum(closes[-slow:]) / slow,
        "candle_count": len(closes),
    }


@register_strategy(
    name="sma_cross",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="Simple SMA fast/slow crossover.",
    params_schema={
        "type": "object",
        "properties": {
            "fast": {"type": "integer", "minimum": 2, "default": 10},
            "slow": {"type": "integer", "minimum": 3, "default": 30},
            "min_confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "default": 0.55,
            },
        },
    },
)
class SMACrossStrategy(Strategy):
    name = "sma_cross"
    version = "1.0.0"

    def __init__(
        self,
        client,
        interval: str = "1m",
        fast: int = 10,
        slow: int = 30,
        min_confidence: float = 0.55,
    ):
        self.client = client
        self.interval = interval
        self.fast = int(fast)
        self.slow = int(slow)
        self.min_confidence = float(min_confidence)

    def get_signal(self, symbol: str) -> SignalResult:
        try:
            kl = self.client.klines(
                symbol, interval=self.interval, limit=max(self.slow + 5, 120)
            )
            closes = [float(x[4]) for x in kl]
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"data_error:{e}")

        snapshot = sma_cross_snapshot(closes, fast=self.fast, slow=self.slow)
        if len(closes) < max(self.fast, self.slow) + 1:
            return SignalResult(Signal.HOLD, 0.0, "insufficient_data", meta=snapshot)

        sig = signal_from_closes(closes, fast=self.fast, slow=self.slow)

        if sig == "BUY":
            conf = 0.65
            if conf < self.min_confidence:
                return SignalResult(Signal.HOLD, conf, "gated", meta=snapshot)
            return SignalResult(Signal.BUY, conf, "sma_cross", meta=snapshot)

        if sig == "SELL":
            conf = 0.65
            if conf < self.min_confidence:
                return SignalResult(Signal.HOLD, conf, "gated", meta=snapshot)
            return SignalResult(Signal.SELL, conf, "sma_cross", meta=snapshot)

        return SignalResult(Signal.HOLD, 0.0, "no_cross", meta=snapshot)
