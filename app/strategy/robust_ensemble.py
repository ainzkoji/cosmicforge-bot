from __future__ import annotations

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy
from app.strategy.indicators import ema, rsi, sma


@register_strategy(
    name="robust_ensemble",
    version="1.0.1",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="EMA trend + SMA trend filter + RSI filter ensemble with confidence gating.",
    params_schema={
        "type": "object",
        "properties": {
            "min_confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "default": 0.50,
            },
            "interval": {"type": "string", "default": "1m"},
        },
    },
)
class RobustEnsembleStrategy(Strategy):
    name = "robust_ensemble"
    version = "1.0.1"

    def __init__(self, client, interval: str = "1m", min_confidence: float = 0.50):
        self.client = client
        self.interval = interval
        self.min_confidence = float(min_confidence)

    def get_signal(self, symbol: str) -> SignalResult:
        klines = self.client.klines(symbol=symbol, interval=self.interval, limit=200)
        closes = [float(k[4]) for k in klines]

        try:
            ema_fast = ema(closes[-80:], 9)
            ema_slow = ema(closes[-80:], 21)
            trend_sma = sma(closes, 50)
            strength_rsi = rsi(closes, 14)
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"data_error:{e}", meta=None)

        trend_up = ema_fast > ema_slow and closes[-1] > trend_sma
        trend_down = ema_fast < ema_slow and closes[-1] < trend_sma

        overbought = strength_rsi > 75
        oversold = strength_rsi < 25

        score_buy = 0
        score_sell = 0
        reasons = []

        if trend_up:
            score_buy += 2
            reasons.append("trend_up")
        if trend_down:
            score_sell += 2
            reasons.append("trend_down")

        if oversold and trend_up:
            score_buy += 1
            reasons.append("oversold_in_uptrend")
        if overbought and trend_down:
            score_sell += 1
            reasons.append("overbought_in_downtrend")

        if score_buy > score_sell:
            confidence = score_buy / 3.0
            sig = Signal.BUY
        elif score_sell > score_buy:
            confidence = score_sell / 3.0
            sig = Signal.SELL
        else:
            confidence = 0.0
            sig = Signal.HOLD

        if confidence < self.min_confidence:
            return SignalResult(Signal.HOLD, 0.0, "gated", meta={"reasons": reasons})

        return SignalResult(
            sig,
            float(confidence),
            "robust_ensemble",
            meta={"reasons": reasons, "rsi": strength_rsi},
        )
