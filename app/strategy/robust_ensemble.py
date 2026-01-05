from __future__ import annotations

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.indicators import ema, rsi, sma


class RobustEnsembleStrategy(Strategy):
    name = "robust_ensemble"

    def __init__(self, client, interval: str = "1m"):
        self.client = client
        self.interval = interval

        # stricter = fewer trades, higher selectivity
        self.min_confidence = 0.50

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

        # Module scores
        trend_up = ema_fast > ema_slow and closes[-1] > trend_sma
        trend_down = ema_fast < ema_slow and closes[-1] < trend_sma

        # filter: avoid chasing when RSI extreme
        overbought = strength_rsi > 75
        oversold = strength_rsi < 100

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

        # confidence calculation (simple but structured)
        # max possible score is 3
        if score_buy > score_sell:
            confidence = score_buy / 3.0
            sig = Signal.BUY
        elif score_sell > score_buy:
            confidence = score_sell / 3.0
            sig = Signal.SELL
        else:
            confidence = 0.0
            sig = Signal.HOLD

        # strict gating
        if confidence < self.min_confidence:
            return SignalResult(
                Signal.HOLD,
                confidence,
                "low_confidence_no_trade",
                meta={
                    "ema_fast": ema_fast,
                    "ema_slow": ema_slow,
                    "sma50": trend_sma,
                    "rsi14": strength_rsi,
                    "score_buy": score_buy,
                    "score_sell": score_sell,
                    "min_conf": self.min_confidence,
                    "reasons": reasons,
                },
            )

        return SignalResult(
            sig,
            confidence,
            "ensemble_alignment",
            meta={
                "ema_fast": ema_fast,
                "ema_slow": ema_slow,
                "sma50": trend_sma,
                "rsi14": strength_rsi,
                "score_buy": score_buy,
                "score_sell": score_sell,
                "min_conf": self.min_confidence,
                "reasons": reasons,
            },
        )
