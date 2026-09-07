from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class Candle:
    open_time: int | None
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_raw(cls, raw: Any) -> "Candle":
        if isinstance(raw, cls):
            candle = raw
        elif isinstance(raw, Mapping):
            candle = cls(
                open_time=_optional_int(
                    raw.get("open_time", raw.get("openTime", raw.get("time")))
                ),
                open=float(raw["open"]),
                high=float(raw["high"]),
                low=float(raw["low"]),
                close=float(raw["close"]),
                volume=float(raw.get("volume", 0.0)),
            )
        elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
            if len(raw) < 6:
                raise ValueError("candle_sequence_too_short")
            candle = cls(
                open_time=_optional_int(raw[0]),
                open=float(raw[1]),
                high=float(raw[2]),
                low=float(raw[3]),
                close=float(raw[4]),
                volume=float(raw[5]),
            )
        else:
            raise ValueError("unsupported_candle_format")

        candle.validate()
        return candle

    def validate(self) -> None:
        values = (self.open, self.high, self.low, self.close, self.volume)
        if not all(math.isfinite(value) for value in values):
            raise ValueError("non_finite_candle")
        if self.volume < 0:
            raise ValueError("negative_volume")
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high_below_ohlc")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("low_above_ohlc")


@dataclass(frozen=True)
class TrendResult:
    is_aligned: bool
    direction: str
    adx: float
    ema_sep_pct: float
    reason: str = "OK"


@dataclass(frozen=True)
class StructureResult:
    retest_active: bool
    level: float | None
    candles_since_break: int | None
    rejection_strength: float
    retest_distance_atr: float | None
    reason: str = "OK"


@dataclass(frozen=True)
class TriggerResult:
    is_confirmed: bool
    pattern: str
    wick_ratio: float
    candle_low: float | None
    candle_high: float | None
    reason: str = "OK"


@dataclass(frozen=True)
class IOFSGateResult:
    passed: bool
    direction: str
    score: int
    reason: str
    trend: TrendResult | None
    structure: StructureResult | None
    trigger: TriggerResult | None
    risk_profile: str
    threshold: int


def validate_candles(candles: Sequence[Candle]) -> None:
    for candle in candles:
        if not isinstance(candle, Candle):
            raise ValueError("invalid_candle_type")
        candle.validate()


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)
