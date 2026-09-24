"""Participation state (Section 9.7).

V1: volume percentile, relative volume, volume acceleration from OHLCV
volume. Taker/aggressor imbalance and trade intensity are computed only when
the source candle format actually carries taker-buy-volume / trade-count
series (e.g. Binance klines) -- otherwise they are ``None`` with an explicit
reason, never zero-filled.
"""
from __future__ import annotations

from typing import List

import numpy as np

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import CandleSeries, ParticipationState, safe_float
from app.trading_intelligence.market_state.indicators import percentile_rank_of_last

MIN_HISTORY = 20
RELATIVE_VOLUME_WINDOW = 20
PERCENTILE_WINDOW = 100
ACCELERATION_SHORT_WINDOW = 5


def compute_participation_state(series: CandleSeries) -> ParticipationState:
    n = len(series)
    if n < MIN_HISTORY:
        return ParticipationState(
            volume_percentile=None,
            relative_volume=None,
            volume_acceleration=None,
            taker_imbalance=None,
            trade_intensity=None,
            available=False,
            reason_codes=(ReasonCode.INSUFFICIENT_HISTORY.value,),
        )

    volume = np.array([float(v) for v in series.volume], dtype=float)
    if not np.all(np.isfinite(volume)):
        return ParticipationState(
            volume_percentile=None,
            relative_volume=None,
            volume_acceleration=None,
            taker_imbalance=None,
            trade_intensity=None,
            available=False,
            reason_codes=(ReasonCode.NONFINITE_FEATURE.value,),
        )

    reason_codes: List[str] = []

    volume_percentile = percentile_rank_of_last(volume, window=PERCENTILE_WINDOW)
    if volume_percentile is None:
        reason_codes.append(ReasonCode.INSUFFICIENT_HISTORY.value)

    rel_window = min(RELATIVE_VOLUME_WINDOW, n - 1)
    trailing_avg = float(np.mean(volume[-rel_window - 1 : -1])) if rel_window > 0 else None
    relative_volume = safe_float(volume[-1] / trailing_avg) if trailing_avg else None

    short_window = min(ACCELERATION_SHORT_WINDOW, n)
    short_avg = float(np.mean(volume[-short_window:]))
    volume_acceleration = safe_float(short_avg / trailing_avg) if trailing_avg else None

    taker_imbalance = None
    trade_intensity = None
    if series.taker_buy_volume is not None and len(series.taker_buy_volume) == n:
        taker_buy = np.array([float(v) for v in series.taker_buy_volume], dtype=float)
        if np.all(np.isfinite(taker_buy)) and volume[-1] > 0:
            taker_sell = volume[-1] - taker_buy[-1]
            taker_imbalance = safe_float((taker_buy[-1] - taker_sell) / volume[-1])
    else:
        reason_codes.append(ReasonCode.AUXILIARY_DATA_MISSING.value)

    if series.trade_count is not None and len(series.trade_count) == n:
        trades = np.array([float(v) for v in series.trade_count], dtype=float)
        trailing_trades = float(np.mean(trades[-rel_window - 1 : -1])) if rel_window > 0 else None
        if trailing_trades and np.isfinite(trades[-1]):
            trade_intensity = safe_float(trades[-1] / trailing_trades)
    else:
        reason_codes.append(ReasonCode.AUXILIARY_DATA_MISSING.value)

    return ParticipationState(
        volume_percentile=volume_percentile,
        relative_volume=relative_volume,
        volume_acceleration=volume_acceleration,
        taker_imbalance=taker_imbalance,
        trade_intensity=trade_intensity,
        available=True,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )
