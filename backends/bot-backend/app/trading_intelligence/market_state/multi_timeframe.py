"""Higher timeframe state (Section 9.10).

Reuses the same trend/volatility calculators against the HTF causal series --
there is exactly one trend-state family, computed the same way regardless of
which timeframe it is applied to. The HTF candle must already have been
proven causally aligned (``MarketSnapshot.htf_is_timestamp_aligned()``) by
the integration adapter before it ever reaches this module; this module
additionally refuses to use it if the HTF series' own latest close is after
``decision_time``, so a caller mistake can never leak a future HTF bar in.
"""
from __future__ import annotations

from typing import Optional

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import CandleSeries, HigherTimeframeState
from app.trading_intelligence.market_state.trend import compute_trend_state
from app.trading_intelligence.market_state.volatility import compute_volatility_state


def compute_higher_timeframe_state(
    htf_series: Optional[CandleSeries],
    *,
    decision_time: int,
    htf_causally_aligned: bool,
    local_direction: str,
) -> HigherTimeframeState:
    if htf_series is None or len(htf_series) == 0:
        return HigherTimeframeState(available=False, reason_codes=(ReasonCode.HTF_UNAVAILABLE.value,))

    if not htf_causally_aligned:
        return HigherTimeframeState(available=False, reason_codes=(ReasonCode.HTF_MISALIGNED.value,))

    latest_htf_close = htf_series.latest_close_time
    if latest_htf_close is not None and latest_htf_close > decision_time:
        return HigherTimeframeState(available=False, reason_codes=(ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value,))

    htf_trend = compute_trend_state(htf_series)
    htf_vol = compute_volatility_state(htf_series)

    if not htf_trend.available:
        return HigherTimeframeState(
            available=False,
            reason_codes=tuple(dict.fromkeys(htf_trend.reason_codes or (ReasonCode.INSUFFICIENT_HISTORY.value,))),
        )

    direction = htf_trend.direction
    if direction in ("UP", "DOWN") and local_direction in ("UP", "DOWN"):
        alignment = "ALIGNED" if direction == local_direction else "CONFLICT"
    elif direction == "FLAT" or local_direction == "FLAT":
        alignment = "NEUTRAL"
    else:
        alignment = "NEUTRAL"

    vol_label = None
    if htf_vol.available:
        if htf_vol.shock_state:
            vol_label = "SHOCK"
        elif htf_vol.compression is not None and htf_vol.compression <= 0.2:
            vol_label = "COMPRESSED"
        elif htf_vol.expansion is not None and htf_vol.expansion >= 1.5:
            vol_label = "EXPANDING"
        else:
            vol_label = "NORMAL"

    return HigherTimeframeState(
        available=True,
        direction=direction,
        structure_alignment=alignment,
        volatility_state=vol_label,
        reason_codes=(),
    )
