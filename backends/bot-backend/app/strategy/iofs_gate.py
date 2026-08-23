from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any

from app.strategy.iofs_components.indicators import calculate_atr
from app.strategy.iofs_components.models import (
    Candle,
    IOFSGateResult,
    StructureResult,
    TrendResult,
    TriggerResult,
    validate_candles,
)
from app.strategy.iofs_components.scorer import QUALITY_THRESHOLDS, score_setup
from app.strategy.iofs_components.structure import find_structure_retest
from app.strategy.iofs_components.trend import check_4h_trend
from app.strategy.iofs_components.trigger import check_trigger_candle

ADX_THRESHOLDS = {
    "conservative": 25.0,
    "balanced": 22.0,
    "aggressive": 20.0,
}


class IOFSGateEvaluator:
    """Evaluate IOFS conditions in strict fail-closed order."""

    def evaluate(
        self,
        candles_by_tf: dict[str, list[Candle]],
        risk_profile: str = "balanced",
    ) -> IOFSGateResult:
        profile = normalize_risk_profile(risk_profile)
        threshold = QUALITY_THRESHOLDS[profile]
        base = {
            "direction": "NONE",
            "score": 0,
            "trend": None,
            "structure": None,
            "trigger": None,
            "risk_profile": profile,
            "threshold": threshold,
        }

        if not isinstance(candles_by_tf, dict) or any(
            timeframe not in candles_by_tf for timeframe in ("4h", "1h", "15m")
        ):
            return IOFSGateResult(False, reason="MISSING_TIMEFRAME", **base)

        candles_4h = candles_by_tf["4h"]
        if not _valid_candles(candles_4h):
            return IOFSGateResult(False, reason="INVALID_CANDLES", **base)

        trend = check_4h_trend(candles_4h, ADX_THRESHOLDS[profile])
        if not trend.is_aligned:
            reason = "INVALID_CANDLES" if trend.reason == "BAD_CANDLES" else "TREND_NOT_ALIGNED"
            return IOFSGateResult(
                False,
                direction="NONE",
                score=0,
                reason=reason,
                trend=trend,
                structure=None,
                trigger=None,
                risk_profile=profile,
                threshold=threshold,
            )

        candles_1h = candles_by_tf["1h"]
        if not _valid_candles(candles_1h):
            return IOFSGateResult(
                False, trend.direction, 0, "INVALID_CANDLES", trend, None, None, profile, threshold
            )
        atr_1h = calculate_atr(candles_1h)
        if atr_1h is None:
            return IOFSGateResult(
                False, trend.direction, 0, "ATR_UNAVAILABLE", trend, None, None, profile, threshold
            )

        structure = find_structure_retest(candles_1h, trend.direction, atr_1h)
        if not structure.retest_active:
            return IOFSGateResult(
                False,
                trend.direction,
                0,
                "STRUCTURE_NOT_ACTIVE",
                trend,
                structure,
                None,
                profile,
                threshold,
            )

        candles_15m = candles_by_tf["15m"]
        if not _valid_candles(candles_15m):
            return IOFSGateResult(
                False,
                trend.direction,
                0,
                "INVALID_CANDLES",
                trend,
                structure,
                None,
                profile,
                threshold,
            )
        atr_15m = calculate_atr(candles_15m)
        if atr_15m is None:
            return IOFSGateResult(
                False,
                trend.direction,
                0,
                "ATR_UNAVAILABLE",
                trend,
                structure,
                None,
                profile,
                threshold,
            )

        trigger = check_trigger_candle(
            candles_15m, structure.level, trend.direction, atr_15m
        )
        if not trigger.is_confirmed:
            return IOFSGateResult(
                False,
                trend.direction,
                0,
                "TRIGGER_NOT_CONFIRMED",
                trend,
                structure,
                trigger,
                profile,
                threshold,
            )

        score = score_setup(trend, structure, trigger)
        if score < threshold:
            return IOFSGateResult(
                False,
                trend.direction,
                score,
                "QUALITY_SCORE_TOO_LOW",
                trend,
                structure,
                trigger,
                profile,
                threshold,
            )

        return IOFSGateResult(
            True,
            trend.direction,
            score,
            "OK",
            trend,
            structure,
            trigger,
            profile,
            threshold,
        )


def normalize_risk_profile(risk_profile: str) -> str:
    profile = str(risk_profile or "").strip().lower()
    return profile if profile in QUALITY_THRESHOLDS else "balanced"


def is_symbol_allowed(symbol: str, allowed_symbols: str) -> bool:
    allowed = {
        item.strip().upper()
        for item in str(allowed_symbols or "").split(",")
        if item.strip()
    }
    return str(symbol or "").strip().upper() in allowed


def is_session_allowed(
    windows_utc: str,
    now_utc: datetime | None = None,
) -> bool:
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    current = now.astimezone(timezone.utc).time().replace(tzinfo=None)

    for segment in str(windows_utc or "").split(","):
        try:
            start_text, end_text = (part.strip() for part in segment.split("-", 1))
            start = time.fromisoformat(start_text)
            end = time.fromisoformat(end_text)
        except (TypeError, ValueError):
            continue
        if start <= current < end:
            return True
    return False


def gate_result_details(
    symbol: str,
    mode: str,
    result: IOFSGateResult,
    *,
    blocked_trade: bool,
    timestamp_utc: datetime | None = None,
) -> dict[str, Any]:
    timestamp = timestamp_utc or datetime.now(timezone.utc)
    trend = result.trend
    structure = result.structure
    trigger = result.trigger
    return {
        "symbol": str(symbol).upper(),
        "timestamp_utc": timestamp.astimezone(timezone.utc).isoformat(),
        "mode": mode,
        "passed": result.passed,
        "direction": result.direction,
        "score": result.score,
        "threshold": result.threshold,
        "reason": result.reason,
        "trend_direction": trend.direction if trend else None,
        "trend_adx": trend.adx if trend else None,
        "trend_ema_sep_pct": trend.ema_sep_pct if trend else None,
        "structure_level": structure.level if structure else None,
        "structure_retest_active": structure.retest_active if structure else None,
        "structure_retest_distance_atr": structure.retest_distance_atr if structure else None,
        "structure_candles_since_break": structure.candles_since_break if structure else None,
        "trigger_confirmed": trigger.is_confirmed if trigger else None,
        "trigger_pattern": trigger.pattern if trigger else None,
        "trigger_wick_ratio": trigger.wick_ratio if trigger else None,
        "risk_profile": result.risk_profile,
        "blocked_trade": blocked_trade,
    }


def make_gate_failure(reason: str, risk_profile: str = "balanced") -> IOFSGateResult:
    profile = normalize_risk_profile(risk_profile)
    return IOFSGateResult(
        False,
        "NONE",
        0,
        reason,
        None,
        None,
        None,
        profile,
        QUALITY_THRESHOLDS[profile],
    )


def _valid_candles(candles: Any) -> bool:
    if not isinstance(candles, list) or not candles:
        return False
    try:
        validate_candles(candles)
    except (TypeError, ValueError):
        return False
    return True
