from __future__ import annotations

from app.strategy.iofs_components.models import StructureResult, TrendResult, TriggerResult

QUALITY_THRESHOLDS = {
    "conservative": 80,
    "balanced": 72,
    "aggressive": 65,
}


def score_setup(
    trend: TrendResult,
    structure: StructureResult,
    trigger: TriggerResult,
) -> int:
    """Return a fail-closed IOFS setup quality score from 0 to 100."""
    if not trend.is_aligned or not structure.retest_active or not trigger.is_confirmed:
        return 0
    if structure.candles_since_break is None or structure.retest_distance_atr is None:
        return 0

    score = 0
    if trend.adx >= 30:
        score += 20
    elif trend.adx >= 25:
        score += 15
    elif trend.adx >= 22:
        score += 10

    if trend.ema_sep_pct >= 0.030:
        score += 10
    elif trend.ema_sep_pct >= 0.015:
        score += 7
    else:
        score += 3

    if structure.candles_since_break <= 8:
        score += 25
    elif structure.candles_since_break <= 20:
        score += 18
    else:
        score += 10

    distance = structure.retest_distance_atr
    if distance <= 0.10:
        score += 20
    elif distance <= 0.20:
        score += 15
    elif distance <= 0.30:
        score += 10

    if trigger.pattern == "ENGULFING" and trigger.wick_ratio >= 1.5:
        score += 25
    elif trigger.pattern == "ENGULFING":
        score += 18
    elif trigger.pattern == "PIN_BAR" and trigger.wick_ratio >= 2.5:
        score += 20
    elif trigger.pattern == "PIN_BAR":
        score += 15

    return min(score, 100)


def passes_quality_gate(score: int, risk_profile: str = "balanced") -> bool:
    profile = str(risk_profile).lower()
    threshold = QUALITY_THRESHOLDS.get(profile, QUALITY_THRESHOLDS["balanced"])
    return isinstance(score, int) and not isinstance(score, bool) and score >= threshold
