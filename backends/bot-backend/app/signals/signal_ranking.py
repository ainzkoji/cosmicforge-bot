from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


RANKING_WEIGHTS = {
    "confidence_score": 0.30,
    "risk_reward_quality": 0.20,
    "liquidity_score": 0.15,
    "spread_score": 0.10,
    "volatility_suitability": 0.10,
    "expected_duration_score": 0.10,
    "symbol_performance_score": 0.05,
}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _risk_reward_score(value: Any) -> float:
    rr = float(value or 0.0)
    if rr >= 3.0:
        return 100.0
    if rr >= 2.5:
        return 90.0
    if rr >= 2.0:
        return 75.0
    if rr >= 1.8:
        return 65.0
    return 0.0


def _duration_score(candidate: dict[str, Any]) -> float:
    minutes = candidate.get("estimated_tp2_minutes")
    if minutes is None:
        return 70.0
    minutes = float(minutes)
    if minutes <= 240:
        return 100.0
    if minutes <= 720:
        return 80.0
    if minutes <= 1440:
        return 60.0
    return 0.0


def _validity_score(candidate: dict[str, Any]) -> float:
    expires_at = candidate.get("expires_at")
    if not expires_at:
        return 70.0
    try:
        expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        minutes_left = (expiry.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds() / 60
    except Exception:
        return 70.0
    if minutes_left >= 60:
        return 100.0
    if minutes_left >= 30:
        return 80.0
    if minutes_left >= 15:
        return 60.0
    return 30.0


def calculate_signal_rank(candidate: dict[str, Any], metrics: dict[str, Any] | None = None) -> dict[str, Any]:
    metrics = metrics or {}
    confidence = _clamp(float(candidate.get("confidence_score") or 0.0))
    risk_reward = _risk_reward_score(candidate.get("risk_reward"))
    liquidity = _clamp(metrics.get("liquidity_score") if metrics.get("liquidity_score") is not None else 50.0)
    spread = _clamp(metrics.get("spread_score") if metrics.get("spread_score") is not None else 50.0)
    volatility = _clamp(metrics.get("volatility_score") if metrics.get("volatility_score") is not None else 70.0)
    duration = _duration_score(candidate)
    symbol_performance = 50.0
    components = {
        "confidence_score": confidence,
        "risk_reward_quality": risk_reward,
        "liquidity_score": liquidity,
        "spread_score": spread,
        "volatility_suitability": volatility,
        "expected_duration_score": duration,
        "symbol_performance_score": symbol_performance,
        "entry_validity_score": _validity_score(candidate),
    }
    weighted = sum(components[key] * weight for key, weight in RANKING_WEIGHTS.items())
    # Entry validity is a small tie-breaker rather than part of the core 100% weight model.
    total = round(weighted + (components["entry_validity_score"] * 0.01), 4)
    return {
        "symbol": candidate.get("symbol"),
        "side": candidate.get("side"),
        "signal_id": candidate.get("signal_id"),
        "candidate_id": candidate.get("candidate_id"),
        "total_rank_score": total,
        "component_scores": components,
        "ranking_reason": (
            f"Rank score {total:.2f}: confidence {confidence:.1f}, R/R {risk_reward:.1f}, "
            f"liquidity {liquidity:.1f}, spread {spread:.1f}, volatility {volatility:.1f}, duration {duration:.1f}. "
            "Recent symbol performance is neutral because symbol performance stats are not available yet."
        ),
    }


def rank_signal_candidates(
    candidates: list[dict[str, Any]],
    pair_metrics: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    pair_metrics = pair_metrics or {}
    ranked = []
    for candidate in candidates:
        rank = calculate_signal_rank(candidate, pair_metrics.get(str(candidate.get("symbol") or "").upper()))
        ranked.append({**candidate, **rank})
    ranked.sort(key=lambda item: (-float(item["total_rank_score"]), str(item.get("symbol") or ""), str(item.get("side") or "")))
    for index, item in enumerate(ranked, start=1):
        item["rank_position"] = index
    return ranked


def select_top_candidates(candidates: list[dict[str, Any]], max_published_per_scan: int = 5) -> list[dict[str, Any]]:
    return list(candidates[: max(0, int(max_published_per_scan))])
