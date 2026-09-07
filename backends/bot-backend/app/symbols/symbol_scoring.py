from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Literal


RecommendedAction = Literal["TRADE", "WATCH", "EXCLUDE", "MANUAL_REVIEW"]

MANUAL_REVIEW_PATTERNS = (
    "TRUMP",
    "MELANIA",
    "BODEN",
    "WIF",
    "1000",
    "1MBABYDOGE",
    "BROCCOLI",
    "MOG",
    "PEPE",
    "BONK",
    "FLOKI",
    "DOGS",
    "PENGU",
    "TOSHI",
    "MEME",
)

TIER_1_BASE_ASSETS = {"BTC", "ETH", "BNB", "SOL"}
TIER_2_BASE_ASSETS = {
    "XRP",
    "ADA",
    "AVAX",
    "LINK",
    "DOGE",
    "DOT",
    "LTC",
    "TRX",
    "AAVE",
    "SUI",
    "NEAR",
    "ATOM",
    "INJ",
    "UNI",
    "ETC",
    "FIL",
    "OP",
    "ARB",
}


@dataclass(frozen=True)
class SymbolScoreInput:
    symbol: str
    rank: int | None = None
    quote_volume_24h: float | None = None
    spread_bps: float | None = None
    exclusion_reasons: list[str] | None = None
    shadow_rows: int = 0
    evaluated_count: int = 0
    would_pass_count: int = 0
    confidence_sample_count: int = 0
    average_confidence: float | None = None
    max_confidence: float | None = None
    average_pass_confidence: float | None = None
    recent_total_pnl: float | None = None
    recent_win_rate_pct: float | None = None
    recent_profit_factor: float | str | None = None
    average_r_multiple: float | None = None
    volatility_quality: float | None = None
    candle_sufficiency: int | None = None
    funding_stability: float | None = None
    open_interest: float | None = None
    denylisted: bool = False
    allow_manual_review: bool = False


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _is_ascii_symbol(symbol: str) -> bool:
    try:
        symbol.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def needs_manual_review(symbol: str) -> bool:
    clean = symbol.upper()
    base = clean[:-4] if clean.endswith("USDT") else clean
    if not _is_ascii_symbol(clean):
        return True
    if not re.fullmatch(r"[A-Z0-9]+USDT", clean):
        return True
    if len(base) < 3:
        return True
    if re.search(r"\d", base) and base not in {"1INCH"}:
        return True
    return any(pattern in clean for pattern in MANUAL_REVIEW_PATTERNS)


def market_tier(symbol: str) -> int:
    base = symbol.upper()[:-4] if symbol.upper().endswith("USDT") else symbol.upper()
    if base in TIER_1_BASE_ASSETS:
        return 1
    if base in TIER_2_BASE_ASSETS:
        return 2
    return 3


def _market_tier_score(symbol: str) -> float:
    tier = market_tier(symbol)
    if tier == 1:
        return 18.0
    if tier == 2:
        return 10.0
    return 0.0


def _liquidity_score(quote_volume_24h: float | None) -> float:
    if not quote_volume_24h or quote_volume_24h <= 0:
        return 0.0
    # 50M is the minimum quality line; 5B+ saturates.
    return _clamp((math.log10(quote_volume_24h) - math.log10(50_000_000)) / 2.0, 0.0, 1.0) * 30.0


def _spread_score(spread_bps: float | None) -> float:
    if spread_bps is None or spread_bps < 0:
        return 0.0
    # Excellent: <= 1 bps. Unusable: >= 10 bps.
    return _clamp((10.0 - spread_bps) / 9.0, 0.0, 1.0) * 25.0


def _profit_factor_score(value: float | str | None) -> float:
    if value == "inf":
        return 10.0
    if value is None:
        return 0.0
    return _clamp(float(value) / 2.0, 0.0, 1.0) * 10.0


def score_symbol(item: SymbolScoreInput) -> dict[str, Any]:
    symbol = item.symbol.upper()
    exclusion_reasons = list(item.exclusion_reasons or [])
    manual_review = needs_manual_review(symbol)

    if item.denylisted:
        return {
            "symbol": symbol,
            "score": 0.0,
            "recommended_action": "EXCLUDE",
            "inclusion_reason": None,
            "exclusion_reason": "denylisted",
            "manual_review": manual_review,
            "components": {},
        }

    if exclusion_reasons:
        return {
            "symbol": symbol,
            "score": 0.0,
            "recommended_action": "EXCLUDE",
            "inclusion_reason": None,
            "exclusion_reason": ",".join(exclusion_reasons),
            "manual_review": manual_review,
            "components": {},
        }

    liquidity = _liquidity_score(item.quote_volume_24h)
    spread = _spread_score(item.spread_bps)
    tier = _market_tier_score(symbol)
    signal_frequency = _clamp(item.would_pass_count / 8.0, 0.0, 1.0) * 8.0
    confidence = _clamp((item.average_pass_confidence or item.average_confidence or 0.0) / 0.75, 0.0, 1.0) * 8.0
    max_confidence = _clamp((item.max_confidence or 0.0) / 0.9, 0.0, 1.0) * 3.0
    volatility = _clamp((item.volatility_quality or 0.5), 0.0, 1.0) * 8.0
    candle = 4.0 if item.candle_sufficiency else (2.0 if item.candle_sufficiency is None else 0.0)
    funding = _clamp((item.funding_stability if item.funding_stability is not None else 0.5), 0.0, 1.0) * 3.0
    open_interest = _clamp(math.log10(max(item.open_interest or 1.0, 1.0)) / 10.0, 0.0, 1.0) * 2.0
    performance = (
        _clamp((item.recent_total_pnl or 0.0) / 150.0, -1.0, 1.0) * 3.0
        + _clamp(((item.recent_win_rate_pct or 0.0) - 35.0) / 40.0, -1.0, 1.0) * 2.0
        + _profit_factor_score(item.recent_profit_factor) * 0.6
        + _clamp((item.average_r_multiple or 0.0), -1.0, 2.0) * 1.5
    )
    rank_penalty = min((item.rank or 999) * 0.02, 4.0)
    manual_penalty = 35.0 if manual_review and not item.allow_manual_review else 0.0
    score = max(
        0.0,
        liquidity
        + spread
        + tier
        + signal_frequency
        + confidence
        + max_confidence
        + volatility
        + candle
        + funding
        + open_interest
        + performance
        - rank_penalty
        - manual_penalty,
    )
    tier_id = market_tier(symbol)
    minimum_pass_count = 1 if tier_id in {1, 2} else 2
    meets_trade_trust = (
        item.would_pass_count >= minimum_pass_count
        and item.evaluated_count >= 8
        and item.confidence_sample_count >= 5
        and (item.average_confidence is None or item.average_confidence >= 0.18)
    )
    meets_market_quality = (
        (item.quote_volume_24h or 0.0) >= 50_000_000
        and item.spread_bps is not None
        and item.spread_bps <= 6.0
    )

    if manual_review and not item.allow_manual_review:
        action: RecommendedAction = "MANUAL_REVIEW"
        inclusion = None
        exclusion = "manual_review_required"
    elif score >= 42.0 and meets_trade_trust and meets_market_quality:
        action = "TRADE"
        inclusion = "trusted_liquid_tiered_symbol_with_repeat_shadow_signal"
        exclusion = None
    elif score >= 35.0:
        action = "WATCH"
        if not meets_trade_trust:
            inclusion = "passes_quality_filters_but_needs_more_signal_history"
        elif not meets_market_quality:
            inclusion = "signal_present_but_market_quality_below_trade_gate"
        else:
            inclusion = "passes_quality_filters_but_below_trade_score"
        exclusion = None
    else:
        action = "WATCH"
        inclusion = "eligible_but_low_current_score"
        exclusion = None

    return {
        "symbol": symbol,
        "score": round(score, 4),
        "recommended_action": action,
        "inclusion_reason": inclusion,
        "exclusion_reason": exclusion,
        "manual_review": manual_review,
        "components": {
            "liquidity": round(liquidity, 4),
            "spread": round(spread, 4),
            "market_tier": round(tier, 4),
            "signal_frequency": round(signal_frequency, 4),
            "confidence": round(confidence, 4),
            "max_confidence": round(max_confidence, 4),
            "volatility": round(volatility, 4),
            "candle_sufficiency": round(candle, 4),
            "funding": round(funding, 4),
            "open_interest": round(open_interest, 4),
            "performance": round(performance, 4),
            "rank_penalty": round(rank_penalty, 4),
            "manual_penalty": round(manual_penalty, 4),
            "meets_trade_trust": meets_trade_trust,
            "meets_market_quality": meets_market_quality,
            "market_tier_id": tier_id,
        },
    }
