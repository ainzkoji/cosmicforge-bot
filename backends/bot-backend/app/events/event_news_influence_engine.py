from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.config import settings
from app.events.event_news_context_aggregator import EventNewsContextAggregator
from shared_lib.persistence.db import DB
from shared_lib.persistence.event_news_mode import (
    ACTION_ANNOTATE_ONLY,
    ACTION_CLOSE_POSITION,
    ACTION_CONFIDENCE_PENALTY,
    ACTION_DELAY_ENTRY,
    ACTION_HARD_BLOCK_ENTRY,
    ACTION_NONE,
    ACTION_OPEN_POSITION,
    ACTION_SIZE_REDUCTION,
    ACTION_SYMBOL_COOLDOWN,
    ACTION_CANDIDATE_SIGNAL,
    MODE_ADVISORY,
    MODE_DISABLED,
    MODE_RISK_LITE,
    MODE_SHADOW,
    get_event_news_influence_summary,
    insert_event_news_influence_decision,
)


FORBIDDEN_PROMPT_3_ACTIONS = {
    ACTION_SYMBOL_COOLDOWN,
    ACTION_HARD_BLOCK_ENTRY,
    ACTION_CANDIDATE_SIGNAL,
    ACTION_OPEN_POSITION,
    ACTION_CLOSE_POSITION,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class EventNewsInfluenceResult:
    mode: str
    requested_action: str
    applied_action: str
    reason: str
    size_multiplier: float = 1.0
    confidence_penalty: float = 0.0
    delay_seconds: int = 0
    expires_at: str | None = None
    execution_impact_allowed: bool = False
    ledger_id: int | None = None
    source_context: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "requested_action": self.requested_action,
            "applied_action": self.applied_action,
            "reason": self.reason,
            "size_multiplier": self.size_multiplier,
            "confidence_penalty": self.confidence_penalty,
            "delay_seconds": self.delay_seconds,
            "expires_at": self.expires_at,
            "execution_impact_allowed": self.execution_impact_allowed,
            "ledger_id": self.ledger_id,
            "source_context": self.source_context or {},
        }


class EventNewsInfluenceEngine:
    """Centralized Event/News soft influence engine.

    Prompt 3 intentionally caps this engine at RISK_LITE. It cannot emit open,
    close, candidate-signal, cooldown, or hard-block actions. The only allowed
    execution impacts in RISK_LITE are capped size reduction and short delay.
    """

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()
        self.aggregator = EventNewsContextAggregator(self.db)

    def evaluate(
        self,
        *,
        symbol: str,
        trace_id: str | None = None,
        side: str | None = None,
        trade_usdt: float | None = None,
        confidence: float | None = None,
    ) -> EventNewsInfluenceResult:
        context = self.aggregator.collect(symbol=symbol, trace_id=trace_id)
        state = context.get("mode") or {}
        mode = str(state.get("current_mode") or MODE_SHADOW).upper()
        requested_action, reason, raw = self._request_action(mode, context)
        applied_action = self._cap_action(mode, requested_action)

        max_reduction = float(getattr(settings, "EVENT_NEWS_RISK_LITE_MAX_SIZE_REDUCTION_PCT", 0.25) or 0.25)
        min_multiplier = max(0.0, 1.0 - max(0.0, min(max_reduction, 0.25)))
        max_penalty = min(float(getattr(settings, "EVENT_NEWS_RISK_LITE_MAX_CONFIDENCE_PENALTY", 0.15) or 0.15), 0.15)
        max_delay = min(int(getattr(settings, "EVENT_NEWS_RISK_LITE_MAX_DELAY_SECONDS", 300) or 300), 300)

        size_multiplier = 1.0
        confidence_penalty = 0.0
        delay_seconds = 0
        expires_at = None
        if mode == MODE_RISK_LITE and applied_action == ACTION_SIZE_REDUCTION:
            size_multiplier = max(min_multiplier, min(1.0, _safe_float(raw.get("size_multiplier"), 1.0)))
            expires_at = _iso(_now() + timedelta(minutes=15))
        elif mode == MODE_RISK_LITE and applied_action == ACTION_CONFIDENCE_PENALTY:
            confidence_penalty = max(0.0, min(max_penalty, _safe_float(raw.get("confidence_penalty"), 0.0)))
            expires_at = _iso(_now() + timedelta(minutes=15))
        elif mode == MODE_RISK_LITE and applied_action == ACTION_DELAY_ENTRY:
            delay_seconds = max(0, min(max_delay, int(raw.get("delay_seconds") or 0)))
            expires_at = _iso(_now() + timedelta(seconds=delay_seconds))

        execution_impact = mode == MODE_RISK_LITE and applied_action in {
            ACTION_SIZE_REDUCTION,
            ACTION_DELAY_ENTRY,
            ACTION_CONFIDENCE_PENALTY,
        }
        if applied_action == ACTION_CONFIDENCE_PENALTY:
            # Runner does not yet have a clean confidence-penalty interface; keep
            # this traceable but non-mutating until a dedicated policy hook exists.
            execution_impact = False

        top_signal = ((context.get("news") or {}).get("top_signal") or {})
        ledger = insert_event_news_influence_decision(
            self.db,
            trace_id=trace_id,
            symbol=str(symbol or "").upper(),
            mode=mode,
            requested_action=requested_action,
            applied_action=applied_action,
            reason=reason,
            confidence=confidence if confidence is not None else _safe_float(top_signal.get("effective_confidence_score"), None),
            reliability_score=_safe_float(top_signal.get("effective_reliability_score"), None),
            fake_news_risk_score=_safe_float(top_signal.get("effective_fake_news_risk_score"), None),
            conflict_flag=_safe_bool(top_signal.get("effective_conflict_flag")),
            market_confirmation_status=top_signal.get("effective_market_confirmation_status"),
            size_multiplier=size_multiplier,
            confidence_penalty=confidence_penalty,
            delay_seconds=delay_seconds,
            cooldown_until=None,
            expires_at=expires_at,
            source_context={
                "symbol": symbol,
                "side": side,
                "trade_usdt": trade_usdt,
                "context": context,
            },
        )
        return EventNewsInfluenceResult(
            mode=mode,
            requested_action=requested_action,
            applied_action=applied_action,
            reason=reason,
            size_multiplier=size_multiplier,
            confidence_penalty=confidence_penalty,
            delay_seconds=delay_seconds,
            expires_at=expires_at,
            execution_impact_allowed=execution_impact,
            ledger_id=ledger.get("id"),
            source_context=context,
        )

    def over_influence_status(self) -> dict[str, Any]:
        hours = int(getattr(settings, "EVENT_NEWS_OVERINFLUENCE_WINDOW_HOURS", 6) or 6)
        summary = get_event_news_influence_summary(self.db, hours=hours)
        max_size_rate = float(getattr(settings, "EVENT_NEWS_OVERINFLUENCE_MAX_SIZE_REDUCTION_RATE", 0.20) or 0.20)
        max_delay_rate = float(getattr(settings, "EVENT_NEWS_OVERINFLUENCE_MAX_DELAY_RATE", 0.10) or 0.10)
        max_avg_reduction = float(getattr(settings, "EVENT_NEWS_OVERINFLUENCE_MAX_AVG_SIZE_REDUCTION", 0.15) or 0.15)

        failures: list[str] = []
        if int(summary.get("hard_block_count") or 0) > 0:
            failures.append("influence_hard_block_emitted")
        if int(summary.get("forbidden_action_count") or 0) > 0:
            failures.append("forbidden_influence_action_emitted")
        if float(summary.get("size_reduction_signal_pct") or 0.0) > max_size_rate:
            failures.append("size_reduction_rate_exceeded")
        if float(summary.get("delay_signal_pct") or 0.0) > max_delay_rate:
            failures.append("delay_rate_exceeded")
        if float(summary.get("avg_size_reduction") or 0.0) > max_avg_reduction:
            failures.append("average_size_reduction_exceeded")
        return {"healthy": not failures, "failures": failures, "summary": summary}

    def _request_action(self, mode: str, context: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
        if mode == MODE_DISABLED:
            return ACTION_NONE, "Event/News mode disabled", {}
        if mode in {MODE_SHADOW, MODE_ADVISORY}:
            return ACTION_ANNOTATE_ONLY, f"{mode} mode is annotate-only", {}
        if mode != MODE_RISK_LITE:
            return ACTION_ANNOTATE_ONLY, f"{mode} is locked above RISK_LITE; annotate-only", {}

        news = context.get("news") or {}
        top = news.get("top_signal") or {}
        provider = context.get("provider_health") or {}
        reliability = _safe_float(top.get("effective_reliability_score"))
        signal_confidence = _safe_float(top.get("effective_confidence_score"))
        fake_risk = _safe_float(top.get("effective_fake_news_risk_score"))
        conflict = _safe_bool(top.get("effective_conflict_flag"))
        severity = str(top.get("severity_level") or "").upper()
        provider_healthy = bool(provider.get("healthy"))

        if (
            news.get("has_recent_signal")
            and reliability >= 0.85
            and signal_confidence >= 0.85
            and fake_risk <= 0.20
            and not conflict
            and severity in {"HIGH", "CRITICAL"}
            and provider_healthy
        ):
            multiplier = 0.75 if severity == "CRITICAL" else 0.85
            return (
                ACTION_SIZE_REDUCTION,
                f"High-reliability {severity} news risk; capped RISK_LITE size reduction",
                {"size_multiplier": multiplier},
            )

        reaction = (context.get("reaction") or {}).get("latest") or {}
        validation = (context.get("validation") or {}).get("latest") or {}
        vol = max(
            _safe_float(reaction.get("volatility_expansion_ratio")),
            _safe_float(validation.get("volatility_expansion")),
        )
        if vol >= 2.0:
            return (
                ACTION_DELAY_ENTRY,
                "Recent event/news reaction volatility remains elevated; short RISK_LITE delay",
                {"delay_seconds": 180},
            )

        market_status = str(top.get("effective_market_confirmation_status") or "").upper()
        if news.get("has_recent_signal") and (
            reliability >= 0.65
            or signal_confidence >= 0.65
            or market_status in {"UNCERTAIN", "WEAK", "UNCONFIRMED"}
        ):
            return (
                ACTION_CONFIDENCE_PENALTY,
                "Medium-confidence news/reaction uncertainty; confidence penalty recorded",
                {"confidence_penalty": 0.10},
            )

        return ACTION_ANNOTATE_ONLY, "No actionable RISK_LITE event/news risk", {}

    def _cap_action(self, mode: str, requested_action: str) -> str:
        if requested_action in FORBIDDEN_PROMPT_3_ACTIONS:
            return ACTION_ANNOTATE_ONLY
        if mode == MODE_DISABLED:
            return ACTION_NONE
        if mode in {MODE_SHADOW, MODE_ADVISORY}:
            return ACTION_ANNOTATE_ONLY if requested_action != ACTION_NONE else ACTION_NONE
        if mode == MODE_RISK_LITE:
            if requested_action in {
                ACTION_NONE,
                ACTION_ANNOTATE_ONLY,
                ACTION_CONFIDENCE_PENALTY,
                ACTION_SIZE_REDUCTION,
                ACTION_DELAY_ENTRY,
            }:
                return requested_action
            return ACTION_ANNOTATE_ONLY
        return ACTION_ANNOTATE_ONLY
