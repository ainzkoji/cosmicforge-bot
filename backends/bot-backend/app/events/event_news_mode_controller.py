from __future__ import annotations

from typing import Any

from app.core.config import settings
from app.events.event_news_influence_engine import EventNewsInfluenceEngine
from app.events.event_news_readiness_evaluator import EventNewsReadinessEvaluator
from shared_lib.persistence.db import DB
from shared_lib.persistence.event_news_mode import (
    ACTION_ANNOTATE_ONLY,
    ACTION_DELAY_ENTRY,
    ACTION_NONE,
    DECISION_DEMOTE,
    DECISION_DISABLE,
    DECISION_HOLD,
    DECISION_PROMOTE,
    MODE_ADVISORY,
    MODE_DISABLED,
    MODE_RISK_GUARD,
    MODE_RISK_LITE,
    MODE_SHADOW,
    MODE_LIVE_SIGNAL_ELIGIBLE,
    ensure_event_news_mode_schema,
    get_event_news_runtime_mode,
    get_recent_event_news_mode_decisions,
    persist_event_news_mode_decision,
)


class EventNewsModeController:
    """Automatic Event/News runtime mode controller.

    Prompt 3 permits automatic ADVISORY -> RISK_LITE promotion only after
    strict evidence gates. RISK_LITE remains capped to soft influence:
    annotate, confidence penalty, size reduction, and short delay.
    """

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()
        ensure_event_news_mode_schema(self.db)
        self.evaluator = EventNewsReadinessEvaluator(self.db)

    def evaluate_and_record(self) -> dict[str, Any]:
        state = get_event_news_runtime_mode(self.db)
        current_mode = str(state.get("current_mode") or MODE_SHADOW).upper()
        auto_promotion = bool(getattr(settings, "EVENT_NEWS_AUTO_PROMOTION_ENABLED", True))
        auto_demotion = bool(getattr(settings, "EVENT_NEWS_AUTO_DEMOTION_ENABLED", True))
        evaluation = self.evaluator.evaluate()
        risk_lite = self._risk_lite_evidence(evaluation)

        from_mode = current_mode
        to_mode = current_mode
        decision_type = DECISION_HOLD
        reason = "Event/News mode held"

        if evaluation.critical_safety_failure:
            decision_type = DECISION_DISABLE
            to_mode = MODE_DISABLED
            reason = "Critical Event/News safety failure; disabling influence controller"
        elif current_mode == MODE_DISABLED:
            decision_type = DECISION_PROMOTE
            to_mode = MODE_SHADOW
            reason = "Critical safety cleared; returning to safe shadow mode"
        elif current_mode == MODE_SHADOW:
            if auto_promotion and evaluation.ready_for_advisory:
                decision_type = DECISION_PROMOTE
                to_mode = MODE_ADVISORY
                reason = "Readiness passed; promoting SHADOW to ADVISORY"
            else:
                reason = "Waiting for SHADOW to ADVISORY readiness"
        elif current_mode == MODE_ADVISORY:
            if auto_demotion and not evaluation.ready_for_advisory:
                decision_type = DECISION_DEMOTE
                to_mode = MODE_SHADOW
                reason = "Readiness degraded; demoting ADVISORY to SHADOW"
            elif auto_promotion and risk_lite["ready"]:
                decision_type = DECISION_PROMOTE
                to_mode = MODE_RISK_LITE
                reason = "RISK_LITE readiness passed; enabling soft influence mode"
            else:
                reason = "ADVISORY readiness remains healthy; RISK_LITE gates not yet met"
        elif current_mode == MODE_RISK_LITE:
            over = EventNewsInfluenceEngine(self.db).over_influence_status()
            if auto_demotion and (not evaluation.ready_for_advisory or not risk_lite["ready"] or not over["healthy"]):
                decision_type = DECISION_DEMOTE
                to_mode = MODE_ADVISORY if evaluation.ready_for_advisory else MODE_SHADOW
                reason = "RISK_LITE safety/readiness degraded; demoting to safe lower mode"
                risk_lite["over_influence"] = over
            else:
                reason = "RISK_LITE readiness and over-influence caps remain healthy"
                risk_lite["over_influence"] = over
        elif current_mode in {MODE_RISK_GUARD, MODE_LIVE_SIGNAL_ELIGIBLE}:
            # Prompt 3 explicitly keeps these states locked.
            decision_type = DECISION_DEMOTE
            to_mode = MODE_RISK_LITE if evaluation.ready_for_advisory and risk_lite["ready"] else MODE_SHADOW
            reason = f"Unsupported locked mode {current_mode}; forcing safe supported mode"
        else:
            decision_type = DECISION_DEMOTE
            to_mode = MODE_SHADOW
            reason = f"Unsupported mode {current_mode}; forcing safe SHADOW"

        evidence = dict(evaluation.evidence)
        evidence["risk_lite"] = risk_lite
        record = persist_event_news_mode_decision(
            self.db,
            decision_type=decision_type,
            from_mode=from_mode,
            to_mode=to_mode,
            readiness_score=evaluation.readiness_score,
            safety_score=evaluation.safety_score,
            evidence=evidence,
            evaluation_window_start=evaluation.evaluation_window_start,
            evaluation_window_end=evaluation.evaluation_window_end,
            reason=reason,
            failed_criteria=evaluation.failed_criteria,
            passed_criteria=evaluation.passed_criteria,
            safety_status=evaluation.safety_status,
            auto_promotion_enabled=auto_promotion,
            auto_demotion_enabled=auto_demotion,
        )
        record.update(
            {
                "current_mode": to_mode,
                "previous_mode": from_mode,
                "max_allowed_action": self._max_action_for_mode(to_mode),
                "execution_impact": to_mode == MODE_RISK_LITE,
                "ready_for_advisory": evaluation.ready_for_advisory,
                "ready_for_risk_lite": risk_lite["ready"],
                "critical_safety_failure": evaluation.critical_safety_failure,
                "evidence": evidence,
            }
        )
        return record

    def status(self) -> dict[str, Any]:
        state = get_event_news_runtime_mode(self.db)
        decisions = get_recent_event_news_mode_decisions(self.db, limit=10)
        current = str(state.get("current_mode") or MODE_SHADOW).upper()
        next_eligible = MODE_ADVISORY if current == MODE_SHADOW else None
        if current == MODE_ADVISORY:
            next_eligible = MODE_RISK_LITE
        elif current == MODE_RISK_LITE:
            next_eligible = "RISK_GUARD_LOCKED"
        return {
            "state": state,
            "recent_decisions": decisions,
            "next_eligible_mode": next_eligible,
            "active_transition_limit": "DISABLED<->SHADOW<->ADVISORY<->RISK_LITE",
            "execution_impact": current == MODE_RISK_LITE,
            "max_allowed_action": state.get("max_allowed_action") or self._max_action_for_mode(current),
        }

    def _max_action_for_mode(self, mode: str | None) -> str:
        if mode == MODE_DISABLED:
            return ACTION_NONE
        if mode == MODE_RISK_LITE:
            return ACTION_DELAY_ENTRY
        return ACTION_ANNOTATE_ONLY

    def _risk_lite_evidence(self, evaluation: Any) -> dict[str, Any]:
        failed: list[str] = []
        passed: list[str] = []
        evidence: dict[str, Any] = {}
        with self.db.connect() as conn:
            runtime = get_event_news_runtime_mode(self.db)
            promoted_at = runtime.get("promoted_at") or runtime.get("updated_at")
            advisory_age_minutes = self._age_minutes(promoted_at)
            news_clusters = self._count(conn, "SELECT COUNT(*) FROM news_clusters")
            news_signals = self._count(conn, "SELECT COUNT(*) FROM news_intelligence_signals")
            validations = self._count(conn, "SELECT COUNT(DISTINCT cluster_id) FROM news_market_reactions")
            false_signals = self._count(conn, "SELECT COUNT(*) FROM news_market_reactions WHERE COALESCE(is_false_signal,0)=1")
            provider_rows = self._count(
                conn,
                """
                SELECT COUNT(*) FROM news_provider_health h
                WHERE h.id = (
                    SELECT MAX(id) FROM news_provider_health
                    WHERE source_id = h.source_id
                )
                """,
            )
            failed_providers = self._count(
                conn,
                """
                SELECT COUNT(*) FROM news_provider_health h
                WHERE h.id = (
                    SELECT MAX(id) FROM news_provider_health
                    WHERE source_id = h.source_id
                )
                AND UPPER(COALESCE(h.status,'')) = 'FAILED'
                """,
            )
            unsafe_signals = self._count(
                conn,
                "SELECT COUNT(*) FROM news_intelligence_signals WHERE shadow_only != 1 OR should_affect_trading != 0",
            )

        false_ratio = (false_signals / validations) if validations else 0.0
        provider_failure_rate = (failed_providers / provider_rows) if provider_rows else 0.0
        dev_allowed = bool(getattr(settings, "EVENT_NEWS_ALLOW_DEV_RISK_LITE_PROMOTION", False))
        env = str(getattr(settings, "BINANCE_ENV", "") or "").lower()
        base_url = str(getattr(settings, "BINANCE_FAPI_BASE_URL", "") or "").lower()
        is_demo = env in {"testnet", "demo"} or "demo-fapi" in base_url or "testnet" in base_url

        evidence.update(
            {
                "advisory_age_minutes": advisory_age_minutes,
                "news_clusters": news_clusters,
                "news_signals": news_signals,
                "validated_clusters": validations,
                "false_signal_ratio": round(false_ratio, 4),
                "provider_failure_rate": round(provider_failure_rate, 4),
                "unsafe_news_signals": unsafe_signals,
                "dev_promotion_allowed": dev_allowed,
                "is_demo_environment": is_demo,
            }
        )

        if not evaluation.ready_for_advisory:
            failed.append("advisory_readiness_not_met")
        else:
            passed.append("advisory_readiness_met")
        if unsafe_signals != 0:
            failed.append("unsafe_news_signal_invariant_failed")
        else:
            passed.append("unsafe_news_signal_invariant_clean")

        if dev_allowed:
            min_minutes = int(getattr(settings, "EVENT_NEWS_DEV_MIN_ADVISORY_MINUTES", 30) or 30)
            min_clusters = int(getattr(settings, "EVENT_NEWS_DEV_MIN_CLUSTERS", 100) or 100)
            min_signals = int(getattr(settings, "EVENT_NEWS_DEV_MIN_SIGNALS", 100) or 100)
            if not is_demo:
                failed.append("dev_risk_lite_requires_demo_or_testnet")
            else:
                passed.append("dev_environment_confirmed")
            if advisory_age_minutes < min_minutes:
                failed.append("dev_advisory_window_too_short")
            else:
                passed.append("dev_advisory_window_met")
            if float(evaluation.readiness_score) < 90.0:
                failed.append("dev_readiness_score_below_90")
            else:
                passed.append("dev_readiness_score_met")
            if news_clusters < min_clusters:
                failed.append("dev_insufficient_news_clusters")
            else:
                passed.append("dev_news_clusters_met")
            if news_signals < min_signals:
                failed.append("dev_insufficient_news_signals")
            else:
                passed.append("dev_news_signals_met")
            if failed_providers > 0:
                failed.append("dev_failed_providers_present")
            else:
                passed.append("dev_no_failed_providers")
        else:
            min_days = float(getattr(settings, "EVENT_NEWS_RISK_LITE_MIN_ADVISORY_DAYS", 7.0) or 7.0)
            min_validated = int(getattr(settings, "EVENT_NEWS_RISK_LITE_MIN_VALIDATED_CLUSTERS", 200) or 200)
            max_false_ratio = float(getattr(settings, "EVENT_NEWS_RISK_LITE_MAX_FALSE_SIGNAL_RATIO", 0.20) or 0.20)
            max_provider_fail = float(getattr(settings, "EVENT_NEWS_RISK_LITE_MAX_PROVIDER_FAILURE_RATE", 0.20) or 0.20)
            if advisory_age_minutes < min_days * 24.0 * 60.0:
                failed.append("risk_lite_advisory_window_too_short")
            else:
                passed.append("risk_lite_advisory_window_met")
            if validations < min_validated:
                failed.append("insufficient_validated_news_clusters")
            else:
                passed.append("validated_news_cluster_count_met")
            if false_ratio > max_false_ratio:
                failed.append("false_signal_ratio_too_high")
            else:
                passed.append("false_signal_ratio_within_limit")
            if provider_failure_rate > max_provider_fail:
                failed.append("provider_failure_rate_too_high")
            else:
                passed.append("provider_failure_rate_within_limit")

        over = EventNewsInfluenceEngine(self.db).over_influence_status()
        evidence["over_influence"] = over
        if not over["healthy"]:
            failed.extend(over["failures"])
        else:
            passed.append("over_influence_caps_clean")

        return {
            "ready": not failed,
            "passed": sorted(set(passed)),
            "failed": sorted(set(failed)),
            "evidence": evidence,
        }

    def _count(self, conn: Any, sql: str) -> int:
        try:
            row = conn.execute(sql).fetchone()
            return int((row[0] if row else 0) or 0)
        except Exception:
            return 0

    def _age_minutes(self, iso_value: str | None) -> float:
        if not iso_value:
            return 0.0
        try:
            dt = __import__("datetime").datetime.fromisoformat(str(iso_value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=__import__("datetime").timezone.utc)
            now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
            return max(0.0, (now - dt.astimezone(__import__("datetime").timezone.utc)).total_seconds() / 60.0)
        except Exception:
            return 0.0
