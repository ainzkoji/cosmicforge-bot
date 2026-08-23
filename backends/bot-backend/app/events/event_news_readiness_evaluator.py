from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.config import settings
from shared_lib.persistence.db import DB


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _table_exists(conn: Any, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _count(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    try:
        row = conn.execute(sql, params).fetchone()
        return int((row[0] if row is not None else 0) or 0)
    except Exception:
        return 0


@dataclass(frozen=True)
class ReadinessEvaluation:
    readiness_score: float
    safety_score: float
    safety_status: str
    ready_for_advisory: bool
    critical_safety_failure: bool
    passed_criteria: list[str]
    failed_criteria: list[str]
    evidence: dict[str, Any]
    evaluation_window_start: str
    evaluation_window_end: str


class EventNewsReadinessEvaluator:
    """Read-only readiness evaluator for Event/News mode promotion.

    This evaluator intentionally does not change trading behavior. It only
    measures whether the shadow intelligence layer is healthy enough to promote
    to ADVISORY, where the max action is still ANNOTATE_ONLY.
    """

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()

    def evaluate(self) -> ReadinessEvaluation:
        now = _now()
        min_hours = float(getattr(settings, "EVENT_NEWS_ADVISORY_MIN_RUNTIME_HOURS", 24.0) or 0.0)
        min_raw_items = int(getattr(settings, "EVENT_NEWS_ADVISORY_MIN_RAW_NEWS_ITEMS", 1) or 0)
        min_clusters = int(getattr(settings, "EVENT_NEWS_ADVISORY_MIN_CLUSTERS", 1) or 0)
        min_health_rows = int(getattr(settings, "EVENT_NEWS_ADVISORY_MIN_PROVIDER_HEALTH_ROWS", 1) or 0)
        max_failed_providers = int(getattr(settings, "EVENT_NEWS_ADVISORY_MAX_FAILED_PROVIDERS", 999999) or 999999)
        max_unsafe_signals = int(getattr(settings, "EVENT_NEWS_ADVISORY_MAX_UNSAFE_SIGNALS", 0) or 0)
        lookback_hours = int(getattr(settings, "EVENT_NEWS_ADVISORY_LOOKBACK_HOURS", 24) or 24)
        window_start = now - timedelta(hours=lookback_hours)

        passed: list[str] = []
        failed: list[str] = []
        critical: list[str] = []
        evidence: dict[str, Any] = {
            "config": self._config_evidence(),
            "lookback_hours": lookback_hours,
            "required_runtime_hours": min_hours,
        }

        if not bool(getattr(settings, "NEWS_INTELLIGENCE_ENABLED", False)):
            failed.append("news_intelligence_disabled")
        else:
            passed.append("news_intelligence_enabled")

        if not bool(getattr(settings, "NEWS_SHADOW_ONLY", True)):
            critical.append("news_shadow_only_disabled")
        else:
            passed.append("news_shadow_only_enabled")

        unsafe_config = self._unsafe_config_flags()
        evidence["unsafe_config_flags"] = unsafe_config
        if unsafe_config:
            critical.extend([f"unsafe_config:{flag}" for flag in unsafe_config])
        else:
            passed.append("news_execution_flags_safe")

        with self.db.connect() as conn:
            raw_items = _count(conn, "SELECT COUNT(*) FROM raw_news_items") if _table_exists(conn, "raw_news_items") else 0
            raw_items_recent = (
                _count(conn, "SELECT COUNT(*) FROM raw_news_items WHERE ingested_utc >= ?", (_iso(window_start),))
                if _table_exists(conn, "raw_news_items")
                else 0
            )
            clusters = _count(conn, "SELECT COUNT(*) FROM news_clusters") if _table_exists(conn, "news_clusters") else 0
            health_rows = (
                _count(conn, "SELECT COUNT(*) FROM news_provider_health")
                if _table_exists(conn, "news_provider_health")
                else 0
            )
            failed_providers = (
                _count(
                    conn,
                    """
                    SELECT COUNT(*) FROM news_provider_health h
                    WHERE h.id = (
                        SELECT MAX(id) FROM news_provider_health
                        WHERE source_id = h.source_id
                    )
                    AND UPPER(COALESCE(h.status, '')) = 'FAILED'
                    """,
                )
                if _table_exists(conn, "news_provider_health")
                else 0
            )
            unsafe_signals = (
                _count(
                    conn,
                    """
                    SELECT COUNT(*)
                    FROM news_intelligence_signals
                    WHERE shadow_only != 1 OR should_affect_trading != 0
                    """,
                )
                if _table_exists(conn, "news_intelligence_signals")
                else 0
            )
            oldest_news = None
            if _table_exists(conn, "raw_news_items"):
                row = conn.execute("SELECT MIN(ingested_utc) AS ts FROM raw_news_items").fetchone()
                oldest_news = row["ts"] if row else None

        runtime_hours = self._runtime_hours(oldest_news, now)
        evidence.update(
            {
                "raw_news_items": raw_items,
                "raw_news_items_recent": raw_items_recent,
                "news_clusters": clusters,
                "news_provider_health_rows": health_rows,
                "failed_providers": failed_providers,
                "unsafe_news_signals": unsafe_signals,
                "oldest_news_ingested_utc": oldest_news,
                "runtime_hours": runtime_hours,
            }
        )

        if unsafe_signals > max_unsafe_signals:
            critical.append("unsafe_news_signal_invariant_failed")
        else:
            passed.append("news_signal_safety_invariant_clean")

        if runtime_hours < min_hours:
            failed.append("runtime_window_too_short")
        else:
            passed.append("minimum_runtime_window_met")

        if raw_items < min_raw_items:
            failed.append("insufficient_raw_news_items")
        else:
            passed.append("raw_news_items_present")

        if clusters < min_clusters:
            failed.append("insufficient_news_clusters")
        else:
            passed.append("news_clusters_present")

        if health_rows < min_health_rows:
            failed.append("provider_health_missing")
        else:
            passed.append("provider_health_present")

        if failed_providers > max_failed_providers:
            failed.append("too_many_failed_news_providers")
        else:
            passed.append("provider_failures_within_limit")

        all_failed = sorted(set(failed + critical))
        passed = sorted(set(passed))
        criteria_count = max(len(passed) + len(all_failed), 1)
        readiness_score = round(100.0 * len(passed) / criteria_count, 2)
        safety_score = 0.0 if critical else 100.0
        safety_status = "CRITICAL" if critical else "SAFE"
        return ReadinessEvaluation(
            readiness_score=readiness_score,
            safety_score=safety_score,
            safety_status=safety_status,
            ready_for_advisory=not all_failed,
            critical_safety_failure=bool(critical),
            passed_criteria=passed,
            failed_criteria=all_failed,
            evidence=evidence,
            evaluation_window_start=_iso(window_start),
            evaluation_window_end=_iso(now),
        )

    def _runtime_hours(self, oldest_news: str | None, now: datetime) -> float:
        if not oldest_news:
            return 0.0
        try:
            dt = datetime.fromisoformat(str(oldest_news).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds() / 3600.0)
        except Exception:
            return 0.0

    def _config_evidence(self) -> dict[str, Any]:
        keys = [
            "NEWS_INTELLIGENCE_ENABLED",
            "NEWS_INTELLIGENCE_SHADOW_MODE",
            "NEWS_SHADOW_ONLY",
            "NEWS_TRADING_ENABLED",
            "NEWS_SIGNAL_CAN_OPEN_TRADES",
            "NEWS_SIGNAL_CAN_CLOSE_TRADES",
            "NEWS_SIGNAL_CAN_BLOCK_TRADES",
            "REAL_TIME_NEWS_CAN_OPEN_TRADES",
            "REAL_TIME_NEWS_CAN_CLOSE_TRADES",
            "REAL_TIME_NEWS_CAN_BLOCK_TRADES",
            "REACTION_ALLOW_RISK_INFLUENCE",
        ]
        return {key: bool(getattr(settings, key, False)) for key in keys}

    def _unsafe_config_flags(self) -> list[str]:
        unsafe_when_true = [
            "NEWS_TRADING_ENABLED",
            "NEWS_SIGNAL_CAN_OPEN_TRADES",
            "NEWS_SIGNAL_CAN_CLOSE_TRADES",
            "NEWS_SIGNAL_CAN_BLOCK_TRADES",
            "REAL_TIME_NEWS_CAN_OPEN_TRADES",
            "REAL_TIME_NEWS_CAN_CLOSE_TRADES",
            "REAL_TIME_NEWS_CAN_BLOCK_TRADES",
            "REACTION_ALLOW_RISK_INFLUENCE",
        ]
        return [key for key in unsafe_when_true if bool(getattr(settings, key, False))]
