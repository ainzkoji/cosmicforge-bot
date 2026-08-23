from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from shared_lib.persistence.db import DB
from shared_lib.persistence.event_news_mode import get_event_news_runtime_mode


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


class EventNewsContextAggregator:
    """Read-only intelligence context collector.

    This class intentionally does not decide, block, size, or execute trades.
    It returns the freshest event/news/reaction context for the influence
    engine, which performs mode-capped decisions separately.
    """

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()

    def collect(self, *, symbol: str, trace_id: str | None = None) -> dict[str, Any]:
        clean_symbol = str(symbol or "").upper()
        now = _now()
        with self.db.connect() as conn:
            return {
                "trace_id": trace_id,
                "symbol": clean_symbol,
                "collected_at": _iso(now),
                "mode": get_event_news_runtime_mode(self.db),
                "event": self._event_context(conn, clean_symbol, now),
                "news": self._news_context(conn, clean_symbol, now),
                "validation": self._validation_context(conn, clean_symbol, now),
                "reaction": self._reaction_context(conn, clean_symbol, now),
                "provider_health": self._provider_health_context(conn),
            }

    def _event_context(self, conn: Any, symbol: str, now: datetime) -> dict[str, Any]:
        out: dict[str, Any] = {
            "active_blackout": False,
            "active_blackout_reason": None,
            "active_blackout_impact": None,
            "minutes_to_next_high_impact_event": None,
            "next_high_impact_event": None,
        }
        if _table_exists(conn, "event_blackout_windows"):
            row = conn.execute(
                """
                SELECT bw.*, ev.title, ev.impact_level, ev.scheduled_utc
                FROM event_blackout_windows bw
                LEFT JOIN economic_events ev ON ev.id = bw.event_id OR ev.event_id = bw.event_id
                WHERE bw.start_utc <= ? AND bw.end_utc >= ?
                  AND (
                    bw.is_global = 1
                    OR UPPER(COALESCE(bw.affected_symbols, '')) LIKE ?
                  )
                ORDER BY bw.end_utc ASC
                LIMIT 1
                """,
                (_iso(now), _iso(now), f"%{symbol}%"),
            ).fetchone()
            if row:
                data = dict(row)
                out.update(
                    {
                        "active_blackout": True,
                        "active_blackout_reason": data.get("reason") or data.get("title"),
                        "active_blackout_impact": data.get("impact_level"),
                        "blackout_start_utc": data.get("start_utc"),
                        "blackout_end_utc": data.get("end_utc"),
                        "is_global": bool(data.get("is_global")),
                    }
                )

        if _table_exists(conn, "economic_events"):
            row = conn.execute(
                """
                SELECT *
                FROM economic_events
                WHERE scheduled_utc >= ?
                  AND UPPER(COALESCE(impact_level, '')) IN ('HIGH', 'CRITICAL')
                ORDER BY scheduled_utc ASC
                LIMIT 1
                """,
                (_iso(now),),
            ).fetchone()
            if row:
                data = dict(row)
                try:
                    scheduled = datetime.fromisoformat(str(data.get("scheduled_utc")).replace("Z", "+00:00"))
                    if scheduled.tzinfo is None:
                        scheduled = scheduled.replace(tzinfo=timezone.utc)
                    out["minutes_to_next_high_impact_event"] = round(
                        (scheduled.astimezone(timezone.utc) - now).total_seconds() / 60.0,
                        2,
                    )
                except Exception:
                    pass
                out["next_high_impact_event"] = data
        return out

    def _news_context(self, conn: Any, symbol: str, now: datetime) -> dict[str, Any]:
        out: dict[str, Any] = {
            "has_recent_signal": False,
            "top_signal": None,
            "recent_signal_count": 0,
        }
        if not _table_exists(conn, "news_intelligence_signals"):
            return out
        since = _iso(now - timedelta(hours=6))
        rows = conn.execute(
            """
            SELECT nis.*, nc.cluster_confidence, nc.highest_reliability_score,
                   nc.conflict_flag AS cluster_conflict_flag,
                   nc.fake_news_risk_score AS cluster_fake_news_risk_score,
                   nc.market_confirmation_status AS cluster_market_confirmation_status
            FROM news_intelligence_signals nis
            LEFT JOIN news_clusters nc ON nc.id = nis.cluster_id
            LEFT JOIN news_asset_mappings nam ON nam.cluster_id = nis.cluster_id
            WHERE nis.created_at >= ?
              AND (
                UPPER(COALESCE(nis.symbol, '')) = ?
                OR UPPER(COALESCE(nam.symbol, '')) = ?
                OR COALESCE(nam.is_global_market_event, 0) = 1
              )
            ORDER BY
              COALESCE(nis.reliability_score, nc.highest_reliability_score, 0) DESC,
              COALESCE(nis.confidence_score, nc.cluster_confidence, 0) DESC,
              nis.created_at DESC
            LIMIT 10
            """,
            (since, symbol, symbol),
        ).fetchall()
        items = [dict(r) for r in rows]
        if not items:
            return out
        top = items[0]
        top["effective_reliability_score"] = max(
            _safe_float(top.get("reliability_score")),
            _safe_float(top.get("highest_reliability_score")),
        )
        top["effective_confidence_score"] = max(
            _safe_float(top.get("confidence_score")),
            _safe_float(top.get("cluster_confidence")),
        )
        top["effective_fake_news_risk_score"] = max(
            _safe_float(top.get("fake_news_risk_score")),
            _safe_float(top.get("cluster_fake_news_risk_score")),
        )
        top["effective_conflict_flag"] = bool(
            _safe_int(top.get("conflict_flag")) or _safe_int(top.get("cluster_conflict_flag"))
        )
        top["effective_market_confirmation_status"] = (
            top.get("market_confirmation_status") or top.get("cluster_market_confirmation_status")
        )
        out.update(
            {
                "has_recent_signal": True,
                "top_signal": top,
                "recent_signal_count": len(items),
            }
        )
        return out

    def _validation_context(self, conn: Any, symbol: str, now: datetime) -> dict[str, Any]:
        if not _table_exists(conn, "news_market_reactions"):
            return {"has_recent_validation": False}
        row = conn.execute(
            """
            SELECT *
            FROM news_market_reactions
            WHERE UPPER(COALESCE(symbol, '')) = ?
              AND created_at >= ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (symbol, _iso(now - timedelta(hours=24))),
        ).fetchone()
        return {"has_recent_validation": bool(row), "latest": dict(row) if row else None}

    def _reaction_context(self, conn: Any, symbol: str, now: datetime) -> dict[str, Any]:
        if not _table_exists(conn, "market_event_reactions"):
            return {"has_recent_reaction": False}
        row = conn.execute(
            """
            SELECT *
            FROM market_event_reactions
            WHERE UPPER(COALESCE(symbol, '')) = ?
              AND created_at >= ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (symbol, _iso(now - timedelta(hours=24))),
        ).fetchone()
        return {"has_recent_reaction": bool(row), "latest": dict(row) if row else None}

    def _provider_health_context(self, conn: Any) -> dict[str, Any]:
        if not _table_exists(conn, "news_provider_health"):
            return {"latest_rows": 0, "failed_rows": 0, "healthy": False}
        rows = conn.execute(
            """
            SELECT *
            FROM news_provider_health h
            WHERE h.id = (
                SELECT MAX(id) FROM news_provider_health
                WHERE source_id = h.source_id
            )
            """
        ).fetchall()
        latest = [dict(r) for r in rows]
        failed = sum(1 for r in latest if str(r.get("status") or "").upper() == "FAILED")
        return {"latest_rows": len(latest), "failed_rows": failed, "healthy": failed == 0 and len(latest) > 0}
