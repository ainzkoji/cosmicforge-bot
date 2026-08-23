from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any

from app.persistence.db import AdminDB


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _bounded_limit(limit: int | None, *, default: int, maximum: int) -> int:
    try:
        parsed = int(limit or default)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def _since(hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _load_json(raw: Any, default: Any) -> Any:
    try:
        if raw is None or raw == "":
            return default
        return json.loads(str(raw))
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Feed status
# ---------------------------------------------------------------------------

def get_news_feed_status(db: AdminDB) -> dict[str, Any]:
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).isoformat()

    today_count = 0
    latest_title: str | None = None
    latest_ingested_utc: str | None = None
    active_sources = 0
    failed_sources = 0
    rss_enabled_sources = 0
    rt_enabled_providers = 0
    rt_healthy_providers = 0

    with db.connect() as conn:
        if _table_exists(conn, "raw_news_items"):
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM raw_news_items "
                "WHERE ingested_utc >= ? AND provider != 'manual'",
                (today_start,),
            ).fetchone()
            today_count = int(row["cnt"] if row else 0)
            latest = conn.execute(
                "SELECT title, ingested_utc FROM raw_news_items "
                "WHERE provider != 'manual' "
                "ORDER BY ingested_utc DESC, id DESC LIMIT 1"
            ).fetchone()
            if latest:
                latest_title = latest["title"]
                latest_ingested_utc = latest["ingested_utc"]

        if _table_exists(conn, "news_provider_health"):
            health_rows = conn.execute(
                """SELECT source_id, status FROM news_provider_health nph
                   WHERE nph.id = (
                       SELECT MAX(id) FROM news_provider_health
                       WHERE source_id = nph.source_id
                   )"""
            ).fetchall()
            active_sources = sum(1 for r in health_rows if r["status"] in ("HEALTHY", "DEGRADED"))
            failed_sources = sum(1 for r in health_rows if r["status"] == "FAILED")

        if _table_exists(conn, "news_sources"):
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM news_sources "
                "WHERE source_type='RSS' AND is_enabled=1 "
                "AND rss_url IS NOT NULL AND rss_url != ''"
            ).fetchone()
            rss_enabled_sources = int(row["cnt"] if row else 0)

        if _table_exists(conn, "real_time_news_provider_status"):
            rt_rows = conn.execute(
                "SELECT provider, is_enabled, health_status "
                "FROM real_time_news_provider_status ORDER BY provider"
            ).fetchall()
            rt_enabled_providers = sum(1 for r in rt_rows if r["is_enabled"])
            rt_healthy_providers = sum(
                1 for r in rt_rows if r["is_enabled"] and r["health_status"] == "HEALTHY"
            )

    ingestion_warning: str | None = None
    if rss_enabled_sources == 0:
        ingestion_warning = (
            "No RSS sources are enabled. RSS is the primary live ingestion path."
        )
    elif today_count == 0:
        ingestion_warning = (
            "No live news has been ingested yet. "
            "RSS is primary; optional API providers may remain disabled."
        )

    return {
        "today_count": today_count,
        "active_sources": active_sources,
        "failed_sources": failed_sources,
        "rss_enabled_sources": rss_enabled_sources,
        "rt_enabled_providers": rt_enabled_providers,
        "rt_healthy_providers": rt_healthy_providers,
        "has_live_data": today_count > 0,
        "latest_title": latest_title,
        "latest_ingested_utc": latest_ingested_utc,
        "ingestion_warning": ingestion_warning,
        "shadow_only": True,
        "signal_can_open_trades": False,
        "signal_can_close_trades": False,
        "signal_can_block_trades": False,
    }


# ---------------------------------------------------------------------------
# Runtime mode
# ---------------------------------------------------------------------------

def get_runtime_mode(db: AdminDB) -> dict[str, Any]:
    with db.connect() as conn:
        if not _table_exists(conn, "event_news_runtime_mode"):
            return {
                "current_mode": "SHADOW",
                "previous_mode": None,
                "max_allowed_action": "ANNOTATE_ONLY",
                "readiness_score": 0,
                "safety_status": "UNKNOWN",
                "failed_criteria": [],
                "passed_criteria": [],
            }
        row = conn.execute(
            "SELECT * FROM event_news_runtime_mode WHERE id = 1"
        ).fetchone()
    if not row:
        return {
            "current_mode": "SHADOW",
            "previous_mode": None,
            "max_allowed_action": "ANNOTATE_ONLY",
            "readiness_score": 0,
            "safety_status": "UNKNOWN",
            "failed_criteria": [],
            "passed_criteria": [],
        }
    data = dict(row)
    data["failed_criteria"] = _load_json(data.get("failed_criteria_json"), [])
    data["passed_criteria"] = _load_json(data.get("passed_criteria_json"), [])
    return data


def get_recent_mode_decisions(db: AdminDB, *, limit: int = 10) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "event_news_mode_decisions"):
            return []
        rows = conn.execute(
            "SELECT * FROM event_news_mode_decisions "
            "ORDER BY created_at DESC, id DESC LIMIT ?",
            (_bounded_limit(limit, default=10, maximum=100),),
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["evidence"] = _load_json(item.get("evidence_json"), {})
        out.append(item)
    return out


# ---------------------------------------------------------------------------
# Influence decisions
# ---------------------------------------------------------------------------

def get_influence_decisions(
    db: AdminDB,
    *,
    symbol: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "event_news_influence_decisions"):
            return []
        lim = _bounded_limit(limit, default=100, maximum=500)
        if symbol:
            rows = conn.execute(
                "SELECT * FROM event_news_influence_decisions "
                "WHERE UPPER(symbol) = UPPER(?) "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                (symbol, lim),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM event_news_influence_decisions "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                (lim,),
            ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["source_context"] = _load_json(item.get("source_context_json"), {})
        out.append(item)
    return out


def get_influence_summary(db: AdminDB, *, hours: int = 6) -> dict[str, Any]:
    since = _since(hours)
    total_count = 0
    eligible_count = 0
    actions: dict[str, Any] = {}
    forbidden_count = 0
    hard_block_count = 0

    with db.connect() as conn:
        if _table_exists(conn, "event_news_influence_decisions"):
            rows = conn.execute(
                """
                SELECT applied_action, COUNT(*) AS cnt,
                       AVG(size_multiplier) AS avg_size_multiplier,
                       AVG(confidence_penalty) AS avg_confidence_penalty,
                       AVG(delay_seconds) AS avg_delay_seconds
                FROM event_news_influence_decisions
                WHERE created_at >= ?
                GROUP BY applied_action
                """,
                (since,),
            ).fetchall()
            total_row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM event_news_influence_decisions "
                "WHERE created_at >= ?",
                (since,),
            ).fetchone()
            total_count = int(total_row["cnt"] if total_row else 0)
            actions = {str(r["applied_action"]): dict(r) for r in rows}
            forbidden_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM event_news_influence_decisions
                WHERE created_at >= ?
                  AND applied_action IN (
                    'SYMBOL_COOLDOWN', 'HARD_BLOCK_ENTRY', 'CANDIDATE_SIGNAL',
                    'OPEN_POSITION', 'CLOSE_POSITION'
                  )
                """,
                (since,),
            ).fetchone()
            forbidden_count = int(forbidden_row["cnt"] if forbidden_row else 0)
            hard_block_row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM event_news_influence_decisions "
                "WHERE created_at >= ? AND applied_action='HARD_BLOCK_ENTRY'",
                (since,),
            ).fetchone()
            hard_block_count = int(hard_block_row["cnt"] if hard_block_row else 0)

        if _table_exists(conn, "decision_traces"):
            try:
                eligible_row = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM decision_traces
                    WHERE ts >= ?
                      AND (
                        UPPER(COALESCE(intended_action, '')) IN ('OPEN_LONG', 'OPEN_SHORT')
                        OR UPPER(COALESCE(signal, '')) IN ('BUY', 'SELL')
                      )
                    """,
                    (since,),
                ).fetchone()
                eligible_count = int(eligible_row["cnt"] if eligible_row else 0)
            except Exception:
                eligible_count = 0

    size_count = int(actions.get("SIZE_REDUCTION", {}).get("cnt") or 0)
    delay_count = int(actions.get("DELAY_ENTRY", {}).get("cnt") or 0)
    annotate_count = int(actions.get("ANNOTATE_ONLY", {}).get("cnt") or 0)
    avg_multiplier = actions.get("SIZE_REDUCTION", {}).get("avg_size_multiplier")
    avg_size_reduction = (1.0 - float(avg_multiplier)) if avg_multiplier is not None else 0.0

    return {
        "summary": {
            "hours": hours,
            "total": total_count,
            "eligible_signal_count": eligible_count,
            "actions": actions,
            "size_reduction_count": size_count,
            "delay_count": delay_count,
            "annotate_only_count": annotate_count,
            "forbidden_action_count": forbidden_count,
            "hard_block_count": hard_block_count,
            "size_reduction_pct": (size_count / total_count) if total_count else 0.0,
            "delay_pct": (delay_count / total_count) if total_count else 0.0,
            "size_reduction_signal_pct": (size_count / eligible_count) if eligible_count else 0.0,
            "delay_signal_pct": (delay_count / eligible_count) if eligible_count else 0.0,
            "avg_size_multiplier": avg_multiplier,
            "avg_size_reduction": avg_size_reduction,
            "avg_delay_seconds": actions.get("DELAY_ENTRY", {}).get("avg_delay_seconds"),
        },
        "demotion_warnings": (
            ["Influence engine emitted hard block; this should remain zero in RISK_LITE."]
            if hard_block_count != 0
            else []
        ) + (
            ["Forbidden influence action emitted; controller should demote/disable."]
            if forbidden_count != 0
            else []
        ),
        "locked_states": ["RISK_GUARD", "LIVE_SIGNAL_ELIGIBLE"],
        "news_execution_allowed": False,
    }


# ---------------------------------------------------------------------------
# Sources + health
# ---------------------------------------------------------------------------

def get_sources(
    db: AdminDB,
    *,
    trusted_only: bool = False,
    blocked_only: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_sources"):
            return []
        query = """
            SELECT ns.id, ns.source_name, ns.source_type, ns.category,
                   ns.is_enabled, ns.rss_url, ns.fetch_interval_seconds,
                   ns.last_fetch_utc, ns.last_success_utc, ns.last_error,
                   ns.base_reliability_score, ns.dynamic_reliability_score,
                   ns.is_trusted, ns.is_blocked,
                   nph.status, nph.items_fetched_last_run,
                   nph.duplicate_count_last_run, nph.last_checked_utc,
                   nph.error_message
            FROM news_sources ns
        """
        has_health = _table_exists(conn, "news_provider_health")
        if has_health:
            query += (
                " LEFT JOIN news_provider_health nph"
                " ON nph.source_id = ns.id"
                " AND nph.id = (SELECT MAX(id) FROM news_provider_health WHERE source_id = ns.id)"
            )
        query += " WHERE 1 = 1"
        params: list[Any] = []
        if trusted_only:
            query += " AND ns.is_trusted = 1"
        if blocked_only:
            query += " AND ns.is_blocked = 1"
        query += " ORDER BY ns.source_type, ns.source_name LIMIT ?"
        params.append(_bounded_limit(limit, default=100, maximum=500))
        rows = conn.execute(query, params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["health_status"] = d.get("status") or (
            "DISABLED" if d.get("is_enabled", 1) == 0 else "UNKNOWN"
        )
        d["last_error"] = d.get("last_error") or d.get("error_message")
        out.append(d)
    return out


def get_health(db: AdminDB) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_provider_health"):
            return []
        rows = conn.execute(
            """
            SELECT * FROM news_provider_health nph
            WHERE nph.id = (
                SELECT MAX(id) FROM news_provider_health
                WHERE source_id = nph.source_id
            )
            ORDER BY source_id
            """
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Raw news items
# ---------------------------------------------------------------------------

def get_recent_items(
    db: AdminDB,
    *,
    since_utc: str,
    provider: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "raw_news_items"):
            return []
        query = """
            SELECT id, provider, source_name, source_domain, source_url, external_id,
                   author, title, body_snippet, published_utc, ingested_utc,
                   latency_seconds, language, is_duplicate, created_at
            FROM raw_news_items
            WHERE published_utc >= ?
        """
        params: list[Any] = [since_utc]
        if provider:
            query += " AND provider = ?"
            params.append(provider)
        query += " ORDER BY published_utc DESC LIMIT ?"
        params.append(_bounded_limit(limit, default=100, maximum=500))
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_manual_imports(db: AdminDB, *, limit: int = 50) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "raw_news_items"):
            return []
        rows = conn.execute(
            "SELECT * FROM raw_news_items "
            "WHERE provider = 'manual' "
            "ORDER BY ingested_utc DESC, id DESC LIMIT ?",
            (_bounded_limit(limit, default=50, maximum=200),),
        ).fetchall()
    return [dict(r) for r in rows]


def get_provider_stats(db: AdminDB, *, since_utc: str) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "raw_news_items"):
            return []
        rows = conn.execute(
            """
            SELECT provider,
                   COUNT(*) as item_count,
                   AVG(latency_seconds) as avg_latency_seconds,
                   MAX(ingested_utc) as last_ingested_utc
            FROM raw_news_items
            WHERE ingested_utc >= ?
            GROUP BY provider
            """,
            (since_utc,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_rt_feed(
    db: AdminDB,
    *,
    since_utc: str,
    provider: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rt_providers = ("cryptopanic", "benzinga", "reuters", "generic")
    if provider:
        rt_providers = (provider,)
    with db.connect() as conn:
        if not _table_exists(conn, "raw_news_items"):
            return []
        rows: list[dict[str, Any]] = []
        for prov in rt_providers:
            items = conn.execute(
                "SELECT id, provider, source_name, source_domain, source_url, "
                "title, body_snippet, published_utc, ingested_utc, "
                "latency_seconds, is_duplicate, language, created_at "
                "FROM raw_news_items "
                "WHERE published_utc >= ? AND provider = ? "
                "ORDER BY published_utc DESC LIMIT ?",
                (since_utc, prov, limit),
            ).fetchall()
            rows.extend([dict(r) for r in items])
    rows.sort(key=lambda r: r.get("ingested_utc", ""), reverse=True)
    return rows[:limit]


# ---------------------------------------------------------------------------
# Clusters
# ---------------------------------------------------------------------------

def get_clusters(
    db: AdminDB,
    *,
    since_utc: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_clusters"):
            return []
        rows = conn.execute(
            """
            SELECT *
            FROM news_clusters
            WHERE first_seen_utc >= ?
            ORDER BY first_seen_utc DESC
            LIMIT ?
            """,
            (since_utc, _bounded_limit(limit, default=50, maximum=200)),
        ).fetchall()
        has_narratives = _table_exists(conn, "news_narratives")
        has_signals = _table_exists(conn, "news_intelligence_signals")
        out = []
        for r in rows:
            d = dict(r)
            cluster_id = d["id"]
            if has_narratives:
                nrows = conn.execute(
                    "SELECT * FROM news_narratives WHERE cluster_id = ? "
                    "ORDER BY narrative_confidence DESC",
                    (cluster_id,),
                ).fetchall()
                d["narratives"] = [dict(n) for n in nrows]
            else:
                d["narratives"] = []
            if has_signals:
                srows = conn.execute(
                    "SELECT * FROM news_intelligence_signals WHERE cluster_id = ? "
                    "ORDER BY created_at DESC",
                    (cluster_id,),
                ).fetchall()
                d["signals"] = [dict(s) for s in srows]
            else:
                d["signals"] = []
            out.append(d)
    return out


def get_cluster_items(db: AdminDB, *, cluster_id: int) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_cluster_items"):
            return []
        if not _table_exists(conn, "raw_news_items"):
            return []
        rows = conn.execute(
            """
            SELECT rni.*, nci.similarity_score
            FROM news_cluster_items nci
            JOIN raw_news_items rni ON rni.id = nci.raw_news_item_id
            WHERE nci.cluster_id = ?
            ORDER BY rni.published_utc DESC
            """,
            (cluster_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_cluster_validation(db: AdminDB, *, cluster_id: int) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_market_reactions"):
            return []
        rows = conn.execute(
            "SELECT * FROM news_market_reactions WHERE cluster_id = ? "
            "ORDER BY created_at DESC",
            (cluster_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_duplicate_clusters(
    db: AdminDB,
    *,
    since_utc: str,
    min_sources: int = 2,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_clusters"):
            return []
        rows = conn.execute(
            """
            SELECT id, canonical_title, first_seen_utc, last_seen_utc,
                   source_count, confirmation_count,
                   first_seen_provider, conflict_flag,
                   fake_news_risk_score, market_confirmation_status
            FROM news_clusters
            WHERE first_seen_utc >= ?
              AND source_count >= ?
            ORDER BY source_count DESC, first_seen_utc DESC
            LIMIT ?
            """,
            (since_utc, min_sources, _bounded_limit(limit, default=50, maximum=200)),
        ).fetchall()
    return [dict(r) for r in rows]


def get_conflict_clusters(
    db: AdminDB,
    *,
    since_utc: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_clusters"):
            return []
        rows = conn.execute(
            """
            SELECT id, canonical_title, first_seen_utc, source_count,
                   conflict_flag, fake_news_risk_score,
                   market_confirmation_status, first_seen_provider
            FROM news_clusters
            WHERE first_seen_utc >= ?
              AND conflict_flag = 1
            ORDER BY first_seen_utc DESC
            LIMIT ?
            """,
            (since_utc, _bounded_limit(limit, default=50, maximum=200)),
        ).fetchall()
    return [dict(r) for r in rows]


def get_market_confirmations(
    db: AdminDB,
    *,
    since_utc: str,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_clusters"):
            return []
        where_status = "AND market_confirmation_status = ?" if status else ""
        params: list[Any] = [since_utc]
        if status:
            params.append(status)
        params.append(_bounded_limit(limit, default=50, maximum=200))
        rows = conn.execute(
            f"""
            SELECT id, canonical_title, first_seen_utc,
                   market_confirmation_status, fake_news_risk_score,
                   confirmation_count, conflict_flag
            FROM news_clusters
            WHERE first_seen_utc >= ?
              AND market_confirmation_status IS NOT NULL
              {where_status}
            ORDER BY first_seen_utc DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Narratives
# ---------------------------------------------------------------------------

def get_active_narratives(
    db: AdminDB,
    *,
    since_utc: str,
    narrative_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_narratives"):
            return []
        if not _table_exists(conn, "news_clusters"):
            return []
        query = """
            SELECT nn.*, nc.canonical_title, nc.first_seen_utc, nc.last_seen_utc,
                   nc.source_count, nc.is_manipulation_suspect
            FROM news_narratives nn
            JOIN news_clusters nc ON nc.id = nn.cluster_id
            WHERE nc.first_seen_utc >= ?
        """
        params: list[Any] = [since_utc]
        if narrative_type:
            query += " AND nn.narrative_type = ?"
            params.append(narrative_type)
        query += " ORDER BY nc.first_seen_utc DESC LIMIT ?"
        params.append(_bounded_limit(limit, default=50, maximum=500))
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_narrative_effectiveness(db: AdminDB, *, limit: int = 50) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "narrative_effectiveness_scores"):
            return []
        rows = conn.execute(
            "SELECT * FROM narrative_effectiveness_scores "
            "ORDER BY avg_impact_score DESC LIMIT ?",
            (_bounded_limit(limit, default=50, maximum=200),),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Data quality + signals
# ---------------------------------------------------------------------------

def get_data_quality_summary(db: AdminDB) -> dict[str, Any]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_clusters"):
            return {
                "total_clusters": 0,
                "valid_clusters": 0,
                "spam_clusters": 0,
                "manipulation_suspects": 0,
                "by_status": [],
            }
        rows = conn.execute(
            "SELECT data_quality_status, COUNT(*) as count "
            "FROM news_clusters GROUP BY data_quality_status"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) FROM news_clusters").fetchone()[0]
        valid = conn.execute(
            "SELECT COUNT(*) FROM news_clusters WHERE is_valid_signal = 1"
        ).fetchone()[0]
        spam = conn.execute(
            "SELECT COUNT(*) FROM news_clusters WHERE spam_score >= 0.45"
        ).fetchone()[0]
        manip = conn.execute(
            "SELECT COUNT(*) FROM news_clusters WHERE manipulation_flag IS NOT NULL"
        ).fetchone()[0]
    return {
        "total_clusters": total,
        "valid_clusters": valid,
        "spam_clusters": spam,
        "manipulation_suspects": manip,
        "by_status": [dict(r) for r in rows],
    }


def get_active_signals(
    db: AdminDB,
    *,
    symbol: str | None = None,
    since_utc: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_intelligence_signals"):
            return []
        query = (
            "SELECT * FROM news_intelligence_signals "
            "WHERE should_affect_trading = 0"
        )
        params: list[Any] = []
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if since_utc:
            query += " AND created_at >= ?"
            params.append(since_utc)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(_bounded_limit(limit, default=50, maximum=200))
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_signal_stats(db: AdminDB, *, since_utc: str) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_intelligence_signals"):
            return []
        rows = conn.execute(
            """
            SELECT symbol, signal_type,
                   COUNT(*) as signal_count,
                   AVG(confidence_score) as avg_confidence,
                   AVG(is_valid_signal) as valid_ratio,
                   SUM(CASE WHEN validation_status = 'PENDING_MARKET_VALIDATION'
                            THEN 1 ELSE 0 END) as pending_validation_count,
                   SUM(CASE WHEN validation_status = 'VALIDATED'
                            THEN 1 ELSE 0 END) as validated_count,
                   SUM(CASE WHEN validation_status = 'INVALIDATED'
                            THEN 1 ELSE 0 END) as invalidated_count,
                   MAX(created_at) as last_signal_utc
            FROM news_intelligence_signals
            WHERE created_at >= ?
              AND should_affect_trading = 0
            GROUP BY symbol, signal_type
            ORDER BY last_signal_utc DESC
            """,
            (since_utc,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Validations
# ---------------------------------------------------------------------------

def get_recent_validations(
    db: AdminDB,
    *,
    since_utc: str,
    symbol: str | None = None,
    false_only: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_market_reactions"):
            return []
        conditions = ["nmr.created_at >= ?"]
        params: list[Any] = [since_utc]
        if symbol:
            conditions.append("nmr.symbol = ?")
            params.append(symbol)
        if false_only:
            conditions.append("nmr.is_false_signal = 1")
        where = " AND ".join(conditions)
        has_clusters = _table_exists(conn, "news_clusters")
        if has_clusters:
            query = f"""
                SELECT nmr.*,
                       nc.canonical_title, nc.first_seen_utc,
                       nc.data_quality_status, nc.manipulation_flag
                FROM news_market_reactions nmr
                LEFT JOIN news_clusters nc ON nc.id = nmr.cluster_id
                WHERE {where}
                ORDER BY nmr.created_at DESC LIMIT ?
            """
        else:
            query = (
                f"SELECT * FROM news_market_reactions nmr "
                f"WHERE {where} ORDER BY nmr.created_at DESC LIMIT ?"
            )
        params.append(_bounded_limit(limit, default=100, maximum=500))
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_validation_summary(db: AdminDB) -> dict[str, Any]:
    with db.connect() as conn:
        if not _table_exists(conn, "news_market_reactions"):
            return {
                "total_validations": 0,
                "correct_count": 0,
                "incorrect_count": 0,
                "false_signal_count": 0,
                "correct_pct": 0.0,
                "incorrect_pct": 0.0,
                "false_signal_pct": 0.0,
                "avg_impact_score": 0.0,
                "avg_effectiveness_score": 0.0,
                "by_latency_category": [],
                "by_false_reason": [],
            }
        total = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions"
        ).fetchone()[0]
        correct = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions "
            "WHERE sentiment_accuracy = 'CORRECT'"
        ).fetchone()[0]
        incorrect = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions "
            "WHERE sentiment_accuracy = 'INCORRECT'"
        ).fetchone()[0]
        false_ct = conn.execute(
            "SELECT COUNT(*) FROM news_market_reactions WHERE is_false_signal = 1"
        ).fetchone()[0]
        avg_impact = conn.execute(
            "SELECT AVG(impact_score) FROM news_market_reactions"
        ).fetchone()[0] or 0.0
        avg_eff = conn.execute(
            "SELECT AVG(signal_effectiveness_score) FROM news_market_reactions"
        ).fetchone()[0] or 0.0
        by_latency = conn.execute(
            "SELECT reaction_latency_category, COUNT(*) as count "
            "FROM news_market_reactions GROUP BY reaction_latency_category"
        ).fetchall()
        by_false = conn.execute(
            "SELECT false_signal_reason, COUNT(*) as count "
            "FROM news_market_reactions WHERE is_false_signal = 1 "
            "GROUP BY false_signal_reason"
        ).fetchall()
    n = max(1, total)
    return {
        "total_validations": total,
        "correct_count": correct,
        "incorrect_count": incorrect,
        "false_signal_count": false_ct,
        "correct_pct": round(correct / n * 100, 1),
        "incorrect_pct": round(incorrect / n * 100, 1),
        "false_signal_pct": round(false_ct / n * 100, 1),
        "avg_impact_score": round(avg_impact, 4),
        "avg_effectiveness_score": round(avg_eff, 4),
        "by_latency_category": [dict(r) for r in by_latency],
        "by_false_reason": [dict(r) for r in by_false],
    }


# ---------------------------------------------------------------------------
# RT provider status
# ---------------------------------------------------------------------------

def get_rt_provider_status(db: AdminDB) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "real_time_news_provider_status"):
            return []
        rows = conn.execute(
            "SELECT * FROM real_time_news_provider_status ORDER BY provider"
        ).fetchall()
    return [dict(r) for r in rows]
