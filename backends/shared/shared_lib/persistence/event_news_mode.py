from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from shared_lib.persistence.db import DB


MODE_DISABLED = "DISABLED"
MODE_SHADOW = "SHADOW"
MODE_ADVISORY = "ADVISORY"
MODE_RISK_LITE = "RISK_LITE"
MODE_RISK_GUARD = "RISK_GUARD"
MODE_LIVE_SIGNAL_ELIGIBLE = "LIVE_SIGNAL_ELIGIBLE"

ACTION_NONE = "NONE"
ACTION_ANNOTATE_ONLY = "ANNOTATE_ONLY"
ACTION_CONFIDENCE_PENALTY = "CONFIDENCE_PENALTY"
ACTION_SIZE_REDUCTION = "SIZE_REDUCTION"
ACTION_DELAY_ENTRY = "DELAY_ENTRY"
ACTION_SYMBOL_COOLDOWN = "SYMBOL_COOLDOWN"
ACTION_HARD_BLOCK_ENTRY = "HARD_BLOCK_ENTRY"
ACTION_CANDIDATE_SIGNAL = "CANDIDATE_SIGNAL"
ACTION_OPEN_POSITION = "OPEN_POSITION"
ACTION_CLOSE_POSITION = "CLOSE_POSITION"

DECISION_INITIALIZE = "INITIALIZE"
DECISION_PROMOTE = "PROMOTE"
DECISION_DEMOTE = "DEMOTE"
DECISION_HOLD = "HOLD"
DECISION_DISABLE = "DISABLE"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True)


def _load(raw: Any, default: Any) -> Any:
    try:
        if raw is None or raw == "":
            return default
        return json.loads(str(raw))
    except Exception:
        return default


def ensure_event_news_mode_schema(db: DB | None = None) -> None:
    db = db or DB()
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_news_runtime_mode (
                id INTEGER PRIMARY KEY,
                current_mode TEXT NOT NULL,
                previous_mode TEXT,
                max_allowed_action TEXT NOT NULL,
                readiness_score REAL NOT NULL DEFAULT 0,
                safety_status TEXT NOT NULL DEFAULT 'UNKNOWN',
                auto_promotion_enabled INTEGER NOT NULL DEFAULT 1,
                auto_demotion_enabled INTEGER NOT NULL DEFAULT 1,
                reason TEXT,
                failed_criteria_json TEXT NOT NULL DEFAULT '[]',
                passed_criteria_json TEXT NOT NULL DEFAULT '[]',
                promoted_at TEXT,
                demoted_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_news_mode_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_type TEXT NOT NULL,
                from_mode TEXT,
                to_mode TEXT,
                readiness_score REAL NOT NULL DEFAULT 0,
                safety_score REAL NOT NULL DEFAULT 0,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                evaluation_window_start TEXT,
                evaluation_window_end TEXT,
                reason TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_news_mode_decisions_created "
            "ON event_news_mode_decisions(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_news_mode_decisions_type "
            "ON event_news_mode_decisions(decision_type)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_news_influence_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trace_id TEXT,
                symbol TEXT,
                mode TEXT NOT NULL,
                requested_action TEXT NOT NULL,
                applied_action TEXT NOT NULL,
                reason TEXT,
                confidence REAL,
                reliability_score REAL,
                fake_news_risk_score REAL,
                conflict_flag INTEGER NOT NULL DEFAULT 0,
                market_confirmation_status TEXT,
                size_multiplier REAL NOT NULL DEFAULT 1.0,
                confidence_penalty REAL NOT NULL DEFAULT 0.0,
                delay_seconds INTEGER NOT NULL DEFAULT 0,
                cooldown_until TEXT,
                expires_at TEXT,
                source_context_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_news_influence_created "
            "ON event_news_influence_decisions(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_news_influence_trace "
            "ON event_news_influence_decisions(trace_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_news_influence_symbol "
            "ON event_news_influence_decisions(symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_news_influence_action "
            "ON event_news_influence_decisions(applied_action)"
        )
        initialize_event_news_mode(conn)


def initialize_event_news_mode(conn: Any) -> None:
    now = utc_now_iso()
    inserted = conn.execute(
        """
        INSERT OR IGNORE INTO event_news_runtime_mode (
            id, current_mode, previous_mode, max_allowed_action,
            readiness_score, safety_status, auto_promotion_enabled,
            auto_demotion_enabled, reason, failed_criteria_json,
            passed_criteria_json, promoted_at, demoted_at, created_at, updated_at
        )
        VALUES (
            1, ?, NULL, ?, 0, 'UNKNOWN', 1, 1,
            'Initial safe shadow mode', '[]', '[]', NULL, NULL, ?, ?
        )
        """,
        (MODE_SHADOW, ACTION_ANNOTATE_ONLY, now, now),
    ).rowcount
    if inserted:
        conn.execute(
            """
            INSERT INTO event_news_mode_decisions (
                decision_type, from_mode, to_mode, readiness_score,
                safety_score, evidence_json, evaluation_window_start,
                evaluation_window_end, reason, created_at
            )
            VALUES (?, NULL, ?, 0, 0, ?, ?, ?, ?, ?)
            """,
            (
                DECISION_INITIALIZE,
                MODE_SHADOW,
                _dump({"max_allowed_action": ACTION_ANNOTATE_ONLY}),
                now,
                now,
                "Initial safe shadow mode",
                now,
            ),
        )


def get_event_news_runtime_mode(db: DB | None = None) -> dict[str, Any]:
    db = db or DB()
    ensure_event_news_mode_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM event_news_runtime_mode WHERE id = 1"
        ).fetchone()
    data = dict(row) if row else {}
    data["failed_criteria"] = _load(data.get("failed_criteria_json"), [])
    data["passed_criteria"] = _load(data.get("passed_criteria_json"), [])
    return data


def get_recent_event_news_mode_decisions(db: DB | None = None, *, limit: int = 20) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_event_news_mode_schema(db)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM event_news_mode_decisions
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["evidence"] = _load(item.get("evidence_json"), {})
        out.append(item)
    return out


def get_recent_event_news_influence_decisions(
    db: DB | None = None,
    *,
    limit: int = 50,
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_event_news_mode_schema(db)
    with db.connect() as conn:
        if symbol:
            rows = conn.execute(
                """
                SELECT *
                FROM event_news_influence_decisions
                WHERE UPPER(symbol) = UPPER(?)
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (symbol, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM event_news_influence_decisions
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["source_context"] = _load(item.get("source_context_json"), {})
        out.append(item)
    return out


def insert_event_news_influence_decision(
    db: DB,
    *,
    trace_id: str | None,
    symbol: str | None,
    mode: str,
    requested_action: str,
    applied_action: str,
    reason: str,
    confidence: float | None,
    reliability_score: float | None,
    fake_news_risk_score: float | None,
    conflict_flag: bool,
    market_confirmation_status: str | None,
    size_multiplier: float,
    confidence_penalty: float,
    delay_seconds: int,
    cooldown_until: str | None,
    expires_at: str | None,
    source_context: dict[str, Any],
) -> dict[str, Any]:
    ensure_event_news_mode_schema(db)
    created_at = utc_now_iso()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO event_news_influence_decisions (
                trace_id, symbol, mode, requested_action, applied_action, reason,
                confidence, reliability_score, fake_news_risk_score, conflict_flag,
                market_confirmation_status, size_multiplier, confidence_penalty,
                delay_seconds, cooldown_until, expires_at, source_context_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trace_id,
                symbol,
                mode,
                requested_action,
                applied_action,
                reason,
                confidence,
                reliability_score,
                fake_news_risk_score,
                1 if conflict_flag else 0,
                market_confirmation_status,
                size_multiplier,
                confidence_penalty,
                delay_seconds,
                cooldown_until,
                expires_at,
                _dump(source_context),
                created_at,
            ),
        )
        row_id = cur.lastrowid
    return {
        "id": row_id,
        "trace_id": trace_id,
        "symbol": symbol,
        "mode": mode,
        "requested_action": requested_action,
        "applied_action": applied_action,
        "reason": reason,
        "confidence": confidence,
        "reliability_score": reliability_score,
        "fake_news_risk_score": fake_news_risk_score,
        "conflict_flag": bool(conflict_flag),
        "market_confirmation_status": market_confirmation_status,
        "size_multiplier": size_multiplier,
        "confidence_penalty": confidence_penalty,
        "delay_seconds": delay_seconds,
        "cooldown_until": cooldown_until,
        "expires_at": expires_at,
        "created_at": created_at,
    }


def get_event_news_influence_summary(
    db: DB | None = None,
    *,
    hours: int = 6,
) -> dict[str, Any]:
    db = db or DB()
    ensure_event_news_mode_schema(db)
    with db.connect() as conn:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
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
        total = conn.execute(
            "SELECT COUNT(*) AS cnt FROM event_news_influence_decisions WHERE created_at >= ?",
            (since,),
        ).fetchone()
        try:
            eligible = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM decision_traces
                WHERE ts >= ?
                  AND (
                    UPPER(COALESCE(intended_action, '')) IN ('OPEN_LONG', 'OPEN_SHORT')
                    OR UPPER(COALESCE(signal, '')) IN ('BUY', 'SELL')
                  )
                """,
                (since,),
            ).fetchone()
        except Exception:
            eligible = {"cnt": 0}
        forbidden = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM event_news_influence_decisions
            WHERE created_at >= ?
              AND applied_action IN (
                'SYMBOL_COOLDOWN', 'HARD_BLOCK_ENTRY', 'CANDIDATE_SIGNAL',
                'OPEN_POSITION', 'CLOSE_POSITION'
              )
            """,
            (since,),
        ).fetchone()
        hard_blocks = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM event_news_influence_decisions
            WHERE created_at >= ? AND applied_action='HARD_BLOCK_ENTRY'
            """,
            (since,),
        ).fetchone()
    actions = {str(row["applied_action"]): dict(row) for row in rows}
    total_count = int((total["cnt"] if total else 0) or 0)
    eligible_count = int((eligible["cnt"] if eligible else 0) or 0)
    size_count = int(actions.get(ACTION_SIZE_REDUCTION, {}).get("cnt") or 0)
    delay_count = int(actions.get(ACTION_DELAY_ENTRY, {}).get("cnt") or 0)
    avg_multiplier = actions.get(ACTION_SIZE_REDUCTION, {}).get("avg_size_multiplier")
    avg_size_reduction = (1.0 - float(avg_multiplier)) if avg_multiplier is not None else 0.0
    return {
        "hours": hours,
        "total": total_count,
        "eligible_signal_count": eligible_count,
        "actions": actions,
        "size_reduction_count": size_count,
        "delay_count": delay_count,
        "annotate_only_count": int(actions.get(ACTION_ANNOTATE_ONLY, {}).get("cnt") or 0),
        "forbidden_action_count": int((forbidden["cnt"] if forbidden else 0) or 0),
        "hard_block_count": int((hard_blocks["cnt"] if hard_blocks else 0) or 0),
        "size_reduction_pct": (size_count / total_count) if total_count else 0.0,
        "delay_pct": (delay_count / total_count) if total_count else 0.0,
        "size_reduction_signal_pct": (size_count / eligible_count) if eligible_count else 0.0,
        "delay_signal_pct": (delay_count / eligible_count) if eligible_count else 0.0,
        "avg_size_multiplier": avg_multiplier,
        "avg_size_reduction": avg_size_reduction,
        "avg_delay_seconds": actions.get(ACTION_DELAY_ENTRY, {}).get("avg_delay_seconds"),
    }


def persist_event_news_mode_decision(
    db: DB,
    *,
    decision_type: str,
    from_mode: str | None,
    to_mode: str | None,
    readiness_score: float,
    safety_score: float,
    evidence: dict[str, Any],
    evaluation_window_start: str | None,
    evaluation_window_end: str | None,
    reason: str,
    failed_criteria: list[str],
    passed_criteria: list[str],
    safety_status: str,
    auto_promotion_enabled: bool,
    auto_demotion_enabled: bool,
) -> dict[str, Any]:
    ensure_event_news_mode_schema(db)
    now = utc_now_iso()
    if to_mode == MODE_DISABLED:
        max_action = ACTION_NONE
    elif to_mode == MODE_RISK_LITE:
        max_action = ACTION_DELAY_ENTRY
    else:
        max_action = ACTION_ANNOTATE_ONLY
    promoted_at = now if decision_type == DECISION_PROMOTE else None
    demoted_at = now if decision_type in {DECISION_DEMOTE, DECISION_DISABLE} else None

    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO event_news_mode_decisions (
                decision_type, from_mode, to_mode, readiness_score,
                safety_score, evidence_json, evaluation_window_start,
                evaluation_window_end, reason, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_type,
                from_mode,
                to_mode,
                readiness_score,
                safety_score,
                _dump(evidence),
                evaluation_window_start,
                evaluation_window_end,
                reason,
                now,
            ),
        )
        decision_id = cur.lastrowid
        conn.execute(
            """
            UPDATE event_news_runtime_mode
            SET current_mode = ?,
                previous_mode = ?,
                max_allowed_action = ?,
                readiness_score = ?,
                safety_status = ?,
                auto_promotion_enabled = ?,
                auto_demotion_enabled = ?,
                reason = ?,
                failed_criteria_json = ?,
                passed_criteria_json = ?,
                promoted_at = COALESCE(?, promoted_at),
                demoted_at = COALESCE(?, demoted_at),
                updated_at = ?
            WHERE id = 1
            """,
            (
                to_mode,
                from_mode,
                max_action,
                readiness_score,
                safety_status,
                1 if auto_promotion_enabled else 0,
                1 if auto_demotion_enabled else 0,
                reason,
                _dump(failed_criteria),
                _dump(passed_criteria),
                promoted_at,
                demoted_at,
                now,
            ),
        )
    return {
        "id": decision_id,
        "decision_type": decision_type,
        "from_mode": from_mode,
        "to_mode": to_mode,
        "readiness_score": readiness_score,
        "safety_score": safety_score,
        "safety_status": safety_status,
        "reason": reason,
        "failed_criteria": failed_criteria,
        "passed_criteria": passed_criteria,
        "created_at": now,
        "max_allowed_action": max_action,
    }
