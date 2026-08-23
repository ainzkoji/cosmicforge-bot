from __future__ import annotations

import json
from typing import Any

from app.persistence.db import AdminDB


DEFAULT_LIMIT = 100
MAX_LIMIT = 500


def _bounded_limit(limit: int | None) -> int:
    try:
        parsed = int(limit or DEFAULT_LIMIT)
    except (TypeError, ValueError):
        parsed = DEFAULT_LIMIT
    return max(1, min(parsed, MAX_LIMIT))


def _load_json(raw: Any, default: Any) -> Any:
    if raw is None or raw == "":
        return default
    try:
        return json.loads(str(raw))
    except Exception:
        return default


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def list_webhooks(db: AdminDB, *, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "tradingview_webhooks"):
            return []
        rows = conn.execute(
            """
            SELECT id, bot_id, name, mode, is_enabled, allowed_symbols_json,
                   allowed_actions_json, max_alert_age_seconds,
                   rate_limit_per_minute, created_at, updated_at, last_used_at
            FROM tradingview_webhooks
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (_bounded_limit(limit),),
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["allowed_symbols"] = _load_json(item.pop("allowed_symbols_json", None), None)
        item["allowed_actions"] = _load_json(item.pop("allowed_actions_json", None), [])
        items.append(item)
    return items


def list_alerts(db: AdminDB, *, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "tradingview_alerts"):
            return []
        rows = conn.execute(
            """
            SELECT *
            FROM tradingview_alerts
            ORDER BY received_at DESC, id DESC
            LIMIT ?
            """,
            (_bounded_limit(limit),),
        ).fetchall()
    return [dict(row) for row in rows]


def list_decisions(db: AdminDB, *, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "tradingview_signal_decisions"):
            return []
        rows = conn.execute(
            """
            SELECT *
            FROM tradingview_signal_decisions
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (_bounded_limit(limit),),
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["normalized_signal"] = _load_json(item.get("normalized_signal_json"), {})
        items.append(item)
    return items


def list_external_signal_queue(db: AdminDB, *, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "external_signal_queue"):
            return []
        rows = conn.execute(
            """
            SELECT *
            FROM external_signal_queue
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (_bounded_limit(limit),),
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["result_json"] = _load_json(item.get("result"), None)
        items.append(item)
    return items


def get_processor_heartbeat(db: AdminDB, bot_instance_id: str) -> dict[str, Any] | None:
    with db.connect() as conn:
        if not _table_exists(conn, "tradingview_processor_heartbeat"):
            return None
        row = conn.execute(
            """
            SELECT *
            FROM tradingview_processor_heartbeat
            WHERE bot_instance_id = ?
            """,
            (bot_instance_id,),
        ).fetchone()
    return dict(row) if row else None


def list_processor_heartbeats(db: AdminDB, *, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "tradingview_processor_heartbeat"):
            return []
        rows = conn.execute(
            """
            SELECT *
            FROM tradingview_processor_heartbeat
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (_bounded_limit(limit),),
        ).fetchall()
    return [dict(row) for row in rows]
