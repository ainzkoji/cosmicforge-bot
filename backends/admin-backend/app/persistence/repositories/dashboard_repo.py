from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence.db import AdminDB


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _scalar(conn, query: str, params: tuple[Any, ...] = (), default: Any = 0) -> Any:
    row = conn.execute(query, params).fetchone()
    if not row:
        return default
    value = row[0]
    return default if value is None else value


def get_dashboard_stats(db: AdminDB) -> dict[str, Any]:
    with db.connect() as conn:
        total_users = _scalar(conn, "SELECT COUNT(*) FROM users") if _table_exists(conn, "users") else 0
        active_subscriptions = (
            _scalar(conn, "SELECT COUNT(*) FROM subscriptions WHERE status = 'active'")
            if _table_exists(conn, "subscriptions")
            else 0
        )
        total_revenue = (
            _scalar(conn, "SELECT SUM(total_revenue) FROM revenue_snapshots", default=0)
            if _table_exists(conn, "revenue_snapshots")
            else 0
        )
        platform_trades = (
            _scalar(conn, "SELECT SUM(total_trades) FROM users", default=0)
            if _table_exists(conn, "users")
            else 0
        )

    return {
        "total_users": int(total_users or 0),
        "active_subscriptions": int(active_subscriptions or 0),
        "total_revenue": float(total_revenue or 0),
        "platform_trades": int(platform_trades or 0),
    }


def get_revenue_overview(db: AdminDB, timeframe: str = "12m") -> dict[str, list[dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    if timeframe == "30d":
        cutoff_date = (now - timedelta(days=30)).date().isoformat()
    elif timeframe == "6m":
        cutoff_date = (now - timedelta(days=180)).date().isoformat()
    else:
        cutoff_date = (now - timedelta(days=365)).date().isoformat()

    with db.connect() as conn:
        if not _table_exists(conn, "revenue_snapshots"):
            return {"data": []}
        rows = conn.execute(
            """
            SELECT date, subscription_revenue, commission_revenue, total_revenue
            FROM revenue_snapshots
            WHERE date >= ?
            ORDER BY date ASC
            """,
            (cutoff_date,),
        ).fetchall()

    return {"data": [dict(row) for row in rows]}


def get_top_trading_pairs(db: AdminDB, limit: int = 5) -> dict[str, list[dict[str, Any]]]:
    with db.connect() as conn:
        if not _table_exists(conn, "trade_fills"):
            return {"data": []}
        rows = conn.execute(
            """
            SELECT symbol, COUNT(*) AS trade_count
            FROM trade_fills
            WHERE (account_id IS NULL OR account_id != 'backfill')
              AND (initiator_type IS NULL OR initiator_type != 'SHADOW')
              AND symbol IS NOT NULL
              AND symbol != ''
            GROUP BY symbol
            ORDER BY trade_count DESC
            LIMIT ?
            """,
            (max(1, min(int(limit or 5), 10)),),
        ).fetchall()

    data = [dict(row) for row in rows]
    total = sum(int(item.get("trade_count") or 0) for item in data)
    for item in data:
        trade_count = int(item.get("trade_count") or 0)
        item["trade_count"] = trade_count
        item["percentage"] = round((trade_count / total * 100), 1) if total > 0 else 0
    return {"data": data}
