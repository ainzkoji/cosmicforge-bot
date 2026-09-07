from __future__ import annotations

from typing import Any

from app.persistence.db import AdminDB


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def list_users_read_only(db: AdminDB, *, status: str | None = None, limit: int = 50) -> dict[str, Any]:
    normalized_limit = max(1, min(int(limit or 50), 100))
    with db.connect() as conn:
        if not _table_exists(conn, "users"):
            return {"users": [], "count": 0}

        has_bot_instances = _table_exists(conn, "bot_instances")
        active_bots_sql = (
            "(SELECT COUNT(*) FROM bot_instances b WHERE b.user_id = u.id AND b.status = 'active')"
            if has_bot_instances
            else "0"
        )
        total_bots_sql = (
            "(SELECT COUNT(*) FROM bot_instances b WHERE b.user_id = u.id)"
            if has_bot_instances
            else "0"
        )
        query = f"""
            SELECT u.id, u.email, u.status, u.role, u.created_at, u.last_login_at,
                   COALESCE(u.total_trades, 0) AS total_trades,
                   COALESCE(u.total_commission, 0) AS total_commission,
                   CASE WHEN COALESCE(u.is_verified, 0) THEN 'verified' ELSE 'unverified' END AS verification_status,
                   {active_bots_sql} AS active_bots,
                   {total_bots_sql} AS total_bots
            FROM users u
        """
        params: list[Any] = []
        if status:
            query += " WHERE u.status = ?"
            params.append(status)
        query += " ORDER BY u.created_at DESC LIMIT ?"
        params.append(normalized_limit)
        rows = conn.execute(query, params).fetchall()

    users = [dict(row) for row in rows]
    return {"users": users, "count": len(users)}
