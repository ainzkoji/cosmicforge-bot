from __future__ import annotations

from typing import Any

from app.persistence.db import AdminDB


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _sum(conn, query: str, params: tuple[Any, ...] = ()) -> float:
    row = conn.execute(query, params).fetchone()
    if not row or row[0] is None:
        return 0.0
    return float(row[0] or 0.0)


def get_revenue_overview(db: AdminDB) -> dict[str, Any]:
    """Return read-only revenue analytics using only stored revenue records."""

    with db.connect() as conn:
        has_invoices = _table_exists(conn, "invoices")
        has_commission_ledger = _table_exists(conn, "commission_ledger")
        has_subscriptions = _table_exists(conn, "subscriptions")

        subscription_revenue = (
            _sum(conn, "SELECT SUM(amount) FROM invoices WHERE status = 'paid'")
            if has_invoices
            else 0.0
        )
        commission_revenue = (
            _sum(conn, "SELECT SUM(commission_amount) FROM commission_ledger")
            if has_commission_ledger
            else 0.0
        )

        revenue_by_plan: list[dict[str, Any]] = []
        if has_invoices and has_subscriptions:
            rows = conn.execute(
                """
                SELECT s.plan_id, SUM(i.amount) AS rev, COUNT(DISTINCT s.user_id) AS users
                FROM invoices i
                JOIN subscriptions s ON i.user_id = s.user_id
                WHERE i.status = 'paid'
                GROUP BY s.plan_id
                ORDER BY rev DESC
                """
            ).fetchall()
            for row in rows:
                revenue = float(row["rev"] or 0.0)
                pct = (revenue / subscription_revenue * 100) if subscription_revenue > 0 else 0.0
                revenue_by_plan.append(
                    {
                        "plan": row["plan_id"] or "Unknown",
                        "revenue": revenue,
                        "percentage": round(pct, 1),
                        "users": int(row["users"] or 0),
                    }
                )

        if not revenue_by_plan and has_subscriptions:
            rows = conn.execute(
                """
                SELECT plan_id, COUNT(*) AS cnt
                FROM subscriptions
                WHERE status = 'active'
                GROUP BY plan_id
                ORDER BY cnt DESC
                """
            ).fetchall()
            total_active = sum(int(row["cnt"] or 0) for row in rows)
            for row in rows:
                count = int(row["cnt"] or 0)
                pct = (count / total_active * 100) if total_active > 0 else 0.0
                revenue_by_plan.append(
                    {
                        "plan": row["plan_id"] or "Unknown",
                        "revenue": 0.0,
                        "percentage": round(pct, 1),
                        "users": count,
                    }
                )

    total_revenue = subscription_revenue + commission_revenue
    by_plan = {item["plan"]: item["revenue"] for item in revenue_by_plan}
    return {
        "total_revenue": total_revenue,
        "subscription_revenue": subscription_revenue,
        "commission_revenue": commission_revenue,
        "revenue_by_plan": revenue_by_plan,
        "by_plan": by_plan,
    }
