"""Bot-backend compatibility helpers for portfolio runtime counts.

The user backend owns portfolio aggregation.  This small shared query keeps
historical callers and consistency tests on the same bot-instance definition.
"""
from __future__ import annotations

import logging

from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)


def _count_running_bots(db: DB, user_id: str) -> int:
    """Count active, broker-healthy bots without failing on older schemas."""
    try:
        with db.connect() as conn:
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(bot_instances)").fetchall()
            }
            health_clause = (
                "AND (broker_health_status IS NULL OR broker_health_status = 'ok')"
                if "broker_health_status" in columns
                else ""
            )
            row = conn.execute(
                f"""SELECT COUNT(*) AS cnt FROM bot_instances
                    WHERE user_id = ? AND status IN ('active', 'error') {health_clause}""",
                (user_id,),
            ).fetchone()
            return int(row["cnt"]) if row else 0
    except Exception as exc:
        logger.debug("_count_running_bots failed: %s", exc)
        return 0
