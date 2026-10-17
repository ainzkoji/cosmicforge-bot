"""Section H Step 7.4: the persistent schema of staged market activation.

One append-only, hash-chained table, ``market_activation_events``: every PAPER / DEMO / LIVE transition that
was approved, refused or blocked for want of policy, and every revocation, per (market, family). The current
stage is the fold of the records; no other table or flag holds it.

Additive and idempotent: a new table and its triggers only. No existing table, row or credential is touched.
An UPDATE or a DELETE on the table aborts (``append_only_triggers``).
"""
from __future__ import annotations

from typing import Any, Tuple

from shared_lib.persistence.cati_platform_schema import append_only_triggers

SCHEMA_VERSION = "1.0.0"
ACTIVATION_EVENT_TABLE = "market_activation_events"

DDL: Tuple[str, ...] = (
    f"""CREATE TABLE IF NOT EXISTS {ACTIVATION_EVENT_TABLE} (
        seq INTEGER PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE,
        market_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        from_stage TEXT,
        to_stage TEXT,
        actor_ref TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        payload TEXT NOT NULL,
        prev_hash TEXT NOT NULL,
        event_hash TEXT NOT NULL UNIQUE
    )""",
    f"CREATE INDEX IF NOT EXISTS idx_{ACTIVATION_EVENT_TABLE}_scope ON {ACTIVATION_EVENT_TABLE}(market_id, family_id, seq)",
    *append_only_triggers(ACTIVATION_EVENT_TABLE, "market_activation_events"),
)


def ensure_on_connection(conn: Any) -> None:
    for statement in DDL:
        conn.execute(statement)


def ensure_markets_schema(db: Any) -> None:
    with db.connect() as conn:
        ensure_on_connection(conn)


__all__ = ["SCHEMA_VERSION", "ACTIVATION_EVENT_TABLE", "DDL", "ensure_on_connection", "ensure_markets_schema"]
