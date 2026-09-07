from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from shared_lib.persistence.db import DB


def upsert_ml_runtime_status(
    db: DB,
    *,
    scope_key: str = "global",
    bot_instance_id: str | None = None,
    enabled: bool,
    shadow_mode: bool,
    loaded: bool,
    model_version: str | None,
    model_path: str | None,
    metadata_path: str | None,
    encoders_path: str | None,
    threshold: float | None,
    hard_block_floor: float | None,
    contract_version: str | None,
    schema_hash: str | None,
    last_score_timestamp: str | None,
    load_error: str | None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_runtime_status (
                scope_key TEXT PRIMARY KEY,
                bot_instance_id TEXT,
                enabled INTEGER NOT NULL DEFAULT 0,
                shadow_mode INTEGER NOT NULL DEFAULT 0,
                loaded INTEGER NOT NULL DEFAULT 0,
                model_version TEXT,
                model_path TEXT,
                metadata_path TEXT,
                encoders_path TEXT,
                threshold REAL,
                hard_block_floor REAL,
                contract_version TEXT,
                schema_hash TEXT,
                last_score_timestamp TEXT,
                last_update_timestamp TEXT NOT NULL,
                load_error TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO ml_runtime_status(
                scope_key,
                bot_instance_id,
                enabled,
                shadow_mode,
                loaded,
                model_version,
                model_path,
                metadata_path,
                encoders_path,
                threshold,
                hard_block_floor,
                contract_version,
                schema_hash,
                last_score_timestamp,
                last_update_timestamp,
                load_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(scope_key) DO UPDATE SET
                bot_instance_id=excluded.bot_instance_id,
                enabled=excluded.enabled,
                shadow_mode=excluded.shadow_mode,
                loaded=excluded.loaded,
                model_version=excluded.model_version,
                model_path=excluded.model_path,
                metadata_path=excluded.metadata_path,
                encoders_path=excluded.encoders_path,
                threshold=excluded.threshold,
                hard_block_floor=excluded.hard_block_floor,
                contract_version=excluded.contract_version,
                schema_hash=excluded.schema_hash,
                last_score_timestamp=excluded.last_score_timestamp,
                last_update_timestamp=excluded.last_update_timestamp,
                load_error=excluded.load_error
            """,
            (
                scope_key,
                bot_instance_id,
                1 if enabled else 0,
                1 if shadow_mode else 0,
                1 if loaded else 0,
                model_version,
                model_path,
                metadata_path,
                encoders_path,
                threshold,
                hard_block_floor,
                contract_version,
                schema_hash,
                last_score_timestamp,
                now,
                load_error,
            ),
        )


def latest_ml_runtime_status(db: DB, *, scope_key: str = "global") -> dict[str, Any] | None:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM ml_runtime_status WHERE scope_key = ?",
            (scope_key,),
        ).fetchone()
    return dict(row) if row else None
