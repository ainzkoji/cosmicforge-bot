from __future__ import annotations

import json
from typing import Any, Dict, Optional

from app.persistence.db import DB, utc_now_iso


class EventLogger:
    """
    Minimal event logger that writes to the existing `events` table.
    """
    def __init__(self, db: DB):
        self.db = db

    def log(
        self,
        *,
        event_type: str,
        run_id: Optional[str] = None,
        cycle_id: Optional[str] = None,
        symbol: Optional[str] = None,
        action: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = json.dumps(details or {}, separators=(",", ":"), ensure_ascii=False)

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO events (timestamp_utc, run_id, cycle_id, symbol, event_type, action, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (utc_now_iso(), run_id, cycle_id, symbol, event_type, action, payload),
            )
