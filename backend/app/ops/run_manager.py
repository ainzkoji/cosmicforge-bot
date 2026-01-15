from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.core.config import settings
from app.persistence.db import DB


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_config_snapshot() -> Dict[str, Any]:
    snap = settings.model_dump()
    # mask secrets
    for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET"):
        if k in snap:
            snap[k] = "***"
    return snap


@dataclass(frozen=True)
class RunInfo:
    run_id: str
    started_at: str
    mode: str
    status: str


class RunManager:
    """
    Robust run lifecycle + stats management.
    - Run is persisted in DB.
    - Stats are derived from events table (source of truth).
    """

    def __init__(self, db: Optional[DB] = None) -> None:
        self.db = db or DB()

    def start(self) -> RunInfo:
        run_id = str(uuid.uuid4())
        started_at = utc_now_iso()
        mode = settings.EXECUTION_MODE

        config_json = json.dumps(safe_config_snapshot(), ensure_ascii=False)

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (run_id, started_at, stopped_at, mode, interval_seconds, max_symbols, config_json, status)
                VALUES (?, ?, NULL, ?, ?, ?, ?, 'RUNNING')
                """,
                (
                    run_id,
                    started_at,
                    mode,
                    self._interval_seconds(settings.DEFAULT_INTERVAL),
                    settings.MAX_SYMBOLS,
                    config_json,
                ),
            )

            conn.execute(
                """
                INSERT INTO run_summary (run_id, cycles, trades, opens, closes, errors, last_event_at, updated_at)
                VALUES (?, 0, 0, 0, 0, 0, NULL, ?)
                """,
                (run_id, utc_now_iso()),
            )

        return RunInfo(
            run_id=run_id, started_at=started_at, mode=mode, status="RUNNING"
        )

    def stop(self, run_id: str, status: str = "STOPPED") -> None:
        stopped_at = utc_now_iso()

        # ✅ SAME LOGIC, just avoid nested DB connections:
        # 1) update run row
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE runs SET stopped_at = ?, status = ? WHERE run_id = ?",
                (stopped_at, status, run_id),
            )

        # 2) refresh summary after the write transaction is closed
        self.refresh_summary(run_id)

    def refresh_summary(self, run_id: str) -> None:
        """
        Recompute run_summary FROM events table (truth).
        This is robust even if the app crashes or restarts.
        """
        with self.db.connect() as conn:
            # cycles: count of distinct cycle_id for this run (if you log cycle_id)
            cycles = conn.execute(
                """
                SELECT COUNT(DISTINCT cycle_id) AS cnt
                FROM events
                WHERE run_id = ? AND cycle_id IS NOT NULL AND cycle_id != ''
                """,
                (run_id,),
            ).fetchone()["cnt"]

            # basic event counts (tweak these mappings as your event_type/action naming evolves)
            trades = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM events
                WHERE run_id = ?
                  AND event_type IN ('TRADE', 'ORDER', 'EXECUTION')
                """,
                (run_id,),
            ).fetchone()["cnt"]

            opens = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM events
                WHERE run_id = ?
                  AND (action LIKE 'OPEN_%' OR action IN ('OPEN_LONG','OPEN_SHORT'))
                """,
                (run_id,),
            ).fetchone()["cnt"]

            closes = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM events
                WHERE run_id = ?
                  AND (action LIKE 'CLOSE_%' OR action IN ('CLOSE_LONG','CLOSE_SHORT'))
                """,
                (run_id,),
            ).fetchone()["cnt"]

            errors = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM events
                WHERE run_id = ?
                  AND event_type IN ('ERROR', 'EXCEPTION')
                """,
                (run_id,),
            ).fetchone()["cnt"]

            last_event_at_row = conn.execute(
                """
                SELECT MAX(timestamp_utc) AS last_ts
                FROM events
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            last_event_at = last_event_at_row["last_ts"] if last_event_at_row else None

            conn.execute(
                """
                UPDATE run_summary
                SET cycles = ?, trades = ?, opens = ?, closes = ?, errors = ?,
                    last_event_at = ?, updated_at = ?
                WHERE run_id = ?
                """,
                (
                    int(cycles or 0),
                    int(trades or 0),
                    int(opens or 0),
                    int(closes or 0),
                    int(errors or 0),
                    last_event_at,
                    utc_now_iso(),
                    run_id,
                ),
            )

    def get_current(self) -> Optional[Dict[str, Any]]:
        """
        Current = latest RUNNING run.
        """
        with self.db.connect() as conn:
            run = conn.execute(
                "SELECT * FROM runs WHERE status = 'RUNNING' ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if not run:
                return None
            run_id = run["run_id"]

        # ✅ refresh outside the previous connection
        self.refresh_summary(run_id)

        with self.db.connect() as conn:
            run = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            summary = conn.execute(
                "SELECT * FROM run_summary WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return {
                "run": dict(run) if run else None,
                "summary": dict(summary) if summary else None,
            }

    def get_last(self) -> Optional[Dict[str, Any]]:
        with self.db.connect() as conn:
            run = conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if not run:
                return None
            run_id = run["run_id"]

        # ✅ refresh outside the previous connection
        self.refresh_summary(run_id)

        with self.db.connect() as conn:
            run = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            summary = conn.execute(
                "SELECT * FROM run_summary WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return {
                "run": dict(run) if run else None,
                "summary": dict(summary) if summary else None,
            }

    @staticmethod
    def _interval_seconds(interval: str) -> int:
        """
        Convert DEFAULT_INTERVAL like '1m', '5m', '15m', '1h' to seconds.
        """
        s = (interval or "1m").strip().lower()
        if s.endswith("m"):
            return int(s[:-1]) * 60
        if s.endswith("h"):
            return int(s[:-1]) * 60
        if s.endswith("s"):
            return int(s[:-1])
        # fallback
        return 60
