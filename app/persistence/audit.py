# app/persistence/audit.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from app.persistence.db import DB, utc_now_iso


class Audit:
    """
    DB audit is the source of truth.
    Additionally mirrors events to logs/live_audit.jsonl so /runner/audit/tail works.
    """

    def __init__(self, db: DB, jsonl_path: str = "logs/live_audit.jsonl"):
        self.db = db
        self.jsonl_path = Path(jsonl_path)

        # ensure logs folder + file exist
        try:
            self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            self.jsonl_path.touch(exist_ok=True)
        except Exception:
            # never crash bot due to audit file issues
            pass

    # backward-compat alias (you call runner.audit.log_event in main.py in some places)
    def log_event(self, *args, **kwargs):
        return self.event(*args, **kwargs)

    def start_run(
        self, run_id: str, mode: str, interval_seconds: int, max_symbols: int
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO runs(run_id, started_at, mode, interval_seconds, max_symbols) VALUES (?,?,?,?,?)",
                (run_id, utc_now_iso(), mode, interval_seconds, max_symbols),
            )

        # also mirror to jsonl
        self._write_jsonl(
            {
                "timestamp_utc": utc_now_iso(),
                "event_type": "RUN_START",
                "run_id": run_id,
                "details": {
                    "mode": mode,
                    "interval_seconds": interval_seconds,
                    "max_symbols": max_symbols,
                },
            }
        )

    def stop_run(self, run_id: str) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE runs SET stopped_at = ? WHERE run_id = ?",
                (utc_now_iso(), run_id),
            )

        self._write_jsonl(
            {
                "timestamp_utc": utc_now_iso(),
                "event_type": "RUN_STOP",
                "run_id": run_id,
                "details": {},
            }
        )

    def event(
        self,
        event_type: str,
        run_id: Optional[str] = None,
        cycle_id: Optional[str] = None,
        symbol: Optional[str] = None,
        action: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = json.dumps(details or {}, ensure_ascii=False)

        # 1) DB (source of truth)
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO events(timestamp_utc, run_id, cycle_id, symbol, event_type, action, details_json)
                VALUES (?,?,?,?,?,?,?)
                """,
                (utc_now_iso(), run_id, cycle_id, symbol, event_type, action, payload),
            )

        # 2) JSONL mirror (for /runner/audit/tail)
        self._write_jsonl(
            {
                "timestamp_utc": utc_now_iso(),
                "event_type": event_type,
                "run_id": run_id,
                "cycle_id": cycle_id,
                "symbol": symbol,
                "action": action,
                "details": details or {},
            }
        )

    def _write_jsonl(self, obj: Dict[str, Any]) -> None:
        try:
            self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with self.jsonl_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        except Exception:
            # never crash trading loop because audit file write failed
            pass
