from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from app.persistence.db import DB, utc_now_iso
from app.ops.context import get_run_id, get_cycle_id


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

    # ✅ ADD: compatibility methods (DO NOT REMOVE ANYTHING ELSE)
    def start_run(
        self,
        run_id: str,
        mode: str,
        interval_seconds: int,
        max_symbols: int,
    ) -> None:
        """
        Compatibility method used by main.py.
        Does NOT manage runs table — it only records a RUN_START event.
        """
        self.event(
            event_type="RUN",
            run_id=run_id,
            cycle_id=None,
            symbol=None,
            action="RUN_START",
            details={
                "mode": mode,
                "interval_seconds": interval_seconds,
                "max_symbols": max_symbols,
            },
        )

    def stop_run(self, run_id: str) -> None:
        """
        Compatibility method used by main.py.
        Records a RUN_STOP event.
        """
        self.event(
            event_type="RUN",
            run_id=run_id,
            cycle_id=None,
            symbol=None,
            action="RUN_STOP",
            details={},
        )

    # backward-compat alias
    def log_event(self, *args, **kwargs):
        return self.event(*args, **kwargs)

    def event(
        self,
        event_type: str,
        run_id: Optional[str] = None,
        cycle_id: Optional[str] = None,
        symbol: Optional[str] = None,
        action: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Robust audit event writer.

        Priority for run_id / cycle_id:
        1) Explicit argument (if provided)
        2) Context-local value (automatic)
        3) None
        """

        resolved_run_id = run_id or get_run_id()
        resolved_cycle_id = cycle_id or get_cycle_id()

        def _json_default(o):
            # date / datetime
            try:
                return o.isoformat()
            except Exception:
                pass

            # Decimal, UUID, etc.
            return str(o)

        payload = json.dumps(details or {}, ensure_ascii=False, default=_json_default)

        # 1) DB (source of truth)
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO events(
                    timestamp_utc,
                    run_id,
                    cycle_id,
                    symbol,
                    event_type,
                    action,
                    details_json
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    utc_now_iso(),
                    resolved_run_id,
                    resolved_cycle_id,
                    symbol,
                    event_type,
                    action,
                    payload,
                ),
            )

        # 2) JSONL mirror (for tailing)
        self._write_jsonl(
            {
                "timestamp_utc": utc_now_iso(),
                "event_type": event_type,
                "run_id": resolved_run_id,
                "cycle_id": resolved_cycle_id,
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
