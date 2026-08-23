from __future__ import annotations

import json
import time
from collections import defaultdict
from typing import Optional

from shared_lib.persistence.db import DB, utc_now_iso

from app.execution.entry_protection import EntryProtection, EntryState, SubmitState, get_entry_protection


class EntryRecoveryService:
    def __init__(self, db: Optional[DB] = None):
        self.db = db or DB()
        self._entry_prot = get_entry_protection(self.db)

    def list_entries(self, bot_id: str, symbol: Optional[str] = None) -> list[dict]:
        now_ms = int(time.time() * 1000)
        rows = self._entry_prot.list_entries(bot_id, symbol=symbol)
        output: list[dict] = []
        for row in rows:
            submitted_at_ms = int(row.get("submitted_at_ms") or 0)
            age_seconds = max(0.0, (now_ms - submitted_at_ms) / 1000.0)
            submit_state = str(row.get("submit_state") or SubmitState.NOT_SUBMITTED.value)
            ttl_seconds = (
                EntryProtection.PRE_SUBMIT_STALE_SECONDS
                if submit_state == SubmitState.NOT_SUBMITTED.value
                else EntryProtection.UNCERTAIN_TTL_SECONDS
            )
            recoverable = age_seconds >= ttl_seconds and int(row.get("flat_confirmations") or 0) >= 1
            row_copy = dict(row)
            row_copy["age_seconds"] = age_seconds
            row_copy["ttl_seconds"] = ttl_seconds
            row_copy["recoverable"] = recoverable
            output.append(row_copy)
        return output

    def release_entry(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        actor_user_id: str,
        exchange_flat_confirmed: bool,
        note: str = "",
        force: bool = False,
    ) -> dict:
        entry = self._entry_prot.get_entry(bot_id, symbol, side)
        if not entry:
            raise ValueError("Entry not found")
        if not exchange_flat_confirmed:
            raise ValueError("exchange_flat_confirmed must be true for manual release")

        submitted_at_ms = int(entry.get("submitted_at_ms") or 0)
        age_seconds = max(0.0, (int(time.time() * 1000) - submitted_at_ms) / 1000.0)
        submit_state = str(entry.get("submit_state") or SubmitState.NOT_SUBMITTED.value)
        ttl_seconds = (
            EntryProtection.PRE_SUBMIT_STALE_SECONDS
            if submit_state == SubmitState.NOT_SUBMITTED.value
            else EntryProtection.UNCERTAIN_TTL_SECONDS
        )
        flat_confirmations = int(entry.get("flat_confirmations") or 0)

        if not force and age_seconds < ttl_seconds and flat_confirmations < 1:
            raise ValueError("Entry is not stale enough for safe manual release")

        released = self._entry_prot.release_entry(
            bot_id=bot_id,
            symbol=symbol,
            side=side,
            reason=f"manual_release:{note[:120] or 'operator_confirmed_flat'}",
        )
        if not released:
            raise ValueError("Entry disappeared before release")

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO events (timestamp_utc, run_id, cycle_id, symbol, event_type, action, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    utc_now_iso(),
                    bot_id,
                    None,
                    symbol.upper(),
                    "ENTRY_PROTECTION_MANUAL_RELEASE",
                    "RELEASED",
                    json.dumps(
                        {
                            "actor_user_id": actor_user_id,
                            "bot_id": bot_id,
                            "symbol": symbol.upper(),
                            "side": side.upper(),
                            "note": note,
                            "force": force,
                            "exchange_flat_confirmed": exchange_flat_confirmed,
                            "released_state": released.get("state"),
                            "released_submit_state": released.get("submit_state"),
                        }
                    ),
                ),
            )

        return {
            "released": True,
            "bot_id": bot_id,
            "symbol": symbol.upper(),
            "side": side.upper(),
            "released_entry": released,
        }

    def list_events(
        self,
        bot_id: Optional[str] = None,
        symbol: Optional[str] = None,
        limit: int = 200,
    ) -> list[dict]:
        return self._entry_prot.list_events(bot_id=bot_id, symbol=symbol, limit=limit)

    def get_forensic_report(
        self,
        bot_id: Optional[str] = None,
        symbol: Optional[str] = None,
        since_ts_ms: Optional[int] = None,
        limit: int = 5000,
    ) -> dict:
        events = self._entry_prot.list_events(bot_id=bot_id, symbol=symbol, limit=limit)
        if since_ts_ms is not None:
            events = [e for e in events if int(e.get("ts_ms") or 0) >= int(since_ts_ms)]
        events_asc = list(reversed(events))
        bot_limits: dict[str, float] = {}
        with self.db.connect() as conn:
            query = "SELECT id, allocation_value FROM bot_instances"
            params: list[object] = []
            if bot_id:
                query += " WHERE id=?"
                params.append(bot_id)
            for row in conn.execute(query, tuple(params)).fetchall():
                bot_limits[str(row["id"])] = float(row["allocation_value"] or 0.0)

        counts: dict[tuple[str, str], int] = defaultdict(int)
        max_counts: dict[tuple[str, str], int] = defaultdict(int)
        duplicate_violations: list[dict] = []
        for event in events_asc:
            key = (str(event.get("bot_id") or ""), str(event.get("symbol") or ""))
            event_type = str(event.get("event_type") or "")
            if event_type == "INTENT_ACQUIRED":
                counts[key] += 1
                max_counts[key] = max(max_counts[key], counts[key])
                if counts[key] > 1:
                    duplicate_violations.append(
                        {
                            "bot_id": key[0],
                            "symbol": key[1],
                            "ts_ms": event.get("ts_ms"),
                            "count": counts[key],
                            "client_order_id": event.get("client_order_id"),
                            "intent_key": event.get("intent_key"),
                        }
                    )
            elif event_type in {"MARK_CLOSED", "RELEASED"}:
                counts[key] = max(0, counts[key] - 1)

        exposure_violations: list[dict] = []
        for event in events_asc:
            event_type = str(event.get("event_type") or "")
            protected_exposure = event.get("protected_exposure")
            max_exposure_limit = event.get("max_exposure_limit")
            if max_exposure_limit is None:
                max_exposure_limit = bot_limits.get(str(event.get("bot_id") or ""), 0.0)
            if protected_exposure is None or max_exposure_limit is None or float(max_exposure_limit) <= 0:
                continue
            if event_type == "EXPOSURE_BLOCKED":
                continue
            if float(protected_exposure) > float(max_exposure_limit) + 1e-9:
                exposure_violations.append(
                    {
                        "bot_id": event.get("bot_id"),
                        "symbol": event.get("symbol"),
                        "event_type": event_type,
                        "protected_exposure": protected_exposure,
                        "max_exposure_limit": max_exposure_limit,
                        "ts_ms": event.get("ts_ms"),
                    }
                )

        reused_groups: dict[tuple[str, str], int] = defaultdict(int)
        for event in events_asc:
            if str(event.get("event_type") or "") == "INTENT_REUSED":
                key = (
                    str(event.get("intent_key") or ""),
                    str(event.get("client_order_id") or ""),
                )
                reused_groups[key] += 1

        submit_unknown_map: dict[tuple[str, str], dict] = {}
        for event in events_asc:
            key = (
                str(event.get("intent_key") or ""),
                str(event.get("client_order_id") or ""),
            )
            event_type = str(event.get("event_type") or "")
            if event_type == "SUBMIT_UNKNOWN":
                submit_unknown_map[key] = {
                    "bot_id": event.get("bot_id"),
                    "symbol": event.get("symbol"),
                    "client_order_id": event.get("client_order_id"),
                    "intent_key": event.get("intent_key"),
                    "submit_unknown_ts_ms": event.get("ts_ms"),
                    "outcome": "UNRESOLVED",
                    "outcome_ts_ms": None,
                }
            elif event_type in {"OPEN_CONFIRMED", "MARK_CLOSED", "RELEASED"} and key in submit_unknown_map:
                submit_unknown_map[key]["outcome"] = event_type
                submit_unknown_map[key]["outcome_ts_ms"] = event.get("ts_ms")

        manual_releases = [
            {
                "bot_id": e.get("bot_id"),
                "symbol": e.get("symbol"),
                "side": e.get("side"),
                "reason": e.get("reason"),
                "ts_ms": e.get("ts_ms"),
            }
            for e in events_asc
            if str(e.get("event_type") or "") == "RELEASED"
            and str(e.get("reason") or "").startswith("manual_release:")
        ]

        return {
            "events_considered": len(events_asc),
            "duplicate_unresolved_violations": duplicate_violations,
            "max_unresolved_per_symbol": [
                {"bot_id": key[0], "symbol": key[1], "max_unresolved": value}
                for key, value in sorted(max_counts.items())
            ],
            "exposure_violations": exposure_violations,
            "reused_retries": [
                {"intent_key": key[0], "client_order_id": key[1], "reuse_count": value}
                for key, value in sorted(reused_groups.items())
            ],
            "submit_unknown_outcomes": list(submit_unknown_map.values()),
            "manual_releases": manual_releases,
        }
