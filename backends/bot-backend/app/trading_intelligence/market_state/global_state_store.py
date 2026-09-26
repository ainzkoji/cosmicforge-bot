"""Append-only GlobalMarketState evidence (``cati_global_market_states``)."""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from app.trading_intelligence.contracts.global_market_state import GlobalMarketState
from app.trading_intelligence.hashing import short_id

TABLE = "cati_global_market_states"


class GlobalMarketStateStore:
    def __init__(self, db: Any):
        self.db = db

    def append(self, state: GlobalMarketState, *, cycle_id: Optional[str] = None,
               bot_instance_id: Optional[str] = None, broker_account_id: Optional[str] = None,
               now_ms: Optional[int] = None, asset_contexts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        ts = int(now_ms or time.time() * 1000)
        rr = state.component("risk_regime")
        row = {"evidence_id": short_id("gmsev", {"s": state.global_state_id, "c": cycle_id, "b": bot_instance_id}),
               "global_state_id": state.global_state_id, "state_hash": state.state_hash,
               "decision_time": state.decision_time, "timeframe": state.timeframe,
               "asset_classes": ",".join(state.asset_classes), "risk_regime": rr.label if rr.status == "AVAILABLE" else None,
               "input_count": len(state.input_market_state_ids), "cycle_id": cycle_id,
               "bot_instance_id": bot_instance_id, "broker_account_id": broker_account_id,
               "schema_version": state.schema_version, "payload": json.dumps({**state.to_dict(), **({"asset_contexts": asset_contexts} if asset_contexts is not None else {})}, sort_keys=True),
               "recorded_at": ts}
        with self.db.connect() as conn:
            conn.execute(f"INSERT OR IGNORE INTO {TABLE} ({', '.join(row)}) VALUES ({', '.join('?' for _ in row)})",
                         tuple(row.values()))
        return row

    def latest(self, *, timeframe: Optional[str] = None, limit: int = 1) -> List[Dict[str, Any]]:
        sql, args = f"SELECT * FROM {TABLE}", []
        if timeframe:
            sql += " WHERE timeframe=?"
            args.append(timeframe)
        with self.db.connect() as conn:
            rows = conn.execute(sql + " ORDER BY decision_time DESC, recorded_at DESC LIMIT ?",
                                (*args, int(limit))).fetchall()
        return [dict(r) for r in rows]


__all__ = ["GlobalMarketStateStore", "TABLE"]
