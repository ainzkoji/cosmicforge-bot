"""Append-only SHADOW TradePlan evidence (Section 18.20).

Schema comes from the canonical migration (``shared_lib.persistence.
cati_schema``); this module issues no DDL and fails closed
(``TradePlanSchemaMissing``) when the table is absent. Rows are inserted
once per ``trade_plan_id`` (an identical re-append is an idempotent no-op)
and database triggers refuse UPDATE/DELETE. There is no execution-state
machine here: operational lifecycle belongs to the future execution path.
No secret is ever persisted -- the payload is the plan's analytical content.
"""
from __future__ import annotations

import dataclasses
import json
from typing import Any, Dict, List, Optional

from app.trading_intelligence.contracts.trade_plan import TradePlan
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import TRADE_PLAN_TABLE_VERSION

TABLE = "cati_trade_plans"


class TradePlanSchemaMissing(RuntimeError):
    """The canonical migration has not created the TradePlan evidence table."""


def trade_plan_payload(plan: TradePlan) -> Dict[str, Any]:
    return json.loads(json.dumps(dataclasses.asdict(plan), default=str, sort_keys=True))


def trade_plan_from_payload(payload: Dict[str, Any]) -> TradePlan:
    """Rebuild the immutable TradePlan from its stored analytical payload and
    VERIFY its identity: the recomputed content hash must equal the stored
    ``trade_plan_hash`` and id (Section 20.3 plan-hash integrity)."""
    from app.trading_intelligence.contracts.instrument import InstrumentKey
    from app.trading_intelligence.contracts.trade_plan import (
        AllowedEntryZone, ExecutionPreferences, ExpectedCosts, InvalidationCondition, TargetZone, ThesisCondition,
    )

    d = dict(payload)
    pairs = lambda xs: tuple(tuple(p) for p in xs)  # noqa: E731
    d["instrument_key"] = InstrumentKey(**d["instrument_key"])
    d["allowed_entry_zone"] = AllowedEntryZone(**d["allowed_entry_zone"])
    d["target_zones"] = tuple(TargetZone(**z) for z in d["target_zones"])
    d["expected_costs"] = ExpectedCosts(**d["expected_costs"])
    prefs = dict(d["execution_preferences"])
    prefs["fallback_order_styles"] = tuple(prefs["fallback_order_styles"])
    d["execution_preferences"] = ExecutionPreferences(**prefs)
    d["thesis_conditions"] = tuple(ThesisCondition(c["code"], pairs(c["evidence"])) for c in d["thesis_conditions"])
    d["invalidation_conditions"] = tuple(InvalidationCondition(c["code"], pairs(c["parameters"]))
                                         for c in d["invalidation_conditions"])
    d["reason_codes"] = tuple(d["reason_codes"])
    d["versions"] = pairs(d["versions"])
    plan = TradePlan(**d)
    from app.trading_intelligence.trade_plan.validation import verify_trade_plan_integrity

    if not verify_trade_plan_integrity(plan):
        raise ValueError("TRADE_PLAN_HASH_MISMATCH: stored plan content does not match its hash")
    return plan


class TradePlanEvidenceStore:
    def __init__(self, db: Any) -> None:
        self._db = db
        with db.connect() as conn:
            row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
        if row is None:
            raise TradePlanSchemaMissing(f"{TABLE} is missing: run shared_lib.persistence.migrations.migrate()")

    def append(self, plan: TradePlan) -> bool:
        """True when a new row was written; False for an idempotent repeat."""
        payload = trade_plan_payload(plan)
        with self._db.connect() as conn:
            cur = conn.execute(
                f"INSERT OR IGNORE INTO {TABLE} (trade_plan_id, trade_plan_hash, user_id, broker_account_id,"
                " bot_instance_id, run_id, cycle_id, candidate_id, economic_opportunity_id, portfolio_decision_id,"
                " reservation_id, canonical_symbol, venue, environment, side, mode, created_at, expires_at,"
                " schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (plan.trade_plan_id, plan.trade_plan_hash, plan.user_id, plan.broker_account_id, plan.bot_instance_id,
                 plan.run_id, plan.cycle_id, plan.source_candidate_id, plan.economic_opportunity_id,
                 plan.portfolio_decision_id, plan.portfolio_reservation_id, plan.instrument_key.canonical_symbol,
                 plan.venue, plan.environment, plan.side, plan.mode, plan.plan_created_at, plan.plan_expiry_time,
                 plan.schema_version, TRADE_PLAN_TABLE_VERSION, json.dumps(payload, sort_keys=True),
                 stable_hash(payload)),
            )
            return cur.rowcount == 1

    def get(self, trade_plan_id: str) -> Optional[Dict[str, Any]]:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {TABLE} WHERE trade_plan_id=?", (trade_plan_id,)).fetchone()
        return dict(row) if row is not None else None

    def load_plan(self, broker_account_id: str, trade_plan_id: str) -> Optional[TradePlan]:
        """Tenant-scoped, integrity-verified plan reload."""
        row = self.get(trade_plan_id)
        if row is None or row["broker_account_id"] != broker_account_id:
            return None
        return trade_plan_from_payload(json.loads(row["payload"]))

    def for_account(self, broker_account_id: str) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {TABLE} WHERE broker_account_id=? ORDER BY created_at, trade_plan_id",
                                (broker_account_id,)).fetchall()
        return [dict(r) for r in rows]


__all__ = ["TABLE", "TradePlanSchemaMissing", "trade_plan_payload", "trade_plan_from_payload", "TradePlanEvidenceStore"]
