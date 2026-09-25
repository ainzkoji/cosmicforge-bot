"""SHADOW capital-routing evidence (Phase 6F).

In shadow mode the capital planner runs for every portfolio-selected
opportunity and its decision is persisted -- including the transfer it WOULD
request and a seeded simulation of how that transfer would have resolved --
but nothing is submitted to any broker and no order is placed.

``cati_capital_plan_evidence`` is append-only (triggers refuse UPDATE and
DELETE). Rows are account-scoped (user_id + broker_account_id) and never
aggregated into shared/global caches.
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, Optional

from app.trading_intelligence.capital.planner import CapitalPlan
from app.trading_intelligence.research.certification.transfer_sim import TransferModel, simulate_transfer

SHADOW = "SHADOW"


class CapitalPlanEvidenceStore:
    TABLE = "cati_capital_plan_evidence"

    def __init__(self, db: Any):
        self.db = db

    def record(self, plan: CapitalPlan, *, user_id: str, bot_instance_id: Optional[str], cycle_id: Optional[str],
               opportunity_id: Optional[str], mode: str = SHADOW, seed: int = 0,
               model: TransferModel = TransferModel(), transferable_at_plan: Optional[float] = None) -> Dict[str, Any]:
        now = int(time.time() * 1000)
        simulated = None
        if plan.needs_transfer and plan.transfer is not None:
            sim = simulate_transfer(seed=seed, transfer_key=plan.transfer.idempotency_key, submitted_at_ms=now,
                                    amount=float(plan.transfer.amount),
                                    transferable_at_submit=float(transferable_at_plan)
                                    if transferable_at_plan is not None else float(plan.transfer.amount), model=model)
            simulated = {"final_status": sim.final_status, "went_unknown": sim.went_unknown,
                         "confirmation_latency_ms": (sim.confirmed_at_ms - now) if sim.confirmed_at_ms else None}
        row = {
            "evidence_id": f"cplan_{uuid.uuid4().hex[:16]}", "user_id": user_id,
            "broker_account_id": plan.broker_account_id, "bot_instance_id": bot_instance_id, "cycle_id": cycle_id,
            "opportunity_id": opportunity_id, "mode": mode, "outcome": plan.outcome, "product": plan.product,
            "plan_json": json.dumps(plan.to_dict(), sort_keys=True), "simulated_transfer_json": json.dumps(simulated),
            "created_at": now,
        }
        with self.db.connect() as conn:
            conn.execute(f"INSERT INTO {self.TABLE} ({', '.join(row)}) VALUES ({', '.join('?' for _ in row)})",
                         tuple(row.values()))
        return row

    def list(self, *, user_id: str, broker_account_id: str, limit: int = 100):
        with self.db.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.TABLE} WHERE user_id=? AND broker_account_id=? "
                                "ORDER BY created_at DESC LIMIT ?", (user_id, broker_account_id, int(limit))).fetchall()
        return [dict(r) for r in rows]


__all__ = ["CapitalPlanEvidenceStore", "SHADOW"]
