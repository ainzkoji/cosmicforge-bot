from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.config import settings
from app.symbols.symbol_promotion import SymbolPromotionEvaluator
from shared_lib.persistence.db import DB


@dataclass(frozen=True)
class DemotionEvaluation:
    decision_type: str
    status: str
    fallback_mode: str
    evidence_summary: dict[str, Any]
    failure_reasons: list[str]

    @property
    def should_demote(self) -> bool:
        return self.decision_type == "AUTO_DEMOTION_RECOMMENDED" and self.status == "FAIL"


class SymbolDemotionEvaluator:
    """Dry-run demotion evaluator for Step 1.

    The first implementation only reports whether auto_top_n would be unsafe.
    It does not switch modes, edit runner symbols, or touch executor allowlists.
    """

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()

    def evaluate(self) -> DemotionEvaluation:
        current_mode = str(getattr(settings, "SYMBOL_UNIVERSE_MODE", "static") or "static")
        fallback_mode = str(getattr(settings, "AUTO_SYMBOL_DEMOTION_FALLBACK_MODE", "dynamic_shadow") or "dynamic_shadow")
        if current_mode != "auto_top_n":
            return DemotionEvaluation(
                decision_type="DEMOTION_EVALUATED",
                status="PASS",
                fallback_mode=fallback_mode,
                evidence_summary={
                    "current_mode": current_mode,
                    "dry_run": True,
                    "reason": "not_in_auto_top_n",
                },
                failure_reasons=[],
            )

        # Reuse the same live-safety checks as promotion. A future Step 2 can
        # add PnL/order-failure/slippage guards before executing demotion.
        promotion = SymbolPromotionEvaluator(self.db)
        with self.db.connect() as conn:
            safety = promotion._live_safety_snapshot(conn)
        failures = [item["reason"] for item in safety["failures"]]
        return DemotionEvaluation(
            decision_type="AUTO_DEMOTION_RECOMMENDED" if failures else "DEMOTION_EVALUATED",
            status="FAIL" if failures else "PASS",
            fallback_mode=fallback_mode,
            evidence_summary={
                "current_mode": current_mode,
                "fallback_mode": fallback_mode,
                "dry_run": True,
                "live_safety": safety,
            },
            failure_reasons=sorted(set(failures)),
        )
