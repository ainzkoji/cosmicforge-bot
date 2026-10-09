"""The one-time registration of hypothesis 18 / Mandate 004 in the research register (Section H, Step 2.2).

Kept as code so the registered record can be re-derived and compared, never so it can be re-run: the register
refuses a second registration of the same number, identity or mandate.
"""
from __future__ import annotations

from typing import Any, Dict

from app.trading_intelligence.research.governance.mandates import amend_mandate, register_hypothesis_and_mandate
from app.trading_intelligence.research.governance.register import ResearchRegister

from . import spec


def hypothesis_record() -> Dict[str, Any]:
    return {
        "hypothesis_id": spec.HYPOTHESIS_ID, "family_id": spec.FAMILY_ID, "mandate_id": spec.MANDATE_ID,
        "hypothesis_number": spec.HYPOTHESIS_NUMBER,
        "strategy_description": "Long-or-flat breakout ensemble on daily bars of the 20 most liquid Binance USD-M "
                                "USDT perpetuals: seven breakout lookbacks (10 to 150 days) give a strength from 0 "
                                "to 1; position size is strength x risk per trade x equity / stop distance, with a "
                                "3 x ATR(20) ratcheting stop, portfolio risk and leverage caps and drawdown brakes.",
        "created_at": "2026-10-07 (specification frozen, commit f85a6cf0); registered 2026-10-09",
        "specification_hash": spec.SPECIFICATION_SHA256,
        "source_commit": "f85a6cf04660a6a74d0bbb08bc12b75b08ae9eae (frozen on origin/step1-portal-to-engine); "
                         "brought to main unchanged as 7d8cea0 (same blob 8f320e95)",
        "data_manifest_hash": "NOT_YET_FROZEN (recorded by the DATASET_FROZEN register record before any run)",
        "development_period": {"start": spec.DEVELOPMENT_START, "end": spec.DEVELOPMENT_END},
        "holdout_period": {"start": spec.HOLDOUT_START, "end": spec.HOLDOUT_END, "opened": False},
        "evaluation_id": "NONE_YET", "decision_date": "NONE_YET", "status": "REGISTERED", "failure_reasons": [],
        "governance_stage": "M0 (research only; live trading DISABLED; demo promotion NOT AUTHORIZED)",
        "evidence_locations": [spec.SPECIFICATION_PATH, spec.INTERPRETATION_PATH,
                               "docs/research/CATI_RESEARCH_GOVERNANCE.md"],
        "origin": "REGISTERED_BEFORE_EVALUATION", "candidate_family": "Daily Trend", "research_engine": "CATI",
        "specifications_of_this_family_tried": 1,
    }


def register_mandate_004(register: ResearchRegister, *, research_code_commit: str, registered_by: str,
                         approved_by: str, approved_at: str, authorization_reference: str) -> Dict[str, Any]:
    out = register_hypothesis_and_mandate(
        register, hypothesis=hypothesis_record(), mandate_id=spec.MANDATE_ID,
        specification_path=spec.SPECIFICATION_PATH, specification_version=spec.SPECIFICATION_VERSION,
        rule_artifact=spec.RULE_ARTIFACT, research_code_commit=research_code_commit, registered_by=registered_by,
        approved_by=approved_by, approved_at=approved_at, authorization_reference=authorization_reference)
    out["amendment_record"] = amend_mandate(
        register, mandate_id=spec.MANDATE_ID, kind="IMPLEMENTATION_INTERPRETATION",
        summary="Twenty points the frozen specification leaves open, fixed before any price series was loaded "
                "(ATR averaging, order sizing price, universe window, stop ratchet, funding timing, missing "
                "data, period accounts, brake variants). No specification value is changed.",
        artifact_path=spec.INTERPRETATION_PATH, changes_strategy_rules=False, recorded_by=registered_by,
        authorization_reference=authorization_reference)
    return out


__all__ = ["hypothesis_record", "register_mandate_004"]
