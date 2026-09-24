"""Shadow / champion-challenger evidence (Section 23.13).

``shadow_compare`` records the deterministic and the ML estimate for the SAME
decision point (market_state_id + candidate lineage) and RETURNS THE
DETERMINISTIC VALUE UNCHANGED -- a shadow prediction can never reach an order.
Outcomes are joined later from the canonical research labels (by
setup_candidate_id); nothing here is ever updated.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence

from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.observability.sanitize import sanitize_payload

from .contracts import CATI_ML_CONTRACT_VERSION


class ShadowRecorder:
    TABLE = "cati_ml_shadow_predictions"

    def __init__(self, db: Any):
        self._db = db

    def shadow_compare(self, *, model_id: str, role: str, deterministic: Any, ml: Any, decision_time: int,
                       market_state_id: Optional[str] = None, setup_candidate_id: Optional[str] = None,
                       ood_score: Optional[float] = None, decision_difference: Optional[str] = None) -> Any:
        payload = sanitize_payload(json.loads(json.dumps({
            "model_id": model_id, "role": role, "deterministic": deterministic, "ml": ml, "ood_score": ood_score,
            "decision_difference": decision_difference, "market_state_id": market_state_id,
            "setup_candidate_id": setup_candidate_id, "decision_time": int(decision_time)}, default=str)))
        pid = short_id("mlsp", {"m": model_id, "ms": market_state_id, "c": setup_candidate_id, "t": decision_time})
        try:
            with self._db.connect() as conn:
                conn.execute(
                    f"INSERT OR IGNORE INTO {self.TABLE} (prediction_id, model_id, role, market_state_id, "
                    "setup_candidate_id, decision_time, recorded_at, schema_version, table_version, payload, "
                    "payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (pid, model_id, role, market_state_id, setup_candidate_id, int(decision_time),
                     int(time.time() * 1000), CATI_ML_CONTRACT_VERSION, CATI_ML_CONTRACT_VERSION,
                     json.dumps(payload, sort_keys=True), stable_hash(payload)))
        except Exception as exc:  # evidence failure is recorded, and never changes the decision
            from app.trading_intelligence.observability.logging import record_stage_error

            record_stage_error("ml.shadow_compare", "ML_SHADOW", exc)
        return deterministic

    def rows(self, model_id: str) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            out = [dict(r) for r in conn.execute(f"SELECT * FROM {self.TABLE} WHERE model_id=? ORDER BY decision_time",
                                                 (model_id,))]
        for r in out:
            r["payload"] = json.loads(r["payload"])
        return out


def champion_challenger(rows: Sequence[Mapping[str, Any]], outcomes: Mapping[str, int]) -> Dict[str, Any]:
    """Brier of deterministic (champion) vs ML (challenger) probabilities on
    realized MARKET outcomes, joined by setup_candidate_id."""
    pairs = [(float(r["payload"]["deterministic"]), float(r["payload"]["ml"]), int(outcomes[r["setup_candidate_id"]]))
             for r in rows if r.get("setup_candidate_id") in outcomes
             and r["payload"].get("deterministic") is not None and r["payload"].get("ml") is not None]
    if not pairs:
        return {"n": 0, "status": "INSUFFICIENT_EVIDENCE"}
    n = len(pairs)
    det = sum((d - y) ** 2 for d, _m, y in pairs) / n
    ml = sum((m - y) ** 2 for _d, m, y in pairs) / n
    return {"n": n, "status": "OK", "brier_deterministic": det, "brier_ml": ml, "ml_not_worse": ml <= det}


__all__ = ["ShadowRecorder", "champion_challenger"]
