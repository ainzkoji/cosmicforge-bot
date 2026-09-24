"""
app/adaptive/audit_log.py

Section 10: Structured observability and auditability for every adaptive decision.

Provides:
  - AdaptiveAuditLog: thread-safe circular buffer of the last N adaptive decisions
  - Each record contains all Section 10 required fields
  - Inspection API: get_last_n(), explain_current()
"""
from __future__ import annotations

import json
import logging
import threading
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from app.adaptive.policies import PolicyDecision

logger = logging.getLogger(__name__)

_BASELINE_STATE = {
    "caution_modifier": 0.0,
    "size_multiplier": 1.0,
    "leverage_multiplier": 1.0,
    "aggressiveness_score": 1.0,
}

# ---------------------------------------------------------------------------
# AdaptiveDecisionRecord — one record per engine tick
# ---------------------------------------------------------------------------

@dataclass
class AdaptiveDecisionRecord:
    """
    Full structured record of one adaptive state resolution.
    Contains all fields required by Section 10.
    """
    timestamp: str
    adaptive_state_before: Dict[str, Any]
    adaptive_state_after: Dict[str, Any]
    inputs_used: Dict[str, Any]
    reason_codes: List[str]
    sample_sizes: Dict[str, int]
    bounds_applied: Dict[str, Any]
    affected_modules: List[str]
    persisted_or_reconstructed: str          # 'DURABLE' | 'HEURISTIC' | 'RECONSTRUCTED'
    confidence_in_adjustment: float          # mean across all active policy decisions
    policy_decisions: List[Dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# AdaptiveAuditLog
# ---------------------------------------------------------------------------

class AdaptiveAuditLog:
    """
    Thread-safe circular buffer for adaptive decision records.

    Capacity: configurable, defaults to 100 records.

    API:
      record(...)         → append a new AdaptiveDecisionRecord
      get_last_n(n)       → List[AdaptiveDecisionRecord] newest first
      explain_current()   → human-readable explanation of current state vs. baseline
      to_json(n)          → JSON string of last n records
    """

    def __init__(self, capacity: int = 100) -> None:
        self._buffer: deque[AdaptiveDecisionRecord] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self.capacity = capacity

    def record(
        self,
        timestamp: str,
        before_state: Dict[str, Any],
        after_state: Dict[str, Any],
        inputs_used: Dict[str, Any],
        reason_codes: List[str],
        policy_decisions: List[PolicyDecision],
        was_reconstructed: bool,
    ) -> None:
        """
        Store one complete adaptive decision record.
        Extracts all Section 10 fields from the provided arguments.
        """
        # Build sample_sizes map from policy decisions
        sample_sizes = {
            pd.policy_name: pd.sample_size for pd in policy_decisions
        }

        # Build bounds_applied map
        bounds_applied = {
            pd.policy_name: {
                "field": pd.output_field,
                "bounds": pd.bounds_applied,
                "raw_target": round(pd.raw_target, 4),
            }
            for pd in policy_decisions
        }

        # Confidence: mean of all triggered policies with sufficient samples
        active = [
            pd.confidence_in_adjustment for pd in policy_decisions
            if pd.triggered
        ]
        mean_confidence = round(sum(active) / len(active), 3) if active else 1.0

        # Affected modules: which downstream modules see meaningful changes
        affected: List[str] = []
        after_size = after_state.get("size_multiplier", 1.0)
        after_lev  = after_state.get("leverage_multiplier", 1.0)
        # NOTE: caution_modifier (formerly confidence_gate_modifier) no longer
        # names an affected module. It used to list DynamicThresholdCalculator,
        # which is deleted; its effect now reaches sizing through the
        # aggressiveness score, which the RiskEngine/Sizing entry already covers.
        if after_size < 1.0:
            affected.append("RiskEngine/Sizing")
        if after_lev < 1.0:
            affected.append("LeverageBand")
        if any(
            pd.policy_name == "StrategyDeweightingPolicy" and pd.triggered
            for pd in policy_decisions
        ):
            affected.append("MasterEnsembleStrategy")

        persistence = "RECONSTRUCTED" if was_reconstructed else (
            "DURABLE" if all(
                pd.persistence == "DURABLE" for pd in policy_decisions if pd.triggered
            ) else "HEURISTIC"
        )

        entry = AdaptiveDecisionRecord(
            timestamp=timestamp,
            adaptive_state_before=before_state,
            adaptive_state_after=after_state,
            inputs_used=inputs_used,
            reason_codes=reason_codes,
            sample_sizes=sample_sizes,
            bounds_applied=bounds_applied,
            affected_modules=affected,
            persisted_or_reconstructed=persistence,
            confidence_in_adjustment=mean_confidence,
            policy_decisions=[asdict(pd) for pd in policy_decisions],
        )

        with self._lock:
            self._buffer.append(entry)

        logger.debug(
            "[AdaptiveAuditLog] Recorded tick at %s | reason_codes=%s | confidence=%.2f",
            timestamp, reason_codes, mean_confidence,
        )

    def get_last_n(self, n: int = 10) -> List[AdaptiveDecisionRecord]:
        """Return the last n records, newest first."""
        with self._lock:
            records = list(self._buffer)
        return list(reversed(records))[:n]

    def explain_current(self, current_state: Dict[str, Any]) -> str:
        """
        Return a human-readable explanation of why the current state differs from the
        neutral baseline. Useful for API/dashboard consumption (Section 10).
        """
        lines = ["=== Adaptive State Explanation ==="]
        for field_name, baseline_val in _BASELINE_STATE.items():
            current_val = current_state.get(field_name, baseline_val)
            delta = round(current_val - baseline_val, 4)
            if abs(delta) < 1e-4:
                lines.append(f"  {field_name}: {current_val:.4f} (at baseline)")
            else:
                direction = "ABOVE" if delta > 0 else "BELOW"
                lines.append(
                    f"  {field_name}: {current_val:.4f} "
                    f"({direction} baseline {baseline_val:.4f} by {abs(delta):.4f})"
                )

        # Pull reason codes from most recent record
        with self._lock:
            if self._buffer:
                last = list(self._buffer)[-1]
                if last.reason_codes:
                    lines.append(f"  Active reason codes: {', '.join(last.reason_codes)}")
                lines.append(f"  Confidence in adjustment: {last.confidence_in_adjustment:.2f}")
                lines.append(f"  Persistence: {last.persisted_or_reconstructed}")

        return "\n".join(lines)

    def to_json(self, n: int = 10) -> str:
        """Return JSON of last n records (for API endpoints)."""
        records = self.get_last_n(n)
        return json.dumps([asdict(r) for r in records], default=str)

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._buffer)
