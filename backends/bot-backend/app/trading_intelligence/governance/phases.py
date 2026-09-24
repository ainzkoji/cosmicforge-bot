"""Section 25 migration phases M0-M9: authority per phase + transition evidence.

Staged-certification interpretation (documented, not a rewrite of evidence):
M5 "replay eligible" requires the HISTORICAL gates A-G (integrity, net
expectancy, cost stress, untouched holdout, concentration, calibration,
evidence count) plus replay operational integrity. M6 then grants CATI sole
alpha on DEMO only, which is where the Forward Demo evidence (Gate H) is
collected; nothing beyond demo is allowed until Gate H (and Gate I) PASS.
CATI is not "production certified" because M5 passed.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Tuple


@dataclass(frozen=True)
class PhaseSpec:
    phase: str
    title: str
    v2_authority: str       # ACTIVE | BENCHMARK_ON_DEMO | BENCHMARK_ON_PROMOTED_SCOPES | NONE
    cati_authority: str     # NONE | ADVISORY | DEMO_SOLE_ALPHA | SCOPED_SOLE_ALPHA | SOLE_ALPHA
    v2_fallback_allowed: bool
    entry_requirements: Tuple[str, ...]


PHASES: Tuple[PhaseSpec, ...] = (
    PhaseSpec("M0", "FREEZE V2", "ACTIVE", "NONE", True, ()),
    PhaseSpec("M1", "SECTION 9 SHADOW", "ACTIVE", "NONE", True, ("v2_benchmark_tag", "section9_shadow_verified")),
    PhaseSpec("M2", "SECTIONS 10-12 SHADOW", "ACTIVE", "NONE", True, ("sections_10_12_shadow_verified",)),
    PhaseSpec("M3", "SECTIONS 13-17 SHADOW", "ACTIVE", "NONE", True, ("sections_13_17_shadow_verified",)),
    PhaseSpec("M4", "SECTION 19 SHADOW", "ACTIVE", "ADVISORY", True, ("section19_shadow_verified",)),
    PhaseSpec("M5", "REPLAY CERTIFICATION", "ACTIVE", "ADVISORY", True,
              ("certification_run_id", "policy_freeze_hash", "certification_policy_frozen")),
    PhaseSpec("M6", "DEMO CATI ACTIVE", "BENCHMARK_ON_DEMO", "DEMO_SOLE_ALPHA", True,
              ("certification_run_id", "policy_freeze_hash", "replay_gates_passed")),
    PhaseSpec("M7", "LIMITED PRODUCTION", "BENCHMARK_ON_PROMOTED_SCOPES", "SCOPED_SOLE_ALPHA", True,
              ("forward_demo_gate_passed", "operational_gate_passed", "promoted_scope_hash")),
    PhaseSpec("M8", "FULL PROMOTION", "NONE", "SOLE_ALPHA", False,
              ("production_evidence_passed", "rollback_release_tag")),
    PhaseSpec("M9", "CLEANUP", "NONE", "SOLE_ALPHA", False, ("rollback_evidence_ref", "migration_window_elapsed")),
)
PHASE_BY_ID: Mapping[str, PhaseSpec] = {p.phase: p for p in PHASES}
PHASE_IDS: Tuple[str, ...] = tuple(p.phase for p in PHASES)
#: the HISTORICAL certification gates M6 requires (Gate H is collected IN M6)
M5_REPLAY_GATES = ("A_INTEGRITY", "B_NET_EXPECTANCY", "C_COST_STRESS", "D_HOLDOUT", "E_CONCENTRATION",
                   "F_CALIBRATION", "G_EVIDENCE_COUNT")


def phase_index(phase: Optional[str]) -> int:
    return PHASE_IDS.index(phase) if phase in PHASE_IDS else -1


def next_phase(phase: str) -> Optional[str]:
    i = phase_index(phase)
    return PHASE_IDS[i + 1] if 0 <= i < len(PHASE_IDS) - 1 else None


def rollback_targets(phase: str) -> Tuple[str, ...]:
    """M1-M6: back to M0 (frozen V2 benchmark only). M7: to M6 or M0.
    M8+: NONE -- rollback is the new-entry kill switch or a tagged release
    revert; CATI never silently falls back to V2."""
    i = phase_index(phase)
    if 1 <= i <= 6:
        return ("M0",)
    if phase == "M7":
        return ("M6", "M0")
    return ()


__all__ = ["PhaseSpec", "PHASES", "PHASE_BY_ID", "PHASE_IDS", "M5_REPLAY_GATES", "phase_index", "next_phase",
           "rollback_targets"]
