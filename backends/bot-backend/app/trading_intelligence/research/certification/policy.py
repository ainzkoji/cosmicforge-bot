"""The one canonical, versioned CertificationPolicy (Section 22).

Every threshold carries its PROVENANCE:

* ``SOURCE_SPEC``      -- stated by the Section 22 specification itself
* ``EXISTING_REPO``    -- an authoritative value already in this repository
                          (cited by module path)
* ``RESEARCH_DEFAULT`` -- a METHODOLOGY parameter (bootstrap repetitions,
                          confidence level, seed, block rule). Never a
                          performance promise; reported in every artifact.
* ``NOT_CONFIGURED``   -- the specification does not give a number and the
                          repository has none. A gate that needs it FAILS
                          CLOSED with ``CERTIFICATION_POLICY_INCOMPLETE``; it
                          never passes by default.

Thresholds may later be TIGHTENED (a new policy version => new hash => new
experiment). They are never loosened because CATI fails.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import CERTIFICATION_POLICY_SCHEMA_VERSION

SOURCE_SPEC = "SOURCE_SPEC"
EXISTING_REPO = "EXISTING_REPO"
RESEARCH_DEFAULT = "RESEARCH_DEFAULT"
NOT_CONFIGURED = "NOT_CONFIGURED"
PROVENANCES = (SOURCE_SPEC, EXISTING_REPO, RESEARCH_DEFAULT, NOT_CONFIGURED)


@dataclass(frozen=True)
class PolicyValue:
    value: Any
    provenance: str
    note: str = ""

    def __post_init__(self) -> None:
        if self.provenance not in PROVENANCES:
            raise ValueError(f"unknown policy provenance {self.provenance!r}")
        if self.provenance == NOT_CONFIGURED and self.value is not None:
            raise ValueError("a NOT_CONFIGURED threshold carries no value")
        if self.provenance != NOT_CONFIGURED and self.value is None:
            raise ValueError("a configured threshold needs a value")

    @property
    def configured(self) -> bool:
        return self.provenance != NOT_CONFIGURED

    def to_dict(self) -> dict:
        v = list(self.value) if isinstance(self.value, tuple) else self.value
        return {"value": v, "provenance": self.provenance, "note": self.note}


def _v(value, provenance, note=""):
    return field(default_factory=lambda: PolicyValue(value, provenance, note))


def _nc(note):
    return field(default_factory=lambda: PolicyValue(None, NOT_CONFIGURED, note))


@dataclass(frozen=True)
class CertificationPolicy:
    schema_version: str = CERTIFICATION_POLICY_SCHEMA_VERSION

    # -- Gate A: integrity ------------------------------------------------------------
    max_lookahead_violations: PolicyValue = _v(0, SOURCE_SPEC, "zero known lookahead violations")
    max_replay_hash_mismatches: PolicyValue = _v(0, SOURCE_SPEC, "deterministic replay identity/hash stable")

    # -- Gate B: net expectancy -------------------------------------------------------
    min_net_expectancy_R: PolicyValue = _v(0.0, SOURCE_SPEC, "positive after-cost expectancy (strictly > 0)")
    min_probability_expectancy_positive: PolicyValue = _nc(
        "spec requires 'high-confidence' P(E>0) but gives no number")

    # -- Gate C: cost stress ----------------------------------------------------------
    cost_stress_multipliers: PolicyValue = _v((1.0, 1.5, 2.0), SOURCE_SPEC, "base, 1.5x, 2x")
    min_net_expectancy_R_at_1_5x: PolicyValue = _v(
        0.0, SOURCE_SPEC, "1.5x must remain economically credible: expectancy stays > 0")
    catastrophic_floor_R_at_2x: PolicyValue = _nc(
        "spec requires 2x not to show 'catastrophic fragility' but defines no floor")

    # -- Gate D: holdout ---------------------------------------------------------------
    holdout_required: PolicyValue = _v(True, SOURCE_SPEC, "untouched chronological holdout")
    holdout_fraction: PolicyValue = _v(
        0.10, EXISTING_REPO, "app/research/dataset.partition: remainder after 0.60/0.15/0.15")
    max_holdout_drawdown_R: PolicyValue = _nc("spec requires 'acceptable drawdown' but gives no number")

    # -- Gate E: concentration ---------------------------------------------------------
    max_single_segment_positive_share: PolicyValue = _nc(
        "spec forbids 'unacceptable' single symbol/month/regime dependency without a number")

    # -- Gate F: calibration (the existing forecast CalibrationPolicy is authoritative) ---
    calibration_policy_ref: PolicyValue = _v(
        "forecast.calibration_report.CalibrationPolicy", EXISTING_REPO,
        "min 300 evaluated forecasts, ECE <= 0.05, Brier skill >= 0.02, 30 per family")
    calibration_bins: PolicyValue = _v(10, EXISTING_REPO, "CalibrationPolicy.reliability_bins")

    # -- Gate G: evidence count ---------------------------------------------------------
    min_accepted_evidence_count: PolicyValue = _nc(
        "spec requires 'enough accepted examples' without a number; never lowered")
    min_stratum_sample_size: PolicyValue = _v(
        15, EXISTING_REPO, "economics AdmissionPolicy.minimum_raw_support")

    # -- Gate H: forward demo -------------------------------------------------------------
    min_forward_demo_days: PolicyValue = _v(30, SOURCE_SPEC, "at least 30 calendar days")
    min_forward_demo_executed_count: PolicyValue = _nc(
        "spec requires a minimum executed-opportunity count without a number")

    # -- Gate I: operational ----------------------------------------------------------------
    max_operational_defects: PolicyValue = _v(
        0, SOURCE_SPEC, "no unresolved reconciliation / protection / idempotency defects")

    # -- statistical methodology (RESEARCH_DEFAULT; part of the policy identity) ------------
    bootstrap_repetitions: PolicyValue = _v(2000, RESEARCH_DEFAULT, "circular block bootstrap draws")
    confidence_level: PolicyValue = _v(0.95, RESEARCH_DEFAULT, "two-sided interval level")
    bootstrap_seed: PolicyValue = _v(22022, RESEARCH_DEFAULT, "deterministic seed")
    bootstrap_block_rule: PolicyValue = _v("ceil(n**(1/3))", RESEARCH_DEFAULT,
                                           "block length for serially dependent R series")
    expected_shortfall_alpha: PolicyValue = _v(0.05, RESEARCH_DEFAULT, "tail fraction for ES")
    min_tail_samples: PolicyValue = _v(20, RESEARCH_DEFAULT, "ES reported only with >= this many trades")

    # -- overfitting controls -------------------------------------------------------------
    cscv_partitions: PolicyValue = _v(8, RESEARCH_DEFAULT, "even number of chronological CSCV folds")
    cscv_min_variants: PolicyValue = _v(2, RESEARCH_DEFAULT, "PBO needs >= 2 actually compared variants")
    max_pbo: PolicyValue = _nc("no authoritative PBO ceiling; PBO is reported, never auto-passed")
    parameter_neighbor_min_positive_share: PolicyValue = _nc(
        "no authoritative share of neighbors that must stay positive")

    # -- stage coverage (windows) -----------------------------------------------------------
    fast_window_days: PolicyValue = _v((30, 60), SOURCE_SPEC, "typical FAST window 30-60 days")
    medium_window_days: PolicyValue = _v(180, SOURCE_SPEC, "typical MEDIUM window ~180 days")
    full_min_days: PolicyValue = _v(730, SOURCE_SPEC, "FULL targets multi-year data (>= 2 years)")
    embargo_bars: PolicyValue = _v(0, RESEARCH_DEFAULT, "extra quiet bars beyond the label-horizon purge")

    # -----------------------------------------------------------------------------------
    def thresholds(self) -> Dict[str, PolicyValue]:
        return {f.name: getattr(self, f.name) for f in fields(self) if isinstance(getattr(self, f.name), PolicyValue)}

    def get(self, name: str) -> PolicyValue:
        value = getattr(self, name)
        if not isinstance(value, PolicyValue):
            raise KeyError(name)
        return value

    def missing(self, *names: str) -> Tuple[str, ...]:
        return tuple(n for n in names if not self.get(n).configured)

    def to_dict(self) -> dict:
        return {"schema_version": self.schema_version,
                "thresholds": {k: v.to_dict() for k, v in sorted(self.thresholds().items())}}

    @property
    def policy_hash(self) -> str:
        return stable_hash(self.to_dict())

    def not_configured(self) -> Tuple[str, ...]:
        return tuple(sorted(k for k, v in self.thresholds().items() if not v.configured))

    def research_defaults(self) -> Tuple[str, ...]:
        return tuple(sorted(k for k, v in self.thresholds().items() if v.provenance == RESEARCH_DEFAULT))

    def configure(self, **values: Any) -> "CertificationPolicy":
        """A NEW policy (new hash) with operator-supplied thresholds, recorded
        as SOURCE_SPEC-level operator configuration. Existing configured
        thresholds may only be TIGHTENED, never loosened."""
        changes = {}
        for name, value in values.items():
            current = self.get(name)
            if current.configured and _loosens(name, current.value, value):
                raise ValueError(f"certification threshold {name} may be tightened, never loosened")
            changes[name] = PolicyValue(value, SOURCE_SPEC, "operator-configured threshold")
        return replace(self, **changes)


#: thresholds where a HIGHER value is stricter (everything else: lower is stricter)
_HIGHER_IS_STRICTER = {
    "min_net_expectancy_R", "min_probability_expectancy_positive", "min_net_expectancy_R_at_1_5x",
    "catastrophic_floor_R_at_2x", "min_accepted_evidence_count", "min_stratum_sample_size",
    "min_forward_demo_days", "min_forward_demo_executed_count", "parameter_neighbor_min_positive_share",
    "min_tail_samples", "bootstrap_repetitions", "confidence_level", "full_min_days",
}
_LOWER_IS_STRICTER = {
    "max_lookahead_violations", "max_replay_hash_mismatches", "max_holdout_drawdown_R",
    "max_single_segment_positive_share", "max_operational_defects", "max_pbo",
}


def _loosens(name: str, old: Any, new: Any) -> bool:
    try:
        if name in _HIGHER_IS_STRICTER:
            return float(new) < float(old)
        if name in _LOWER_IS_STRICTER:
            return float(new) > float(old)
    except (TypeError, ValueError):
        return True
    return new != old  # structural settings: any change is a new policy, never a silent loosening


def default_certification_policy() -> CertificationPolicy:
    return CertificationPolicy()


def policy_summary(policy: Optional[CertificationPolicy] = None) -> Mapping[str, Any]:
    p = policy or default_certification_policy()
    return {"policy_hash": p.policy_hash, "not_configured": list(p.not_configured()),
            "research_defaults": list(p.research_defaults())}


__all__ = ["CertificationPolicy", "PolicyValue", "default_certification_policy", "policy_summary",
           "SOURCE_SPEC", "EXISTING_REPO", "RESEARCH_DEFAULT", "NOT_CONFIGURED"]
