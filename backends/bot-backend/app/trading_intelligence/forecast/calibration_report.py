"""Offline forecast/library calibration evaluation (Section 12.15 A12-A13).

Walk-forward and embargoed: every held-out row is forecast using ONLY rows
whose entire label horizon had finished before that row's decision time, so
the evaluation never scores a forecast against evidence that contained its
own outcome. Status assignment is controlled by a VERSIONED
``CalibrationPolicy`` (RESEARCH DEFAULT values, not production thresholds);
a library is never CALIBRATED merely because it exists.

CLI::

    python -m app.trading_intelligence.forecast.calibration_report \\
        --library <artifact_dir> [--embargo-bars N]
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.forecast import CalibrationStatus, ForecastStatus, TerminalOutcome
from app.trading_intelligence.forecast.engine import forecast_from_dimensions
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import CALIBRATION_SCHEMA_VERSION

_CLASSES = (
    TerminalOutcome.TARGET_BEFORE_STOP.value,
    TerminalOutcome.STOP_BEFORE_TARGET.value,
    TerminalOutcome.TIMEOUT.value,
)


@dataclass(frozen=True)
class CalibrationPolicy:
    """RESEARCH DEFAULTS -- to be replaced only after replay certification."""

    schema_version: str = CALIBRATION_SCHEMA_VERSION
    reliability_bins: int = 10
    min_train_rows: int = 30
    #: Deterministic cap on evaluated rows (stride sampling) to bound cost.
    max_eval_rows: int = 2000
    #: Fewer evaluated forecasts than this => at best UNCALIBRATED.
    min_research_samples: int = 50
    #: CALIBRATED requires at least this many evaluated forecasts...
    min_calibrated_samples: int = 300
    #: ...an ECE at or below this...
    max_ece: float = 0.05
    #: ...and a Brier skill (vs the base-rate forecaster) at or above this.
    min_brier_skill: float = 0.02
    #: Every setup family present must itself have this many evaluated rows.
    min_family_samples: int = 30
    support_bucket_edges: Tuple[int, ...] = (15, 50)

    @property
    def policy_hash(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True)
class CalibrationReport:
    library_hash: str
    policy_hash: str
    n_evaluated: int
    n_skipped_no_train: int
    brier_score: Optional[float]
    brier_baseline: Optional[float]
    brier_skill: Optional[float]
    ece: Optional[float]
    reliability: Tuple[Mapping[str, Any], ...]
    multiclass_brier: Optional[float]
    class_frequency: Mapping[str, Mapping[str, float]]
    by_setup_family: Mapping[str, Mapping[str, Any]]
    by_regime: Mapping[str, Mapping[str, Any]]
    by_support: Mapping[str, Mapping[str, Any]]
    schema_version: str = CALIBRATION_SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["reliability"] = [dict(b) for b in self.reliability]
        return d


class _TrainView:
    """Duck-typed stand-in for a HistoricalOutcomeLibrary over a walk-forward
    training slice (avoids re-hashing a full library per held-out row)."""

    library_hash = "walk_forward_training_slice"
    library_version = "walk_forward"
    calibration_status = CalibrationStatus.UNCALIBRATED.value

    def __init__(self, rows: Sequence[Any]):
        self.rows = tuple(rows)


class _CandidateStub:
    def __init__(self, row: Any):
        self.setup_candidate_id = row.label.setup_candidate_id
        self.market_state_id = row.label.market_state_id
        self.setup_family = row.label.setup_family
        self.room_to_target_R = row.continuous_features.get("room_to_target_R")


def _bucket_metrics(pairs: Sequence[Tuple[float, int]], bins: int) -> Dict[str, Any]:
    n = len(pairs)
    if n == 0:
        return {"n": 0, "brier": None, "ece": None}
    brier = sum((p - y) ** 2 for p, y in pairs) / n
    return {"n": n, "brier": brier, "ece": _ece(pairs, bins)}


def _ece(pairs: Sequence[Tuple[float, int]], bins: int) -> float:
    n = len(pairs)
    total = 0.0
    for b in range(bins):
        lo, hi = b / bins, (b + 1) / bins
        members = [(p, y) for p, y in pairs if (lo <= p < hi) or (b == bins - 1 and p == 1.0)]
        if members:
            mean_p = sum(p for p, _ in members) / len(members)
            obs = sum(y for _, y in members) / len(members)
            total += (len(members) / n) * abs(obs - mean_p)
    return total


def _support_bucket(raw_support: int, edges: Sequence[int]) -> str:
    for i, edge in enumerate(edges):
        if raw_support < edge:
            return f"<{edge}" if i == 0 else f"{edges[i - 1]}-{edge - 1}"
    return f">={edges[-1]}"


def evaluate_library_calibration(
    library: Any, *, embargo_ms: int = 0, policy: Optional[CalibrationPolicy] = None,
) -> CalibrationReport:
    policy = policy or CalibrationPolicy()
    rows = sorted(library.rows, key=lambda r: (r.label.decision_time, r.label.label_id))
    stride = max(1, -(-len(rows) // policy.max_eval_rows)) if policy.max_eval_rows else 1

    binary: List[Tuple[float, int]] = []
    per_family: Dict[str, List[Tuple[float, int]]] = {}
    per_regime: Dict[str, List[Tuple[float, int]]] = {}
    per_support: Dict[str, List[Tuple[float, int]]] = {}
    multi_sq = 0.0
    forecast_mass = {c: 0.0 for c in _CLASSES}
    observed_count = {c: 0 for c in _CLASSES}
    skipped = 0

    for j in range(0, len(rows), stride):
        test = rows[j]
        t = test.label.decision_time
        train = [r for r in rows[:j] if r.label.decision_time + embargo_ms <= t]
        if len(train) < policy.min_train_rows:
            skipped += 1
            continue
        fc = forecast_from_dimensions(_CandidateStub(test), test.cohort_dimensions, _TrainView(train))
        if fc.status != ForecastStatus.VALID.value:
            skipped += 1
            continue
        y = 1 if test.label.net_profitable else 0
        binary.append((fc.p_net_profitable_mean, y))
        per_family.setdefault(test.label.setup_family, []).append((fc.p_net_profitable_mean, y))
        per_regime.setdefault(test.cohort_dimensions.get("dominant_regime", "UNKNOWN"), []).append((fc.p_net_profitable_mean, y))
        per_support.setdefault(_support_bucket(fc.raw_support, policy.support_bucket_edges), []).append((fc.p_net_profitable_mean, y))
        probs = {
            TerminalOutcome.TARGET_BEFORE_STOP.value: fc.p_target_before_stop,
            TerminalOutcome.STOP_BEFORE_TARGET.value: fc.p_stop_before_target,
            TerminalOutcome.TIMEOUT.value: fc.p_timeout,
        }
        for c in _CLASSES:
            forecast_mass[c] += probs[c]
            hit = 1.0 if test.label.terminal_outcome == c else 0.0
            observed_count[c] += int(hit)
            multi_sq += (probs[c] - hit) ** 2

    n = len(binary)
    if n:
        brier = sum((p - y) ** 2 for p, y in binary) / n
        base = sum(y for _, y in binary) / n
        baseline = sum((base - y) ** 2 for _, y in binary) / n
        skill = (1.0 - brier / baseline) if baseline > 0 else None
        bins = []
        for b in range(policy.reliability_bins):
            lo, hi = b / policy.reliability_bins, (b + 1) / policy.reliability_bins
            members = [(p, y) for p, y in binary if (lo <= p < hi) or (b == policy.reliability_bins - 1 and p == 1.0)]
            bins.append({
                "bin": b, "lower": lo, "upper": hi, "n": len(members),
                "mean_forecast": (sum(p for p, _ in members) / len(members)) if members else None,
                "observed_frequency": (sum(y for _, y in members) / len(members)) if members else None,
            })
        ece = _ece(binary, policy.reliability_bins)
        multi = multi_sq / n
        freq = {c: {"forecast": forecast_mass[c] / n, "observed": observed_count[c] / n} for c in _CLASSES}
    else:
        brier = baseline = skill = ece = multi = None
        bins, freq = [], {c: {"forecast": 0.0, "observed": 0.0} for c in _CLASSES}

    return CalibrationReport(
        library_hash=library.library_hash, policy_hash=policy.policy_hash, n_evaluated=n, n_skipped_no_train=skipped,
        brier_score=brier, brier_baseline=baseline, brier_skill=skill, ece=ece, reliability=tuple(bins),
        multiclass_brier=multi, class_frequency=freq,
        by_setup_family={k: _bucket_metrics(v, policy.reliability_bins) for k, v in sorted(per_family.items())},
        by_regime={k: _bucket_metrics(v, policy.reliability_bins) for k, v in sorted(per_regime.items())},
        by_support={k: _bucket_metrics(v, policy.reliability_bins) for k, v in sorted(per_support.items())},
    )


def derive_status(report: CalibrationReport, policy: CalibrationPolicy) -> str:
    """CALIBRATED only when every versioned condition holds; otherwise the
    library stays RESEARCH_ONLY (a report exists) or UNCALIBRATED."""
    if report.n_evaluated < policy.min_research_samples:
        return CalibrationStatus.UNCALIBRATED.value
    ok = (
        report.n_evaluated >= policy.min_calibrated_samples
        and report.ece is not None and report.ece <= policy.max_ece
        and report.brier_skill is not None and report.brier_skill >= policy.min_brier_skill
        and all(m["n"] >= policy.min_family_samples for m in report.by_setup_family.values())
    )
    return CalibrationStatus.CALIBRATED.value if ok else CalibrationStatus.RESEARCH_ONLY.value


def build_calibration_record(report: CalibrationReport, policy: CalibrationPolicy) -> Dict[str, Any]:
    return {
        "library_hash": report.library_hash, "policy": asdict(policy), "policy_hash": policy.policy_hash,
        "report": report.to_dict(), "status": derive_status(report, policy),
        "schema_version": CALIBRATION_SCHEMA_VERSION,
    }


def status_from_stored_record(record: Mapping[str, Any], *, library_hash: str) -> str:
    """Re-derive the status from the stored report + policy (never trust a
    bare 'status' assertion). Any inconsistency => UNCALIBRATED."""
    if record.get("library_hash") != library_hash or record.get("report", {}).get("library_hash") != library_hash:
        return CalibrationStatus.UNCALIBRATED.value
    pol = dict(record["policy"])
    pol["support_bucket_edges"] = tuple(pol.get("support_bucket_edges", ()))
    policy = CalibrationPolicy(**pol)
    if policy.policy_hash != record.get("policy_hash"):
        return CalibrationStatus.UNCALIBRATED.value
    r = dict(record["report"])
    r["reliability"] = tuple(r.get("reliability", ()))
    report = CalibrationReport(**r)
    return derive_status(report, policy)


def main(argv: Optional[Sequence[str]] = None) -> int:
    from app.trading_intelligence.forecast.artifact import CALIBRATION_FILE, load_library_artifact
    from app.trading_intelligence.contracts.setup import timeframe_to_ms

    ap = argparse.ArgumentParser(description="Offline CATI library calibration report")
    ap.add_argument("--library", required=True, help="library artifact directory")
    ap.add_argument("--embargo-bars", type=int, default=None, help="label-horizon embargo in bars (default: manifest horizon)")
    ap.add_argument("--expected-hash", default=None)
    ap.add_argument("--mode", default="RUNTIME", choices=["RUNTIME", "TEST", "DEVELOPMENT"],
                    help="library trust mode; RUNTIME refuses synthetic/fixture/unknown libraries")
    args = ap.parse_args(argv)

    library, manifest = load_library_artifact(args.library, expected_hash=args.expected_hash, mode=args.mode)
    bars = args.embargo_bars if args.embargo_bars is not None else int(manifest.get("label_horizon_bars", 0))
    bar_ms = timeframe_to_ms(manifest["timeframe"]) or 0
    policy = CalibrationPolicy()
    report = evaluate_library_calibration(library, embargo_ms=bars * bar_ms, policy=policy)
    record = build_calibration_record(report, policy)
    (Path(args.library) / CALIBRATION_FILE).write_text(json.dumps(record, sort_keys=True, indent=2), encoding="utf-8")
    print(json.dumps({"library_hash": report.library_hash, "n_evaluated": report.n_evaluated,
                      "brier": report.brier_score, "ece": report.ece, "status": record["status"]}, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
