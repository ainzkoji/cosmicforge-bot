"""Drift / live-calibration monitoring for CATI estimators (Section 23).

Population Stability Index over training-vs-live feature distributions; a
drifted feature is reported (and the promotion gate / fallback can act on
it). Monitoring never changes a decision by itself.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Sequence

#: conventional PSI reading: < 0.1 stable, 0.1-0.25 moderate, > 0.25 significant
PSI_SIGNIFICANT = 0.25


def psi(expected: Sequence[float], actual: Sequence[float], *, bins: int = 10) -> float:
    exp = sorted(float(x) for x in expected if x is not None)
    act = [float(x) for x in actual if x is not None]
    if len(exp) < bins or not act:
        return float("nan")
    edges = [exp[int(i * (len(exp) - 1) / bins)] for i in range(1, bins)]

    def dist(xs):
        counts = [0] * bins
        for x in xs:
            counts[sum(1 for e in edges if x > e)] += 1
        return [max(c / len(xs), 1e-6) for c in counts]

    e, a = dist(exp), dist(act)
    return sum((ai - ei) * math.log(ai / ei) for ei, ai in zip(e, a))


def drift_report(train_rows: Sequence[Mapping[str, Any]], live_rows: Sequence[Mapping[str, Any]],
                 columns: Sequence[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for c in columns:
        try:
            v = psi([r.get(c) for r in train_rows], [r.get(c) for r in live_rows])
        except (TypeError, ValueError):
            v = float("nan")  # categorical column: not a PSI feature
        out[c] = {"psi": v, "drifted": (not math.isnan(v)) and v > PSI_SIGNIFICANT}
    out["any_drift"] = any(isinstance(x, dict) and x["drifted"] for x in out.values())
    return out


__all__ = ["psi", "drift_report", "PSI_SIGNIFICANT"]
