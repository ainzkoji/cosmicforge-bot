#!/usr/bin/env python3
"""Candidate impact-model report from recorded calibration observations (EXT-LIQ-01). READ-ONLY.

    python scripts/perf/liquidity_impact_report.py --url sqlite:///path/to/platform.db --out report.json
    python scripts/perf/liquidity_impact_report.py --url postgresql://... --provenance recorded --out report.json

Reads ``platform_spread_observations`` (written per entry attempt by ``scale.spread_gate.record_observation``) and,
per (venue, symbol, side), fits a CANDIDATE model of realized slippage against participation:

    realized_slippage_bps = a + b * participation_pct        participation_pct = 100 * intended_notional / band_notional

(ordinary least squares on the filled observations), together with the ratio of realized slippage to the stored
visible-book-walk estimate, and the distributions of participation, slippage, spread, depth age and partial fills.

What this is NOT:

* It is never a calibration. The report is always ``status: UNCALIBRATED`` / ``calibrated: false``. It writes
  nothing anywhere: no policy, no budget, no record (it opens read-only transactions only). A calibrated policy
  needs an owner approval reference (``SpreadPolicy`` and the schema enforce it) that no code creates; this tool
  does not change that.
* Provenance is DECLARED by the caller (``--provenance``), not verified. The default is ``synthetic``: observations
  from fake venues, drills and tests. A report on synthetic observations is labelled ``evidence_class: SYNTHETIC``
  and says ``usable_for_calibration: false`` whatever the fit looks like. ``recorded`` (observations of real venue
  attempts) still yields only a CANDIDATE for owner review.
* The stored ``impact_estimate_bps`` is a mechanical walk of the VISIBLE book (``VISIBLE_BOOK_WALK_UNCALIBRATED``):
  hidden liquidity, queue position and the book's reaction are unknown to it.

The report is reproducible: ``input_fingerprint`` hashes every observation used, and ``report_fingerprint`` hashes
the report without its generation time, so the same observations always give the same fingerprints.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
for _p in (ROOT / "backends" / "shared", ROOT / "backends" / "bot-backend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

REPORT_SCHEMA = "cosmicforge-liquidity-impact-candidate-v1"
MODEL_VERSION = "ols-slippage-vs-participation-1"
#: below this many FILLED observations a group gets no candidate coefficients at all
MIN_FILLED_OBSERVATIONS = 100
PROVENANCES = ("synthetic", "recorded")
COLUMNS = ("observation_id", "source", "venue", "symbol", "side", "account_id", "intended_quantity",
           "intended_notional", "depth_age_ms", "depth_levels", "spread_bps", "band_notional", "reference_price",
           "fill_price", "filled_quantity", "partial_fill", "realized_slippage_bps", "impact_estimate_bps",
           "impact_model", "impact_calibrated", "entry_status", "recorded_at")


class ReportError(RuntimeError):
    pass


def _dec(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    return d if d.is_finite() else None


def _quantiles(values: Sequence[Decimal]) -> Dict[str, Optional[str]]:
    d = sorted(values)
    if not d:
        return {"n": 0, "min": None, "p50": None, "p95": None, "max": None}
    pick = lambda q: d[min(len(d) - 1, int(round(q * (len(d) - 1))))]  # noqa: E731
    return {"n": len(d), "min": str(d[0]), "p50": str(pick(0.5)), "p95": str(pick(0.95)), "max": str(d[-1])}


def _ols(xs: Sequence[float], ys: Sequence[float]) -> Optional[Dict[str, float]]:
    n = len(xs)
    if n < 2:
        return None
    mx, my = math.fsum(xs) / n, math.fsum(ys) / n
    sxx = math.fsum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return None
    b = math.fsum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    a = my - b * mx
    resid = [y - (a + b * x) for x, y in zip(xs, ys)]
    sst = math.fsum((y - my) ** 2 for y in ys)
    sse = math.fsum(r * r for r in resid)
    return {"intercept_bps": round(a, 6), "slope_bps_per_participation_pct": round(b, 6),
            "residual_std_bps": round(math.sqrt(sse / max(n - 2, 1)), 6),
            "r_squared": round(1 - sse / sst, 6) if sst > 0 else None, "n": n}


def read_observations(db) -> List[Dict[str, Any]]:
    with db.transaction(write=False) as conn:
        try:
            rows = conn.all(f"SELECT {', '.join(COLUMNS)} FROM platform_spread_observations ORDER BY observation_id")
        except Exception as exc:  # noqa: BLE001
            raise ReportError(f"OBSERVATIONS_UNAVAILABLE: {type(exc).__name__}") from exc
    return [dict(zip(COLUMNS, r)) for r in rows]


def _fingerprint(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def build_report(observations: Sequence[Dict[str, Any]], *, provenance: str = "synthetic",
                 min_filled: int = MIN_FILLED_OBSERVATIONS) -> Dict[str, Any]:
    if provenance not in PROVENANCES:
        raise ReportError(f"PROVENANCE_UNKNOWN:{provenance}")
    canonical = [{k: (str(v) if v is not None else None) for k, v in o.items()} for o in observations]
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for o in observations:
        groups.setdefault(f"{o['venue']}|{o['symbol']}|{o['side']}", []).append(o)
    out_groups = {}
    for key in sorted(groups):
        rows = groups[key]
        filled = [o for o in rows if _dec(o["fill_price"]) is not None and _dec(o["realized_slippage_bps"]) is not None]
        part, slip, est, ratio = [], [], [], []
        for o in filled:
            band, want = _dec(o["band_notional"]), _dec(o["intended_notional"])
            s = _dec(o["realized_slippage_bps"])
            if band and band > 0 and want is not None:
                part.append((Decimal(100) * want / band, s))
            slip.append(s)
            e = _dec(o["impact_estimate_bps"])
            if e is not None:
                est.append(e)
                if e > 0:
                    ratio.append(s / e)
        reasons = []
        if provenance == "synthetic":
            reasons.append("SYNTHETIC_OBSERVATIONS")
        if len(filled) < min_filled:
            reasons.append(f"INSUFFICIENT_FILLED_OBSERVATIONS:{len(filled)}<{min_filled}")
        fit = None
        if len(part) >= min_filled:
            fit = _ols([float(x) for x, _ in part], [float(y) for _, y in part])
            if fit is None:
                reasons.append("PARTICIPATION_HAS_NO_SPREAD")
        partials = [o for o in rows if o["partial_fill"] is not None]
        out_groups[key] = {
            "observations": len(rows), "filled_observations": len(filled),
            "sources": sorted({str(o["source"]) for o in rows}),
            "impact_models_seen": sorted({str(o["impact_model"]) for o in rows}),
            "any_row_claims_calibrated_impact": any(int(o["impact_calibrated"] or 0) != 0 for o in rows),
            "participation_pct": _quantiles([x for x, _ in part]),
            "realized_slippage_bps": _quantiles(slip),
            "visible_walk_estimate_bps": _quantiles(est),
            "realized_over_visible_walk": _quantiles([r.quantize(Decimal("0.0001")) for r in ratio]),
            "spread_bps": _quantiles([d for d in (_dec(o["spread_bps"]) for o in rows) if d is not None]),
            "depth_age_ms": _quantiles([d for d in (_dec(o["depth_age_ms"]) for o in rows) if d is not None]),
            "partial_fill_rate": (round(sum(int(o["partial_fill"]) for o in partials) / len(partials), 6)
                                  if partials else None),
            "candidate": None if fit is None else {
                "model": MODEL_VERSION, "formula": "realized_slippage_bps = intercept + slope * participation_pct",
                **fit, "valid_participation_pct_range": [str(min(x for x, _ in part)), str(max(x for x, _ in part))]},
            "status": "UNCALIBRATED", "not_usable_because": reasons,
        }
    usable = provenance == "recorded" and any(g["candidate"] for g in out_groups.values())
    report = {
        "schema": REPORT_SCHEMA, "model_version": MODEL_VERSION, "status": "UNCALIBRATED", "calibrated": False,
        "evidence_class": "SYNTHETIC" if provenance == "synthetic" else "RECORDED_PROVENANCE_DECLARED_NOT_VERIFIED",
        "provenance_declared": provenance,
        "usable_for_calibration": False,
        "candidate_for_owner_review": usable,
        "activation": ("NOT ACTIVATED. A calibrated spreading policy needs an owner approval reference "
                       "(EXT-LIQ-01). This report writes nothing and no code path sets calibrated."),
        "observations_total": len(observations), "min_filled_observations": min_filled,
        "input_fingerprint": _fingerprint(canonical), "groups": out_groups,
        "limits": ("The stored impact estimate walks the VISIBLE book only. A fit says how realized slippage related "
                   "to participation in THESE observations; it is not a capacity and not a venue fact."),
    }
    report["report_fingerprint"] = _fingerprint(report)
    report["generated_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return report


def report_from_database(url: str, *, provenance: str = "synthetic",
                         min_filled: int = MIN_FILLED_OBSERVATIONS) -> Dict[str, Any]:
    from shared_lib.persistence.sql import Database
    db = Database(url, pool_size=1)
    try:
        return build_report(read_observations(db), provenance=provenance, min_filled=min_filled)
    finally:
        db.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--url", required=True, help="the platform database URL (read only)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--provenance", choices=PROVENANCES, default="synthetic")
    ap.add_argument("--min-filled", type=int, default=MIN_FILLED_OBSERVATIONS)
    a = ap.parse_args(argv)
    report = report_from_database(a.url, provenance=a.provenance, min_filled=a.min_filled)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "evidence_class": report["evidence_class"],
                      "observations": report["observations_total"], "groups": len(report["groups"]),
                      "report_fingerprint": report["report_fingerprint"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
